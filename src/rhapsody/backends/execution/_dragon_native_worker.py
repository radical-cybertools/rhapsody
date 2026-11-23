"""Code that runs inside the Dragon processes of ``DragonNativeExecutionBackend``.

The backend registers this module with ``cloudpickle.register_pickle_by_value``, so its functions
travel to Dragon processes by value. A worker therefore never imports ``rhapsody``, only the
standard library, cloudpickle and Dragon. Keep it that way: no imports from ``rhapsody`` here.

Messages are cloudpickled to ``bytes`` before they go on a Dragon queue, so a value that cannot
be pickled is reported as an error instead of failing inside the queue. A task's value is pickled
on its own inside its message, so a value the client cannot load fails only that task.
"""

import asyncio
import contextlib
import ctypes
import heapq
import inspect
import io
import itertools
import os
import pickle
import queue
import sys
import tempfile
import threading
import time
import traceback
from collections import deque

try:
    import cloudpickle
except ImportError:  # pragma: no cover - only Dragon processes run this code, and Dragon needs it
    cloudpickle = None

try:
    from dragon.managed_memory import DragonPoolAllocationNotAvailable as PoolFull
except ImportError:  # pragma: no cover - only Dragon processes run this code
    PoolFull = MemoryError

# Every process a task starts inherits this variable, so the backend can find and signal a
# task's leftover children after Dragon has killed the task's own process.
TOKEN_ENV = "RHAPSODY_DRAGON_NATIVE_TASK"  # noqa: S105 - a variable name, not a secret

READY = "ready"
DONE = "done"
RESULT = "result"
# From the relay: a task it could not put on its worker's inbox.
UNSENT = "unsent"

# Seconds a result waits for room in Dragon's memory pool before its task fails.
PUT_PATIENCE = 60.0
# Most messages, and bytes, the relay forwards to the client at once, and the seconds between its
# checks for results while it waits for the client.
RELAY_BATCH = 4096
RELAY_BYTES = 1 << 22
RELAY_POLL = 0.0005
# Seconds the relay's control thread tries a put on an inbox of its own node before it hands the
# task to the node's threads.
RELAY_FAST_PUT = 0.01
# Threads that put tasks on one node's inboxes: a worker whose inbox takes nothing holds one of
# them, so fewer stalled workers than this on a node hold up none of its others.
RELAY_SENDERS = 4


def call(fn, args, kwargs):
    """Call ``fn``; drive the result with ``asyncio.run`` when ``fn`` is async."""
    result = fn(*args, **kwargs)
    if inspect.iscoroutine(result):
        return asyncio.run(result)
    if inspect.isawaitable(result):

        async def _await():
            return await result

        return asyncio.run(_await())
    return result


def outcome(fn, args, kwargs):
    """Return ``(ok, value, traceback)`` for one call of ``fn``.

    ``SystemExit`` and its like come back as a ``RuntimeError``: raised in the client when it awaits
    the task, they would end the client too.
    """
    try:
        return True, call(fn, args, kwargs), None
    except Exception as exc:  # noqa: BLE001 - the error goes back to the client
        return False, exc, traceback.format_exc()
    except BaseException as exc:  # noqa: BLE001
        return False, RuntimeError(f"task raised {exc!r}"), traceback.format_exc()


def pack(msg, value_index):
    """Pickle ``msg``, with ``msg[value_index]`` pickled on its own inside it.

    The client can then always load the message, and it fails only this task when it cannot load
    the value. A value that cannot be pickled here is replaced by an error, and ``ok``, the field
    before it, becomes False.
    """
    msg = list(msg)
    try:
        msg[value_index] = cloudpickle.dumps(msg[value_index])
    except Exception as exc:  # noqa: BLE001
        name = type(msg[value_index]).__name__
        error = RuntimeError(f"cannot pickle the task's {name}: {exc!r}")
        msg[value_index] = cloudpickle.dumps(error)
        msg[value_index - 1] = False
    return cloudpickle.dumps(tuple(msg))


def put_waiting(results, raw, flush=False):
    """Put ``raw`` on ``results``, waiting for room: on a full pool, Dragon raises and does not
    wait.

    The room comes back as the client takes earlier results off the queue. With ``flush``, the
    put returns only once the queue holds ``raw``, not when Dragon's transport has taken it.
    """
    deadline = time.monotonic() + PUT_PATIENCE
    delay = 0.01
    while True:
        try:
            results.put(raw, flush=flush)
            return
        except PoolFull:
            if time.monotonic() >= deadline:
                raise
            time.sleep(delay)
            delay = min(delay * 2, 1.0)


def too_large(exc) -> bool:
    """Whether Dragon refused a message larger than its memory pool can ever hold
    (``DRAGON_DYNHEAP_REQUESTED_SIZE_TOO_LARGE``), as against a pool that is full for now.

    Dragon raises a ``DragonPoolError`` or a ``DragonFLIError`` for it, not the pool-full error:
    only its return code tells it apart.
    """
    seen = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        if "REQUESTED_SIZE_TOO_LARGE" in f"{getattr(exc, '_lib_err_str', '')} {exc.args!r}":
            return True
        exc = exc.__cause__ or exc.__context__
    return False


def send(results, msg, value_index, flush=False):
    """Send a result message; one that Dragon cannot take fails its task, not the worker."""
    raw = pack(msg, value_index)
    try:
        put_waiting(results, raw, flush)
    except Exception as exc:  # noqa: BLE001 - the error goes back to the client
        fields = list(msg)
        fields[value_index] = RuntimeError(f"cannot send {len(raw)} bytes of results: {exc}")
        fields[value_index - 1] = False
        fields[value_index + 2 :] = [""] * len(fields[value_index + 2 :])  # the output may be why
        put_waiting(results, pack(tuple(fields), value_index), flush)


def _open_append(path):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    return os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)


def flush_all():
    """Flush Python's ``sys.stdout`` and ``sys.stderr``, and every stdio stream of C code."""
    for stream in (sys.stdout, sys.stderr):
        with contextlib.suppress(Exception):
            stream.flush()
    with contextlib.suppress(Exception):
        ctypes.CDLL(None).fflush(None)


@contextlib.contextmanager
def redirected(out_fd, err_fd):
    """Point file descriptors 1 and 2 at ``out_fd`` and ``err_fd`` for the duration.

    Redirecting the descriptors, not only ``sys.stdout``, also catches output from C code and from
    child processes. Buffers are flushed at each switch, C's too, so output goes with the task that
    wrote it. The saved descriptors are not inheritable, so a child keeps none of them.
    """
    flush_all()
    saved = os.dup(1), os.dup(2)
    os.dup2(out_fd, 1)
    os.dup2(err_fd, 2)
    try:
        yield
    finally:
        flush_all()
        os.dup2(saved[0], 1)
        os.dup2(saved[1], 2)
        os.close(saved[0])
        os.close(saved[1])


def _anon_fd():
    """An unnamed file on this node's temporary directory."""
    tmp = tempfile.gettempdir()
    try:
        return os.open(tmp, os.O_RDWR | os.O_TMPFILE, 0o600)  # type: ignore[attr-defined]
    except (AttributeError, OSError):  # no O_TMPFILE here, or not on this file system
        fd, path = tempfile.mkstemp(dir=tmp)
        os.unlink(path)
        return fd


def _read_all(fd):
    os.lseek(fd, 0, os.SEEK_SET)
    chunks = []
    while True:
        chunk = os.read(fd, 1 << 20)
        if not chunk:
            break
        chunks.append(chunk)
    return b"".join(chunks).decode(errors="replace")


def run_task(token, fn, args, kwargs, out_path, err_path, inline):
    """Run one pool task; return ``(ok, value, traceback, stdout, stderr)``.

    With ``out_path``/``err_path`` the output is appended to those files and the paths come back.
    Otherwise, with ``inline``, it is captured in anonymous files on this node and comes back as
    text. With neither, it goes wherever the worker's own output goes. What fails around the
    function, say an output path this node cannot write, fails only the task, not the worker.
    """
    cwd = None
    fds = []
    os.environ[TOKEN_ENV] = token
    try:
        cwd = os.getcwd()
        if out_path:
            fds.append(_open_append(out_path))
            fds.append(_open_append(err_path))
        elif inline:
            fds.append(_anon_fd())
            fds.append(_anon_fd())
        else:
            ok, value, tb = outcome(fn, args, kwargs)
            return ok, value, tb, "", ""
        with redirected(*fds):
            ok, value, tb = outcome(fn, args, kwargs)
        if out_path:
            return ok, value, tb, out_path, err_path
        return ok, value, tb, _read_all(fds[0]), _read_all(fds[1])
    except Exception as exc:  # noqa: BLE001 - the error goes back to the client
        return False, exc, traceback.format_exc(), "", ""
    finally:
        for fd in fds:
            os.close(fd)
        os.environ.pop(TOKEN_ENV, None)
        if cwd is not None:  # the task may have removed the directory it moved to
            with contextlib.suppress(OSError):
                os.chdir(cwd)


def worker_main(wid, inbox, results):
    """A pool worker: report ready, then run tasks from ``inbox`` until it receives ``None``.

    Each inbox item is a pickled ``(uid, fn_data, rest)``: ``fn_data`` pickles the task's function,
    and ``rest`` its ``(token, args, kwargs, out_path, err_path, inline)``, continuing that pickle,
    so one unpickler loads the two as from one pickle. The client keeps a function's pickle while
    the function stays the same, so many tasks can carry the same ``fn_data``. A call that cannot
    be unpickled here (say, its function's module is missing on this node) still fails only its
    own task. Each result is a pickled ``(DONE, wid, uid, ok, value, traceback, stdout, stderr)``.
    """
    import socket

    results.put(cloudpickle.dumps((READY, wid, socket.gethostname(), os.getpid())))
    while True:
        raw = inbox.get()
        if raw is None:
            return
        uid, fn_data, rest = cloudpickle.loads(raw)
        del raw
        loader = pickle.Unpickler(io.BytesIO(fn_data + rest))  # noqa: S301 - the client's message
        del fn_data, rest
        try:
            fn = loader.load()
            token, args, kwargs, out_path, err_path, inline = loader.load()
        except BaseException as exc:  # noqa: BLE001 - the error goes back to the client
            del loader
            send(results, (DONE, wid, uid, False, exc, traceback.format_exc(), "", ""), 4)
            continue
        del loader  # its memo holds everything it loaded
        ok, value, tb, out, err = run_task(token, fn, args, kwargs, out_path, err_path, inline)
        send(results, (DONE, wid, uid, ok, value, tb, out, err), 4)
        # Waiting for the next task, the worker keeps nothing of this one: a large result would
        # stay on the node, in no task's measurement.
        del fn, args, kwargs, value, tb, out, err


def relay_main(
    control, results, inbound, idle_interval, send_timeout, linger, patience=None, local=None
):
    """Carry tasks to the workers and results back, in a process of its own on the client's node.

    The backend's threads share the GIL of the client's process. Every queue call gives the GIL up
    about twice, and when another thread there keeps it busy, each time costs up to a switch
    interval to get it back: one put and one get per task made the backend start some 50 tasks a
    second next to a thread that never blocks. Here nothing else wants the GIL, so the relay makes
    the calls for each task, and the client sends and takes them in batches:

    - ``control`` brings lists of operations: ``("inbox", wid, queue, host)`` names a worker's
      inbox and its node, ``("forget", wid)`` drops it, ``("send", wid, uid, payload)`` puts a
      task on it, and ``("ack",)`` says the client has handled the last batch it took. ``None``
      ends the relay's threads. The control thread puts a task on an inbox of ``local``, its own
      node, at once when that takes at most ``RELAY_FAST_PUT`` seconds, and hands every other send
      on: ``RELAY_SENDERS`` threads per node make those puts, each worker's in order but the
      workers apart, so a worker or node whose inboxes take nothing holds up neither other nodes
      nor, up to ``RELAY_SENDERS - 1`` stalled workers, others on its node, and acknowledgements
      are never late behind a put.
    - A sender waits out a full memory pool for up to ``patience`` seconds (``PUT_PATIENCE`` when
      None), retrying the same task, so tasks reach each worker in order. A send that fails comes
      back to the client as ``(UNSENT, wid, uid, kind, error, payload)`` through ``results``, in
      order with the worker's messages, or in the next batch when ``results`` has no room;
      ``kind`` is ``"full"`` (the pool stayed full; the payload comes back as its size in bytes),
      ``"too_large"`` (larger than the pool can ever hold: its size too), ``"timeout"`` (the
      inbox took no task for ``send_timeout`` seconds) or ``"broken"``.
    - ``results`` brings what workers and function processes send, and the relay forwards it to
      ``inbound`` as ``(messages, drained_at)``: the messages it had, and the last time it found
      ``results`` empty, or None if it did not. It forwards a batch once the client has
      acknowledged the last one, so a batch holds everything that came while the client was busy,
      or after ``10 * idle_interval`` seconds without an acknowledgement, so that one lost or late
      never holds results for good. After an acknowledgement that came with tasks to send, it
      waits a tenth of the time the client took over the last batch, at most ``linger`` seconds,
      so that short tasks come back in the next batch, not in one each: the slower the client, the
      more each batch is worth. An idle client gets an empty batch every ``idle_interval``
      seconds, which still moves ``drained_at`` on. A full pool never ends the relay: a batch
      waits for room for as long as the pool is full. A ``None`` on ``results`` is forwarded as
      ``None`` and ends the relay.
    """
    ready = threading.Event()  # the client has handled the last batch it took
    ready.set()
    forwarded_at = [0.0]  # when the relay last forwarded a batch
    hold_until = [0.0]  # until when it waits for the tasks the client just sent
    stale_after = 10 * idle_interval
    notes: queue.SimpleQueue[bytes] = queue.SimpleQueue()  # reports with no room on ``results``

    def report(wid, uid, kind, error, payload):
        msg = (UNSENT, wid, uid, kind, f"{type(error).__name__}: {error}", payload)
        raw = cloudpickle.dumps(msg)
        try:  # in order with the worker's own messages
            results.put(raw, timeout=idle_interval)
        except Exception:  # noqa: BLE001 - a full pool: it goes with the next batch instead
            notes.put(raw)

    def put_one(inbox, wid, uid, payload, waits):
        """Put one task on its worker's inbox; return when to try again, or None when done.

        On a full pool the task waits, for up to ``patience`` from its first try, without holding
        a thread: ``waits`` keeps its deadline and its next backoff between tries.
        """
        try:
            inbox.put(payload, timeout=send_timeout)
        except Exception as exc:  # noqa: BLE001 - the client decides what follows
            if too_large(exc):
                report(wid, uid, "too_large", exc, len(payload))
            elif isinstance(exc, PoolFull):
                now = time.monotonic()
                limit = PUT_PATIENCE if patience is None else patience
                deadline, delay = waits.get(uid, (now + limit, 0.01))
                if now < deadline:
                    waits[uid] = (deadline, min(2 * delay, 1.0))
                    return min(now + delay, deadline)
                report(wid, uid, "full", exc, len(payload))
            elif isinstance(exc, queue.Full):
                error = TimeoutError(f"the put took over {send_timeout} s")
                report(wid, uid, "timeout", error, payload)
            else:  # the client replaces the worker
                report(wid, uid, "broken", exc, payload)
        waits.pop(uid, None)
        return None

    def or_exit(target, *args):
        def run():
            try:
                target(*args)
            except BaseException:  # noqa: BLE001 - the client sees the relay die, not tasks vanish
                traceback.print_exc()
                os._exit(70)

        return run

    class NodeSender:
        """The puts for one node's workers, made by ``RELAY_SENDERS`` threads.

        A worker's operations go one at a time and in order, but the workers are served apart: a
        worker whose inbox takes nothing holds a thread, and the other threads serve the rest. A
        task that finds the pool full is tried again later, and holds no thread meanwhile.
        """

        def __init__(self):
            self.cv = threading.Condition()
            self.ops = {}  # wid -> its operations not yet made
            self.waiting: deque[int] = deque()  # wids with operations, served by no thread
            self.later = []  # heap of (when, seq, wid): workers whose next put waits for room
            self.seq = itertools.count()
            self.served = set()  # wids a thread is serving
            self.inboxes = {}
            self.waits = {}  # uid -> (deadline, next backoff) of a task waiting for room
            self.closed = False
            for _ in range(RELAY_SENDERS):
                threading.Thread(target=or_exit(self.run), name="relay_send", daemon=True).start()

        def busy_with(self, wid):
            """Whether this node's threads have operations of ``wid`` to make."""
            with self.cv:
                return wid in self.ops

        def add(self, ops):
            """Queue a batch's operations for this node, and wake one thread for them."""
            with self.cv:
                for op in ops:
                    wid = op[1]
                    queued = self.ops.setdefault(wid, deque())
                    queued.append(op)
                    if len(queued) == 1 and wid not in self.served:
                        self.waiting.append(wid)
                if self.waiting:
                    self.cv.notify()

        def close(self):
            with self.cv:
                self.closed = True
                self.cv.notify_all()

        def next_wid(self):
            """The next worker to serve, once there is one; None once closed.

            Call with cv held.
            """
            while True:
                now = time.monotonic()
                while self.later and self.later[0][0] <= now:
                    self.waiting.append(heapq.heappop(self.later)[2])
                if self.waiting:
                    return self.waiting.popleft()
                if self.closed:
                    return None
                self.cv.wait(self.later[0][0] - now if self.later else None)

        def run(self):
            while True:
                with self.cv:
                    wid = self.next_wid()
                    if wid is None:
                        return
                    self.served.add(wid)
                    op = self.ops[wid][0]
                    if self.waiting:  # another thread takes the rest, should this put stall
                        self.cv.notify()
                again = self.make(op)
                with self.cv:
                    self.served.discard(wid)
                    if again is not None:  # the same task first, once there may be room
                        heapq.heappush(self.later, (again, next(self.seq), wid))
                        continue
                    self.ops[wid].popleft()
                    if self.ops[wid]:
                        self.waiting.append(wid)
                    else:
                        del self.ops[wid]

        def make(self, op):
            if op[0] == "inbox":
                self.inboxes[op[1]] = op[2]
            elif op[0] == "forget":
                self.inboxes.pop(op[1], None)
            else:
                _, wid, uid, payload = op
                inbox = self.inboxes.get(wid)
                if inbox is None:
                    report(wid, uid, "broken", KeyError(f"no inbox for worker {wid}"), payload)
                else:
                    return put_one(inbox, wid, uid, payload, self.waits)
            return None

    def run_control():
        senders = {}  # node -> its NodeSender
        node_of = {}  # wid -> node
        local_inboxes = {}  # wid -> inbox, for the workers on the relay's own node
        try:
            while True:
                raw = control.get()
                if raw is None:
                    return
                sent = False
                by_node: dict[str | None, list[tuple]] = {}  # this batch's operations, by node
                handed = set()  # wids whose operations in this batch go to their node's threads
                for op in cloudpickle.loads(raw):
                    if op[0] == "ack":
                        if sent:
                            now = time.monotonic()
                            hold_until[0] = now + min(linger, (now - forwarded_at[0]) / 10)
                        ready.set()
                        continue
                    if op[0] == "inbox":
                        node = op[3] if len(op) > 3 else None
                        node_of[op[1]] = node
                        if node == local and local is not None:
                            local_inboxes[op[1]] = op[2]
                        if node not in senders:
                            senders[node] = NodeSender()
                    elif op[0] == "forget":
                        node = node_of.pop(op[1], None)
                        local_inboxes.pop(op[1], None)
                    else:
                        sent = True
                        wid = op[1]
                        node = node_of.get(wid)
                        if node not in senders:
                            _, wid, uid, payload = op
                            report(
                                wid, uid, "broken", KeyError(f"no inbox for worker {wid}"), payload
                            )
                            continue
                        inbox = local_inboxes.get(wid)
                        if (
                            inbox is not None
                            and wid not in handed
                            and not senders[node].busy_with(wid)
                        ):
                            try:  # most puts take no time: make them here, with no hand-off
                                inbox.put(op[3], timeout=RELAY_FAST_PUT)
                                continue
                            except Exception:  # noqa: BLE001, S110 - the node's threads try again
                                pass
                    if node in senders:
                        handed.add(op[1])
                        by_node.setdefault(node, []).append(op)
                for node, ops in by_node.items():
                    senders[node].add(ops)
        finally:
            for sender in senders.values():
                sender.close()

    def forward(raw):
        """Put ``raw`` on ``inbound``: it waits while the client has the last batch, and for as long
        as the pool is full."""
        delay = 0.01
        while True:
            try:
                inbound.put(raw)
                return
            except PoolFull:
                time.sleep(delay)
                delay = min(2 * delay, 1.0)

    thread = threading.Thread(target=or_exit(run_control), name="relay_control", daemon=True)
    thread.start()
    batch: list[bytes] = []  # pickled messages, forwarded as they are
    size = 0
    drained_at: float | None = None

    def take_notes():
        nonlocal size
        while True:
            try:
                note = notes.get_nowait()
            except queue.Empty:
                return
            batch.append(note)
            size += len(note)

    while True:
        take_notes()
        if not batch:
            mark = time.monotonic()
            try:
                item = results.get(timeout=idle_interval)
            except queue.Empty:
                take_notes()
                if batch or not (
                    ready.is_set() or time.monotonic() - forwarded_at[0] >= stale_after
                ):
                    continue
                try:  # a heartbeat that finds no room is skipped
                    inbound.put(cloudpickle.dumps(([], mark)), timeout=idle_interval)
                except Exception:  # noqa: BLE001, S112
                    continue
                ready.clear()
                forwarded_at[0] = time.monotonic()
                continue
            if item is None:
                break
            batch.append(item)
            size += len(item)
        stop = False
        while len(batch) < RELAY_BATCH and size < RELAY_BYTES:
            now = time.monotonic()
            try:
                item = results.get(block=False)
            except queue.Empty:
                drained_at = now
                break
            if item is None:
                stop = True
                break
            batch.append(item)
            size += len(item)
        if stop:
            break
        if len(batch) < RELAY_BATCH and size < RELAY_BYTES:
            if not ready.is_set() and time.monotonic() - forwarded_at[0] < stale_after:
                ready.wait(RELAY_POLL)
                continue
            left = hold_until[0] - time.monotonic()
            if ready.is_set() and left > 0:
                time.sleep(min(left, RELAY_POLL))
                continue
        ready.clear()
        forward(cloudpickle.dumps((batch, drained_at)))
        forwarded_at[0] = time.monotonic()
        batch, size, drained_at = [], 0, None
    take_notes()
    if batch:
        forward(cloudpickle.dumps((batch, drained_at)))
    forward(None)
    thread.join(timeout=5.0)


def _redirect_for_good(out_path, err_path):
    """Append this process's descriptors 1 and 2 to two files from now on."""
    for fd, path in ((1, out_path), (2, err_path)):
        new = _open_append(path)
        os.dup2(new, fd)
        os.close(new)


def run_function_process(results, uid, fn, args, kwargs, out_path, err_path):
    """Run a function task in its own Dragon process and send its return value to ``results``.

    Output is appended to ``out_path``/``err_path`` when they are set, and flushed before the result
    goes: the client reports the task when the result comes, and may read the output then. A raised
    exception is sent, then re-raised, so Dragon prints its traceback to stderr and the process
    exits with 1. The result is flushed, so it is on the queue before the process exits.
    """
    if out_path:
        _redirect_for_good(out_path, err_path)
    ok, value, tb = outcome(fn, args, kwargs)
    flush_all()
    send(results, (RESULT, uid, ok, value, tb), 3, flush=True)
    if not ok:
        raise value


def run_group_member(fn, args, kwargs, out_path, err_path):
    """Run one member of a multi-process function task; its exit code is its result."""
    if out_path:
        _redirect_for_good(out_path, err_path)
    call(fn, args, kwargs)


# Runs an executable with its output appended to two files. Local Services gets no pipe from the
# task, so a background child holding the task's output cannot stall it. The files' directories
# are made first, when missing: the work directory need not exist on the task's node.
EXEC_SCRIPT = (
    'o=$1; e=$2; shift 2; for f in "$o" "$e"; do [ -d "${f%/*}" ] || mkdir -p "${f%/*}"; done; '
    'exec "$@" >>"$o" 2>>"$e"'
)

# Sends signal $2 to every process whose initial environment holds the line $1, i.e. a task's
# leftover children: Dragon 0.15.0 signals only the task's own process.
REAP_SCRIPT = (
    'for f in $(grep -lzxF -e "$1" /proc/[0-9]*/environ 2>/dev/null); do '
    'p=${f#/proc/}; p=${p%/environ}; kill -s "$2" "$p" 2>/dev/null; done; exit 0'
)

# The same for every process whose initial environment holds a line that contains $1: at shutdown,
# what all of a backend's tasks left, with $1 the start of its tokens' lines.
REAP_ALL_SCRIPT = (
    'for f in $(grep -lzF -e "$1" /proc/[0-9]*/environ 2>/dev/null); do '
    'p=${f#/proc/}; p=${p%/environ}; kill -s "$2" "$p" 2>/dev/null; done; exit 0'
)
