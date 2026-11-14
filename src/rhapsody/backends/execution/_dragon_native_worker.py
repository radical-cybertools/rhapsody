"""Code that runs inside the Dragon processes of ``DragonNativeExecutionBackend``.

The backend registers this module with ``cloudpickle.register_pickle_by_value``, so its functions
travel to Dragon processes by value. A worker therefore never imports ``rhapsody``, only the
standard library, cloudpickle and Dragon. Keep it that way: the one import from ``rhapsody`` here,
``_measure`` (``observe_peaks``), is standard-library only and is registered by value too.

Messages are cloudpickled to ``bytes`` before they go on a Dragon queue, so a value that cannot
be pickled is reported as an error instead of failing inside the queue. A task's value is pickled
on its own inside its message, so a value the client cannot load fails only that task.
"""

import asyncio
import contextlib
import ctypes
import inspect
import os
import signal
import socket
import sys
import tempfile
import threading
import time
import traceback

try:
    import cloudpickle
except ImportError:  # pragma: no cover - only Dragon processes run this code, and Dragon needs it
    cloudpickle = None

from . import _measure  # noqa: E402 - travels by value, as this module does

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

# Seconds a result waits for room in Dragon's memory pool before its task fails.
PUT_PATIENCE = 60.0


def raise_oom_score(adj):
    """Raise this process's ``oom_score_adj`` to ``adj`` (Linux); return the value it has.

    Under memory pressure the kernel then kills a pool worker before Dragon's services, whose death
    would end the whole runtime. Any process may raise its own value, and its children inherit it;
    lowering it needs privileges, so a higher value is kept. None where it cannot be read.
    """
    if adj is None:
        return None
    try:
        with open("/proc/self/oom_score_adj") as f:
            current = int(f.read())
        if current < adj:
            with open("/proc/self/oom_score_adj", "w") as f:
                f.write(str(adj))
            return adj
        return current
    except (OSError, ValueError):
        return None


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


def _safe_error(exc):
    """``SystemExit`` and its like come back as a ``RuntimeError``: raised in the client when it
    awaits the task, they would end the client too."""
    return exc if isinstance(exc, Exception) else RuntimeError(f"task raised {exc!r}")


def outcome(fn, args, kwargs, measure=False):
    """Return ``(ok, value, traceback)`` for one call of ``fn``.

    With ``measure`` (``observe_peaks``), the call is measured in this worker (``_measure``), and
    ``value`` is ``(value, observed)``, or ``(error, observed)`` when the call raised, also on
    ``SystemExit`` and its like.
    """
    if measure:
        try:
            return True, _measure.measure(call, (fn, args, kwargs), {}, True, BaseException), None
        except _measure.MeasuredError as merr:
            error, observed = merr.args
            tb = "".join(traceback.format_exception(type(error), error, error.__traceback__))
            return False, (_safe_error(error), observed), tb
        except Exception as exc:  # noqa: BLE001 - the error goes back to the client
            return False, (exc, None), traceback.format_exc()
    try:
        return True, call(fn, args, kwargs), None
    except BaseException as exc:  # noqa: BLE001 - the error goes back to the client
        return False, _safe_error(exc), traceback.format_exc()


def pack(msg, value_index, measured=False):
    """Pickle ``msg``, with ``msg[value_index]`` pickled on its own inside it.

    The client can then always load the message, and it fails only this task when it cannot load
    the value. A value that cannot be pickled here is replaced by an error, and ``ok``, the field
    before it, becomes False. A ``measured`` value is ``(value, observed)``: the error names the
    value's type, and the measurement is kept.
    """
    msg = list(msg)
    value = msg[value_index]
    try:
        msg[value_index] = cloudpickle.dumps(value)
    except Exception as exc:  # noqa: BLE001
        inner = measured and isinstance(value, tuple) and len(value) == 2
        name = type(value[0] if inner else value).__name__
        error = RuntimeError(f"cannot pickle the task's {name}: {exc!r}")
        msg[value_index] = cloudpickle.dumps((error, value[1]) if inner else error)
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


def send(results, msg, value_index, flush=False, measured=False):
    """Send a result message; one that Dragon cannot take fails its task, not the worker."""
    raw = pack(msg, value_index, measured)
    try:
        put_waiting(results, raw, flush)
    except Exception as exc:  # noqa: BLE001 - the error goes back to the client
        fields = list(msg)
        error = RuntimeError(f"cannot send {len(raw)} bytes of results: {exc}")
        value = fields[value_index]
        inner = measured and isinstance(value, tuple) and len(value) == 2
        fields[value_index] = (error, value[1]) if inner else error
        fields[value_index - 1] = False
        fields[value_index + 2 : value_index + 4] = ["", ""][: len(fields) - value_index - 2]
        put_waiting(results, pack(tuple(fields), value_index, measured), flush)


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


def run_task(token, fn, args, kwargs, out_path, err_path, inline, measure=False):
    """Run one pool task; return ``(ok, value, traceback, stdout, stderr)``.

    With ``out_path``/``err_path`` the output is appended to those files and the paths come back.
    Otherwise, with ``inline``, it is captured in anonymous files on this node and comes back as
    text. With neither, it goes wherever the worker's own output goes. What fails around the
    function, say an output path this node cannot write, fails only the task, not the worker.
    With ``measure``, the call is measured (see :func:`outcome`).
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
            ok, value, tb = outcome(fn, args, kwargs, measure)
            return ok, value, tb, "", ""
        with redirected(*fds):
            ok, value, tb = outcome(fn, args, kwargs, measure)
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


def worker_main(wid, inbox, results, oom_score_adj=None):
    """A pool worker: report ready, then run tasks from ``inbox`` until it receives ``None``.

    It first raises its ``oom_score_adj`` (see :func:`raise_oom_score`), and reports ready as
    ``(READY, wid, hostname, pid, oom_score_adj)``. Each inbox item is a pickled ``(uid, body)``,
    where ``body`` is a pickled ``(token, fn, args, kwargs, out_path, err_path, inline, measure)``:
    a body that cannot be unpickled here (say, its function's module is missing on this node) still
    fails only its own task. Each item bumps the worker's sequence number, which starts at 1, so
    ``(wid, seq)`` names a task where the uid is not known, as in a per-node memory sample. Each
    result is a pickled ``(DONE, wid, uid, ok, value, traceback, stdout, stderr, seq)``; with
    ``measure``, ``value`` is ``(value or error, observed)`` (see :func:`outcome`).
    """
    adj = raise_oom_score(oom_score_adj)
    results.put(cloudpickle.dumps((READY, wid, socket.gethostname(), os.getpid(), adj)))
    seq = 0
    while True:
        raw = inbox.get()
        if raw is None:
            return
        seq += 1
        uid, body = cloudpickle.loads(raw)
        try:
            token, fn, args, kwargs, out_path, err_path, inline, measure = cloudpickle.loads(body)
        except BaseException as exc:  # noqa: BLE001 - the error goes back to the client
            send(results, (DONE, wid, uid, False, exc, traceback.format_exc(), "", "", seq), 4)
            continue
        del body
        ok, value, tb, out, err = run_task(
            token, fn, args, kwargs, out_path, err_path, inline, measure
        )
        send(results, (DONE, wid, uid, ok, value, tb, out, err, seq), 4, measured=measure)


def _redirect_for_good(out_path, err_path):
    """Append this process's descriptors 1 and 2 to two files from now on."""
    for fd, path in ((1, out_path), (2, err_path)):
        new = _open_append(path)
        os.dup2(new, fd)
        os.close(new)


def _record_this_process(peak_dir):
    """Start this process's record (``observe_peaks``); return the function that writes it.

    A SIGTERM (a cancel or a timeout) writes the record before the process dies of it. The record is
    written once.
    """
    if not peak_dir:
        return lambda exit_code: None
    started, cpu_start = time.time(), _measure.cpu_now()
    written = []

    def write(exit_code):
        if not written:
            written.append(exit_code)
            _measure.write_record(peak_dir, _measure.process_record(started, cpu_start, exit_code))

    def on_term(sig, _frame):
        write(-sig)
        signal.signal(sig, signal.SIG_DFL)
        os.kill(os.getpid(), sig)

    if threading.current_thread() is threading.main_thread():
        with contextlib.suppress(ValueError, OSError):
            signal.signal(signal.SIGTERM, on_term)
    return write


def run_function_process(results, uid, fn, args, kwargs, out_path, err_path, peak_dir=None):
    """Run a function task in its own Dragon process and send its return value to ``results``.

    Output is appended to ``out_path``/``err_path`` when they are set. A raised exception is sent,
    then re-raised, so Dragon prints its traceback to stderr and the process exits with 1. The
    result is flushed, so it is on the queue before the process exits. With ``peak_dir``, the
    process's record (``_measure.process_record``) is written there before the result is sent.
    """
    if out_path:
        _redirect_for_good(out_path, err_path)
    write_record = _record_this_process(peak_dir)
    ok, value, tb = outcome(fn, args, kwargs)
    write_record(0 if ok else 1)
    send(results, (RESULT, uid, ok, value, tb), 3, flush=True)
    if not ok:
        raise value


def run_group_member(fn, args, kwargs, out_path, err_path, peak_dir=None):
    """Run one member of a multi-process function task; its exit code is its result.

    With ``peak_dir``, the member writes its record there, as a launched rank does.
    """
    if out_path:
        _redirect_for_good(out_path, err_path)
    write_record = _record_this_process(peak_dir)
    try:
        call(fn, args, kwargs)
    except BaseException:
        write_record(1)
        raise
    write_record(0)


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
