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
import inspect
import json
import os
import resource
import socket
import sys
import tempfile
import time
import traceback

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

# Seconds a result waits for room in Dragon's memory pool before its task fails.
PUT_PATIENCE = 60.0

# observe_peaks. What a task used, as ``task["observed"]``, measured where it runs, with the keys
# and rules of ``rhapsody.backends.execution._peak``, which a worker cannot import: the attribute
# under which a raised exception carries its measurement, and the launcher's record file.
OBSERVED_ATTR = "rhapsody_observed"
GIB = float(1 << 30)
MAXRSS_UNIT = 1 if sys.platform == "darwin" else 1024


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


def _status_bytes(field):
    """A ``kB`` field of ``/proc/self/status`` (e.g. ``VmRSS``), in bytes; None off Linux."""
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith(field + ":"):
                    return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        pass
    return None


def _reset_peak_rss():
    """Reset this process's ``VmHWM`` to its current RSS (Linux >= 4.0); return that RSS."""
    try:
        with open("/proc/self/clear_refs", "w") as f:
            f.write("5")
    except OSError:
        return None
    return _status_bytes("VmRSS")


def _call_observed(base, started, cpu_seconds):
    ended = time.time()
    peak = _status_bytes("VmHWM") if base is not None else None
    observed = {
        "host_mem": None if peak is None else max(peak - base, 0) / GIB,
        "gpu_mem": None,
        "started": started,
        "ended": ended,
        "cpu_seconds": cpu_seconds,
        "host": socket.gethostname(),
    }
    if peak is not None:
        observed["approximate"] = True
    return observed


def measured(fn, args, kwargs):
    """Call ``fn`` as :func:`call` does in a pool worker; return ``(value, observed)``.

    The measurement of ``_peak.measured_call``: the call's wall-clock ``started`` and ``ended``,
    the ``host``, the worker's CPU time during the call as ``cpu_seconds`` (a worker runs one call
    at a time), and on Linux ``host_mem``, the worker's peak RSS during the call minus its RSS
    before, marked ``approximate``. An exception propagates with the measurement as its
    ``rhapsody_observed`` attribute.
    """
    base = _reset_peak_rss()
    started = time.time()
    cpu_start = time.process_time()
    try:
        value = call(fn, args, kwargs)
    except Exception as exc:
        # An exception that takes no attributes goes without
        with contextlib.suppress(Exception):
            setattr(
                exc, OBSERVED_ATTR, _call_observed(base, started, time.process_time() - cpu_start)
            )
        raise
    return value, _call_observed(base, started, time.process_time() - cpu_start)


def process_record(started, exit_code):
    """This process's record as the peak launcher writes one for a command it ran.

    For a function task that runs as its own Dragon process, which the launcher cannot run:
    ``host_mem`` is the larger peak RSS of this process and of its waited-for children (the
    largest single process), ``cpu_seconds`` their CPU time; no GPU memory.
    """
    me = resource.getrusage(resource.RUSAGE_SELF)
    children = resource.getrusage(resource.RUSAGE_CHILDREN)
    return {
        "started": started,
        "ended": time.time(),
        "cpu_seconds": me.ru_utime + me.ru_stime + children.ru_utime + children.ru_stime,
        "host_mem": max(me.ru_maxrss, children.ru_maxrss) * MAXRSS_UNIT / GIB,
        "gpu_mem": None,
        "gpu_mem_devices": {},
        "nvml": False,
        "samples": 0,
        "exit_code": exit_code,
        "host": socket.gethostname(),
    }


def write_record(peak_dir, record):
    """Write ``record`` to ``peak_dir/<host>.<pid>.<random>.json`` atomically, as the launcher
    writes its own, which ``_peak.read_peaks`` combines."""
    try:
        os.makedirs(peak_dir, exist_ok=True)
        name = f"{record['host']}.{os.getpid()}.{os.urandom(4).hex()}.json"
        tmp = os.path.join(peak_dir, f".{name}.tmp")
        with open(tmp, "w") as f:
            json.dump(record, f)
        os.replace(tmp, os.path.join(peak_dir, name))
    except OSError as exc:
        print(f"rhapsody: cannot write peaks to {peak_dir}: {exc}", file=sys.stderr)


@contextlib.contextmanager
def recorded(peak_dir):
    """Write this process's record to ``peak_dir`` when the block ends, if ``peak_dir`` is set."""
    started = time.time()
    exit_code = 1
    try:
        yield
        exit_code = 0
    finally:
        if peak_dir:
            write_record(peak_dir, process_record(started, exit_code))


def outcome(fn, args, kwargs, measure=False):
    """Return ``(ok, value, traceback)`` for one call of ``fn``.

    With ``measure``, ``value`` is ``(value, observed)`` (see :func:`measured`). ``SystemExit`` and
    its like come back as a ``RuntimeError``: raised in the client when it awaits the task, they
    would end the client too.
    """
    try:
        if measure:
            return True, measured(fn, args, kwargs), None
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


def worker_main(wid, inbox, results):
    """A pool worker: report ready, then run tasks from ``inbox`` until it receives ``None``.

    Each inbox item is a pickled ``(uid, body)``, where ``body`` is a pickled
    ``(token, fn, args, kwargs, out_path, err_path, inline, measure)``: a body that cannot be
    unpickled here (say, its function's module is missing on this node) still fails only its own
    task. Each result is a pickled ``(DONE, wid, uid, ok, value, traceback, stdout, stderr)``; with
    ``measure``, a successful task's ``value`` is ``(value, observed)``.
    """
    results.put(cloudpickle.dumps((READY, wid, socket.gethostname(), os.getpid())))
    while True:
        raw = inbox.get()
        if raw is None:
            return
        uid, body = cloudpickle.loads(raw)
        try:
            token, fn, args, kwargs, out_path, err_path, inline, measure = cloudpickle.loads(body)
        except BaseException as exc:  # noqa: BLE001 - the error goes back to the client
            send(results, (DONE, wid, uid, False, exc, traceback.format_exc(), "", ""), 4)
            continue
        del body
        ok, value, tb, out, err = run_task(
            token, fn, args, kwargs, out_path, err_path, inline, measure
        )
        send(results, (DONE, wid, uid, ok, value, tb, out, err), 4)


def _redirect_for_good(out_path, err_path):
    """Append this process's descriptors 1 and 2 to two files from now on."""
    for fd, path in ((1, out_path), (2, err_path)):
        new = _open_append(path)
        os.dup2(new, fd)
        os.close(new)


def run_function_process(results, uid, fn, args, kwargs, out_path, err_path, peak_dir=None):
    """Run a function task in its own Dragon process and send its return value to ``results``.

    Output is appended to ``out_path``/``err_path`` when they are set. A raised exception is sent,
    then re-raised, so Dragon prints its traceback to stderr and the process exits with 1. The
    result is flushed, so it is on the queue before the process exits. With ``peak_dir``, the
    process's record (see :func:`process_record`) is written there before the result is sent.
    """
    if out_path:
        _redirect_for_good(out_path, err_path)
    started = time.time()
    ok, value, tb = outcome(fn, args, kwargs)
    if peak_dir:
        write_record(peak_dir, process_record(started, 0 if ok else 1))
    send(results, (RESULT, uid, ok, value, tb), 3, flush=True)
    if not ok:
        raise value


def run_group_member(fn, args, kwargs, out_path, err_path, peak_dir=None):
    """Run one member of a multi-process function task; its exit code is its result.

    With ``peak_dir``, the member writes its record there, as a launched rank does.
    """
    if out_path:
        _redirect_for_good(out_path, err_path)
    with recorded(peak_dir):
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
