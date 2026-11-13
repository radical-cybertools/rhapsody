"""Measure one call where it runs: its window, host, CPU time and memory, for ``observe_peaks``.

Standard library only, and nothing from rhapsody: the Dragon backends send this module to their
workers by value (``cloudpickle.register_pickle_by_value``), so a worker measures a call without
importing rhapsody. The Concurrent backend imports it as usual.

A measured call returns ``(value, observed)``. One that raises raises :class:`MeasuredError`,
which carries the call's exception and its measurement together across a process pool or Dragon's
result queue; :func:`split_error` takes them apart. ``observed`` has the keys of the peak launcher's
records (:mod:`rhapsody.backends.execution._peak`): ``host_mem`` and ``gpu_mem`` (GiB, None when not
measured), ``started`` and ``ended`` (wall clock, where the call ran), ``cpu_seconds`` and ``host``,
plus ``approximate`` when the figures cover only part of what the call used.

:func:`process_record` and :func:`write_record` give a function that runs as a process of its own,
which the launcher cannot run, the record the launcher writes for a command.
"""

import json
import os
import resource
import socket
import sys
import time

GIB = float(1 << 30)
# ru_maxrss is in KiB on Linux and in bytes on macOS.
MAXRSS_UNIT = 1 if sys.platform == "darwin" else 1024


class MeasuredError(Exception):
    """A measured call's exception and its measurement, as ``args`` ``(error, observed)``.

    It is raised in place of the call's own exception, so the measurement never sits on the user's
    exception object (which several tasks may share) and travels wherever the exception is pickled.
    """

    # Read from the class, so it holds when the class itself travelled by value.
    _rhapsody_measured = True

    def __init__(self, error, observed):
        super().__init__(error, observed)

    def __str__(self):
        return str(self.args[0])


def split_error(exc):
    """``(error, observed)`` for a :class:`MeasuredError`, else ``(exc, None)``."""
    if getattr(type(exc), "_rhapsody_measured", False):
        args = exc.args
        if isinstance(args, tuple) and len(args) == 2:
            return args[0], args[1]
    return exc, None


def status_bytes(field):
    """A ``kB`` field of ``/proc/self/status`` (e.g. ``VmRSS``), in bytes; None off Linux."""
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith(field + ":"):
                    return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        pass
    return None


def reset_peak_rss():
    """Reset this process's ``VmHWM`` to its current RSS (Linux >= 4.0); return that RSS."""
    try:
        with open("/proc/self/clear_refs", "w") as f:
            f.write("5")
    except OSError:
        return None
    return status_bytes("VmRSS")


def _cpu_with_children():
    """CPU seconds of this process and of the children it has waited for."""
    me = resource.getrusage(resource.RUSAGE_SELF)
    children = resource.getrusage(resource.RUSAGE_CHILDREN)
    return me.ru_utime + me.ru_stime + children.ru_utime + children.ru_stime


def measure(fn, args, kwargs, memory=True, catch=Exception):
    """Call ``fn(*args, **kwargs)`` and return ``(value, observed)``.

    With ``memory`` (a worker that runs one call at a time), ``cpu_seconds`` is the CPU time of the
    process and of the children it waited for, during the call, and on Linux ``host_mem`` is the
    process's peak RSS during the call minus its RSS before, marked ``approximate``: memory the
    allocator kept from earlier calls is reused without counting, and leftover threads of earlier
    calls count. Without ``memory`` (a thread of a pool that runs several calls at once),
    ``cpu_seconds`` is the calling thread's only, without the threads and processes the call
    starts, ``host_mem`` is None, and the measurement is marked ``approximate``. An exception of
    type ``catch`` is raised as :class:`MeasuredError`.
    """
    base = reset_peak_rss() if memory else None
    cpu_clock = _cpu_with_children if memory else time.thread_time
    started = time.time()
    cpu_start = cpu_clock()
    try:
        value = fn(*args, **kwargs)
    except catch as exc:
        observed = _call_observed(base, started, cpu_clock() - cpu_start, memory)
        raise MeasuredError(exc, observed) from exc
    return value, _call_observed(base, started, cpu_clock() - cpu_start, memory)


def _call_observed(base, started, cpu_seconds, memory):
    ended = time.time()
    peak = status_bytes("VmHWM") if base is not None else None
    observed = {
        "host_mem": None if peak is None else max(peak - base, 0) / GIB,
        "gpu_mem": None,
        "started": started,
        "ended": ended,
        "cpu_seconds": cpu_seconds,
        "host": socket.gethostname(),
    }
    if peak is not None or not memory:
        observed["approximate"] = True
    return observed


def cpu_now():
    """What :func:`process_record` subtracts from the CPU time it reports."""
    return _cpu_with_children()


def process_record(started, cpu_start, exit_code):
    """This process's record, in the format the peak launcher writes for a command.

    For a function task that runs as a Dragon process of its own: ``host_mem`` is the larger
    lifetime peak RSS of this process and of the children it waited for (the largest single
    process, as the launcher reports); ``cpu_seconds`` is their CPU time since ``cpu_start``
    (:func:`cpu_now`). No GPU memory.
    """
    me = resource.getrusage(resource.RUSAGE_SELF)
    children = resource.getrusage(resource.RUSAGE_CHILDREN)
    return {
        "started": started,
        "ended": time.time(),
        "cpu_seconds": max(_cpu_with_children() - cpu_start, 0.0),
        "host_mem": max(me.ru_maxrss, children.ru_maxrss) * MAXRSS_UNIT / GIB,
        "gpu_mem": None,
        "gpu_mem_devices": {},
        "nvml": False,
        "samples": 0,
        "exit_code": exit_code,
        "host": socket.gethostname(),
    }


def write_record(peak_dir, record):
    """Write ``record`` into ``peak_dir`` atomically, as the launcher does.

    The backend made ``peak_dir`` before the task started. It is not made here: once the backend
    has read and removed it, a late writer must not bring it back.
    """
    name = f"{record['host']}.{os.getpid()}.{os.urandom(4).hex()}.json"
    tmp = os.path.join(peak_dir, f".{name}.tmp")
    try:
        with open(tmp, "w") as f:
            json.dump(record, f)
        os.replace(tmp, os.path.join(peak_dir, name))
    except OSError as exc:
        print(f"rhapsody: cannot write peaks to {peak_dir}: {exc}", file=sys.stderr)
