"""Run a command and report its peak memory.

Usage::

    python -I -m rhapsody.backends.execution._peak --out-dir DIR [--interval S] -- CMD [ARG ...]

The command runs as a child of this process, which reaps it with ``os.wait4`` to read its peak
resident set size (``ru_maxrss``: the largest single process of the command's tree, not a sum).
``ru_maxrss`` is a high-water mark kept across ``exec``, so it includes the copy of this process
the child was before it ran the command: a command whose own peak stays below this process's size
at fork (about 10 MiB) reports that size. The record keeps it as ``floor``, so that
:func:`read_peaks` can tell such ranks apart. When ``pynvml`` is importable, a thread also
samples, every ``--interval`` seconds, the memory that NVML reports for the command's processes on
each GPU. This process then writes ``DIR/<host>.<pid>.json``::

    {"host_mem": GiB, "floor": GiB, "gpu_mem": GiB per GPU or null,
     "gpu_mem_devices": {uuid: GiB}, "started": wall s, "ended": wall s, "cpu_seconds": s,
     "host": name, ...}

``started`` and ``ended`` are this process's wall-clock time just before it forks the command
and just after the command exited: the command's execution window, as seen on its node.

It then exits the way the command did: the same exit code, or the same signal.

- **Signals.** The command starts with the default disposition of every signal (Python ignores
  ``SIGPIPE`` and ``SIGXFSZ``) and, unless this process runs in a terminal's foreground, in a
  process group of its own. Signals sent to this process, or to its group, reach the command's
  group once, forwarded. On Linux the command is also killed if this process dies.
- **Isolation.** Backends run it with ``python -I``, so the task's ``PYTHONHOME``,
  ``PYTHONPATH`` or ``PYTHONWARNINGS`` cannot break it; the command still gets the task's
  environment unchanged.
- **GPUs.** NVML is set up in the sampler thread, so a slow or broken NVML neither delays the
  command nor kills this process; ``usedGpuMemory`` that NVML cannot tell counts as unknown, not
  zero.

The module needs only the standard library (and optionally ``pynvml``), so the backends run it by
path, without importing rhapsody, to keep the per-task start cost small.

Function tasks have no process of their own; :func:`measured_call` gives them an approximate figure
(see :mod:`rhapsody.backends.execution._measure`).
"""

import argparse
import errno
import json
import os
import re
import resource
import shutil
import signal
import socket
import sys
import threading
import time
import uuid
from typing import Any
from typing import Callable
from typing import NamedTuple
from typing import Optional

GIB = float(1 << 30)
# ru_maxrss is in KiB on Linux and in bytes on macOS.
MAXRSS_UNIT = 1 if sys.platform == "darwin" else 1024
FORWARDED_SIGNALS = (
    signal.SIGTERM,
    signal.SIGINT,
    signal.SIGHUP,
    signal.SIGQUIT,
    signal.SIGUSR1,
    signal.SIGUSR2,
)
# Python starts with these ignored; a command must not inherit that.
RESET_SIGNALS = tuple(
    getattr(signal, name) for name in ("SIGPIPE", "SIGXFSZ") if hasattr(signal, name)
)
PR_SET_PDEATHSIG = 1
# Seconds stop() waits for the GPU sampler thread, which may hang in NVML on a bad node.
GPU_STOP_TIMEOUT = 5.0
# A rank whose peak is within this of its floor (GiB) never grew past the launcher's own size:
# the child touches a few pages of its copy of the launcher before it runs the command.
FLOOR_SLACK = 1.0 / 1024


def launcher_argv(out_dir: str) -> list[str]:
    """The command that runs a task's command under this launcher: append the command to it."""
    return [sys.executable, "-I", os.path.abspath(__file__), "--out-dir", out_dir, "--"]


def make_peak_dir(root: str, uid: str) -> str:
    """Make and return a fresh, absolute record directory for one run of task ``uid``.

    The name is the uid made safe for a path, plus a random suffix, so a uid such as ``..`` or
    ``/x`` cannot point outside ``root``, and a uid reused after a cancel gets a directory of its
    own. It is made now, before the task starts: writers never make it, so a late writer cannot
    bring it back once it has been read and removed.
    """
    root = os.path.abspath(root)
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", str(uid)).strip(".") or "task"
    path = os.path.join(root, f"{safe}.{uuid.uuid4().hex[:8]}")
    os.makedirs(path)
    return path


def rss_now() -> int:
    """This process's resident set size in bytes: ``VmRSS``, or off Linux its peak so far."""
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        pass
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * MAXRSS_UNIT


def _die_with_parent_hook() -> Callable[[], None]:
    """Return a function the child calls before exec: on Linux, ask for SIGKILL when we die.

    Dragon delivers a kill to this process only, so without it a SIGKILLed launcher would leave the
    command running. Loaded before fork so the child does no library lookup.
    """
    if not sys.platform.startswith("linux"):
        return lambda: None
    try:
        import ctypes

        prctl = ctypes.CDLL(None, use_errno=True).prctl
    except (OSError, AttributeError):
        return lambda: None
    parent = os.getpid()

    def hook() -> None:
        prctl(PR_SET_PDEATHSIG, signal.SIGKILL, 0, 0, 0)
        if os.getppid() != parent:  # the parent died before prctl took effect
            os._exit(128 + signal.SIGKILL)

    return hook


def _in_terminal_foreground() -> bool:
    """Whether this process runs in its terminal's foreground group (typed at a shell)."""
    try:
        return os.isatty(0) and os.tcgetpgrp(0) == os.getpgrp()
    except OSError:
        return False


def spawn(cmd: list[str], own_group: bool = False, mask: Optional[set] = None) -> int:
    """Fork and exec ``cmd``, keeping this process's stdio, environment and file descriptors.

    The child resets the signals this process changed (``SIGPIPE``, ``SIGXFSZ``, the forwarded
    ones) to their defaults, with ``own_group`` starts a process group of its own, and restores the
    signal ``mask`` before it execs.
    """
    before_exec = _die_with_parent_hook()
    pid = os.fork()
    if pid == 0:  # pragma: no cover - runs in the child, which execs or exits
        try:
            before_exec()
            if own_group:
                os.setpgid(0, 0)
            for sig in RESET_SIGNALS + FORWARDED_SIGNALS:
                signal.signal(sig, signal.SIG_DFL)
            if mask is not None:
                signal.pthread_sigmask(signal.SIG_SETMASK, mask)
            os.execvp(cmd[0], cmd)  # noqa: S606 - runs the task command as given
        except OSError as exc:
            os.write(2, f"{cmd[0]}: {exc.strerror}\n".encode())
            os._exit(127 if exc.errno == errno.ENOENT else 126)
        os._exit(127)
    if own_group:  # also here, so a signal forwarded at once finds the group
        try:
            os.setpgid(pid, pid)
        except OSError:  # the child already exec'd (EACCES) or exited (ESRCH)
            pass
    return pid


def descendants(pid: int) -> set[int]:
    """``pid`` and its live descendants, from ``/proc/<pid>/task/<tid>/children`` on Linux.

    Elsewhere, or if the kernel lacks the ``children`` file, only ``pid`` itself.
    """
    found = {pid}
    todo = [pid]
    while todo:
        p = todo.pop()
        try:
            tids = os.listdir(f"/proc/{p}/task")
        except OSError:
            continue
        for tid in tids:
            try:
                with open(f"/proc/{p}/task/{tid}/children") as f:
                    kids = [int(c) for c in f.read().split()]
            except (OSError, ValueError):
                continue
            for k in kids:
                if k not in found:
                    found.add(k)
                    todo.append(k)
    return found


def load_nvml():
    """Return an initialized ``pynvml`` module, or None if it is missing, broken or sees no GPU."""
    try:
        import pynvml
    except Exception:  # ImportError, or a broken or deprecated install
        return None
    try:
        pynvml.nvmlInit()
        if pynvml.nvmlDeviceGetCount() == 0:
            pynvml.nvmlShutdown()
            return None
    except Exception:  # NVMLError: no driver, no permission, ...
        return None
    return pynvml


class GpuSampler(threading.Thread):
    """Samples the GPU memory NVML reports for a process tree; keeps the peak per device.

    ``peak`` maps device UUID to the largest sum of ``usedGpuMemory`` (bytes) seen at one sample
    for the tree's processes on that device. A device appears only once the tree was seen on it
    with a usage NVML could tell. With ``nvml`` None, the thread loads NVML itself (see
    :func:`load_nvml`) and shuts it down when it stops; ``active`` says whether it had NVML. An
    error in NVML ends the sampling and keeps what was seen.
    """

    def __init__(
        self,
        nvml,
        pid: int,
        interval: float,
        tree: Callable[[int], set[int]] = descendants,
    ):
        super().__init__(name="peak-gpu-sampler", daemon=True)
        self.nvml = nvml
        self.pid = pid
        self.interval = interval
        self.tree = tree
        self.peak: dict[str, int] = {}
        self.samples = 0
        self.active = nvml is not None
        self._owns_nvml = nvml is None
        self._stop_event = threading.Event()
        self._devices: Optional[list] = None

    def _open(self) -> None:
        if self.nvml is None:
            self.nvml = load_nvml()
            self.active = self.nvml is not None
        if self.nvml is None or self._devices is not None:
            return
        devices = []
        for i in range(self.nvml.nvmlDeviceGetCount()):
            handle = self.nvml.nvmlDeviceGetHandleByIndex(i)
            uuid_ = self.nvml.nvmlDeviceGetUUID(handle)
            devices.append((uuid_.decode() if isinstance(uuid_, bytes) else uuid_, handle))
        self._devices = devices

    def sample(self) -> None:
        self._open()
        if self.nvml is None:
            return
        pids = self.tree(self.pid)
        for uuid_, handle in self._devices or ():
            try:
                procs = self.nvml.nvmlDeviceGetComputeRunningProcesses(handle)
            except Exception:  # noqa: S112 - NVMLError: device busy, lost, unsupported
                continue
            # usedGpuMemory is None when NVML cannot tell (no permission, MIG): unknown, not 0
            known = [
                p.usedGpuMemory for p in procs if p.pid in pids and p.usedGpuMemory is not None
            ]
            if known:
                self.peak[uuid_] = max(self.peak.get(uuid_, 0), sum(known))
        self.samples += 1

    def run(self) -> None:
        try:
            while True:
                self.sample()
                if self.nvml is None or self._stop_event.wait(self.interval):
                    return
        except Exception:  # noqa: S110 - NVML failed: keep what was seen
            return
        finally:
            if self._owns_nvml and self.nvml is not None:
                try:
                    self.nvml.nvmlShutdown()
                except Exception:  # noqa: S110
                    pass

    def stop(self, timeout: Optional[float] = GPU_STOP_TIMEOUT) -> None:
        self._stop_event.set()
        if self.is_alive():
            self.join(timeout)


def report(
    rusage,
    gpu: Optional[GpuSampler],
    exit_code: int,
    started: Optional[float] = None,
    ended: Optional[float] = None,
    floor: Optional[int] = None,
) -> dict:
    """Build the peaks record from the child's rusage and, if any, the GPU sampler.

    ``floor`` is this process's RSS in bytes when it forked the command.
    """
    devices = {u: b / GIB for u, b in gpu.peak.items()} if gpu else {}
    return {
        "started": started,
        "ended": ended,
        "cpu_seconds": _cpu_seconds(rusage),
        "host_mem": rusage.ru_maxrss * MAXRSS_UNIT / GIB,
        "floor": None if floor is None else floor / GIB,
        # None when GPUs were not sampled or the tree never showed up on one: "not observed"
        # keeps asyncflow from learning a zero for a GPU task it could not see.
        "gpu_mem": max(devices.values()) if devices else None,
        "gpu_mem_devices": devices,
        "nvml": bool(gpu is not None and gpu.active),
        "samples": gpu.samples if gpu else 0,
        "exit_code": exit_code,
        "host": socket.gethostname(),
    }


def _cpu_seconds(rusage) -> Optional[float]:
    try:
        return rusage.ru_utime + rusage.ru_stime
    except AttributeError:
        return None


def write_report(out_dir: str, record: dict) -> None:
    """Write ``record`` to ``out_dir/<host>.<pid>.json`` atomically; warn instead of failing.

    ``out_dir`` was made by the backend (:func:`make_peak_dir`); it is not made here, so a record
    written after the backend gave up on it does not bring the directory back.
    """
    try:
        name = f"{record['host']}.{os.getpid()}.json"
        tmp = os.path.join(out_dir, f".{name}.tmp")
        with open(tmp, "w") as f:
            json.dump(record, f)
        os.replace(tmp, os.path.join(out_dir, name))
    except OSError as exc:
        print(f"rhapsody peak launcher: cannot write peaks to {out_dir}: {exc}", file=sys.stderr)


def exit_like(exit_code: int) -> None:
    """Exit with ``exit_code``; a negative code re-raises that signal on this process."""
    sys.stdout.flush()
    sys.stderr.flush()
    if exit_code >= 0:
        os._exit(exit_code)
    sig = -exit_code
    try:
        # Do not dump a second core for the launcher when the command dumped one.
        resource.setrlimit(resource.RLIMIT_CORE, (0, resource.getrlimit(resource.RLIMIT_CORE)[1]))
    except (ValueError, OSError):
        pass
    signal.signal(sig, signal.SIG_DFL)
    signal.pthread_sigmask(signal.SIG_UNBLOCK, [sig])
    os.kill(os.getpid(), sig)
    os._exit(128 + sig)  # the signal does not terminate by default (e.g. SIGCHLD)


def _wait_exited(pid: int) -> bool:
    """Wait until ``pid`` has exited, without reaping it; False where the OS cannot.

    As a zombie its pid cannot be reused, so a signal forwarded meanwhile cannot reach another
    process.
    """
    waitid = getattr(os, "waitid", None)
    nowait = getattr(os, "WNOWAIT", 0)
    if waitid is None or not nowait:
        return False
    try:
        waitid(os.P_PID, pid, os.WEXITED | nowait)  # retried on EINTR (PEP 475)
    except OSError:
        return False
    return True


def run(cmd: list[str], out_dir: str, interval: float = 0.2) -> int:
    """Run ``cmd``, write its peaks to ``out_dir``, and return its exit code (negative: signal)."""
    child: dict = {"pid": None, "group": False}

    def forward(sig, _frame):
        pid = child["pid"]
        if pid is None:  # exited: forward nothing more
            return
        try:
            if child["group"]:
                os.killpg(pid, sig)
            else:
                os.kill(pid, sig)
        except (ProcessLookupError, PermissionError):
            pass

    # Signals that come before the child's pid is known are held, then forwarded.
    mask = signal.pthread_sigmask(signal.SIG_BLOCK, FORWARDED_SIGNALS)
    for sig in FORWARDED_SIGNALS:
        signal.signal(sig, forward)
    own_group = not _in_terminal_foreground()
    floor = rss_now()
    started = time.time()
    try:
        pid = spawn(cmd, own_group=own_group, mask=mask)
        child.update(pid=pid, group=own_group)
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, mask)

    gpu: Optional[GpuSampler] = GpuSampler(None, pid, interval)
    try:
        gpu.start()  # loads NVML in its own thread
    except RuntimeError:  # no thread to be had
        gpu = None

    if _wait_exited(pid):
        child["pid"] = None
    _, status, rusage = os.wait4(pid, 0)  # retried on EINTR (PEP 475)
    child["pid"] = None
    ended = time.time()
    exit_code = os.waitstatus_to_exitcode(status)

    if gpu is not None:
        gpu.stop()
    write_report(out_dir, report(rusage, gpu, exit_code, started, ended, floor))
    return exit_code


def read_records(peak_dir: str) -> Optional[list[dict]]:
    """The complete records in ``peak_dir``, then remove the directory; None if unreadable."""
    if os.path.basename(peak_dir.rstrip(os.sep)) in ("", ".", ".."):
        return None  # never remove a work directory itself
    records = []
    try:
        names = os.listdir(peak_dir)
    except OSError:
        return None
    for name in names:
        if name.startswith(".") or not name.endswith(".json"):
            continue
        try:
            with open(os.path.join(peak_dir, name)) as f:
                rec = json.load(f)
        except (OSError, ValueError):
            continue
        if isinstance(rec, dict) and isinstance(rec.get("host_mem"), (int, float)):
            records.append(rec)
    shutil.rmtree(peak_dir, ignore_errors=True)
    return records


def _at_floor(rec: dict) -> bool:
    floor = rec.get("floor")
    return isinstance(floor, (int, float)) and rec["host_mem"] <= floor + FLOOR_SLACK


def combine(records: list[dict], ranks: int) -> Optional[dict]:
    """Combine a run's records into ``task["observed"]``; None unless there are exactly ``ranks``.

    A partial sum would understate the demand, and an extra record is not this run's.

    - ``host_mem`` adds the ranks up (an upper bound: their peaks need not coincide). A rank whose
      peak stayed at its ``floor`` (the launcher's size when it forked the command, which
      ``ru_maxrss`` keeps) used at most that much, so all such ranks count one floor between them
      instead of one each. Every other rank's peak is its command's own.
    - ``host_mem_max`` is the largest rank's peak, and ``ranks`` the number of records.
    - ``gpu_mem`` is the busiest GPU's, the ranks sharing a GPU adding up.
    - When every record has them, ``started`` is the earliest start, ``ended`` the latest end,
      ``cpu_seconds`` the sum, and ``host`` the node (or ``hosts``, sorted, for a job on several).
    """
    if len(records) != ranks or not records:
        return None
    gpu: dict[str, float] = {}
    for rec in records:
        devices = rec.get("gpu_mem_devices")
        for device, gib in (devices if isinstance(devices, dict) else {}).items():
            gpu[device] = gpu.get(device, 0.0) + gib
    floors = [rec["floor"] for rec in records if _at_floor(rec)]
    above = sum(rec["host_mem"] for rec in records if not _at_floor(rec))
    observed = {
        "host_mem": above + (max(floors) if floors else 0.0),
        "host_mem_max": max(rec["host_mem"] for rec in records),
        "ranks": len(records),
        "gpu_mem": max(gpu.values()) if gpu else None,
    }
    if all(rec.get("started") is not None for rec in records):
        observed["started"] = min(rec["started"] for rec in records)
    if all(rec.get("ended") is not None for rec in records):
        observed["ended"] = max(rec["ended"] for rec in records)
    if all(rec.get("cpu_seconds") is not None for rec in records):
        observed["cpu_seconds"] = sum(rec["cpu_seconds"] for rec in records)
    hosts = sorted({rec["host"] for rec in records if rec.get("host")})
    if len(hosts) == 1:
        observed["host"] = hosts[0]
    elif hosts:
        observed["hosts"] = hosts
    return observed


def read_peaks(peak_dir: str, ranks: int) -> Optional[dict]:
    """Combine the records in ``peak_dir`` into ``task["observed"]`` (see :func:`combine`), then
    remove the directory."""
    records = read_records(peak_dir)
    return None if records is None else combine(records, ranks)


class MeasuredResult(NamedTuple):
    """A pooled function's return value with its approximate peak (see :func:`measured_call`)."""

    value: Any
    observed: Optional[dict]


def measured_call(fn: Callable, args: tuple, kwargs: dict, memory: bool = True) -> MeasuredResult:
    """Call ``fn`` and return its value with its measurement (``_measure.measure``).

    For function tasks in long-lived pool workers, where the OS keeps no per-call peak. A call that
    raises raises ``_measure.MeasuredError``, which carries the call's exception and its
    measurement; ``_measure.split_error`` takes them apart.
    """
    from . import _measure  # here, so the launcher run by path needs only the standard library

    value, observed = _measure.measure(fn, args, kwargs, memory)
    return MeasuredResult(value, observed)


def main(argv: Optional[list[str]] = None) -> None:
    argv = sys.argv[1:] if argv is None else argv
    parser = argparse.ArgumentParser(
        prog="python -I -m rhapsody.backends.execution._peak",
        description="Run CMD and write its peak host and GPU memory to OUT_DIR.",
        usage="%(prog)s --out-dir OUT_DIR [--interval S] -- CMD [ARG ...]",
    )
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--interval", type=float, default=0.2, help="GPU sampling period")
    if "--" not in argv:
        parser.error("missing '--' before the command")
    split = argv.index("--")
    opts, cmd = parser.parse_args(argv[:split]), argv[split + 1 :]
    if not cmd:
        parser.error("no command after '--'")
    exit_like(run(cmd, opts.out_dir, opts.interval))


if __name__ == "__main__":
    # Run by path without -I, this file's directory leads sys.path: its modules (concurrent.py,
    # dragon.py, ...) would shadow what is imported lazily here (ctypes, pynvml and their imports).
    _here = os.path.dirname(os.path.abspath(__file__))
    if sys.path and os.path.abspath(sys.path[0] or os.curdir) == _here:
        sys.path.pop(0)
    main()
