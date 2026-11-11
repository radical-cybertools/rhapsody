"""Run a command and report its peak memory.

Usage::

    python -m rhapsody.backends.execution._peak --out-dir DIR [--interval S] -- CMD [ARG ...]

The command runs as a child of this process, which reaps it with ``os.wait4`` to read its peak
resident set size (``ru_maxrss``: the largest single process of the command's tree, not a sum).
The kernel carries the RSS of this process at fork (about 10 MiB) into the child's ``ru_maxrss``
across ``exec``, so that is the smallest peak reported. When ``pynvml`` is importable, a thread also samples, every ``--interval`` seconds, the memory that
NVML reports for the command's processes on each GPU. This process then writes
``DIR/<host>.<pid>.json``::

    {"host_mem": GiB, "gpu_mem": GiB per GPU or null, "gpu_mem_devices": {uuid: GiB},
     "started": wall s, "ended": wall s, "cpu_seconds": s, "host": name, ...}

``started`` and ``ended`` are this process's wall-clock time just before it forks the command
and just after it reaped it: the command's execution window, as seen on its node.

and exits the way the command did: the same exit code, or the same signal. Signals sent to this
process are forwarded to the command; on Linux the command is also killed if this process dies.

The module needs only the standard library (and optionally ``pynvml``), so ``DragonExecutionBackend``
runs it by path, without importing rhapsody, to keep the per-task start cost small.

Function tasks have no process of their own; :func:`measured_call` gives them an approximate figure.
"""

import argparse
import errno
import json
import os
import resource
import shutil
import signal
import socket
import sys
import threading
import time
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
PR_SET_PDEATHSIG = 1


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


def spawn(cmd: list[str]) -> int:
    """Fork and exec ``cmd``, keeping this process's stdio, environment and file descriptors."""
    before_exec = _die_with_parent_hook()
    pid = os.fork()
    if pid == 0:  # pragma: no cover - runs in the child, which execs or exits
        try:
            before_exec()
            os.execvp(cmd[0], cmd)  # noqa: S606 - runs the task command as given
        except OSError as exc:
            os.write(2, f"{cmd[0]}: {exc.strerror}\n".encode())
            os._exit(127 if exc.errno == errno.ENOENT else 126)
        os._exit(127)
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
    """Return an initialized ``pynvml`` module, or None if it is missing or NVML has no GPUs."""
    try:
        import pynvml
    except ImportError:
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
    for the tree's processes on that device. A device appears only once the tree was seen on it.
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
        self._stop_event = threading.Event()
        self._devices = []
        for i in range(nvml.nvmlDeviceGetCount()):
            handle = nvml.nvmlDeviceGetHandleByIndex(i)
            uuid = nvml.nvmlDeviceGetUUID(handle)
            self._devices.append((uuid.decode() if isinstance(uuid, bytes) else uuid, handle))

    def sample(self) -> None:
        pids = self.tree(self.pid)
        for uuid, handle in self._devices:
            try:
                procs = self.nvml.nvmlDeviceGetComputeRunningProcesses(handle)
            except Exception:  # noqa: S112 - NVMLError: device busy, lost, unsupported
                continue
            mine = [p for p in procs if p.pid in pids]
            if mine:
                # usedGpuMemory is None when NVML cannot tell (e.g. without permission).
                used = sum(p.usedGpuMemory or 0 for p in mine)
                self.peak[uuid] = max(self.peak.get(uuid, 0), used)
        self.samples += 1

    def run(self) -> None:
        while True:
            self.sample()
            if self._stop_event.wait(self.interval):
                return

    def stop(self) -> None:
        self._stop_event.set()
        if self.is_alive():
            self.join()


def report(
    rusage,
    gpu: Optional[GpuSampler],
    exit_code: int,
    started: Optional[float] = None,
    ended: Optional[float] = None,
) -> dict:
    """Build the peaks record from the child's rusage and, if any, the GPU sampler."""
    devices = {u: b / GIB for u, b in gpu.peak.items()} if gpu else {}
    return {
        "started": started,
        "ended": ended,
        "cpu_seconds": _cpu_seconds(rusage),
        "host_mem": rusage.ru_maxrss * MAXRSS_UNIT / GIB,
        # None when GPUs were not sampled or the tree never showed up on one: "not observed"
        # keeps asyncflow from learning a zero for a GPU task it could not see.
        "gpu_mem": max(devices.values()) if devices else None,
        "gpu_mem_devices": devices,
        "nvml": gpu is not None,
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
    """Write ``record`` to ``out_dir/<host>.<pid>.json`` atomically; warn instead of failing."""
    try:
        os.makedirs(out_dir, exist_ok=True)
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


def run(cmd: list[str], out_dir: str, interval: float = 0.2) -> int:
    """Run ``cmd``, write its peaks to ``out_dir``, and return its exit code (negative: signal)."""
    started = time.time()
    pid = spawn(cmd)

    def forward(sig, _frame):
        try:
            os.kill(pid, sig)
        except ProcessLookupError:
            pass

    for sig in FORWARDED_SIGNALS:
        signal.signal(sig, forward)

    nvml = load_nvml()
    gpu = None
    if nvml is not None:
        try:
            gpu = GpuSampler(nvml, pid, interval)
            gpu.start()
        except Exception:
            gpu = None

    _, status, rusage = os.wait4(pid, 0)  # retried on EINTR (PEP 475)
    ended = time.time()
    exit_code = os.waitstatus_to_exitcode(status)

    if gpu is not None:
        gpu.stop()
    if nvml is not None:
        try:
            nvml.nvmlShutdown()
        except Exception:  # noqa: S110
            pass
    write_report(out_dir, report(rusage, gpu, exit_code, started, ended))
    return exit_code


def read_peaks(peak_dir: str, ranks: int) -> Optional[dict]:
    """Combine the records in ``peak_dir`` into ``task["observed"]``, then remove the directory.

    A job's ranks add up, on the host (an upper bound: their peaks need not coincide) and on each
    GPU they share; ``gpu_mem`` is the busiest GPU's. None unless all ``ranks`` reported, since a
    partial sum would understate the demand. When every record has them, ``started`` is the
    earliest start, ``ended`` the latest end, ``cpu_seconds`` the sum, and ``host`` the node (or
    ``hosts``, sorted, for a job on several).
    """
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
                records.append(json.load(f))
        except (OSError, ValueError):
            continue
    shutil.rmtree(peak_dir, ignore_errors=True)
    if not records or len(records) < ranks:
        return None
    gpu: dict[str, float] = {}
    for rec in records:
        for device, gib in (rec.get("gpu_mem_devices") or {}).items():
            gpu[device] = gpu.get(device, 0.0) + gib
    observed = {
        "host_mem": sum(rec["host_mem"] for rec in records),
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


class MeasuredResult(NamedTuple):
    """A pooled function's return value with its approximate peak (see :func:`measured_call`)."""

    value: Any
    observed: Optional[dict]


def _status_bytes(field: str) -> Optional[int]:
    """A ``kB`` field of ``/proc/self/status`` (e.g. ``VmRSS``), in bytes; None off Linux."""
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith(field + ":"):
                    return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        pass
    return None


def _reset_peak_rss() -> Optional[int]:
    """Reset this process's ``VmHWM`` to its current RSS (Linux >= 4.0); return that RSS."""
    try:
        with open("/proc/self/clear_refs", "w") as f:
            f.write("5")
    except OSError:
        return None
    return _status_bytes("VmRSS")


def measured_call(fn: Callable, args: tuple, kwargs: dict, memory: bool = True) -> MeasuredResult:
    """Call ``fn`` and return its value with the host-memory growth of this process during the call.

    For function tasks, which Dragon's Batch runs one at a time in long-lived pool workers, where the
    OS keeps no per-call peak. The peak RSS mark is reset before the call and read after it, so
    ``host_mem`` is the worker's peak during the call minus its RSS before. It is approximate:
    memory the allocator kept from earlier calls is reused without counting, and other threads of
    the worker count. ``gpu_mem`` is not measured: CUDA contexts and caching allocators outlive a
    call. ``host_mem`` is None off Linux, and with ``memory=False``, for workers that run several
    calls at once (threads), where the process's peak is not the call's. ``observed`` always has
    the call's wall-clock ``started`` and ``ended`` and the ``host``. Exceptions from ``fn``
    propagate unchanged, without a measurement.
    """
    base = _reset_peak_rss() if memory else None
    started = time.time()
    value = fn(*args, **kwargs)
    ended = time.time()
    peak = _status_bytes("VmHWM") if base is not None else None
    observed = {
        "host_mem": None if peak is None else max(peak - base, 0) / GIB,
        "gpu_mem": None,
        "started": started,
        "ended": ended,
        "host": socket.gethostname(),
    }
    if peak is not None:
        observed["approximate"] = True
    return MeasuredResult(value, observed)


def main(argv: Optional[list[str]] = None) -> None:
    argv = sys.argv[1:] if argv is None else argv
    parser = argparse.ArgumentParser(
        prog="python -m rhapsody.backends.execution._peak",
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
    # Run by path, this file's directory leads sys.path: its modules (concurrent.py, dragon.py, ...)
    # would shadow what is imported lazily here (ctypes, pynvml and their imports).
    sys.path.pop(0)
    main()
