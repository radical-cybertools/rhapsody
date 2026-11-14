"""The peak launcher (``rhapsody.backends.execution._peak``), tested without Dragon or a GPU.

The launcher runs a command as its child, reaps it with ``wait4`` for its peak RSS, samples its
GPU memory through NVML when ``pynvml`` is present, and exits as the command did. These tests run
it as the backend does (``python <path to _peak.py> --out-dir DIR -- CMD ...``) and use a fake
NVML for the GPU sampler.
"""

# FakeNvml mirrors pynvml's camelCase function names.
# ruff: noqa: N802

import json
import os
import pickle
import signal
import socket
import subprocess
import sys
import time
from collections import namedtuple
from types import SimpleNamespace

import pytest

from rhapsody.backends.execution import _measure
from rhapsody.backends.execution import _peak

LAUNCHER = os.path.abspath(_peak.__file__)
MIB = 1 << 20
# Linux >= 6.2 keeps RSS in per-CPU counters, so a peak can read a few MiB under the truth.
SLACK = 16 * MIB
LINUX = sys.platform.startswith("linux")


def _launch(out_dir, *cmd, **kwargs):
    try:  # the backend makes the record directory; the launcher never does
        os.makedirs(out_dir, exist_ok=True)
    except OSError:
        pass
    return subprocess.run(
        [*_peak.launcher_argv(str(out_dir)), *cmd],
        capture_output=True,
        text=True,
        timeout=60,
        **kwargs,
    )


def _record(out_dir):
    names = [n for n in os.listdir(out_dir) if n.endswith(".json")]
    assert len(names) == 1, names
    with open(os.path.join(out_dir, names[0])) as f:
        return json.load(f)


def _alloc(mib, code=0):
    """A Python child that touches ``mib`` MiB, then exits with ``code``."""
    return (sys.executable, "-c", f"import sys; b = b'x' * ({mib} << 20); sys.exit({code})")


# ============================================================================
# Host memory and exit status, through the real launcher
# ============================================================================


@pytest.mark.parametrize("mib", [64, 512])
def test_peak_rss_of_child_that_allocates(tmp_path, mib):
    proc = _launch(tmp_path, *_alloc(mib))
    assert proc.returncode == 0, proc.stderr
    rec = _record(tmp_path)
    # The child's peak is the buffer plus a Python interpreter (well under 64 MiB).
    assert mib * MIB - SLACK <= rec["host_mem"] * (1 << 30) <= (mib + 64) * MIB
    assert rec["exit_code"] == 0
    assert rec["gpu_mem"] is None or rec["nvml"]


def test_record_has_the_execution_window_cpu_time_and_host(tmp_path):
    before = time.time()
    proc = _launch(tmp_path, sys.executable, "-c", "import time; time.sleep(0.3)")
    after = time.time()
    assert proc.returncode == 0, proc.stderr
    rec = _record(tmp_path)
    assert before <= rec["started"] <= rec["ended"] <= after
    assert rec["ended"] - rec["started"] >= 0.3
    assert rec["cpu_seconds"] >= 0
    assert rec["host"] == socket.gethostname()


def test_peak_rss_tracks_allocation_size(tmp_path):
    small, large = tmp_path / "small", tmp_path / "large"
    _launch(small, *_alloc(100))
    _launch(large, *_alloc(400))
    grown = (_record(large)["host_mem"] - _record(small)["host_mem"]) * (1 << 30)
    assert abs(grown - 300 * MIB) < 20 * MIB


def test_exit_code_and_stdio_pass_through(tmp_path):
    cmd = "import sys; print('to stdout'); print('to stderr', file=sys.stderr); sys.exit(7)"
    proc = _launch(tmp_path, sys.executable, "-c", cmd)
    assert proc.returncode == 7
    assert proc.stdout == "to stdout\n"
    assert proc.stderr == "to stderr\n"
    assert _record(tmp_path)["exit_code"] == 7


def test_death_by_signal_is_reraised(tmp_path):
    cmd = "import os, signal; os.kill(os.getpid(), signal.SIGTERM)"
    proc = _launch(tmp_path, sys.executable, "-c", cmd)
    assert proc.returncode == -signal.SIGTERM
    assert _record(tmp_path)["exit_code"] == -signal.SIGTERM


def test_missing_command_exits_127(tmp_path):
    proc = _launch(tmp_path, "rhapsody-no-such-command")
    assert proc.returncode == 127
    assert "rhapsody-no-such-command" in proc.stderr
    assert _record(tmp_path)["exit_code"] == 127


def test_command_arguments_and_path_lookup(tmp_path):
    proc = _launch(tmp_path, "sh", "-c", 'echo "$0 $1"', "a b", "c")
    assert proc.returncode == 0
    assert proc.stdout == "a b c\n"


def _start_sleeper(out_dir):
    """Launch a child that prints its pid, then sleeps; return (launcher, child pid)."""
    cmd = "import os, time; print(os.getpid(), flush=True); time.sleep(60)"
    launcher = subprocess.Popen(
        [sys.executable, LAUNCHER, "--out-dir", str(out_dir), "--", sys.executable, "-c", cmd],
        stdout=subprocess.PIPE,
        text=True,
    )
    return launcher, int(launcher.stdout.readline())


def _gone(pid, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        time.sleep(0.05)
    return False


def test_sigterm_is_forwarded_to_the_command(tmp_path):
    launcher, child = _start_sleeper(tmp_path)
    launcher.send_signal(signal.SIGTERM)
    assert launcher.wait(timeout=10) == -signal.SIGTERM
    assert _gone(child)
    assert _record(tmp_path)["exit_code"] == -signal.SIGTERM


@pytest.mark.skipif(not LINUX, reason="PR_SET_PDEATHSIG is Linux only")
def test_command_dies_with_a_killed_launcher(tmp_path):
    launcher, child = _start_sleeper(tmp_path)
    launcher.kill()
    assert launcher.wait(timeout=10) == -signal.SIGKILL
    assert _gone(child)
    assert not os.path.exists(tmp_path) or not os.listdir(tmp_path)


def test_usage_errors_exit_2(tmp_path):
    no_dash = subprocess.run(
        [sys.executable, "-I", LAUNCHER, "--out-dir", str(tmp_path), "true"], capture_output=True
    )
    no_cmd = subprocess.run(
        [sys.executable, "-I", LAUNCHER, "--out-dir", str(tmp_path), "--"], capture_output=True
    )
    assert no_dash.returncode == no_cmd.returncode == 2


def test_unwritable_out_dir_keeps_exit_code(tmp_path):
    blocker = tmp_path / "file"
    blocker.write_text("")
    proc = _launch(blocker / "sub", *_alloc(1, code=3))
    assert proc.returncode == 3
    assert "cannot write peaks" in proc.stderr


# ============================================================================
# GPU sampling, against a fake NVML
# ============================================================================


Proc = namedtuple("Proc", ["pid", "usedGpuMemory"])


class FakeNvml:
    """Two GPUs; ``procs[i]`` is what device i reports for the next sample."""

    class NVMLError(Exception):
        pass

    def __init__(self, ndev=2):
        self.procs = [[] for _ in range(ndev)]
        self.inits = self.shutdowns = 0

    def nvmlInit(self):
        self.inits += 1

    def nvmlShutdown(self):
        self.shutdowns += 1

    def nvmlDeviceGetCount(self):
        return len(self.procs)

    def nvmlDeviceGetHandleByIndex(self, i):
        return i

    def nvmlDeviceGetUUID(self, handle):
        return f"GPU-{handle}".encode()  # older pynvml returns bytes

    def nvmlDeviceGetComputeRunningProcesses(self, handle):
        if self.procs[handle] is None:
            raise self.NVMLError("not supported")
        return self.procs[handle]


def _sampler(nvml, tree=frozenset({10, 11})):
    return _peak.GpuSampler(nvml, 10, 0.01, tree=lambda pid: set(tree))


def test_gpu_peak_is_per_device_max_over_samples_of_the_tree_sum():
    nvml = FakeNvml()
    gpu = _sampler(nvml)
    nvml.procs = [[Proc(10, 3 * MIB), Proc(11, 2 * MIB), Proc(99, 50 * MIB)], []]
    gpu.sample()
    nvml.procs = [[Proc(10, 1 * MIB)], [Proc(11, 4 * MIB)]]
    gpu.sample()
    assert gpu.peak == {"GPU-0": 5 * MIB, "GPU-1": 4 * MIB}
    assert gpu.samples == 2


def test_gpu_unknown_usage_and_device_errors_are_tolerated():
    nvml = FakeNvml()
    gpu = _sampler(nvml)
    nvml.procs = [[Proc(10, None)], None]
    gpu.sample()
    assert gpu.peak == {}  # usage NVML cannot tell is unknown, not zero
    nvml.procs = [[Proc(10, None), Proc(11, 2 * MIB)], []]
    gpu.sample()
    assert gpu.peak == {"GPU-0": 2 * MIB}


def test_report_gpu_mem_is_none_unless_the_tree_was_seen_on_a_gpu():
    rusage = SimpleNamespace(ru_maxrss=(1 << 30) // _peak.MAXRSS_UNIT)  # 1 GiB
    nvml = FakeNvml()
    unseen = _sampler(nvml)
    nvml.procs = [[Proc(99, 8 * MIB)], []]
    unseen.sample()
    rec = _peak.report(rusage, unseen, 0)
    assert rec["host_mem"] == pytest.approx(1.0)
    assert rec["gpu_mem"] is None and rec["nvml"] is True

    seen = _sampler(nvml)
    nvml.procs = [[Proc(10, 512 * MIB)], [Proc(11, 1024 * MIB)]]
    seen.sample()
    rec = _peak.report(rusage, seen, 0)
    assert rec["gpu_mem"] == pytest.approx(1.0)
    assert rec["gpu_mem_devices"] == {"GPU-0": 0.5, "GPU-1": 1.0}

    rec = _peak.report(rusage, None, 0)
    assert rec["gpu_mem"] is None and rec["nvml"] is False


def test_sampler_thread_samples_until_stopped():
    nvml = FakeNvml()
    nvml.procs = [[Proc(10, MIB)], []]
    gpu = _sampler(nvml)
    gpu.start()
    time.sleep(0.1)
    gpu.stop()
    assert not gpu.is_alive()
    assert gpu.samples >= 2
    assert gpu.peak == {"GPU-0": MIB}


def test_load_nvml_without_pynvml(monkeypatch):
    monkeypatch.setitem(sys.modules, "pynvml", None)  # import pynvml raises ImportError
    assert _peak.load_nvml() is None


def test_load_nvml_without_gpus_or_driver(monkeypatch):
    nvml = FakeNvml(ndev=0)
    monkeypatch.setitem(sys.modules, "pynvml", nvml)
    assert _peak.load_nvml() is None
    assert nvml.shutdowns == 1

    def no_driver():
        raise FakeNvml.NVMLError("driver not loaded")

    broken = FakeNvml()
    broken.nvmlInit = no_driver
    monkeypatch.setitem(sys.modules, "pynvml", broken)
    assert _peak.load_nvml() is None


def test_descendants_finds_grandchildren():
    if not os.path.exists(f"/proc/{os.getpid()}/task/{os.getpid()}/children"):
        assert _peak.descendants(os.getpid()) == {os.getpid()}
        return
    cmd = "import subprocess, sys, time; subprocess.Popen(['sleep', '30']); time.sleep(30)"
    child = subprocess.Popen([sys.executable, "-c", cmd])
    try:
        deadline = time.monotonic() + 10
        while len(_peak.descendants(child.pid)) < 2 and time.monotonic() < deadline:
            time.sleep(0.05)
        tree = _peak.descendants(os.getpid())
        assert child.pid in tree and len(tree) >= 3
    finally:
        for pid in _peak.descendants(child.pid):
            os.kill(pid, signal.SIGKILL)
        child.wait()


# ============================================================================
# measured_call: function tasks in pooled workers
# ============================================================================


def _hold(mib):
    buf = b"x" * (mib * MIB)
    return len(buf)


def test_measured_call_returns_the_peak_and_the_growth():
    before = time.time()
    result = _peak.measured_call(_hold, (256,), {})
    assert isinstance(result, _peak.MeasuredResult)
    assert result.value == 256 * MIB
    assert before <= result.observed["started"] <= result.observed["ended"] <= time.time()
    assert result.observed["host"] == socket.gethostname()
    if not LINUX:
        assert result.observed["host_mem"] is None
        assert "approximate" not in result.observed
        return
    assert result.observed["approximate"] is True
    assert result.observed["gpu_mem"] is None
    growth = result.observed["host_mem_growth"] * (1 << 30)
    assert 256 * MIB - SLACK <= growth <= 300 * MIB
    # host_mem is the whole peak during the call: this process's own size included
    assert result.observed["host_mem"] * (1 << 30) >= growth + 8 * MIB


def test_measured_call_without_memory_reports_only_times():
    result = _peak.measured_call(_hold, (8,), {}, False)
    assert result.observed["host_mem"] is None
    assert result.observed["approximate"] is True  # the thread's CPU time only
    assert result.observed["started"] <= result.observed["ended"]


def _spin(seconds):
    end = time.thread_time() + seconds  # this thread's CPU time
    while time.thread_time() < end:
        pass


def test_measured_call_reports_cpu_time_of_the_process_or_the_thread():
    busy = _peak.measured_call(_spin, (0.2,), {})
    window = busy.observed["ended"] - busy.observed["started"]
    assert 0.2 <= busy.observed["cpu_seconds"] <= window + 0.05
    idle = _peak.measured_call(time.sleep, (0.2,), {}, False)  # the calling thread's
    assert idle.observed["ended"] - idle.observed["started"] >= 0.2
    assert 0 <= idle.observed["cpu_seconds"] < 0.05


def test_measured_call_raises_the_error_with_its_measurement():
    def boom(x, y=0):
        time.sleep(0.05)
        raise ValueError(x + y)

    with pytest.raises(_measure.MeasuredError, match="3") as excinfo:
        _peak.measured_call(boom, (1,), {"y": 2})
    error, observed = _measure.split_error(excinfo.value)
    assert isinstance(error, ValueError) and str(error) == "3"
    assert observed["ended"] - observed["started"] >= 0.05
    assert observed["cpu_seconds"] >= 0 and observed["host"] == socket.gethostname()
    # Both survive the pickling that carries them back from a pool worker
    again = _measure.split_error(pickle.loads(pickle.dumps(excinfo.value)))
    assert str(again[0]) == "3" and again[1] == observed
    plain = ValueError("plain")
    assert _measure.split_error(plain) == (plain, None)


# ============================================================================
# Review findings: each test fails without its fix
# ============================================================================


def test_the_command_starts_with_default_signal_dispositions(tmp_path):
    """Python ignores SIGPIPE and SIGXFSZ; a command used to inherit that."""
    proc = _launch(tmp_path, "bash", "-c", "trap -p")
    assert proc.returncode == 0 and proc.stdout == "", proc.stdout
    proc = _launch(tmp_path / "p", "bash", "-c", "set -o pipefail; yes | head -n1 >/dev/null")
    assert proc.returncode == 128 + signal.SIGPIPE, proc.stderr
    assert "Broken pipe" not in proc.stderr


def test_the_task_environment_does_not_break_the_launcher(tmp_path):
    env = {**os.environ, "PYTHONHOME": "/nonexistent", "PYTHONWARNINGS": "error"}
    proc = _launch(tmp_path, "sh", "-c", 'echo "$PYTHONHOME"', env=env)
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout == "/nonexistent\n"  # the command still gets the task's environment
    assert _record(tmp_path)["exit_code"] == 0


def test_a_broken_pynvml_is_no_nvml(monkeypatch):
    import importlib.abc

    class Broken(importlib.abc.MetaPathFinder):
        def find_spec(self, name, path=None, target=None):
            if name == "pynvml":
                raise FutureWarning("pynvml is deprecated")  # under -W error
            return None

    monkeypatch.delitem(sys.modules, "pynvml", raising=False)
    monkeypatch.setattr(sys, "meta_path", [Broken(), *sys.meta_path])
    assert _peak.load_nvml() is None


@pytest.fixture
def launcher_signals():
    """Restores the handlers _peak.run() installs in this process."""
    saved = {sig: signal.getsignal(sig) for sig in _peak.FORWARDED_SIGNALS}
    yield
    for sig, handler in saved.items():
        signal.signal(sig, handler)


def test_slow_or_failing_nvml_neither_delays_nor_kills_the_run(
    tmp_path, monkeypatch, launcher_signals
):
    def slow():
        time.sleep(0.5)
        raise RuntimeError("NVML is broken")

    monkeypatch.setattr(_peak, "load_nvml", slow)
    code = _peak.run(["sh", "-c", "exit 4"], str(tmp_path))
    rec = _record(tmp_path)
    assert code == rec["exit_code"] == 4
    assert rec["ended"] - rec["started"] < 0.4  # NVML loads in the sampler thread
    assert rec["nvml"] is False and rec["gpu_mem"] is None


def test_nothing_is_forwarded_once_the_command_exited(tmp_path, monkeypatch, launcher_signals):
    _peak.run(["sh", "-c", "exit 0"], str(tmp_path))
    forward = signal.getsignal(signal.SIGUSR1)
    sent = []
    monkeypatch.setattr(os, "kill", lambda *a: sent.append(a))
    monkeypatch.setattr(os, "killpg", lambda *a: sent.append(a))
    forward(signal.SIGUSR1, None)  # a late signal: its pid may be another process's now
    assert sent == []


COUNT_USR1 = """
import os, signal, sys, time
hits = []
signal.signal(signal.SIGUSR1, lambda *_: hits.append(1))
open(sys.argv[1], "w").close()
deadline = time.time() + 1.0
while time.time() < deadline:
    time.sleep(0.01)
with open(sys.argv[2], "w") as f:
    f.write(str(len(hits)))
"""


def test_a_signal_to_the_launchers_group_reaches_the_command_once(tmp_path):
    ready, count, out = tmp_path / "ready", tmp_path / "count", tmp_path / "rec"
    out.mkdir()
    launcher = subprocess.Popen(
        [*_peak.launcher_argv(str(out)), sys.executable, "-c", COUNT_USR1, ready, count],
        stdin=subprocess.DEVNULL,
        start_new_session=True,  # its own group, as Ctrl-C or Slurm would signal
    )
    deadline = time.time() + 10
    while not ready.exists() and time.time() < deadline:
        time.sleep(0.01)
    os.killpg(launcher.pid, signal.SIGUSR1)
    assert launcher.wait(timeout=10) == 0
    assert count.read_text() == "1"


def test_peak_dirs_are_fresh_absolute_and_inside_their_root(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    first = _peak.make_peak_dir("root", "task.1")
    again = _peak.make_peak_dir("root", "task.1")  # the uid reused after a cancel
    assert os.path.isabs(first) and first != again
    for uid in ("..", ".", "/etc", "a/../../b", ""):
        path = _peak.make_peak_dir("root", uid)
        assert os.path.dirname(path) == str(tmp_path / "root") and os.path.isdir(path)


def test_read_peaks_never_removes_a_work_directory(tmp_path):
    keep = tmp_path / "work" / "keep.txt"
    keep.parent.mkdir()
    keep.write_text("x")
    work = str(tmp_path / "work")  # as strings: pathlib would drop the "."
    assert _peak.read_peaks(work + "/sub/..", 1) is None
    assert _peak.read_peaks(work + "/.", 1) is None
    assert _peak.read_peaks(work + "/./", 1) is None
    assert keep.exists()


def test_a_late_writer_does_not_bring_a_read_directory_back(tmp_path):
    d = _peak.make_peak_dir(str(tmp_path), "t")
    assert _peak.read_peaks(d, 1) is None  # gave up on it: removed
    _peak.write_report(d, {"host": "h", "host_mem": 1.0})
    _measure.write_record(d, {"host": "h", "host_mem": 1.0})
    assert not os.path.exists(d)


def test_read_peaks_wants_exactly_its_ranks(tmp_path):
    d = str(tmp_path / "t")
    os.makedirs(d)
    for i in range(3):
        with open(os.path.join(d, f"h.{i}.json"), "w") as f:
            json.dump({"host_mem": 1.0, "host": "h"}, f)
    assert _peak.read_peaks(d, 2) is None  # an extra record is not this run's
    os.makedirs(d)
    with open(os.path.join(d, "h.json"), "w") as f:
        json.dump({"host": "h"}, f)  # no host_mem
    assert _peak.read_peaks(d, 1) is None


def _busy_thread(seconds):
    import threading

    t = threading.Thread(target=_spin, args=(seconds,))
    t.start()
    t.join()


def test_thread_measurement_is_marked_approximate_and_misses_started_threads():
    _, in_thread = _measure.measure(_busy_thread, (0.2,), {}, memory=False)
    assert in_thread["approximate"] is True and in_thread["cpu_seconds"] < 0.1
    _, in_process = _measure.measure(_busy_thread, (0.2,), {}, memory=True)
    assert in_process["cpu_seconds"] >= 0.2


def test_the_measurement_never_lands_on_the_users_exception():
    shared = ValueError("stored once, raised by several tasks")

    def reraise(seconds):
        time.sleep(seconds)
        raise shared

    errors = []
    for seconds in (0.01, 0.1):
        with pytest.raises(_measure.MeasuredError) as excinfo:
            _measure.measure(reraise, (seconds,), {})
        errors.append(_measure.split_error(excinfo.value))
    (e1, o1), (e2, o2) = errors
    assert e1 is e2 is shared and vars(shared) == {}
    assert o2["ended"] - o2["started"] > o1["ended"] - o1["started"]


def test_process_records_have_the_launchers_keys(tmp_path):
    launched = tmp_path / "l"
    _launch(launched, "sh", "-c", "exit 0")
    record = _measure.process_record(time.time(), _measure.cpu_now(), 0)
    assert set(record) == set(_record(launched))
    assert (_measure.GIB, _measure.MAXRSS_UNIT) == (_peak.GIB, _peak.MAXRSS_UNIT)


HOLD_AND_RECORD = """
import sys, time
from rhapsody.backends.execution import _measure
started, cpu = time.time(), _measure.cpu_now()
b = b"x" * (96 << 20)
_measure.write_record(sys.argv[1], _measure.process_record(started, cpu, 0))
"""


def test_a_process_record_is_its_own_lifetime_peak(tmp_path):
    d = _peak.make_peak_dir(str(tmp_path), "t")
    subprocess.run([sys.executable, "-c", HOLD_AND_RECORD, d], check=True, timeout=60)
    observed = _peak.read_peaks(d, 1)
    assert observed["host_mem"] * (1 << 30) >= 96 * MIB - SLACK
    assert observed["host"] == socket.gethostname()


# ============================================================================
# What a node held: the launcher's floor, absolute peaks
# ============================================================================


def test_the_launcher_records_its_size_at_fork_as_the_floor(tmp_path):
    out = tmp_path / "small"
    assert _launch(out, "sh", "-c", "exit 0").returncode == 0
    rec = _record(out)
    assert 1 * MIB <= rec["floor"] * (1 << 30) <= 256 * MIB
    if LINUX:  # ru_maxrss keeps the child's copy of the launcher: a tiny command shows the floor
        assert _peak._at_floor(rec)
        big = tmp_path / "big"
        _launch(big, *_alloc(64))
        assert not _peak._at_floor(_record(big))


def _rec(host_mem, floor=None, host="h"):
    return {"host_mem": host_mem, "floor": floor, "host": host, "gpu_mem_devices": {}}


def test_ranks_at_the_launchers_floor_count_as_one():
    floor = 14 / 1024  # the launcher's VmRSS; a forked child keeps less of it (5 to 15 MiB)
    # Above the floor, a rank's peak is its command's own: the sum is exact
    big = _peak.combine([_rec(0.5, floor), _rec(0.25, floor), _rec(0.5, floor)], 3)
    assert big["host_mem"] == pytest.approx(1.25)
    assert (big["host_mem_max"], big["ranks"]) == (0.5, 3)
    # 64 ranks that never grew past what the child kept of the launcher: one rank, not 64
    tiny = 5 / 1024
    small = _peak.combine([_rec(tiny, floor) for _ in range(64)], 64)
    assert small["host_mem"] == pytest.approx(tiny)
    assert (small["host_mem_max"], small["ranks"]) == (tiny, 64)
    # Both kinds in one job: the small ones count as their largest
    mixed = _peak.combine([_rec(0.5, floor), _rec(tiny, floor), _rec(floor * 1.01, floor)], 3)
    assert mixed["host_mem"] == pytest.approx(0.5 + floor * 1.01)
    # Records without a floor (a process's own) count as they are
    assert _peak.combine([_rec(0.1), _rec(0.1)], 2)["host_mem"] == pytest.approx(0.2)


FORK_AND_RECORD = """
import os, sys
held = b"x" * (256 << 20)  # the parent's size, which the child's ru_maxrss would keep
pid = os.fork()
if pid == 0:
    os.execv(sys.executable, [sys.executable, "-c", sys.argv[2], sys.argv[1]])
os.waitpid(pid, 0)
"""


@pytest.mark.skipif(not LINUX, reason="VmHWM is Linux's")
def test_a_process_record_does_not_count_the_process_that_forked_it(tmp_path):
    d = _peak.make_peak_dir(str(tmp_path), "t")
    subprocess.run(
        [sys.executable, "-c", FORK_AND_RECORD, d, HOLD_AND_RECORD], check=True, timeout=60
    )
    observed = _peak.read_peaks(d, 1)
    assert 96 * MIB - SLACK <= observed["host_mem"] * (1 << 30) <= 240 * MIB
