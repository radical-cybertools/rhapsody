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
import signal
import socket
import subprocess
import sys
import time
from collections import namedtuple
from types import SimpleNamespace

import pytest

from rhapsody.backends.execution import _peak

LAUNCHER = os.path.abspath(_peak.__file__)
MIB = 1 << 20
# Linux >= 6.2 keeps RSS in per-CPU counters, so a peak can read a few MiB under the truth.
SLACK = 16 * MIB
LINUX = sys.platform.startswith("linux")


def _launch(out_dir, *cmd, **kwargs):
    return subprocess.run(
        [sys.executable, LAUNCHER, "--out-dir", str(out_dir), "--", *cmd],
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
        [sys.executable, LAUNCHER, "--out-dir", str(tmp_path), "true"], capture_output=True
    )
    no_cmd = subprocess.run(
        [sys.executable, LAUNCHER, "--out-dir", str(tmp_path), "--"], capture_output=True
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
    assert gpu.peak == {"GPU-0": 0}


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


def test_measured_call_returns_value_and_growth():
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
    assert 256 * MIB - SLACK <= result.observed["host_mem"] * (1 << 30) <= 300 * MIB


def test_measured_call_without_memory_reports_only_times():
    result = _peak.measured_call(_hold, (8,), {}, False)
    assert result.observed["host_mem"] is None and "approximate" not in result.observed
    assert result.observed["started"] <= result.observed["ended"]


def test_measured_call_propagates_exceptions():
    def boom(x, y=0):
        raise ValueError(x + y)

    with pytest.raises(ValueError, match="3"):
        _peak.measured_call(boom, (1,), {"y": 2})
