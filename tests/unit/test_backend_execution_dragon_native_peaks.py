"""DragonNativeExecutionBackend(observe_peaks=True): what each task used, as ``task["observed"]``.

Runs against the fake Dragon of ``test_backend_execution_dragon_native``: Python targets (pool
workers, function processes, group members) run as threads of this process, executables as real
subprocesses, so executables really run under the peak launcher. The real-runtime test at the end
needs ``dragon python -m pytest``.
"""

import os
import pickle
import socket
import sys
import time

import pytest

cloudpickle = pytest.importorskip("cloudpickle")

# The fake Dragon and helpers of #121's tests (tests/unit is on sys.path while pytest runs)
from test_backend_execution_dragon_native import Recorder  # noqa: E402
from test_backend_execution_dragon_native import _gate  # noqa: E402, F401 - autouse fixture
from test_backend_execution_dragon_native import asquare  # noqa: E402
from test_backend_execution_dragon_native import backend  # noqa: E402, F401 - fixture
from test_backend_execution_dragon_native import boom  # noqa: E402
from test_backend_execution_dragon_native import dragon  # noqa: E402, F401 - fixture
from test_backend_execution_dragon_native import exe_task  # noqa: E402
from test_backend_execution_dragon_native import fn_task  # noqa: E402
from test_backend_execution_dragon_native import needs_runtime  # noqa: E402
from test_backend_execution_dragon_native import square  # noqa: E402
from test_backend_execution_dragon_native import until  # noqa: E402

from rhapsody.backends.execution import _dragon_native_worker as worker_module  # noqa: E402
from rhapsody.backends.execution import _peak  # noqa: E402
from rhapsody.backends.execution.dragon import PEAK_LAUNCHER  # noqa: E402
from rhapsody.backends.execution.dragon_native import DragonNativeExecutionBackend  # noqa: E402

MIB = 1 << 20
SLACK = 16 * MIB  # Linux >= 6.2 keeps RSS in per-CPU counters, which can read a few MiB low
LINUX = sys.platform.startswith("linux")
WINDOW = ("started", "ended", "cpu_seconds", "host")


@pytest.fixture
async def observing(dragon, tmp_path):  # noqa: F811
    """A started backend with observe_peaks, 2 workers per node and inline capture off."""
    be = DragonNativeExecutionBackend(
        workers_per_node=2, inline_stdio=False, kill_grace=0.5, observe_peaks=True
    )
    be._work_dir = str(tmp_path)
    await be
    yield be
    await be.shutdown()


def alloc(mib, code=0):
    """An executable task that holds ``mib`` MiB, then exits with ``code``."""
    script = f"import sys; b = b'x' * ({mib} << 20); sys.exit({code})"
    return exe_task(sys.executable, "-c", script)


def spin_then(code, seconds=0.1):
    """Busy for ``seconds`` of this thread's CPU time, then return 7 or raise."""
    end = time.thread_time() + seconds
    while time.thread_time() < end:
        pass
    if code:
        raise ValueError("spun, then failed")
    return 7


def assert_window(observed, before):
    assert all(observed.get(k) is not None for k in WINDOW), observed
    assert before <= observed["started"] <= observed["ended"] <= time.time()
    assert observed["cpu_seconds"] >= 0


async def run(backend, tasks, timeout=30.0):
    rec = Recorder(backend)
    await backend.submit_tasks(tasks)
    return [(await rec.wait(t["uid"], timeout))[0] for t in tasks]


# ============================================================================
# Off by default
# ============================================================================


async def test_off_by_default_nothing_observed_and_nothing_launched(backend, dragon, tmp_path):  # noqa: F811
    tasks = [
        fn_task(square, 3),
        fn_task(square, 4, task_backend_specific_kwargs={"process_template": {}}),
        alloc(1),
        exe_task(
            "/bin/sh", "-c", "exit 0", task_backend_specific_kwargs={"process_templates": [(2, {})]}
        ),
    ]
    assert await run(backend, tasks) == ["DONE"] * 4
    assert all("observed" not in t for t in tasks)
    assert not any(PEAK_LAUNCHER in p.args for p in dragon.started)
    assert not list(tmp_path.glob("rhapsody-peaks.*"))


def test_peak_source_is_checked(dragon):  # noqa: F811
    with pytest.raises(ValueError, match="native API does not report"):
        DragonNativeExecutionBackend(observe_peaks=True, peak_source="dragon")
    with pytest.raises(ValueError, match="peak_source must be one of"):
        DragonNativeExecutionBackend(peak_source="nvml")
    DragonNativeExecutionBackend(observe_peaks=False, peak_source="dragon")  # not used
    DragonNativeExecutionBackend(observe_peaks=True, peak_source="launcher")


# ============================================================================
# Pool functions: measured in the worker
# ============================================================================


async def test_pool_functions_report_the_call_done_and_failed(observing):
    before = time.time()
    tasks = [fn_task(spin_then, 0), fn_task(asquare, 5), fn_task(spin_then, 1), fn_task(boom)]
    assert await run(observing, tasks) == ["DONE", "DONE", "FAILED", "FAILED"]
    assert [t.get("return_value") for t in tasks[:2]] == [7, 25]  # the value, not the measurement
    for task in tasks:
        observed = task["observed"]
        assert_window(observed, before)
        assert observed["gpu_mem"] is None and observed["host"] == socket.gethostname()
        if LINUX:
            assert observed["approximate"] is True and observed["host_mem"] >= 0
        else:
            assert observed["host_mem"] is None
    assert tasks[0]["observed"]["cpu_seconds"] >= 0.1  # this worker's (a thread here) CPU time
    assert isinstance(tasks[2]["exception"], ValueError)
    assert "spun, then failed" in tasks[2]["stderr"]


# ============================================================================
# Executables: under the peak launcher
# ============================================================================


async def test_executables_run_under_the_launcher_done_and_failed(observing, dragon, tmp_path):  # noqa: F811
    before = time.time()
    tasks = [alloc(64), alloc(32, code=3)]
    assert await run(observing, tasks) == ["DONE", "FAILED"]
    assert (tasks[0]["exit_code"], tasks[1]["exit_code"]) == (0, 3)
    for task, mib in zip(tasks, (64, 32)):
        observed = task["observed"]
        assert_window(observed, before)
        assert observed["host_mem"] * (1 << 30) >= mib * MIB - SLACK
    launched = [p for p in dragon.started if p.target == sys.executable]
    assert len(launched) == 2
    assert all(p.args[:2] == (PEAK_LAUNCHER, "--out-dir") and p.args[3] == "--" for p in launched)
    roots = list(tmp_path.glob("rhapsody-peaks.*"))
    assert len(roots) == 1 and os.listdir(roots[0]) == []  # records read and removed
    await observing.shutdown()
    assert not list(tmp_path.glob("rhapsody-peaks.*"))  # roots go at shutdown


async def test_executable_output_still_comes_back_inline(dragon, tmp_path):  # noqa: F811
    be = DragonNativeExecutionBackend(workers_per_node=1, observe_peaks=True)
    be._work_dir = str(tmp_path)
    await be
    try:
        task = exe_task("/bin/sh", "-c", "echo hello; echo oops >&2; exit 2")
        assert await run(be, [task]) == ["FAILED"]
    finally:
        await be.shutdown()
    assert (task["stdout"], task["stderr"], task["exit_code"]) == ("hello\n", "oops\n", 2)
    assert task["observed"]["started"] <= task["observed"]["ended"]


async def test_a_group_sums_its_launched_ranks(observing, dragon):  # noqa: F811
    script = "import sys; b = b'x' * (40 << 20); sys.exit(int(sys.argv[1]))"
    ok = exe_task(
        sys.executable,
        "-c",
        script,
        "0",
        task_backend_specific_kwargs={"process_templates": [(2, {}), (1, {})]},
    )
    bad = exe_task(
        sys.executable,
        "-c",
        script,
        "5",
        task_backend_specific_kwargs={"process_templates": [(2, {})]},
    )
    before = time.time()
    assert await run(observing, [ok, bad]) == ["DONE", "FAILED"]
    assert ok["return_value"] == [0, 0, 0] and bad["exit_code"] == 5
    assert ok["observed"]["host_mem"] * (1 << 30) >= 3 * (40 * MIB - SLACK)
    assert bad["observed"]["host_mem"] * (1 << 30) >= 2 * (40 * MIB - SLACK)
    for task in (ok, bad):
        assert_window(task["observed"], before)
    group = dragon.groups[0]
    assert all(t.target == sys.executable for _, t in group.templates)


async def test_a_timed_out_executable_reports_what_it_used(observing):
    task = exe_task("/bin/sleep", "30", task_backend_specific_kwargs={"timeout": 0.5})
    assert await run(observing, [task]) == ["FAILED"]
    assert isinstance(task["exception"], TimeoutError)
    # The launcher forwards the SIGTERM, reaps the command and writes its record
    assert task["observed"]["ended"] - task["observed"]["started"] >= 0.4


async def test_a_cancelled_executable_reports_nothing_and_leaves_no_records(observing, tmp_path):
    task = exe_task("/bin/sleep", "30")
    rec = Recorder(observing)
    await observing.submit_tasks([task])
    await until(lambda: observing._tasks[task["uid"]].puids)
    assert await observing.cancel_task(task["uid"])
    assert await rec.wait(task["uid"]) == ["CANCELED"]
    assert "observed" not in task
    # The task's thread reads and removes what the killed launcher wrote, if anything
    await until(lambda: all(os.listdir(r) == [] for r in tmp_path.glob("rhapsody-peaks.*")))


# ============================================================================
# Functions as their own process, and as group members: a record from inside
# ============================================================================


async def test_function_processes_and_group_members_write_their_records(observing):
    tmpl = {"process_template": {}}
    group = {"process_templates": [(2, {})]}
    tasks = [
        fn_task(spin_then, 0, task_backend_specific_kwargs=tmpl),
        fn_task(spin_then, 1, task_backend_specific_kwargs=tmpl),
        fn_task(spin_then, 0, task_backend_specific_kwargs=group),
        fn_task(spin_then, 1, task_backend_specific_kwargs=group),
    ]
    before = time.time()
    assert await run(observing, tasks) == ["DONE", "FAILED", "DONE", "FAILED"]
    assert tasks[0]["return_value"] == 7 and isinstance(tasks[1]["exception"], ValueError)
    for task in tasks:
        observed = task["observed"]
        assert_window(observed, before)
        assert observed["host_mem"] > 0 and observed["gpu_mem"] is None
        assert "approximate" not in observed  # the process's own peak, not a growth
    # Two members, two records, summed: here both are this test process
    assert tasks[2]["observed"]["host_mem"] == pytest.approx(
        2 * tasks[0]["observed"]["host_mem"], rel=0.2
    )


# ============================================================================
# The worker-side helpers against _peak
# ============================================================================


def test_worker_measurement_has_the_keys_of_measured_call():
    assert worker_module.OBSERVED_ATTR == _peak.OBSERVED_ATTR
    value, observed = worker_module.measured(square, (6,), {})
    reference = _peak.measured_call(square, (6,), {}).observed
    assert value == 36 and set(observed) == set(reference)
    value, observed = worker_module.measured(asquare, (2,), {})  # driven as call() does
    assert value == 4 and observed["started"] <= observed["ended"]


def test_a_raising_call_carries_its_measurement_through_pickling():
    with pytest.raises(ValueError, match="spun, then failed") as excinfo:
        worker_module.measured(spin_then, (1,), {})
    observed = getattr(excinfo.value, worker_module.OBSERVED_ATTR)
    assert observed["cpu_seconds"] >= 0.1
    again = cloudpickle.loads(cloudpickle.dumps(excinfo.value))
    assert getattr(again, _peak.OBSERVED_ATTR) == observed
    assert pickle.loads(pickle.dumps(excinfo.value)).rhapsody_observed == observed


def test_run_task_returns_the_value_with_its_measurement():
    ok, value, tb, _, _ = worker_module.run_task("tok", square, (3,), {}, None, None, False, True)
    assert ok and value[0] == 9 and value[1]["started"] <= value[1]["ended"]
    ok, value, tb, _, _ = worker_module.run_task("tok", boom, (), {}, None, None, False, True)
    assert not ok and getattr(value, worker_module.OBSERVED_ATTR)["host"] == socket.gethostname()


def test_records_from_inside_a_process_read_as_the_launchers_do(tmp_path):
    peak_dir = str(tmp_path / "uid")
    started = time.time()
    worker_module.write_record(peak_dir, worker_module.process_record(started, 0))
    worker_module.write_record(peak_dir, worker_module.process_record(started, 1))
    observed = _peak.read_peaks(peak_dir, 2)
    assert observed["host"] == socket.gethostname() and observed["gpu_mem"] is None
    assert observed["started"] == started and observed["ended"] >= started
    assert observed["host_mem"] > 0 and observed["cpu_seconds"] > 0
    assert not os.path.exists(peak_dir)


def test_a_group_member_writes_its_record_also_when_it_raises(tmp_path):
    peak_dir = str(tmp_path / "uid")
    with pytest.raises(ValueError):
        worker_module.run_group_member(spin_then, (1,), {}, None, None, peak_dir)
    worker_module.run_group_member(spin_then, (0,), {}, None, None, peak_dir)
    records = [r for r in os.listdir(peak_dir) if r.endswith(".json")]
    assert len(records) == 2
    assert _peak.read_peaks(peak_dir, 2)["cpu_seconds"] >= 0.2


# ============================================================================
# Real Dragon runtime (``dragon python -m pytest``)
# ============================================================================


@needs_runtime
async def test_real_runtime_observed(tmp_path):
    """Every task kind, DONE and FAILED, on a real runtime: each reports its window."""
    # Dragon's workers cannot import this test module: send its functions by value.
    cloudpickle.register_pickle_by_value(sys.modules[__name__])
    be = await DragonNativeExecutionBackend(workers_per_node=2, observe_peaks=True)
    be._work_dir = str(tmp_path)
    tmpl = {"process_template": {}}
    group = {"process_templates": [(2, {})]}
    tasks = [
        fn_task(spin_then, 0),
        fn_task(spin_then, 1),
        alloc(64),
        alloc(32, code=3),
        fn_task(spin_then, 0, task_backend_specific_kwargs=tmpl),
        fn_task(spin_then, 1, task_backend_specific_kwargs=tmpl),
        exe_task(sys.executable, "-c", "b = b'x' * (40 << 20)", task_backend_specific_kwargs=group),
        fn_task(spin_then, 0, task_backend_specific_kwargs=group),
    ]
    before = time.time()
    try:
        states = await run(be, tasks, timeout=120)
    finally:
        await be.shutdown()
        cloudpickle.unregister_pickle_by_value(sys.modules[__name__])
    assert states == ["DONE", "FAILED"] * 3 + ["DONE", "DONE"], [t.get("exception") for t in tasks]
    for task in tasks:
        assert_window(task["observed"], before)
    assert tasks[2]["observed"]["host_mem"] * (1 << 30) >= 64 * MIB - SLACK
    assert tasks[6]["observed"]["host_mem"] * (1 << 30) >= 2 * (40 * MIB - SLACK)
    assert tasks[0]["observed"]["approximate"] is True
    assert not list(tmp_path.glob("rhapsody-peaks.*"))
