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
import threading
import time

import pytest

cloudpickle = pytest.importorskip("cloudpickle")

# The fake Dragon and helpers of #121's tests (tests/unit is on sys.path while pytest runs)
from test_backend_execution_dragon_native import Recorder  # noqa: E402
from test_backend_execution_dragon_native import _gate  # noqa: E402, F401 - autouse fixture
from test_backend_execution_dragon_native import asquare  # noqa: E402
from test_backend_execution_dragon_native import backend  # noqa: E402, F401 - fixture
from test_backend_execution_dragon_native import blocked  # noqa: E402
from test_backend_execution_dragon_native import boom  # noqa: E402
from test_backend_execution_dragon_native import dragon  # noqa: E402, F401 - fixture
from test_backend_execution_dragon_native import exe_task  # noqa: E402
from test_backend_execution_dragon_native import fn_task  # noqa: E402
from test_backend_execution_dragon_native import full_until  # noqa: E402
from test_backend_execution_dragon_native import needs_runtime  # noqa: E402
from test_backend_execution_dragon_native import pool_full  # noqa: E402, F401 - fixture
from test_backend_execution_dragon_native import square  # noqa: E402
from test_backend_execution_dragon_native import until  # noqa: E402

from rhapsody.backends.execution import _dragon_native_worker as worker_module  # noqa: E402
from rhapsody.backends.execution import _measure  # noqa: E402
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
    launched = [p for p in dragon.started if PEAK_LAUNCHER in p.args]
    assert len(launched) == 2 and all(p.target == sys.executable for p in launched)
    # python -I _peak.py --out-dir DIR -- command
    assert all(p.args[:3] == ("-I", PEAK_LAUNCHER, "--out-dir") for p in launched)
    assert all(p.args[4] == "--" for p in launched)
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
        # The CPU time of the call, not of the process's whole life (here: pytest's)
        assert observed["cpu_seconds"] <= observed["ended"] - observed["started"] + 0.5
    # In the fake, processes are threads of pytest: their real peaks are tested in
    # test_backend_execution_peak.py (a process record in a process of its own) and on Dragon.


# ============================================================================
# The worker-side helpers against _peak
# ============================================================================


def test_the_worker_measures_with_measured_calls_keys():
    ok, (value, observed), _ = worker_module.outcome(square, (6,), {}, measure=True)
    reference = _peak.measured_call(square, (6,), {}).observed
    assert ok and value == 36 and set(observed) == set(reference)
    ok, (value, observed), _ = worker_module.outcome(asquare, (2,), {}, measure=True)
    assert ok and value == 4 and observed["started"] <= observed["ended"]  # driven as call()


def test_a_raising_call_comes_back_as_its_error_with_its_measurement():
    ok, (error, observed), tb = worker_module.outcome(spin_then, (1,), {}, measure=True)
    assert not ok and isinstance(error, ValueError) and "spun, then failed" in tb
    assert observed["cpu_seconds"] >= 0.1 and observed["host"] == socket.gethostname()
    msg = cloudpickle.loads(worker_module.pack(("done", 0, "u", ok, (error, observed)), 4, True))
    again = cloudpickle.loads(msg[4])
    assert isinstance(again[0], ValueError) and again[1] == observed


def exits():
    sys.exit(3)


def test_sys_exit_comes_back_as_a_runtime_error_with_its_measurement():
    ok, (error, observed), _ = worker_module.outcome(exits, (), {}, measure=True)
    assert not ok and isinstance(error, RuntimeError) and "SystemExit" in str(error)
    assert observed["started"] <= observed["ended"]


def returns_lock():
    return threading.Lock()


def test_an_unpicklable_value_is_named_and_keeps_its_measurement():
    ok, value, tb, _, _ = worker_module.run_task(
        "tok", returns_lock, (), {}, None, None, False, True
    )
    raw = worker_module.pack(("done", 0, "u", ok, value, tb, "", ""), 4, measured=True)
    msg = cloudpickle.loads(raw)
    assert msg[3] is False
    error, observed = cloudpickle.loads(msg[4])
    assert "cannot pickle the task's lock" in str(error)
    assert observed["started"] <= observed["ended"]


def test_records_from_inside_a_process_read_as_the_launchers_do(tmp_path):
    peak_dir = _peak.make_peak_dir(str(tmp_path), "uid")
    started, cpu = time.time(), _measure.cpu_now()
    _measure.write_record(peak_dir, _measure.process_record(started, cpu, 0))
    _measure.write_record(peak_dir, _measure.process_record(started, cpu, 1))
    observed = _peak.read_peaks(peak_dir, 2)
    assert observed["host"] == socket.gethostname() and observed["gpu_mem"] is None
    assert observed["started"] == started and observed["ended"] >= started
    assert observed["host_mem"] > 0 and observed["cpu_seconds"] >= 0
    assert not os.path.exists(peak_dir)


def test_a_group_member_writes_its_record_also_when_it_raises(tmp_path):
    peak_dir = _peak.make_peak_dir(str(tmp_path), "uid")
    with pytest.raises(ValueError):
        worker_module.run_group_member(spin_then, (1,), {}, None, None, peak_dir)
    worker_module.run_group_member(spin_then, (0,), {}, None, None, peak_dir)
    records = [r for r in os.listdir(peak_dir) if r.endswith(".json")]
    assert len(records) == 2
    assert _peak.read_peaks(peak_dir, 2)["cpu_seconds"] >= 0.2


MEASURED_IN_A_PROCESS = """
import json, sys
from rhapsody.backends.execution import _dragon_native_worker as w
ok, (value, observed), _ = w.outcome(lambda: len(b"x" * (128 << 20)), (), {}, measure=True)
print(json.dumps(observed))
"""


@pytest.mark.skipif(not LINUX, reason="RSS is measured on Linux only")
def test_a_pool_call_reports_its_peak_and_its_growth_in_a_process_of_its_own():
    """In the fake, workers are threads of pytest; here the worker's measurement runs in a real
    process, as on Dragon.

    ``host_mem`` is the worker's whole peak during the call, ``host_mem_growth`` the call's
    growth.
    """
    import json
    import subprocess

    out = subprocess.run(
        [sys.executable, "-c", MEASURED_IN_A_PROCESS], capture_output=True, text=True, timeout=60
    )
    observed = json.loads(out.stdout)
    assert observed["approximate"] is True
    growth = observed["host_mem_growth"] * (1 << 30)
    assert growth >= 128 * MIB - SLACK
    assert observed["host_mem"] * (1 << 30) >= growth + 8 * MIB  # the worker's own size


WORKER_WITHOUT_RHAPSODY = """
import sys, cloudpickle
outcome = cloudpickle.loads(sys.stdin.buffer.read())
ok, (value, observed), _ = outcome(len, ([1, 2],), {}, True)
print(value, "started" in observed, any(m.startswith("rhapsody") for m in sys.modules))
"""


def test_the_worker_measures_without_importing_rhapsody(dragon, tmp_path):  # noqa: F811
    """_measure travels by value with the worker's code, as the backend registers both."""
    import subprocess

    cloudpickle.register_pickle_by_value(worker_module)
    cloudpickle.register_pickle_by_value(_measure)
    try:
        blob = cloudpickle.dumps(worker_module.outcome)
    finally:
        cloudpickle.unregister_pickle_by_value(worker_module)
        cloudpickle.unregister_pickle_by_value(_measure)
    out = subprocess.run(
        [sys.executable, "-I", "-c", WORKER_WITHOUT_RHAPSODY],
        input=blob,
        capture_output=True,
        timeout=60,
    )
    assert out.returncode == 0, out.stderr.decode()
    assert out.stdout.decode().split() == ["2", "True", "False"]


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


# ============================================================================
# Review findings: each test fails without its fix
# ============================================================================


async def make(tmp_path, observe=True, workers=1):
    be = DragonNativeExecutionBackend(
        workers_per_node=workers, inline_stdio=False, kill_grace=0.5, observe_peaks=observe
    )
    be._work_dir = str(tmp_path)
    await be
    return be


class ApiError(Exception):
    """An error whose __getattr__ raises KeyError for an unknown field."""

    def __init__(self, payload):
        super().__init__(payload)
        self.payload = payload

    def __getattr__(self, name):
        if name.startswith("__"):
            raise AttributeError(name)
        return self.__dict__["payload"][name]

    def __reduce__(self):
        return (ApiError, (self.payload,))


def raises_api_error():
    raise ApiError({"status": 500})


async def test_a_strange_error_neither_strands_its_task_nor_its_worker(dragon, tmp_path):  # noqa: F811
    be = await make(tmp_path)  # one worker per node
    try:
        bad, after = (
            fn_task(raises_api_error, placement={"host": "n0"}),
            fn_task(square, 4, placement={"host": "n0"}),
        )
        assert await run(be, [bad], timeout=5) == ["FAILED"]
        assert isinstance(bad["exception"], ApiError) and "observed" in bad
        assert await run(be, [after], timeout=5) == ["DONE"]  # the worker was fed
    finally:
        await be.shutdown()


async def test_sys_exit_in_the_pool_reports_its_measurement(dragon, tmp_path):  # noqa: F811
    be = await make(tmp_path)
    try:
        task = fn_task(exits)
        assert await run(be, [task]) == ["FAILED"]
    finally:
        await be.shutdown()
    assert isinstance(task["exception"], RuntimeError) and "observed" in task


async def test_the_task_environment_does_not_break_the_launcher(dragon, tmp_path):  # noqa: F811
    be = await make(tmp_path)
    try:
        env = {"PYTHONHOME": "/nonexistent"}
        task = exe_task(
            "/bin/echo", "hi", task_backend_specific_kwargs={"process_template": {"env": env}}
        )
        assert await run(be, [task]) == ["DONE"]
    finally:
        await be.shutdown()
    assert task["observed"]["started"] <= task["observed"]["ended"]


async def test_a_reused_uid_gets_a_record_directory_of_its_own(dragon, tmp_path):  # noqa: F811
    be = await make(tmp_path)
    try:
        first = be.build_task(exe_task("/bin/echo", "x", uid="same"))
        second = be.build_task(exe_task("/bin/echo", "x", uid="same"))
    finally:
        await be.shutdown()
    assert first.peak_dir != second.peak_dir


async def test_unsafe_uids_keep_the_work_directory(dragon, tmp_path):  # noqa: F811
    keep = tmp_path / "keep.txt"
    keep.write_text("x")
    be = await make(tmp_path)
    try:
        uids = ("..", str(tmp_path / "absolute-uid"), "a/../../b")
        tasks = [exe_task("/bin/echo", "x") for _ in uids]
        for task, uid in zip(tasks, uids):
            task["uid"] = uid
        assert await run(be, tasks) == ["DONE"] * 3
    finally:
        await be.shutdown()
    assert keep.exists()
    assert all(task["observed"]["started"] <= task["observed"]["ended"] for task in tasks)


async def test_a_relative_work_dir_and_a_template_cwd_keep_the_records(
    dragon,
    tmp_path,
    monkeypatch,  # noqa: F811
):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "taskdir").mkdir()
    be = await make(tmp_path)
    be._work_dir = "run"
    try:
        tmpl = {"process_template": {"cwd": str(tmp_path / "taskdir")}}
        task = alloc(8)
        task["task_backend_specific_kwargs"] = tmpl
        assert await run(be, [task]) == ["DONE"]
    finally:
        await be.shutdown()
    assert task["observed"]["host_mem"] > 0
    assert not list((tmp_path / "taskdir").rglob("*.json"))


async def test_a_bad_record_costs_the_observation_not_the_task(
    dragon,
    tmp_path,
    monkeypatch,  # noqa: F811
):
    from rhapsody.backends.execution import dragon_native

    def broken(records, ranks):
        raise KeyError("host_mem")

    monkeypatch.setattr(dragon_native._peak, "combine", broken)
    be = await make(tmp_path)
    try:
        task = exe_task("/bin/echo", "x")
        assert await run(be, [task], timeout=5) == ["DONE"]
    finally:
        await be.shutdown()
    assert "observed" not in task


async def test_records_stay_gone_once_read(dragon, tmp_path):  # noqa: F811
    be = await make(tmp_path)
    try:
        tasks = [alloc(1), alloc(1, code=2)]
        assert await run(be, tasks) == ["DONE", "FAILED"]
        (root,) = tmp_path.glob("rhapsody-peaks.*")  # made when the tasks were built
        assert os.listdir(root) == []
    finally:
        await be.shutdown()


# ============================================================================
# Tasks that die with their process, and what workers say about themselves
# ============================================================================


def assert_killed(observed, before, killed, code, timed_out=False):
    assert observed["host_mem"] is None and observed["gpu_mem"] is None
    assert before <= observed["started"] <= observed["ended"] <= time.time()
    assert (observed["killed"], observed["exit_code"]) == (killed, code)
    assert observed.get("timed_out", False) is timed_out


async def test_a_task_whose_worker_died_reports_what_is_known(dragon, tmp_path):  # noqa: F811
    import signal

    be = await make(tmp_path)
    try:
        before = time.time()
        task = fn_task(blocked, placement={"host": "n0"})
        await be.submit_tasks([task])
        rec = Recorder(be)
        await until(lambda: be._tasks[task["uid"]].worker is not None)
        be._tasks[task["uid"]].worker.proc._exit(-signal.SIGKILL)  # say, the OOM killer
        assert (await rec.wait(task["uid"], 10))[0] == "FAILED"
    finally:
        await be.shutdown()
    assert_killed(task["observed"], before, "SIGKILL", -signal.SIGKILL)
    assert task["observed"]["host"] == socket.gethostname()  # the worker's own name


async def test_a_pool_task_that_timed_out_reports_what_is_known(dragon, tmp_path):  # noqa: F811
    import signal

    be = await make(tmp_path)
    try:
        before = time.time()
        task = fn_task(blocked, task_backend_specific_kwargs={"timeout": 0.2})
        assert await run(be, [task], timeout=10) == ["FAILED"]
    finally:
        await be.shutdown()
    assert isinstance(task["exception"], TimeoutError)
    assert_killed(task["observed"], before, "SIGKILL", -signal.SIGKILL, timed_out=True)


async def test_a_process_task_killed_before_its_record_reports_what_is_known(dragon, tmp_path):  # noqa: F811
    """In the fake, a function process is a thread: SIGTERM ends it without its record."""
    import signal

    be = await make(tmp_path)
    try:
        before = time.time()
        kwargs = {"process_template": {}, "timeout": 0.3}
        task = fn_task(blocked, task_backend_specific_kwargs=kwargs)
        assert await run(be, [task], timeout=10) == ["FAILED"]
    finally:
        await be.shutdown()
    assert_killed(task["observed"], before, "SIGTERM", -signal.SIGTERM, timed_out=True)


async def test_without_observe_peaks_a_dead_workers_task_reports_nothing(dragon, tmp_path):  # noqa: F811
    be = await make(tmp_path, observe=False)
    try:
        task = fn_task(blocked, task_backend_specific_kwargs={"timeout": 0.2})
        assert await run(be, [task], timeout=10) == ["FAILED"]
    finally:
        await be.shutdown()
    assert "observed" not in task


async def test_the_client_keeps_what_ready_says_and_numbers_each_workers_tasks(
    dragon,  # noqa: F811
    tmp_path,
):
    be = await make(tmp_path)
    try:
        workers = list(be._workers.values())
        assert all(w.pid == os.getpid() for w in workers)  # the fake's workers are threads
        assert all(w.node == socket.gethostname() for w in workers)
        if LINUX:
            assert all(w.oom_score_adj >= 500 for w in workers)
        tasks = [fn_task(square, i, placement={"host": "n0"}) for i in range(3)]
        assert await run(be, tasks) == ["DONE"] * 3
        (w,) = [w for w in be._workers.values() if w.host == "n0"]
        assert w.sent == 3
    finally:
        await be.shutdown()


async def test_the_relay_keeps_what_workers_say_and_an_unsent_task_gives_its_number_back(
    dragon,  # noqa: F811
    tmp_path,
    pool_full,  # noqa: F811
    monkeypatch,
):
    """READY, DONE with the worker's number, and ``(value, observed)`` cross the relay intact.

    The relay could not put the first task on a full pool twice: the worker never saw those sends,
    so the client takes their numbers back, and both still count the task as the worker's first.
    """
    be = await make(tmp_path)
    try:
        (w,) = [w for w in be._workers.values() if w.host == "n0"]
        assert (w.pid, w.node) == (os.getpid(), socket.gethostname())
        failures = iter(range(2))
        monkeypatch.setattr(
            w.inbox, "put", full_until(w.inbox, lambda: next(failures, None) is not None)
        )
        numbers = []
        on_done = be._on_done

        def record(msg, completions):
            rec = be._workers[msg[1]].task
            numbers.append((msg[2], msg[-1], rec.seq if rec is not None else None))
            on_done(msg, completions)

        monkeypatch.setattr(be, "_on_done", record)
        tasks = [fn_task(square, i, placement={"host": "n0"}) for i in range(3)]
        assert await run(be, tasks) == ["DONE"] * 3
        assert [t["return_value"] for t in tasks] == [0, 1, 4]
        assert numbers == [(t["uid"], i, i) for i, t in enumerate(tasks, 1)]
        assert w.sent == 3
        for task in tasks:  # the measurement came back with each value
            assert all(task["observed"].get(k) is not None for k in WINDOW)
            if LINUX:
                assert task["observed"]["approximate"] is True
    finally:
        await be.shutdown()


def test_the_worker_numbers_its_tasks_and_reports_ready_with_its_pid():
    import queue

    class Results(list):
        def put(self, raw, flush=False):
            self.append(cloudpickle.loads(raw))

    inbox, results = queue.Queue(), Results()
    body = cloudpickle.dumps(("tok", square, (3,), {}, None, None, False, False))
    for uid in ("a", "b"):
        inbox.put(cloudpickle.dumps((uid, body)))
    inbox.put(cloudpickle.dumps(("c", b"not a pickle")))
    inbox.put(None)
    worker_module.worker_main(7, inbox, results)
    ready, *done = results
    assert ready == (worker_module.READY, 7, socket.gethostname(), os.getpid(), None)
    assert [(m[2], m[3], m[-1]) for m in done] == [("a", True, 1), ("b", True, 2), ("c", False, 3)]


RAISE_OOM_SCORE = """
from rhapsody.backends.execution._dragon_native_worker import raise_oom_score
print(raise_oom_score(500), raise_oom_score(100), open("/proc/self/oom_score_adj").read().strip())
"""


@pytest.mark.skipif(not LINUX, reason="oom_score_adj is Linux's")
def test_a_worker_raises_its_oom_score_and_never_lowers_it():
    import subprocess

    out = subprocess.run(
        [sys.executable, "-c", RAISE_OOM_SCORE], capture_output=True, text=True, timeout=60
    )
    first, second, now = (int(v) for v in out.stdout.split())
    assert first == second == now >= 500, out.stderr
