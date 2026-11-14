"""DragonExecutionBackend ``observe_peaks``, tested without a Dragon install.

With ``observe_peaks=True``, executable tasks run under the peak launcher, whose per-process records
the monitor thread combines into ``task["observed"]``, unless the Dragon in use reports each
process's rusage with its Batch result (``Task.rusage``), which is then read instead. Native function
tasks are wrapped in ``_peak.measured_call``. These tests swap Batch and ProcessTemplate for fakes.
One fake Batch runs each process template for real (reaping it with ``wait4`` when it plays a Dragon
with rusage), so the launcher, the monitor thread and the delivery to the callback are exercised end
to end.
"""

import asyncio
import inspect
import json
import os
import queue
import subprocess
import sys
from unittest.mock import patch

import pytest

from rhapsody.backends.execution import _measure
from rhapsody.backends.execution import _peak
from rhapsody.backends.execution import dragon as dragon_module
from rhapsody.backends.execution.dragon import PEAK_LAUNCHER
from rhapsody.backends.execution.dragon import DragonExecutionBackend
from rhapsody.backends.execution.dragon import _observed_from_rusage
from rhapsody.backends.execution.dragon import _read_peaks

MIB = 1 << 20
# Linux >= 6.2 keeps RSS in per-CPU counters, so a peak can read a few MiB under the truth.
SLACK = 16 * MIB


class FakeProcessTemplate:
    """Stands in for dragon.native.process.ProcessTemplate; keeps what it was given."""

    def __init__(self, target, args=None, kwargs=None, **extra):
        self.target = target
        self.args = args
        self.kwargs = kwargs
        self.extra = extra


class FakeBatchTask:
    def __init__(self, tuid):
        self.uid = tuid
        self.result = self.error = None
        self.traceback = None
        self.stdout_path = self.stderr_path = None
        self.rusage = None

    def get(self, block=True):
        if self.error is not None:
            raise self.error
        return self.result

    def get_stdout(self, block=False):
        return ""

    def get_stderr(self, block=False):
        return ""


class FakeOptionsProxy:
    def __init__(self, batch):
        self._batch = batch

    def function(self, target, *args, **kwargs):
        return self._batch.submit("function", target, args, kwargs)

    def process(self, template):
        return self._batch.submit("process", template)

    def job(self, templates):
        return self._batch.submit("job", templates)


class FakeBatch:
    """Records submissions; with ``run=True`` runs them and reports them through poll()."""

    num_workers = 1
    num_managers = 1

    def __init__(self, run=False, rusage=False):
        self.run = run
        self.rusage = rusage
        self.calls = []
        self.done = queue.Queue()

    def options(self, **kwargs):
        return FakeOptionsProxy(self)

    def submit(self, kind, *payload):
        self.calls.append((kind, payload))
        task = FakeBatchTask(f"tuid-{len(self.calls)}")
        if self.run:
            try:
                task.result, task.rusage = self._execute(kind, payload, self.rusage)
            except Exception as exc:
                task.error = exc
            self.done.put(task.uid)
        return task

    @staticmethod
    def _execute(kind, payload, with_rusage=False):
        """Run a submission; return (result, rusage) as a Dragon with Task.rusage would.

        A function's target, arguments, result and exception go through cloudpickle, as Batch sends
        them to a pool worker and back.
        """
        if kind == "function":
            import cloudpickle

            target, args, kwargs = cloudpickle.loads(cloudpickle.dumps(payload))
            try:
                result = target(*args, **kwargs)
            except Exception as exc:
                raise cloudpickle.loads(cloudpickle.dumps(exc)) from None
            return cloudpickle.loads(cloudpickle.dumps(result)), None
        templates = [(1, payload[0])] if kind == "process" else payload[0]
        codes, rusages = [], []
        for nranks, t in templates:
            for _ in range(nranks):
                argv = [str(t.target), *(str(a) for a in t.args)]
                child = subprocess.Popen(argv)
                _, status, ru = os.wait4(child.pid, 0)
                child.returncode = os.waitstatus_to_exitcode(status)
                codes.append(child.returncode)
                rusages.append(
                    {"max_rss": ru.ru_maxrss * _peak.MAXRSS_UNIT, "utime": 0, "stime": 0}
                )
        if kind == "process":
            return codes[0], (rusages[0] if with_rusage else None)
        return codes, (rusages if with_rusage else None)

    def poll(self, timeout=None):
        try:
            return self.done.get(timeout=timeout)
        except queue.Empty:
            return None

    def join(self, timeout=None):
        pass

    def destroy(self, timeout=None):
        pass

    def log_dir(self):
        raise RuntimeError("task_logs disabled")


class FakeTaskCancelledError(Exception):
    """Stands in for dragon.workflows.batch.TaskCancelledError."""


def _make_backend(batch, tmp_path, observe_peaks=True, dragon_rusage=False, peak_source="auto"):
    with (
        patch.object(dragon_module, "Batch", return_value=batch),
        patch.object(dragon_module, "ProcessTemplate", FakeProcessTemplate),
        patch.object(dragon_module, "_batch_reports_rusage", return_value=dragon_rusage),
    ):
        backend = DragonExecutionBackend(observe_peaks=observe_peaks, peak_source=peak_source)
    backend._work_dir = str(tmp_path)
    return backend


@pytest.fixture
def backend(tmp_path):
    """Backend on a recording FakeBatch, ProcessTemplate faked while the test runs."""
    batch = FakeBatch()
    be = _make_backend(batch, tmp_path)
    with patch.object(dragon_module, "ProcessTemplate", FakeProcessTemplate):
        yield be


def _exe(uid="t1", **extra):
    return {"uid": uid, "executable": "/bin/echo", "arguments": ["hi", 3], **extra}


def _peak_root(backend):
    return os.path.join(backend._work_dir, f"rhapsody-peaks.{backend._peak_tag}")


def _launcher_args(backend, uid):
    peak_dir = backend._task_registry[uid]["peak_dir"]
    return ("-I", PEAK_LAUNCHER, "--out-dir", peak_dir, "--")


# ============================================================================
# build_task: what goes to Dragon
# ============================================================================


@pytest.mark.asyncio
async def test_executable_runs_under_launcher(backend):
    await backend.build_task(_exe())
    (kind, (template,)) = backend.batch.calls[0]
    assert kind == "process"
    assert template.target == sys.executable
    assert template.args == (*_launcher_args(backend, "t1"), "/bin/echo", "hi", 3)
    reg = backend._task_registry["t1"]
    assert os.path.dirname(reg["peak_dir"]) == _peak_root(backend)
    assert os.path.basename(reg["peak_dir"]).startswith("t1.") and os.path.isdir(reg["peak_dir"])
    assert reg["peak_ranks"] == 1


@pytest.mark.asyncio
async def test_executable_with_process_template_keeps_user_settings(backend):
    task = _exe(task_backend_specific_kwargs={"process_template": {"cwd": "/scratch"}})
    await backend.build_task(task)
    (kind, (template,)) = backend.batch.calls[0]
    assert kind == "process"
    assert template.target == sys.executable
    assert template.args[-3:] == ("/bin/echo", "hi", 3)
    assert template.extra == {"cwd": "/scratch"}


@pytest.mark.asyncio
async def test_job_ranks_all_run_under_launcher(backend):
    task = _exe(task_backend_specific_kwargs={"process_templates": [(2, {}), (3, {"cwd": "/x"})]})
    await backend.build_task(task)
    (kind, (templates,)) = backend.batch.calls[0]
    assert kind == "job"
    assert [n for n, _ in templates] == [2, 3]
    for _, t in templates:
        assert t.target == sys.executable
        assert t.args[: len(_launcher_args(backend, "t1"))] == _launcher_args(backend, "t1")
    assert backend._task_registry["t1"]["peak_ranks"] == 5


@pytest.mark.asyncio
async def test_native_function_is_wrapped_in_measured_call(backend):
    def f(a, b=0):
        return a + b

    await backend.build_task({"uid": "f1", "function": f, "args": [1], "kwargs": {"b": 2}})
    (kind, (target, args, kwargs)) = backend.batch.calls[0]
    assert kind == "function"
    assert target is _measure.measure
    assert args == (f, (1,), {"b": 2})
    assert kwargs == {}
    assert backend._task_registry["f1"]["measured"] is True
    assert backend._task_registry["f1"]["peak_dir"] is None


@pytest.mark.asyncio
async def test_function_in_process_template_is_not_measured(backend):
    def f():
        return 0

    task = {"uid": "f1", "function": f, "task_backend_specific_kwargs": {"process_template": {}}}
    await backend.build_task(task)
    (kind, (template,)) = backend.batch.calls[0]
    assert template.target is f
    reg = backend._task_registry["f1"]
    assert reg["peak_dir"] is None and reg["measured"] is False


@pytest.mark.asyncio
async def test_observe_peaks_off_leaves_tasks_alone(tmp_path):
    def f():
        return 0

    be = _make_backend(FakeBatch(), tmp_path, observe_peaks=False)
    with patch.object(dragon_module, "ProcessTemplate", FakeProcessTemplate):
        await be.build_task(_exe())
        await be.build_task({"uid": "f1", "function": f})
    (_, (template,)), (_, (target, args, _)) = be.batch.calls
    assert template.target == "/bin/echo" and template.args == ("hi", 3)
    assert target is f and args == ()
    assert be._peak_roots == set()


# ============================================================================
# _read_peaks: combining launcher records
# ============================================================================


def _write(peak_dir, name, **rec):
    os.makedirs(peak_dir, exist_ok=True)
    with open(os.path.join(peak_dir, name), "w") as f:
        json.dump(rec, f)


def test_read_peaks_single_process(tmp_path):
    d = str(tmp_path / "t1")
    _write(d, "h.1.json", host_mem=1.5, gpu_mem=None, gpu_mem_devices={})
    assert _read_peaks(d, 1) == {"host_mem": 1.5, "host_mem_max": 1.5, "ranks": 1, "gpu_mem": None}
    assert not os.path.exists(d)


def test_read_peaks_job_sums_ranks_and_shared_gpus(tmp_path):
    d = str(tmp_path / "t1")
    _write(d, "a.1.json", host_mem=1.0, gpu_mem_devices={"GPU-a": 2.0})
    _write(d, "a.2.json", host_mem=2.0, gpu_mem_devices={"GPU-a": 3.0, "GPU-b": 1.0})
    _write(d, "b.1.json", host_mem=0.5, gpu_mem_devices={"GPU-c": 4.0})
    assert _read_peaks(d, 3) == {"host_mem": 3.5, "host_mem_max": 2.0, "ranks": 3, "gpu_mem": 5.0}


def test_read_peaks_combines_the_ranks_windows_and_hosts(tmp_path):
    d = str(tmp_path / "t1")
    _write(d, "a.1.json", host_mem=1.0, started=5.0, ended=9.0, cpu_seconds=3.0, host="a")
    _write(d, "b.1.json", host_mem=1.0, started=4.5, ended=8.0, cpu_seconds=2.5, host="b")
    observed = _read_peaks(d, 2)
    assert (observed["started"], observed["ended"]) == (4.5, 9.0)
    assert observed["cpu_seconds"] == 5.5
    assert observed["hosts"] == ["a", "b"] and "host" not in observed
    d = str(tmp_path / "t2")
    _write(d, "a.1.json", host_mem=1.0, started=5.0, host="a")
    _write(d, "a.2.json", host_mem=1.0, host="a")  # an older launcher's record
    observed = _read_peaks(d, 2)
    assert "started" not in observed and observed["host"] == "a"


def test_read_peaks_incomplete_or_missing_is_none(tmp_path):
    d = str(tmp_path / "t1")
    assert _read_peaks(d, 1) is None
    _write(d, "a.1.json", host_mem=1.0)
    assert _read_peaks(d, 2) is None
    assert not os.path.exists(d)


def test_read_peaks_skips_temp_and_corrupt_files(tmp_path):
    d = str(tmp_path / "t1")
    _write(d, "a.1.json", host_mem=1.0)
    _write(d, ".a.2.json.tmp", host_mem=9.0)
    with open(os.path.join(d, "a.3.json"), "w") as f:
        f.write("{not json")
    assert _read_peaks(d, 1) == {"host_mem": 1.0, "host_mem_max": 1.0, "ranks": 1, "gpu_mem": None}


# ============================================================================
# Monitor thread to callback, with the real launcher
# ============================================================================


async def _run(backend, tasks, timeout=60.0):
    """Submit ``tasks``; return {uid: (state, task)} once each reached a final state."""
    loop = asyncio.get_running_loop()
    finals = {}
    all_done = asyncio.Event()

    def callback(task, state):
        assert loop is asyncio.get_running_loop()
        if state in ("DONE", "FAILED", "CANCELED"):
            finals[task["uid"]] = (state, task)
            if len(finals) == len(tasks):
                all_done.set()

    backend._callback_func = callback
    # Looked up by the monitor thread when a task raises, so patched while it runs.
    with patch.object(dragon_module, "TaskCancelledError", FakeTaskCancelledError):
        try:
            await backend.submit_tasks(tasks)
            await asyncio.wait_for(all_done.wait(), timeout)
        finally:
            await backend.shutdown()
    return finals


def _alloc_task(uid, mib, code=0, **extra):
    script = f"import sys; b = b'x' * ({mib} << 20); sys.exit({code})"
    return {"uid": uid, "executable": sys.executable, "arguments": ["-c", script], **extra}


@pytest.mark.asyncio
async def test_observed_reaches_callback_for_done_and_failed(tmp_path):
    be = _make_backend(FakeBatch(run=True), tmp_path)
    with patch.object(dragon_module, "ProcessTemplate", FakeProcessTemplate):
        finals = await _run(be, [_alloc_task("ok", 200), _alloc_task("bad", 100, code=4)])

    state, task = finals["ok"]
    assert state == "DONE" and task["return_value"] == 0
    assert 200 * MIB - SLACK <= task["observed"]["host_mem"] * (1 << 30) <= 264 * MIB
    state, task = finals["bad"]
    # Exit codes pass through to _deliver_batch, which reports FAILED with exit_code
    assert state == "FAILED" and task["exit_code"] == 4
    assert 100 * MIB - SLACK <= task["observed"]["host_mem"] * (1 << 30) <= 164 * MIB
    # Records are read and removed; shutdown removes the root.
    assert os.listdir(tmp_path) == []


@pytest.mark.asyncio
async def test_job_observed_is_the_sum_of_its_ranks(tmp_path):
    be = _make_backend(FakeBatch(run=True), tmp_path)
    job = _alloc_task("job", 100, task_backend_specific_kwargs={"process_templates": [(3, {})]})
    with patch.object(dragon_module, "ProcessTemplate", FakeProcessTemplate):
        finals = await _run(be, [job])
    state, task = finals["job"]
    assert state == "DONE" and task["return_value"] == [0, 0, 0]
    # three ranks: their buffers plus a Python interpreter each
    assert 300 * MIB - SLACK <= task["observed"]["host_mem"] * (1 << 30) <= 3 * (100 + 40) * MIB


@pytest.mark.asyncio
async def test_measured_function_value_is_unwrapped(tmp_path):
    def hold(mib):
        return len(b"x" * (mib * MIB))

    def boom():
        raise RuntimeError("boom")

    be = _make_backend(FakeBatch(run=True), tmp_path)
    tasks = [{"uid": "ok", "function": hold, "args": [256]}, {"uid": "bad", "function": boom}]
    finals = await _run(be, tasks)

    state, task = finals["ok"]
    assert state == "DONE" and task["return_value"] == 256 * MIB
    assert task["observed"]["started"] <= task["observed"]["ended"]
    if sys.platform.startswith("linux"):
        assert task["observed"]["approximate"] is True
        assert task["observed"]["host_mem"] * (1 << 30) >= 256 * MIB - SLACK
    else:
        assert task["observed"]["host_mem"] is None
    assert task["observed"]["cpu_seconds"] >= 0
    state, task = finals["bad"]
    assert state == "FAILED" and isinstance(task["exception"], RuntimeError)
    # The call raised: its measurement came with the exception
    assert task["observed"]["started"] <= task["observed"]["ended"]
    assert task["observed"]["cpu_seconds"] >= 0


# ============================================================================
# Peaks from Dragon's Batch rusage (a Dragon with Task.rusage), launcher fallback
# ============================================================================


def test_stock_dragon_has_no_batch_rusage():
    # Without Dragon (or with stock dragonhpc 0.14.1) the launcher is the source.
    if dragon_module.dragon is None:
        assert dragon_module._batch_reports_rusage() is False


@pytest.mark.asyncio
async def test_auto_reads_dragon_rusage_instead_of_launching(tmp_path):
    def f():
        return 0

    be = _make_backend(FakeBatch(), tmp_path, dragon_rusage=True)
    tmpl = {"uid": "f2", "function": f, "task_backend_specific_kwargs": {"process_template": {}}}
    with patch.object(dragon_module, "ProcessTemplate", FakeProcessTemplate):
        await be.build_task(_exe())
        await be.build_task({"uid": "f1", "function": f})
        await be.build_task(tmpl)
    (_, (template,)), (_, (target, _, _)), (_, (ftemplate,)) = be.batch.calls
    assert template.target == "/bin/echo" and template.args == ("hi", 3)
    assert be._task_registry["t1"]["dragon_rusage"] is True
    assert be._task_registry["t1"]["peak_dir"] is None
    # Native functions are still measured in their pool worker.
    assert target is _measure.measure and be._task_registry["f1"]["dragon_rusage"] is False
    # A function in its own process gets Dragon's rusage too.
    assert ftemplate.target is f and be._task_registry["f2"]["dragon_rusage"] is True
    assert be._peak_roots == set()


@pytest.mark.asyncio
async def test_peak_source_launcher_overrides_dragon_rusage(tmp_path):
    be = _make_backend(FakeBatch(), tmp_path, dragon_rusage=True, peak_source="launcher")
    with patch.object(dragon_module, "ProcessTemplate", FakeProcessTemplate):
        await be.build_task(_exe())
    (_, (template,)) = be.batch.calls[0]
    assert template.target == sys.executable
    assert be._task_registry["t1"]["dragon_rusage"] is False


def test_peak_source_is_checked(tmp_path):
    with pytest.raises(ValueError, match="needs a Dragon"):
        _make_backend(FakeBatch(), tmp_path, dragon_rusage=False, peak_source="dragon")
    with pytest.raises(ValueError, match="peak_source must be one of"):
        _make_backend(FakeBatch(), tmp_path, peak_source="nvml")
    # Without observe_peaks the source does not matter.
    _make_backend(FakeBatch(), tmp_path, observe_peaks=False, peak_source="dragon")


def test_observed_from_rusage():
    one = {"max_rss": 1 << 30, "utime": 0.1, "stime": 0.0}
    assert _observed_from_rusage(one) == {
        "host_mem": 1.0,
        "host_mem_max": 1.0,
        "ranks": 1,
        "gpu_mem": None,
        "cpu_seconds": 0.1,
    }
    three = _observed_from_rusage([one, one, one])
    assert three["host_mem"] == 3.0 and three["cpu_seconds"] == pytest.approx(0.3)
    # Dragon's rusage has no floor to subtract: each process counts its own (decision-4, D1)
    assert (three["host_mem_max"], three["ranks"]) == (1.0, 3)
    # A Dragon that reports when Local Services spawned and reaped each process
    timed = [{**one, "start": 10.0, "end": 12.0}, {**one, "start": 9.5, "end": 11.0}]
    assert _observed_from_rusage(timed)["started"] == 9.5
    assert _observed_from_rusage(timed)["ended"] == 12.0
    assert "started" not in _observed_from_rusage([one, timed[0]])
    assert _observed_from_rusage(None) is None
    assert _observed_from_rusage([]) is None
    assert _observed_from_rusage([one, None]) is None
    assert _observed_from_rusage({"max_rss": None}) is None


@pytest.mark.asyncio
async def test_dragon_rusage_reaches_callback(tmp_path):
    be = _make_backend(FakeBatch(run=True, rusage=True), tmp_path, dragon_rusage=True)
    job = _alloc_task("job", 200, task_backend_specific_kwargs={"process_templates": [(2, {})]})
    with patch.object(dragon_module, "ProcessTemplate", FakeProcessTemplate):
        finals = await _run(be, [_alloc_task("ok", 400), _alloc_task("bad", 300, code=5), job])

    # wait4 here, as in Dragon: a peak includes what the child inherited from its parent at fork.
    state, task = finals["ok"]
    assert state == "DONE" and task["observed"]["gpu_mem"] is None
    assert task["observed"]["host_mem"] * (1 << 30) >= 400 * MIB - SLACK
    state, task = finals["bad"]
    assert state == "FAILED" and task["observed"]["host_mem"] * (1 << 30) >= 300 * MIB - SLACK
    state, task = finals["job"]
    assert state == "DONE" and task["observed"]["host_mem"] * (1 << 30) >= 400 * MIB - SLACK
    assert os.listdir(tmp_path) == []


# ============================================================================
# Review findings (MTU-51): each test fails without its fix
# ============================================================================


def test_positional_arguments_keep_their_v070_meaning():
    params = inspect.signature(DragonExecutionBackend.__init__).parameters
    positional = [n for n, p in params.items() if p.kind is p.POSITIONAL_OR_KEYWORD]
    assert positional == ["self", "batch_kwargs", "name", "size_pool_to_affinity", "pmi"]
    assert (
        params["observe_peaks"].kind is params["peak_source"].kind is inspect.Parameter.KEYWORD_ONLY
    )
    params = inspect.signature(DragonExecutionBackend.create).parameters
    positional = [n for n, p in params.items() if p.kind is p.POSITIONAL_OR_KEYWORD]
    assert positional == ["batch_kwargs", "pmi"]


@pytest.mark.asyncio
async def test_a_pool_worker_measures_without_importing_rhapsody(backend):
    """The measured target travels by value: unpickled where rhapsody is absent, it works."""
    import cloudpickle

    await backend.build_task({"uid": "f1", "function": len, "args": [[1, 2, 3]]})
    (_, (target, args, kwargs)) = backend.batch.calls[0]
    blob = cloudpickle.dumps((target, args, kwargs))
    script = (
        "import sys, cloudpickle\n"
        "target, args, kwargs = cloudpickle.loads(sys.stdin.buffer.read())\n"
        "value, observed = target(*args, **kwargs)\n"
        "print(value, 'started' in observed, 'rhapsody' in sys.modules)\n"
    )
    out = subprocess.run(
        [sys.executable, "-I", "-c", script], input=blob, capture_output=True, timeout=60
    )
    assert out.returncode == 0, out.stderr.decode()
    assert out.stdout.decode().split() == ["3", "True", "False"]


@pytest.mark.asyncio
async def test_a_relative_work_dir_and_a_task_cwd_keep_their_records(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    be = _make_backend(FakeBatch(run=True), tmp_path)
    be._work_dir = "work"
    (tmp_path / "elsewhere").mkdir()
    task = _alloc_task("t1", 8, task_backend_specific_kwargs={"process_template": {}})
    with patch.object(dragon_module, "ProcessTemplate", FakeProcessTemplate):
        await be.build_task(task)
    peak_dir = be._task_registry["t1"]["peak_dir"]
    assert os.path.isabs(peak_dir) and peak_dir.startswith(str(tmp_path / "work"))


@pytest.mark.asyncio
async def test_unsafe_uids_stay_inside_the_record_root(backend):
    for uid in ("..", "/etc", "a/../../b"):
        await backend.build_task(_exe(uid=uid))
        peak_dir = backend._task_registry[uid]["peak_dir"]
        assert os.path.dirname(peak_dir) == os.path.abspath(_peak_root(backend))


@pytest.mark.asyncio
async def test_a_bad_record_costs_the_observation_not_the_task(tmp_path):
    be = _make_backend(FakeBatch(run=True), tmp_path)
    with patch.object(dragon_module, "_read_peaks", side_effect=KeyError("host_mem")):
        with patch.object(dragon_module, "ProcessTemplate", FakeProcessTemplate):
            finals = await _run(be, [_alloc_task("ok", 8)])
    state, task = finals["ok"]
    assert state == "DONE" and "observed" not in task


@pytest.mark.asyncio
async def test_observe_peaks_off_runs_nothing_extra_end_to_end(tmp_path):
    def double(x):
        return 2 * x

    be = _make_backend(FakeBatch(run=True), tmp_path, observe_peaks=False)
    with patch.object(dragon_module, "ProcessTemplate", FakeProcessTemplate):
        finals = await _run(
            be, [_alloc_task("exe", 8), {"uid": "f", "function": double, "args": [4]}]
        )
    assert finals["f"][1]["return_value"] == 8
    assert all("observed" not in task for _, task in finals.values())
    assert all(
        PEAK_LAUNCHER not in map(str, getattr(p[0], "args", None) or ()) for _, p in be.batch.calls
    )
    assert not os.path.exists(_peak_root(be))
