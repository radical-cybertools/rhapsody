"""DragonExecutionBackend ``observe_peaks``, tested without a Dragon install.

With ``observe_peaks=True``, executable tasks run under the peak launcher, whose per-process records
the monitor thread combines into ``task["observed"]``; native function tasks are wrapped in
``_peak.measured_call``. These tests swap Batch and ProcessTemplate for fakes. One fake Batch runs
each process template for real, so the launcher, the monitor thread and the delivery to the
callback are exercised end to end.
"""

import asyncio
import json
import os
import queue
import subprocess
import sys
from unittest.mock import patch

import pytest

from rhapsody.backends.execution import _peak
from rhapsody.backends.execution import dragon as dragon_module
from rhapsody.backends.execution.dragon import PEAK_LAUNCHER
from rhapsody.backends.execution.dragon import DragonExecutionBackend
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

    def __init__(self, run=False):
        self.run = run
        self.calls = []
        self.done = queue.Queue()

    def options(self, **kwargs):
        return FakeOptionsProxy(self)

    def submit(self, kind, *payload):
        self.calls.append((kind, payload))
        task = FakeBatchTask(f"tuid-{len(self.calls)}")
        if self.run:
            try:
                task.result = self._execute(kind, payload)
            except Exception as exc:
                task.error = exc
            self.done.put(task.uid)
        return task

    @staticmethod
    def _execute(kind, payload):
        if kind == "function":
            target, args, kwargs = payload
            return target(*args, **kwargs)
        templates = [(1, payload[0])] if kind == "process" else payload[0]
        codes = []
        for nranks, t in templates:
            for _ in range(nranks):
                argv = [str(t.target), *(str(a) for a in t.args)]
                codes.append(subprocess.run(argv, timeout=60).returncode)
        return codes[0] if kind == "process" else codes

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


def _make_backend(batch, tmp_path, observe_peaks=True):
    with (
        patch.object(dragon_module, "Batch", return_value=batch),
        patch.object(dragon_module, "ProcessTemplate", FakeProcessTemplate),
    ):
        backend = DragonExecutionBackend(observe_peaks=observe_peaks)
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


def _peak_dir(backend, uid):
    return os.path.join(backend._work_dir, f"rhapsody-peaks.{backend._peak_tag}", uid)


def _launcher_args(backend, uid):
    return (PEAK_LAUNCHER, "--out-dir", _peak_dir(backend, uid), "--")


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
    assert reg["peak_dir"] == _peak_dir(backend, "t1")
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
    assert target is _peak.measured_call
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
    assert _read_peaks(d, 1) == {"host_mem": 1.5, "gpu_mem": None}
    assert not os.path.exists(d)


def test_read_peaks_job_sums_ranks_and_shared_gpus(tmp_path):
    d = str(tmp_path / "t1")
    _write(d, "a.1.json", host_mem=1.0, gpu_mem_devices={"GPU-a": 2.0})
    _write(d, "a.2.json", host_mem=2.0, gpu_mem_devices={"GPU-a": 3.0, "GPU-b": 1.0})
    _write(d, "b.1.json", host_mem=0.5, gpu_mem_devices={"GPU-c": 4.0})
    assert _read_peaks(d, 3) == {"host_mem": 3.5, "gpu_mem": 5.0}


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
    assert _read_peaks(d, 1) == {"host_mem": 1.0, "gpu_mem": None}


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
    assert state == "FAILED" and isinstance(task["exception"], SystemExit)
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
    assert 300 * MIB - SLACK <= task["observed"]["host_mem"] * (1 << 30) <= 492 * MIB


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
    if sys.platform.startswith("linux"):
        assert task["observed"]["approximate"] is True
        assert task["observed"]["host_mem"] * (1 << 30) >= 256 * MIB - SLACK
    else:
        assert "observed" not in task
    state, task = finals["bad"]
    assert state == "FAILED" and isinstance(task["exception"], RuntimeError)
    assert "observed" not in task
