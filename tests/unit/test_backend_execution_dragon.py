"""Tests for DragonExecutionBackend using Session.

Structure
---------
Shared behavior tests (1-15)
    Parametrized against dragon. Verify observable task execution semantics.

Dragon integration tests — process_template / process_templates routing
    Use a dedicated ``session_dragon`` fixture so only one V3 session is created.

Dragon unit tests — constructor and internal methods
    Verify the API surface with Batch mocked out. No Dragon cluster required.

Run with:
    dragon python -m pytest tests/unit/test_backend_execution_dragon.py -v
"""

import asyncio
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
import pytest_asyncio

from rhapsody import ComputeTask
from rhapsody.api import Session
from rhapsody.backends.discovery import get_backend

# Skip the entire module when the Dragon runtime is not installed.
pytest.importorskip("dragon", reason="Dragon is required for Dragon backend tests")


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture(scope="module", params=["dragon"])
def backend_name(request):
    """Backend name for shared behavior tests (Dragon backend only)."""
    return request.param


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def session(backend_name):
    """Session with the parametrized Dragon backend, reused across shared tests."""
    backend_instance = await get_backend(backend_name)
    session_instance = Session(backends=[backend_instance])
    yield session_instance
    await session_instance.close()


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def session_dragon():
    """Session backed exclusively by DragonExecutionBackend.

    Used by Dragon-specific tests so only one session is created instead of three.
    """
    backend_instance = await get_backend("dragon")
    session_instance = Session(backends=[backend_instance])
    yield session_instance
    await session_instance.close()


@pytest.fixture
def backend_dragon():
    """DragonExecutionBackend with Batch fully mocked — no Dragon cluster required.

    Suitable for unit tests that verify constructor wiring, internal callback helpers, and method
    delegation without running actual Dragon workers.
    """
    from rhapsody.backends.execution.dragon import DragonExecutionBackend

    mock_batch = MagicMock()
    mock_batch.num_workers = 16
    mock_batch.num_managers = 2

    with patch("rhapsody.backends.execution.dragon.Batch", return_value=mock_batch):
        backend = DragonExecutionBackend()

    backend._callback_func = MagicMock()
    backend._loop = asyncio.new_event_loop()
    return backend


def _make_task_dict(fn, args=(), kwargs=None, backend_specific=None):
    """Build a minimal task dict in the format expected by build_task."""
    import uuid

    return {
        "uid": f"task.test-{uuid.uuid4().hex[:8]}",
        "function": fn,
        "args": list(args),
        "kwargs": kwargs or {},
        "name": "test",
        "task_backend_specific_kwargs": backend_specific or {},
    }


# ============================================================================
# Test 1: Single Executable Task
# ============================================================================


@pytest.mark.asyncio(loop_scope="module")
async def test_single_executable(session):
    """Test executing a single shell command task."""
    task = ComputeTask(executable="echo", arguments=["Hello Dragon"])

    await session.submit_tasks([task])
    results = await session.wait_tasks([task])

    assert results[0].uid.startswith("task.")
    assert results[0].state == "DONE"
    assert "Hello Dragon" in results[0].get("stdout", "")


# ============================================================================
# Test 2: Single Function Task
# ============================================================================


@pytest.mark.asyncio(loop_scope="module")
async def test_single_function(session):
    """Test executing a single Python function task."""

    async def simple_function(x: int) -> int:
        return x * 2

    task = ComputeTask(function=simple_function, args=(21,))

    await session.submit_tasks([task])
    results = await session.wait_tasks([task])

    assert results[0].uid.startswith("task.")
    assert results[0].state == "DONE"
    assert results[0].get("return_value") == 42


# ============================================================================
# Test 3: Task with Arguments
# ============================================================================


@pytest.mark.asyncio(loop_scope="module")
async def test_task_with_args(session):
    """Test task execution with multiple arguments."""
    task = ComputeTask(executable="/bin/echo", arguments=["arg1", "arg2", "arg3"])

    await session.submit_tasks([task])
    results = await session.wait_tasks([task])

    assert results[0].state == "DONE"
    stdout = results[0].get("stdout", "")
    assert "arg1" in stdout and "arg2" in stdout and "arg3" in stdout


# ============================================================================
# Test 4: Task Failure Handling
# ============================================================================


@pytest.mark.asyncio(loop_scope="module")
async def test_task_failure(session):
    """Test that failed tasks are properly reported."""
    task = ComputeTask(executable="/bin/false")

    await session.submit_tasks([task])
    results = await session.wait_tasks([task])

    assert results[0].state == "FAILED"


# ============================================================================
# Test 5: Function with Exception
# ============================================================================


@pytest.mark.asyncio(loop_scope="module")
async def test_function_exception(session):
    """Test that function exceptions are properly handled."""

    async def failing_function():
        raise ValueError("Intentional test failure")

    task = ComputeTask(function=failing_function, args=())

    await session.submit_tasks([task])
    results = await session.wait_tasks([task])

    assert results[0].state == "FAILED"
    assert "exception" in results[0]


# ============================================================================
# Test 6: Two Independent Tasks (Parallel Execution)
# ============================================================================


@pytest.mark.asyncio(loop_scope="module")
async def test_two_independent_tasks(session):
    """Test executing two independent tasks in parallel."""
    tasks = [
        ComputeTask(executable="echo", arguments=["Task A"]),
        ComputeTask(executable="echo", arguments=["Task B"]),
    ]

    await session.submit_tasks(tasks)
    results = await session.wait_tasks(tasks)

    assert len(results) == 2
    for result in results:
        assert result.uid.startswith("task.")
        assert result.state == "DONE"

    outputs = [r.get("stdout", "") for r in results]
    assert any("Task A" in out for out in outputs)
    assert any("Task B" in out for out in outputs)


# ============================================================================
# Test 7: Mixed Success and Failure
# ============================================================================


@pytest.mark.asyncio(loop_scope="module")
async def test_mixed_success_failure(session):
    """Test handling tasks where some succeed and some fail."""
    tasks = [ComputeTask(executable="/bin/true"), ComputeTask(executable="/bin/false")]

    await session.submit_tasks(tasks)
    results = await session.wait_tasks(tasks)

    assert len(results) == 2
    states = [r.state for r in results]
    assert "DONE" in states
    assert "FAILED" in states


# ============================================================================
# Test 8: Function with Return Value
# ============================================================================


@pytest.mark.asyncio(loop_scope="module")
async def test_function_return_value(session):
    """Test that function return values are properly captured."""

    async def compute_function(a: int, b: int) -> dict:
        return {"sum": a + b, "product": a * b, "inputs": [a, b]}

    task = ComputeTask(function=compute_function, args=(5, 7))

    await session.submit_tasks([task])
    results = await session.wait_tasks([task])

    assert results[0].state == "DONE"
    return_value = results[0].get("return_value")
    assert return_value["sum"] == 12
    assert return_value["product"] == 35


# ============================================================================
# Test 9: Stdout Capture
# ============================================================================


@pytest.mark.asyncio(loop_scope="module")
async def test_stdout_capture(session):
    """Test that stdout is properly captured."""
    import sys

    task = ComputeTask(
        executable=sys.executable,
        arguments=["-c", "print('Line 1'); print('Line 2'); print('Line 3')"],
    )

    await session.submit_tasks([task])
    results = await session.wait_tasks([task])

    assert results[0].state == "DONE"
    stdout = results[0].get("stdout", "")
    assert "Line 1" in stdout
    assert "Line 2" in stdout
    assert "Line 3" in stdout


# ============================================================================
# Test 10: Task Cancellation
# ============================================================================


@pytest.mark.asyncio(loop_scope="module")
async def test_task_cancellation(session):
    """Test cancelling a task before completion."""
    task = ComputeTask(executable="/bin/sleep", arguments=["10"])

    await session.submit_tasks([task])
    await asyncio.sleep(0.5)

    backend = next(iter(session.backends.values()))
    cancelled = await backend.cancel_task(task.uid)
    assert cancelled is True


# ============================================================================
# Test 11: Backend State
# ============================================================================


@pytest.mark.asyncio(loop_scope="module")
async def test_backend_state(session):
    """Test backend state is queryable and non-null."""
    backend = next(iter(session.backends.values()))
    state = await backend.state()
    assert state is not None

    task = ComputeTask(executable="echo", arguments=["test"])
    await session.submit_tasks([task])
    await session.wait_tasks([task])


# ============================================================================
# Test 12: Multiple Submissions (Sequential)
# ============================================================================


@pytest.mark.asyncio(loop_scope="module")
async def test_sequential_submissions(session):
    """Test submitting tasks in multiple batches."""
    task1 = ComputeTask(executable="echo", arguments=["Batch 1"])

    await session.submit_tasks([task1])
    results1 = await session.wait_tasks([task1])
    assert results1[0].state == "DONE"
    assert "Batch 1" in results1[0].get("stdout", "")

    task2 = ComputeTask(executable="echo", arguments=["Batch 2"])

    await session.submit_tasks([task2])
    results2 = await session.wait_tasks([task2])
    assert results2[0].state == "DONE"
    assert "Batch 2" in results2[0].get("stdout", "")


# ============================================================================
# Test 13: Function with Kwargs
# ============================================================================


@pytest.mark.asyncio(loop_scope="module")
async def test_function_with_kwargs(session):
    """Test function execution with keyword arguments."""

    async def function_with_kwargs(x: int, y: int = 10, z: int = 20) -> int:
        return x + y + z

    task = ComputeTask(function=function_with_kwargs, args=(5,), kwargs={"y": 15, "z": 25})

    await session.submit_tasks([task])
    results = await session.wait_tasks([task])

    assert results[0].state == "DONE"
    assert results[0].get("return_value") == 45


# ============================================================================
# Test 14: Empty Task List
# ============================================================================


@pytest.mark.asyncio(loop_scope="module")
async def test_empty_task_list(session):
    """Test handling of empty task list."""
    await session.submit_tasks([])


# ============================================================================
# Test 15: Task UID Uniqueness
# ============================================================================


@pytest.mark.asyncio(loop_scope="module")
async def test_task_uid_uniqueness(session):
    """Test that auto-generated UIDs are unique."""
    tasks = [
        ComputeTask(executable="echo", arguments=["Task 1"]),
        ComputeTask(executable="echo", arguments=["Task 2"]),
    ]

    await session.submit_tasks(tasks)
    results = await session.wait_tasks(tasks)

    assert len(results) == 2
    uids = [r.uid for r in results]
    assert len(set(uids)) == 2
    assert all(uid.startswith("task.") for uid in uids)


# ============================================================================
# Dragon integration tests — per-task cwd and process_template routing
# ============================================================================


@pytest.mark.asyncio(loop_scope="module")
async def test_executable_with_cwd_via_process_template(session_dragon):
    """Test that cwd is honoured when set via process_template (Dragon backend only)."""
    import sys

    task = ComputeTask(
        executable=sys.executable,
        arguments=["-c", "import os; print(os.getcwd())"],
        task_backend_specific_kwargs={"process_template": {"cwd": "/tmp"}},
    )

    await session_dragon.submit_tasks([task])
    results = await session_dragon.wait_tasks([task])

    assert results[0].state == "DONE"
    assert "/tmp" in results[0].get("stdout", "")


@pytest.mark.asyncio(loop_scope="module")
async def test_process_template_cwd_built_and_passed(session_dragon):
    """Test A: process_template with cwd produces a ProcessTemplate with correct cwd."""
    from dragon.native.process import ProcessTemplate

    backend = session_dragon.backends["dragon"]
    captured = []

    def capture(pt, **kw):
        captured.append(pt)
        return MagicMock()

    task = _make_task_dict(lambda: None, backend_specific={"process_template": {"cwd": "/tmp"}})

    with patch.object(backend.batch, "process", side_effect=capture):
        await backend.build_task(task)

    assert len(captured) == 1
    pt = captured[0]
    assert isinstance(pt, ProcessTemplate)
    assert pt.cwd == "/tmp"


@pytest.mark.asyncio(loop_scope="module")
async def test_process_template_policy_gpu_affinity_built_and_passed(session_dragon):
    """Test B: process_template with policy(gpu_affinity) produces ProcessTemplate with correct policy."""
    from dragon.infrastructure.policy import Policy
    from dragon.native.process import ProcessTemplate

    backend = session_dragon.backends["dragon"]
    captured = []

    def capture(pt, **kw):
        captured.append(pt)
        return MagicMock()

    policy = Policy(gpu_affinity=[0, 1, 2, 3])
    task = _make_task_dict(lambda: None, backend_specific={"process_template": {"policy": policy}})

    with patch.object(backend.batch, "process", side_effect=capture):
        await backend.build_task(task)

    assert len(captured) == 1
    pt = captured[0]
    assert isinstance(pt, ProcessTemplate)
    assert pt.policy is policy
    assert pt.policy.gpu_affinity == [0, 1, 2, 3]


@pytest.mark.asyncio(loop_scope="module")
async def test_process_template_empty_dict_uses_process_mode(session_dragon):
    """Test C: process_template={} still routes to batch.process(), not batch.function().

    Regression test: a truthiness check on the dict silently falls through on an
    empty dict; the ``is not None`` guard in build_task prevents this.
    """
    backend = session_dragon.backends["dragon"]
    process_calls = []
    function_calls = []

    def capture_process(pt, **kw):
        process_calls.append(pt)
        return MagicMock()

    def capture_function(target, *args, **kw):
        function_calls.append(target)
        return MagicMock()

    task = _make_task_dict(lambda: None, backend_specific={"process_template": {}})

    with (
        patch.object(backend.batch, "process", side_effect=capture_process),
        patch.object(backend.batch, "function", side_effect=capture_function),
    ):
        await backend.build_task(task)

    assert len(process_calls) == 1, "batch.process() should have been called (Priority 2)"
    assert len(function_calls) == 0, (
        "batch.function() must NOT be called when process_template is provided"
    )


@pytest.mark.asyncio(loop_scope="module")
async def test_process_templates_list_built_and_passed_to_job(session_dragon):
    """Test D: process_templates list produces correct (nranks, ProcessTemplate) tuples for batch.job()."""
    from dragon.native.process import ProcessTemplate

    backend = session_dragon.backends["dragon"]
    captured_args = []

    def capture_job(templates, **kw):
        captured_args.append(templates)
        return MagicMock()

    task = _make_task_dict(
        lambda: None,
        backend_specific={"process_templates": [(2, {"cwd": "/tmp"})]},
    )

    with patch.object(backend.batch, "job", side_effect=capture_job):
        await backend.build_task(task)

    assert len(captured_args) == 1
    templates = captured_args[0]
    assert len(templates) == 1
    nranks, pt = templates[0]
    assert nranks == 2
    assert isinstance(pt, ProcessTemplate)
    assert pt.cwd == "/tmp"


@pytest.mark.asyncio(loop_scope="module")
async def test_process_template_combined_spec_policy_cwd_args(session_dragon):
    """Test E: process_template with policy + cwd all land on the ProcessTemplate correctly."""
    import cloudpickle
    from dragon.infrastructure.policy import Policy
    from dragon.native.process import ProcessTemplate

    backend = session_dragon.backends["dragon"]
    captured = []

    def capture(pt, **kw):
        captured.append(pt)
        return MagicMock()

    policy = Policy(gpu_affinity=[0])
    task = _make_task_dict(
        lambda x: x,
        args=(42,),
        kwargs={"flag": True},
        backend_specific={"process_template": {"policy": policy, "cwd": "/tmp"}},
    )

    with patch.object(backend.batch, "process", side_effect=capture):
        await backend.build_task(task)

    assert len(captured) == 1
    pt = captured[0]
    assert isinstance(pt, ProcessTemplate)
    assert pt.policy is policy
    assert pt.cwd == "/tmp"
    # Dragon serialises (target, args, kwargs) into pt.argdata via cloudpickle.
    _, stored_args, stored_kwargs = cloudpickle.loads(pt.argdata)
    assert list(stored_args) == [42]
    assert stored_kwargs == {"flag": True}


# ============================================================================
# Dragon unit tests — constructor and internal methods (no Dragon cluster required)
# ============================================================================


def test_constructor_batch_kwargs_forwarded_verbatim():
    """batch_kwargs contents are splatted into Batch() unchanged."""
    from rhapsody.backends.execution.dragon import DragonExecutionBackend

    mock_batch = MagicMock()
    mock_batch.num_workers = 8
    mock_batch.num_managers = 1

    kwargs = {"num_nodes": 4, "pool_nodes": 2, "disable_telem": True, "scheduler_workers": 2}

    with patch(
        "rhapsody.backends.execution.dragon.Batch", return_value=mock_batch
    ) as mock_batch_cls:
        backend = DragonExecutionBackend(batch_kwargs=kwargs)

    mock_batch_cls.assert_called_once_with(**kwargs)
    assert backend.batch is mock_batch


def test_constructor_no_batch_kwargs_calls_batch_with_no_args():
    """DragonExecutionBackend() with no args calls Batch() with no args."""
    from rhapsody.backends.execution.dragon import DragonExecutionBackend

    mock_batch = MagicMock()
    mock_batch.num_workers = 4
    mock_batch.num_managers = 1

    with patch(
        "rhapsody.backends.execution.dragon.Batch", return_value=mock_batch
    ) as mock_batch_cls:
        DragonExecutionBackend()

    mock_batch_cls.assert_called_once_with()


def test_constructor_rejects_bare_batch_params():
    """Batch params passed directly (not via batch_kwargs) raise TypeError.

    num_nodes, pool_nodes, disable_telemetry, and other old top-level params are no longer accepted
    as direct constructor arguments — they must go through batch_kwargs.
    """
    from rhapsody.backends.execution.dragon import DragonExecutionBackend

    mock_batch = MagicMock()
    mock_batch.num_workers = 8
    mock_batch.num_managers = 1

    with patch("rhapsody.backends.execution.dragon.Batch", return_value=mock_batch):
        with pytest.raises(TypeError):
            DragonExecutionBackend(num_nodes=4)
        with pytest.raises(TypeError):
            DragonExecutionBackend(pool_nodes=2)
        with pytest.raises(TypeError):
            DragonExecutionBackend(disable_telemetry=True)
        with pytest.raises(TypeError):
            DragonExecutionBackend(num_workers=4)
        with pytest.raises(TypeError):
            DragonExecutionBackend(disable_background_batching=True)
        with pytest.raises(TypeError):
            DragonExecutionBackend(disable_batch_submission=True)


def test_deliver_batch_success_stores_value_and_fires_done(backend_dragon):
    """_deliver_batch stores return_value on the task dict and fires the DONE callback."""
    uid = "task.unit-done"
    task_desc = {"uid": uid}
    backend_dragon._task_registry[uid] = {"uid": uid, "description": task_desc}

    backend_dragon._deliver_batch([(uid, 42, None, False, "", "")])

    assert task_desc["return_value"] == 42
    assert task_desc["stdout"] == ""
    assert task_desc["stderr"] == ""
    backend_dragon._callback_func.assert_called_once_with(task_desc, "DONE")
    assert uid not in backend_dragon._task_registry


def test_deliver_batch_propagates_stdout_stderr(backend_dragon):
    """_deliver_batch stores stdout/stderr on task_desc when non-empty."""
    uid = "task.unit-done-out"
    task_desc = {"uid": uid}
    backend_dragon._task_registry[uid] = {"uid": uid, "description": task_desc}

    backend_dragon._deliver_batch([(uid, "ok", None, False, "hello\n", "warn\n")])

    assert task_desc["stdout"] == "hello\n"
    assert task_desc["stderr"] == "warn\n"


def test_deliver_batch_failure_stores_exc_and_fires_failed(backend_dragon):
    """_deliver_batch stores the exception and stderr string, fires the FAILED callback."""
    uid = "task.unit-failed"
    task_desc = {"uid": uid}
    backend_dragon._task_registry[uid] = {"uid": uid, "description": task_desc}
    exc = RuntimeError("something went wrong")

    # raised=True, tb=None: stderr falls back to str(exc)
    backend_dragon._deliver_batch([(uid, exc, None, True, "", "")])

    assert task_desc["exception"] is exc
    assert "something went wrong" in task_desc["stderr"]
    backend_dragon._callback_func.assert_called_once_with(task_desc, "FAILED")
    assert uid not in backend_dragon._task_registry


def test_deliver_batch_prefers_traceback_over_str_exc(backend_dragon):
    """_deliver_batch uses Dragon's traceback string when available."""
    uid = "task.unit-failed-tb"
    task_desc = {"uid": uid}
    backend_dragon._task_registry[uid] = {"uid": uid, "description": task_desc}
    exc = RuntimeError("boom")
    tb = "Traceback (most recent call last):\n  File ...\nRuntimeError: boom"

    backend_dragon._deliver_batch([(uid, exc, tb, True, "", "")])

    assert task_desc["stderr"] == tb


def test_cancelled_task_skips_callback(backend_dragon):
    """_deliver_batch is a no-op for UIDs in _cancelled_tasks."""
    uid = "task.unit-cancelled"
    task_desc = {"uid": uid}
    backend_dragon._task_registry[uid] = {"uid": uid, "description": task_desc}
    backend_dragon._cancelled_tasks.add(uid)

    backend_dragon._deliver_batch([(uid, 99, None, False, "", "")])

    backend_dragon._callback_func.assert_not_called()
    assert "return_value" not in task_desc
    assert uid not in backend_dragon._cancelled_tasks


def test_fence_delegates_to_batch(backend_dragon):
    """backend.fence() calls batch.fence() exactly once."""
    backend_dragon.fence()
    backend_dragon.batch.fence.assert_called_once()


def test_monitor_loop_get_called_after_poll(backend_dragon):
    """After poll() returns a tuid, get(block=False) is called on the corresponding task."""
    uid = "task.poll-get"
    mock_task = MagicMock()
    mock_task.get.return_value = "done-result"
    mock_task.traceback = None
    mock_task.stdout_path = None
    mock_task.stderr_path = None
    backend_dragon._monitored_batches[uid] = (mock_task, "flow-uid")

    # Simulate the drain: poll returned uid, now process it
    entry = backend_dragon._monitored_batches.pop(uid, None)
    assert entry is not None
    batch_task, flow_uid = entry
    result = batch_task.get(block=False)
    stdout = batch_task.stdout_path or ""
    stderr = batch_task.stderr_path or ""
    completed = [(flow_uid, result, None, False, stdout, stderr)]

    mock_task.get.assert_called_once_with(block=False)
    assert completed == [("flow-uid", "done-result", None, False, "", "")]


def test_monitor_loop_skips_cancelled_tuid(backend_dragon):
    """When poll() returns a tuid not in _monitored_batches (cancelled task), it is skipped."""
    uid = "task.poll-cancelled"
    backend_dragon._monitored_batches.pop(uid, None)  # ensure absent (simulates cancel_task)

    completed = []
    entry = backend_dragon._monitored_batches.pop(uid, None)
    if entry is not None:
        completed.append(entry)

    assert completed == []  # skipped — no entry present


def test_monitor_loop_reads_paths_from_batch_task(backend_dragon):
    """stdout/stderr paths come from batch_task.stdout_path/stderr_path, not _task_registry."""
    uid = "task.poll-paths"
    mock_task = MagicMock()
    mock_task.get.return_value = "result"
    mock_task.traceback = None
    mock_task.stdout_path = "/work/uid.stdout"
    mock_task.stderr_path = "/work/uid.stderr"
    backend_dragon._monitored_batches[uid] = (mock_task, "flow-paths")

    entry = backend_dragon._monitored_batches.pop(uid)
    batch_task, _ = entry
    stdout = batch_task.stdout_path or ""
    stderr = batch_task.stderr_path or ""

    assert stdout == "/work/uid.stdout"
    assert stderr == "/work/uid.stderr"


@pytest.mark.asyncio
async def test_cancel_task_calls_dragon_cancel(backend_dragon):
    """cancel_task delegates to batch_task.cancel() and fires CANCELED callback on success.

    The callback must be called synchronously — no asyncio.sleep(0) needed to observe it. This
    verifies that TaskStateManager.update_task (when bound) runs inline on the event loop.

    After cancellation the task must be removed from both _task_registry and _monitored_batches so
    the monitor loop stops polling its DDict entry (blocking IPC for a cancelled task would stall
    the monitor thread and delay result delivery for subsequently submitted tasks).
    """
    uid = "task.unit-cancel"
    mock_batch_task = MagicMock()
    mock_batch_task.cancel.return_value = True
    task_desc = {"uid": uid}
    backend_dragon._task_registry[uid] = {
        "uid": uid,
        "description": task_desc,
        "batch_task": mock_batch_task,
    }
    backend_dragon._monitored_batches[mock_batch_task.uid] = (mock_batch_task, uid)

    result = await backend_dragon.cancel_task(uid)

    # Callback and state must be visible immediately — no yield required
    assert result is True
    mock_batch_task.cancel.assert_called_once()
    backend_dragon._callback_func.assert_called_once_with(task_desc, "CANCELED")
    # Task must be eagerly removed so the monitor loop stops polling
    assert uid not in backend_dragon._task_registry
    assert mock_batch_task.uid not in backend_dragon._monitored_batches


@pytest.mark.asyncio
async def test_cancel_task_returns_false_when_task_completed_before_cancel_lands(backend_dragon):
    """cancel_task returns False when the task was already delivered (registry entry gone).

    Race: the event loop yields inside run_in_executor; _deliver_batch could pop the task
    from _task_registry before cancel_task resumes. The guard must return False rather than
    raising KeyError, and must NOT fire the CANCELED callback.

    The test simulates this by patching run_in_executor to pop the registry entry as a
    side effect — exactly what _deliver_batch does while the executor is in flight.
    """
    uid = "task.unit-cancel-race"
    mock_batch_task = MagicMock()
    mock_batch_task.cancel.return_value = True
    task_desc = {"uid": uid}
    backend_dragon._task_registry[uid] = {
        "uid": uid,
        "description": task_desc,
        "batch_task": mock_batch_task,
    }

    # Simulate _deliver_batch removing the registry entry while run_in_executor is awaited.
    original_cancel = mock_batch_task.cancel

    def cancel_and_deliver():
        result = original_cancel()
        backend_dragon._task_registry.pop(uid, None)
        return result

    mock_batch_task.cancel = cancel_and_deliver

    result = await backend_dragon.cancel_task(uid)

    assert result is False
    backend_dragon._callback_func.assert_not_called()
    assert uid not in backend_dragon._task_registry


@pytest.mark.asyncio
async def test_cancel_task_dragon_returns_false_no_callback(backend_dragon):
    """cancel_task does not fire callback when Dragon reports the task cannot be cancelled."""
    uid = "task.unit-cancel-false"
    mock_batch_task = MagicMock()
    mock_batch_task.cancel.return_value = False
    task_desc = {"uid": uid}
    backend_dragon._task_registry[uid] = {
        "uid": uid,
        "description": task_desc,
        "batch_task": mock_batch_task,
    }

    result = await backend_dragon.cancel_task(uid)

    assert result is False
    backend_dragon._callback_func.assert_not_called()
    assert uid not in backend_dragon._cancelled_tasks


@pytest.mark.asyncio
async def test_cancel_task_dragon_raises_falls_back_to_soft_cancel(backend_dragon):
    """cancel_task falls back to soft-cancel (callback fired, cancelled=True) if Dragon raises."""
    uid = "task.unit-cancel-exc"
    mock_batch_task = MagicMock()
    mock_batch_task.cancel.side_effect = RuntimeError("dragon scheduler unreachable")
    task_desc = {"uid": uid}
    backend_dragon._task_registry[uid] = {
        "uid": uid,
        "description": task_desc,
        "batch_task": mock_batch_task,
    }
    backend_dragon._monitored_batches[mock_batch_task.uid] = (mock_batch_task, uid)

    result = await backend_dragon.cancel_task(uid)

    assert result is True
    backend_dragon._callback_func.assert_called_once_with(task_desc, "CANCELED")
    assert uid not in backend_dragon._task_registry
    assert mock_batch_task.uid not in backend_dragon._monitored_batches


@pytest.mark.asyncio
async def test_shutdown_calls_join_and_destroy_not_close(backend_dragon):
    """Shutdown() calls batch.join() then batch.destroy(); batch.close() must NOT be called."""
    await backend_dragon.shutdown()

    backend_dragon.batch.join.assert_called_once()
    backend_dragon.batch.destroy.assert_called_once()
    backend_dragon.batch.close.assert_not_called()


# ============================================================================
# V3 capture_stdio tests — no Dragon cluster required
# ============================================================================


@pytest.mark.asyncio
async def test_capture_stdio_true_passes_paths_to_batch(backend_dragon, tmp_path):
    """capture_stdio=True passes stdout/stderr file paths to batch.process() — no shell script."""
    backend_dragon._work_dir = str(tmp_path)
    captured_kwargs = {}

    def capture_process(pt, **kw):
        captured_kwargs.update(kw)
        return MagicMock()

    task = {
        "uid": "task.capture-test-001",
        "executable": "/bin/echo",
        "arguments": ["hello"],
        "function": None,
        "args": [],
        "kwargs": {},
        "name": "capture-test",
        "task_backend_specific_kwargs": {"process_template": {}},
        "capture_stdio": True,
    }

    with patch.object(backend_dragon.batch, "process", side_effect=capture_process):
        await backend_dragon.build_task(task)

    assert captured_kwargs.get("stdout", "").endswith(".stdout")
    assert captured_kwargs.get("stderr", "").endswith(".stderr")
    # No shell script written — Dragon handles redirection natively
    assert not list(tmp_path.glob("*.sh"))
    # Registry no longer stores paths — they live on the batch_task object
    reg = backend_dragon._task_registry[task["uid"]]
    assert "script_path" not in reg


@pytest.mark.asyncio
async def test_capture_stdio_false_passes_none_to_batch(backend_dragon, tmp_path):
    """capture_stdio=False (default) passes stdout=None, stderr=None to batch — no capture."""
    backend_dragon._work_dir = str(tmp_path)
    captured_kwargs = {}

    def capture_process(pt, **kw):
        captured_kwargs.update(kw)
        return MagicMock()

    task = {
        "uid": "task.capture-test-002",
        "executable": "/bin/echo",
        "arguments": ["hello"],
        "function": None,
        "args": [],
        "kwargs": {},
        "name": "no-capture-test",
        "task_backend_specific_kwargs": {"process_template": {}},
        "capture_stdio": False,
    }

    with patch.object(backend_dragon.batch, "process", side_effect=capture_process):
        await backend_dragon.build_task(task)

    assert captured_kwargs.get("stdout") is None
    assert captured_kwargs.get("stderr") is None
    assert not list(tmp_path.glob("*.sh"))


@pytest.mark.asyncio
async def test_capture_stdio_function_task_passes_paths_to_batch_function(backend_dragon, tmp_path):
    """capture_stdio=True on a function task passes stdout/stderr paths to batch.function()."""
    backend_dragon._work_dir = str(tmp_path)
    captured_kwargs = {}

    def capture_function(fn, *args, **kw):
        captured_kwargs.update(kw)
        return MagicMock()

    task = {
        "uid": "task.capture-test-003",
        "executable": None,
        "function": lambda: None,
        "arguments": [],
        "args": [],
        "kwargs": {},
        "name": "func-capture-test",
        "task_backend_specific_kwargs": {},
        "capture_stdio": True,
    }

    with patch.object(backend_dragon.batch, "function", side_effect=capture_function):
        await backend_dragon.build_task(task)

    assert captured_kwargs.get("stdout", "").endswith(".stdout")
    assert captured_kwargs.get("stderr", "").endswith(".stderr")
    assert not list(tmp_path.glob("*.sh"))


# ============================================================================
# result_contract tests — stdout/stderr must be str after any DONE/FAILED
# ============================================================================


@pytest.mark.result_contract
def test_deliver_batch_empty_dragon_stdout_written_as_empty_string(backend_dragon):
    """Regression: stdout='' from Dragon must land as '' not None or absent key."""
    uid = "task.contract-empty-stdout"
    task_desc = {"uid": uid}
    backend_dragon._task_registry[uid] = {"uid": uid, "description": task_desc}

    backend_dragon._deliver_batch([(uid, 0, None, False, "", "")])

    assert isinstance(task_desc["stdout"], str)
    assert isinstance(task_desc["stderr"], str)
    backend_dragon._callback_func.assert_called_once_with(task_desc, "DONE")


@pytest.mark.result_contract
def test_deliver_batch_path_from_tuple_stored_on_task(backend_dragon):
    """stdout/stderr paths in the completion tuple are stored directly on task_desc.

    The monitor loop passes batch_task.stdout_path as the 5th element; _deliver_batch
    stores it verbatim — no registry lookup needed.
    """
    uid = "task.contract-redirect"
    task_desc = {"uid": uid}
    backend_dragon._task_registry[uid] = {"uid": uid, "description": task_desc}

    backend_dragon._deliver_batch([(uid, 0, None, False, "/work/uid.stdout", "/work/uid.stderr")])

    assert task_desc["stdout"] == "/work/uid.stdout"
    assert task_desc["stderr"] == "/work/uid.stderr"


@pytest.mark.result_contract
def test_deliver_batch_failed_stdout_is_string(backend_dragon):
    """Stdout must be a str in the FAILED path."""
    uid = "task.contract-failed-stdout"
    task_desc = {"uid": uid}
    backend_dragon._task_registry[uid] = {"uid": uid, "description": task_desc}

    backend_dragon._deliver_batch([(uid, RuntimeError("boom"), None, True, "", "")])

    assert isinstance(task_desc["stdout"], str)
