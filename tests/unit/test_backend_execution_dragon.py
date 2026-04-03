"""Tests for Dragon execution backends (V1, V2, V3) using Session.

Structure
---------
Shared behavior tests (1-15)
    Parametrized across dragon_v1 / dragon_v2 / dragon_v3. Verify observable task
    execution semantics that every Dragon backend must satisfy.

V3 integration tests — process_template / process_templates routing
    Use a dedicated ``session_v3`` fixture so only one V3 session is created,
    rather than creating three parametrized sessions and skipping two.

V3 unit tests — constructor and internal methods
    Verify the API surface introduced by the batch.py migration (new constructor
    parameters, _deliver_result, _deliver_failure, fence delegation) with Batch
    mocked out. No Dragon cluster is required for these tests.

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


@pytest.fixture(scope="module", params=["dragon_v1", "dragon_v2", "dragon_v3"])
def backend_name(request):
    """Parametrize shared behavior tests across all Dragon backend versions."""
    return request.param


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def session(backend_name):
    """Session with the parametrized Dragon backend, reused across shared tests."""
    backend_instance = await get_backend(backend_name)
    session_instance = Session(backends=[backend_instance])
    yield session_instance
    await session_instance.close()


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def session_v3():
    """Session backed exclusively by DragonExecutionBackendV3.

    Used by V3-specific tests so only one session is created instead of three.
    """
    backend_instance = await get_backend("dragon_v3")
    session_instance = Session(backends=[backend_instance])
    yield session_instance
    await session_instance.close()


@pytest.fixture
def backend_v3():
    """DragonExecutionBackendV3 with Batch fully mocked — no Dragon cluster required.

    Suitable for unit tests that verify constructor wiring, internal callback helpers, and method
    delegation without running actual Dragon workers.
    """
    from rhapsody.backends.execution.dragon import DragonExecutionBackendV3

    mock_batch = MagicMock()
    mock_batch.num_workers = 16
    mock_batch.num_managers = 2

    with patch("rhapsody.backends.execution.dragon.Batch", return_value=mock_batch):
        backend = DragonExecutionBackendV3()

    backend._callback_func = MagicMock()
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
# V3 integration tests — per-task cwd and process_template routing
# ============================================================================


@pytest.mark.asyncio(loop_scope="module")
async def test_executable_with_cwd_via_process_template(session_v3):
    """Test that cwd is honoured when set via process_template (V3 only)."""
    import sys

    task = ComputeTask(
        executable=sys.executable,
        arguments=["-c", "import os; print(os.getcwd())"],
        task_backend_specific_kwargs={"process_template": {"cwd": "/tmp"}},
    )

    await session_v3.submit_tasks([task])
    results = await session_v3.wait_tasks([task])

    assert results[0].state == "DONE"
    assert "/tmp" in results[0].get("stdout", "")


@pytest.mark.asyncio(loop_scope="module")
async def test_process_template_cwd_built_and_passed(session_v3):
    """Test A: process_template with cwd produces a ProcessTemplate with correct cwd."""
    from dragon.native.process import ProcessTemplate

    backend = session_v3.backends["dragon"]
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
async def test_process_template_policy_gpu_affinity_built_and_passed(session_v3):
    """Test B: process_template with policy(gpu_affinity) produces ProcessTemplate with correct policy."""
    from dragon.infrastructure.policy import Policy
    from dragon.native.process import ProcessTemplate

    backend = session_v3.backends["dragon"]
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
async def test_process_template_empty_dict_uses_process_mode(session_v3):
    """Test C: process_template={} still routes to batch.process(), not batch.function().

    Regression test: a truthiness check on the dict silently falls through on an
    empty dict; the ``is not None`` guard in build_task prevents this.
    """
    backend = session_v3.backends["dragon"]
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
async def test_process_templates_list_built_and_passed_to_job(session_v3):
    """Test D: process_templates list produces correct (nranks, ProcessTemplate) tuples for batch.job()."""
    from dragon.native.process import ProcessTemplate

    backend = session_v3.backends["dragon"]
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
async def test_process_template_combined_spec_policy_cwd_args(session_v3):
    """Test E: process_template with policy + cwd all land on the ProcessTemplate correctly."""
    import cloudpickle
    from dragon.infrastructure.policy import Policy
    from dragon.native.process import ProcessTemplate

    backend = session_v3.backends["dragon"]
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
# V3 unit tests — constructor and internal methods (no Dragon cluster required)
# ============================================================================


def test_v3_constructor_accepts_new_params_and_forwards_to_batch():
    """num_nodes, pool_nodes, and disable_telemetry are accepted and forwarded to Batch."""
    from rhapsody.backends.execution.dragon import DragonExecutionBackendV3

    mock_batch = MagicMock()
    mock_batch.num_workers = 8
    mock_batch.num_managers = 1

    with patch(
        "rhapsody.backends.execution.dragon.Batch", return_value=mock_batch
    ) as mock_batch_cls:
        backend = DragonExecutionBackendV3(num_nodes=4, pool_nodes=2, disable_telemetry=True)

    mock_batch_cls.assert_called_once_with(num_nodes=4, pool_nodes=2, disable_telem=True)
    assert backend.batch is mock_batch


def test_v3_constructor_rejects_removed_params():
    """num_workers, disable_background_batching, and disable_batch_submission no longer exist."""
    from rhapsody.backends.execution.dragon import DragonExecutionBackendV3

    mock_batch = MagicMock()
    mock_batch.num_workers = 8
    mock_batch.num_managers = 1

    with patch("rhapsody.backends.execution.dragon.Batch", return_value=mock_batch):
        with pytest.raises(TypeError):
            DragonExecutionBackendV3(num_workers=4)
        with pytest.raises(TypeError):
            DragonExecutionBackendV3(disable_background_batching=True)
        with pytest.raises(TypeError):
            DragonExecutionBackendV3(disable_batch_submission=True)


def test_v3_deliver_result_stores_value_and_fires_done(backend_v3):
    """_deliver_result stores return_value on the task dict and fires the DONE callback."""
    uid = "task.unit-done"
    task_desc = {"uid": uid}
    backend_v3._task_registry[uid] = {"uid": uid, "description": task_desc}

    backend_v3._deliver_result(uid, result=42, stdout=None, stderr=None)

    assert task_desc["return_value"] == 42
    assert "stdout" not in task_desc
    assert "stderr" not in task_desc
    backend_v3._callback_func.assert_called_once_with(task_desc, "DONE")
    # registry entry removed after delivery
    assert uid not in backend_v3._task_registry


def test_v3_deliver_result_propagates_stdout_stderr(backend_v3):
    """_deliver_result stores stdout/stderr on task_desc when non-empty."""
    uid = "task.unit-done-out"
    task_desc = {"uid": uid}
    backend_v3._task_registry[uid] = {"uid": uid, "description": task_desc}

    backend_v3._deliver_result(uid, result="ok", stdout="hello\n", stderr="warn\n")

    assert task_desc["stdout"] == "hello\n"
    assert task_desc["stderr"] == "warn\n"


def test_v3_deliver_failure_stores_exc_and_fires_failed(backend_v3):
    """_deliver_failure stores the exception and stderr string, fires the FAILED callback."""
    uid = "task.unit-failed"
    task_desc = {"uid": uid}
    backend_v3._task_registry[uid] = {"uid": uid, "description": task_desc}
    exc = RuntimeError("something went wrong")

    # Without tb: stderr falls back to str(exc)
    backend_v3._deliver_failure(uid, exc, tb=None, stdout=None, stderr=None)

    assert task_desc["exception"] is exc
    assert "something went wrong" in task_desc["stderr"]
    backend_v3._callback_func.assert_called_once_with(task_desc, "FAILED")
    assert uid not in backend_v3._task_registry


def test_v3_deliver_failure_prefers_traceback_over_str_exc(backend_v3):
    """_deliver_failure uses Dragon's traceback string when available."""
    uid = "task.unit-failed-tb"
    task_desc = {"uid": uid}
    backend_v3._task_registry[uid] = {"uid": uid, "description": task_desc}
    exc = RuntimeError("boom")
    tb = "Traceback (most recent call last):\n  File ...\nRuntimeError: boom"

    backend_v3._deliver_failure(uid, exc, tb=tb, stdout=None, stderr=None)

    assert task_desc["stderr"] == tb


def test_v3_cancelled_task_skips_both_callbacks(backend_v3):
    """_deliver_result and _deliver_failure are no-ops for UIDs in _cancelled_tasks."""
    uid = "task.unit-cancelled"
    task_desc = {"uid": uid}
    backend_v3._task_registry[uid] = {"uid": uid, "description": task_desc}
    backend_v3._cancelled_tasks.add(uid)

    backend_v3._deliver_result(uid, result=99, stdout=None, stderr=None)
    # registry is popped on first call; re-register so _deliver_failure can also exercise guard
    backend_v3._task_registry[uid] = {"uid": uid, "description": task_desc}
    backend_v3._cancelled_tasks.add(uid)
    backend_v3._deliver_failure(uid, RuntimeError("ignored"), tb=None, stdout=None, stderr=None)

    backend_v3._callback_func.assert_not_called()
    assert "return_value" not in task_desc
    # uid cleaned out of _cancelled_tasks by both guards
    assert uid not in backend_v3._cancelled_tasks


def test_v3_fence_delegates_to_batch(backend_v3):
    """backend.fence() calls batch.fence() exactly once."""
    backend_v3.fence()
    backend_v3.batch.fence.assert_called_once()
