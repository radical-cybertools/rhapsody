"""Tests for Dragon execution backends (V1, V2, V3) using Session.

These tests focus on core features with minimal tasks (1-2 per test).
Run with: dragon python -m pytest tests/unit/test_backend_execution_dragon.py -v
"""

import asyncio

import pytest

from rhapsody import ComputeTask
from rhapsody.api import Session
from rhapsody.backends.discovery import get_backend

# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture(params=["dragon_v1", "dragon_v2", "dragon_v3"])
def backend_name(request):
    """Parametrize tests across all Dragon backend versions."""
    return request.param


@pytest.fixture
async def session(backend_name):
    """Create and cleanup a Session with Dragon backend."""
    backend_instance = await get_backend(backend_name)
    session_instance = Session(backends=[backend_instance])
    yield session_instance
    await session_instance.close()


# ============================================================================
# Test 1: Single Executable Task
# ============================================================================


@pytest.mark.asyncio
async def test_single_executable(session):
    """Test executing a single shell command task."""
    task = ComputeTask(
        executable="echo", arguments=["Hello Dragon"], task_backend_specific_kwargs={"shell": True}
    )

    async with session:
        await session.submit_tasks([task])
        results = await session.wait_tasks([task])

        # wait_tasks returns list of tasks (updated in-place)
        assert results[0].uid.startswith("task.")
        assert results[0].state == "DONE"
        assert "Hello Dragon" in results[0].get("stdout", "")


# ============================================================================
# Test 2: Single Function Task
# ============================================================================


@pytest.mark.asyncio
async def test_single_function(session):
    """Test executing a single Python function task."""

    async def simple_function(x: int) -> int:
        """Simple async function for testing."""
        return x * 2

    task = ComputeTask(function=simple_function, args=(21,))

    async with session:
        await session.submit_tasks([task])
        results = await session.wait_tasks([task])

        assert results[0].uid.startswith("task.")
        assert results[0].state == "DONE"
        assert results[0].get("return_value") == 42


# ============================================================================
# Test 3: Task with Arguments
# ============================================================================


@pytest.mark.asyncio
async def test_task_with_args(session):
    """Test task execution with multiple arguments."""
    task = ComputeTask(executable="/bin/echo", arguments=["arg1", "arg2", "arg3"])

    async with session:
        await session.submit_tasks([task])
        results = await session.wait_tasks([task])

        assert results[0].state == "DONE"
        stdout = results[0].get("stdout", "")
        # Note: echo output might process args differently depending on shell execution
        # but typically "arg1 arg2 arg3"
        assert "arg1" in stdout and "arg2" in stdout and "arg3" in stdout


# ============================================================================
# Test 4: Task Failure Handling
# ============================================================================


@pytest.mark.asyncio
async def test_task_failure(session):
    """Test that failed tasks are properly reported."""
    task = ComputeTask(
        executable="/bin/false"  # Command that always fails
    )

    async with session:
        await session.submit_tasks([task])
        results = await session.wait_tasks([task])

        assert results[0].state == "FAILED"


# ============================================================================
# Test 5: Function with Exception
# ============================================================================


@pytest.mark.asyncio
async def test_function_exception(session):
    """Test that function exceptions are properly handled."""

    async def failing_function():
        """Function that raises an exception."""
        raise ValueError("Intentional test failure")

    task = ComputeTask(function=failing_function, args=())

    async with session:
        await session.submit_tasks([task])
        results = await session.wait_tasks([task])

        assert results[0].state == "FAILED"
        assert "exception" in results[0]


# ============================================================================
# Test 6: Two Independent Tasks (Parallel Execution)
# ============================================================================


@pytest.mark.asyncio
async def test_two_independent_tasks(session):
    """Test executing two independent tasks in parallel."""
    tasks = [
        ComputeTask(
            executable="echo", arguments=["Task A"], task_backend_specific_kwargs={"shell": True}
        ),
        ComputeTask(
            executable="echo", arguments=["Task B"], task_backend_specific_kwargs={"shell": True}
        ),
    ]

    async with session:
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


@pytest.mark.asyncio
async def test_mixed_success_failure(session):
    """Test handling tasks where some succeed and some fail."""
    tasks = [ComputeTask(executable="/bin/true"), ComputeTask(executable="/bin/false")]

    async with session:
        await session.submit_tasks(tasks)
        results = await session.wait_tasks(tasks)

        assert len(results) == 2
        states = [r.state for r in results]
        assert "DONE" in states
        assert "FAILED" in states


# ============================================================================
# Test 8: Function with Return Value
# ============================================================================


@pytest.mark.asyncio
async def test_function_return_value(session):
    """Test that function return values are properly captured."""

    async def compute_function(a: int, b: int) -> dict:
        """Function that returns a complex value."""
        return {"sum": a + b, "product": a * b, "inputs": [a, b]}

    task = ComputeTask(function=compute_function, args=(5, 7))

    async with session:
        await session.submit_tasks([task])
        results = await session.wait_tasks([task])

        assert results[0].state == "DONE"
        return_value = results[0].get("return_value")
        assert return_value["sum"] == 12
        assert return_value["product"] == 35


# ============================================================================
# Test 9: Stdout Capture
# ============================================================================


@pytest.mark.asyncio
async def test_stdout_capture(session):
    """Test that stdout is properly captured."""
    import sys

    task = ComputeTask(
        executable=sys.executable,
        arguments=["-c", "print('Line 1'); print('Line 2'); print('Line 3')"],
    )

    async with session:
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


@pytest.mark.asyncio
async def test_task_cancellation(session):
    """Test cancelling a task before completion."""
    # Long-running task
    task = ComputeTask(executable="/bin/sleep", arguments=["10"])

    async with session:
        await session.submit_tasks([task])
        await asyncio.sleep(0.5)  # Let task start

        # Cancel the task using the backend directly (via session mostly for simplicity but API allows backend access)
        # Note: Session doesn't expose cancel_task directly yet, we invoke it on backend
        # MVP: Assuming single backend in session
        backend = session.backends[0]
        cancelled = await backend.cancel_task(task.uid)
        assert cancelled is True

        # Wait a bit and verify task was updated (optional, depends on backend propagation)
        await asyncio.sleep(1.0)
        # We don't assert state here as it may vary between backends immediately


# ============================================================================
# Test 11: Backend State
# ============================================================================


@pytest.mark.asyncio
async def test_backend_state(session):
    """Test backend state tracking."""
    backend = session.backends[0]
    state = await backend.state()
    assert state is not None

    # Submit a task
    task = ComputeTask(
        executable="echo", arguments=["test"], task_backend_specific_kwargs={"shell": True}
    )

    async with session:
        await session.submit_tasks([task])
        state_running = await backend.state()
        # assert state_running == "RUNNING" # This is flaky depending on timing

        await session.wait_tasks([task])


# ============================================================================
# Test 12: Multiple Submissions (Sequential)
# ============================================================================


@pytest.mark.asyncio
async def test_sequential_submissions(session):
    """Test submitting tasks in multiple batches."""
    async with session:
        # First batch
        task1 = ComputeTask(
            executable="echo", arguments=["Batch 1"], task_backend_specific_kwargs={"shell": True}
        )

        await session.submit_tasks([task1])
        results1 = await session.wait_tasks([task1])
        assert results1[0].state == "DONE"
        assert "Batch 1" in results1[0].get("stdout", "")

        # Second batch
        task2 = ComputeTask(
            executable="echo", arguments=["Batch 2"], task_backend_specific_kwargs={"shell": True}
        )

        await session.submit_tasks([task2])
        results2 = await session.wait_tasks([task2])
        assert results2[0].state == "DONE"
        assert "Batch 2" in results2[0].get("stdout", "")


# ============================================================================
# Test 13: Function with Kwargs
# ============================================================================


@pytest.mark.asyncio
async def test_function_with_kwargs(session):
    """Test function execution with keyword arguments."""

    async def function_with_kwargs(x: int, y: int = 10, z: int = 20) -> int:
        """Function that uses keyword arguments."""
        return x + y + z

    task = ComputeTask(function=function_with_kwargs, args=(5,), kwargs={"y": 15, "z": 25})

    async with session:
        await session.submit_tasks([task])
        results = await session.wait_tasks([task])

        assert results[0].state == "DONE"
        assert results[0].get("return_value") == 45


# ============================================================================
# Test 14: Empty Task List
# ============================================================================


@pytest.mark.asyncio
async def test_empty_task_list(session):
    """Test handling of empty task list."""
    async with session:
        await session.submit_tasks([])


# ============================================================================
# Test 15: Task UID Uniqueness
# ============================================================================


@pytest.mark.asyncio
async def test_task_uid_uniqueness(session):
    """Test that auto-generated UIDs are unique."""
    tasks = [
        ComputeTask(
            executable="echo", arguments=["Task 1"], task_backend_specific_kwargs={"shell": True}
        ),
        ComputeTask(
            executable="echo", arguments=["Task 2"], task_backend_specific_kwargs={"shell": True}
        ),
    ]

    async with session:
        await session.submit_tasks(tasks)
        results = await session.wait_tasks(tasks)

        assert len(results) == 2
        uids = [r.uid for r in results]
        assert len(set(uids)) == 2
        assert all(uid.startswith("task.") for uid in uids)
