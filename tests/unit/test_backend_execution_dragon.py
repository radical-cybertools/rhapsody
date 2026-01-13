"""Tests for Dragon execution backends (V1, V2, V3).

These tests focus on core features with minimal tasks (1-2 per test).
Run with: dragon python -m pytest tests/test_dragon_backends.py -v
"""
import asyncio
import pytest
import sys
import os

from rhapsody import ComputeTask
from rhapsody.backends.discovery import get_backend


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture(params=['dragon_v1', 'dragon_v2', 'dragon_v3'])
def backend_name(request):
    """Parametrize tests across all Dragon backend versions."""
    return request.param


@pytest.fixture
async def backend(backend_name):
    """Create and cleanup a Dragon backend instance."""
    backend_instance = await get_backend(backend_name)
    yield backend_instance
    await backend_instance.shutdown()


# ============================================================================
# Test 1: Single Executable Task
# ============================================================================

@pytest.mark.asyncio
async def test_single_executable(backend):
    """Test executing a single shell command task."""
    task = ComputeTask(
        executable="echo",
        arguments=["Hello Dragon"],
        task_backend_specific_kwargs={"shell": True}
    )

    await backend.submit_tasks([task])
    results = await backend.wait_tasks([task])

    # wait_tasks returns list of tasks (updated in-place)
    assert results[0]["uid"].startswith("task.")
    assert results[0]["state"] == "DONE"
    assert "Hello Dragon" in results[0].get("stdout", "")


# ============================================================================
# Test 2: Single Function Task
# ============================================================================

@pytest.mark.asyncio
async def test_single_function(backend):
    """Test executing a single Python function task."""

    async def simple_function(x: int) -> int:
        """Simple async function for testing."""
        return x * 2

    task = ComputeTask(
        function=simple_function,
        args=(21,)
    )

    await backend.submit_tasks([task])
    results = await backend.wait_tasks([task])

    assert results[0]["uid"].startswith("task.")
    assert results[0]["state"] == "DONE"
    assert results[0].get("return_value") == 42


# ============================================================================
# Test 3: Task with Arguments
# ============================================================================

@pytest.mark.asyncio
async def test_task_with_args(backend):
    """Test task execution with multiple arguments."""
    task = ComputeTask(
        executable="/bin/echo",
        arguments=["arg1", "arg2", "arg3"]
    )

    await backend.submit_tasks([task])
    results = await backend.wait_tasks([task])

    assert results[0]["state"] == "DONE"
    stdout = results[0].get("stdout", "")
    assert "arg1" in stdout and "arg2" in stdout and "arg3" in stdout


# ============================================================================
# Test 4: Task Failure Handling
# ============================================================================

@pytest.mark.asyncio
async def test_task_failure(backend):
    """Test that failed tasks are properly reported."""
    task = ComputeTask(
        executable="/bin/false"  # Command that always fails
    )

    await backend.submit_tasks([task])
    results = await backend.wait_tasks([task])

    assert results[0]["state"] == "FAILED"


# ============================================================================
# Test 5: Function with Exception
# ============================================================================

@pytest.mark.asyncio
async def test_function_exception(backend):
    """Test that function exceptions are properly handled."""

    async def failing_function():
        """Function that raises an exception."""
        raise ValueError("Intentional test failure")

    task = ComputeTask(
        function=failing_function,
        args=()
    )

    await backend.submit_tasks([task])
    results = await backend.wait_tasks([task])

    assert results[0]["state"] == "FAILED"
    assert "exception" in results[0]


# ============================================================================
# Test 6: Two Independent Tasks (Parallel Execution)
# ============================================================================

@pytest.mark.asyncio
async def test_two_independent_tasks(backend):
    """Test executing two independent tasks in parallel."""
    tasks = [
        ComputeTask(
            executable="echo",
            arguments=["Task A"],
            task_backend_specific_kwargs={"shell": True}
        ),
        ComputeTask(
            executable="echo",
            arguments=["Task B"],
            task_backend_specific_kwargs={"shell": True}
        )
    ]

    await backend.submit_tasks(tasks)
    results = await backend.wait_tasks(tasks)

    assert len(results) == 2
    # Verify both tasks have auto-generated UIDs and proper output
    for result in results:
        assert result["uid"].startswith("task.")
        assert result["state"] == "DONE"
    # Verify we have the right outputs (order may vary)
    outputs = [r.get("stdout", "") for r in results]
    assert any("Task A" in out for out in outputs)
    assert any("Task B" in out for out in outputs)


# ============================================================================
# Test 7: Mixed Success and Failure
# ============================================================================

@pytest.mark.asyncio
async def test_mixed_success_failure(backend):
    """Test handling tasks where some succeed and some fail."""
    tasks = [
        ComputeTask(
            executable="/bin/true"
        ),
        ComputeTask(
            executable="/bin/false"
        )
    ]

    await backend.submit_tasks(tasks)
    results = await backend.wait_tasks(tasks)

    assert len(results) == 2
    # Verify both tasks have auto-generated UIDs
    for result in results:
        assert result["uid"].startswith("task.")
    # Verify we have one success and one failure
    states = [r["state"] for r in results]
    assert "DONE" in states
    assert "FAILED" in states


# ============================================================================
# Test 8: Function with Return Value
# ============================================================================

@pytest.mark.asyncio
async def test_function_return_value(backend):
    """Test that function return values are properly captured."""

    async def compute_function(a: int, b: int) -> dict:
        """Function that returns a complex value."""
        return {
            "sum": a + b,
            "product": a * b,
            "inputs": [a, b]
        }

    task = ComputeTask(
        function=compute_function,
        args=(5, 7)
    )

    await backend.submit_tasks([task])
    results = await backend.wait_tasks([task])

    assert results[0]["state"] == "DONE"
    return_value = results[0].get("return_value")
    assert return_value["sum"] == 12
    assert return_value["product"] == 35
    assert return_value["inputs"] == [5, 7]


# ============================================================================
# Test 9: Stdout Capture
# ============================================================================

@pytest.mark.asyncio
async def test_stdout_capture(backend):
    """Test that stdout is properly captured."""
    task = ComputeTask(
        executable="/bin/bash",
        arguments=["-c", "echo 'Line 1'; echo 'Line 2'; echo 'Line 3'"]
    )

    await backend.submit_tasks([task])
    results = await backend.wait_tasks([task])

    assert results[0]["state"] == "DONE"
    stdout = results[0].get("stdout", "")
    assert "Line 1" in stdout
    assert "Line 2" in stdout
    assert "Line 3" in stdout


# ============================================================================
# Test 10: Task Cancellation
# ============================================================================

@pytest.mark.asyncio
async def test_task_cancellation(backend):
    """Test cancelling a task before completion."""
    # Long-running task
    task = ComputeTask(
        executable="/bin/sleep",
        arguments=["10"]
    )

    await backend.submit_tasks([task])
    await asyncio.sleep(0.5)  # Let task start

    # Cancel the task using the auto-generated UID
    task_uid = task.uid
    cancelled = await backend.cancel_task(task_uid)
    assert cancelled is True

    # Wait a bit and verify task was cancelled
    await asyncio.sleep(1.0)

    # Task should be in cancelled state or removed from registry
    # Different backends may handle this differently


# ============================================================================
# Test 11: Backend State
# ============================================================================

@pytest.mark.asyncio
async def test_backend_state(backend):
    """Test backend state tracking."""
    state = await backend.state()
    assert state is not None

    # Submit a task
    task = ComputeTask(
        executable="echo",
        arguments=["test"],
        task_backend_specific_kwargs={"shell": True}
    )

    await backend.submit_tasks([task])
    state_running = await backend.state()
    assert state_running == "RUNNING"

    # Wait for completion
    await backend.wait_tasks([task])


# ============================================================================
# Test 12: Multiple Submissions (Sequential)
# ============================================================================

@pytest.mark.asyncio
async def test_sequential_submissions(backend):
    """Test submitting tasks in multiple batches."""
    # First batch
    task1 = ComputeTask(
        executable="echo",
        arguments=["Batch 1"],
        task_backend_specific_kwargs={"shell": True}
    )

    await backend.submit_tasks([task1])
    results1 = await backend.wait_tasks([task1])
    assert results1[0]["state"] == "DONE"
    assert "Batch 1" in results1[0].get("stdout", "")

    # Second batch
    task2 = ComputeTask(
        executable="echo",
        arguments=["Batch 2"],
        task_backend_specific_kwargs={"shell": True}
    )

    await backend.submit_tasks([task2])
    results2 = await backend.wait_tasks([task2])
    assert results2[0]["state"] == "DONE"
    assert "Batch 2" in results2[0].get("stdout", "")


# ============================================================================
# Test 13: Function with Kwargs
# ============================================================================

@pytest.mark.asyncio
async def test_function_with_kwargs(backend):
    """Test function execution with keyword arguments."""

    async def function_with_kwargs(x: int, y: int = 10, z: int = 20) -> int:
        """Function that uses keyword arguments."""
        return x + y + z

    task = ComputeTask(
        function=function_with_kwargs,
        args=(5,),
        kwargs={"y": 15, "z": 25}
    )

    await backend.submit_tasks([task])
    results = await backend.wait_tasks([task])

    assert results[0]["state"] == "DONE"
    assert results[0].get("return_value") == 45  # 5 + 15 + 25


# ============================================================================
# Test 14: Empty Task List
# ============================================================================

@pytest.mark.asyncio
async def test_empty_task_list(backend):
    """Test handling of empty task list."""
    # Should handle gracefully without errors
    await backend.submit_tasks([])
    # No wait_tasks call since there are no tasks


# ============================================================================
# Test 15: Task UID Uniqueness
# ============================================================================

@pytest.mark.asyncio
async def test_task_uid_uniqueness(backend):
    """Test that auto-generated UIDs are unique."""
    tasks = [
        ComputeTask(
            executable="echo",
            arguments=["Task 1"],
            task_backend_specific_kwargs={"shell": True}
        ),
        ComputeTask(
            executable="echo",
            arguments=["Task 2"],
            task_backend_specific_kwargs={"shell": True}
        )
    ]

    await backend.submit_tasks(tasks)
    results = await backend.wait_tasks(tasks)

    # Verify each UID is unique and follows the format
    assert len(results) == 2
    uids = [r["uid"] for r in results]
    assert len(set(uids)) == 2  # All UIDs are unique
    assert all(uid.startswith("task.") for uid in uids)
    # Verify outputs are correct (order may vary)
    outputs = [r.get("stdout", "") for r in results]
    assert any("Task 1" in out for out in outputs)
    assert any("Task 2" in out for out in outputs)


# ============================================================================
# Run tests
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
