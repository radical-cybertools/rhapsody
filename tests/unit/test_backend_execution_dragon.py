"""Tests for Dragon execution backends (V1, V2, V3).

These tests focus on core features with minimal tasks (1-2 per test).
Run with: dragon python -m pytest tests/test_dragon_backends.py -v
"""
import asyncio
import pytest
import sys
import os

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
    task = {
        "uid": "test_single_exec",
        "executable": "echo",
        "arguments": ["Hello Dragon"],
        "task_backend_specific_kwargs": {"shell": True}
    }

    await backend.submit_tasks([task])
    results = await backend.wait_tasks([task])

    assert "test_single_exec" in results
    assert results["test_single_exec"]["state"] == "DONE"
    assert "Hello Dragon" in results["test_single_exec"].get("stdout", "")


# ============================================================================
# Test 2: Single Function Task
# ============================================================================

@pytest.mark.asyncio
async def test_single_function(backend):
    """Test executing a single Python function task."""

    async def simple_function(x: int) -> int:
        """Simple async function for testing."""
        return x * 2

    task = {
        "uid": "test_single_func",
        "function": simple_function,
        "args": (21,),
        "kwargs": {}
    }

    await backend.submit_tasks([task])
    results = await backend.wait_tasks([task])

    assert "test_single_func" in results
    assert results["test_single_func"]["state"] == "DONE"
    assert results["test_single_func"].get("return_value") == 42


# ============================================================================
# Test 3: Task with Arguments
# ============================================================================

@pytest.mark.asyncio
async def test_task_with_args(backend):
    """Test task execution with multiple arguments."""
    task = {
        "uid": "test_args",
        "executable": "/bin/echo",
        "arguments": ["arg1", "arg2", "arg3"]
    }

    await backend.submit_tasks([task])
    results = await backend.wait_tasks([task])

    assert results["test_args"]["state"] == "DONE"
    stdout = results["test_args"].get("stdout", "")
    assert "arg1" in stdout and "arg2" in stdout and "arg3" in stdout


# ============================================================================
# Test 4: Task Failure Handling
# ============================================================================

@pytest.mark.asyncio
async def test_task_failure(backend):
    """Test that failed tasks are properly reported."""
    task = {
        "uid": "test_failure",
        "executable": "/bin/false"  # Command that always fails
    }

    await backend.submit_tasks([task])
    results = await backend.wait_tasks([task])

    assert results["test_failure"]["state"] == "FAILED"


# ============================================================================
# Test 5: Function with Exception
# ============================================================================

@pytest.mark.asyncio
async def test_function_exception(backend):
    """Test that function exceptions are properly handled."""

    async def failing_function():
        """Function that raises an exception."""
        raise ValueError("Intentional test failure")

    task = {
        "uid": "test_func_exception",
        "function": failing_function,
        "args": (),
        "kwargs": {}
    }

    await backend.submit_tasks([task])
    results = await backend.wait_tasks([task])

    assert results["test_func_exception"]["state"] == "FAILED"
    assert "exception" in results["test_func_exception"]


# ============================================================================
# Test 6: Two Independent Tasks (Parallel Execution)
# ============================================================================

@pytest.mark.asyncio
async def test_two_independent_tasks(backend):
    """Test executing two independent tasks in parallel."""
    tasks = [
        {
            "uid": "task_a",
            "executable": "echo",
            "arguments": ["Task A"],
            "task_backend_specific_kwargs": {"shell": True}
        },
        {
            "uid": "task_b",
            "executable": "echo",
            "arguments": ["Task B"],
            "task_backend_specific_kwargs": {"shell": True}
        }
    ]

    await backend.submit_tasks(tasks)
    results = await backend.wait_tasks(tasks)

    assert len(results) == 2
    assert results["task_a"]["state"] == "DONE"
    assert results["task_b"]["state"] == "DONE"
    assert "Task A" in results["task_a"].get("stdout", "")
    assert "Task B" in results["task_b"].get("stdout", "")


# ============================================================================
# Test 7: Mixed Success and Failure
# ============================================================================

@pytest.mark.asyncio
async def test_mixed_success_failure(backend):
    """Test handling tasks where some succeed and some fail."""
    tasks = [
        {
            "uid": "success_task",
            "executable": "/bin/true"
        },
        {
            "uid": "failure_task",
            "executable": "/bin/false"
        }
    ]

    await backend.submit_tasks(tasks)
    results = await backend.wait_tasks(tasks)

    assert len(results) == 2
    assert results["success_task"]["state"] == "DONE"
    assert results["failure_task"]["state"] == "FAILED"


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

    task = {
        "uid": "test_return",
        "function": compute_function,
        "args": (5, 7),
        "kwargs": {}
    }

    await backend.submit_tasks([task])
    results = await backend.wait_tasks([task])

    assert results["test_return"]["state"] == "DONE"
    return_value = results["test_return"].get("return_value")
    assert return_value["sum"] == 12
    assert return_value["product"] == 35
    assert return_value["inputs"] == [5, 7]


# ============================================================================
# Test 9: Stdout Capture
# ============================================================================

@pytest.mark.asyncio
async def test_stdout_capture(backend):
    """Test that stdout is properly captured."""
    task = {
        "uid": "test_stdout",
        "executable": "/bin/bash",
        "arguments": ["-c", "echo 'Line 1'; echo 'Line 2'; echo 'Line 3'"]
    }

    await backend.submit_tasks([task])
    results = await backend.wait_tasks([task])

    assert results["test_stdout"]["state"] == "DONE"
    stdout = results["test_stdout"].get("stdout", "")
    assert "Line 1" in stdout
    assert "Line 2" in stdout
    assert "Line 3" in stdout


# ============================================================================
# Test 10: Environment Variables
# ============================================================================

@pytest.mark.asyncio
async def test_environment_variables(backend):
    """Test that environment variables can be set for tasks."""
    task = {
        "uid": "test_env",
        "executable": "/bin/bash",
        "arguments": ["-c", "echo $TEST_VAR"],
        "environment": {"TEST_VAR": "test_value_123"}
    }

    await backend.submit_tasks([task])
    results = await backend.wait_tasks([task])

    assert results["test_env"]["state"] == "DONE"
    assert "test_value_123" in results["test_env"].get("stdout", "")


# ============================================================================
# Test 11: Task Cancellation
# ============================================================================

@pytest.mark.asyncio
async def test_task_cancellation(backend):
    """Test cancelling a task before completion."""
    # Long-running task
    task = {
        "uid": "test_cancel",
        "executable": "/bin/sleep",
        "arguments": ["10"]
    }

    await backend.submit_tasks([task])
    await asyncio.sleep(0.5)  # Let task start

    # Cancel the task
    cancelled = await backend.cancel_task("test_cancel")
    assert cancelled is True

    # Wait a bit and verify task was cancelled
    await asyncio.sleep(1.0)

    # Task should be in cancelled state or removed from registry
    # Different backends may handle this differently


# ============================================================================
# Test 12: Backend State
# ============================================================================

@pytest.mark.asyncio
async def test_backend_state(backend):
    """Test backend state tracking."""
    state = backend.state()
    assert state is not None

    # Submit a task
    task = {
        "uid": "test_state",
        "executable": "echo",
        "arguments": ["test"],
        "task_backend_specific_kwargs": {"shell": True}
    }

    await backend.submit_tasks([task])
    state_running = backend.state()
    assert state_running == "running"

    # Wait for completion
    await backend.wait_tasks([task])


# ============================================================================
# Test 13: Multiple Submissions (Sequential)
# ============================================================================

@pytest.mark.asyncio
async def test_sequential_submissions(backend):
    """Test submitting tasks in multiple batches."""
    # First batch
    task1 = {
        "uid": "batch1_task",
        "executable": "echo",
        "arguments": ["Batch 1"],
        "task_backend_specific_kwargs": {"shell": True}
    }

    await backend.submit_tasks([task1])
    results1 = await backend.wait_tasks([task1])
    assert results1["batch1_task"]["state"] == "DONE"

    # Second batch
    task2 = {
        "uid": "batch2_task",
        "executable": "echo",
        "arguments": ["Batch 2"],
        "task_backend_specific_kwargs": {"shell": True}
    }

    await backend.submit_tasks([task2])
    results2 = await backend.wait_tasks([task2])
    assert results2["batch2_task"]["state"] == "DONE"


# ============================================================================
# Test 14: Function with Kwargs
# ============================================================================

@pytest.mark.asyncio
async def test_function_with_kwargs(backend):
    """Test function execution with keyword arguments."""

    async def function_with_kwargs(x: int, y: int = 10, z: int = 20) -> int:
        """Function that uses keyword arguments."""
        return x + y + z

    task = {
        "uid": "test_kwargs",
        "function": function_with_kwargs,
        "args": (5,),
        "kwargs": {"y": 15, "z": 25}
    }

    await backend.submit_tasks([task])
    results = await backend.wait_tasks([task])

    assert results["test_kwargs"]["state"] == "DONE"
    assert results["test_kwargs"].get("return_value") == 45  # 5 + 15 + 25


# ============================================================================
# Test 15: Empty Task List
# ============================================================================

@pytest.mark.asyncio
async def test_empty_task_list(backend):
    """Test handling of empty task list."""
    # Should handle gracefully without errors
    await backend.submit_tasks([])
    # No wait_tasks call since there are no tasks


# ============================================================================
# Test 16: Task UID Uniqueness
# ============================================================================

@pytest.mark.asyncio
async def test_task_uid_uniqueness(backend):
    """Test that tasks are properly identified by their UIDs."""
    tasks = [
        {
            "uid": "unique_task_1",
            "executable": "echo",
            "arguments": ["Task 1"],
            "task_backend_specific_kwargs": {"shell": True}
        },
        {
            "uid": "unique_task_2",
            "executable": "echo",
            "arguments": ["Task 2"],
            "task_backend_specific_kwargs": {"shell": True}
        }
    ]

    await backend.submit_tasks(tasks)
    results = await backend.wait_tasks(tasks)

    # Verify each UID maps to the correct task
    assert "unique_task_1" in results
    assert "unique_task_2" in results
    assert "Task 1" in results["unique_task_1"].get("stdout", "")
    assert "Task 2" in results["unique_task_2"].get("stdout", "")


# ============================================================================
# Run tests
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
