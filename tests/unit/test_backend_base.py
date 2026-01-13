"""Unit tests for base backend interface.

This module tests the base backend abstract class and interface defined in rhapsody.backends.base.
"""

import inspect
import os

import pytest

from rhapsody import ComputeTask


def test_base_execution_backend_import():
    """Test that BaseExecutionBackend can be imported."""
    from rhapsody.backends.base import BaseExecutionBackend

    assert BaseExecutionBackend is not None


def test_base_execution_backend_is_abstract():
    """Test that BaseExecutionBackend is abstract and cannot be instantiated."""
    from rhapsody.backends.base import BaseExecutionBackend

    # Should raise TypeError when trying to instantiate abstract class
    with pytest.raises(TypeError):
        BaseExecutionBackend()


def test_session_class_import():
    """Test that Session class can be imported."""
    from rhapsody.backends.base import Session

    assert Session is not None


def test_session_instantiation():
    """Test that Session class can be instantiated."""
    from rhapsody.backends.base import Session

    session = Session()
    assert session is not None


def test_session_path_attribute():
    """Test that Session sets path attribute to current working directory."""
    from rhapsody.backends.base import Session

    original_cwd = os.getcwd()
    session = Session()

    # Should set path to current working directory
    assert hasattr(session, "path")
    assert session.path == original_cwd


def test_base_execution_backend_abstract_methods():
    """Test that BaseExecutionBackend has all required abstract methods."""
    from rhapsody.backends.base import BaseExecutionBackend

    # Get all abstract methods
    abstract_methods = BaseExecutionBackend.__abstractmethods__

    # Expected abstract methods based on the source code
    expected_methods = {
        "submit_tasks",
        "shutdown",
        "state",
        "task_state_cb",
        "get_task_states_map",
        "build_task",
        "link_implicit_data_deps",
        "link_explicit_data_deps",
        "cancel_task",
    }

    assert abstract_methods == expected_methods


def test_base_execution_backend_method_signatures():
    """Test that BaseExecutionBackend abstract methods have correct signatures."""
    from rhapsody.backends.base import BaseExecutionBackend

    # Check submit_tasks signature
    submit_tasks_sig = inspect.signature(BaseExecutionBackend.submit_tasks)
    assert len(submit_tasks_sig.parameters) == 2  # self, tasks
    assert "tasks" in submit_tasks_sig.parameters

    # Check shutdown signature
    shutdown_sig = inspect.signature(BaseExecutionBackend.shutdown)
    assert len(shutdown_sig.parameters) == 1  # self only

    # Check state signature
    state_sig = inspect.signature(BaseExecutionBackend.state)
    assert len(state_sig.parameters) == 1  # self only

    # Check cancel_task signature
    cancel_task_sig = inspect.signature(BaseExecutionBackend.cancel_task)
    assert len(cancel_task_sig.parameters) == 2  # self, uid
    assert "uid" in cancel_task_sig.parameters


@pytest.mark.asyncio
async def test_wait_tasks_basic_functionality():
    """Test that wait_tasks correctly waits for all tasks to complete."""
    from rhapsody.backends import ConcurrentExecutionBackend

    # Create a backend instance
    backend = await ConcurrentExecutionBackend()

    # Define tasks with simple commands
    tasks = [
        ComputeTask(
            executable="echo",
            arguments=["hello"]
        ),
        ComputeTask(
            executable="echo",
            arguments=["world"]
        ),
        ComputeTask(
            executable="false"
        ),  # This will fail
    ]

    # Submit tasks
    await backend.submit_tasks(tasks)

    # Wait for tasks to complete (returns list of tasks)
    completed_tasks = await backend.wait_tasks(tasks, timeout=5.0)

    # Verify all tasks are tracked
    assert len(completed_tasks) == 3

    # Verify all tasks have auto-generated UIDs
    for task in completed_tasks:
        assert task["uid"].startswith("task.")

    # Verify tasks by index (order is preserved)
    # First two should succeed
    assert completed_tasks[0]["state"] == "DONE"
    assert "stdout" in completed_tasks[0]

    assert completed_tasks[1]["state"] == "DONE"
    assert "stdout" in completed_tasks[1]

    # Third should fail
    assert completed_tasks[2]["state"] == "FAILED"
    assert completed_tasks[2]["exit_code"] != 0

    # Cleanup
    await backend.shutdown()


@pytest.mark.asyncio
async def test_wait_tasks_with_timeout():
    """Test that wait_tasks respects timeout parameter."""
    import asyncio
    from rhapsody.backends import ConcurrentExecutionBackend

    # Create a backend instance
    backend = await ConcurrentExecutionBackend()

    # Define tasks with one long-running task
    tasks = [
        ComputeTask(
            executable="echo",
            arguments=["quick"]
        ),
        ComputeTask(
            executable="sleep",
            arguments=["10"]
        ),  # Long sleep
    ]

    # Submit tasks
    await backend.submit_tasks(tasks)

    # Wait with short timeout - should timeout
    with pytest.raises(asyncio.TimeoutError):
        await backend.wait_tasks(tasks, timeout=0.5)

    # Cancel the long-running task using its auto-generated UID
    long_running_task = tasks[1]
    await backend.cancel_task(long_running_task.uid)

    # Cleanup
    await backend.shutdown()
