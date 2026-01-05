"""Unit tests for base backend interface.

This module tests the base backend abstract class and interface defined in rhapsody.backends.base.
"""

import inspect
import os

import pytest


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
        "register_callback",
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

    # Check task_state_cb signature
    task_state_cb_sig = inspect.signature(BaseExecutionBackend.task_state_cb)
    assert len(task_state_cb_sig.parameters) == 3  # self, task, state
    assert "task" in task_state_cb_sig.parameters
    assert "state" in task_state_cb_sig.parameters

    # Check cancel_task signature
    cancel_task_sig = inspect.signature(BaseExecutionBackend.cancel_task)
    assert len(cancel_task_sig.parameters) == 2  # self, uid
    assert "uid" in cancel_task_sig.parameters


@pytest.mark.asyncio
async def test_wait_tasks_basic_functionality():
    """Test that wait_tasks correctly waits for all tasks to complete."""
    from rhapsody.backends.base import BaseExecutionBackend

    # Simulate callback results with full task objects
    results = [
        ({"uid": "task_1", "stdout": "output1", "return_value": 0}, "DONE"),
        ({"uid": "task_2", "stdout": "output2", "return_value": 42}, "DONE"),
        ({"uid": "task_3", "stderr": "error", "exception": Exception("failed")}, "FAILED"),
    ]

    # Wait for 3 tasks
    completed_tasks = await BaseExecutionBackend.wait_tasks(results, 3, verbose=False)

    # Verify all tasks are tracked
    assert len(completed_tasks) == 3

    # Verify task_1
    assert completed_tasks["task_1"]["state"] == "DONE"
    assert completed_tasks["task_1"]["stdout"] == "output1"
    assert completed_tasks["task_1"]["return_value"] == 0

    # Verify task_2
    assert completed_tasks["task_2"]["state"] == "DONE"
    assert completed_tasks["task_2"]["stdout"] == "output2"
    assert completed_tasks["task_2"]["return_value"] == 42

    # Verify task_3
    assert completed_tasks["task_3"]["state"] == "FAILED"
    assert completed_tasks["task_3"]["stderr"] == "error"
    assert "exception" in completed_tasks["task_3"]


@pytest.mark.asyncio
async def test_wait_tasks_deduplication_and_terminal_states():
    """Test that wait_tasks deduplicates tasks and handles terminal states correctly."""
    import asyncio
    from rhapsody.backends.base import BaseExecutionBackend

    # Simulate results list that gets populated over time
    results = []

    # Simulate async task completion in background
    async def simulate_task_completion():
        await asyncio.sleep(0.05)
        results.append(({"uid": "task_1"}, "RUNNING"))  # Non-terminal state
        await asyncio.sleep(0.05)
        results.append(({"uid": "task_1", "stdout": "done", "return_value": 0}, "DONE"))  # Terminal state
        results.append(({"uid": "task_1", "stdout": "done", "return_value": 0}, "DONE"))  # Duplicate - should be ignored
        await asyncio.sleep(0.05)
        results.append(({"uid": "task_2", "stdout": "canceled"}, "CANCELED"))  # Different spelling
        await asyncio.sleep(0.05)
        results.append(({"uid": "task_3", "stdout": "cancelled"}, "CANCELLED"))  # Alternative spelling

    # Start background task
    completion_task = asyncio.create_task(simulate_task_completion())

    # Wait for tasks to complete
    completed_tasks = await BaseExecutionBackend.wait_tasks(
        results, 3, timeout=5.0, sleep_interval=0.05, verbose=False
    )

    # Wait for background task
    await completion_task

    # Verify deduplication and correct final states
    assert len(completed_tasks) == 3, "Should have exactly 3 unique tasks"
    assert completed_tasks["task_1"]["state"] == "DONE"
    assert completed_tasks["task_1"]["stdout"] == "done"
    assert completed_tasks["task_1"]["return_value"] == 0
    assert completed_tasks["task_2"]["state"] == "CANCELED"
    assert completed_tasks["task_2"]["stdout"] == "canceled"
    assert completed_tasks["task_3"]["state"] == "CANCELLED"
    assert completed_tasks["task_3"]["stdout"] == "cancelled"

    # Verify duplicates were ignored (only 3 tasks despite multiple entries for task_1)
    task_1_count = sum(1 for task, _ in results if task.get("uid") == "task_1")
    assert task_1_count >= 2, "Should have multiple entries for task_1 in results"
    assert len(completed_tasks) == 3, "But only 3 unique tasks in completed_tasks"
