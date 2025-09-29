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
