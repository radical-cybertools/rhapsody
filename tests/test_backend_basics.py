"""
Basic tests for Rhapsody backend system.

This module contains basic tests to verify that the backend extraction
and setup is working correctly.
"""

import pytest
import rhapsody


def test_basic_imports():
    """Test that basic imports work."""
    from rhapsody.backends import get_backend, discover_backends
    from rhapsody.backends.base import BaseExecutionBackend
    from rhapsody.backends.constants import TasksMainStates, StateMapper

    assert get_backend is not None
    assert discover_backends is not None
    assert BaseExecutionBackend is not None
    assert TasksMainStates is not None
    assert StateMapper is not None


def test_discover_backends():
    """Test backend discovery."""
    available = rhapsody.discover_backends()

    # These should always be available (no optional deps)
    assert 'noop' in available
    assert 'concurrent' in available

    # These depend on optional packages
    assert 'dask' in available  # May be True or False
    assert 'radical_pilot' in available  # May be True or False

    print(f"Available backends: {available}")


def test_noop_backend():
    """Test that noop backend can be created."""
    backend = rhapsody.get_backend('noop')

    # Should be able to create without error
    assert backend is not None
    assert hasattr(backend, 'submit_tasks')
    assert hasattr(backend, 'shutdown')


def test_backend_registry():
    """Test backend registry functionality."""
    registry = rhapsody.BackendRegistry

    backends = registry.list_backends()
    assert len(backends) >= 2  # At least noop and concurrent

    # Test getting a backend class
    noop_class = registry.get_backend_class('noop')
    assert noop_class is not None

    # Test error on invalid backend
    with pytest.raises(ValueError):
        registry.get_backend_class('nonexistent')


@pytest.mark.asyncio
async def test_noop_backend_basic_usage():
    """Test basic noop backend usage."""
    backend = rhapsody.get_backend('noop')

    # Set up a simple callback
    results = []
    def callback(task, state):
        results.append((task['uid'], state))

    backend.register_callback(callback)

    # Submit a simple task
    tasks = [{
        'uid': 'test_task_001',
        'executable': '/bin/echo',
        'args': ['hello', 'world']
    }]

    await backend.submit_tasks(tasks)

    # Noop backend should immediately mark task as done
    assert len(results) == 1
    assert results[0][0] == 'test_task_001'
    assert results[0][1] == 'DONE'

    await backend.shutdown()


if __name__ == "__main__":
    # Run basic tests
    test_basic_imports()
    test_discover_backends()
    test_noop_backend()
    test_backend_registry()

    print("All basic tests passed!")