"""Basic tests for Rhapsody backend system.

This module contains basic tests to verify that the backend extraction and setup is working
correctly.
"""

import pytest

import rhapsody


def test_basic_imports():
    """Test that basic imports work."""
    from rhapsody.backends import discover_backends
    from rhapsody.backends import get_backend
    from rhapsody.backends.base import BaseExecutionBackend
    from rhapsody.backends.constants import StateMapper
    from rhapsody.backends.constants import TasksMainStates

    assert get_backend is not None
    assert discover_backends is not None
    assert BaseExecutionBackend is not None
    assert TasksMainStates is not None
    assert StateMapper is not None


def test_discover_backends():
    """Test backend discovery."""
    available = rhapsody.discover_backends()

    # These should always be available
    assert "dask" in available  # May be True or False
    assert "radical_pilot" in available  # May be True or False

    print(f"Available backends: {available}")


def test_backend_registry():
    """Test backend registry functionality."""
    registry = rhapsody.BackendRegistry

    backends = registry.list_backends()
    assert len(backends) >= 2  # At least dask and radical_pilot

    # Test getting a backend class for each available backend
    for backend_name in backends:
        backend_class = registry.get_backend_class(backend_name)
        assert backend_class is not None

    # Test error on invalid backend
    with pytest.raises(ValueError):
        registry.get_backend_class("nonexistent")


if __name__ == "__main__":
    # Run basic tests
    test_basic_imports()
    test_discover_backends()
    test_backend_registry()

    print("All basic tests passed!")
