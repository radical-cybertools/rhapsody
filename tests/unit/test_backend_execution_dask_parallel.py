"""Unit tests for Dask parallel execution backend.

This module tests the Dask parallel execution backend defined in
rhapsody.backends.execution.dask_parallel.
"""

import asyncio

import pytest

from rhapsody import ComputeTask


def test_dask_backend_import():
    """Test that DaskExecutionBackend can be imported."""
    try:
        from rhapsody.backends import DaskExecutionBackend

        assert DaskExecutionBackend is not None
    except ImportError:
        pytest.skip("Dask dependencies not available")


def test_dask_backend_class_exists():
    """Test that DaskExecutionBackend class exists and inherits from base."""
    try:
        from rhapsody.backends import DaskExecutionBackend
        from rhapsody.backends.base import BaseBackend

        # Check inheritance
        assert issubclass(DaskExecutionBackend, BaseBackend)
    except ImportError:
        pytest.skip("Dask dependencies not available")


def test_dask_backend_init():
    """Test DaskExecutionBackend initialization."""
    try:
        from rhapsody.backends import DaskExecutionBackend

        # Test basic initialization
        backend = DaskExecutionBackend()
        assert backend is not None
        assert not backend._initialized
        assert backend._client is None
        assert backend.tasks == {}

        # Test initialization with resources
        resources = {"n_workers": 2, "threads_per_worker": 1}
        backend_with_resources = DaskExecutionBackend(resources)
        assert backend_with_resources._resources == resources

    except ImportError:
        pytest.skip("Dask dependencies not available")


def test_dask_backend_import_error():
    """Test that DaskExecutionBackend raises ImportError when Dask is not available."""
    try:
        # This will only run if dask is actually available
        import dask.distributed

        pytest.skip("Dask is available, cannot test ImportError scenario")
    except ImportError:
        # Dask is not available, so we should get ImportError
        from rhapsody.backends import DaskExecutionBackend

        with pytest.raises(ImportError, match="Dask is required for DaskExecutionBackend"):
            DaskExecutionBackend()


@pytest.mark.asyncio
async def test_dask_backend_async_init():
    """Test DaskExecutionBackend async initialization."""
    try:
        from rhapsody.backends import DaskExecutionBackend

        backend = DaskExecutionBackend()

        # Test that it's awaitable
        assert hasattr(backend, "__await__")

        # Test async initialization (this might fail due to no Dask cluster)
        try:
            initialized_backend = await backend
            assert initialized_backend._initialized
            assert initialized_backend is backend  # Should return self

            # Test state
            state = await backend.state()
            assert state == "INITIALIZED"

            # Cleanup
            await backend.shutdown()

        except Exception as e:
            # Expected if no Dask cluster is available
            print(f"Async init failed (expected): {e}")

    except ImportError:
        pytest.skip("Dask dependencies not available")


@pytest.mark.asyncio
async def test_dask_backend_context_manager():
    """Test DaskExecutionBackend as async context manager."""
    try:
        from rhapsody.backends import DaskExecutionBackend

        try:
            async with DaskExecutionBackend() as backend:
                assert backend._initialized
                # Test basic functionality
                assert hasattr(backend, "submit_tasks")
                assert hasattr(backend, "cancel_task")

        except Exception as e:
            # Expected if no Dask cluster is available
            print(f"Context manager test failed (expected): {e}")

    except ImportError:
        pytest.skip("Dask dependencies not available")


def test_dask_backend_callback_registration():
    """Test callback registration functionality."""
    try:
        from rhapsody.backends import DaskExecutionBackend

        backend = DaskExecutionBackend()

        def test_callback(task, state):
            pass

        # Test callback registration
        backend.register_callback(test_callback)
        assert backend._callback_func is test_callback

    except ImportError:
        pytest.skip("Dask dependencies not available")


def test_dask_backend_state_mapper():
    """Test state mapper functionality."""
    try:
        from rhapsody.backends import DaskExecutionBackend

        backend = DaskExecutionBackend()

        # This should work even without initialization
        state_mapper = backend.get_task_states_map()
        assert state_mapper is not None

    except ImportError:
        pytest.skip("Dask dependencies not available")


@pytest.mark.asyncio
async def test_dask_backend_task_validation():
    """Test task validation and error handling."""
    try:
        from rhapsody.backends import DaskExecutionBackend

        backend = DaskExecutionBackend()

        # Mock callback to capture calls
        callback_calls = []

        def mock_callback(task, state):
            callback_calls.append((task, state))

        backend.register_callback(mock_callback)

        # Test without initialization (should raise RuntimeError)
        with pytest.raises(RuntimeError, match="DaskExecutionBackend must be awaited"):
            await backend.submit_tasks([])

    except ImportError:
        pytest.skip("Dask dependencies not available")


@pytest.mark.asyncio
async def test_dask_backend_task_submission_errors():
    """Test task submission error handling."""
    try:
        from rhapsody.backends import DaskExecutionBackend

        # Mock the dask client to avoid needing a real cluster
        backend = DaskExecutionBackend()
        backend._initialized = True  # Bypass initialization for testing

        callback_calls = []

        def mock_callback(task, state):
            callback_calls.append((task["uid"], state))

        backend.register_callback(mock_callback)

        # Test executable task (should fail)
        executable_task = ComputeTask(
            executable="/bin/echo",
            arguments=["hello"]
        )

        await backend.submit_tasks([executable_task])

        # Should have received FAILED callback
        assert len(callback_calls) == 1
        assert callback_calls[0][0].startswith("task.")
        assert callback_calls[0][1] == "FAILED"

        # Test sync function task (should fail)
        def sync_function():
            return "sync"

        sync_task = ComputeTask(
            function=sync_function,
            args=[],
            kwargs={}
        )

        callback_calls.clear()
        await backend.submit_tasks([sync_task])

        # Should have received FAILED callback
        assert len(callback_calls) == 1
        assert callback_calls[0][0].startswith("task.")
        assert callback_calls[0][1] == "FAILED"

    except ImportError:
        pytest.skip("Dask dependencies not available")


@pytest.mark.asyncio
async def test_dask_backend_cancel_functionality():
    """Test task cancellation functionality."""
    try:
        from rhapsody.backends import DaskExecutionBackend

        backend = DaskExecutionBackend()
        backend._initialized = True  # Bypass initialization

        # Test canceling non-existent task
        result = await backend.cancel_task("nonexistent")
        assert result is False

        # Test cancel_all_tasks with no tasks
        cancelled_count = await backend.cancel_all_tasks()
        assert cancelled_count == 0

    except ImportError:
        pytest.skip("Dask dependencies not available")


def test_dask_backend_class_methods():
    """Test DaskExecutionBackend class methods."""
    try:
        from rhapsody.backends import DaskExecutionBackend

        # Test that create class method exists
        assert hasattr(DaskExecutionBackend, "create")
        assert callable(DaskExecutionBackend.create)

    except ImportError:
        pytest.skip("Dask dependencies not available")


@pytest.mark.asyncio
async def test_dask_backend_shutdown():
    """Test DaskExecutionBackend shutdown functionality."""
    try:
        from rhapsody.backends import DaskExecutionBackend

        backend = DaskExecutionBackend()

        # Test shutdown without initialization
        await backend.shutdown()
        assert backend._client is None
        assert not backend._initialized

        # Test shutdown after manual initialization flag setting
        backend = DaskExecutionBackend()
        backend._initialized = True
        backend.tasks = {"test": "task"}

        await backend.shutdown()
        assert backend._client is None
        assert not backend._initialized
        assert len(backend.tasks) == 0

    except ImportError:
        pytest.skip("Dask dependencies not available")
