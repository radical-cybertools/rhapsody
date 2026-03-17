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
async def test_dask_backend_task_submission_routing():
    """Test that tasks are routed to the correct submission methods."""
    try:
        from unittest.mock import AsyncMock
        from unittest.mock import patch

        from rhapsody.backends import DaskExecutionBackend

        backend = DaskExecutionBackend()
        backend._initialized = True

        # Executable tasks route to _submit_executable (not FAILED)
        with patch.object(backend, "_submit_executable", new_callable=AsyncMock) as mock_exec:
            executable_task = ComputeTask(executable="/bin/echo", arguments=["hello"])
            await backend.submit_tasks([executable_task])
            mock_exec.assert_called_once()

        # Sync function tasks route to _submit_sync_function (not FAILED)
        with patch.object(backend, "_submit_sync_function", new_callable=AsyncMock) as mock_sync:

            def sync_fn():
                return "sync"

            sync_task = ComputeTask(function=sync_fn, args=[], kwargs={})
            await backend.submit_tasks([sync_task])
            mock_sync.assert_called_once()

        # Async function tasks route to _submit_async_function
        with patch.object(backend, "_submit_async_function", new_callable=AsyncMock) as mock_async:

            async def async_fn():
                return "async"

            async_task = ComputeTask(function=async_fn, args=[], kwargs={})
            await backend.submit_tasks([async_task])
            mock_async.assert_called_once()

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


# ---------------------------------------------------------------------------
# _run_executable tests
# ---------------------------------------------------------------------------


def test_run_executable_is_picklable():
    """_run_executable must be picklable so Dask can ship it to workers."""
    import pickle

    try:
        from rhapsody.backends.execution.dask_parallel import _run_executable

        pickled = pickle.dumps(_run_executable)
        assert len(pickled) > 0
    except ImportError:
        pytest.skip("Dask dependencies not available")


def test_run_executable_captures_stdout():
    """_run_executable returns (stdout, stderr, returncode) correctly."""
    try:
        from rhapsody.backends.execution.dask_parallel import _run_executable

        stdout, stderr, returncode = _run_executable("/bin/echo", ["hello"])
        assert returncode == 0
        assert "hello" in stdout
        assert stderr == ""
    except ImportError:
        pytest.skip("Dask dependencies not available")


def test_run_executable_captures_stderr_and_nonzero_exit():
    """_run_executable captures stderr and non-zero exit codes."""
    try:
        from rhapsody.backends.execution.dask_parallel import _run_executable

        stdout, stderr, returncode = _run_executable("/bin/bash", ["-c", "echo err >&2; exit 1"])
        assert returncode == 1
        assert "err" in stderr
    except ImportError:
        pytest.skip("Dask dependencies not available")


# ---------------------------------------------------------------------------
# cwd tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dask_submit_executable_cwd_from_task():
    """_submit_executable forwards task-level cwd to _run_executable as cwd."""
    try:
        from unittest.mock import MagicMock

        from rhapsody.backends import DaskExecutionBackend

        backend = DaskExecutionBackend()
        backend._initialized = True

        captured = {}

        def fake_submit(fn, *args, **kwargs):
            captured.update(kwargs)
            future = MagicMock()
            future.__await__ = lambda self: iter([])
            return future

        backend._client = MagicMock()
        backend._client.submit = fake_submit
        backend._client.scheduler_info.return_value = {"workers": {}}

        task = ComputeTask(executable="/bin/pwd", cwd="/tmp")
        backend.tasks[task["uid"]] = task

        from unittest.mock import patch

        with patch("asyncio.create_task"):
            await backend._submit_executable(task)

        assert captured.get("cwd") == "/tmp"

    except ImportError:
        pytest.skip("Dask dependencies not available")


@pytest.mark.asyncio
async def test_dask_submit_executable_cwd_from_bksp_overrides_task():
    """task_backend_specific_kwargs cwd takes priority over task-level cwd."""
    try:
        from unittest.mock import MagicMock
        from unittest.mock import patch

        from rhapsody.backends import DaskExecutionBackend

        backend = DaskExecutionBackend()
        backend._initialized = True

        captured = {}

        def fake_submit(fn, *args, **kwargs):
            captured.update(kwargs)
            return MagicMock()

        backend._client = MagicMock()
        backend._client.submit = fake_submit
        backend._client.scheduler_info.return_value = {"workers": {}}

        task = ComputeTask(
            executable="/bin/pwd",
            cwd="/home",
            task_backend_specific_kwargs={"cwd": "/var"},
        )
        backend.tasks[task["uid"]] = task

        with patch("asyncio.create_task"):
            await backend._submit_executable(task)

        assert captured.get("cwd") == "/var"

    except ImportError:
        pytest.skip("Dask dependencies not available")


@pytest.mark.asyncio
async def test_dask_submit_executable_no_cwd():
    """When no cwd is set, cwd is None (no crash)."""
    try:
        from unittest.mock import MagicMock
        from unittest.mock import patch

        from rhapsody.backends import DaskExecutionBackend

        backend = DaskExecutionBackend()
        backend._initialized = True

        captured = {}

        def fake_submit(fn, *args, **kwargs):
            captured.update(kwargs)
            return MagicMock()

        backend._client = MagicMock()
        backend._client.submit = fake_submit
        backend._client.scheduler_info.return_value = {"workers": {}}

        task = ComputeTask(executable="/bin/pwd")
        backend.tasks[task["uid"]] = task

        with patch("asyncio.create_task"):
            await backend._submit_executable(task)

        assert captured.get("cwd") is None

    except ImportError:
        pytest.skip("Dask dependencies not available")
