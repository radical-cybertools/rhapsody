"""Unit tests for Ensemble Launcher execution backend."""

import asyncio
from concurrent.futures import Future as ConcurrentFuture
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from rhapsody import ComputeTask

try:
    from rhapsody.backends.execution.el import EnsembleExecutionBackend

    _el_available = True
except ImportError:
    _el_available = False

pytestmark = pytest.mark.skipif(not _el_available, reason="ensemble_launcher not installed")


# ---------------------------------------------------------------------------
# Import and class structure
# ---------------------------------------------------------------------------


def test_el_backend_import():
    try:
        from rhapsody.backends.execution import EnsembleExecutionBackend
    except ModuleNotFoundError:
        raise ModuleNotFoundError


def test_el_backend_inherits_base():
    from rhapsody.backends.base import BaseBackend

    assert issubclass(EnsembleExecutionBackend, BaseBackend)


# ---------------------------------------------------------------------------
# __init__
# ---------------------------------------------------------------------------


@patch("rhapsody.backends.execution.el.get_nodes", return_value=["node0"])
def test_el_backend_init_defaults(mock_nodes):
    backend = EnsembleExecutionBackend()
    assert backend is not None
    assert not backend._initialized
    assert backend._client is None
    assert backend._el is None
    assert backend.tasks == {}
    assert backend._client_only is False
    assert backend._node_id == "global"


@patch("rhapsody.backends.execution.el.get_nodes", return_value=["node0"])
def test_el_backend_init_client_only(mock_nodes):
    backend = EnsembleExecutionBackend(
        client_only=True, node_id="main.w0", checkpoint_dir="/tmp/ckpt"
    )
    assert backend._client_only is True
    assert backend._node_id == "main.w0"


@patch("rhapsody.backends.execution.el.get_nodes", return_value=["node0"])
def test_el_backend_init_custom_name(mock_nodes):
    backend = EnsembleExecutionBackend(name="my_el")
    assert backend.name == "my_el"


@patch("rhapsody.backends.execution.el.get_nodes", return_value=["node0"])
def test_el_backend_init_default_name(mock_nodes):
    backend = EnsembleExecutionBackend()
    assert backend.name == "EnsembleExecutionBackend"


# ---------------------------------------------------------------------------
# Awaitable / async init
# ---------------------------------------------------------------------------


@patch("rhapsody.backends.execution.el.get_nodes", return_value=["node0"])
def test_el_backend_is_awaitable(mock_nodes):
    backend = EnsembleExecutionBackend()
    assert hasattr(backend, "__await__")


@pytest.mark.asyncio
@patch("rhapsody.backends.execution.el.get_nodes", return_value=["node0"])
async def test_el_backend_async_init(mock_nodes):
    backend = EnsembleExecutionBackend()

    mock_el = MagicMock()
    mock_client = MagicMock()

    with (
        patch("rhapsody.backends.execution.el.EnsembleLauncher", return_value=mock_el),
        patch("rhapsody.backends.execution.el.ClusterClient", return_value=mock_client),
        patch("rhapsody.backends.execution.el.asyncio.to_thread", side_effect=_sync_to_thread),
    ):
        result = await backend
        assert result is backend
        assert backend._initialized
        mock_el.start.assert_called_once_with(wait_time=5)
        mock_client.start.assert_called_once()


@pytest.mark.asyncio
@patch("rhapsody.backends.execution.el.get_nodes", return_value=["node0"])
async def test_el_backend_async_init_client_only(mock_nodes):
    backend = EnsembleExecutionBackend(client_only=True, checkpoint_dir="/tmp/ckpt")

    mock_client = MagicMock()

    with (
        patch("rhapsody.backends.execution.el.EnsembleLauncher") as mock_el_cls,
        patch("rhapsody.backends.execution.el.ClusterClient", return_value=mock_client),
        patch("rhapsody.backends.execution.el.asyncio.to_thread", side_effect=_sync_to_thread),
    ):
        await backend
        assert backend._initialized
        assert backend._el is None
        mock_el_cls.assert_not_called()
        mock_client.start.assert_called_once()


# ---------------------------------------------------------------------------
# Context manager
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@patch("rhapsody.backends.execution.el.get_nodes", return_value=["node0"])
async def test_el_backend_context_manager(mock_nodes):
    backend = EnsembleExecutionBackend()

    mock_el = MagicMock()
    mock_client = MagicMock()

    with (
        patch("rhapsody.backends.execution.el.EnsembleLauncher", return_value=mock_el),
        patch("rhapsody.backends.execution.el.ClusterClient", return_value=mock_client),
        patch("rhapsody.backends.execution.el.asyncio.to_thread", side_effect=_sync_to_thread),
    ):
        async with backend as b:
            assert b._initialized
            assert b is backend

    assert not backend._initialized
    mock_client.teardown.assert_called_once()
    mock_el.stop.assert_called_once()


# ---------------------------------------------------------------------------
# Callback registration
# ---------------------------------------------------------------------------


@patch("rhapsody.backends.execution.el.get_nodes", return_value=["node0"])
def test_el_backend_callback_registration(mock_nodes):
    backend = EnsembleExecutionBackend()

    def my_cb(task, state):
        pass

    backend.register_callback(my_cb)
    assert backend._callback_func is my_cb


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@patch("rhapsody.backends.execution.el.get_nodes", return_value=["node0"])
async def test_el_backend_state(mock_nodes):
    backend = EnsembleExecutionBackend()
    state = await backend.state()
    assert state == "INITIALIZED"


# ---------------------------------------------------------------------------
# State mapper
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@patch("rhapsody.backends.execution.el.get_nodes", return_value=["node0"])
async def test_el_backend_state_mapper(mock_nodes):
    backend = EnsembleExecutionBackend()

    with (
        patch("rhapsody.backends.execution.el.EnsembleLauncher", return_value=MagicMock()),
        patch("rhapsody.backends.execution.el.ClusterClient", return_value=MagicMock()),
        patch("rhapsody.backends.execution.el.asyncio.to_thread", side_effect=_sync_to_thread),
    ):
        await backend

    mapper = backend.get_task_states_map()
    assert mapper is not None


# ---------------------------------------------------------------------------
# submit_tasks — not initialized guard
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@patch("rhapsody.backends.execution.el.get_nodes", return_value=["node0"])
async def test_el_backend_submit_not_initialized(mock_nodes):
    backend = EnsembleExecutionBackend()

    with pytest.raises(RuntimeError, match="EnsembleExecutionBackend must be awaited"):
        await backend.submit_tasks([])


# ---------------------------------------------------------------------------
# submit_tasks — happy path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@patch("rhapsody.backends.execution.el.get_nodes", return_value=["node0"])
async def test_el_backend_submit_tasks(mock_nodes):
    backend = EnsembleExecutionBackend()
    backend._initialized = True
    from rhapsody.backends.constants import BackendMainStates

    backend._backend_state = BackendMainStates.INITIALIZED

    captured = []
    backend.register_callback(lambda t, s: captured.append((t["uid"], s)))

    fut = ConcurrentFuture()
    mock_client = MagicMock()
    mock_client.submit.return_value = fut
    backend._client = mock_client

    task = ComputeTask(
        function=lambda x: x * 2,
        args=(5,),
        kwargs={},
    )

    await backend.submit_tasks([task])

    assert task["uid"] in backend.tasks
    # future stored is the asyncio.Task wrapping _handle_task
    assert isinstance(backend.tasks[task["uid"]]["future"], asyncio.Task)
    state = await backend.state()
    assert state == "RUNNING"

    # Resolve the underlying future so the task completes
    fut.set_result(10)
    await backend.tasks[task["uid"]]["future"]

    states = [s for _, s in captured]
    assert "RUNNING" in states
    assert "DONE" in states


@pytest.mark.asyncio
@patch("rhapsody.backends.execution.el.get_nodes", return_value=["node0"])
async def test_el_submit_multiple_tasks_complete(mock_nodes):
    """All submitted tasks complete and fire callbacks."""
    backend = EnsembleExecutionBackend()
    backend._initialized = True
    from rhapsody.backends.constants import BackendMainStates

    backend._backend_state = BackendMainStates.INITIALIZED
    backend.register_callback(lambda t, s: None)

    mock_client = MagicMock()
    futs = [ConcurrentFuture() for _ in range(3)]
    mock_client.submit.side_effect = futs
    backend._client = mock_client

    tasks = [ComputeTask(function=lambda: 1, args=(), kwargs={}) for _ in range(3)]

    await backend.submit_tasks(tasks)
    async_tasks = [backend.tasks[t["uid"]]["future"] for t in tasks]

    for f in futs:
        f.set_result(42)
    await asyncio.gather(*async_tasks)

    for t in tasks:
        assert t["return_value"] == 42


# ---------------------------------------------------------------------------
# submit_tasks — shutdown guard
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@patch("rhapsody.backends.execution.el.get_nodes", return_value=["node0"])
async def test_el_backend_submit_after_shutdown(mock_nodes):
    backend = EnsembleExecutionBackend()
    backend._initialized = True
    from rhapsody.backends.constants import BackendMainStates

    backend._backend_state = BackendMainStates.SHUTDOWN

    with pytest.raises(RuntimeError, match="Cannot submit during shutdown"):
        await backend.submit_tasks([])


# ---------------------------------------------------------------------------
# build_task
# ---------------------------------------------------------------------------


@patch("rhapsody.backends.execution.el.get_nodes", return_value=["node0"])
def test_el_build_task_sync_function(mock_nodes):
    from ensemble_launcher.ensemble import Task as ELTask

    backend = EnsembleExecutionBackend()

    def my_func(a, b):
        return a + b

    task = ComputeTask(
        function=my_func,
        args=(1, 2),
        kwargs={},
        task_backend_specific_kwargs={"nnodes": 2, "ranks": 4, "gpus_per_rank": 1},
    )

    el_task = backend.build_task(task)
    assert isinstance(el_task, ELTask)
    assert el_task.nnodes == 2
    assert el_task.ppn == 2
    assert el_task.ngpus_per_process == 1


@patch("rhapsody.backends.execution.el.get_nodes", return_value=["node0"])
def test_el_build_task_async_function(mock_nodes):
    from ensemble_launcher.ensemble import AsyncTask as AsyncELTask

    backend = EnsembleExecutionBackend()

    async def my_async_func(a):
        return a

    task = ComputeTask(
        function=my_async_func,
        args=(1,),
        kwargs={},
    )

    el_task = backend.build_task(task)
    assert isinstance(el_task, AsyncELTask)


@patch("rhapsody.backends.execution.el.get_nodes", return_value=["node0"])
def test_el_build_task_executable(mock_nodes):
    from ensemble_launcher.ensemble import Task as ELTask

    backend = EnsembleExecutionBackend()

    task = ComputeTask(
        executable="/bin/echo",
        args=("hello",),
        kwargs={},
    )

    el_task = backend.build_task(task)
    assert isinstance(el_task, ELTask)
    assert el_task.executable == "/bin/echo"


@patch("rhapsody.backends.execution.el.get_nodes", return_value=["node0"])
def test_el_build_task_default_resources(mock_nodes):
    backend = EnsembleExecutionBackend()

    task = ComputeTask(
        function=lambda: 1,
        args=(),
        kwargs={},
    )

    el_task = backend.build_task(task)
    assert el_task.nnodes == 1
    assert el_task.ppn == 1
    assert el_task.ngpus_per_process == 0


# ---------------------------------------------------------------------------
# cancel_task
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@patch("rhapsody.backends.execution.el.get_nodes", return_value=["node0"])
async def test_el_cancel_task_success(mock_nodes):
    backend = EnsembleExecutionBackend()

    mock_future = MagicMock()
    mock_future.cancel.return_value = True
    backend.tasks["task-1"] = {"uid": "task-1", "future": mock_future}

    result = await backend.cancel_task("task-1")
    assert result is True
    mock_future.cancel.assert_called_once()


@pytest.mark.asyncio
@patch("rhapsody.backends.execution.el.get_nodes", return_value=["node0"])
async def test_el_cancel_task_not_found(mock_nodes):
    backend = EnsembleExecutionBackend()
    result = await backend.cancel_task("nonexistent")
    assert result is False


# ---------------------------------------------------------------------------
# shutdown
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@patch("rhapsody.backends.execution.el.get_nodes", return_value=["node0"])
async def test_el_shutdown(mock_nodes):
    backend = EnsembleExecutionBackend()
    backend._initialized = True

    mock_client = MagicMock()
    mock_el = MagicMock()
    backend._client = mock_client
    backend._el = mock_el
    backend.tasks = {"t1": {}}

    await backend.shutdown()

    mock_client.teardown.assert_called_once()
    mock_el.stop.assert_called_once()
    assert backend._client is None
    assert backend._el is None
    assert len(backend.tasks) == 0
    assert not backend._initialized
    state = await backend.state()
    assert state == "SHUTDOWN"


@pytest.mark.asyncio
@patch("rhapsody.backends.execution.el.get_nodes", return_value=["node0"])
async def test_el_shutdown_client_only(mock_nodes):
    backend = EnsembleExecutionBackend(client_only=True, checkpoint_dir="/tmp/ckpt")
    backend._initialized = True

    mock_client = MagicMock()
    backend._client = mock_client
    backend._el = None

    await backend.shutdown()

    mock_client.teardown.assert_called_once()
    assert backend._client is None
    assert not backend._initialized


@pytest.mark.asyncio
@patch("rhapsody.backends.execution.el.get_nodes", return_value=["node0"])
async def test_el_shutdown_without_init(mock_nodes):
    backend = EnsembleExecutionBackend()
    await backend.shutdown()
    assert not backend._initialized


# ---------------------------------------------------------------------------
# Callback firing (done / failed)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@patch("rhapsody.backends.execution.el.get_nodes", return_value=["node0"])
async def test_el_callback_done(mock_nodes):
    backend = EnsembleExecutionBackend()
    backend._initialized = True
    from rhapsody.backends.constants import BackendMainStates

    backend._backend_state = BackendMainStates.INITIALIZED

    captured = []
    backend.register_callback(lambda t, s: captured.append((t["uid"], s)))

    fut = ConcurrentFuture()
    mock_client = MagicMock()
    mock_client.submit.return_value = fut
    backend._client = mock_client

    task = ComputeTask(function=lambda: 42, args=(), kwargs={})
    await backend.submit_tasks([task])

    fut.set_result(42)
    await backend.tasks[task["uid"]]["future"]

    states = [s for _, s in captured]
    assert "RUNNING" in states
    assert "DONE" in states
    assert task["return_value"] == 42
    assert task["stdout"] == ""
    assert task["stderr"] == ""


@pytest.mark.asyncio
@patch("rhapsody.backends.execution.el.get_nodes", return_value=["node0"])
async def test_el_callback_failed(mock_nodes):
    backend = EnsembleExecutionBackend()
    backend._initialized = True
    from rhapsody.backends.constants import BackendMainStates

    backend._backend_state = BackendMainStates.INITIALIZED

    captured = []
    backend.register_callback(lambda t, s: captured.append((t["uid"], s)))

    fut = ConcurrentFuture()
    mock_client = MagicMock()
    mock_client.submit.return_value = fut
    backend._client = mock_client

    task = ComputeTask(function=lambda: 1, args=(), kwargs={})
    await backend.submit_tasks([task])

    fut.set_exception(ValueError("boom"))
    await backend.tasks[task["uid"]]["future"]

    states = [s for _, s in captured]
    assert "RUNNING" in states
    assert "FAILED" in states
    assert isinstance(task["exception"], ValueError)
    assert task["stdout"] == ""
    assert "boom" in task["stderr"]


# ---------------------------------------------------------------------------
# task_state_cb
# ---------------------------------------------------------------------------


@patch("rhapsody.backends.execution.el.get_nodes", return_value=["node0"])
def test_el_task_state_cb(mock_nodes):
    backend = EnsembleExecutionBackend()
    captured = []
    backend.register_callback(lambda t, s: captured.append(s))

    backend.task_state_cb({"uid": "x"}, "RUNNING")
    assert captured == ["RUNNING"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _sync_to_thread(fn, /, *args, **kwargs):
    """Drop-in replacement for asyncio.to_thread that runs synchronously."""
    return fn(*args, **kwargs)
