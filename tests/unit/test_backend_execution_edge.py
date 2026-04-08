"""Unit tests for EdgeExecutionBackend (refactored: delegates to RhapsodyClient)."""

import asyncio

import pytest
from unittest.mock import MagicMock, patch, AsyncMock

from rhapsody.backends.execution.edge import EdgeExecutionBackend


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_rhapsody_client(sid="session.abc123"):
    """Return a mock RhapsodyClient (PluginClient)."""
    rh = MagicMock()
    rh.sid = sid
    rh.submit_tasks   = MagicMock(return_value=[
        {"uid": "t.001", "state": "SUBMITTED"}])
    rh.cancel_task     = MagicMock(return_value={"uid": "t.001",
                                                 "status": "canceled"})
    rh.cancel_all_tasks = MagicMock(return_value={"canceled": 5})
    rh.close           = MagicMock()
    rh.register_notification_callback = MagicMock()
    return rh


def _mock_bridge_client(rh=None):
    """Return a mock BridgeClient whose chain produces *rh*."""
    if rh is None:
        rh = _mock_rhapsody_client()
    ec = MagicMock()
    ec.get_plugin = MagicMock(return_value=rh)
    bc = MagicMock()
    bc.get_edge_client = MagicMock(return_value=ec)
    bc.close           = MagicMock()
    return bc, rh


def _make_backend(**kwargs):
    """Create an EdgeExecutionBackend (not yet initialised)."""
    defaults = {
        "bridge_url": "http://localhost:8000",
        "edge_name":  "test_edge",
    }
    defaults.update(kwargs)
    return EdgeExecutionBackend(**defaults)


async def _init_backend(**kwargs):
    """Create and initialise a backend with mocked BridgeClient chain."""
    backend = _make_backend(**kwargs)
    bc, rh  = _mock_bridge_client()
    with patch("rhapsody.backends.execution.edge.BridgeClient",
               return_value=bc):
        await backend._async_init()
    # Expose mocks for assertions
    backend._mock_bc = bc
    backend._mock_rh = rh
    return backend


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

def test_edge_backend_construction():
    backend = _make_backend()
    assert backend._bridge_url      == "http://localhost:8000"
    assert backend._edge_name       == "test_edge"
    assert backend._plugin_name     == "rhapsody"
    assert backend._remote_backends == ["dragon_v3"]
    assert backend._initialized     is False


def test_edge_backend_custom_params():
    backend = _make_backend(
        plugin_name="my_rhapsody",
        backends=["dragon_v3"],
        name="my_edge",
    )
    assert backend._plugin_name     == "my_rhapsody"
    assert backend._remote_backends == ["dragon_v3"]
    assert backend.name             == "my_edge"


# ---------------------------------------------------------------------------
# Async init
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_async_init_creates_client_chain():
    backend = await _init_backend()

    assert backend._initialized is True
    assert backend._bc is not None
    assert backend._rh is not None

    # get_edge_client called with the edge name
    backend._mock_bc.get_edge_client.assert_called_once_with("test_edge")
    # get_plugin called with plugin name + backends
    ec = backend._mock_bc.get_edge_client.return_value
    ec.get_plugin.assert_called_once_with("rhapsody",
                                          backends=["dragon_v3"])

    # Notification callbacks registered
    calls = backend._mock_rh.register_notification_callback.call_args_list
    topics = [c[1]["topic"] for c in calls]
    assert "task_status"       in topics
    assert "task_status_batch" in topics


@pytest.mark.asyncio
async def test_async_init_idempotent():
    backend = await _init_backend()
    rh      = backend._mock_rh

    await backend._async_init()
    # register_notification_callback should NOT be called again
    assert rh.register_notification_callback.call_count == 2  # initial only


# ---------------------------------------------------------------------------
# submit_tasks
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_submit_tasks_delegates_to_rhapsody_client():
    backend = await _init_backend()

    tasks = [{"uid": "t.001", "executable": "/bin/echo",
              "arguments": ["hi"]}]
    await backend.submit_tasks(tasks)

    backend._mock_rh.submit_tasks.assert_called_once()
    submitted = backend._mock_rh.submit_tasks.call_args[0][0]
    assert len(submitted)      == 1
    assert submitted[0]["uid"] == "t.001"

    # Task tracked locally
    assert "t.001" in backend._tasks


@pytest.mark.asyncio
async def test_submit_tasks_sets_running_state():
    backend = await _init_backend()
    assert await backend.state() == "INITIALIZED"

    await backend.submit_tasks([{"uid": "t.1", "executable": "/bin/true"}])
    assert await backend.state() == "RUNNING"


# ---------------------------------------------------------------------------
# cancel_task
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cancel_task():
    backend = await _init_backend()
    backend._tasks["t.001"] = {"uid": "t.001", "state": "RUNNING"}

    result = await backend.cancel_task("t.001")
    assert result is True
    assert backend._tasks["t.001"]["state"] == "CANCELED"
    backend._mock_rh.cancel_task.assert_called_once_with("t.001")


@pytest.mark.asyncio
async def test_cancel_unknown_task():
    backend = await _init_backend()
    result  = await backend.cancel_task("no_such_task")
    assert result is False


# ---------------------------------------------------------------------------
# cancel_all_tasks
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cancel_all_tasks():
    backend = await _init_backend()
    count   = await backend.cancel_all_tasks()
    assert count == 5
    backend._mock_rh.cancel_all_tasks.assert_called_once()


# ---------------------------------------------------------------------------
# shutdown
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_shutdown_closes_clients():
    backend = await _init_backend()
    await backend.shutdown()

    backend._mock_rh.close.assert_called_once()
    backend._mock_bc.close.assert_called_once()
    assert backend._rh is None
    assert backend._bc is None
    assert await backend.state() == "SHUTDOWN"


# ---------------------------------------------------------------------------
# Notification handling
# ---------------------------------------------------------------------------

def test_on_task_notification_single():
    backend = _make_backend()
    backend._tasks["t.001"] = {"uid": "t.001", "state": "SUBMITTED"}

    backend._on_task_notification(
        edge="hpc1", plugin="rhapsody",
        topic="task_status",
        data={"uid": "t.001", "state": "DONE",
              "stdout": "hello\n", "exit_code": 0},
    )

    assert backend._tasks["t.001"]["state"]   == "DONE"
    assert backend._tasks["t.001"]["stdout"]   == "hello\n"


def test_on_task_notification_batch():
    backend = _make_backend()
    backend._tasks["t.001"] = {"uid": "t.001", "state": "SUBMITTED"}
    backend._tasks["t.002"] = {"uid": "t.002", "state": "SUBMITTED"}

    backend._on_task_notification(
        edge="hpc1", plugin="rhapsody",
        topic="task_status_batch",
        data={"tasks": [
            {"uid": "t.001", "state": "DONE"},
            {"uid": "t.002", "state": "FAILED", "error": "boom"},
        ]},
    )

    assert backend._tasks["t.001"]["state"] == "DONE"
    assert backend._tasks["t.002"]["state"] == "FAILED"
    assert backend._tasks["t.002"]["error"] == "boom"


def test_on_task_notification_ignores_unknown_task():
    backend = _make_backend()
    # No tasks registered — should not crash
    backend._on_task_notification(
        edge="hpc1", plugin="rhapsody",
        topic="task_status",
        data={"uid": "unknown", "state": "DONE"},
    )


# ---------------------------------------------------------------------------
# state / context manager
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_state():
    backend = await _init_backend()
    assert await backend.state() == "INITIALIZED"


@pytest.mark.asyncio
async def test_context_manager():
    backend = _make_backend()
    bc, rh  = _mock_bridge_client()
    with patch("rhapsody.backends.execution.edge.BridgeClient",
               return_value=bc):
        async with backend as b:
            assert b._initialized is True
        assert await b.state() == "SHUTDOWN"
