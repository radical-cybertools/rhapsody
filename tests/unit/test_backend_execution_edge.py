"""Unit tests for EdgeExecutionBackend."""

import asyncio
import base64

import pytest
from unittest.mock import MagicMock, patch

from rhapsody.backends.execution.edge import EdgeExecutionBackend


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_httpx_client(json_resp=None, status_code=200):
    """Return a mock httpx.Client with configurable responses."""
    if json_resp is None:
        json_resp = {}
    mock_resp = MagicMock()
    mock_resp.status_code = status_code
    mock_resp.is_error = (status_code >= 400)
    mock_resp.json = MagicMock(return_value=json_resp)
    mock_resp.raise_for_status = MagicMock()

    mock_client = MagicMock()
    mock_client.post = MagicMock(return_value=mock_resp)
    mock_client.get = MagicMock(return_value=mock_resp)
    mock_client.close = MagicMock()
    return mock_client


def _make_backend(**kwargs):
    """Create an EdgeExecutionBackend with mocked HTTP client."""
    defaults = {
        "bridge_url": "http://localhost:8000",
        "edge_name": "test_edge",
    }
    defaults.update(kwargs)
    backend = EdgeExecutionBackend(**defaults)
    backend._http = _mock_httpx_client({"sid": "session.abc123"})
    return backend


async def _init_backend(**kwargs):
    """Create and initialize a backend with mocked HTTP."""
    backend = _make_backend(**kwargs)
    # Mock SSE so it doesn't actually start a thread
    with patch.object(backend, '_start_sse_listener'):
        await backend._async_init()
    return backend


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

def test_edge_backend_construction():
    backend = _make_backend()
    assert backend._bridge_url == "http://localhost:8000"
    assert backend._edge_name == "test_edge"
    assert backend._plugin_name == "rhapsody"
    assert backend._remote_backends == ["dragon_v3"]
    assert backend._initialized is False


def test_edge_backend_custom_params():
    backend = _make_backend(
        plugin_name="my_rhapsody",
        backends=["concurrent"],
        name="my_edge",
    )
    assert backend._plugin_name == "my_rhapsody"
    assert backend._remote_backends == ["concurrent"]
    assert backend.name == "my_edge"


# ---------------------------------------------------------------------------
# Async init
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_async_init_registers_session():
    backend = await _init_backend()

    assert backend._initialized is True
    assert backend._sid == "session.abc123"
    assert backend._base_url == "http://localhost:8000/test_edge/rhapsody"

    # Check register_session was called
    call_args = backend._http.post.call_args
    assert "register_session" in call_args[0][0]
    payload = call_args[1]["json"]
    assert payload["backends"] == ["dragon_v3"]


@pytest.mark.asyncio
async def test_async_init_idempotent():
    backend = await _init_backend()
    call_count = backend._http.post.call_count

    await backend._async_init()
    assert backend._http.post.call_count == call_count  # no extra calls


# ---------------------------------------------------------------------------
# Task serialization
# ---------------------------------------------------------------------------

def test_serialize_executable_task():
    """Executable tasks should pass through without cloudpickle."""
    td = EdgeExecutionBackend._serialize_task({
        "uid": "t.001",
        "executable": "/bin/echo",
        "arguments": ["hello"],
    })
    assert td["executable"] == "/bin/echo"
    assert "_pickled_fields" not in td


def test_serialize_function_task():
    """Callable function must be cloudpickle-encoded."""
    import cloudpickle

    def adder(a, b):
        return a + b

    td = EdgeExecutionBackend._serialize_task({
        "uid": "t.002",
        "function": adder,
        "args": (3, 4),
        "kwargs": {},
    })

    assert isinstance(td["function"], str)
    assert td["function"].startswith("cloudpickle::")
    assert "_pickled_fields" in td
    assert "function" in td["_pickled_fields"]

    # Verify round-trip
    raw = base64.b64decode(td["function"][len("cloudpickle::"):])
    fn = cloudpickle.loads(raw)
    assert fn(3, 4) == 7


def test_serialize_non_json_args():
    """Non-JSON-serializable args must be pickled."""
    pytest.importorskip("cloudpickle")

    class Custom:
        pass

    td = EdgeExecutionBackend._serialize_task({
        "uid": "t.003",
        "function": lambda x: x,
        "args": (Custom(),),
    })

    assert "args" in td["_pickled_fields"]
    assert "function" in td["_pickled_fields"]


def test_serialize_json_args_not_pickled():
    """JSON-serializable args must NOT be pickled."""
    pytest.importorskip("cloudpickle")

    td = EdgeExecutionBackend._serialize_task({
        "uid": "t.004",
        "function": lambda x: x,
        "args": [1, 2, 3],
    })

    # function is pickled, but args are plain JSON
    assert "function" in td["_pickled_fields"]
    assert "args" not in td["_pickled_fields"]
    assert td["args"] == [1, 2, 3]


def test_serialize_strips_future():
    """Internal 'future' field must be removed."""
    td = EdgeExecutionBackend._serialize_task({
        "uid": "t.005",
        "executable": "/bin/true",
        "future": asyncio.Future(),
    })
    assert "future" not in td


# ---------------------------------------------------------------------------
# submit_tasks
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_submit_tasks_posts_to_remote():
    backend = await _init_backend()
    backend._http.post.reset_mock()

    submit_resp = MagicMock()
    submit_resp.raise_for_status = MagicMock()
    submit_resp.json = MagicMock(return_value=[
        {"uid": "t.001", "state": "SUBMITTED"}
    ])
    backend._http.post.return_value = submit_resp

    tasks = [{"uid": "t.001", "executable": "/bin/echo", "arguments": ["hi"]}]
    await backend.submit_tasks(tasks)

    call_args = backend._http.post.call_args
    assert "/submit/session.abc123" in call_args[0][0]
    payload = call_args[1]["json"]
    assert len(payload["tasks"]) == 1
    assert payload["tasks"][0]["executable"] == "/bin/echo"

    assert "t.001" in backend._tasks


# ---------------------------------------------------------------------------
# cancel_task
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cancel_task():
    backend = await _init_backend()
    backend._tasks["t.001"] = {"uid": "t.001", "state": "RUNNING"}

    cancel_resp = MagicMock()
    cancel_resp.raise_for_status = MagicMock()
    cancel_resp.json = MagicMock(return_value={"uid": "t.001", "status": "canceled"})
    backend._http.post.return_value = cancel_resp

    result = await backend.cancel_task("t.001")
    assert result is True
    assert backend._tasks["t.001"]["state"] == "CANCELED"


@pytest.mark.asyncio
async def test_cancel_unknown_task():
    backend = await _init_backend()
    result = await backend.cancel_task("no_such_task")
    assert result is False


# ---------------------------------------------------------------------------
# cancel_all_tasks
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cancel_all_tasks():
    backend = await _init_backend()

    cancel_resp = MagicMock()
    cancel_resp.raise_for_status = MagicMock()
    cancel_resp.json = MagicMock(return_value={"canceled": 5})
    backend._http.post.return_value = cancel_resp

    count = await backend.cancel_all_tasks()
    assert count == 5


# ---------------------------------------------------------------------------
# shutdown
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_shutdown_unregisters_session():
    backend = await _init_backend()
    backend._http.post.reset_mock()

    unreg_resp = MagicMock()
    unreg_resp.raise_for_status = MagicMock()
    backend._http.post.return_value = unreg_resp

    await backend.shutdown()

    call_args = backend._http.post.call_args
    assert "unregister_session/session.abc123" in call_args[0][0]
    backend._http.close.assert_called_once()


# ---------------------------------------------------------------------------
# SSE notification handling
# ---------------------------------------------------------------------------

def test_handle_notification_updates_task():
    backend = _make_backend()
    backend._edge_name = "hpc1"
    backend._plugin_name = "rhapsody"

    callback_calls = []
    backend._callback_func = lambda t, s: callback_calls.append((t, s))
    backend._loop = None  # no event loop, direct call for testing

    backend._tasks["t.001"] = {
        "uid": "t.001", "state": "SUBMITTED"
    }

    # Simulate notification — but since _loop is None, callback won't fire
    # via call_soon_threadsafe. Test just the task update part.
    backend._handle_notification({
        "edge": "hpc1",
        "plugin": "rhapsody",
        "topic": "task_status",
        "data": {
            "uid": "t.001",
            "state": "DONE",
            "stdout": "hello\n",
            "exit_code": 0,
        },
    })

    assert backend._tasks["t.001"]["state"] == "DONE"
    assert backend._tasks["t.001"]["stdout"] == "hello\n"


def test_handle_notification_ignores_wrong_edge():
    backend = _make_backend()
    backend._edge_name = "hpc1"
    backend._plugin_name = "rhapsody"
    backend._tasks["t.001"] = {"uid": "t.001", "state": "SUBMITTED"}

    backend._handle_notification({
        "edge": "other_edge",
        "plugin": "rhapsody",
        "topic": "task_status",
        "data": {"uid": "t.001", "state": "DONE"},
    })

    # Should not update
    assert backend._tasks["t.001"]["state"] == "SUBMITTED"


def test_handle_notification_ignores_unknown_task():
    backend = _make_backend()
    backend._edge_name = "hpc1"
    backend._plugin_name = "rhapsody"

    # No tasks registered — should not crash
    backend._handle_notification({
        "edge": "hpc1",
        "plugin": "rhapsody",
        "topic": "task_status",
        "data": {"uid": "unknown", "state": "DONE"},
    })


# ---------------------------------------------------------------------------
# state / context manager
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_state():
    backend = await _init_backend()
    assert await backend.state() == "INITIALIZED"

    backend._http.post.return_value = MagicMock(
        raise_for_status=MagicMock(),
        json=MagicMock(return_value=[{"uid": "t.1", "state": "SUBMITTED"}]),
    )
    await backend.submit_tasks([{"uid": "t.1", "executable": "/bin/true"}])
    assert await backend.state() == "RUNNING"


@pytest.mark.asyncio
async def test_context_manager():
    backend = _make_backend()
    with patch.object(backend, '_start_sse_listener'):
        async with backend as b:
            assert b._initialized is True
            assert b._sid == "session.abc123"
        # After exit, should be shutdown
        assert await b.state() == "SHUTDOWN"
