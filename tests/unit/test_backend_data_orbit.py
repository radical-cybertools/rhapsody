"""Unit tests for OrbitDataBackend (remote data backend lifecycle via ORBIT)."""

import asyncio
from unittest.mock import MagicMock

import pytest

from rhapsody.backends.data import DataBackendStartupError
from rhapsody.backends.data import DataBackendState
from rhapsody.backends.data import DataBackendStateError
from rhapsody.backends.data import OrbitDataBackend
from rhapsody.backends.data import OrbitEndpoint


class _FakeExecBackend:
    """Stands in for OrbitExecutionBackend: awaitable, `_rh` (None once shut down)."""

    def __init__(self, rh):
        self._rh = rh
        self.awaited = 0

    def __await__(self):
        async def _init():
            self.awaited += 1
            return self

        return _init().__await__()


def _rh(endpoints=("node1:6379",), state="READY"):
    rh = MagicMock()
    rh.data_start = MagicMock(
        return_value={
            "name": "redis",
            "kind": "redis",
            "state": "READY",
            "endpoints": list(endpoints),
        }
    )
    rh.data_endpoints = MagicMock(
        return_value={"backends": {"redis_data": {"state": state, "endpoints": list(endpoints)}}}
    )
    rh.data_shutdown = MagicMock(return_value={"name": "redis", "status": "shutdown"})
    return rh


@pytest.mark.asyncio
async def test_start_returns_remote_endpoints():
    rh = _rh(endpoints=["n1:1", "n2:2"])
    exe = _FakeExecBackend(rh)
    backend = OrbitDataBackend(exe, "redis", port=6379)

    await backend.start()

    assert exe.awaited == 1
    rh.data_start.assert_called_once_with("redis", name="redis_data", port=6379)
    assert backend.state is DataBackendState.READY
    assert backend.endpoints == [
        OrbitEndpoint(kind="redis", descriptor="n1:1"),
        OrbitEndpoint(kind="redis", descriptor="n2:2"),
    ]
    assert [e.serialize() for e in backend.endpoints] == ["n1:1", "n2:2"]


@pytest.mark.asyncio
async def test_name_default_avoids_execution_backend_names():
    # "dragon" is the remote session's DragonExecutionBackend name
    assert OrbitDataBackend(_FakeExecBackend(_rh()), "dragon").name == "dragon_data"
    rh = _rh()
    backend = OrbitDataBackend(_FakeExecBackend(rh), "redis", name="cache")
    assert backend.name == "cache"
    await backend.start()
    rh.data_start.assert_called_once_with("redis", name="cache")


@pytest.mark.asyncio
async def test_ready_queries_remote_state():
    rh = _rh(state="READY")
    backend = await OrbitDataBackend(_FakeExecBackend(rh), "redis")
    assert await backend.ready() is True

    rh.data_endpoints.return_value = {"backends": {}}
    assert await backend.ready() is False


@pytest.mark.asyncio
async def test_shutdown_stops_remote_once():
    rh = _rh()
    backend = await OrbitDataBackend(_FakeExecBackend(rh), "redis")
    await backend.shutdown()
    await backend.shutdown()
    rh.data_shutdown.assert_called_once_with("redis_data")
    assert backend.state is DataBackendState.SHUTDOWN


@pytest.mark.asyncio
async def test_shutdown_never_started_is_local_only():
    rh = _rh()
    backend = OrbitDataBackend(_FakeExecBackend(rh), "redis")
    await backend.shutdown()
    rh.data_shutdown.assert_not_called()


@pytest.mark.asyncio
async def test_shutdown_tolerates_remote_failure():
    rh = _rh()
    rh.data_shutdown.side_effect = RuntimeError("session gone")
    backend = await OrbitDataBackend(_FakeExecBackend(rh), "redis")
    await backend.shutdown()
    assert backend.state is DataBackendState.SHUTDOWN


@pytest.mark.asyncio
async def test_remote_start_failure_is_startup_error():
    rh = _rh()
    rh.data_start.side_effect = RuntimeError("409 exists")
    backend = OrbitDataBackend(_FakeExecBackend(rh), "redis")
    with pytest.raises(DataBackendStartupError):
        await backend.start()
    assert backend.state is DataBackendState.FAILED
    # a lost reply may hide a store the remote side did start: stop it
    rh.data_shutdown.assert_called_once_with("redis_data")
    await backend.shutdown()
    rh.data_shutdown.assert_called_once()


@pytest.mark.asyncio
async def test_remote_start_cancelled_cleans_up():
    rh = _rh()
    rh.data_start.side_effect = lambda *a, **k: (_ for _ in ()).throw(asyncio.CancelledError())
    backend = OrbitDataBackend(_FakeExecBackend(rh), "redis")
    with pytest.raises(asyncio.CancelledError):
        await backend.start()
    rh.data_shutdown.assert_called_once_with("redis_data")


@pytest.mark.asyncio
async def test_shutdown_after_execution_backend_closed_is_quiet(caplog):
    rh = _rh()
    exe = _FakeExecBackend(rh)
    backend = await OrbitDataBackend(exe, "redis")
    exe._rh = None  # Session.close shut the execution backend down first
    with caplog.at_level("WARNING"):
        await backend.shutdown()
    rh.data_shutdown.assert_not_called()
    assert not caplog.records


@pytest.mark.asyncio
async def test_start_after_execution_backend_closed():
    exe = _FakeExecBackend(_rh())
    exe._rh = None
    with pytest.raises(DataBackendStateError, match="shut down"):
        await OrbitDataBackend(exe, "redis").start()


@pytest.mark.asyncio
async def test_rejected_start_leaves_remote_store_alone():
    rh = _rh()
    err = RuntimeError("HTTP 409 — backend 'redis_data' exists")
    err.status_code = 409
    rh.data_start.side_effect = err
    with pytest.raises(DataBackendStartupError):
        await OrbitDataBackend(_FakeExecBackend(rh), "redis").start()
    rh.data_shutdown.assert_not_called()


@pytest.mark.asyncio
async def test_ready_false_after_execution_backend_closed():
    exe = _FakeExecBackend(_rh())
    backend = await OrbitDataBackend(exe, "redis")
    exe._rh = None
    assert await backend.ready() is False
