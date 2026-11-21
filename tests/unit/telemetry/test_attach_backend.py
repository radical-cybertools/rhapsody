"""TelemetryManager.attach_backend(): one adapter per backend, one Dragon node poller."""

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

pytest.importorskip("opentelemetry", reason="opentelemetry-sdk not installed")

from rhapsody.backends.execution.concurrent import ConcurrentExecutionBackend
from rhapsody.telemetry.adapters.concurrent import ConcurrentTelemetryAdapter
from rhapsody.telemetry.manager import TelemetryManager


class DragonExecutionBackend:
    """Stand-in: attach_backend() matches Dragon backends by class name."""

    def submit_tasks(self, tasks):
        pass


class DragonVllmInferenceBackend(DragonExecutionBackend):
    pass


class DragonDataBackend:
    """Data backend: no submit_tasks."""


def _attach(manager, backend, name="b"):
    manager.attach_backend(backend, session_id="s", backend_name=name, interval=60)


async def test_same_backend_attached_twice_gets_one_adapter():
    manager = TelemetryManager(session_id="s")
    backend = await ConcurrentExecutionBackend(executor=ThreadPoolExecutor(max_workers=1))
    try:
        _attach(manager, backend)
        _attach(manager, backend)
        assert [type(a).__name__ for a in manager._adapters] == ["ConcurrentTelemetryAdapter"]
    finally:
        await backend.shutdown()


def test_data_backend_gets_no_adapter():
    manager = TelemetryManager(session_id="s")
    _attach(manager, DragonDataBackend())
    assert manager._adapters == []


def test_dragon_backends_share_one_node_poller():
    manager = TelemetryManager(session_id="s")
    _attach(manager, DragonExecutionBackend(), "dragon")
    _attach(manager, DragonVllmInferenceBackend(), "vllm")
    assert [type(a).__name__ for a in manager._adapters] == ["DragonTelemetryAdapter"]


async def test_adapter_start_twice_runs_one_worker():
    adapter = ConcurrentTelemetryAdapter(session_id="s", backend_name="b", interval=60)
    adapter._manager = TelemetryManager(session_id="s")
    before = threading.active_count()
    adapter.start()
    first = adapter._thread
    adapter.start()
    try:
        assert adapter._thread is first
        assert threading.active_count() == before + 1
    finally:
        adapter.stop()
