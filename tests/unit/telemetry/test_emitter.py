"""Emitter handed to backends: no-op default, real emitter, handoff by attach_backend()."""

import json
import pickle
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

pytest.importorskip("opentelemetry", reason="opentelemetry-sdk not installed")

from rhapsody.api.session import Session
from rhapsody.backends.data.base import DataBackend
from rhapsody.backends.execution.concurrent import ConcurrentExecutionBackend
from rhapsody.telemetry.emitter import BackendEmitter
from rhapsody.telemetry.emitter import NoopEmitter
from rhapsody.telemetry.manager import TelemetryManager


class _Holder:
    """A backend stand-in that is picklable."""

    telemetry = NoopEmitter()


class _StubData:
    """Data backend stand-in: no submit_tasks."""

    name = "data"

    async def shutdown(self):
        pass


def _attach(manager, backend, name="b"):
    manager.attach_backend(backend, session_id="s", backend_name=name, interval=60)


async def _drain(manager):
    await manager._queue.join()


# ---------------------------------------------------------------- no-op emitter


def test_noop_emitter_is_disabled_and_silent():
    emitter = NoopEmitter()
    assert emitter.enabled is False
    assert emitter.emit("rhapsody.x.Y", {"a": 1}, event_time=1.0, task_id="t") is None


def test_backends_default_to_the_noop_emitter():
    class _Data(DataBackend):
        async def _do_start(self): ...
        async def _do_shutdown(self): ...
        async def _do_ready(self):
            return True

    assert isinstance(ConcurrentExecutionBackend.telemetry, NoopEmitter)
    assert isinstance(DataBackend.telemetry, NoopEmitter)
    assert isinstance(_Data.telemetry, NoopEmitter)


# -------------------------------------------------------------- backend emitter


async def test_emitter_follows_the_manager_lifecycle():
    manager = TelemetryManager(session_id="s")
    emitter = BackendEmitter(manager, "b")
    assert emitter.enabled is False
    emitter.emit("rhapsody.x.Y")  # not started: dropped, no error
    assert manager._queue.qsize() == 0

    await manager.start()
    assert emitter.enabled is True
    await manager.stop()
    assert emitter.enabled is False
    emitter.emit("rhapsody.x.Y")  # stopped: dropped, no error


async def test_emit_builds_an_event_with_session_backend_and_payload():
    manager = TelemetryManager(session_id="sess")
    await manager.start()
    received = []
    manager.subscribe(received.append)
    attrs = {"batch_id": 3, "requests": [{"n": 1}]}
    BackendEmitter(manager, "vllm").emit(
        "rhapsody.inference.BatchEnded", attrs, event_time=123.5, task_id="task.1"
    )
    await _drain(manager)
    await manager.stop()

    (event,) = [e for e in received if e.event_type == "rhapsody.inference.BatchEnded"]
    assert event.session_id == "sess"
    assert event.backend == "vllm"
    assert event.task_id == "task.1"
    assert event.event_time == 123.5
    assert event.attributes is attrs


async def test_emit_defaults_event_time_to_now_and_attributes_to_empty():
    manager = TelemetryManager(session_id="s")
    await manager.start()
    received = []
    manager.subscribe(received.append)
    before = time.time()
    BackendEmitter(manager, "b").emit("rhapsody.x.Y")
    await _drain(manager)
    await manager.stop()

    (event,) = [e for e in received if e.event_type == "rhapsody.x.Y"]
    assert before <= event.event_time <= time.time()
    assert event.attributes == {}
    assert event.task_id is None


async def test_emitted_event_reaches_the_jsonl_file(tmp_path):
    manager = TelemetryManager(session_id="s", checkpoint_path=str(tmp_path))
    await manager.start()
    BackendEmitter(manager, "b").emit("rhapsody.x.Y", {"n": 7})
    await _drain(manager)
    await manager.stop()

    (path,) = tmp_path.glob("*.jsonl")
    lines = [json.loads(line) for line in path.read_text().splitlines()]
    (event,) = [d for d in lines if d.get("section") == "event" and d["name"] == "rhapsody.x.Y"]
    assert event["backend"] == "b"
    assert event["attributes"] == {"n": 7}


async def test_a_name_without_a_namespace_is_rejected():
    manager = TelemetryManager(session_id="s")
    emitter = BackendEmitter(manager, "b")
    with pytest.raises(ValueError, match="namespaced"):
        emitter.emit("TaskCreated")  # rejected even while disabled
    await manager.start()
    with pytest.raises(ValueError, match="namespaced"):
        emitter.emit("TaskCreated")
    await manager.stop()


def test_pickling_an_emitter_drops_the_manager():
    manager = TelemetryManager(session_id="s")
    holder = _Holder()
    holder.telemetry = BackendEmitter(manager, "b")
    restored = pickle.loads(pickle.dumps(holder))
    assert isinstance(restored.telemetry, NoopEmitter)


# ------------------------------------------------------------ handoff on attach


async def test_attach_backend_hands_over_an_emitter():
    manager = TelemetryManager(session_id="s")
    backend = await ConcurrentExecutionBackend(executor=ThreadPoolExecutor(max_workers=1))
    try:
        assert isinstance(backend.telemetry, NoopEmitter)
        _attach(manager, backend, name="exec")
        assert isinstance(backend.telemetry, BackendEmitter)
        assert backend.telemetry.enabled is False  # attached before start()
        await manager.start()
        assert backend.telemetry.enabled is True  # same handle, now live
        await manager.stop()
    finally:
        await backend.shutdown()


def test_attach_backend_hands_an_emitter_to_data_backends_without_an_adapter():
    manager = TelemetryManager(session_id="s")
    backend = _StubData()
    _attach(manager, backend, name="data")
    assert isinstance(backend.telemetry, BackendEmitter)
    assert manager._adapters == []


def test_attach_backend_replaces_the_emitter_for_a_new_manager():
    first, second = TelemetryManager(session_id="a"), TelemetryManager(session_id="b")
    backend = _StubData()
    _attach(first, backend)
    _attach(second, backend)
    assert backend.telemetry._manager is second


def test_attach_backend_survives_an_object_that_rejects_the_attribute():
    class _Slotted:
        __slots__ = ()

        def submit_tasks(self, tasks):
            pass

    _attach(TelemetryManager(session_id="s"), _Slotted())  # must not raise


# --------------------------------------------------------------- Session wiring


async def test_session_hands_emitters_to_data_backends_before_and_after_start():
    before, after = _StubData(), _StubData()
    session = Session(backends=[before])
    telemetry = await session.start_telemetry(resource_poll_interval=3600)
    try:
        assert isinstance(before.telemetry, BackendEmitter)
        session.add_backend(after)
        assert isinstance(after.telemetry, BackendEmitter)
        assert after.telemetry._manager is telemetry
        assert telemetry._adapters == []
    finally:
        await session.close()


# ------------------------------------------------------------------- import rule


def test_importing_the_emitter_does_not_import_opentelemetry():
    """Telemetry is opt-in: the backends import the emitter, so it must stay OTel-free."""
    code = (
        "import sys; sys.modules['opentelemetry'] = None; "  # any import of it would now fail
        "import rhapsody.telemetry.emitter, rhapsody.backends.base; print('ok')"
    )
    out = subprocess.run([sys.executable, "-I", "-c", code], capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "ok"
