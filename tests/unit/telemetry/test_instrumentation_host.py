"""The TelemetryManager as a generic host: it routes events to instrumentations by backend."""

import asyncio
import logging
import pathlib
import re
import time
from unittest.mock import MagicMock

import pytest

pytest.importorskip("opentelemetry", reason="opentelemetry-sdk not installed")

import rhapsody.backends.base as backends_base
import rhapsody.backends.data.base as backends_data_base
import rhapsody.telemetry.manager as manager_module
from rhapsody.telemetry.events import TaskCompleted
from rhapsody.telemetry.events import TaskCreated
from rhapsody.telemetry.events import make_event
from rhapsody.telemetry.instrumentation import Instrumentation
from rhapsody.telemetry.instrumentation import get_instrumentation_class
from rhapsody.telemetry.instrumentation import register_instrumentation
from rhapsody.telemetry.instrumentation import registry
from rhapsody.telemetry.manager import TelemetryManager

DOMAIN = "test-domain"
OWN_PREFIX = "rhapsody.test."


class Recorder(Instrumentation):
    """Records what the host does to it; owns only events named ``rhapsody.test.*``."""

    scope_name = "rhapsody.instrumentation.test"
    instances: list = []

    def __init__(self):
        self.ctx = None
        self.events = []
        self.calls = []
        self.fail_on = None
        Recorder.instances.append(self)

    def instrument(self, ctx):
        self.calls.append("instrument")
        self.ctx = ctx
        self.session_ctx_at_instrument = ctx.session_span_context()
        self.events = []  # reset per-session state
        self.counter = ctx.meter.create_counter("test.events", unit="1")

    def on_event(self, event):
        if not event.event_type.startswith(OWN_PREFIX):
            return  # the contract: ignore the types you do not own
        if event.event_type == self.fail_on:
            raise RuntimeError("boom")
        self.events.append(event)
        self.counter.add(1, {"backend": event.backend})

    def uninstrument(self):
        self.calls.append("uninstrument")


class Backend:
    """Stand-in backend that names the test domain."""

    telemetry_domain = DOMAIN

    def submit_tasks(self, tasks):
        pass


class DataStub:
    telemetry_domain = DOMAIN


@pytest.fixture(autouse=True)
def _registered():
    saved = dict(registry._REGISTRY)  # tests register domains: do not leak them
    Recorder.instances = []
    register_instrumentation(DOMAIN, Recorder)
    yield
    registry._REGISTRY.clear()
    registry._REGISTRY.update(saved)


def _attach(manager, backend, name):
    manager.attach_backend(backend, session_id="s", backend_name=name, interval=60)


async def _drain(manager):
    await manager._queue.join()


def _scopes(manager):
    return {
        sm.scope.name: sorted(m.name for m in sm.metrics)
        for rm in manager.read_metrics().resource_metrics
        for sm in rm.scope_metrics
    }


# --------------------------------------------------------------------- routing


async def test_events_reach_only_the_instrumentation_of_their_backend():
    manager = TelemetryManager(session_id="s")
    a, b = Backend(), Backend()
    _attach(manager, a, "a")
    _attach(manager, b, "b")
    await manager.start()
    a.telemetry.emit("rhapsody.test.One")
    b.telemetry.emit("rhapsody.test.Two")
    a.telemetry.emit("rhapsody.test.Three")
    await _drain(manager)
    await manager.stop()

    by_backend = {inst.ctx.backend_name: inst for inst in Recorder.instances}
    assert [e.event_type for e in by_backend["a"].events] == [
        "rhapsody.test.One",
        "rhapsody.test.Three",
    ]
    assert [e.event_type for e in by_backend["b"].events] == ["rhapsody.test.Two"]


async def test_the_instrumentation_also_receives_other_events_of_its_backend():
    """Routing is by backend name: the instrumentation must ignore types it does not own."""
    manager = TelemetryManager(session_id="s")
    _attach(manager, Backend(), "b")
    seen = []
    (inst,) = Recorder.instances
    original = inst.on_event
    inst.on_event = lambda event: (seen.append(event.event_type), original(event))
    await manager.start()
    manager.emit(make_event(TaskCreated, session_id="s", backend="b", task_id="t1"))
    manager.emit(make_event(TaskCreated, session_id="s", backend="other", task_id="t2"))
    await _drain(manager)
    await manager.stop()

    assert seen.count("TaskCreated") == 1  # delivered once (the other backend's event is not)...
    assert inst.events == []  # ...and ignored


async def test_a_backend_without_a_domain_has_no_route():
    class Plain:
        def submit_tasks(self, tasks):
            pass

    manager = TelemetryManager(session_id="s")
    _attach(manager, Plain(), "plain")
    await manager.start()
    assert manager._routes == {}
    assert Recorder.instances == []
    await manager.stop()


async def test_a_domain_that_is_not_a_string_is_ignored():
    manager = TelemetryManager(session_id="s")
    _attach(manager, MagicMock(), "mock")  # MagicMock().telemetry_domain is a MagicMock
    await manager.start()
    assert manager._routes == {}
    assert Recorder.instances == []
    await manager.stop()


async def test_data_backends_are_instrumented_too():
    manager = TelemetryManager(session_id="s")
    data = DataStub()
    _attach(manager, data, "data")
    await manager.start()
    data.telemetry.emit("rhapsody.test.Data")
    await _drain(manager)
    await manager.stop()

    (inst,) = Recorder.instances
    assert [e.event_type for e in inst.events] == ["rhapsody.test.Data"]
    assert manager._adapters == []


async def test_an_unknown_domain_is_logged_and_ignored(caplog):
    class Odd:
        telemetry_domain = "no-such-domain"

        def submit_tasks(self, tasks):
            pass

    manager = TelemetryManager(session_id="s")
    with caplog.at_level(logging.WARNING):
        _attach(manager, Odd(), "odd")
    assert "no-such-domain" in caplog.text
    await manager.start()
    assert manager._routes == {}
    await manager.stop()


async def test_a_second_backend_with_the_same_name_is_ignored():
    manager = TelemetryManager(session_id="s")
    _attach(manager, Backend(), "same")
    _attach(manager, Backend(), "same")
    assert len(Recorder.instances) == 1


async def test_attaching_the_same_backend_object_twice_is_idempotent():
    manager = TelemetryManager(session_id="s")
    backend = Backend()
    await manager.start()
    _attach(manager, backend, "b")
    _attach(manager, backend, "b")
    assert len(Recorder.instances) == 1
    assert Recorder.instances[0].calls == ["instrument"]
    await manager.stop()


def test_the_registry_resolves_a_module_path_lazily():
    register_instrumentation("by-path", f"{__name__}:Recorder")
    assert get_instrumentation_class("by-path") is Recorder
    assert get_instrumentation_class("never-registered") is None


# ------------------------------------------------------------------- lifecycle


async def test_attach_before_start_instruments_at_start():
    manager = TelemetryManager(session_id="s")
    _attach(manager, Backend(), "b")
    (inst,) = Recorder.instances
    assert inst.calls == []
    await manager.start()
    assert inst.calls == ["instrument"]
    await manager.stop()


async def test_attach_after_start_instruments_at_once():
    manager = TelemetryManager(session_id="s")
    await manager.start()
    _attach(manager, Backend(), "b")
    (inst,) = Recorder.instances
    assert inst.calls == ["instrument"]
    await manager.stop()


async def test_stop_uninstruments_once_and_a_restart_instruments_again():
    manager = TelemetryManager(session_id="s")
    backend = Backend()
    _attach(manager, backend, "b")
    (inst,) = Recorder.instances
    await manager.start()
    await manager.stop()
    assert inst.calls == ["instrument", "uninstrument"]
    assert manager._routes == {}

    await manager.start()
    assert inst.calls == ["instrument", "uninstrument", "instrument"]
    backend.telemetry.emit("rhapsody.test.AfterRestart")
    await _drain(manager)
    assert [e.event_type for e in inst.events] == ["rhapsody.test.AfterRestart"]
    assert _scopes(manager)["rhapsody.instrumentation.test"] == ["test.events"]  # new meter works
    await manager.stop()
    assert inst.calls[-1] == "uninstrument"


async def test_the_context_has_a_meter_scoped_to_the_instrumentation():
    manager = TelemetryManager(session_id="sess")
    backend = Backend()
    _attach(manager, backend, "b")
    await manager.start()
    backend.telemetry.emit("rhapsody.test.One")
    await _drain(manager)

    (inst,) = Recorder.instances
    assert inst.ctx.session_id == "sess"
    assert inst.ctx.backend_name == "b"
    assert _scopes(manager)["rhapsody.instrumentation.test"] == ["test.events"]
    await manager.stop()


async def test_the_context_finds_session_and_task_spans():
    manager = TelemetryManager(session_id="s")
    _attach(manager, Backend(), "b")
    await manager.start()
    (inst,) = Recorder.instances
    assert inst.ctx.session_span_context().is_valid
    assert inst.ctx.task_span("t1") is None
    assert inst.ctx.span_context_for("t1") is None

    manager.emit(make_event(TaskCreated, session_id="s", backend="b", task_id="t1"))
    await _drain(manager)
    open_span = inst.ctx.task_span("t1")
    assert open_span is not None
    assert inst.ctx.span_context_for("t1") == open_span.get_span_context()

    manager.emit(make_event(TaskCompleted, session_id="s", backend="b", task_id="t1"))
    await _drain(manager)
    assert inst.ctx.task_span("t1") is None
    assert inst.ctx.span_context_for("t1") == open_span.get_span_context()
    await manager.stop()
    assert inst.ctx.session_span_context() is None  # the session span has ended


async def test_the_session_span_exists_inside_instrument_in_both_attach_orders():
    before = TelemetryManager(session_id="s")
    _attach(before, Backend(), "before")
    await before.start()
    after = TelemetryManager(session_id="s")
    await after.start()
    _attach(after, Backend(), "after")

    assert len(Recorder.instances) == 2
    for inst in Recorder.instances:
        assert inst.session_ctx_at_instrument is not None
        assert inst.session_ctx_at_instrument.is_valid
    await before.stop()
    await after.stop()


async def test_a_retried_stop_uninstruments_once():
    manager = TelemetryManager(session_id="s")
    _attach(manager, Backend(), "b")
    await manager.start()
    (inst,) = Recorder.instances
    real_shutdown = manager._meter_provider.shutdown
    attempts = []

    def flaky_shutdown(*args, **kwargs):
        attempts.append(1)
        if len(attempts) == 1:
            raise RuntimeError("shutdown failed")
        return real_shutdown(*args, **kwargs)

    manager._meter_provider.shutdown = flaky_shutdown
    with pytest.raises(RuntimeError):
        await manager.stop()
    await manager.stop()  # the retry
    assert manager._stopped
    assert inst.calls.count("uninstrument") == 1


async def test_stop_after_the_dispatch_loop_ended_still_uninstruments():
    manager = TelemetryManager(session_id="s")
    _attach(manager, Backend(), "b")
    await manager.start()
    (inst,) = Recorder.instances
    manager._dispatch_task.cancel()
    await asyncio.sleep(0)
    await manager.stop()  # the ended-early path
    assert manager._stopped
    assert inst.calls == ["instrument", "uninstrument"]


async def test_one_failing_uninstrument_does_not_skip_the_others(caplog):
    class Stubborn(Recorder):
        def uninstrument(self):
            super().uninstrument()
            if self.ctx.backend_name == "a":
                raise RuntimeError("stuck")

    register_instrumentation(DOMAIN, Stubborn)
    manager = TelemetryManager(session_id="s")
    _attach(manager, Backend(), "a")  # first in routing order, and the one that raises
    _attach(manager, Backend(), "b")
    await manager.start()
    with caplog.at_level(logging.ERROR):
        await manager.stop()
    assert "Instrumentation of a failed to stop" in caplog.text
    assert [inst.calls[-1] for inst in Recorder.instances] == ["uninstrument", "uninstrument"]


async def test_a_backend_attached_between_stop_and_restart_is_instrumented_at_restart():
    manager = TelemetryManager(session_id="s")
    await manager.start()
    await manager.stop()
    _attach(manager, Backend(), "late")
    (inst,) = Recorder.instances
    assert inst.calls == []
    await manager.start()
    assert inst.calls == ["instrument"]
    await manager.stop()


# -------------------------------------------------------------------- failures


async def test_a_failing_on_event_does_not_stop_the_rest(caplog):
    manager = TelemetryManager(session_id="s")
    backend = Backend()
    _attach(manager, backend, "b")
    await manager.start()
    (inst,) = Recorder.instances
    inst.fail_on = "rhapsody.test.Bad"
    received = []
    manager.subscribe(received.append)

    with caplog.at_level(logging.DEBUG):
        backend.telemetry.emit("rhapsody.test.Bad")
        backend.telemetry.emit("rhapsody.test.Good")
        backend.telemetry.emit("rhapsody.test.Bad")
        await _drain(manager)
    await manager.stop()

    assert [e.event_type for e in inst.events] == ["rhapsody.test.Good"]
    assert [e.event_type for e in received if e.event_type.startswith(OWN_PREFIX)] == [
        "rhapsody.test.Bad",
        "rhapsody.test.Good",
        "rhapsody.test.Bad",
    ]
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING and "failed on" in r.msg]
    assert len(warnings) == 1  # the first failure is loud, the rest are debug


async def test_a_failing_instrument_leaves_the_backend_unrouted_and_start_succeeds(caplog):
    class Broken(Recorder):
        def instrument(self, ctx):
            raise RuntimeError("no meter")

    register_instrumentation(DOMAIN, Broken)
    manager = TelemetryManager(session_id="s")
    backend = Backend()
    _attach(manager, backend, "b")
    with caplog.at_level(logging.WARNING):
        await manager.start()
    assert "Could not start the instrumentation of b" in caplog.text
    assert manager._routes == {}
    assert manager._running
    backend.telemetry.emit("rhapsody.test.One")  # still fine
    await _drain(manager)
    await manager.stop()


async def test_a_failing_uninstrument_does_not_break_stop(caplog):
    class Stubborn(Recorder):
        def uninstrument(self):
            raise RuntimeError("stuck")

    register_instrumentation(DOMAIN, Stubborn)
    manager = TelemetryManager(session_id="s")
    _attach(manager, Backend(), "b")
    await manager.start()
    with caplog.at_level(logging.ERROR):
        await manager.stop()
    assert "failed to stop" in caplog.text
    assert manager._stopped


# ------------------------------------------------------------------ no domain


def test_the_manager_knows_no_inference_domain():
    """The host stays generic: domain names live in instrumentations."""
    source = pathlib.Path(manager_module.__file__).read_text()
    words = ["inference", "vllm", "gen_ai", "queue.duration", "model.duration", "dragon_model"]
    found = [w for w in words if re.search(re.escape(w), source, re.IGNORECASE)]
    assert found == []


def test_backends_import_no_opentelemetry_and_only_the_neutral_emitter():
    for module in (backends_base, backends_data_base):
        source = pathlib.Path(module.__file__).read_text()
        assert "opentelemetry" not in source
        imports = re.findall(r"^\s*from (rhapsody\.telemetry\S*) import", source, re.MULTILINE)
        assert set(imports) == {"rhapsody.telemetry.emitter"}


@pytest.mark.performance
async def test_routing_adds_little_per_event():
    """One dict lookup per event, plus the instrumentation call when there is one: compare a hosted
    run with a plain one in the same process, by ratio."""
    n = 20_000

    async def run(with_instrumentation):
        manager = TelemetryManager(session_id="s")
        if with_instrumentation:
            _attach(manager, Backend(), "b")
        await manager.start()
        t = time.perf_counter()
        for i in range(n):
            manager.emit(make_event(TaskCreated, session_id="s", backend="b", task_id=str(i)))
        await _drain(manager)
        elapsed = time.perf_counter() - t
        await manager.stop()
        return elapsed

    await run(False)  # warm up
    plain, hosted = await run(False), await run(True)
    assert hosted < plain * 1.5
