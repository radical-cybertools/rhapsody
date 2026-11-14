"""TaskInstrumentation on its own, and as the manager's default instrumentation."""

import logging

import pytest

pytest.importorskip("opentelemetry", reason="opentelemetry-sdk not installed")

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from rhapsody.telemetry.events import BaseEvent
from rhapsody.telemetry.events import ResourceUpdate
from rhapsody.telemetry.events import SessionStarted
from rhapsody.telemetry.events import TaskCompleted
from rhapsody.telemetry.events import TaskCreated
from rhapsody.telemetry.events import TaskFailed
from rhapsody.telemetry.events import TaskStarted
from rhapsody.telemetry.events import TaskSubmitted
from rhapsody.telemetry.events import make_event
from rhapsody.telemetry.instrumentation import Instrumentation
from rhapsody.telemetry.instrumentation import InstrumentationContext
from rhapsody.telemetry.instrumentation import registry
from rhapsody.telemetry.instrumentation.task import TaskInstrumentation
from rhapsody.telemetry.manager import TelemetryManager


def _ctx(provider):
    return InstrumentationContext(
        meter=provider.get_meter("test"),
        tracer=None,
        session_id="s",
        backend_name="",
        session_span_context=lambda: None,
        task_span=lambda task_id: None,
        span_context_for=lambda task_id: None,
    )


def _started_instrumentation():
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    inst = TaskInstrumentation()
    inst.instrument(_ctx(provider))
    return inst, reader, provider


def _ev(cls, task_id="t1", backend="b", **kw):
    kw.setdefault("attributes", {"executable": "x", "task_type": "executable"})
    return make_event(cls, session_id="s", backend=backend, task_id=task_id, **kw)


def _points(reader, name):
    data = reader.get_metrics_data()
    if data is None:
        return []
    return [
        dp
        for rm in data.resource_metrics
        for sm in rm.scope_metrics
        for m in sm.metrics
        if m.name == name
        for dp in m.data.data_points
    ]


def _total(reader, name):
    return sum(dp.value for dp in _points(reader, name))


# ------------------------------------------------------------ direct, no manager


def test_each_task_event_updates_its_metric():
    inst, reader, _ = _started_instrumentation()
    for cls, kw in (
        (TaskSubmitted, {}),
        (TaskStarted, {}),
        (TaskCompleted, {"duration_seconds": 1.0}),
    ):
        inst.on_event(_ev(cls, **kw))
    inst.on_event(_ev(TaskStarted, task_id="t2"))
    inst.on_event(_ev(TaskFailed, task_id="t2", duration_seconds=2.0, error_type="E"))

    assert _total(reader, "tasks_submitted") == 1
    assert _total(reader, "tasks_started") == 2
    assert _total(reader, "tasks_completed") == 1
    assert _total(reader, "tasks_failed") == 1
    assert _total(reader, "tasks_running") == 0
    (hist,) = _points(reader, "task_duration_seconds")
    assert (hist.count, hist.sum) == (2, 3.0)


def test_only_a_task_counted_as_running_decrements_the_gauge():
    inst, reader, _ = _started_instrumentation()
    inst.on_event(_ev(TaskCompleted, task_id="never-started", duration_seconds=0.0))
    assert _total(reader, "tasks_running") == 0  # no decrement without a +1
    inst.on_event(_ev(TaskStarted))
    inst.on_event(_ev(TaskStarted))  # duplicate: counted once
    assert _total(reader, "tasks_started") == 1
    assert _total(reader, "tasks_running") == 1
    inst.on_event(_ev(TaskCompleted, duration_seconds=1.0))
    inst.on_event(_ev(TaskCompleted, duration_seconds=1.0))  # duplicate terminal
    assert _total(reader, "tasks_running") == 0
    assert _total(reader, "tasks_completed") == 3


def test_the_decrement_lands_on_the_series_of_the_increment():
    inst, reader, _ = _started_instrumentation()
    inst.on_event(_ev(TaskStarted, attributes={"executable": "a", "task_type": "x"}))
    inst.on_event(
        _ev(TaskCompleted, attributes={"executable": "b", "task_type": "y"}, duration_seconds=1.0)
    )
    running = {dp.attributes["executable"]: dp.value for dp in _points(reader, "tasks_running")}
    assert running == {"a": 0}  # +1 and -1 on the same series; no series for "b"


def test_resource_updates_track_deltas_and_skip_none():
    inst, reader, _ = _started_instrumentation()

    def update(**kw):
        inst.on_event(make_event(ResourceUpdate, session_id="s", backend="b", node_id="n1", **kw))

    update(cpu_percent=10.0, memory_percent=20.0)
    update(cpu_percent=30.0)  # memory None: untouched
    update(gpu_percent=0.0, gpu_id=0)  # 0.0 is a value, not None
    assert _total(reader, "node_cpu_utilization") == 30.0
    assert _total(reader, "node_memory_utilization") == 20.0
    assert len(_points(reader, "node_gpu_utilization")) == 1


def test_events_it_does_not_own_are_ignored():
    inst, reader, _ = _started_instrumentation()
    inst.on_event(make_event(SessionStarted, session_id="s", backend="b"))
    inst.on_event(_ev(TaskCreated))
    inst.on_event(make_event(BaseEvent, session_id="s", backend="b", event_type="x.Y"))
    assert reader.get_metrics_data() is None or all(
        not m.data.data_points
        for rm in reader.get_metrics_data().resource_metrics
        for sm in rm.scope_metrics
        for m in sm.metrics
    )


def test_instrument_again_resets_state_and_uses_the_new_meter():
    inst, first_reader, _ = _started_instrumentation()
    inst.on_event(_ev(TaskStarted))
    second_reader = InMemoryMetricReader()
    inst.instrument(_ctx(MeterProvider(metric_readers=[second_reader])))
    inst.on_event(_ev(TaskCompleted, duration_seconds=1.0))  # no +1 in this session
    assert _total(second_reader, "tasks_running") == 0
    assert _total(second_reader, "tasks_completed") == 1
    assert _total(first_reader, "tasks_completed") == 0


def test_counts_of_nothing_is_zeros():
    assert TaskInstrumentation.counts(None) == {
        "submitted": 0,
        "completed": 0,
        "failed": 0,
        "canceled": 0,
        "running": 0,
    }


# ----------------------------------------------------------- inside the manager


class _Spy(TaskInstrumentation):
    instances: list = []

    def __init__(self):
        super().__init__()
        self.calls = []
        _Spy.instances.append(self)

    def instrument(self, ctx):
        self.calls.append("instrument")
        super().instrument(ctx)

    def uninstrument(self):
        self.calls.append("uninstrument")


@pytest.fixture
def _registry():
    saved = dict(registry._REGISTRY)
    _Spy.instances = []
    yield
    registry._REGISTRY.clear()
    registry._REGISTRY.update(saved)


async def _run(manager, *events):
    for event in events:
        manager.emit(event)
    await manager._queue.join()


async def test_every_backend_is_counted_without_a_domain():
    manager = TelemetryManager(session_id="s")
    await manager.start()
    await _run(manager, _ev(TaskSubmitted, backend="anything"), _ev(TaskSubmitted, backend="else"))
    assert manager.task_count()["submitted"] == 2
    await manager.stop()


async def test_a_backend_with_its_own_instrumentation_is_counted_too(_registry):
    seen = []

    class Own(Instrumentation):
        scope_name = "rhapsody.instrumentation.own"

        def instrument(self, ctx):
            pass

        def on_event(self, event):
            seen.append(event.event_type)

    registry.register_instrumentation("own", Own)

    class Backend:
        telemetry_domain = "own"

        def submit_tasks(self, tasks):
            pass

    manager = TelemetryManager(session_id="s")
    manager.attach_backend(Backend(), session_id="s", backend_name="tagged", interval=60)
    await manager.start()
    await _run(manager, _ev(TaskSubmitted, backend="tagged"))
    assert seen == ["TaskSubmitted"]  # its own instrumentation saw it...
    assert manager.task_count()["submitted"] == 1  # ...and the default counted it
    await manager.stop()


async def test_the_task_domain_cannot_be_hosted_twice(caplog):
    class Backend:
        telemetry_domain = "task"

        def submit_tasks(self, tasks):
            pass

    manager = TelemetryManager(session_id="s")
    with caplog.at_level(logging.WARNING):
        manager.attach_backend(Backend(), session_id="s", backend_name="b", interval=60)
    assert "hosted for every backend" in caplog.text
    await manager.start()
    await _run(manager, _ev(TaskSubmitted, backend="b"))
    assert manager.task_count()["submitted"] == 1  # not 2
    await manager.stop()


async def test_a_failing_default_never_stops_start(_registry, caplog):
    class Broken(TaskInstrumentation):
        def instrument(self, ctx):
            raise RuntimeError("no meter")

    registry.register_instrumentation("task", Broken)
    manager = TelemetryManager(session_id="s")
    with caplog.at_level(logging.WARNING):
        await manager.start()
    assert "Could not start the default 'task' instrumentation" in caplog.text
    record = next(r for r in caplog.records if "Could not start" in r.getMessage())
    assert record.levelno == logging.WARNING
    assert record.exc_info is not None  # the traceback is kept
    assert manager._running
    assert manager._default_route == ()
    await _run(manager, _ev(TaskSubmitted))
    assert manager.task_count() == dict.fromkeys(
        ("submitted", "completed", "failed", "canceled", "running"), 0
    )
    await manager.stop()


async def test_a_failing_default_does_not_silence_another_instrumentation(_registry, caplog):
    class Flaky(TaskInstrumentation):
        def on_event(self, event):
            raise RuntimeError("default broke")

    class Own(Instrumentation):
        scope_name = "rhapsody.instrumentation.own"

        def instrument(self, ctx):
            pass

        def on_event(self, event):
            raise RuntimeError("own broke")

    class Backend:
        telemetry_domain = "own"

        def submit_tasks(self, tasks):
            pass

    registry.register_instrumentation("task", Flaky)
    registry.register_instrumentation("own", Own)
    manager = TelemetryManager(session_id="s")
    manager.attach_backend(Backend(), session_id="s", backend_name="b", interval=60)
    await manager.start()
    with caplog.at_level(logging.WARNING):
        await _run(manager, _ev(TaskSubmitted, backend="b"), _ev(TaskSubmitted, backend="b"))
    await manager.stop()
    warned = [
        r.args[0]
        for r in caplog.records
        if "failed on" in r.msg and r.levelno >= logging.WARNING and r.args[1] == "b"
    ]
    assert sorted(warned) == ["Flaky", "Own"]  # one warning per instrumentation on backend b


async def test_counts_survive_stop_and_a_second_stop_uninstruments_once(_registry):
    registry.register_instrumentation("task", _Spy)
    manager = TelemetryManager(session_id="s")
    await manager.start()
    await _run(manager, _ev(TaskSubmitted), _ev(TaskStarted))
    await manager.stop()
    assert manager.task_count()["submitted"] == 1  # still readable after stop
    assert manager.summary()["tasks"]["running"] == 1
    manager._stopped = False  # as a retried stop() after a failed one would see it
    await manager.stop()
    (spy,) = _Spy.instances
    assert spy.calls == ["instrument", "uninstrument"]


async def test_task_count_is_zero_before_anything_happens():
    manager = TelemetryManager(session_id="s")
    assert manager.task_count()["submitted"] == 0
    await manager.start()
    assert manager.task_count()["running"] == 0
    await manager.stop()


async def test_a_restart_starts_counting_afresh():
    manager = TelemetryManager(session_id="s")
    await manager.start()
    await _run(manager, _ev(TaskStarted))
    await manager.stop()
    await manager.start()
    await _run(manager, _ev(TaskCompleted, duration_seconds=1.0))  # its start was last run
    counts = manager.task_count()
    assert counts["completed"] == 1
    assert counts["running"] == 0  # not -1: the old +1 belongs to the previous run
    await manager.stop()
