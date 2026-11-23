"""Telemetry's work per event stays small, and what callers read stays the same.

An application with tens of thousands of tasks per engine sends six events per task through the
manager. Per event, the task instrumentation updated up to three OpenTelemetry instruments, the
dispatch loop asked whether each subscriber was a coroutine function, and making an event read the
operating system's random source. These tests pin the cheaper path and what it must keep: task
counts for every metric reader (cumulative and delta), every event for every subscriber, unique
version-4 event ids, and the checkpoint's event lines.
"""

import asyncio
import json
import os
import pathlib
import random
import uuid

import pytest

pytest.importorskip("opentelemetry", reason="opentelemetry-sdk not installed")

from opentelemetry.sdk.metrics import Counter
from opentelemetry.sdk.metrics import Histogram
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics import ObservableCounter
from opentelemetry.sdk.metrics import ObservableUpDownCounter
from opentelemetry.sdk.metrics import UpDownCounter
from opentelemetry.sdk.metrics.export import AggregationTemporality
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from rhapsody.telemetry.events import TaskCompleted
from rhapsody.telemetry.events import TaskCreated
from rhapsody.telemetry.events import TaskQueued
from rhapsody.telemetry.events import TaskStarted
from rhapsody.telemetry.events import TaskSubmitted
from rhapsody.telemetry.events import make_event
from rhapsody.telemetry.instrumentation import InstrumentationContext
from rhapsody.telemetry.instrumentation.task import TaskInstrumentation
from rhapsody.telemetry.manager import TelemetryManager

ATTRS = {"executable": "evaluate", "task_type": "function"}


def _event(cls, task_id, **kw):
    kw.setdefault("attributes", dict(ATTRS))
    return make_event(cls, session_id="s", backend="b", task_id=task_id, **kw)


def _lifecycle(task_id):
    return [
        _event(TaskCreated, task_id),
        _event(TaskQueued, task_id),
        _event(TaskSubmitted, task_id),
        _event(TaskStarted, task_id),
        _event(TaskCompleted, task_id, duration_seconds=0.5),
    ]


class _CountingMeter:
    """A meter whose synchronous instruments count their calls."""

    def __init__(self, meter):
        self._meter = meter
        self.calls = {"add": 0, "record": 0}

    def _counting(self, instrument, method):
        call = getattr(instrument, method)

        def counted(*args, **kwargs):
            self.calls[method] += 1
            return call(*args, **kwargs)

        setattr(instrument, method, counted)
        return instrument

    def create_counter(self, *args, **kwargs):
        return self._counting(self._meter.create_counter(*args, **kwargs), "add")

    def create_up_down_counter(self, *args, **kwargs):
        return self._counting(self._meter.create_up_down_counter(*args, **kwargs), "add")

    def create_histogram(self, *args, **kwargs):
        return self._counting(self._meter.create_histogram(*args, **kwargs), "record")

    def __getattr__(self, name):
        return getattr(self._meter, name)


def _context(meter):
    return InstrumentationContext(
        meter=meter,
        tracer=None,
        session_id="s",
        backend_name="",
        session_span_context=lambda: None,
        task_span=lambda task_id: None,
        span_context_for=lambda task_id: None,
    )


def _totals(reader):
    data = reader.get_metrics_data()
    totals = {}
    if data is None:
        return totals
    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for metric in sm.metrics:
                for dp in metric.data.data_points:
                    value = dp.count if hasattr(dp, "bucket_counts") else dp.value
                    totals[metric.name] = totals.get(metric.name, 0) + value
    return totals


def test_task_metrics_make_no_instrument_call_per_lifecycle_event():
    """Counting a task cost five instrument calls; now only its duration is recorded."""
    reader = InMemoryMetricReader()
    meter = _CountingMeter(MeterProvider(metric_readers=[reader]).get_meter("test"))
    inst = TaskInstrumentation()
    inst.instrument(_context(meter))
    for i in range(10):
        for event in _lifecycle(f"t{i}"):
            inst.on_event(event)
    assert meter.calls == {"add": 0, "record": 10}
    totals = _totals(reader)
    assert totals["tasks_submitted"] == 10
    assert totals["tasks_started"] == 10
    assert totals["tasks_completed"] == 10
    assert totals["tasks_running"] == 0
    assert totals["task_duration_seconds"] == 10


async def test_a_reader_the_caller_passes_sees_the_task_counts():
    extra = InMemoryMetricReader()
    manager = TelemetryManager(session_id="s", metric_readers=[extra])
    await manager.start()
    try:
        for i in range(3):
            for event in _lifecycle(f"t{i}")[:4]:  # submitted and started, not yet ended
                manager.emit(event)
        await manager._queue.join()
        assert _totals(extra)["tasks_running"] == 3
        for i in range(3):
            manager.emit(_event(TaskCompleted, f"t{i}", duration_seconds=1.0))
        await manager._queue.join()
        assert _totals(extra)["tasks_completed"] == 3
        assert _totals(extra)["tasks_running"] == 0
        assert manager.task_count()["completed"] == 3
    finally:
        await manager.stop()


async def test_a_delta_reader_sees_each_task_once():
    """The task counters are observable instruments: a reader that sets its temporality per
    instrument kind names them, as OTLP's delta preference does."""
    delta = {
        Counter: AggregationTemporality.DELTA,
        ObservableCounter: AggregationTemporality.DELTA,
        UpDownCounter: AggregationTemporality.CUMULATIVE,
        ObservableUpDownCounter: AggregationTemporality.CUMULATIVE,
        Histogram: AggregationTemporality.DELTA,
    }
    reader = InMemoryMetricReader(preferred_temporality=delta)
    manager = TelemetryManager(session_id="s", metric_readers=[reader])
    await manager.start()
    try:
        for i in range(3):
            for event in _lifecycle(f"a{i}"):
                manager.emit(event)
        await manager._queue.join()
        assert _totals(reader)["tasks_completed"] == 3
        for i in range(2):
            for event in _lifecycle(f"b{i}"):
                manager.emit(event)
        await manager._queue.join()
        assert _totals(reader)["tasks_completed"] == 2
    finally:
        await manager.stop()


async def test_a_subscribers_kind_is_checked_once_not_per_event(monkeypatch):
    checks = []
    is_coroutine = asyncio.iscoroutinefunction

    def counting(func):
        checks.append(func)
        return is_coroutine(func)

    manager = TelemetryManager(session_id="s")
    await manager.start()
    seen_sync, seen_async = [], []

    async def on_async(event):
        seen_async.append(event.event_id)

    try:
        monkeypatch.setattr(asyncio, "iscoroutinefunction", counting)
        manager.subscribe(seen_sync.append)
        manager.subscribe(on_async)
        events = [e for i in range(20) for e in _lifecycle(f"t{i}")]
        for event in events:
            manager.emit(event)
        await manager._queue.join()
        await asyncio.sleep(0)
        assert len(checks) <= 2
        sent = [e.event_id for e in events]
        assert [e.event_id for e in seen_sync if e.task_id] == sent  # SessionStarted aside
        assert set(sent) <= set(seen_async)
    finally:
        monkeypatch.undo()
        await manager.stop()


def test_event_ids_do_not_follow_the_random_module():
    """A task that seeds ``random`` must not make two events share an id."""
    random.seed(7)
    first = _event(TaskSubmitted, "t").event_id
    random.seed(7)
    second = _event(TaskSubmitted, "t").event_id
    assert first != second
    for event_id in (first, second):
        assert uuid.UUID(event_id).version == 4


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs os.fork")
def test_event_ids_differ_in_a_forked_child():
    read, write = os.pipe()
    pid = os.fork()
    if pid == 0:  # pragma: no cover - the child
        try:
            os.write(write, _event(TaskSubmitted, "t").event_id.encode())
        finally:
            os._exit(0)
    os.close(write)
    mine = _event(TaskSubmitted, "t").event_id
    child = os.read(read, 64).decode()
    os.close(read)
    os.waitpid(pid, 0)
    assert len(child) == 32 and child != mine


async def test_checkpoint_event_lines_keep_every_field(tmp_path):
    manager = TelemetryManager(session_id="s", checkpoint_path=str(tmp_path))
    await manager.start()
    sent = _lifecycle("t1")
    sent[-1] = _event(
        TaskCompleted, "t1", duration_seconds=0.5, attributes={**ATTRS, "odd": {1, 2}}
    )
    for event in sent:
        manager.emit(event)
    await manager.stop()
    (path,) = pathlib.Path(tmp_path).glob("*.jsonl")
    lines = [json.loads(line) for line in path.read_text().splitlines()]
    events = [line for line in lines if line.get("section") == "event" and line["task_id"] == "t1"]
    assert [e["event_id"] for e in events] == [e.event_id for e in sent]
    completed = events[-1]
    assert list(completed) == [
        "event_id",
        "event_type",
        "event_time",
        "emit_time",
        "session_id",
        "backend",
        "task_id",
        "node_id",
        "attributes",
        "duration_seconds",
        "trace_id",
        "span_id",
        "name",
        "section",
    ]
    assert completed["attributes"]["odd"] == "{1, 2}"  # what json cannot write, as str()
    assert completed["name"] == "TaskCompleted" and completed["span_id"] is not None


def test_the_checkpoint_field_names_keep_no_event_class_alive():
    """AsyncFlow defines an event class per engine; a long-lived service makes many engines."""
    import gc
    import weakref

    from rhapsody.telemetry.events import define_event
    from rhapsody.telemetry.manager import _field_names

    resolved = define_event("asyncflow.TaskResolved")
    assert _field_names(resolved)[0] == "event_id"
    ref = weakref.ref(resolved)
    del resolved
    gc.collect()
    assert ref() is None


def test_task_counts_take_a_list_valued_attribute():
    """OpenTelemetry takes a sequence as an attribute value; the tallies must too."""
    reader = InMemoryMetricReader()
    inst = TaskInstrumentation()
    inst.instrument(_context(MeterProvider(metric_readers=[reader]).get_meter("test")))
    attributes = {"executable": ["mpirun", "app"], "task_type": "executable"}
    for event in (
        _event(TaskSubmitted, "t1", attributes=attributes),
        _event(TaskStarted, "t1", attributes=attributes),
        _event(TaskCompleted, "t1", duration_seconds=1.0, attributes=attributes),
    ):
        inst.on_event(event)
    totals = _totals(reader)
    assert totals["tasks_submitted"] == 1 and totals["tasks_completed"] == 1
    assert totals["tasks_running"] == 0


def _points_of(reader, name):
    data = reader.get_metrics_data()
    return [
        (dict(dp.attributes).get("executable"), dp.value)
        for rm in data.resource_metrics
        for sm in rm.scope_metrics
        for m in sm.metrics
        if m.name == name
        for dp in m.data.data_points
    ]


def test_task_counts_take_any_attribute_value_and_keep_kinds_apart():
    """As OpenTelemetry's own counters do: 1 and True are two series, a list and a tuple of the same
    items one, and a mapping, a set or a nested list is counted too."""
    reader = InMemoryMetricReader()
    inst = TaskInstrumentation()
    inst.instrument(_context(MeterProvider(metric_readers=[reader]).get_meter("test")))
    values = (1, True, {"a": 1}, {1}, [["a"]], ("x", "y"), ["x", "y"])
    for i, executable in enumerate(values):
        attributes = {"executable": executable, "task_type": "function"}
        inst.on_event(_event(TaskSubmitted, f"t{i}", attributes=attributes))
    points = _points_of(reader, "tasks_submitted")
    assert sum(value for _, value in points) == len(values)
    assert len(points) == len(values) - 1  # the list and the tuple share a series
    assert (("x", "y"), 2) in points
    assert [value for executable, value in points if executable is True] == [1]
    assert [value for executable, value in points if executable == 1 and executable is not True]


def test_a_collection_survives_a_tally_changing_under_it():
    """A reader on another thread collects while the dispatch loop adds series (free-threaded Python
    raises then, where the GIL would not)."""
    from rhapsody.telemetry.instrumentation.task import _observer

    class Changing(dict):
        calls = 0

        def items(self):
            Changing.calls += 1
            if Changing.calls == 1:
                raise RuntimeError("dictionary changed size during iteration")
            return super().items()

    key = ("s", "b", "x", "function")
    tally = Changing({key: 2})
    observations = _observer(tally, {key: {"executable": "x"}})(None)
    assert [o.value for o in observations] == [2]
