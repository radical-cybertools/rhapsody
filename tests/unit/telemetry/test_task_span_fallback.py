"""Task spans created at their start or terminal event, when TaskCreated was never seen.

The span code stays in the manager; these tests pin the attributes of such a span, which the metrics
golden test does not cover.
"""

import pytest

pytest.importorskip("opentelemetry", reason="opentelemetry-sdk not installed")

from rhapsody.telemetry.events import TaskCanceled
from rhapsody.telemetry.events import TaskCompleted
from rhapsody.telemetry.events import TaskFailed
from rhapsody.telemetry.events import TaskStarted
from rhapsody.telemetry.events import make_event
from rhapsody.telemetry.manager import TelemetryManager

BASE = {
    "session_id": "s",
    "backend": "b",
    "executable": "exe",
    "task_type": "kind",
    "task_id": "t1",
}


def _event(cls, **kw):
    attributes = {"executable": "exe", "task_type": "kind", **kw.pop("attributes", {})}
    return make_event(cls, session_id="s", backend="b", task_id="t1", attributes=attributes, **kw)


async def _task_spans(*events):
    manager = TelemetryManager(session_id="s")
    await manager.start()
    for event in events:
        manager.emit(event)
    await manager.stop()
    return [dict(span.attributes) for span in manager.read_traces() if span.name == "task"]


async def test_started_then_completed_without_created():
    (attrs,) = await _task_spans(_event(TaskStarted), _event(TaskCompleted, duration_seconds=1.0))
    assert attrs == {**BASE, "status": "completed"}


async def test_completed_alone_is_marked_incomplete():
    (attrs,) = await _task_spans(
        _event(TaskCompleted, attributes={"incomplete_lifecycle": True}),
    )
    assert attrs == {**BASE, "status": "completed", "incomplete_lifecycle": True}


async def test_failed_alone_carries_the_error_type():
    (attrs,) = await _task_spans(
        _event(TaskFailed, error_type="ValueError", attributes={"error_type": "ValueError"}),
    )
    assert attrs == {**BASE, "status": "failed", "error_type": "ValueError"}


async def test_canceled_alone():
    (attrs,) = await _task_spans(_event(TaskCanceled))
    assert attrs == {**BASE, "status": "canceled"}
