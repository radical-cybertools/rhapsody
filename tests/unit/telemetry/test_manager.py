"""Unit tests for TelemetryManager (OTel wiring, EventBus, subscriber dispatch)."""

import asyncio
import json
import os
import tempfile
import time

import pytest

pytest.importorskip("opentelemetry", reason="opentelemetry-sdk not installed")

from rhapsody.telemetry.events import TaskCompleted
from rhapsody.telemetry.events import TaskFailed
from rhapsody.telemetry.events import TaskQueued
from rhapsody.telemetry.events import TaskStarted
from rhapsody.telemetry.events import TaskSubmitted
from rhapsody.telemetry.events import make_event
from rhapsody.telemetry.manager import TelemetryManager


@pytest.fixture
async def manager():
    m = TelemetryManager(session_id="test-session")
    await m.start()
    yield m
    if m._running:
        await m.stop()


class TestEmit:
    async def test_emit_is_nonblocking(self, manager):
        """put_nowait must not block — 10K calls should complete essentially instantly."""
        start = time.perf_counter()
        for i in range(10_000):
            manager.emit(make_event(
                TaskSubmitted,
                session_id="test-session",
                backend="concurrent",
                task_id=f"task.{i:06d}",
            ))
        elapsed = time.perf_counter() - start
        assert elapsed < 0.5  # generous upper bound — real overhead is <5ms

    async def test_queue_drains_on_stop(self, manager):
        N = 50
        for i in range(N):
            manager.emit(make_event(
                TaskSubmitted,
                session_id="test-session",
                backend="concurrent",
                task_id=f"task.{i:06d}",
            ))
        await manager.stop()
        assert manager._queue.empty()


class TestOtelInstruments:
    async def test_task_submitted_counter(self, manager):
        manager.emit(make_event(
            TaskSubmitted, session_id="test-session", backend="concurrent", task_id="t1"
        ))
        await asyncio.sleep(0.1)
        metrics = manager.read_metrics()
        total = _sum_counter(metrics, "tasks_submitted")
        assert total >= 1

    async def test_task_lifecycle_counters(self, manager):
        now = time.time()
        manager.emit(make_event(TaskStarted, session_id="test-session", backend="concurrent", task_id="t1"))
        manager.emit(make_event(TaskCompleted, session_id="test-session", backend="concurrent", task_id="t1", duration_seconds=0.5))
        await asyncio.sleep(0.1)
        metrics = manager.read_metrics()
        assert _sum_counter(metrics, "tasks_started") >= 1
        assert _sum_counter(metrics, "tasks_completed") >= 1

    async def test_tasks_running_gauge(self, manager):
        manager.emit(make_event(TaskStarted, session_id="test-session", backend="concurrent", task_id="t1"))
        manager.emit(make_event(TaskStarted, session_id="test-session", backend="concurrent", task_id="t2"))
        manager.emit(make_event(TaskCompleted, session_id="test-session", backend="concurrent", task_id="t1", duration_seconds=0.1))
        await asyncio.sleep(0.1)
        metrics = manager.read_metrics()
        running = _sum_gauge(metrics, "tasks_running")
        assert running == 1  # t2 still running

    async def test_failed_task(self, manager):
        manager.emit(make_event(TaskStarted, session_id="test-session", backend="concurrent", task_id="t1"))
        manager.emit(make_event(TaskFailed, session_id="test-session", backend="concurrent", task_id="t1", duration_seconds=0.2, error_type="RuntimeError"))
        await asyncio.sleep(0.1)
        metrics = manager.read_metrics()
        assert _sum_counter(metrics, "tasks_failed") >= 1
        assert _sum_gauge(metrics, "tasks_running") == 0


class TestSpans:
    async def test_task_span_created_and_closed(self, manager):
        manager.emit(make_event(TaskStarted, session_id="test-session", backend="concurrent", task_id="t1"))
        manager.emit(make_event(TaskCompleted, session_id="test-session", backend="concurrent", task_id="t1", duration_seconds=0.1))
        await asyncio.sleep(0.1)
        spans = [s for s in manager.read_traces() if s.name == "task"]
        assert len(spans) == 1
        assert spans[0].end_time is not None
        assert spans[0].attributes.get("status") == "completed"

    async def test_failed_span_status(self, manager):
        manager.emit(make_event(TaskStarted, session_id="test-session", backend="concurrent", task_id="t1"))
        manager.emit(make_event(TaskFailed, session_id="test-session", backend="concurrent", task_id="t1", duration_seconds=0.05, error_type="ValueError"))
        await asyncio.sleep(0.1)
        spans = [s for s in manager.read_traces() if s.name == "task"]
        assert spans[0].attributes.get("status") == "failed"
        assert spans[0].attributes.get("error_type") == "ValueError"

    async def test_session_span(self, manager):
        await manager.stop()
        session_spans = [s for s in manager.read_traces() if s.name == "session"]
        assert len(session_spans) == 1
        assert session_spans[0].end_time is not None
        # prevent double-stop in fixture teardown
        manager._telemetry = None  # type: ignore[attr-defined]


class TestSubscriber:
    async def test_sync_subscriber_receives_events(self, manager):
        received = []
        manager.subscribe(received.append)
        manager.emit(make_event(TaskSubmitted, session_id="test-session", backend="concurrent", task_id="t1"))
        await asyncio.sleep(0.1)
        assert any(e.event_type == "TaskSubmitted" for e in received)

    async def test_async_subscriber(self, manager):
        received = []

        async def handler(event):
            received.append(event)

        manager.subscribe(handler)
        manager.emit(make_event(TaskSubmitted, session_id="test-session", backend="concurrent", task_id="t2"))
        await asyncio.sleep(0.15)
        assert any(e.event_type == "TaskSubmitted" for e in received)

    async def test_subscriber_exception_does_not_crash_loop(self, manager):
        def bad_sub(event):
            raise RuntimeError("subscriber error")

        good_received = []
        manager.subscribe(bad_sub)
        manager.subscribe(good_received.append)
        manager.emit(make_event(TaskSubmitted, session_id="test-session", backend="concurrent", task_id="t1"))
        await asyncio.sleep(0.1)
        # dispatch loop still alive and good subscriber still called
        assert manager._dispatch_task and not manager._dispatch_task.done()
        assert len(good_received) >= 1


class TestConvenienceMethods:
    async def test_task_count(self, manager):
        manager.emit(make_event(TaskSubmitted, session_id="test-session", backend="concurrent", task_id="t1"))
        manager.emit(make_event(TaskSubmitted, session_id="test-session", backend="concurrent", task_id="t2"))
        manager.emit(make_event(TaskCompleted, session_id="test-session", backend="concurrent", task_id="t1", duration_seconds=0.1))
        manager.emit(make_event(TaskFailed,    session_id="test-session", backend="concurrent", task_id="t2", duration_seconds=0.1, error_type="Err"))
        await asyncio.sleep(0.1)
        counts = manager.task_count()
        assert counts["submitted"] == 2
        assert counts["completed"] == 1
        assert counts["failed"] == 1
        assert counts["running"] == 0

    async def test_task_spans_plain_dicts(self, manager):
        manager.emit(make_event(TaskCompleted, session_id="test-session", backend="concurrent",
                                task_id="t1", duration_seconds=0.5,
                                attributes={"executable": "/bin/echo", "task_type": "ComputeTask"}))
        await asyncio.sleep(0.1)
        spans = manager.task_spans()
        assert len(spans) == 1
        s = spans[0]
        assert s["task_id"] == "t1"
        assert s["status"] == "completed"
        assert s["executable"] == "/bin/echo"
        assert s["task_type"] == "ComputeTask"
        assert s["duration_seconds"] is not None
        assert "span_id" in s
        assert "trace_id" in s

    async def test_task_spans_failed_has_error_type(self, manager):
        manager.emit(make_event(TaskFailed, session_id="test-session", backend="concurrent",
                                task_id="t1", duration_seconds=0.1, error_type="ValueError",
                                attributes={"error_type": "ValueError"}))
        await asyncio.sleep(0.1)
        spans = manager.task_spans()
        assert spans[0]["status"] == "failed"
        assert spans[0]["error_type"] == "ValueError"

    async def test_session_duration_after_stop(self, manager):
        await manager.stop()
        dur = manager.session_duration()
        assert dur > 0.0
        manager._telemetry = None  # prevent double-stop in fixture

    async def test_summary_structure(self, manager):
        manager.emit(make_event(TaskSubmitted, session_id="test-session", backend="concurrent", task_id="t1"))
        manager.emit(make_event(TaskCompleted, session_id="test-session", backend="concurrent",
                                task_id="t1", duration_seconds=0.3))
        await asyncio.sleep(0.1)
        s = manager.summary()
        assert "session_id" in s
        assert "tasks" in s
        assert "duration" in s
        assert s["tasks"]["submitted"] == 1
        assert s["tasks"]["completed"] == 1
        assert s["duration"]["mean_seconds"] is not None


class TestCheckpointFile:
    async def test_file_created_with_correct_name(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            m = TelemetryManager(
                session_id="cp-test",
                checkpoint_path=tmpdir,
            )
            await m.start()
            m.emit(make_event(TaskSubmitted, session_id="cp-test", backend="concurrent", task_id="t1"))
            await asyncio.sleep(0.05)
            await m.stop()

            files = os.listdir(tmpdir)
            assert len(files) == 1
            assert files[0].startswith("rhapsody.session.cp-test.")
            assert files[0].endswith(".telemetry.jsonl")

    async def test_file_contains_all_sections(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            m = TelemetryManager(session_id="cp-sections", checkpoint_path=tmpdir)
            await m.start()
            m.emit(make_event(TaskSubmitted, session_id="cp-sections", backend="concurrent", task_id="t1"))
            m.emit(make_event(TaskCompleted, session_id="cp-sections", backend="concurrent",
                              task_id="t1", duration_seconds=0.1))
            await asyncio.sleep(0.1)
            await m.stop()

            filepath = os.path.join(tmpdir, os.listdir(tmpdir)[0])
            lines = [json.loads(l) for l in open(filepath)]
            sections = {l["section"] for l in lines}
            assert "event"  in sections
            assert "metric" in sections
            assert "span"   in sections

    async def test_events_have_event_type(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            m = TelemetryManager(session_id="cp-events", checkpoint_path=tmpdir)
            await m.start()
            m.emit(make_event(TaskQueued, session_id="cp-events", backend="concurrent",
                              task_id="t1",
                              attributes={"executable": "/bin/echo", "task_type": "ComputeTask"}))
            await asyncio.sleep(0.1)
            await m.stop()

            filepath = os.path.join(tmpdir, os.listdir(tmpdir)[0])
            events = [json.loads(l) for l in open(filepath) if json.loads(l)["section"] == "event"]
            types = {e["event_type"] for e in events}
            assert "TaskQueued" in types
            queued = next(e for e in events if e["event_type"] == "TaskQueued")
            # executable and task_type are now in the attributes dict
            assert queued["attributes"]["executable"] == "/bin/echo"
            assert queued["attributes"]["task_type"] == "ComputeTask"

    async def test_no_file_when_path_is_none(self):
        m = TelemetryManager(session_id="no-file")
        await m.start()
        m.emit(make_event(TaskSubmitted, session_id="no-file", backend="concurrent", task_id="t1"))
        await asyncio.sleep(0.05)
        await m.stop()
        assert m._checkpoint_file is None


# ------------------------------------------------------------------
# Helpers to navigate OTel MetricsData
# ------------------------------------------------------------------

def _sum_counter(metrics_data, name: str) -> float:
    total = 0.0
    if metrics_data is None:
        return total
    for rm in metrics_data.resource_metrics:
        for sm in rm.scope_metrics:
            for metric in sm.metrics:
                if metric.name == name:
                    for dp in metric.data.data_points:
                        total += dp.value
    return total


def _sum_gauge(metrics_data, name: str) -> float:
    total = 0.0
    if metrics_data is None:
        return total
    for rm in metrics_data.resource_metrics:
        for sm in rm.scope_metrics:
            for metric in sm.metrics:
                if metric.name == name:
                    for dp in metric.data.data_points:
                        total += dp.value
    return total
