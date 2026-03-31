"""Unit tests for Session.enable_telemetry() integration."""

import asyncio
from concurrent.futures import ThreadPoolExecutor

import pytest

pytest.importorskip("opentelemetry", reason="opentelemetry-sdk not installed")

from rhapsody.api.session import Session
from rhapsody.api.task import ComputeTask
from rhapsody.backends.execution.concurrent import ConcurrentExecutionBackend
from rhapsody.telemetry.manager import TelemetryManager


@pytest.fixture
async def backend():
    b = await ConcurrentExecutionBackend(executor=ThreadPoolExecutor(max_workers=4))
    yield b
    await b.shutdown()


class TestEnableTelemetry:
    async def test_returns_telemetry_manager(self, backend):
        session = Session(backends=[backend])
        telemetry = session.enable_telemetry()
        assert isinstance(telemetry, TelemetryManager)
        assert session._telemetry is telemetry

    async def test_observer_wired(self, backend):
        session = Session(backends=[backend])
        telemetry = session.enable_telemetry()
        assert session._state_manager._telemetry_observer == telemetry._on_task_state_change

    async def test_no_otel_import_before_start(self, backend):
        """enable_telemetry() must not import opentelemetry at call time."""
        import sys
        session = Session(backends=[backend])
        before = set(sys.modules.keys())
        session.enable_telemetry()
        after = set(sys.modules.keys())
        new_modules = after - before
        # opentelemetry.sdk should not be imported yet
        assert not any("opentelemetry.sdk" in m for m in new_modules)

    async def test_task_submitted_event(self, backend):
        session = Session(backends=[backend])
        telemetry = session.enable_telemetry()
        await telemetry.start()

        received = []
        telemetry.subscribe(lambda e: received.append(e))

        tasks = [ComputeTask(executable="echo", arguments=["hi"])]
        await session.submit_tasks(tasks)
        await asyncio.sleep(0.05)

        types = [e.event_type for e in received]
        assert "TaskSubmitted" in types

        await session.close()

    async def test_full_lifecycle_events(self, backend):
        session = Session(backends=[backend])
        telemetry = session.enable_telemetry()
        await telemetry.start()

        received = []
        telemetry.subscribe(lambda e: received.append(e))

        tasks = [
            ComputeTask(executable="/bin/echo", arguments=["hello"]),
            ComputeTask(executable="/bin/echo", arguments=["world"]),
        ]
        await session.submit_tasks(tasks)
        await session.wait_tasks(tasks, timeout=10.0)
        await session.close()
        await asyncio.sleep(0.1)

        types = {e.event_type for e in received}
        assert "SessionStarted" in types
        assert "TaskSubmitted" in types
        assert "TaskCompleted" in types or "TaskFailed" in types
        assert "SessionEnded" in types

    async def test_read_metrics_after_tasks(self, backend):
        session = Session(backends=[backend])
        telemetry = session.enable_telemetry()
        await telemetry.start()

        tasks = [ComputeTask(executable="/bin/echo", arguments=[str(i)]) for i in range(5)]
        await session.submit_tasks(tasks)
        await session.wait_tasks(tasks, timeout=10.0)
        await session.close()
        await asyncio.sleep(0.1)

        metrics = telemetry.read_metrics()
        submitted = _sum_counter(metrics, "tasks_submitted")
        completed = _sum_counter(metrics, "tasks_completed")
        running = _sum_gauge(metrics, "tasks_running")

        assert submitted == 5
        assert completed == 5
        assert running == 0

    async def test_read_traces_after_tasks(self, backend):
        session = Session(backends=[backend])
        telemetry = session.enable_telemetry()
        await telemetry.start()

        tasks = [ComputeTask(executable="/bin/echo", arguments=[str(i)]) for i in range(3)]
        await session.submit_tasks(tasks)
        await session.wait_tasks(tasks, timeout=10.0)
        await session.close()
        await asyncio.sleep(0.1)

        spans = telemetry.read_traces()
        task_spans = [s for s in spans if s.name == "task"]
        assert len(task_spans) == 3
        assert all(s.end_time is not None for s in task_spans)
        assert all(s.attributes.get("status") in ("completed", "failed") for s in task_spans)

    async def test_close_stops_telemetry(self, backend):
        session = Session(backends=[backend])
        telemetry = session.enable_telemetry()
        await telemetry.start()
        assert telemetry._running is True
        await session.close()
        assert telemetry._running is False
        assert session._telemetry is None


# ------------------------------------------------------------------
# Helpers
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
    return _sum_counter(metrics_data, name)
