"""Task and node-resource metrics.

The only place that knows the task metrics: instrument names, descriptions and attributes. The
manager hosts it as a default instrumentation that receives every event; it records the task
lifecycle counters, the running-tasks gauge, the duration histogram and the node utilisation gauges
from the events, and ignores every other event type. Names and attributes are unchanged from when
this code lived in the manager (see tests/unit/telemetry/test_task_metrics_golden.py).

Metric attributes stay bounded (no task_id): per-task identity is on the task span.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Any

from rhapsody.telemetry.instrumentation.base import Instrumentation

if TYPE_CHECKING:
    from rhapsody.telemetry.events import BaseEvent
    from rhapsody.telemetry.instrumentation.base import InstrumentationContext


class TaskInstrumentation(Instrumentation):
    """Records task lifecycle and node resource metrics from RHAPSODY events."""

    scope_name = "rhapsody"

    def __init__(self) -> None:
        self._handlers = {
            "TaskSubmitted": self._on_submitted,
            "TaskStarted": self._on_started,
            "TaskCompleted": self._on_completed,
            "TaskFailed": self._on_failed,
            "TaskCanceled": self._on_canceled,
            "ResourceUpdate": self._on_resource,
        }

    def instrument(self, ctx: InstrumentationContext) -> None:
        meter = ctx.meter
        self._c_submitted = meter.create_counter(
            "tasks_submitted", description="Total tasks submitted"
        )
        self._c_started = meter.create_counter("tasks_started", description="Total tasks started")
        self._c_completed = meter.create_counter(
            "tasks_completed", description="Total tasks completed"
        )
        self._c_failed = meter.create_counter("tasks_failed", description="Total tasks failed")
        self._c_canceled = meter.create_counter(
            "tasks_canceled", description="Total tasks canceled"
        )
        self._g_running = meter.create_up_down_counter(
            "tasks_running", description="Tasks currently running"
        )
        self._h_duration = meter.create_histogram(
            "task_duration_seconds", description="Task execution duration in seconds"
        )
        self._g_cpu = meter.create_up_down_counter(
            "node_cpu_utilization", description="Node CPU utilization percent"
        )
        self._g_memory = meter.create_up_down_counter(
            "node_memory_utilization", description="Node memory utilization percent"
        )
        self._g_gpu = meter.create_up_down_counter(
            "node_gpu_utilization", description="Node GPU utilization percent"
        )
        # Per-session state: a task counted in tasks_running (TaskStarted seen, no terminal event
        # yet) with the attributes of its +1, so a task that ends before RUNNING does not
        # decrement the shared series and the -1 hits the series the +1 went to.
        self._running_tasks: dict[str, dict] = {}
        # Last-seen resource values, for the UpDownCounter delta tracking.
        self._last: dict[str, dict[str, float]] = {"cpu": {}, "mem": {}, "gpu": {}}

    def on_event(self, event: BaseEvent) -> None:
        handler = self._handlers.get(event.event_type)
        if handler is not None:
            handler(event)

    @staticmethod
    def counts(metrics_data: Any) -> dict:
        """Task counts from an OTel MetricsData snapshot (zeros when there are no metrics)."""
        return {
            "submitted": int(_sum_metric(metrics_data, "tasks_submitted")),
            "completed": int(_sum_metric(metrics_data, "tasks_completed")),
            "failed": int(_sum_metric(metrics_data, "tasks_failed")),
            "canceled": int(_sum_metric(metrics_data, "tasks_canceled")),
            "running": int(_sum_metric(metrics_data, "tasks_running")),
        }

    # -- handlers ----------------------------------------------------------

    def _on_submitted(self, event: BaseEvent) -> None:
        self._c_submitted.add(1, self._attributes(event))

    def _on_started(self, event: BaseEvent) -> None:
        if event.task_id not in self._running_tasks:
            attrs = self._attributes(event)
            self._running_tasks[event.task_id] = attrs
            self._c_started.add(1, attrs)
            self._g_running.add(1, attrs)

    def _on_completed(self, event: BaseEvent) -> None:
        self._end(self._c_completed, event)

    def _on_failed(self, event: BaseEvent) -> None:
        self._end(self._c_failed, event)

    def _on_canceled(self, event: BaseEvent) -> None:
        self._end(self._c_canceled, event)

    def _end(self, counter: Any, event: BaseEvent) -> None:
        attrs = self._attributes(event)
        counter.add(1, attrs)
        self._h_duration.record(event.duration_seconds, attrs)
        running_attrs = self._running_tasks.pop(event.task_id, None)
        if running_attrs is not None:
            self._g_running.add(-1, running_attrs)

    def _on_resource(self, event: BaseEvent) -> None:
        attrs = {
            "session_id": event.session_id,
            "backend": event.backend or "",
            "node_id": event.node_id,
        }
        for gauge, key, val in (
            (self._g_cpu, "cpu", event.cpu_percent),
            (self._g_memory, "mem", event.memory_percent),
            (self._g_gpu, "gpu", event.gpu_percent),
        ):
            if val is not None:
                last = self._last[key].get(event.node_id, 0.0)
                gauge.add(val - last, attrs)
                self._last[key][event.node_id] = val

    @staticmethod
    def _attributes(event: BaseEvent) -> dict:
        return {
            "session_id": event.session_id,
            "backend": event.backend or "",
            "executable": event.attributes.get("executable", ""),
            "task_type": event.attributes.get("task_type", ""),
        }


def _sum_metric(metrics_data: Any, name: str) -> float:
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
