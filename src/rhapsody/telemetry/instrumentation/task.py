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
from typing import Callable

from rhapsody.telemetry.instrumentation.base import Instrumentation

if TYPE_CHECKING:
    from rhapsody.telemetry.events import BaseEvent
    from rhapsody.telemetry.instrumentation.base import InstrumentationContext


class TaskInstrumentation(Instrumentation):
    """Records task lifecycle and node resource metrics from RHAPSODY events.

    The task counters and the running-tasks gauge are tallied here, per attribute set, and read by
    observable instruments when the metrics are collected: one instrument call per lifecycle event
    was the largest part of telemetry's cost per task. Readers see the same sums, cumulative or
    delta. Each finished task's duration still goes to the histogram as it ends.
    """

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
        # Per session: the attributes of each series, by key, and each counter's tally by key.
        # Callbacks hold the tallies of their session, so a reader of an earlier session keeps
        # reading that session's counts.
        self._series: dict[tuple, dict] = {}
        self._submitted: dict[tuple, int] = {}
        self._started: dict[tuple, int] = {}
        self._completed: dict[tuple, int] = {}
        self._failed: dict[tuple, int] = {}
        self._canceled: dict[tuple, int] = {}
        self._running: dict[tuple, int] = {}
        for name, tally, description in (
            ("tasks_submitted", self._submitted, "Total tasks submitted"),
            ("tasks_started", self._started, "Total tasks started"),
            ("tasks_completed", self._completed, "Total tasks completed"),
            ("tasks_failed", self._failed, "Total tasks failed"),
            ("tasks_canceled", self._canceled, "Total tasks canceled"),
        ):
            meter.create_observable_counter(
                name, callbacks=[_observer(tally, self._series)], description=description
            )
        meter.create_observable_up_down_counter(
            "tasks_running",
            callbacks=[_observer(self._running, self._series)],
            description="Tasks currently running",
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
        # yet) with the series of its +1, so a task that ends before RUNNING does not decrement the
        # shared series and the -1 hits the series the +1 went to.
        self._running_tasks: dict[str, tuple] = {}
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

    def _key(self, event: BaseEvent) -> tuple:
        """The event's series: its attributes are kept once, under this key."""
        attributes = event.attributes
        executable = attributes.get("executable", "")
        task_type = attributes.get("task_type", "")
        key = (
            event.session_id,
            event.backend or "",
            _key_value(executable),
            _key_value(task_type),
        )
        if key not in self._series:
            self._series[key] = {
                "session_id": event.session_id,
                "backend": event.backend or "",
                "executable": executable,
                "task_type": task_type,
            }
        return key

    def _on_submitted(self, event: BaseEvent) -> None:
        key = self._key(event)
        self._submitted[key] = self._submitted.get(key, 0) + 1

    def _on_started(self, event: BaseEvent) -> None:
        if event.task_id not in self._running_tasks:
            key = self._key(event)
            self._running_tasks[event.task_id] = key
            self._started[key] = self._started.get(key, 0) + 1
            self._running[key] = self._running.get(key, 0) + 1

    def _on_completed(self, event: BaseEvent) -> None:
        self._end(self._completed, event)

    def _on_failed(self, event: BaseEvent) -> None:
        self._end(self._failed, event)

    def _on_canceled(self, event: BaseEvent) -> None:
        self._end(self._canceled, event)

    def _end(self, tally: dict, event: BaseEvent) -> None:
        key = self._key(event)
        tally[key] = tally.get(key, 0) + 1
        self._h_duration.record(event.duration_seconds, self._series[key])
        running_key = self._running_tasks.pop(event.task_id, None)
        if running_key is not None:
            self._running[running_key] -= 1

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


_PLAIN = (bool, str, int, float, bytes, type(None))


def _key_value(value: Any) -> Any:
    """An attribute value as part of a tally's key, equal where OpenTelemetry's values are equal.

    As OpenTelemetry takes attribute values: plain values kept apart by kind (1 and True are two
    series), sequences as tuples (a list and a tuple of the same items are one value), mappings item
    by item, and anything else as its ``str()``.
    """
    kind = type(value)
    if kind is str:
        return value
    if kind in _PLAIN:
        return (kind.__name__, value)
    if isinstance(value, (list, tuple)):
        return ("sequence", *[_key_value(item) for item in value])
    if isinstance(value, dict):
        return ("mapping", *[(key, _key_value(item)) for key, item in value.items()])
    return ("str", str(value))


def _observer(tally: dict, series: dict) -> Callable[[Any], list]:
    """An observable instrument's callback: one observation per series of ``tally``."""
    from opentelemetry.metrics import Observation

    def observe(options: Any) -> list:
        # The collection may run on a reader's thread while the dispatch loop adds a series: copy
        # first, again if the dict changed under the copy (free-threaded Python raises then).
        for _ in range(10):
            try:
                items = list(tally.items())
                break
            except RuntimeError:
                continue
        else:
            items = []
        return [Observation(value, series[key]) for key, value in items]

    return observe


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
