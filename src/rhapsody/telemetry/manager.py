"""RHAPSODY TelemetryManager.

Central orchestrator: owns the asyncio EventBus (Queue), wires OpenTelemetry SDK
instruments and tracers, dispatches events to metrics, traces, and external subscribers,
and writes a single JSONL checkpoint file containing events, metric snapshots, and spans.

OTel imports are deferred inside start() so that importing this module never pulls
in opentelemetry at startup unless telemetry is explicitly enabled.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
import time
from pathlib import Path
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable

from rhapsody.telemetry.events import BaseEvent
from rhapsody.telemetry.events import SessionEnded
from rhapsody.telemetry.events import SessionStarted
from rhapsody.telemetry.events import TaskCompleted
from rhapsody.telemetry.events import TaskFailed
from rhapsody.telemetry.events import TaskQueued
from rhapsody.telemetry.events import TaskStarted
from rhapsody.telemetry.events import TaskSubmitted
from rhapsody.telemetry.events import make_event

if TYPE_CHECKING:
    from rhapsody.telemetry.adapters.base import TelemetryAdapter

logger = logging.getLogger(__name__)


class TelemetryManager:
    """Central telemetry orchestrator for a RHAPSODY session.

    Owns:
    - asyncio.Queue EventBus (non-blocking producer via put_nowait)
    - OpenTelemetry MeterProvider + TracerProvider (in-memory, no exporters)
    - Background dispatch loop
    - External subscriber list
    - Optional JSONL checkpoint file

    Usage::

        telemetry = TelemetryManager(
            session_id="session.0001",
            checkpoint_interval=30.0,
            checkpoint_path="./telemetry/",
        )
        await telemetry.start()
        telemetry.subscribe(my_callback)
        # ... session runs ...
        await telemetry.stop()

        print(telemetry.summary())
        print(telemetry.task_spans())
    """

    def __init__(
        self,
        session_id: str,
        checkpoint_interval: float | None = None,
        checkpoint_path: str | None = None,
    ) -> None:
        self._session_id = session_id
        self._queue: asyncio.Queue[BaseEvent] = asyncio.Queue()
        self._subscribers: list[Callable[[BaseEvent], Any]] = []
        self._adapters: list[TelemetryAdapter] = []
        self._running = False
        self._dispatch_task: asyncio.Task | None = None
        self._checkpoint_task: asyncio.Task | None = None
        self._session_start_time: float = 0.0

        # Checkpoint config
        self._checkpoint_interval = checkpoint_interval
        self._checkpoint_path = checkpoint_path
        self._checkpoint_file: Any | None = None  # open file handle

        # Raw event log (in-memory, for convenience methods)
        self._event_log: list[BaseEvent] = []

        # OTel objects — set in start()
        self._meter_provider: Any = None
        self._tracer_provider: Any = None
        self._metric_reader: Any = None
        self._span_exporter: Any = None
        self._meter: Any = None
        self._tracer: Any = None

        # OTel instruments — set in _register_instruments()
        self._c_submitted: Any = None
        self._c_started: Any = None
        self._c_completed: Any = None
        self._c_failed: Any = None
        self._g_running: Any = None
        self._h_duration: Any = None
        self._g_cpu: Any = None
        self._g_memory: Any = None
        self._g_gpu: Any = None

        # Active OTel spans keyed by task_id
        self._active_spans: dict[str, Any] = {}
        self._session_span: Any = None

        # Wall-clock time recorded when RUNNING is first seen for each task.
        self._task_start_times: dict[str, float] = {}

        # Temporary span context store: task_id → SpanContext, populated in
        # _process_event() on TaskCompleted/Failed, consumed in _span_ids_for_event().
        self._completed_span_ctx: dict[str, Any] = {}

        # Last-seen resource values for UpDownCounter delta tracking
        self._last: dict[str, dict[str, float]] = {"cpu": {}, "mem": {}, "gpu": {}}

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the telemetry manager: set up OTel SDK, open checkpoint file, start loops."""
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import InMemoryMetricReader
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor
        from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

        self._metric_reader = InMemoryMetricReader()
        self._span_exporter = InMemorySpanExporter()
        self._meter_provider = MeterProvider(metric_readers=[self._metric_reader])
        self._tracer_provider = TracerProvider()
        self._tracer_provider.add_span_processor(SimpleSpanProcessor(self._span_exporter))
        self._meter = self._meter_provider.get_meter("rhapsody")
        self._tracer = self._tracer_provider.get_tracer("rhapsody")

        self._register_instruments()

        # Open checkpoint file
        if self._checkpoint_path is not None:
            self._open_checkpoint_file()

        self._session_start_time = time.time()
        self._running = True
        self._dispatch_task = asyncio.create_task(self._dispatch_loop(), name="telemetry-dispatch")

        if self._checkpoint_interval is not None and self._checkpoint_file is not None:
            self._checkpoint_task = asyncio.create_task(
                self._checkpoint_loop(), name="telemetry-checkpoint"
            )

        for adapter in self._adapters:
            try:
                adapter.start(self)
            except Exception:
                logger.exception("Adapter %s failed to start", type(adapter).__name__)

        self.emit(
            make_event(
                SessionStarted,
                session_id=self._session_id,
                backend="rhapsody",
            )
        )

    async def stop(self) -> None:
        """Stop: emit SessionEnded, drain queue, flush file, stop adapters, shutdown OTel."""
        duration = time.time() - self._session_start_time
        self.emit(
            make_event(
                SessionEnded,
                session_id=self._session_id,
                backend="rhapsody",
                duration_seconds=duration,
            )
        )

        self._running = False

        for adapter in self._adapters:
            try:
                adapter.stop()
            except Exception:
                logger.exception("Adapter %s failed to stop", type(adapter).__name__)

        if self._checkpoint_task and not self._checkpoint_task.done():
            self._checkpoint_task.cancel()
            try:
                await self._checkpoint_task
            except asyncio.CancelledError:
                pass

        if self._dispatch_task and not self._dispatch_task.done():
            try:
                await asyncio.wait_for(self._dispatch_task, timeout=5.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                logger.warning("Telemetry dispatch loop did not drain cleanly within timeout")

        # Final flush of metrics + spans to file
        if self._checkpoint_file is not None:
            self._write_metric_snapshot()
            self._write_span_snapshot()
            self._checkpoint_file.flush()
            self._checkpoint_file.close()
            self._checkpoint_file = None

        if self._meter_provider:
            self._meter_provider.shutdown()
        if self._tracer_provider:
            self._tracer_provider.shutdown()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def emit(self, event: BaseEvent) -> None:
        """Enqueue an event for processing.

        Non-blocking, O(1), safe to call from any context.
        """
        self._queue.put_nowait(event)

    def subscribe(self, callback: Callable[[BaseEvent], Any]) -> None:
        """Register a callback to receive every event.

        Callbacks may be sync or async. Async callbacks are fire-and-forget tasks. Exceptions in
        callbacks never crash the dispatch loop.
        """
        self._subscribers.append(callback)

    def register_adapter(self, adapter: TelemetryAdapter) -> None:
        """Register a backend telemetry adapter."""
        self._adapters.append(adapter)

    def read_metrics(self) -> Any:
        """Return current OTel MetricsData snapshot (InMemoryMetricReader)."""
        if self._metric_reader is None:
            return None
        return self._metric_reader.get_metrics_data()

    def read_traces(self) -> tuple:
        """Return all finished OTel ReadableSpan objects (InMemorySpanExporter)."""
        if self._span_exporter is None:
            return ()
        return self._span_exporter.get_finished_spans()

    # ------------------------------------------------------------------
    # Convenience methods
    # ------------------------------------------------------------------

    def summary(self) -> dict:
        """Return a plain-dict summary — no OTel knowledge required.

        Includes task counts, duration statistics, session duration, and latest resource utilization
        per node/backend.
        """
        self.read_metrics()
        spans = self.task_spans()

        task_counts = self.task_count()

        durations = [
            s["duration_seconds"]
            for s in spans
            if s["duration_seconds"] is not None and not s.get("incomplete_lifecycle", False)
        ]
        duration_stats: dict = {}
        if durations:
            duration_stats = {
                "total_seconds": round(sum(durations), 6),
                "mean_seconds": round(sum(durations) / len(durations), 6),
                "min_seconds": round(min(durations), 6),
                "max_seconds": round(max(durations), 6),
            }

        # Latest resource reading per (backend, node_id)
        resource_events = [e for e in self._event_log if e.event_type == "ResourceUpdate"]
        nodes: dict = {}
        for ev in resource_events:
            key = f"{ev.backend}/{ev.node_id}"
            nodes[key] = {
                "backend": ev.backend,
                "node_id": ev.node_id,
                "cpu_percent": ev.cpu_percent,
                "memory_percent": ev.memory_percent,
                "gpu_percent": ev.gpu_percent,
                "disk_read_bytes": ev.disk_read_bytes,
                "disk_write_bytes": ev.disk_write_bytes,
                "net_sent_bytes": ev.net_sent_bytes,
                "net_recv_bytes": ev.net_recv_bytes,
            }

        return {
            "session_id": self._session_id,
            "duration_seconds": self.session_duration(),
            "tasks": task_counts,
            "duration": duration_stats,
            "resources": nodes,
        }

    def task_count(self) -> dict:
        """Return task counts as a plain dict."""
        metrics = self.read_metrics()
        return {
            "submitted": int(_sum_metric(metrics, "tasks_submitted")),
            "completed": int(_sum_metric(metrics, "tasks_completed")),
            "failed": int(_sum_metric(metrics, "tasks_failed")),
            "running": int(_sum_metric(metrics, "tasks_running")),
        }

    def task_spans(self) -> list[dict]:
        """Return one plain dict per task span — no OTel knowledge required.

        Each dict contains: task_id, backend, executable, task_type, status,
        duration_seconds, error_type (failed tasks only), span_id, trace_id.
        """
        raw = self.read_traces()
        result = []
        for s in raw:
            if s.name != "task":
                continue
            attrs = dict(s.attributes or {})
            dur = (s.end_time - s.start_time) / 1e9 if s.end_time else None
            record: dict = {
                "task_id": attrs.get("task_id"),
                "backend": attrs.get("backend"),
                "executable": attrs.get("executable", ""),
                "task_type": attrs.get("task_type", ""),
                "status": attrs.get("status"),
                "duration_seconds": round(dur, 6) if dur is not None else None,
                "span_id": hex(s.context.span_id),
                "trace_id": hex(s.context.trace_id),
            }
            if "error_type" in attrs:
                record["error_type"] = attrs["error_type"]
            if attrs.get("incomplete_lifecycle"):
                record["incomplete_lifecycle"] = True
            result.append(record)
        return result

    def session_duration(self) -> float:
        """Return the session duration in seconds (0.0 if not yet stopped)."""
        ended = [e for e in self._event_log if e.event_type == "SessionEnded"]
        if ended:
            return round(ended[-1].duration_seconds, 6)
        if self._session_start_time:
            return round(time.time() - self._session_start_time, 6)
        return 0.0

    # ------------------------------------------------------------------
    # Internal: event bridge (called from TaskStateManager observer slot)
    # ------------------------------------------------------------------

    def _on_task_submitted(self, task: dict) -> None:
        """Called from Session.submit_tasks() after routing sets task['backend']."""
        self.emit(
            make_event(
                TaskSubmitted,
                session_id=self._session_id,
                backend=task.get("backend", ""),
                task_id=task["uid"],
                event_time=time.time(),
                attributes={
                    "executable": _task_executable(task),
                    "task_type": task.get("task_type", ""),
                },
            )
        )

    def _on_task_queued(self, task: dict) -> None:
        """Called from Session.submit_tasks() just before backend.submit_tasks()."""
        self.emit(
            make_event(
                TaskQueued,
                session_id=self._session_id,
                backend=task.get("backend", ""),
                task_id=task["uid"],
                attributes={
                    "executable": _task_executable(task),
                    "task_type": task.get("task_type", ""),
                },
            )
        )

    def _on_task_state_change(self, task: dict, state: str) -> None:
        """Called from TaskStateManager._update_task_impl — must be non-blocking."""
        now_wall = time.time()
        task_id = task["uid"]
        backend = task.get("backend", "")
        executable = _task_executable(task)
        task_type = task.get("task_type", "")

        if state == "RUNNING":
            # Record the wall-clock time we first see RUNNING so we can compute
            # duration in DONE/FAILED.
            self._task_start_times[task_id] = now_wall
            self.emit(
                make_event(
                    TaskStarted,
                    session_id=self._session_id,
                    backend=backend,
                    task_id=task_id,
                    event_time=now_wall,
                    attributes={
                        "executable": executable,
                        "task_type": task_type,
                    },
                )
            )

        elif state == "DONE":
            running_ts = self._task_start_times.pop(task_id, None)
            ended = now_wall
            if running_ts is not None:
                duration = max(ended - running_ts, 0.0)
                attrs: dict = {"executable": executable, "task_type": task_type}
            else:
                # NOTE: RUNNING was never seen for this task (backend did not emit
                # a RUNNING callback, or the task completed before telemetry started).
                # ended = now_wall is used as a best-effort event_time, but
                # duration_seconds = 0.0 and incomplete_lifecycle=True signal that
                # the true start time is unknown. If all tasks show incomplete_lifecycle,
                # the backend is likely not emitting a RUNNING callback — add one.
                duration = 0.0
                attrs = {
                    "executable": executable,
                    "task_type": task_type,
                    "incomplete_lifecycle": True,
                }
            self.emit(
                make_event(
                    TaskCompleted,
                    session_id=self._session_id,
                    backend=backend,
                    task_id=task_id,
                    event_time=ended,
                    duration_seconds=duration,
                    attributes=attrs,
                )
            )

        elif state == "FAILED":
            running_ts = self._task_start_times.pop(task_id, None)
            ended = now_wall
            exc = task.get("exception")
            error_type = type(exc).__name__ if exc is not None else "unknown"
            if running_ts is not None:
                duration = max(ended - running_ts, 0.0)
                attrs = {"executable": executable, "task_type": task_type, "error_type": error_type}
            else:
                # NOTE: RUNNING was never seen for this task — same situation as the
                # DONE branch above. ended = now_wall, duration_seconds = 0.0,
                # incomplete_lifecycle=True. Check whether the backend emits "RUNNING"
                # before reporting this as a data quality issue.
                duration = 0.0
                attrs = {
                    "executable": executable,
                    "task_type": task_type,
                    "error_type": error_type,
                    "incomplete_lifecycle": True,
                }
            self.emit(
                make_event(
                    TaskFailed,
                    session_id=self._session_id,
                    backend=backend,
                    task_id=task_id,
                    event_time=ended,
                    duration_seconds=duration,
                    error_type=error_type,
                    attributes=attrs,
                )
            )

    # ------------------------------------------------------------------
    # Internal: span correlation
    # ------------------------------------------------------------------

    def _span_ids_for_event(self, event: BaseEvent) -> tuple[str | None, str | None]:
        """Return (trace_id_hex, span_id_hex) for the OTel span correlated to this event.

        Must be called AFTER _process_event() so that spans have been created/ended.

        Correlation rules:
        - TaskStarted/Completed/Failed  → trace_id + span_id of the task span
        - SessionStarted/SessionEnded   → trace_id + span_id of the session span
        - TaskSubmitted/TaskQueued      → session trace_id only (span not yet created),
                                          span_id = None (OTel-aligned: event belongs to
                                          the trace but precedes the task span)
        - ResourceUpdate                → (None, None) — node metric, not task-correlated
        """
        span_ctx = None

        if event.event_type == "TaskStarted":
            span = self._active_spans.get(event.task_id)
            if span:
                span_ctx = span.get_span_context()

        elif event.event_type in ("TaskCompleted", "TaskFailed"):
            span_ctx = self._completed_span_ctx.pop(event.task_id, None)

        elif event.event_type in ("SessionStarted", "SessionEnded"):
            if self._session_span:
                span_ctx = self._session_span.get_span_context()

        elif event.event_type in ("TaskSubmitted", "TaskQueued"):
            # No task span exists yet — attach to the session trace so the event
            # can be stitched into a full timeline; span_id remains None.
            if self._session_span:
                session_ctx = self._session_span.get_span_context()
                if session_ctx.is_valid:
                    return hex(session_ctx.trace_id), None

        elif event.event_type == "ResourceUpdate":
            # Node/GPU metric belongs to the session scope — attach to the session
            # span so it is queryable as a child of the root in OTel tools.
            if self._session_span:
                span_ctx = self._session_span.get_span_context()

        if span_ctx is None or not span_ctx.is_valid:
            return None, None
        return hex(span_ctx.trace_id), hex(span_ctx.span_id)

    # ------------------------------------------------------------------
    # Internal: OTel instruments
    # ------------------------------------------------------------------

    def _register_instruments(self) -> None:
        self._c_submitted = self._meter.create_counter(
            "tasks_submitted", description="Total tasks submitted"
        )
        self._c_started = self._meter.create_counter(
            "tasks_started", description="Total tasks started"
        )
        self._c_completed = self._meter.create_counter(
            "tasks_completed", description="Total tasks completed"
        )
        self._c_failed = self._meter.create_counter(
            "tasks_failed", description="Total tasks failed"
        )
        self._g_running = self._meter.create_up_down_counter(
            "tasks_running", description="Tasks currently running"
        )
        self._h_duration = self._meter.create_histogram(
            "task_duration_seconds", description="Task execution duration in seconds"
        )
        self._g_cpu = self._meter.create_up_down_counter(
            "node_cpu_utilization", description="Node CPU utilization percent"
        )
        self._g_memory = self._meter.create_up_down_counter(
            "node_memory_utilization", description="Node memory utilization percent"
        )
        self._g_gpu = self._meter.create_up_down_counter(
            "node_gpu_utilization", description="Node GPU utilization percent"
        )

    # ------------------------------------------------------------------
    # Internal: dispatch loop
    # ------------------------------------------------------------------

    async def _dispatch_loop(self) -> None:
        """Background task: drain queue, update OTel instruments, notify subscribers."""
        from opentelemetry import trace

        while self._running or not self._queue.empty():
            try:
                event = await asyncio.wait_for(self._queue.get(), timeout=0.05)
            except asyncio.TimeoutError:
                continue

            # Keep raw log for convenience methods
            self._event_log.append(event)

            try:
                self._process_event(event, trace)
            except Exception:
                logger.exception("Error processing telemetry event %s", event.event_type)

            # Write event line to JSONL file — enriched with OTel correlation IDs
            if self._checkpoint_file is not None:
                try:
                    d = dataclasses.asdict(event)
                    trace_id, span_id = self._span_ids_for_event(event)
                    d["trace_id"] = trace_id
                    d["span_id"] = span_id
                    d["name"] = event.event_type  # OTel-compatible alias
                    self._write_line("event", d)
                except Exception:
                    logger.debug("Failed to write event to checkpoint file", exc_info=True)

            for sub in self._subscribers:
                try:
                    if asyncio.iscoroutinefunction(sub):
                        asyncio.create_task(sub(event))
                    else:
                        sub(event)
                except Exception:
                    logger.debug("Subscriber raised exception", exc_info=True)

            self._queue.task_done()

    def _process_event(self, event: BaseEvent, trace_mod: Any) -> None:
        """Translate a RHAPSODY event into OTel instrument calls and span lifecycle."""
        etype = event.event_type
        attrs = {"session_id": event.session_id, "backend": event.backend}

        if etype == "TaskSubmitted":
            self._c_submitted.add(1, {**attrs, "task_id": event.task_id})

        elif etype == "TaskStarted":
            executable = event.attributes.get("executable", "")
            task_type = event.attributes.get("task_type", "")
            a = {
                **attrs,
                "task_id": event.task_id,
                "executable": executable,
                "task_type": task_type,
            }
            self._c_started.add(1, a)
            self._g_running.add(1, a)
            ctx = trace_mod.set_span_in_context(self._session_span) if self._session_span else None
            self._active_spans[event.task_id] = self._tracer.start_span(
                "task", context=ctx, attributes=a
            )

        elif etype == "TaskCompleted":
            executable = event.attributes.get("executable", "")
            task_type = event.attributes.get("task_type", "")
            a = {
                **attrs,
                "task_id": event.task_id,
                "executable": executable,
                "task_type": task_type,
            }
            self._c_completed.add(1, a)
            self._h_duration.record(event.duration_seconds, a)
            span = self._active_spans.pop(event.task_id, None)
            if span is not None:
                self._g_running.add(-1, a)
            else:
                ctx = (
                    trace_mod.set_span_in_context(self._session_span)
                    if self._session_span
                    else None
                )
                span = self._tracer.start_span("task", context=ctx, attributes=a)
            span.set_attribute("status", "completed")
            span.set_attribute("executable", executable)
            span.set_attribute("task_type", task_type)
            if event.attributes.get("incomplete_lifecycle"):
                span.set_attribute("incomplete_lifecycle", True)
            span.end()
            self._completed_span_ctx[event.task_id] = span.get_span_context()

        elif etype == "TaskFailed":
            executable = event.attributes.get("executable", "")
            task_type = event.attributes.get("task_type", "")
            error_type = event.attributes.get("error_type", event.error_type)
            a = {
                **attrs,
                "task_id": event.task_id,
                "executable": executable,
                "task_type": task_type,
            }
            self._c_failed.add(1, a)
            self._h_duration.record(event.duration_seconds, a)
            span = self._active_spans.pop(event.task_id, None)
            if span is not None:
                self._g_running.add(-1, a)
            else:
                ctx = (
                    trace_mod.set_span_in_context(self._session_span)
                    if self._session_span
                    else None
                )
                span = self._tracer.start_span("task", context=ctx, attributes=a)
            span.set_attribute("status", "failed")
            span.set_attribute("error_type", error_type)
            span.set_attribute("executable", executable)
            span.set_attribute("task_type", task_type)
            if event.attributes.get("incomplete_lifecycle"):
                span.set_attribute("incomplete_lifecycle", True)
            span.end()
            self._completed_span_ctx[event.task_id] = span.get_span_context()

        elif etype == "SessionStarted":
            self._session_span = self._tracer.start_span("session", attributes=attrs)

        elif etype == "SessionEnded":
            if self._session_span:
                self._session_span.end()
                self._session_span = None

        elif etype == "ResourceUpdate":
            ra = {**attrs, "node_id": event.node_id}
            for gauge, key, val in (
                (self._g_cpu, "cpu", event.cpu_percent),
                (self._g_memory, "mem", event.memory_percent),
                (self._g_gpu, "gpu", event.gpu_percent),
            ):
                if val is not None:
                    last = self._last[key].get(event.node_id, 0.0)
                    gauge.add(val - last, ra)
                    self._last[key][event.node_id] = val

    # ------------------------------------------------------------------
    # Internal: checkpoint file
    # ------------------------------------------------------------------

    def _open_checkpoint_file(self) -> None:
        path = Path(self._checkpoint_path)
        path.mkdir(parents=True, exist_ok=True)
        ts = int(self._session_start_time or time.time())
        filename = f"rhapsody.session.{self._session_id}.{ts}.telemetry.jsonl"
        filepath = path / filename
        try:
            self._checkpoint_file = open(filepath, "w", buffering=1)  # line-buffered
            logger.info("Telemetry checkpoint file: %s", filepath)
        except OSError:
            logger.exception("Could not open telemetry checkpoint file %s", filepath)

    def _write_line(self, section: str, data: dict) -> None:
        """Append one JSON line to the checkpoint file."""
        data["section"] = section
        self._checkpoint_file.write(json.dumps(data, default=str) + "\n")

    def _write_metric_snapshot(self) -> None:
        """Append a metric snapshot line to the checkpoint file."""
        if self._checkpoint_file is None:
            return
        metrics = self.read_metrics()
        snapshot: dict = {"checkpoint_time": time.time()}
        if metrics:
            for rm in metrics.resource_metrics:
                for sm in rm.scope_metrics:
                    for metric in sm.metrics:
                        dps = metric.data.data_points
                        if not dps:
                            continue
                        # Histogram data points expose .sum/.count, not .value
                        if hasattr(dps[0], "sum") and not hasattr(dps[0], "value"):
                            snapshot[metric.name + "_count"] = sum(dp.count for dp in dps)
                            snapshot[metric.name + "_sum"] = sum(dp.sum for dp in dps)
                        else:
                            snapshot[metric.name] = sum(dp.value for dp in dps)
        try:
            self._write_line("metric", snapshot)
        except Exception:
            logger.debug("Failed to write metric snapshot", exc_info=True)

    def _write_span_snapshot(self) -> None:
        """Append finished span records to the checkpoint file."""
        if self._checkpoint_file is None:
            return
        for s in self.read_traces():
            dur = (s.end_time - s.start_time) / 1e6 if s.end_time else None
            record = {
                "name": s.name,
                "span_id": hex(s.context.span_id),
                "trace_id": hex(s.context.trace_id),
                "parent_span_id": hex(s.parent.span_id) if s.parent else None,
                "start_ns": s.start_time,
                "end_ns": s.end_time,
                "start_time_s": s.start_time / 1e9 if s.start_time else None,
                "end_time_s": s.end_time / 1e9 if s.end_time else None,
                "duration_ms": round(dur, 3) if dur is not None else None,
                "attributes": dict(s.attributes or {}),
            }
            try:
                self._write_line("span", record)
            except Exception:
                logger.debug("Failed to write span record", exc_info=True)

    async def _checkpoint_loop(self) -> None:
        """Periodically flush metric snapshots and span records to the checkpoint file."""
        while self._running:
            await asyncio.sleep(self._checkpoint_interval)
            if self._checkpoint_file is not None:
                self._write_metric_snapshot()
                self._write_span_snapshot()
                self._checkpoint_file.flush()


# ------------------------------------------------------------------
# Module-level helper
# ------------------------------------------------------------------


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


def _task_executable(task: dict) -> str:
    """Extract the executable/function identifier from a task dict."""
    # ComputeTask stores executable directly
    if "executable" in task:
        return str(task["executable"])
    # FunctionTask stores func reference
    func = task.get("func") or task.get("function")
    if func is not None:
        return getattr(func, "__name__", str(func))
    # AITask or others may have a model field
    model = task.get("model") or task.get("model_name")
    if model:
        return str(model)
    return ""
