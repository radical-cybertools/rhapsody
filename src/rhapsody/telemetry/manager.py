"""RHAPSODY TelemetryManager.

Central orchestrator: owns the asyncio EventBus (Queue), the OpenTelemetry SDK providers and the
task and session spans, hosts the instrumentations that record metrics (see
rhapsody.telemetry.instrumentation), dispatches events to them and to external subscribers, and
writes a single JSONL checkpoint file containing events, metric snapshots, and spans.

OTel imports are deferred inside start() so that importing this module never pulls
in opentelemetry at startup unless telemetry is explicitly enabled.
"""

from __future__ import annotations

import asyncio
import dataclasses
import functools
import json
import logging
import time
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable

from rhapsody.telemetry.emitter import BackendEmitter
from rhapsody.telemetry.events import BaseEvent
from rhapsody.telemetry.events import SessionEnded
from rhapsody.telemetry.events import SessionStarted
from rhapsody.telemetry.events import TaskCanceled
from rhapsody.telemetry.events import TaskCompleted
from rhapsody.telemetry.events import TaskCreated
from rhapsody.telemetry.events import TaskFailed
from rhapsody.telemetry.events import TaskQueued
from rhapsody.telemetry.events import TaskStarted
from rhapsody.telemetry.events import TaskSubmitted
from rhapsody.telemetry.events import make_event

if TYPE_CHECKING:
    from opentelemetry.sdk.metrics.export import MetricReader
    from opentelemetry.sdk.resources import Resource as OtelResource
    from opentelemetry.sdk.trace import SpanProcessor

    from rhapsody.telemetry.adapters.base import TelemetryAdapter
    from rhapsody.telemetry.instrumentation import Instrumentation

logger = logging.getLogger(__name__)

# Queued by stop() behind the backlog: the dispatch loop exits when it reaches it.
_STOP_SENTINEL = object()


class SpanBuffer:
    """O(1)-append in-process span store.

    InMemorySpanExporter (an OTel test utility) does `_finished_spans += tuple(spans)` on every
    export call — O(n) per call, O(n²) total for n spans. For 40K spans that costs ~4 seconds inside
    the dispatch loop. list.extend() keeps append at O(1) amortized.

    Implements the SpanExporter interface so it can be passed to any SpanProcessor.
    """

    def __init__(self) -> None:
        import threading

        self._spans: list = []
        self._lock = threading.Lock()

    def export(self, spans) -> None:
        with self._lock:
            self._spans.extend(spans)

    def get_finished_spans(self) -> tuple:
        with self._lock:
            return tuple(self._spans)

    def clear(self) -> None:
        with self._lock:
            self._spans.clear()

    def shutdown(self) -> None:
        pass  # keep spans accessible via get_finished_spans() after tracer_provider.shutdown()

    def force_flush(self, timeout_millis: int = 30_000) -> bool:
        return True


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

    # stop() keeps waiting while the event backlog shrinks within this many seconds.
    _DRAIN_STALL_TIMEOUT = 5.0

    def __init__(
        self,
        session_id: str,
        checkpoint_interval: float | None = None,
        checkpoint_path: str | None = None,
        span_processors: list[SpanProcessor] | None = None,
        metric_readers: list[MetricReader] | None = None,
        resource: OtelResource | None = None,
        drain_on_stop: bool = True,
    ) -> None:
        self._session_id = session_id
        # stop(): process the event backlog (True) or discard it for a fast shutdown.
        self._drain_on_stop = drain_on_stop
        self._queue: asyncio.Queue = asyncio.Queue()
        # Events of the batch the dispatch loop is flushing: lost if the loop dies mid-batch.
        self._inflight = 0
        self._subscribers: list[Callable[[BaseEvent], Any]] = []
        self._adapters: list[TelemetryAdapter] = []
        # Backends that already have an adapter (by id), and whether the one
        # runtime-wide Dragon node poller exists — see attach_backend().
        self._attached_backends: set[int] = set()
        self._dragon_adapter_attached = False
        # Domain instrumentations hosted by backend name, and the event routes (backend name ->
        # instrumentations; a name is routed exactly while it is instrumented). The manager knows
        # no domain: it hosts what the registry returns for a backend's `telemetry_domain` and
        # passes each event to the instrumentations of the backend it came from. Names already
        # warned about are kept so a failing instrumentation is loud once, not once per event.
        self._instrumentations: dict[str, Instrumentation] = {}
        self._routes: dict[str, tuple[Instrumentation, ...]] = {}
        self._failed_instrumentations: set[tuple[str, type]] = set()
        # Default instrumentations (every event goes to them, whatever its backend), only the ones
        # that started; the route is rebuilt from them at start() and cleared at stop().
        self._default_instrumentations: dict[str, Instrumentation] = {}
        self._default_route: tuple[Instrumentation, ...] = ()
        self._running = False
        self._stopped = False  # set once stop() has completed its cleanup
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
        self._tracer: Any = None

        # Active OTel spans keyed by task_id
        self._active_spans: dict[str, Any] = {}
        self._session_span: Any = None
        self._completed_session_ctx: Any = None  # snapshot after session span ends

        # Wall-clock time recorded when RUNNING is first seen for each task.
        self._task_start_times: dict[str, float] = {}

        # Temporary span context store: task_id → SpanContext, populated in
        # _process_event() on TaskCompleted/Failed, consumed in _span_ids_for_event().
        self._completed_span_ctx: dict[str, Any] = {}

        # Registered by higher-layer runtimes to inject custom OTel span attributes.
        # Each callable receives the event and returns a dict of extra attributes.
        self._span_attribute_extractors: list[Callable[[BaseEvent], dict]] = []

        # OTel context snapshot captured at emit() time for each TaskCreated event.
        # Keyed by task_id. Consumed (popped) in _process_event() to restore the
        # correct parent span context in the dispatch loop.
        self._task_contexts: dict[str, Any] = {}

        # The session span as an OTel context: the explicit parent of spans that have no caller
        # span. Never attached: start() and stop() may run in different asyncio tasks, and an
        # ambient attach belongs to one task (and would leak into the caller's OTel context).
        self._session_ctx: Any = None

        # Caller-supplied OTel extension points wired into RHAPSODY's internal providers
        # at start() time alongside SpanBuffer / InMemoryMetricReader.
        # Callers own exporter construction; RHAPSODY never imports specific exporters.
        self._extra_span_processors: list = list(span_processors or [])
        self._extra_metric_readers: list = list(metric_readers or [])
        # Optional Resource override. None → Resource.create() in start(), which reads
        # OTEL_SERVICE_NAME and OTEL_RESOURCE_ATTRIBUTES from the environment automatically.
        self._resource: Any = resource

    # ------------------------------------------------------------------
    # Public properties
    # ------------------------------------------------------------------

    @property
    def session_id(self) -> str:
        """The session identifier shared by all events from this manager."""
        return self._session_id

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the telemetry manager: set up OTel SDK, open checkpoint file, start loops."""
        if self._running:
            return  # already started: a second dispatch loop would be orphaned
        self._stopped = False
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import InMemoryMetricReader
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor

        # Resource.create() reads OTEL_SERVICE_NAME / OTEL_RESOURCE_ATTRIBUTES from the
        # environment automatically; the passed dict provides fallback defaults only.
        _resource = self._resource or Resource.create(
            {"service.name": "rhapsody", "session_id": self._session_id}
        )

        self._metric_reader = InMemoryMetricReader()
        self._span_exporter = SpanBuffer()

        self._meter_provider = MeterProvider(
            metric_readers=[self._metric_reader] + self._extra_metric_readers,
            resource=_resource,
        )
        self._tracer_provider = TracerProvider(resource=_resource)
        self._tracer_provider.add_span_processor(SimpleSpanProcessor(self._span_exporter))
        for proc in self._extra_span_processors:
            self._tracer_provider.add_span_processor(proc)

        self._tracer = self._tracer_provider.get_tracer("rhapsody")

        # Open checkpoint file
        if self._checkpoint_path is not None:
            self._open_checkpoint_file()

        self._session_start_time = time.time()

        # Create the session span synchronously so it is available before any
        # workflow_scope() / span_scope() call — _dispatch_loop runs in a separate
        # asyncio task and would otherwise process SessionStarted too late.
        self._session_span = self._tracer.start_span(
            "session", attributes={"session_id": self._session_id, "backend": "rhapsody"}
        )
        from opentelemetry import trace as trace_mod

        self._session_ctx = trace_mod.set_span_in_context(self._session_span)

        # Instrumentations could not get a meter before now. After the session span exists, so
        # ctx.session_span_context() is valid inside instrument() whether a backend was attached
        # before or after start(); before the dispatch loop starts. The defaults go first: a
        # backend's route is the default route plus its own instrumentation.
        self._start_default_instrumentations()
        for backend_name in list(self._instrumentations):
            self._instrument(backend_name)

        # A fresh queue: nothing left by an earlier stop() (e.g. a sentinel) reaches this loop.
        self._queue = asyncio.Queue()
        self._running = True
        self._dispatch_task = asyncio.create_task(
            self._dispatch_loop(self._queue), name="telemetry-dispatch"
        )

        if self._checkpoint_interval is not None and self._checkpoint_file is not None:
            self._checkpoint_task = asyncio.create_task(
                self._checkpoint_loop(), name="telemetry-checkpoint"
            )

        for adapter in self._adapters:
            adapter.start()

        self.emit(
            make_event(
                SessionStarted,
                session_id=self._session_id,
                backend="rhapsody",
            )
        )

    async def stop(self) -> None:
        """Stop: emit SessionEnded, drain queue, flush file, stop adapters, shutdown OTel."""
        if self._stopped:
            return  # a completed stop() already cleaned up; a cancelled one is retried
        if not self._drain_on_stop:
            # Discard before SessionEnded is queued, so the session span still ends.
            discarded = self._discard_queue()
            if discarded:
                logger.warning(
                    "Telemetry drain disabled: %d queued events not processed", discarded
                )
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
        # Drain: queue the sentinel behind the backlog and wait for the dispatch loop to
        # reach it. The queue is FIFO, so the loop ending at the sentinel means every event
        # before it was processed. emit() is now a no-op (guard on _running). Keep waiting
        # while the backlog shrinks; on a stall, cancel the loop so nothing runs while the
        # checkpoint file and the OTel providers are closed below.
        self._queue.put_nowait(_STOP_SENTINEL)
        task = self._dispatch_task
        stalled = False
        last = self._queue.qsize()
        while task is not None and not task.done():
            await asyncio.wait({task}, timeout=self._DRAIN_STALL_TIMEOUT)
            if task.done():
                break
            now = self._queue.qsize()
            if now >= last:
                stalled = True
                task.cancel()
                # Bounded; asyncio.wait() also lets a cancellation of stop() itself propagate.
                await asyncio.wait({task}, timeout=self._DRAIN_STALL_TIMEOUT)
                if not task.done():
                    logger.warning("Telemetry dispatch loop did not stop after cancel")
                break
            logger.info("Telemetry: draining %d events before shutdown", now - 1)
            last = now
        exc = (
            task.exception() if task is not None and task.done() and not task.cancelled() else None
        )
        if exc is not None:
            logger.error("Telemetry dispatch loop failed", exc_info=exc)
        ended_early = task is None or not task.done() or task.cancelled() or exc is not None
        if ended_early:
            # SessionEnded may be among the unprocessed events: end the session span here.
            if self._session_span is not None:
                self._completed_session_ctx = self._session_span.get_span_context()
                self._session_span.end()
                self._session_span = None
            # Not processed: what is still queued (the sentinel aside), and possibly part of
            # the batch the loop was handling when it ended.
            queued = self._discard_queue()
            inflight, self._inflight = self._inflight, 0
            if queued or inflight:
                logger.warning(
                    "Telemetry dispatch loop %s; %d queued events not processed%s",
                    "stalled" if stalled else "is not running",
                    queued,
                    f", plus up to {inflight} in the batch it was handling" if inflight else "",
                )

        for adapter in self._adapters:
            try:
                adapter.stop()
            except Exception:
                logger.exception("Adapter %s failed to stop", type(adapter).__name__)

        # Before the providers shut down below: instrumentations still hold instruments. Each
        # is taken out of its registry first, so a retried stop() never uninstruments it twice.
        for backend_name in list(self._routes):
            del self._routes[backend_name]
            try:
                self._instrumentations[backend_name].uninstrument()
            except Exception:
                logger.exception("Instrumentation of %s failed to stop", backend_name)
        defaults = list(self._default_instrumentations.items())
        self._default_instrumentations.clear()
        self._default_route = ()
        for domain, instrumentation in defaults:
            try:
                instrumentation.uninstrument()
            except Exception:
                logger.exception("Default %r instrumentation failed to stop", domain)

        if self._checkpoint_task and not self._checkpoint_task.done():
            self._checkpoint_task.cancel()
            try:
                await self._checkpoint_task
            except asyncio.CancelledError:
                pass

        # Final flush of metrics + spans to file
        if self._checkpoint_file is not None:
            self._write_metric_snapshot()
            self._write_span_snapshot()
            self._checkpoint_file.flush()
            self._checkpoint_file.close()
            self._checkpoint_file = None

        self._session_ctx = None

        if self._meter_provider:
            self._meter_provider.shutdown()
        if self._tracer_provider:
            self._tracer_provider.shutdown()
        self._stopped = True

    def _discard_queue(self) -> int:
        """Empty the event queue without processing it; return the number of events dropped."""
        dropped = 0
        while True:
            try:
                item = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                return dropped
            self._queue.task_done()
            if item is not _STOP_SENTINEL:
                dropped += 1

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def emit(self, event: BaseEvent) -> None:
        """Enqueue an event for async processing (JSONL, metrics, spans).

        Non-blocking, O(1) for all event types. For TaskCreated events, captures the caller's OTel
        context so _process_event() can restore it in the dispatch loop and create the task span
        with the correct structural parent.
        """
        if not self._running:
            return
        self._queue.put_nowait(event)
        if self._tracer is not None and event.event_type == "TaskCreated":
            from opentelemetry import context as _ctx
            from opentelemetry import trace as _trace

            ctx = _ctx.get_current()
            if not _trace.get_current_span(ctx).get_span_context().is_valid:
                ctx = self._session_ctx  # no caller span: the session is the parent
            self._task_contexts[event.task_id] = ctx

    def subscribe(self, callback: Callable[[BaseEvent], Any]) -> None:
        """Register a callback to receive every event.

        Callbacks may be sync or async. Async callbacks are fire-and-forget tasks. Exceptions in
        callbacks never crash the dispatch loop.

        TELEMETRY-LIMITATION(L5): subscribers receive events only. OTel metrics recorded by an
        instrumentation reach them only through :meth:`read_metrics` or a metric reader passed as
        ``metric_readers``, not as events. Why not now: metrics are aggregated by the OTel SDK, not
        emitted per event. Planned: Tier 3 (subscribers move to a standard SpanProcessor /
        MetricReader). See docs/telemetry/reference.md#known-limitations.
        """
        self._subscribers.append(callback)

    def annotate_current_span(self, event: BaseEvent) -> None:
        """Add this event as a timestamped annotation on the currently active OTel span.

        Must be called from a context where a span is active (e.g. inside span_scope()). No-op if no
        valid span is active or telemetry has not been started.
        """
        if self._tracer is None:
            return
        from opentelemetry import trace as _trace

        active = _trace.get_current_span()
        if active is None or not active.get_span_context().is_valid:
            return
        attrs = event.attributes
        active.add_event(
            event.event_type,
            attributes={k: str(v) for k, v in attrs.items()} if attrs else {},
        )

    def register_span_enricher(self, func: Callable[[BaseEvent], dict]) -> None:
        """Register a callable that enriches OTel span attributes at span creation time.

        Called for every TaskCreated event (and TaskStarted fallback). The callable receives the
        event and must return a dict of additional OTel attributes to merge. Exceptions are logged
        at DEBUG level so faulty extractors cannot crash the dispatch loop.
        """
        self._span_attribute_extractors.append(func)

    @contextmanager
    def span_scope(self, name: str, attributes: dict | None = None):
        """Create a named span, make it the active OTel context for the duration.

        Any emit(TaskCreated) calls made while this scope is active will capture this span as the
        parent, so task spans become structural children.

        No-op when telemetry has not been started (self._tracer is None).
        """
        if self._tracer is None:
            yield
            return
        from opentelemetry import context as otel_context
        from opentelemetry import trace as trace_mod

        _active = trace_mod.get_current_span()
        _ctx = None if _active.get_span_context().is_valid else self._session_ctx
        span = self._tracer.start_span(name, context=_ctx, attributes=attributes or {})
        token = otel_context.attach(trace_mod.set_span_in_context(span))
        try:
            yield
        finally:
            otel_context.detach(token)
            span.end()

    def register_adapter(self, adapter: TelemetryAdapter) -> None:
        """Register a backend telemetry adapter.

        Injects ``self`` as the adapter's manager so :meth:`TelemetryAdapter.start`
        requires no arguments — the back-reference is set here, once.
        """
        adapter._manager = self
        self._adapters.append(adapter)

    def attach_backend(
        self,
        backend,
        *,
        session_id: str,
        backend_name: str,
        interval: float = 5.0,
    ) -> None:
        """Hand the backend its emitter, then register its resource telemetry adapter.

        Every backend, including data backends, gets an emitter through its ``register_emitter``
        hook (an object without the hook, such as a duck-typed stand-in, gets the ``telemetry``
        attribute set directly); it is replaced on each call, so a backend reused by a new
        manager emits into that one. A failing hook is logged as a warning and never breaks
        attach. Then the appropriate telemetry adapter is registered. Silently skips backends
        with no adapter (LocalExecutionBackend, NoopExecutionBackend, unknown types) and data
        backends (no ``submit_tasks``). Safe to call before or after start(); if already running
        the adapter is started immediately. Attaching the same backend again adds no second
        adapter, and all Dragon backends of a session share one node poller.

        If the backend names a ``telemetry_domain``, the instrumentation registered for it is
        hosted too (see :meth:`_attach_instrumentation`).
        """
        emitter = BackendEmitter(self, backend_name)
        try:
            register = getattr(backend, "register_emitter", None)
            if register is not None:
                register(emitter)
            else:
                # Legacy: an object that is not a BaseBackend or DataBackend (a duck-typed
                # stand-in, an engine's own simple backend) has no hook.
                backend.telemetry = emitter
        except Exception:
            # A failing override, or an object with a read-only attribute, must not break attach.
            logger.warning("Could not hand an emitter to %s", backend_name, exc_info=True)

        # Before the early returns below: data backends and Dragon backends need it too.
        self._attach_instrumentation(backend, backend_name)

        if not hasattr(backend, "submit_tasks"):
            return  # data backend: no task execution, no resource adapter
        if id(backend) in self._attached_backends:
            return
        self._attached_backends.add(id(backend))

        try:
            from rhapsody.backends.execution.concurrent import ConcurrentExecutionBackend
            from rhapsody.telemetry.adapters.concurrent import ConcurrentTelemetryAdapter

            if isinstance(backend, ConcurrentExecutionBackend):
                adapter = ConcurrentTelemetryAdapter(
                    session_id=session_id,
                    backend_name=backend_name,
                    interval=interval,
                )
                self.register_adapter(adapter)
                if self._running:
                    adapter.start()
                return
        except Exception:
            logger.warning("Could not attach ConcurrentTelemetryAdapter", exc_info=True)

        backend_cls = type(backend).__name__

        if backend_cls == "DaskExecutionBackend":
            try:
                from rhapsody.telemetry.adapters.dask import DaskTelemetryAdapter

                client = getattr(backend, "_client", None) or getattr(backend, "client", None)
                if client is not None:
                    adapter = DaskTelemetryAdapter(
                        client=client,
                        session_id=session_id,
                        backend_name=backend_name,
                        interval=interval,
                    )
                    self.register_adapter(adapter)
                    if self._running:
                        adapter.start()
            except Exception:
                logger.warning("Could not attach DaskTelemetryAdapter", exc_info=True)
            return

        if backend_cls.startswith("Dragon"):
            # Dragon node metrics are runtime-wide: a second Dragon backend in the
            # session (e.g. an AI backend) would poll the same nodes again.
            if self._dragon_adapter_attached:
                return
            try:
                from rhapsody.telemetry.adapters.dragon import DragonTelemetryAdapter

                adapter = DragonTelemetryAdapter(
                    session_id=session_id,
                    backend_name=backend_name,
                    interval=interval,
                )
                self.register_adapter(adapter)
                self._dragon_adapter_attached = True
                if self._running:
                    adapter.start()
            except Exception:
                logger.warning("Could not attach DragonTelemetryAdapter", exc_info=True)
            return

        # LocalExecutionBackend, NoopExecutionBackend, unknown — silently skip.

    def _attach_instrumentation(self, backend: Any, backend_name: str) -> None:
        """Host the instrumentation of the backend's ``telemetry_domain``, if it has one.

        The instrumentation is started now if the manager is running, else at start(). A domain
        that is not a non-empty string (stand-in backends) is ignored; an unknown domain is
        logged and ignored.

        TELEMETRY-LIMITATION(L15): instrumentations are keyed by backend name, so two backends of
        one session with the same name share one slot and the later one is ignored.
        Why not now: Session and WorkflowEngine key their backends by name, so a same-name
        backend replaces the earlier one there; instrumentations follow the same rule.
        Planned: none needed. See docs/telemetry/reference.md#known-limitations.
        """
        domain = getattr(backend, "telemetry_domain", None)
        if not isinstance(domain, str) or not domain or backend_name in self._instrumentations:
            return
        from rhapsody.telemetry.instrumentation import get_instrumentation_class
        from rhapsody.telemetry.instrumentation.registry import DEFAULT_DOMAINS

        if domain in DEFAULT_DOMAINS:
            # Default domains already see every event: hosting one again would count twice.
            logger.warning("Telemetry domain %r is hosted for every backend; ignored", domain)
            return

        try:
            cls = get_instrumentation_class(domain)
            if cls is None:
                logger.warning("No instrumentation registered for telemetry domain %r", domain)
                return
            instrumentation = cls()
        except Exception:
            logger.warning("Could not create the %r instrumentation", domain, exc_info=True)
            return
        self._instrumentations[backend_name] = instrumentation
        if self._running:
            self._instrument(backend_name)

    def _instrument(self, backend_name: str) -> None:
        """Start a backend's instrumentation with a scoped meter and tracer, then route to it.

        A failing instrumentation is logged and gets no events; it never stops the manager.
        """
        instrumentation = self._instrumentations[backend_name]
        what = f"the instrumentation of {backend_name}"
        if self._start_instrumentation(instrumentation, backend_name, what):
            self._routes[backend_name] = (*self._default_route, instrumentation)

    def _start_default_instrumentations(self) -> None:
        """Create and start the default instrumentations; the ones that fail are left out."""
        from rhapsody.telemetry.instrumentation import get_instrumentation_class
        from rhapsody.telemetry.instrumentation.registry import DEFAULT_DOMAINS

        for domain in DEFAULT_DOMAINS:
            try:
                instrumentation = get_instrumentation_class(domain)()
            except Exception:
                logger.warning(
                    "Could not create the default %r instrumentation", domain, exc_info=True
                )
                continue
            if self._start_instrumentation(
                instrumentation, "", f"the default {domain!r} instrumentation"
            ):
                self._default_instrumentations[domain] = instrumentation
        self._default_route = tuple(self._default_instrumentations.values())

    def _start_instrumentation(
        self, instrumentation: Instrumentation, backend_name: str, what: str
    ) -> bool:
        """Call ``instrument()`` with a context scoped to the instrumentation.

        On failure, log a warning with the traceback (``what`` names the instrumentation) and
        return False.
        """
        from rhapsody.telemetry.instrumentation import InstrumentationContext

        try:
            scope = instrumentation.scope_name
            instrumentation.instrument(
                InstrumentationContext(
                    meter=self._meter_provider.get_meter(scope),
                    tracer=self._tracer_provider.get_tracer(scope),
                    session_id=self._session_id,
                    backend_name=backend_name,
                    session_span_context=self._session_span_context,
                    task_span=self._active_spans.get,
                    span_context_for=self._task_span_context,
                )
            )
        except Exception:
            logger.warning("Could not start %s", what, exc_info=True)
            return False
        return True

    def _session_span_context(self) -> Any:
        """The session span's context, or None once the session span has ended."""
        return self._session_span.get_span_context() if self._session_span else None

    def _task_span_context(self, task_id: str) -> Any:
        """Context of a task's span: the open one, else the ended one the host still holds."""
        span = self._active_spans.get(task_id)
        if span is not None:
            return span.get_span_context()
        return self._completed_span_ctx.get(task_id)

    def _route_event(self, event: BaseEvent, route: tuple) -> None:
        """Pass an event to its backend's instrumentations; a failing one never stops the rest."""
        for instrumentation in route:
            try:
                instrumentation.on_event(event)
            except Exception:
                # Warn once per (backend, instrumentation), then stay quiet: this runs per event.
                key = (event.backend, type(instrumentation))
                first = key not in self._failed_instrumentations
                self._failed_instrumentations.add(key)
                logger.log(
                    logging.WARNING if first else logging.DEBUG,
                    "Instrumentation %s of %s failed on %s",
                    type(instrumentation).__name__,
                    event.backend,
                    event.event_type,
                    exc_info=True,
                )

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

    def export_as_otlp(self, path: str | None = None) -> str:
        """Export finished spans as a valid OTLP JSON string.

        Produces output compatible with Jaeger, Tempo, Grafana, and any standard
        OTel visualizer. The JSONL checkpoint is RHAPSODY's internal analysis format
        and is intentionally not OTLP-compliant; this method provides the standard
        export path alongside it.

        Args:
            path: If given, write the JSON to this file (parent dirs created as needed).

        Returns:
            OTLP JSON string (``{"resourceSpans": [...]}``)
        """
        spans = self.read_traces()

        def _typed_kv(k: str, v) -> dict:
            if isinstance(v, bool):
                return {"key": k, "value": {"boolValue": v}}
            if isinstance(v, int):
                return {"key": k, "value": {"intValue": str(v)}}
            if isinstance(v, float):
                return {"key": k, "value": {"doubleValue": v}}
            if isinstance(v, (list, tuple)):
                return {
                    "key": k,
                    "value": {"arrayValue": {"values": [_typed_kv("", e)["value"] for e in v]}},
                }
            return {"key": k, "value": {"stringValue": str(v)}}

        otlp_spans = []
        for s in spans:
            try:
                status_code = s.status.status_code.value
            except AttributeError:
                status_code = 0
            status_obj: dict = {"code": status_code}
            if s.status.description:
                status_obj["message"] = s.status.description

            # TELEMETRY-LIMITATION(L18): the OTLP export writes the OTel SpanKind value, which is
            # off by one from the OTLP value (OTel CLIENT is 2, OTLP CLIENT is 3), hardcodes the
            # instrumentation scope "rhapsody" for every span and drops span links. Exported
            # spans that carry links (such as batch spans) are therefore mislabeled and lose them.
            # Why not now: existing behaviour of the export, reported (T19) and not changed here.
            # Planned: Tier 2 (use the SDK's OTLP encoder). See
            # docs/telemetry/reference.md#known-limitations.
            try:
                kind = s.kind.value
            except AttributeError:
                kind = 0

            span_dict: dict = {
                "traceId": format(s.context.trace_id, "032x"),
                "spanId": format(s.context.span_id, "016x"),
                "name": s.name,
                "kind": kind,
                "startTimeUnixNano": str(s.start_time) if s.start_time else "0",
                "endTimeUnixNano": str(s.end_time) if s.end_time else "0",
                "attributes": [_typed_kv(k, v) for k, v in (s.attributes or {}).items()],
                "status": status_obj,
            }
            if s.parent:
                span_dict["parentSpanId"] = format(s.parent.span_id, "016x")
            if s.events:
                span_dict["events"] = [
                    {
                        "timeUnixNano": str(e.timestamp),
                        "name": e.name,
                        "attributes": [_typed_kv(k, v) for k, v in (e.attributes or {}).items()],
                    }
                    for e in s.events
                ]
            otlp_spans.append(span_dict)

        res_attrs = dict(self._tracer_provider.resource.attributes)
        res_attrs.setdefault("session_id", self._session_id)
        payload = {
            "resourceSpans": [
                {
                    "resource": {"attributes": [_typed_kv(k, v) for k, v in res_attrs.items()]},
                    "scopeSpans": [
                        {
                            "scope": {"name": "rhapsody"},
                            "spans": otlp_spans,
                        }
                    ],
                }
            ]
        }

        result = json.dumps(payload)

        if path is not None:
            p = Path(path)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(result)

        return result

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
        from rhapsody.telemetry.instrumentation.task import TaskInstrumentation

        return TaskInstrumentation.counts(self.read_metrics())

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

    def _on_task_created(self, task: dict) -> None:
        """Called from Session.submit_tasks() when the future is created and the task is
        registered."""
        self.emit(
            make_event(
                TaskCreated,
                session_id=self._session_id,
                backend=task.get("backend", ""),
                task_id=task["uid"],
                attributes={
                    "executable": _task_executable(task),
                    "task_type": task.get("task_type", ""),
                },
            )
        )

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
            attrs: dict = {"executable": executable, "task_type": task_type}
            # Optional placement info — populated by backends that know it.
            # Schema: {node_id: str, cores: int, gpu_ids: list[int]}.
            # When future placement APIs land they should write the same key.
            placement = task.get("placement")
            node_id_for_event = None
            if isinstance(placement, dict):
                # Snapshot without deepcopy: a placement may hold uncopyable objects.
                attrs["placement"] = {
                    k: list(v) if isinstance(v, list) else v for k, v in placement.items()
                }
                node_id_for_event = placement.get("node_id")
            self.emit(
                make_event(
                    TaskStarted,
                    session_id=self._session_id,
                    backend=backend,
                    task_id=task_id,
                    event_time=now_wall,
                    node_id=node_id_for_event,
                    attributes=attrs,
                )
            )

        elif state == "DONE":
            running_ts = self._task_start_times.pop(task_id, None)
            ended = now_wall
            if running_ts is not None:
                duration = max(ended - running_ts, 0.0)
                attrs = {"executable": executable, "task_type": task_type}
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

        elif state == "CANCELED":
            running_ts = self._task_start_times.pop(task_id, None)
            ended = now_wall
            if running_ts is not None:
                duration = max(ended - running_ts, 0.0)
                attrs = {"executable": executable, "task_type": task_type}
            else:
                duration = 0.0
                attrs = {
                    "executable": executable,
                    "task_type": task_type,
                    "incomplete_lifecycle": True,
                }
            self.emit(
                make_event(
                    TaskCanceled,
                    session_id=self._session_id,
                    backend=backend,
                    task_id=task_id,
                    event_time=ended,
                    duration_seconds=duration,
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
        - TaskCreated/Submitted/Queued/Started/Completed/Failed → task span
        - SessionStarted/SessionEnded   → session span
        - ResourceUpdate                → session span
        """
        span_ctx = None

        if event.event_type in ("TaskCreated", "TaskStarted", "TaskSubmitted", "TaskQueued"):
            span = self._active_spans.get(event.task_id)
            if span:
                span_ctx = span.get_span_context()
            else:
                # Same-batch scenario: _process_event(TaskCompleted/Failed/Canceled) already
                # ran and moved the span_ctx to _completed_span_ctx. Use .get (not .pop) —
                # the terminal event's own _span_ids_for_event call will pop it.
                span_ctx = self._completed_span_ctx.get(event.task_id)
            if span_ctx is None and event.event_type in ("TaskSubmitted", "TaskQueued"):
                # Fallback: TaskCreated not yet processed — attach to session trace only.
                if self._session_span:
                    session_ctx = self._session_span.get_span_context()
                    if session_ctx.is_valid:
                        return hex(session_ctx.trace_id), None

        elif event.event_type in ("TaskCompleted", "TaskFailed", "TaskCanceled"):
            span_ctx = self._completed_span_ctx.pop(event.task_id, None)

        elif event.event_type in ("SessionStarted", "SessionEnded"):
            if self._session_span:
                span_ctx = self._session_span.get_span_context()
            elif event.event_type == "SessionEnded":
                # Same-batch: _process_event already ended _session_span; use snapshot.
                span_ctx = self._completed_session_ctx

        elif event.event_type == "ResourceUpdate":
            # Node/GPU metric belongs to the session scope — attach to the session
            # span so it is queryable as a child of the root in OTel tools.
            if self._session_span:
                span_ctx = self._session_span.get_span_context()

        else:
            # Generic fallback for custom/extension events (e.g.
            # any user-emitted event via telemetry.emit()).
            # Correlation by duck typing:
            #   - has task_id → belongs to the task span (active or recently completed)
            #   - no task_id  → belongs to the session span
            # Use .get not .pop — custom events are never terminal; they never own the
            # span lifecycle.
            task_id = getattr(event, "task_id", None)
            if task_id:
                span = self._active_spans.get(task_id)
                if span:
                    span_ctx = span.get_span_context()
                else:
                    span_ctx = self._completed_span_ctx.get(task_id)
            elif self._session_span:
                span_ctx = self._session_span.get_span_context()

        if span_ctx is None or not span_ctx.is_valid:
            return None, None
        return hex(span_ctx.trace_id), hex(span_ctx.span_id)

    # ------------------------------------------------------------------
    # Internal: dispatch loop
    # ------------------------------------------------------------------

    async def _dispatch_loop(self, queue: asyncio.Queue) -> None:
        """Background task: drain ``queue`` in batches, update OTel instruments, notify subscribers.

        The queue is passed in, not read from ``self._queue``: a loop only ever serves the queue
        it was started with, even if a later start() creates a new one.
        """
        from opentelemetry import context as otel_context
        from opentelemetry import trace

        _batch_size = 500

        while True:
            # Fast path: drain up to _batch_size events without yielding to the event loop.
            # get_nowait() is a plain deque.popleft() — zero asyncio scheduling overhead.
            batch: list[BaseEvent] = []
            try:
                while len(batch) < _batch_size:
                    item = queue.get_nowait()
                    if item is _STOP_SENTINEL:
                        queue.task_done()
                        if batch:
                            self._flush_batch(batch, trace, otel_context)
                            for _ in batch:
                                queue.task_done()
                        return
                    batch.append(item)
            except asyncio.QueueEmpty:
                pass

            if not batch:
                # Slow path: queue was empty — wait without a timeout.
                # Plain queue.get(), no wait_for(), no timer handle, no Task allocation.
                item = await queue.get()
                if item is _STOP_SENTINEL:
                    queue.task_done()
                    return
                batch.append(item)

            self._flush_batch(batch, trace, otel_context)
            for _ in batch:
                queue.task_done()  # task_done AFTER processing (Fix 1)

            # Yield only when a full batch was drained — queue likely has more items.
            # Partial batch means queue is exhausted; await queue.get() yields naturally.
            if len(batch) == _batch_size:
                await asyncio.sleep(0)

    def _flush_batch(self, batch: list[BaseEvent], trace_mod: Any, otel_context: Any) -> None:
        """Process a collected batch: OTel instruments, JSONL write, subscribers."""
        self._inflight = len(batch)  # reported by stop() if the loop dies in this batch
        routes = self._routes
        default_route = self._default_route
        for event in batch:
            self._event_log.append(event)
            try:
                self._process_event(event, trace_mod, otel_context)
            except Exception:
                logger.exception("Error processing telemetry event %s", event.event_type)
            # One dictionary lookup per event: a backend with its own instrumentation has its
            # own route (the default route plus its own), every other backend the default route.
            route = routes.get(event.backend, default_route)
            if route:
                self._route_event(event, route)

        if self._checkpoint_file is not None:
            self._write_batch(batch)

        if self._subscribers:
            for event in batch:
                for sub in self._subscribers:
                    try:
                        if asyncio.iscoroutinefunction(sub):
                            asyncio.create_task(sub(event))
                        else:
                            sub(event)
                    except Exception:
                        logger.debug("Subscriber raised exception", exc_info=True)
        self._inflight = 0

    def _write_batch(self, events: list[BaseEvent]) -> None:
        """Write a batch of events to JSONL in a single file.write() call."""
        lines = []
        for event in events:
            try:
                # fields() + getattr: shallow copy that captures init=False class-level
                # fields (e.g. event_type) which vars()/.__dict__ misses on frozen subclasses.
                d = {f.name: getattr(event, f.name) for f in dataclasses.fields(event)}
                trace_id, span_id = self._span_ids_for_event(event)
                d["trace_id"] = trace_id
                d["span_id"] = span_id
                d["name"] = event.event_type
                d["section"] = "event"
                lines.append(json.dumps(d, default=str))
            except Exception:
                logger.debug("Failed to serialize event for checkpoint", exc_info=True)
        if lines:
            try:
                self._checkpoint_file.write("\n".join(lines) + "\n")
            except OSError:
                logger.warning(
                    "Telemetry checkpoint write failed; events lost for this batch",
                    exc_info=True,
                )

    def _process_event(self, event: BaseEvent, trace_mod: Any, otel_context: Any) -> None:
        """Update the task and session spans for an event (metrics: the task instrumentation)."""
        etype = event.event_type

        if etype == "TaskCreated":
            executable = event.attributes.get("executable", "")
            task_type = event.attributes.get("task_type", "")
            a = {
                "session_id": event.session_id,
                "backend": event.backend or "",
                "task_id": event.task_id,
                "executable": executable,
                "task_type": task_type,
            }
            for extractor in self._span_attribute_extractors:
                try:
                    a.update(extractor(event))
                except Exception:
                    logger.debug("span attribute extractor failed", exc_info=True)
            # TELEMETRY-LIMITATION(L4): the task span starts (and later ends) at the time the
            # dispatch loop processes the event, not at the event's own time (existing issue
            # T3), while spans recorded by instrumentations can use real times, so the two can
            # be offset by the dispatch delay. Why not now: existing behaviour of all task
            # spans, reported and not changed here. Planned: Tier 3 (Tier 2 partially).
            # See docs/telemetry/reference.md#known-limitations.
            _ctx = self._task_contexts.pop(event.task_id, None) or self._session_ctx
            self._active_spans[event.task_id] = self._tracer.start_span(
                "task", context=_ctx, attributes=a
            )

        elif etype == "TaskStarted":
            if event.task_id not in self._active_spans:
                # Fallback: TaskCreated was never seen (incomplete lifecycle).
                a_span = {
                    "session_id": event.session_id,
                    "backend": event.backend or "",
                    "executable": event.attributes.get("executable", ""),
                    "task_type": event.attributes.get("task_type", ""),
                    "task_id": event.task_id,
                }
                for extractor in self._span_attribute_extractors:
                    try:
                        a_span.update(extractor(event))
                    except Exception:
                        logger.debug("span attribute extractor failed", exc_info=True)
                _ctx = self._task_contexts.pop(event.task_id, None) or self._session_ctx
                self._active_spans[event.task_id] = self._tracer.start_span(
                    "task", context=_ctx, attributes=a_span
                )

        elif etype == "TaskCompleted":
            executable = event.attributes.get("executable", "")
            task_type = event.attributes.get("task_type", "")
            span = self._active_spans.pop(event.task_id, None)
            if span is None:
                span = self._tracer.start_span(
                    "task",
                    context=self._session_ctx,
                    attributes=self._fallback_span_attributes(event, executable, task_type),
                )
            span.set_attribute("status", "completed")
            span.set_attribute("executable", executable)
            span.set_attribute("task_type", task_type)
            if event.attributes.get("incomplete_lifecycle"):
                span.set_attribute("incomplete_lifecycle", True)
            span.set_status(trace_mod.StatusCode.OK)
            span.end()
            self._completed_span_ctx[event.task_id] = span.get_span_context()

        elif etype == "TaskFailed":
            executable = event.attributes.get("executable", "")
            task_type = event.attributes.get("task_type", "")
            error_type = event.attributes.get("error_type", event.error_type)
            span = self._active_spans.pop(event.task_id, None)
            if span is None:
                span = self._tracer.start_span(
                    "task",
                    context=self._session_ctx,
                    attributes=self._fallback_span_attributes(event, executable, task_type),
                )
            span.set_attribute("status", "failed")
            span.set_attribute("error_type", error_type)
            span.set_attribute("executable", executable)
            span.set_attribute("task_type", task_type)
            if event.attributes.get("incomplete_lifecycle"):
                span.set_attribute("incomplete_lifecycle", True)
            span.set_status(trace_mod.StatusCode.ERROR, error_type)
            span.end()
            self._completed_span_ctx[event.task_id] = span.get_span_context()

        elif etype == "TaskCanceled":
            executable = event.attributes.get("executable", "")
            task_type = event.attributes.get("task_type", "")
            span = self._active_spans.pop(event.task_id, None)
            if span is None:
                span = self._tracer.start_span(
                    "task",
                    context=self._session_ctx,
                    attributes=self._fallback_span_attributes(event, executable, task_type),
                )
            span.set_attribute("status", "canceled")
            span.set_attribute("executable", executable)
            span.set_attribute("task_type", task_type)
            if event.attributes.get("incomplete_lifecycle"):
                span.set_attribute("incomplete_lifecycle", True)
            span.set_status(trace_mod.StatusCode.ERROR)
            span.end()
            self._completed_span_ctx[event.task_id] = span.get_span_context()

        elif etype == "TaskQueued":
            span = self._active_spans.get(event.task_id)
            if span is not None and span.is_recording():
                span.add_event("TaskQueued", attributes={"task_id": event.task_id})

        elif etype == "SessionStarted":
            pass  # session span created synchronously in start() for immediate availability

        elif etype == "SessionEnded":
            if self._session_span:
                self._completed_session_ctx = self._session_span.get_span_context()
                self._session_span.end()
                self._session_span = None

    @staticmethod
    def _fallback_span_attributes(event: BaseEvent, executable: str, task_type: str) -> dict:
        """Attributes of a task span created at its terminal event (TaskCreated was never seen)."""
        return {
            "session_id": event.session_id,
            "backend": event.backend or "",
            "executable": executable,
            "task_type": task_type,
            "task_id": event.task_id,
        }

    # ------------------------------------------------------------------
    # Internal: checkpoint file
    # ------------------------------------------------------------------

    def _open_checkpoint_file(self) -> None:
        path = Path(self._checkpoint_path)
        path.mkdir(parents=True, exist_ok=True)
        ts = int(self._session_start_time or time.time())
        filename = f"{self._session_id}.{ts}.telemetry.jsonl"
        filepath = path / filename
        try:
            self._checkpoint_file = open(filepath, "w", buffering=131072)  # 128 KB block buffer
            logger.info("Telemetry checkpoint file: %s", filepath)
        except OSError:
            logger.exception("Could not open telemetry checkpoint file %s", filepath)

    def _write_line(self, section: str, data: dict) -> None:
        """Append one JSON line to the checkpoint file."""
        data["section"] = section
        self._checkpoint_file.write(json.dumps(data, default=str) + "\n")

    def _write_metric_snapshot(self) -> None:
        """Append a metric snapshot line to the checkpoint file.

        TELEMETRY-LIMITATION(L3): the snapshot sums data points across all attributes (existing
        issue T9), so series an instrumentation splits by attribute (for example by request
        source) are added together in the JSONL file; ``read_metrics()`` keeps the dimensions.
        Why not now: existing format, reported and not changed here. Planned: Tier 2 (JSONL as a
        proper metric exporter). See docs/telemetry/reference.md#known-limitations.
        """
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
                "status_code": s.status.status_code.name,
                "status_description": s.status.description or None,
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


def _task_executable(task: dict) -> str:
    """Extract the executable/function identifier from a task dict."""
    # ComputeTask stores executable directly
    if "executable" in task:
        return str(task["executable"])
    # FunctionTask stores func reference. The label must not vary per task (it is a
    # metric attribute): unwrap partials, and never use str(func), which embeds an address.
    func = task.get("func") or task.get("function")
    if func is not None:
        while isinstance(func, functools.partial):
            func = func.func
        return getattr(func, "__name__", None) or type(func).__name__
    # AITask or others may have a model field
    model = task.get("model") or task.get("model_name")
    if model:
        return str(model)
    return ""
