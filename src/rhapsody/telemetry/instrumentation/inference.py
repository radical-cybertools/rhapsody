"""Inference telemetry: batch spans, per-request queue waits and batch metrics.

The only place that knows the inference telemetry. The vLLM backend reports plain data through its
emitter (two events per batch, ``rhapsody.inference.BatchStarted`` and ``BatchEnded``); this
instrumentation turns it into OpenTelemetry using the API only. It records:

- one ``chat {model}`` span per batch (``SpanKind.CLIENT``) with the batch's real start and end
  times, parented to the session span and linked to the task spans of the batch;
- the per-task facts (model, batch id, prompt count, queue wait, timeouts) as attributes of the
  open task spans;
- histograms of the request queue wait, the batch duration and the Dragon model duration, and
  counters of requests, prompts, timed-out prompts and batches.

No prompt or response text is ever carried. The feature is experimental: the GenAI semantic
conventions it follows are still in development.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from typing import Any

from opentelemetry import trace

from rhapsody.telemetry.instrumentation.base import Instrumentation

if TYPE_CHECKING:
    from rhapsody.telemetry.events import BaseEvent
    from rhapsody.telemetry.instrumentation.base import InstrumentationContext

logger = logging.getLogger(__name__)

_PREFIX = "rhapsody.inference."

# Duration histograms: advisory buckets 0.01 s ... 327.68 s, doubling (needs opentelemetry-sdk 1.30+).
_DURATION_BUCKETS = [0.01 * 2**k for k in range(16)]

# TELEMETRY-LIMITATION(L7): the batch span links to at most 128 task spans, because the SDK keeps
# at most 128 links per span by default (OTEL_SPAN_LINK_COUNT_LIMIT) and a larger batch would only
# build links that are thrown away; the real request count is the span attribute
# rhapsody.inference.batch.requests.
# Why not now: the SDK default is a deliberate bound. Planned: configuration.
# See docs/telemetry/reference.md#known-limitations.
_MAX_LINKS = 128

# TELEMETRY-LIMITATION(L8): the attribute names follow the OpenTelemetry GenAI semantic
# conventions (``gen_ai.*``), which are still in development and may change; they are the names of
# the installed opentelemetry-semantic-conventions at the time of writing, and ``gen_ai.provider.name``
# is not set because vLLM has no registered value.
# Why not now: there is no stable version to pin. Planned: follow the conventions when they
# stabilize. See docs/telemetry/reference.md#known-limitations.
_MODEL = "gen_ai.request.model"
_OPERATION = "gen_ai.operation.name"
_ERROR_TYPE = "error.type"
_SOURCE = "rhapsody.inference.request.source"
_TRIGGER = "rhapsody.inference.batch.trigger"


class InferenceInstrumentation(Instrumentation):
    """Records the telemetry of the vLLM batcher from its ``rhapsody.inference.*`` events."""

    scope_name = "rhapsody.instrumentation.inference"

    def __init__(self) -> None:
        self._handlers = {
            _PREFIX + "BatchStarted": self._on_batch_started,
            _PREFIX + "BatchEnded": self._on_batch_ended,
        }

    def instrument(self, ctx: InstrumentationContext) -> None:
        # TELEMETRY-LIMITATION(L16): only what the batch events carry is recorded, so there is no
        # live queue depth and no cumulative totals between batches (the vLLM batcher is one
        # sequential loop: one batch is in flight at a time).
        # Why not now: reading live state needs an instrument callback, which the emitter handle
        # does not offer yet. Planned: the emitter handle gains `.meter` (Tier 1).
        # See docs/telemetry/reference.md#known-limitations.
        self._ctx = ctx
        meter = ctx.meter
        self._h_queue = meter.create_histogram(
            "rhapsody.inference.request.queue.duration",
            unit="s",
            description="Time a request waited in the batcher queue before its batch was sent",
            explicit_bucket_boundaries_advisory=_DURATION_BUCKETS,
        )
        self._h_batch = meter.create_histogram(
            "rhapsody.inference.batch.duration",
            unit="s",
            description="Time from sending a batch to Dragon until its last response",
            explicit_bucket_boundaries_advisory=_DURATION_BUCKETS,
        )
        self._h_model = meter.create_histogram(
            "rhapsody.inference.model.duration",
            unit="s",
            description="Model inference time of a batch as reported by Dragon",
            explicit_bucket_boundaries_advisory=_DURATION_BUCKETS,
        )
        self._c_requests = meter.create_counter(
            "rhapsody.inference.requests", unit="{request}", description="Requests batched"
        )
        self._c_prompts = meter.create_counter(
            "rhapsody.inference.prompts", unit="{prompt}", description="Prompts batched"
        )
        self._c_timed_out = meter.create_counter(
            "rhapsody.inference.timed_out.prompts",
            unit="{prompt}",
            description="Prompts that got no response within the batch deadline",
        )
        self._c_batches = meter.create_counter(
            "rhapsody.inference.batches", unit="{batch}", description="Batches sent to Dragon"
        )
        # The batch span of the batch in flight (batch id -> span). The batcher is one sequential
        # loop, so at most one is open.
        self._open: dict[Any, Any] = {}

    def uninstrument(self) -> None:
        # A batch that never ended (the backend's failure path emits no BatchEnded) still has its
        # span open: end it before the providers shut down.
        for batch_id in list(self._open):
            self._end_unfinished(self._open.pop(batch_id))

    def on_event(self, event: BaseEvent) -> None:
        handler = self._handlers.get(event.event_type)
        if handler is not None:
            handler(event)

    # -- batch events --------------------------------------------------------

    def _on_batch_started(self, event: BaseEvent) -> None:
        attrs = event.attributes
        # The batcher is one sequential loop: a span still open here belongs to a batch that never
        # ended (failure or cancel).
        for batch_id in list(self._open):
            self._end_unfinished(self._open.pop(batch_id))

        links = []
        for task_id in attrs.get("task_ids") or ():
            if len(links) >= _MAX_LINKS:
                break
            span = self._ctx.task_span(task_id)
            if span is not None:
                links.append(trace.Link(span.get_span_context()))

        session_ctx = self._ctx.session_span_context()
        parent = (
            trace.set_span_in_context(trace.NonRecordingSpan(session_ctx))
            if session_ctx is not None
            else None
        )
        model = attrs.get("model")
        span = self._ctx.tracer.start_span(
            f"chat {model}" if model else "chat",
            context=parent,
            kind=trace.SpanKind.CLIENT,
            start_time=_ns(event.event_time),
            links=links,
            attributes=_clean(
                {
                    "session_id": event.session_id,
                    "backend": event.backend,
                    _OPERATION: "chat",
                    _MODEL: model,
                    "rhapsody.inference.batch.id": attrs.get("batch_id"),
                    "rhapsody.inference.batch.size": attrs.get("batch_prompts"),
                    "rhapsody.inference.batch.requests": attrs.get("batch_requests"),
                    _TRIGGER: attrs.get("batch_trigger"),
                }
            ),
        )
        self._open[attrs.get("batch_id")] = span

    def _on_batch_ended(self, event: BaseEvent) -> None:
        attrs = event.attributes
        batch_id = attrs.get("batch_id")
        span = self._open.pop(batch_id, None)
        model = attrs.get("model")
        try:
            self._record_batch(event, attrs, model)
            self._record_requests(event, attrs, model)
        finally:
            if span is not None:
                self._end_batch_span(span, event, attrs)

    def _record_batch(self, event: BaseEvent, attrs: dict, model: Any) -> None:
        base = _clean({"session_id": event.session_id, "backend": event.backend, _MODEL: model})
        duration = attrs.get("batch_duration_s")
        if _is_number(duration):
            self._h_batch.record(duration, base)
        model_duration = attrs.get("dragon_model_duration_s")
        if _is_number(model_duration) and model_duration > 0:
            self._h_model.record(model_duration, base)
        self._c_batches.add(1, {**base, _TRIGGER: attrs.get("batch_trigger") or "unknown"})

    def _record_requests(self, event: BaseEvent, attrs: dict, model: Any) -> None:
        base = _clean({"session_id": event.session_id, "backend": event.backend, _MODEL: model})
        # The metric attributes of each source, built once per batch.
        labels = {source: {**base, _SOURCE: source} for source in ("task", "api")}
        # source -> [requests, prompts, timed out prompts]; a few counter calls per batch.
        totals = {"task": [0, 0, 0], "api": [0, 0, 0]}
        task_attributes = _clean(
            {_MODEL: model, "rhapsody.inference.batch.id": attrs.get("batch_id")}
        )
        for row in attrs.get("requests") or ():
            values = _row_values(row)
            if values is None:
                logger.debug("Skipping a malformed inference request row: %r", row)
                continue
            task_id, prompts, queue, timed_out = values
            source = "task" if task_id else "api"
            self._h_queue.record(queue, labels[source])
            total = totals[source]
            total[0] += 1
            total[1] += prompts
            total[2] += timed_out
            span = self._ctx.task_span(task_id) if task_id else None
            if span is not None:
                facts = {
                    **task_attributes,
                    "rhapsody.inference.prompt_count": prompts,
                    "rhapsody.inference.request.queue.duration": queue,
                }
                if timed_out:
                    facts[_ERROR_TYPE] = "TimeoutError"
                    facts["rhapsody.inference.timed_out_prompts"] = timed_out
                span.set_attributes(facts)
        for source, (requests, prompts, timed_out) in totals.items():
            if requests:
                self._c_requests.add(requests, labels[source])
                self._c_prompts.add(prompts, labels[source])
                if timed_out:
                    self._c_timed_out.add(timed_out, labels[source])

    def _end_batch_span(self, span: Any, event: BaseEvent, attrs: dict) -> None:
        try:
            dragon_duration = attrs.get("dragon_model_duration_s")
            if _is_number(dragon_duration):
                span.set_attribute("rhapsody.inference.dragon.model.duration", dragon_duration)
            host = attrs.get("dragon_worker_host")
            if host:
                span.set_attribute("rhapsody.inference.dragon.worker.host", host)
            timed_out = attrs.get("timed_out_prompts") or 0
            if timed_out:
                span.set_attribute(_ERROR_TYPE, "TimeoutError")
                span.set_attribute("rhapsody.inference.timed_out_prompts", timed_out)
                span.set_status(trace.StatusCode.ERROR, f"{timed_out} prompts timed out")
        finally:
            end = _ns(event.event_time)
            start = getattr(span, "start_time", None)
            span.end(end_time=max(end, start) if start is not None else end)

    @staticmethod
    def _end_unfinished(span: Any) -> None:
        span.set_status(trace.StatusCode.ERROR, "batch did not finish")
        span.end()


def _ns(seconds: float) -> int:
    return int(seconds * 1e9)


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _row_values(row: Any) -> tuple | None:
    """``(task_id, prompt_count, queue_duration_s, timed_out_prompts)`` of a request row.

    None when the row is not a dict or a number is missing or not a number, so a bad row is skipped
    as a whole and never leaves the histogram and the counters out of step.
    """
    if not isinstance(row, dict):
        return None
    prompts = row.get("prompt_count")
    queue = row.get("queue_duration_s")
    timed_out = row.get("timed_out_prompts") or 0
    if not (_is_number(prompts) and _is_number(queue) and _is_number(timed_out)):
        return None
    return row.get("task_id"), prompts, queue, timed_out


def _clean(attributes: dict) -> dict:
    """Drop None values: OpenTelemetry attributes cannot be None."""
    return {k: v for k, v in attributes.items() if v is not None}
