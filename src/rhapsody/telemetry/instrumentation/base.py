"""Base classes of the instrumentation layer.

An *instrumentation* is the only place that knows a telemetry domain (inference, tasks, ...): metric
names, units, bucket advice, span attributes. It uses the OpenTelemetry **API** only, through the
scoped meter and tracer the host hands it, and it never imports the SDK.

The :class:`~rhapsody.telemetry.manager.TelemetryManager` is becoming a generic host: it owns the
providers, the lifecycle and the event routing, and knows no inference domain (the task metrics
still live in it until they move to a task instrumentation). It routes every event by
``event.backend`` to the instrumentations of that backend (one dictionary lookup per event) and
calls :meth:`Instrumentation.on_event`.

A backend names its domain with the plain string class attribute ``telemetry_domain``; the
registry (:mod:`rhapsody.telemetry.instrumentation.registry`) maps that name to the class.
"""

from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable

if TYPE_CHECKING:
    from rhapsody.telemetry.events import BaseEvent


@dataclass(frozen=True)
class InstrumentationContext:
    """What the host gives an instrumentation when it starts it.

    Attributes:
        meter: OTel ``Meter`` scoped to the instrumentation's ``scope_name``.
        tracer: OTel ``Tracer`` scoped to the instrumentation's ``scope_name``.
        session_id: Session the events belong to.
        backend_name: Name of the backend this instrumentation serves.
        session_span_context: Returns the session span's ``SpanContext``, or None once it ended.
        task_span: ``task_span(task_id)`` returns the task's open span, or None.
        span_context_for: ``span_context_for(task_id)`` returns the ``SpanContext`` of the task's
            open span, or of its ended span while the host still holds it, or None.
    """

    meter: Any
    tracer: Any
    session_id: str
    backend_name: str
    session_span_context: Callable[[], Any]
    task_span: Callable[[str], Any]
    span_context_for: Callable[[str], Any]


class Instrumentation(ABC):
    """One telemetry domain, hosted by the TelemetryManager for one backend.

    Lifecycle: ``instrument(ctx)`` when the host starts (or when the backend is attached to a
    running host), ``on_event(event)`` for every event of the backend, ``uninstrument()`` before
    the host's providers shut down. After ``uninstrument()`` the host may call ``instrument()``
    again with a new context (a manager that is started again), so ``instrument()`` must create
    **all** its instruments and reset **all** per-session state: nothing may be cached in
    ``__init__``, because the providers of the previous run are shut down.

    Events are routed by backend name, not by event type, so ``on_event`` also receives the other
    events of its backend (task events, for example). It must ignore the event types it does not
    own. It runs in the host's dispatch loop: keep it fast. An exception is logged by the host and
    never stops the other events.
    """

    #: Name of the OTel instrumentation scope, ``rhapsody.instrumentation.<domain>``.
    scope_name: str

    @abstractmethod
    def instrument(self, ctx: InstrumentationContext) -> None:
        """Create the instruments and keep the context."""

    @abstractmethod
    def on_event(self, event: BaseEvent) -> None:
        """Handle one event of this instrumentation's backend; ignore types it does not own."""

    def uninstrument(self) -> None:  # noqa: B027
        """Release what ``instrument()`` created.

        Default: nothing to release.
        """
