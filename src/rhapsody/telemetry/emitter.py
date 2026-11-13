"""The telemetry handle a backend uses to report plain data.

Every backend (execution, AI, data) gets an :class:`Emitter` as ``backend.telemetry``. When the
backend is attached to a session or workflow engine with telemetry, the
:class:`~rhapsody.telemetry.manager.TelemetryManager` hands it a :class:`BackendEmitter` through
the backend's ``register_emitter`` hook. Until then, and whenever telemetry is off,
``backend.telemetry`` is a :class:`NoopEmitter`, so a backend never checks for ``None``::

    if self.telemetry.enabled:
        self.telemetry.emit("rhapsody.inference.BatchStarted", {"batch_id": 1})

A backend uses only this handle. It never sees the ``TelemetryManager`` and never imports
OpenTelemetry. RHAPSODY does not own the execution events of the runtimes behind its backends
(Dragon, Dask, RADICAL-Pilot, ...), so a backend translates what the runtime reports into plain
data; the instrumentation for that backend's domain turns it into spans and metrics. See
docs/telemetry/reference.md ("Why backends do not call OTel").

This module imports only the standard library and ``rhapsody.telemetry.events``: importing it never
pulls in OpenTelemetry.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Protocol

from rhapsody.telemetry.events import BaseEvent
from rhapsody.telemetry.events import make_event

if TYPE_CHECKING:
    from rhapsody.telemetry.manager import TelemetryManager


class Emitter(Protocol):
    """What a backend may do with telemetry: check ``enabled`` and ``emit`` plain data."""

    enabled: bool

    def emit(
        self,
        name: str,
        attributes: dict | None = None,
        *,
        event_time: float | None = None,
        task_id: str | None = None,
    ) -> None:
        """Report one event.

        Args:
            name: Namespaced event name (contains a ``.``), e.g. ``rhapsody.inference.BatchEnded``.
            attributes: Plain numbers, strings and lists/dicts of them. Never prompt or response
                text. The caller hands the dict over and must not mutate it afterwards.
            event_time: When the event really happened (``time.time()``); defaults to now.
            task_id: The task the event belongs to, if any.
        """


class NoopEmitter:
    """The emitter of a backend with no telemetry: ``enabled`` is False and ``emit`` does nothing."""

    enabled = False

    def emit(
        self,
        name: str,
        attributes: dict | None = None,
        *,
        event_time: float | None = None,
        task_id: str | None = None,
    ) -> None:
        return None


class BackendEmitter:
    """Emitter bound to one backend of one :class:`TelemetryManager`.

    Events carry the manager's session id and the backend's name, which is also the key the manager
    routes them by. ``enabled`` follows the manager: False before ``start()`` and after ``stop()``.

    TELEMETRY-LIMITATION(L17): ``emit()`` must be called on the event-loop thread, because the
    manager's queue (``asyncio.Queue.put_nowait``) is not thread-safe. Backends whose callbacks run
    in worker threads (Dask, RADICAL-Pilot) would need to marshal onto the loop first.
    Why not now: the only emitting backend, the vLLM batcher, runs on the loop.
    Planned: Tier 1 of the telemetry refactor (the OTel API is thread-safe) or a
    ``call_soon_threadsafe`` hop in the emitter. See docs/telemetry/reference.md#known-limitations.
    """

    __slots__ = ("_manager", "_backend_name")

    def __init__(self, manager: TelemetryManager, backend_name: str) -> None:
        self._manager = manager
        self._backend_name = backend_name

    @property
    def enabled(self) -> bool:
        return self._manager._running

    def emit(
        self,
        name: str,
        attributes: dict | None = None,
        *,
        event_time: float | None = None,
        task_id: str | None = None,
    ) -> None:
        # A namespaced name keeps backends from emitting the host's own event types
        # (TaskCreated, ...); same rule as define_event().
        if "." not in name:
            raise ValueError(
                f"Emitter.emit: name {name!r} must be namespaced (e.g. 'rhapsody.inference.{name}')."
            )
        manager = self._manager
        if not manager._running:
            return
        extra = {} if event_time is None else {"event_time": event_time}
        manager.emit(
            make_event(
                BaseEvent,
                event_type=name,
                session_id=manager.session_id,
                backend=self._backend_name,
                task_id=task_id,
                attributes={} if attributes is None else attributes,
                **extra,
            )
        )

    def __reduce__(self):
        # A pickled backend (the vLLM backend defines __getstate__, Dragon ships backends to
        # workers) must not carry the manager along: it comes back with a no-op emitter.
        return (NoopEmitter, ())
