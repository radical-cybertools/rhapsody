"""RHAPSODY telemetry events.

All telemetry originates from these normalized events. Frozen dataclasses ensure
immutability and safe zero-copy fan-out to multiple subscribers.

Canonical lifecycle:
    TaskSubmitted → (TaskQueued) → TaskStarted → TaskCompleted | TaskFailed

TaskQueued is optional. TaskStarted is the authoritative "execution began" event.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from dataclasses import field


def _new_event_id() -> str:
    return uuid.uuid4().hex


def _now_wall() -> float:
    return time.time()


@dataclass(frozen=True)
class BaseEvent:
    """Base class for all RHAPSODY telemetry events.

    Attributes:
        event_id:   Unique event identifier (uuid4 hex, no hyphens).
        event_type: Discriminator string matching the subclass name.
        event_time: Wall-clock time when the event actually occurred (from task
                    history where available; otherwise time of emission).
        emit_time:  Wall-clock time at the point of emission into the event bus.
                    Both event_time and emit_time use time.time() — their
                    difference is the queue latency in seconds.
        session_id: Owning session identifier.
        backend:    Name of the execution backend that produced the event.
        task_id:    Task identifier, None for session-level events.
        node_id:    Node/worker identifier, None when not applicable.
        attributes: Optional metadata dict. Carries task-context fields such as
                    executable, task_type, error_type (for lifecycle events) and
                    incomplete_lifecycle when STARTED was not recorded.
    """

    event_id: str
    event_type: str
    event_time: float
    emit_time: float
    session_id: str
    backend: str
    task_id: str | None = None
    node_id: str | None = None
    attributes: dict = field(default_factory=dict)


@dataclass(frozen=True)
class SessionStarted(BaseEvent):
    """Emitted when a session begins (telemetry enabled and started)."""

    event_type: str = field(default="SessionStarted", init=False)


@dataclass(frozen=True)
class SessionEnded(BaseEvent):
    """Emitted when a session ends (telemetry stopped)."""

    duration_seconds: float = 0.0
    event_type: str = field(default="SessionEnded", init=False)


@dataclass(frozen=True)
class TaskSubmitted(BaseEvent):
    """Emitted when a task is submitted to the session.

    attributes keys: executable, task_type
    """

    event_type: str = field(default="TaskSubmitted", init=False)


@dataclass(frozen=True)
class TaskQueued(BaseEvent):
    """Emitted when a task is handed off to a backend's execution queue.

    Optional event. Marks the boundary between session-level submission and
    backend-level scheduling. Gap between TaskSubmitted.event_time and
    TaskQueued.event_time is the session routing latency.

    attributes keys: executable, task_type
    """

    event_type: str = field(default="TaskQueued", init=False)


@dataclass(frozen=True)
class TaskStarted(BaseEvent):
    """Emitted when a task transitions to RUNNING state (execution physically begins).

    attributes keys: executable, task_type
    """

    event_type: str = field(default="TaskStarted", init=False)


@dataclass(frozen=True)
class TaskCompleted(BaseEvent):
    """Emitted when a task reaches DONE state.

    attributes keys: executable, task_type
    If incomplete_lifecycle=True in attributes, duration_seconds is 0.0
    because the STARTED timestamp was not recorded.
    """

    duration_seconds: float = 0.0
    event_type: str = field(default="TaskCompleted", init=False)


@dataclass(frozen=True)
class TaskFailed(BaseEvent):
    """Emitted when a task reaches FAILED state.

    attributes keys: executable, task_type, error_type
    If incomplete_lifecycle=True in attributes, duration_seconds is 0.0
    because the STARTED timestamp was not recorded.
    """

    duration_seconds: float = 0.0
    error_type: str = "unknown"
    event_type: str = field(default="TaskFailed", init=False)


@dataclass(frozen=True)
class ResourceUpdate(BaseEvent):
    """Emitted periodically by backend adapters with node resource utilization.

    cpu/memory/gpu values are point-in-time percentages (not deltas). disk/net values are byte
    deltas since the previous poll. gpu_percent is None when the backend does not expose GPU
    metrics. disk/net fields are None when psutil is unavailable.
    """

    cpu_percent: float = 0.0
    memory_percent: float = 0.0
    gpu_percent: float | None = None
    disk_read_bytes: float | None = None
    disk_write_bytes: float | None = None
    net_sent_bytes: float | None = None
    net_recv_bytes: float | None = None
    event_type: str = field(default="ResourceUpdate", init=False)


def make_event(cls: type, *, session_id: str, backend: str, **kwargs) -> BaseEvent:
    """Construct any event with auto-generated id and consistent wall-clock timestamps.

    Both event_time and emit_time use time.time() so their difference is interpretable as queue
    latency in seconds (emit_time >= event_time always holds when event_time is taken from a recent
    task history entry).
    """
    now = time.time()
    return cls(
        event_id=_new_event_id(),
        event_time=kwargs.pop("event_time", now),
        emit_time=now,
        session_id=session_id,
        backend=backend,
        **kwargs,
    )
