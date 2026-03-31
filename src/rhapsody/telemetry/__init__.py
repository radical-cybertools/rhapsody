"""RHAPSODY Telemetry System.

Event-driven, backend-agnostic telemetry using OpenTelemetry SDK as the
metrics and traces backend (in-memory, no exporters).

Usage::

    session = Session(backends=[backend])
    telemetry = session.enable_telemetry()
    await telemetry.start()

    telemetry.subscribe(lambda e: print(e.event_type))

    async with session:
        await session.submit_tasks(tasks)
        await session.wait_tasks(tasks)

    spans   = telemetry.read_traces()
    metrics = telemetry.read_metrics()

Install::

    pip install 'rhapsody-py[telemetry]'
"""

from rhapsody.telemetry.events import BaseEvent
from rhapsody.telemetry.events import ResourceUpdate
from rhapsody.telemetry.events import SessionEnded
from rhapsody.telemetry.events import SessionStarted
from rhapsody.telemetry.events import TaskCompleted
from rhapsody.telemetry.events import TaskFailed
from rhapsody.telemetry.events import TaskQueued
from rhapsody.telemetry.events import TaskStarted
from rhapsody.telemetry.events import TaskSubmitted
from rhapsody.telemetry.interfaces.reader import TelemetryReader
from rhapsody.telemetry.interfaces.subscriber import TelemetrySubscriber
from rhapsody.telemetry.manager import TelemetryManager

__all__ = [
    "TelemetryManager",
    "TelemetryReader",
    "TelemetrySubscriber",
    "BaseEvent",
    "SessionStarted",
    "SessionEnded",
    "TaskSubmitted",
    "TaskQueued",
    "TaskStarted",
    "TaskCompleted",
    "TaskFailed",
    "ResourceUpdate",
]
