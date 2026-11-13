"""Instrumentation layer: domain telemetry hosted by the generic TelemetryManager."""

from rhapsody.telemetry.instrumentation.base import Instrumentation
from rhapsody.telemetry.instrumentation.base import InstrumentationContext
from rhapsody.telemetry.instrumentation.registry import get_instrumentation_class
from rhapsody.telemetry.instrumentation.registry import register_instrumentation

__all__ = [
    "Instrumentation",
    "InstrumentationContext",
    "get_instrumentation_class",
    "register_instrumentation",
]
