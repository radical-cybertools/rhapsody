"""Maps a backend's ``telemetry_domain`` to the instrumentation class that serves it.

The host looks the name up when a backend is attached. A class may be registered directly or as a
``"module:ClassName"`` string, which is imported on first use so that importing the registry never
imports OpenTelemetry or any instrumentation.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rhapsody.telemetry.instrumentation.base import Instrumentation

# domain name -> class, or "module:ClassName" resolved on first use.
_REGISTRY: dict[str, type[Instrumentation] | str] = {
    "task": "rhapsody.telemetry.instrumentation.task:TaskInstrumentation",
    "inference": "rhapsody.telemetry.instrumentation.inference:InferenceInstrumentation",
}

# Domains the manager hosts for every session and routes every event to (not tied to a backend).
# A fixed constant, not a registration flag: a plugin cannot put its own call on every event.
DEFAULT_DOMAINS: tuple[str, ...] = ("task",)


def register_instrumentation(domain: str, instrumentation: type[Instrumentation] | str) -> None:
    """Register the instrumentation class (or ``"module:Class"`` path) for a telemetry domain.

    Registering a domain again replaces the earlier entry.
    """
    _REGISTRY[domain] = instrumentation


def get_instrumentation_class(domain: str) -> type[Instrumentation] | None:
    """Return the class registered for ``domain``, or None if there is none.

    A ``"module:Class"`` entry is imported here, so a bad path raises ``ImportError`` or
    ``AttributeError``; the manager catches it and logs a warning.
    """
    entry = _REGISTRY.get(domain)
    if isinstance(entry, str):
        module_name, _, class_name = entry.partition(":")
        entry = getattr(importlib.import_module(module_name), class_name)
        _REGISTRY[domain] = entry
    return entry
