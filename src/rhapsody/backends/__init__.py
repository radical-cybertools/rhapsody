"""Backend subsystem for Rhapsody.

This module provides execution backends for running scientific workflows on various computing
infrastructures.
"""

from __future__ import annotations

from .base import BaseExecutionBackend
from rhapsody.api.session import Session
from .constants import StateMapper
from .constants import TasksMainStates
from .discovery import BackendRegistry
from .discovery import discover_backends
from .discovery import get_backend

# Import all execution backends for convenient access
from .execution import *  # noqa: F401, F403

__all__ = [
    "BackendRegistry",
    "get_backend",
    "discover_backends",
    "BaseExecutionBackend",
    "Session",
    "TasksMainStates",
    "StateMapper",
    # Execution backends (imported from .execution)
    "ConcurrentExecutionBackend",
    "DaskExecutionBackend",  # Optional
    "RadicalExecutionBackend",  # Optional
    "DragonExecutionBackendV1",  # Optional
    "DragonExecutionBackendV2",  # Optional
    "DragonExecutionBackendV3",  # Optional
    "DragonTelemetryCollector",  # Optional
]
