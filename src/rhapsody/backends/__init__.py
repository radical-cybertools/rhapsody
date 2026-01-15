"""Backend subsystem for Rhapsody.

This module provides execution backends for running scientific workflows on various computing
infrastructures.
"""

from __future__ import annotations

from rhapsody.api.session import Session

from .base import BaseExecutionBackend
from .constants import StateMapper
from .constants import TasksMainStates
from .discovery import BackendRegistry
from .discovery import discover_backends
from .discovery import get_backend

# Import all execution backends for convenient access
from .execution import ConcurrentExecutionBackend
from .execution import DaskExecutionBackend
from .execution import DragonExecutionBackendV1
from .execution import DragonExecutionBackendV2
from .execution import DragonExecutionBackendV3
from .execution import DragonTelemetryCollector
from .execution import RadicalExecutionBackend

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
