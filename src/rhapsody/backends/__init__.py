"""
Backend subsystem for Rhapsody.

This module provides execution backends for running scientific workflows
on various computing infrastructures.
"""

from __future__ import annotations

from .discovery import BackendRegistry, get_backend, discover_backends
from .base import BaseExecutionBackend, Session
from .constants import TasksMainStates, StateMapper

__all__ = [
    "BackendRegistry",
    "get_backend",
    "discover_backends",
    "BaseExecutionBackend",
    "Session",
    "TasksMainStates",
    "StateMapper",
]