"""Backend subsystem for Rhapsody.

This module provides execution backends for running scientific workflows on various computing
infrastructures.
"""

from __future__ import annotations

import importlib
from typing import Any

from rhapsody.api import Session

from .base import BaseBackend
from .constants import StateMapper
from .constants import TasksMainStates
from .discovery import BackendRegistry
from .discovery import discover_backends
from .discovery import get_backend

# Import all execution backends for convenient access
from .execution import ConcurrentExecutionBackend

__all__ = [
    "BackendRegistry",
    "get_backend",
    "discover_backends",
    "BaseBackend",
    "Session",
    "TasksMainStates",
    "StateMapper",
    "ConcurrentExecutionBackend",
]

# Try to import optional execution backends
try:
    from .execution import DaskExecutionBackend  # noqa: F401

    __all__.append("DaskExecutionBackend")
except ImportError:
    pass

try:
    from .execution import RadicalExecutionBackend  # noqa: F401

    __all__.append("RadicalExecutionBackend")
except ImportError:
    pass

try:
    from .execution import DragonExecutionBackend  # noqa: F401

    __all__.append("DragonExecutionBackend")
except ImportError:
    pass

try:
    from .execution import OrbitExecutionBackend  # noqa: F401

    __all__.append("OrbitExecutionBackend")
except ImportError:
    pass

# Optional AI (inference) backends load on first use, not with this package. Importing one
# imports dragon.ai.inference, whose llm_engine sets DRAGON_PATCH_MP to "" at import; the
# processes a program starts afterwards through Dragon's multiprocessing bridge then lack
# Dragon's start method. A plain `import rhapsody` must not do that.
_LAZY_BACKENDS = {"DragonVllmInferenceBackend": ".ai.vllm"}


def __getattr__(name: str) -> Any:
    """Import an optional AI backend the first time it is asked for."""
    if name in _LAZY_BACKENDS:
        module = importlib.import_module(_LAZY_BACKENDS[name], __name__)
        value = getattr(module, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
