"""Execution backends for Rhapsody.

This module contains concrete implementations of execution backends for different computing
environments.
"""

from __future__ import annotations

from .concurrent import ConcurrentExecutionBackend

# Import backends that are always available (no optional deps)
from .noop import NoopExecutionBackend

__all__ = [
    "NoopExecutionBackend",
    "ConcurrentExecutionBackend",
]

# Try to import optional backends
try:
    from .dask_parallel import DaskExecutionBackend  # noqa: F401

    __all__.append("DaskExecutionBackend")
except ImportError:
    pass

try:
    from .radical_pilot import RadicalExecutionBackend  # noqa: F401

    __all__.append("RadicalExecutionBackend")
except ImportError:
    pass
