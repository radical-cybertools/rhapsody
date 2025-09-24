"""
Execution backends for Rhapsody.

This module contains concrete implementations of execution backends
for different computing environments.
"""

from __future__ import annotations

# Import backends that are always available (no optional deps)
from .noop import NoopExecutionBackend
from .concurrent import ConcurrentExecutionBackend

__all__ = [
    "NoopExecutionBackend",
    "ConcurrentExecutionBackend",
]

# Try to import optional backends
try:
    from .dask_parallel import DaskExecutionBackend
    __all__.append("DaskExecutionBackend")
except ImportError:
    pass

try:
    from .radical_pilot import RadicalExecutionBackend
    __all__.append("RadicalExecutionBackend")
except ImportError:
    pass