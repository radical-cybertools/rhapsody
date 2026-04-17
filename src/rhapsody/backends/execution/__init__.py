"""Execution backends for Rhapsody.

This module contains concrete implementations of execution backends for different computing
environments.
"""

from __future__ import annotations

from .concurrent import ConcurrentExecutionBackend  # noqa: F401

__all__ = ["ConcurrentExecutionBackend"]

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

try:
    from .dragon import DragonExecutionBackendV3  # noqa: F401
    from .dragon import DragonExecutionBackendV3Client  # noqa: F401

    __all__.append("DragonExecutionBackendV3")
    __all__.append("DragonExecutionBackendV3Client")
except ImportError:
    pass
