"""Execution backends for Rhapsody.

This module contains concrete implementations of execution backends for different computing
environments.
"""

from __future__ import annotations
from .concurrent import ConcurrentExecutionBackend # noqa: F401

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
    from .dragon import DragonExecutionBackendV1  # noqa: F401
    from .dragon import DragonExecutionBackendV2  # noqa: F401
    from .dragon import DragonExecutionBackendV3  # noqa: F401

    __all__.extend(["DragonExecutionBackendV1",
                    "DragonExecutionBackendV2",
                    "DragonExecutionBackendV3"])
except ImportError:
    pass
