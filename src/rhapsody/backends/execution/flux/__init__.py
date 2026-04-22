"""Flux execution helpers for Rhapsody.

This sub-package provides the FluxHelper, FluxService, and FluxModule classes
together with jobspec factory functions, encapsulating all interaction with the
Flux resource-manager Python bindings.
"""

from .flux_backend import FluxExecutionBackend
from .flux_helper import FluxHelper
from .flux_module import FluxModule
from .flux_module import spec_from_command
from .flux_module import spec_from_dict
from .flux_service import FluxService

__all__ = [
    "FluxExecutionBackend",
    "FluxHelper",
    "FluxModule",
    "FluxService",
    "spec_from_command",
    "spec_from_dict",
]
