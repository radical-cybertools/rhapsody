"""
Rhapsody: Runtime system for executing heterogeneous HPC-AI workflows.

Rhapsody provides execution backends for scientific workflows, enabling execution
on various computing infrastructures including HPC clusters, local machines,
and distributed computing systems.
"""

from __future__ import annotations

from .backends import get_backend, discover_backends, BackendRegistry

__version__ = "0.1.0"

__all__ = [
    "__version__",
    "get_backend",
    "discover_backends",
    "BackendRegistry",
]