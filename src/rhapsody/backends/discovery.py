"""Backend discovery and factory functions for Rhapsody.

This module provides utilities for discovering available backends and creating backend instances
dynamically.
"""

from __future__ import annotations

import importlib
from typing import Any

from .base import BaseExecutionBackend


class BackendRegistry:
    """Registry for managing available execution backends.

    Backends are discovered dynamically by inspecting the rhapsody.backends.execution module.
    """

    _backends: dict[str, type[BaseExecutionBackend]] = {}
    _initialized: bool = False

    @classmethod
    def _discover_backends(cls) -> None:
        """Discover available backends from the execution module."""
        if cls._initialized:
            return

        # Import the execution module to access its __all__ exports
        try:
            execution_module = importlib.import_module("rhapsody.backends.execution")
        except ImportError:
            cls._initialized = True
            return

        # Mapping of backend names to their class names
        backend_mapping = {
            "concurrent": "ConcurrentExecutionBackend",
            "dask": "DaskExecutionBackend",
            "radical_pilot": "RadicalExecutionBackend",
            "dragon_v1": "DragonExecutionBackendV1",
            "dragon_v2": "DragonExecutionBackendV2",
            "dragon_v3": "DragonExecutionBackendV3",
        }

        # Try to import each backend
        for backend_name, class_name in backend_mapping.items():
            try:
                backend_class = getattr(execution_module, class_name)
                cls._backends[backend_name] = backend_class
            except AttributeError:
                # Backend not available (import failed in execution/__init__.py)
                pass

        cls._initialized = True

    @classmethod
    def get_backend_class(cls, backend_name: str) -> type[BaseExecutionBackend]:
        """Get backend class by name.

        Args:
            backend_name: Name of the backend to retrieve

        Returns:
            Backend class type

        Raises:
            ValueError: If backend is not registered
        """
        cls._discover_backends()

        if backend_name not in cls._backends:
            available = list(cls._backends.keys())
            raise ValueError(f"Backend '{backend_name}' not found. Available: {available}")

        return cls._backends[backend_name]

    @classmethod
    def list_backends(cls) -> list[str]:
        """List all registered backend names."""
        cls._discover_backends()
        return list(cls._backends.keys())

    @classmethod
    def register_backend(cls, name: str, backend_class: type[BaseExecutionBackend]) -> None:
        """Register a new backend.

        Args:
            name: Name of the backend
            backend_class: Backend class (not import path)
        """
        cls._backends[name] = backend_class


def get_backend(backend_name: str, *args: Any, **kwargs: Any) -> BaseExecutionBackend:
    """Factory function to create backend instances.

    Args:
        backend_name: Name of the backend to create
        *args: Positional arguments for backend constructor
        **kwargs: Keyword arguments for backend constructor

    Returns:
        Backend instance (may need to be awaited for async backends)

    Example:
        # Dask backend
        backend = get_backend('dask', resources={'threads': 4})

        # RADICAL-Pilot backend
        backend = get_backend('radical_pilot')
    """
    backend_class = BackendRegistry.get_backend_class(backend_name)
    return backend_class(*args, **kwargs)


def discover_backends() -> dict[str, bool]:
    """Discover which backends are available based on optional dependencies.

    Returns:
        Dictionary mapping backend names to availability status
    """
    # Simply return which backends were successfully imported
    # The BackendRegistry._discover_backends() already handles import failures
    available_backends = BackendRegistry.list_backends()
    return {name: True for name in available_backends}
