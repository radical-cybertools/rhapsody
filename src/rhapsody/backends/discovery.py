"""Backend discovery and factory functions for Rhapsody.

This module provides utilities for discovering available backends and creating backend instances
dynamically.
"""

from __future__ import annotations

import importlib

from .base import BaseExecutionBackend


class BackendRegistry:
    """Registry for managing available execution backends."""

    _backends: dict[str, str] = {
        "noop": "rhapsody.backends.execution.noop.NoopExecutionBackend",
        "concurrent": "rhapsody.backends.execution.concurrent.ConcurrentExecutionBackend",
        "dask": "rhapsody.backends.execution.dask_parallel.DaskExecutionBackend",
        "radical_pilot": "rhapsody.backends.execution.radical_pilot.RadicalExecutionBackend",
    }

    @classmethod
    def get_backend_class(cls, backend_name: str) -> type[BaseExecutionBackend]:
        """Get backend class by name.

        Args:
            backend_name: Name of the backend to retrieve

        Returns:
            Backend class type

        Raises:
            ValueError: If backend is not registered
            ImportError: If backend module cannot be imported
        """
        if backend_name not in cls._backends:
            available = list(cls._backends.keys())
            raise ValueError(f"Backend '{backend_name}' not found. Available: {available}")

        module_path = cls._backends[backend_name]
        module_name, class_name = module_path.rsplit(".", 1)

        try:
            module = importlib.import_module(module_name)
            backend_class = getattr(module, class_name)
            return backend_class
        except ImportError as e:
            raise ImportError(f"Failed to import backend '{backend_name}': {e}") from e

    @classmethod
    def list_backends(cls) -> list[str]:
        """List all registered backend names."""
        return list(cls._backends.keys())

    @classmethod
    def register_backend(cls, name: str, import_path: str) -> None:
        """Register a new backend.

        Args:
            name: Name of the backend
            import_path: Full import path to the backend class
        """
        cls._backends[name] = import_path


def get_backend(backend_name: str, *args, **kwargs) -> BaseExecutionBackend:
    """Factory function to create backend instances.

    Args:
        backend_name: Name of the backend to create
        *args: Positional arguments for backend constructor
        **kwargs: Keyword arguments for backend constructor

    Returns:
        Backend instance (may need to be awaited for async backends)

    Example:
        # Synchronous backend
        backend = get_backend('noop')

        # Async backend (needs await)
        backend = await get_backend('dask', resources={'threads': 4})
    """
    backend_class = BackendRegistry.get_backend_class(backend_name)
    return backend_class(*args, **kwargs)


def discover_backends() -> dict[str, bool]:
    """Discover which backends are available based on optional dependencies.

    Returns:
        Dictionary mapping backend names to availability status
    """
    availability = {}

    for backend_name in BackendRegistry.list_backends():
        try:
            # Try to import the backend class to check if dependencies are available
            BackendRegistry.get_backend_class(backend_name)
            availability[backend_name] = True
        except ImportError:
            availability[backend_name] = False

    return availability
