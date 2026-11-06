"""Backend discovery and factory functions for Rhapsody.

This module provides utilities for discovering available backends and creating backend instances
dynamically.
"""

from __future__ import annotations

import importlib
import logging
import re
from typing import Any

from .base import BaseBackend


class BackendRegistry:
    """Registry for managing available execution backends.

    Backends are discovered dynamically by inspecting the rhapsody.backends.execution module.
    """

    _backends: dict[str, type[BaseBackend]] = {}
    _initialized: bool = False

    @classmethod
    def _discover_backends(cls) -> None:
        """Discover available backends from the execution module.

        Dynamically discovers backends by inspecting the __all__ exports from
        rhapsody.backends.execution module. Automatically derives backend names from class names by
        converting CamelCase to snake_case.
        """
        if cls._initialized:
            return

        # Import the execution module to access its __all__ exports
        try:
            execution_module = importlib.import_module("rhapsody.backends.execution")
        except ImportError:
            cls._initialized = True
            return

        # Dynamically discover backends from __all__
        available_classes = getattr(execution_module, "__all__", [])

        for class_name in available_classes:
            # Skip non-backend classes (like telemetry collectors)
            # Match backends by checking if they contain "Backend" or match versioned pattern
            is_backend = (
                "Backend" in class_name
                and "Collector" not in class_name
                and "Telemetry" not in class_name
            )
            if not is_backend:
                continue

            try:
                backend_class = getattr(execution_module, class_name)

                # Derive backend name from class name
                # ConcurrentExecutionBackend -> concurrent
                # DaskExecutionBackend -> dask
                # RadicalExecutionBackend -> radical_pilot
                # DragonExecutionBackend -> dragon
                backend_name = cls._derive_backend_name(class_name)

                cls._backends[backend_name] = backend_class
            except AttributeError:
                # Backend class not available despite being in __all__
                pass

        # Deprecated name alias — dragon_v3 resolves to dragon for backward compatibility
        if "dragon" in cls._backends:
            cls._backends.setdefault("dragon_v3", cls._backends["dragon"])

        cls._initialized = True

    @classmethod
    def _derive_backend_name(cls, class_name: str) -> str:
        """Derive backend name from class name.

        Converts CamelCase class names to snake_case backend names.
        Special handling for known patterns:
        - ConcurrentExecutionBackend -> concurrent
        - DaskExecutionBackend -> dask
        - RadicalExecutionBackend -> radical_pilot
        - DragonExecutionBackend -> dragon

        Args:
            class_name: The class name (e.g., "DaskExecutionBackend")

        Returns:
            Snake case backend name (e.g., "dask")
        """
        # Remove common suffixes
        name = class_name.replace("ExecutionBackend", "").replace("Backend", "")

        # Handle version suffixes (V1, V2, V3)
        match = re.match(r"(.+?)(V\d+)$", name)
        if match:
            base_name, version = match.groups()
            name = base_name + "_" + version.lower()

        # Convert CamelCase to snake_case
        # Insert underscore before uppercase letters (except first)
        name = re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()

        # Special case: "radical" should become "radical_pilot"
        if name == "radical":
            name = "radical_pilot"

        return name

    @classmethod
    def get_backend_class(cls, backend_name: str) -> type[BaseBackend]:
        """Get backend class by name.

        Args:
            backend_name: Name of the backend to retrieve

        Returns:
            Backend class type

        Raises:
            ValueError: If backend is not registered
            ImportError: If backend string path cannot be imported
        """
        cls._discover_backends()

        if backend_name not in cls._backends:
            available = list(cls._backends.keys())
            raise ValueError(f"Backend '{backend_name}' not found. Available: {available}")

        backend_value = cls._backends[backend_name]

        # If it's a string (import path), try to import it
        if isinstance(backend_value, str):
            try:
                module_path, class_name = backend_value.rsplit(".", 1)
                module = importlib.import_module(module_path)
                backend_class = getattr(module, class_name)
                # Cache the imported class for future use
                cls._backends[backend_name] = backend_class
                return backend_class
            except (ImportError, AttributeError, ValueError) as e:
                raise ImportError(
                    f"Failed to import backend '{backend_name}' from '{backend_value}': {e}"
                ) from e

        return backend_value

    @classmethod
    def list_backends(cls) -> list[str]:
        """List all registered backend names."""
        cls._discover_backends()
        return list(cls._backends.keys())

    @classmethod
    def register_backend(cls, name: str, backend_class: type[BaseBackend] | str) -> None:
        """Register a new backend.

        Args:
            name: Name of the backend
            backend_class: Backend class or import path string (e.g., 'module.path.ClassName')
        """
        cls._backends[name] = backend_class


def get_backend(backend_name: str, *args: Any, **kwargs: Any) -> BaseBackend:
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
    BackendRegistry._discover_backends()
    available_backends = {}

    for name in BackendRegistry.list_backends():
        try:
            # Try to get the backend class - this will import if it's a string path
            BackendRegistry.get_backend_class(name)
            available_backends[name] = True
        except (ImportError, ValueError, AttributeError):
            # Backend registered but can't be imported
            available_backends[name] = False

    return available_backends


class DataBackendRegistry:
    """Registry for data-infrastructure backends (`DataBackend` subclasses).

    Built-in data backends are discovered from the `__all__` exports of
    `rhapsody.backends.data` (`RedisDataBackend` -> `redis`, `DragonDataBackend` -> `dragon`).
    External packages add their own via the `rhapsody.data_backends` entry-point group; a
    built-in name wins over an entry point of the same name.
    """

    ENTRY_POINT_GROUP = "rhapsody.data_backends"

    _backends: dict[str, Any] = {}
    _initialized: bool = False

    @classmethod
    def _discover_backends(cls) -> None:
        if cls._initialized:
            return

        try:
            data_module = importlib.import_module("rhapsody.backends.data")
        except ImportError:
            data_module = None

        for class_name in getattr(data_module, "__all__", []):
            if not class_name.endswith("DataBackend") or class_name == "DataBackend":
                continue
            backend_class = getattr(data_module, class_name, None)
            # `_registrable = False` marks wrappers (OrbitDataBackend) that are
            # not a store type a session can build by name.
            if backend_class is None or not getattr(backend_class, "_registrable", True):
                continue
            name = BackendRegistry._derive_backend_name(class_name[: -len("DataBackend")])
            cls._backends.setdefault(name, backend_class)

        try:
            from importlib.metadata import entry_points

            for ep in entry_points(group=cls.ENTRY_POINT_GROUP):
                cls._backends.setdefault(ep.name, ep)
        except Exception:  # noqa: BLE001 - broken metadata must not break discovery
            logging.getLogger(__name__).warning(
                "Could not read '%s' entry points", cls.ENTRY_POINT_GROUP, exc_info=True
            )

        cls._initialized = True

    @classmethod
    def get_backend_class(cls, backend_name: str) -> type:
        """Get data backend class by name.

        Raises:
            ValueError: If the name is not registered.
            ImportError: If a registered import path cannot be imported.
        """
        cls._discover_backends()

        if backend_name not in cls._backends:
            available = list(cls._backends.keys())
            raise ValueError(f"Data backend '{backend_name}' not found. Available: {available}")

        backend_value = cls._backends[backend_name]
        if not isinstance(backend_value, type):
            try:
                if isinstance(backend_value, str):
                    module_path, class_name = backend_value.rsplit(".", 1)
                    backend_value = getattr(importlib.import_module(module_path), class_name)
                else:  # importlib.metadata.EntryPoint
                    backend_value = backend_value.load()
            except (ImportError, AttributeError, ValueError) as e:
                raise ImportError(
                    f"Failed to import data backend '{backend_name}' "
                    f"from '{cls._backends[backend_name]}': {e}"
                ) from e
            cls._backends[backend_name] = backend_value

        return backend_value

    @classmethod
    def list_backends(cls) -> list[str]:
        """List all registered data backend names."""
        cls._discover_backends()
        return list(cls._backends.keys())

    @classmethod
    def register_backend(cls, name: str, backend_class: type | str) -> None:
        """Register a data backend class or import path under `name`."""
        cls._backends[name] = backend_class


def get_data_backend(backend_name: str, *args: Any, **kwargs: Any) -> Any:
    """Factory for data backend instances (not started).

    Example:
        backend = get_data_backend('redis', hosts=['localhost'])
        await backend.start()
    """
    backend_class = DataBackendRegistry.get_backend_class(backend_name)
    return backend_class(*args, **kwargs)
