"""Dask distributed execution backend for parallel and distributed computing.

This module provides a backend that executes tasks on Dask clusters, supporting both local and
distributed execution environments.
"""

from __future__ import annotations

import asyncio
import logging
from functools import wraps
from typing import Any
from typing import Callable

import typeguard

from ..base import BaseExecutionBackend
from ..constants import StateMapper, BackendMainStates

try:
    import dask.distributed as dask
except ImportError:
    dask = None


def _get_logger() -> logging.Logger:
    """Get logger for dask backend module.

    This function provides lazy logger evaluation, ensuring the logger
    is created after the user has configured logging, not at module import time.
    """
    return logging.getLogger(__name__)


class DaskExecutionBackend(BaseExecutionBackend):
    """An async-only Dask execution backend for distributed task execution.

    Handles task submission, cancellation, and proper async event loop handling
    for distributed task execution using Dask. All functions must be async.

    Usage:
        backend = await DaskExecutionBackend(resources)
        # or
        async with DaskExecutionBackend(resources) as backend:
            await backend.submit_tasks(tasks)
    """

    @typeguard.typechecked
    def __init__(self, resources: dict | None = None):
        """Initialize the Dask execution backend (non-async setup only).

        Args:
            resources: Dictionary of resource requirements for tasks. Contains
                configuration parameters for the Dask client initialization.
        """

        if dask is None:
            raise ImportError("Dask is required for DaskExecutionBackend.")

        super().__init__()

        self.logger = _get_logger()
        self.tasks = {}
        self._client = None
        self._callback_func: Callable = lambda t, s: None
        self._resources = resources or {}
        self._initialized = False
        self._backend_state = BackendMainStates.INITIALIZED

    def __await__(self):
        """Make DaskExecutionBackend awaitable like Dask Client."""
        return self._async_init().__await__()

    async def _async_init(self):
        """Unified async initialization with backend and task state registration.

        Pattern:
        1. Register backend states first
        2. Register task states
        3. Set backend state to INITIALIZED
        4. Initialize backend components
        """
        if not self._initialized:
            try:
                # Step 1: Register backend states
                self.logger.debug("Registering backend states...")
                StateMapper.register_backend_states_with_defaults(backend=self)

                # Step 2: Register task states
                self.logger.debug("Registering task states...")
                StateMapper.register_backend_tasks_states_with_defaults(backend=self)

                # Step 3: Set backend state to INITIALIZED
                self._backend_state = BackendMainStates.INITIALIZED
                self.logger.debug(f"Backend state set to: {self._backend_state.value}")

                # Step 4: Initialize backend components
                await self._initialize()
                self._initialized = True

                self.logger.info("Dask backend fully initialized and ready")

            except Exception as e:
                self.logger.exception(f"Dask backend initialization failed: {e}")
                self._initialized = False
                raise
        return self

    async def _initialize(self) -> None:
        """Initialize the Dask client and set up worker environments.

        Raises:
            Exception: If Dask client initialization fails.
        """
        try:
            self._client = await dask.Client(asynchronous=True, **self._resources)
            dashboard_link = self._client.dashboard_link
            self.logger.info(f"Dask backend initialized with dashboard at {dashboard_link}")
        except Exception as e:
            self.logger.exception(f"Failed to initialize Dask client: {str(e)}")
            raise

    def register_callback(self, func: Callable) -> None:
        """Register a callback for task state changes.

        Args:
            func: Function to be called when task states change. Should accept
                task and state parameters.
        """
        self._callback_func = func

    def get_task_states_map(self) -> StateMapper:
        """Retrieve a mapping of task IDs to their current states.

        Returns:
            StateMapper: Object containing the mapping of task states for this backend.
        """
        return StateMapper(backend=self)

    async def cancel_task(self, uid: str) -> bool:
        """Cancel a task by its UID.

        Args:
            uid (str): The UID of the task to cancel.

        Returns:
            bool: True if the task was found and cancellation was attempted,
            False otherwise.
        """
        self._ensure_initialized()
        if uid in self.tasks:
            task = self.tasks[uid]
            future = task.get("future")
            if future:
                return await future.cancel()
        return False

    async def submit_tasks(self, tasks: list[dict[str, Any]]) -> None:
        """Submit async tasks to Dask cluster.

        Processes a list of tasks and submits them to the Dask cluster for execution.
        Filters out future objects from arguments and validates that all functions
        are async coroutine functions.

        Args:
            tasks: List of task dictionaries containing:
                - uid: Unique task identifier
                - function: Async callable to execute
                - args: Positional arguments
                - kwargs: Keyword arguments
                - executable: Optional executable path (not supported)
                - task_backend_specific_kwargs: Backend-specific parameters

        Note:
            Executable tasks are not supported and will result in FAILED state.
            Only async functions are supported - sync functions will result in
            FAILED state.
            Future objects are filtered out from arguments as they are not picklable.
        """
        self._ensure_initialized()

        # Set backend state to RUNNING when tasks are submitted
        if self._backend_state != BackendMainStates.RUNNING:
            self._backend_state = BackendMainStates.RUNNING
            self.logger.debug(f"Backend state set to: {self._backend_state.value}")

        for task in tasks:
            is_func_task = bool(task.get("function"))
            is_exec_task = bool(task.get("executable"))

            if is_exec_task:
                error_msg = "DaskExecutionBackend does not support executable tasks"
                task["stderr"] = ValueError(error_msg)
                self._callback_func(task, "FAILED")
                continue

            # Validate that function is async
            if is_func_task and not asyncio.iscoroutinefunction(task["function"]):
                error_msg = "DaskExecutionBackend only supports async functions"
                task["exception"] = ValueError(error_msg)
                self._callback_func(task, "FAILED")
                continue

            self.tasks[task["uid"]] = task

            # Filter out future objects as they are not picklable
            filtered_args = [arg for arg in task["args"] if not isinstance(arg, asyncio.Future)]
            task["args"] = tuple(filtered_args)
            try:
                await self._submit_async_function(task)
            except Exception as e:
                task["exception"] = e
                self._callback_func(task, "FAILED")

    async def _submit_to_dask(self, task: dict[str, Any], fn: Callable, *args) -> None:
        """Submit function to Dask and register completion callback.

        Submits the wrapped function to Dask client and registers a callback
        to handle task completion or failure.

        Args:
            task: Task dictionary containing task metadata and configuration.
            fn: The async function to submit to Dask.
            *args: Arguments to pass to the function.
        """

        async def on_done(f):
            task_uid = task["uid"]
            try:
                result = await f
                task["return_value"] = result
                self._callback_func(task, "DONE")
            except dask.client.FutureCancelledError:
                self._callback_func(task, "CANCELED")
            except Exception as e:
                task["exception"] = e
                self._callback_func(task, "FAILED")
            finally:
                # Clean up the future reference once task is complete
                if task_uid in self.tasks:
                    del self.tasks[task_uid]

        dask_future = self._client.submit(fn, *args, **task["task_backend_specific_kwargs"])

        # Store the future for potential cancellation
        self.tasks[task["uid"]]["future"] = dask_future

        # Schedule the callback to run when future completes
        asyncio.create_task(on_done(dask_future))

    async def _submit_async_function(self, task: dict[str, Any]) -> None:
        """Submit async function to Dask.

        Creates an async wrapper that preserves the original function name
        for better visibility in the Dask dashboard.

        Args:
            task: Task dictionary containing the async function and its parameters.
        """

        # Preserve the real task name in dask dashboard
        @wraps(task["function"])
        async def async_wrapper():
            return await task["function"](*task["args"], **task["kwargs"])

        await self._submit_to_dask(task, async_wrapper)

    async def cancel_all_tasks(self) -> int:
        """Cancel all currently running/pending tasks.

        Returns:
            Number of tasks that were successfully cancelled
        """
        self._ensure_initialized()
        cancelled_count = 0
        task_uids = list(self.tasks.keys())

        for task_uid in task_uids:
            if await self.cancel_task(task_uid):
                cancelled_count += 1

        return cancelled_count

    def link_explicit_data_deps(
        self,
        src_task: dict[str, Any] | None = None,
        dst_task: dict[str, Any] | None = None,
        file_name: str | None = None,
        file_path: str | None = None,
    ) -> None:
        """Handle explicit data dependencies between tasks.

        Args:
            src_task: The source task that produces the dependency.
            dst_task: The destination task that depends on the source.
            file_name: Name of the file that represents the dependency.
            file_path: Full path to the file that represents the dependency.
        """
        pass

    def link_implicit_data_deps(self, src_task: dict[str, Any], dst_task: dict[str, Any]) -> None:
        """Handle implicit data dependencies for a task.

        Args:
            src_task: The source task that produces data.
            dst_task: The destination task that depends on the source task's output.
        """
        pass

    async def state(self) -> str:
        """Get backend state.

        Returns:
            str: Current backend state (INITIALIZED, RUNNING, SHUTDOWN)
        """
        return self._backend_state.value

    async def task_state_cb(self, task: dict, state: str) -> None:
        """Callback function invoked when a task's state changes.

        Args:
            task: Dictionary containing task information and metadata.
            state: The new state of the task.
        """
        pass

    async def build_task(self, task: dict) -> None:
        """Build or prepare a task for execution.

        Args:
            task: Dictionary containing task definition, parameters, and metadata
                required for task construction.
        """
        pass

    async def shutdown(self) -> None:
        """Shutdown the Dask client and clean up resources.

        Closes the Dask client connection, clears task storage, and handles any cleanup exceptions
        gracefully.
        """
        # Set backend state to SHUTDOWN
        self._backend_state = BackendMainStates.SHUTDOWN
        self.logger.debug(f"Backend state set to: {self._backend_state.value}")

        if self._client is not None:
            try:
                # Cancel all running tasks first
                await self.cancel_all_tasks()

                # Close the client
                await self._client.close()
                self.logger.info("Dask client shutdown complete")
            except Exception as e:
                self.logger.exception(f"Error during shutdown: {str(e)}")
            finally:
                self._client = None
                self.logger.info("Dask execution backend shutdown complete")

        # Always clean up state regardless of client presence
        self.tasks.clear()
        self._initialized = False

    def _ensure_initialized(self):
        """Ensure the backend has been properly initialized."""
        if not self._initialized:
            raise RuntimeError(
                "DaskExecutionBackend must be awaited before use. "
                "Use: backend = await DaskExecutionBackend(resources)"
            )

    async def __aenter__(self):
        """Async context manager entry."""
        if not self._initialized:
            await self._async_init()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.shutdown()

    # Class method for cleaner instantiation (optional alternative pattern)
    @classmethod
    async def create(cls, resources: dict | None = None) -> DaskExecutionBackend:
        """Alternative factory method for creating initialized backend.

        Args:
            resources: Configuration parameters for Dask client initialization.

        Returns:
            Fully initialized DaskExecutionBackend instance.
        """
        backend = cls(resources)
        return await backend
