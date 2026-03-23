"""Flux execution backend for Rhapsody.

This module provides the FluxExecutionBackend which connects to a Flux
resource manager instance to execute tasks.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from rhapsody.backends.base import BaseBackend
from rhapsody.backends.constants import BackendMainStates
from rhapsody.backends.constants import StateMapper
from rhapsody.backends.constants import TasksMainStates
from .flux_helper import FluxHelper
from .flux_module import spec_from_dict
from .flux_service import FluxService

logger = logging.getLogger(__name__)


class FluxExecutionBackend(BaseBackend):
    """Execution backend that manages tasks via Flux."""

    def __init__(
        self,
        uri: str | None = None,
        launcher: str | None = None,
        name: str = "flux",
        resources: dict | None = None,
    ):
        """Initialize the Flux execution backend.

        Args:
            uri: Optional existing FLUX_URI. If None, a new Flux instance
                will be started using FluxService.
            launcher: Optional launcher prefix (e.g. MPI launcher) if
                starting a new Flux instance.
            name: Name of the backend.
            resources: Optional resources config.
        """
        super().__init__(name=name)

        self.logger = logger
        self._uri = uri
        self._launcher = launcher
        self._resources = resources or {}

        self._flux_service: FluxService | None = None
        self._flux_helper: FluxHelper | None = None

        self.tasks: dict[str, dict] = {}
        self._callback_func: callable = lambda t, s: None

        self._initialized = False
        self._backend_state = BackendMainStates.INITIALIZED

    def __await__(self):
        """Make backend awaitable."""
        return self._async_init().__await__()

    async def _async_init(self):
        """Initialize the Flux connection asynchronously."""
        if not self._initialized:
            try:
                self.logger.debug("Registering backend states...")
                StateMapper.register_backend_states_with_defaults(backend=self)

                self.logger.debug("Registering task states...")
                StateMapper.register_backend_tasks_states_with_defaults(backend=self)

                self._backend_state = BackendMainStates.INITIALIZED

                if not self._uri:
                    self.logger.info("No FLUX_URI provided, starting a new FluxService")
                    self._flux_service = FluxService(launcher=self._launcher)
                    # Use asyncio.to_thread to avoid blocking the event loop
                    success = await asyncio.to_thread(self._flux_service.start, 60.0)
                    if not success:
                        raise RuntimeError("FluxService failed to start within timeout")
                    self._uri = self._flux_service.uri

                self.logger.info("Connecting FluxHelper to %s", self._uri)
                self._flux_helper = FluxHelper(uri=self._uri, log=self.logger)
                self._flux_helper.register_cb(self._flux_event_cb)
                await asyncio.to_thread(self._flux_helper.start)

                self._initialized = True
                self.logger.info("Flux execution backend started successfully")

            except Exception as e:
                self.logger.exception("Flux backend initialization failed: %s", e)
                self._initialized = False
                raise
        return self

    def get_task_states_map(self):
        """Return the state mapper for this backend."""
        return StateMapper(backend=self)

    def _flux_event_cb(self, task_uid: str, event: Any) -> None:
        """Handle events coming from the Flux journal.

        Maps Flux journal events to Rhapsody task states and invokes callbacks.
        """
        if task_uid not in self.tasks:
            return
        task = self.tasks[task_uid]
        event_name = event.name

        state = None
        if event_name in ("submit", "alloc", "start"):
            state = TasksMainStates.RUNNING.value
        elif event_name == "finish":
            # Context schema for finish usually has 'status' (the exit code)
            status = getattr(event, "context", {}).get("status", 0)
            if status == 0:
                state = TasksMainStates.DONE.value
                task["exit_code"] = 0
            else:
                state = TasksMainStates.FAILED.value
                task["exit_code"] = status
        elif event_name == "exception":
            state = TasksMainStates.FAILED.value
            task["exit_code"] = 1

        if state:
            try:
                self._callback_func(task, state)
            except Exception as e:
                self.logger.exception("Error in task callback for %s: %s", task_uid, e)

    async def submit_tasks(self, tasks: list[dict[str, Any]]) -> None:
        """Submit tasks to Flux."""
        if self._backend_state != BackendMainStates.RUNNING:
            self._backend_state = BackendMainStates.RUNNING

        specs = []
        for task in tasks:
            self.tasks[task["uid"]] = task
            # Map canonical task dictionary values to the kwargs expected by spec_from_dict
            td = {
                "uid": task["uid"],
                "executable": task["executable"],
                "arguments": task.get("arguments", []),
                "ranks": task.get("ranks", 1),
                "timeout": task.get("timeout", 0.0),
                "environment": task.get("environment"),
                "sandbox": task.get("sandbox"),
            }
            # Handle resource requests. Rhapsody uses dicts like:
            # cpu_reqs: {"cpu_processes": X, "cpu_threads": Y}
            cpu_reqs = task.get("cpu_reqs", {})
            if cpu_reqs:
                td["cores_per_rank"] = cpu_reqs.get("cpu_threads", 1)

            gpu_reqs = task.get("gpu_reqs", {})
            if gpu_reqs:
                td["gpus_per_rank"] = gpu_reqs.get("gpus", 0)

            # Execution modifiers
            if "stdout" in task:
                td["stdout"] = task["stdout"]
            if "stderr" in task:
                td["stderr"] = task["stderr"]

            spec = spec_from_dict(td)
            specs.append(spec)

        if specs:
            await asyncio.to_thread(self._flux_helper.submit, specs)

    async def cancel_task(self, uid: str) -> bool:
        """Cancel a task by its UID."""
        if uid in self.tasks and self._flux_helper:
            try:
                await asyncio.to_thread(self._flux_helper.cancel, uid)
                task = self.tasks[uid]
                self._callback_func(task, TasksMainStates.CANCELED.value)
                return True
            except Exception as e:
                self.logger.error("Failed to cancel flux task %s: %s", uid, e)
        return False

    async def shutdown(self) -> None:
        """Shutdown the Flux backend and service."""
        self._backend_state = BackendMainStates.SHUTDOWN

        if self._flux_helper:
            await asyncio.to_thread(self._flux_helper.stop)

        if self._flux_service:
            await asyncio.to_thread(self._flux_service.stop)

        self.logger.info("Flux execution backend shutdown complete")

    def build_task(self, task: dict) -> None:
        """Required by BaseBackend, no-op for Flux."""
        pass

    def state(self) -> str:
        """Get backend state."""
        return self._backend_state.value

    def task_state_cb(self, task: dict, state: str) -> None:
        """Required by BaseBackend, handled internally via _flux_event_cb."""
        pass

    async def __aenter__(self):
        """Async context manager entry."""
        if not self._initialized:
            await self._async_init()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.shutdown()
