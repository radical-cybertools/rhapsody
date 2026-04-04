from __future__ import annotations

import asyncio
import logging
import os
from typing import TYPE_CHECKING

from rhapsody.api.errors import TaskExecutionError

if TYPE_CHECKING:
    from rhapsody.api.task import BaseTask
    from rhapsody.backends.base import BaseBackend


logger = logging.getLogger(__name__)


class TaskStateManager:
    """Centralized manager for task state updates and monitoring.

    This class subscribes to backend callbacks and provides a synchronization mechanism for waiting
    on task completion.
    """

    def __init__(self):
        self._task_futures: dict[str, asyncio.Future] = {}
        self._terminal_states = set()  # Will be populated by backends
        self._lock = asyncio.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Bind an event loop to the manager for thread-safe updates."""
        self._loop = loop

    def update_task(self, task: dict | BaseTask, state: str, **kwargs: object) -> None:
        """Update task state and notify waiters (thread-safe)."""
        if self._loop and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self._update_task_impl, task, state)
        else:
            # Fallback if no loop bound (e.g. synch tests) or loop closed
            # Warning: This is not thread-safe if called from another thread
            logger.error(
                "update_task called without a valid event loop "
                f"(loop={self._loop}, closed={getattr(self._loop, 'is_closed', lambda: None)()})"
            )
            self._update_task_impl(task, state)

    def _update_task_impl(self, task: dict | BaseTask, state: str) -> None:
        """Actual update logic, expected to run on the event loop."""
        uid = task["uid"]

        # Update the task object in-place (Single Source of Truth)
        task["state"] = state

        # If terminal, notify waiters
        if state in self._terminal_states:
            if uid in self._task_futures:
                fut = self._task_futures.pop(uid)
                if not fut.done():
                    exc = task.get("exception")
                    exit_code = task.get("exit_code")
                    if isinstance(exc, BaseException):
                        fut.set_exception(exc)
                    elif exit_code is not None and exit_code != 0:
                        fut.set_exception(
                            TaskExecutionError(uid, task.get("stderr") or "", exit_code)
                        )
                    else:
                        fut.set_result(task)

    def get_wait_future(self, uid: str, task: dict | BaseTask) -> asyncio.Future:
        """Get or create a future to wait for a specific task."""
        if self._loop is None:
            try:
                self.bind_loop(asyncio.get_running_loop())
            except RuntimeError:
                pass

        if uid not in self._task_futures:
            # Create a future on the correct loop
            if self._loop:
                self._task_futures[uid] = self._loop.create_future()
            else:
                self._task_futures[uid] = asyncio.Future()

            # If already done before we started waiting, resolve immediately
            if task.get("state") in self._terminal_states:
                exc = task.get("exception")
                exit_code = task.get("exit_code")
                if isinstance(exc, BaseException):
                    self._task_futures[uid].set_exception(exc)
                elif exit_code is not None and exit_code != 0:
                    self._task_futures[uid].set_exception(
                        TaskExecutionError(uid, task.get("stderr") or "", exit_code)
                    )
                else:
                    self._task_futures[uid].set_result(task)

        return self._task_futures[uid]

    def set_terminal_states(self, states: set[str]) -> None:
        """Update the set of states considered terminal."""
        self._terminal_states = states


class Session:
    """Manages execution session, task submission, and monitoring.

    The Session acts as the central coordinator. It is initialized with a list of execution backends
    and manages the flow of tasks and their state updates.
    """

    def __init__(
        self,
        backends: list[BaseBackend] | None = None,
        uid: str | None = None,
        work_dir: str | None = None,
    ):
        """Initialize a new session.

        Args:
            backends: List of execution backends to use. If None, no backends are configured.
            uid: Optional unique identifier for the session.
            work_dir: working directory (default: cwd).
        """
        self.uid = uid or "session.0000"
        self.work_dir = work_dir or os.getcwd()
        self._tasks: dict[str, BaseTask | dict] = {}
        self._state_manager = TaskStateManager()

        # Register callbacks with all provided backends
        backends_list = backends or []
        self.backends: dict[str, BaseBackend] = {}
        for backend in backends_list:
            self.add_backend(backend)

    def add_backend(self, backend: BaseBackend) -> None:
        """Add a backend to the session and register callbacks.

        Args:
            backend: The execution or inference backend to add.
        """
        self.backends[backend.name] = backend

        # Register state manager callback
        backend.register_callback(self._state_manager.update_task)

        logger.debug(f"Setting up backend callback for'{backend.name}' with Session '{self.uid}'")

        # Sync terminal states from backend
        if hasattr(backend, "get_task_states_map"):
            state_mapper = backend.get_task_states_map()
            self._state_manager._terminal_states.update(state_mapper.terminal_states)

        logger.debug(f"Registered backend '{backend.name}' with Session '{self.uid}'")

    async def submit_tasks(self, tasks: list[dict | BaseTask]) -> list[asyncio.Future]:
        """Submit tasks to execution backends and return futures.

        Args:
            tasks: List of tasks to submit.

        Returns:
            List of asyncio.Future objects representing task lifecycles.
        """
        # Ensure we have a bound loop for callbacks
        if not self._state_manager._loop:
            try:
                self._state_manager.bind_loop(asyncio.get_running_loop())
            except RuntimeError:
                pass
        if not self.backends:
            raise RuntimeError("No backends configured in Session")

        # Group tasks by their explicit backend target
        tasks_by_backend: dict[str, list] = {}
        futures = []
        for task in tasks:
            uid = task["uid"]
            self._tasks[uid] = task

            # Create and bind future
            fut = self._state_manager.get_wait_future(uid, task)
            if hasattr(task, "bind_future"):
                task.bind_future(fut)
            futures.append(fut)

            # Routing decision
            target_name = task.get("backend")
            if not target_name:
                # If no backend specified, use the first one as default
                target_name = next(iter(self.backends))
                task["backend"] = target_name  # Ensure it's recorded

            if target_name not in self.backends:
                available = list(self.backends.keys())
                raise ValueError(
                    f"Backend '{target_name}' requested by task {uid} not found in Session. "
                    f"Available backends: {available}"
                )

            tasks_by_backend.setdefault(target_name, []).append(task)

        # Submit each group to its respective backend concurrently
        submission_tasks = []
        for name, backend_tasks in tasks_by_backend.items():
            backend = self.backends[name]
            submission_tasks.append(backend.submit_tasks(backend_tasks))

        if submission_tasks:
            await asyncio.gather(*submission_tasks)

        logger.info(f"Successfully submitted {len(tasks)} tasks")

        return futures

    async def wait_tasks(
        self,
        tasks: list[dict | BaseTask],
        timeout: float | None = None,
    ) -> list[dict | BaseTask]:
        """Wait for tasks to reach a terminal state.

        Args:
            tasks: List of tasks to wait for.
            timeout: Maximum time to wait in seconds.

        Returns:
            The list of completed task objects.

        Raises:
            asyncio.TimeoutError: If timeout is reached.
        """
        if not tasks:
            return []

        futures = []
        for task in tasks:
            uid = task["uid"]
            futures.append(self._state_manager.get_wait_future(uid, task))

        # Wait for all futures; return_exceptions=True prevents task failures from
        # propagating — callers inspect task.state / task.exception directly.
        try:
            await asyncio.wait_for(
                asyncio.gather(*futures, return_exceptions=True), timeout=timeout
            )
        except asyncio.TimeoutError:
            # Check how many finished
            finished = sum(
                1 for t in tasks if t.get("state") in self._state_manager._terminal_states
            )
            raise asyncio.TimeoutError(
                f"Timeout after {timeout}s: {finished}/{len(tasks)} tasks completed"
            )

        return tasks

    async def close(self) -> None:
        """Shutdown all backends."""
        for backend in self.backends.values():
            await backend.shutdown()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
