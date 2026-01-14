
from __future__ import annotations

import asyncio
import logging
import os
import time
from collections import Counter
from collections import defaultdict
from typing import TYPE_CHECKING
from typing import Any
from typing import Optional

if TYPE_CHECKING:
    from rhapsody.api.task import BaseTask
    from rhapsody.backends.base import BaseExecutionBackend
from rhapsody.backends.constants import StateMapper, TasksMainStates


logger = logging.getLogger(__name__)


class TaskStateManager:
    """Centralized manager for task state updates and monitoring.

    This class subscribes to backend callbacks and provides a synchronization
    mechanism for waiting on task completion.
    """

    def __init__(self):
        self._task_futures: dict[str, asyncio.Future] = {}
        self._task_states: dict[str, str] = {}
        self._terminal_states = set()  # Will be populated by backends
        self._lock = asyncio.Lock()
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Bind an event loop to the manager for thread-safe updates."""
        self._loop = loop

    def update_task(self, task: dict | BaseTask, state: str) -> None:
        """Update task state and notify waiters (thread-safe)."""
        if self._loop and not self._loop.is_closed():
             self._loop.call_soon_threadsafe(self._update_task_impl, task, state)
        else:
             # Fallback if no loop bound (e.g. synch tests) or loop closed
             # Warning: This is not thread-safe if called from another thread
             self._update_task_impl(task, state)

    def _update_task_impl(self, task: dict | BaseTask, state: str) -> None:
        """Actual update logic, expected to run on the event loop."""
        uid = task['uid']
        now = time.time()
        
        # Update the task object in-place (Single Source of Truth update)
        task['state'] = state
        
        # Telemetry: Record transition history
        if 'history' not in task:
            task['history'] = {}
        task['history'][state] = now
        
        self._task_states[uid] = state
        
        # If terminal, notify waiters
        if state in self._terminal_states:
            if uid in self._task_futures:
                fut = self._task_futures[uid]
                if not fut.done():
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
            
            # If already done before we started waiting, set result immediately
            if self._task_states.get(uid) in self._terminal_states:
                self._task_futures[uid].set_result(task)
        
        return self._task_futures[uid]

    def set_terminal_states(self, states: set[str]) -> None:
        """Update the set of states considered terminal."""
        self._terminal_states = states


class Session:
    """Manages execution session, task submission, and monitoring.

    The Session acts as the central coordinator. It is initialized with a list
    of execution backends and manages the flow of tasks and their state updates.
    """

    def __init__(
        self,
        backends: Optional[list[BaseExecutionBackend]] = None,
        uid: Optional[str] = None,
        work_dir: Optional[str] = None,
    ):
        """Initialize a new session.

        Args:
            backends: List of execution backends to use. If None, no backends are configured.
            uid: Optional unique identifier for the session.
            work_dir: working directory (default: cwd).
        """
        self.uid = uid or 'session.0000'
        self.work_dir = work_dir or os.getcwd()
        self.backends = backends or []
        self._tasks: dict[str, BaseTask | dict] = {}
        
        self._state_manager = TaskStateManager()
        
        # Register callbacks with all provided backends
        for backend in self.backends:
            # We wrap the update to ensuring it matches the signature expected by backends
            # backend.register_callback(self._on_backend_update)
            # Actually, backends usually take a function: cb(task, state)
            backend.register_callback(self._state_manager.update_task)
            
            # Also sync terminal states from backend
            if hasattr(backend, 'get_task_states_map'):
                state_mapper = backend.get_task_states_map()
                # Aggregate terminal states (union)
                self._state_manager._terminal_states.update(state_mapper.terminal_states)

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
            raise RuntimeError("No execution backends configured in Session")

        # MVP Strategy: Submit all tasks to the first backend.
        # Future: Implement scheduling/routing logic.
        backend = self.backends[0]
        
        # Track tasks locally for telemetry and futures
        futures = []
        for task in tasks:
            uid = task['uid']
            self._tasks[uid] = task
            
            # Create and bind future
            fut = self._state_manager.get_wait_future(uid, task)
            if hasattr(task, 'bind_future'):
                task.bind_future(fut)
            futures.append(fut)

            # Mark submission time
            if 'history' not in task:
                task['history'] = {}
            if 'submitted' not in task['history']:
                task['history']['submitted'] = time.time()
        
        # ensure we are calling the backend submit_tasks
        await backend.submit_tasks(tasks)
        return futures

    async def wait_tasks(
        self,
        tasks: list[dict | BaseTask],
        timeout: Optional[float] = None,
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
            uid = task['uid']
            futures.append(self._state_manager.get_wait_future(uid, task))

        # Wait for all futures
        try:
             await asyncio.wait_for(asyncio.gather(*futures), timeout=timeout)
        except asyncio.TimeoutError:
             # Check how many finished
             finished = sum(1 for t in tasks if t.get('state') in self._state_manager._terminal_states)
             raise asyncio.TimeoutError(
                 f"Timeout after {timeout}s: {finished}/{len(tasks)} tasks completed"
             )

        return tasks

    def get_statistics(self) -> dict[str, Any]:
        """Get session-wide delivery and performance statistics.

        Returns:
            Dictionary containing task counts, success rates, and latencies.
        """
        stats: dict[str, Any] = {
            "counts": Counter(),
            "latencies": {
                "total": [],
                "queue": [],
                "execution": [],
            },
            "summary": {}
        }
        
        for task in self._tasks.values():
            state = task.get('state', 'UNKNOWN')
            stats["counts"][state] += 1
            
            history = task.get('history', {})
            submitted = history.get('submitted')
            running = history.get('RUNNING')
            done = history.get('DONE') or history.get('FAILED') or history.get('CANCELED')
            
            if submitted and done:
                stats["latencies"]["total"].append(done - submitted)
            
            if submitted and running:
                stats["latencies"]["queue"].append(running - submitted)
                
            if running and done:
                stats["latencies"]["execution"].append(done - running)
        
        # Calculate averages for summary
        for key, values in stats["latencies"].items():
            if values:
                stats["summary"][f"avg_{key}"] = sum(values) / len(values)
            else:
                stats["summary"][f"avg_{key}"] = 0.0
                
        stats["summary"]["total_tasks"] = len(self._tasks)
        return stats

    async def close(self) -> None:
        """Shutdown all backends."""
        for backend in self.backends:
            await backend.shutdown()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
