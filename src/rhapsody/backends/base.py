"""Base classes and interfaces for Rhapsody execution backends.

This module defines the abstract base class and core interfaces that all execution backends must
implement.
"""

from __future__ import annotations

import os
from abc import ABC
from abc import abstractmethod
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple


class BaseExecutionBackend(ABC):
    """Abstract base class for execution backends that manage task execution and state.

    This class defines the interface for execution backends that handle task submission, state
    management, and dependency linking in a distributed or parallel execution environment.
    """

    def __init__(self):
        """Initialize the backend with internal result tracking."""
        self._internal_results = []
        self._callback_registered = False

    @abstractmethod
    async def submit_tasks(self, tasks: list[dict]) -> None:
        """Submit a list of tasks for execution.

        Args:
            tasks: A list of dictionaries containing task definitions and metadata.
                Each task dictionary should contain the necessary information for
                task execution.
        """
        pass

    @abstractmethod
    async def shutdown(self) -> None:
        """Gracefully shutdown the execution backend.

        This method should clean up resources, terminate running tasks if necessary, and prepare the
        backend for termination.
        """
        pass

    @abstractmethod
    def state(self) -> str:
        """Get the current state of the execution backend.

        Returns:
            A string representing the current state of the backend (e.g., 'running',
            'idle', 'shutting_down', 'error').
        """
        pass

    @abstractmethod
    def task_state_cb(self, task: dict, state: str) -> None:
        """Callback function invoked when a task's state changes.

        Args:
            task: Dictionary containing task information and metadata.
            state: The new state of the task (e.g., 'pending', 'running', 'completed',
                'failed').
        """
        pass

    @abstractmethod
    def register_callback(self, func: Callable[[dict[str, Any], str], None]) -> None:
        """Register a callback function for task state changes.

        Args:
            func: A callable that will be invoked when task states change.
                The function should accept task and state parameters.
        """
        pass

    @abstractmethod
    def get_task_states_map(self) -> Any:
        """Retrieve a mapping of task IDs to their current states.

        Returns:
            A dictionary mapping task identifiers to their current execution states.
        """
        pass

    @abstractmethod
    def build_task(self, task: dict) -> None:
        """Build or prepare a task for execution.

        Args:
            task: Dictionary containing task definition, parameters, and metadata
                required for task construction.
        """
        pass

    @abstractmethod
    def link_implicit_data_deps(self, src_task: dict[str, Any], dst_task: dict[str, Any]) -> None:
        """Link implicit data dependencies between two tasks.

        Creates a dependency relationship where the destination task depends on
        data produced by the source task, with the dependency being inferred
        automatically.

        Args:
            src_task: The source task that produces data.
            dst_task: The destination task that depends on the source task's output.
        """
        pass

    @abstractmethod
    def link_explicit_data_deps(
        self,
        src_task: dict[str, Any] | None = None,
        dst_task: dict[str, Any] | None = None,
        file_name: str | None = None,
        file_path: str | None = None,
    ) -> None:
        """Link explicit data dependencies between tasks or files.

        Creates explicit dependency relationships based on specified file names
        or paths, allowing for more precise control over task execution order.

        Args:
            src_task: The source task that produces the dependency.
            dst_task: The destination task that depends on the source.
            file_name: Name of the file that represents the dependency.
            file_path: Full path to the file that represents the dependency.
        """
        pass

    @abstractmethod
    async def cancel_task(self, uid: str) -> bool:
        """Cancel a task in the execution backend.

        Args:
            uid: Task identifier

        Raises:
            NotImplementedError: If the backend doesn't support cancellation
        """
        raise NotImplementedError("Not implemented in the base backend")

    def _internal_callback(self, task: dict, state: str) -> None:
        """Internal callback that tracks all task state changes."""
        self._internal_results.append((task, state))

    async def wait_tasks(
        self,
        tasks: List[Dict[str, Any]],
        timeout: Optional[float] = None,
        sleep_interval: float = 0.1,
    ) -> Dict[str, Dict[str, Any]]:
        """Wait for tasks to complete by monitoring internal callback results.

        This method automatically tracks task completion using internal callbacks.
        No need to manually register callbacks or track results.

        Args:
            tasks: List of task dictionaries that were submitted.
                   Each task dict must contain a 'uid' field.
            timeout: Optional timeout in seconds. If None, waits indefinitely.
                    Raises asyncio.TimeoutError if timeout is reached.
            sleep_interval: Seconds to sleep when no progress is detected (default: 0.1).

        Returns:
            Dictionary mapping task UIDs to their complete task objects (dicts).
            Each task dict contains 'uid', 'state', and optionally 'stdout',
            'stderr', 'return_value', 'exception', etc.

        Raises:
            asyncio.TimeoutError: If timeout is specified and exceeded.
            ValueError: If tasks list is empty or tasks missing 'uid' field.

        Example:
            # Simple usage - no manual callback setup needed!
            tasks = [
                {"uid": "task_1", "executable": "echo hello"},
                {"uid": "task_2", "executable": "echo world"}
            ]

            await backend.submit_tasks(tasks)
            completed = await backend.wait_tasks(tasks)

            # Access results
            for uid, task in completed.items():
                print(f"{uid}: {task['state']}")
                if task['state'] == 'DONE':
                    print(f"  Output: {task.get('stdout', '')}")
        """
        import asyncio

        # Validation
        if not tasks:
            raise ValueError("tasks list cannot be empty")

        # Register internal callback once
        if not self._callback_registered:
            self.register_callback(self._internal_callback)
            self._callback_registered = True

        # Get terminal states once (cached after first call)
        if not hasattr(self, '_terminal_states'):
            state_mapper = self.get_task_states_map()
            self._terminal_states = set(state_mapper.terminal_states)
            # Also support string 'CANCELLED' variant for backward compatibility
            self._terminal_states.add('CANCELLED')

        num_tasks = len(tasks)

        # Extract task UIDs for tracking
        task_uids = set()
        for task in tasks:
            if 'uid' not in task:
                raise ValueError(f"Task missing 'uid' field: {task}")
            task_uids.add(task['uid'])

        # Track finished tasks: uid -> task_dict (with state added)
        finished_tasks = {}

        # Track processed result indices to avoid re-processing
        processed_index = 0

        # Timeout handling
        start_time = asyncio.get_event_loop().time() if timeout else None

        while len(finished_tasks) < num_tasks:
            # Check timeout
            if timeout is not None:
                elapsed = asyncio.get_event_loop().time() - start_time
                if elapsed >= timeout:
                    raise asyncio.TimeoutError(
                        f"Timeout after {timeout}s: {len(finished_tasks)}/{num_tasks} tasks completed"
                    )

            # Process new results only (from processed_index onwards)
            made_progress = False
            while processed_index < len(self._internal_results):
                task, state = self._internal_results[processed_index]
                processed_index += 1

                # Extract task UID
                task_uid = task.get('uid') if isinstance(task, dict) else str(task)

                # Only process terminal states for tasks we're waiting for
                if (state in self._terminal_states and
                    task_uid in task_uids and
                    task_uid not in finished_tasks):
                    # Store complete task object with state
                    task_with_state = task.copy() if isinstance(task, dict) else {'uid': task_uid}
                    task_with_state['state'] = state
                    finished_tasks[task_uid] = task_with_state
                    made_progress = True

            # Sleep only if no progress was made
            if not made_progress and len(finished_tasks) < num_tasks:
                await asyncio.sleep(sleep_interval)

        return finished_tasks


class Session:
    """Manages execution session state and working directory.

    This class maintains session-specific information including the current working directory path
    for task execution.
    """

    def __init__(self):
        """Initialize a new session with the current working directory.

        Sets the session path to the current working directory at the time of initialization.
        """
        self.path = os.getcwd()
