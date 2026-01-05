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

    @staticmethod
    async def wait_tasks(
        results: List[Tuple[str, str]],
        num_tasks: int,
        timeout: Optional[float] = None,
        sleep_interval: float = 0.1,
        verbose: bool = True
    ) -> Dict[str, str]:
        """Wait for tasks to complete by monitoring callback results.

        This is a utility method for backends that use callback patterns.
        It monitors a results list populated by registered callbacks and waits
        until all tasks reach terminal states.

        Args:
            results: List of (task_uid, state) tuples populated by callbacks.
                     This list is mutated by the callback function registered
                     via backend.register_callback().
            num_tasks: Expected number of tasks to wait for.
            timeout: Optional timeout in seconds. If None, waits indefinitely.
                    Raises asyncio.TimeoutError if timeout is reached.
            sleep_interval: Seconds to sleep when no progress is detected (default: 0.1).
            verbose: If True, prints task completion messages (default: True).

        Returns:
            Dictionary mapping task UIDs to their final states.

        Raises:
            asyncio.TimeoutError: If timeout is specified and exceeded.
            ValueError: If num_tasks is less than 1.

        Example:
            results = []
            def callback(task, state):
                results.append((task["uid"], state))

            backend.register_callback(callback)
            await backend.submit_tasks(tasks)

            # Wait for all tasks to complete
            final_states = await backend.wait_tasks(results, len(tasks))

            # Check for failures
            failed = [uid for uid, state in final_states.items() if state == 'FAILED']
        """
        import asyncio

        # Validation
        if num_tasks < 1:
            raise ValueError(f"num_tasks must be >= 1, got {num_tasks}")

        # Terminal states (support both CANCELED spellings)
        TERMINAL_STATES = {'DONE', 'FAILED', 'CANCELED', 'CANCELLED'}

        # Track finished tasks: uid -> final_state
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
            while processed_index < len(results):
                task_uid, state = results[processed_index]
                processed_index += 1

                # Only process terminal states and avoid duplicates
                if state in TERMINAL_STATES and task_uid not in finished_tasks:
                    finished_tasks[task_uid] = state
                    made_progress = True

                    if verbose:
                        print(f"Task {task_uid}: {state}")

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
