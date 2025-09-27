"""No-operation execution backend for testing and development.

This module provides a no-op backend that simulates task execution without actually running any
tasks.
"""

from __future__ import annotations

from typing import Callable

from ..base import BaseExecutionBackend
from ..base import Session
from ..constants import StateMapper


class NoopExecutionBackend(BaseExecutionBackend):
    """A no-operation execution backend for testing and development purposes.

    This backend simulates task execution without actually running any tasks. All submitted tasks
    immediately return dummy output and transition to DONE state. Useful for testing workflow logic
    without computational overhead.
    """

    def __init__(self):
        """Initialize the no-op execution backend.

        Sets up dummy task storage, session, and default callback function. Registers backend states
        and confirms successful initialization.
        """
        self.tasks = {}
        self.session = Session()
        self._callback_func: Callable = lambda task, state: None  # default no-op
        StateMapper.register_backend_states_with_defaults(backend=self)

    def state(self):
        """Get the current state of the no-op execution backend.

        Returns:
            str: Always returns 'IDLE' as this backend performs no actual work.
        """
        return "IDLE"

    def task_state_cb(self, task: dict, state: str) -> None:
        """Callback function invoked when a task's state changes.

        Args:
            task: Dictionary containing task information and metadata.
            state: The new state of the task.

        Note:
            This is a no-op implementation that performs no actions.
        """
        pass

    def get_task_states_map(self):
        """Retrieve a mapping of task IDs to their current states.

        Returns:
            StateMapper: Object containing the mapping of task states for this backend.
        """
        return StateMapper(backend=self)

    def register_callback(self, func: Callable):
        """Register a callback for task state changes.

        Args:
            func: Function to be called when task states change. Should accept
                task and state parameters.
        """
        self._callback_func = func

    def build_task(self, uid, task_desc, task_specific_kwargs):
        """Build or prepare a task for execution.

        Args:
            uid: Unique identifier for the task.
            task_desc: Dictionary containing task description and metadata.
            task_specific_kwargs: Backend-specific keyword arguments for the task.

        Note:
            This is a no-op implementation that performs no actual task building.
        """
        pass

    async def cancel_task(self, uid: str) -> bool:
        """Cancel a task by its UID.

        Args:
            uid: The UID of the task to cancel.

        Returns:
            bool: Always returns False since noop backend doesn't track running tasks
                  that can be cancelled.
        """
        return False

    async def submit_tasks(self, tasks) -> None:
        """Submit tasks for mock execution.

        Immediately marks all tasks as completed with dummy output without
        performing any actual computation.

        Args:
            tasks: List of task dictionaries to be processed. Each task will
                receive dummy stdout and return_value before being marked as DONE.
        """
        for task in tasks:
            task["stdout"] = "Dummy Output"
            task["return_value"] = "Dummy Output"
            self._callback_func(task, "DONE")

    def link_explicit_data_deps(self, src_task=None, dst_task=None, file_name=None, file_path=None):
        """Handle explicit data dependencies between tasks.

        Args:
            src_task: The source task that produces the dependency.
            dst_task: The destination task that depends on the source.
            file_name: Name of the file that represents the dependency.
            file_path: Full path to the file that represents the dependency.

        Note:
            This is a no-op implementation as this backend doesn't handle dependencies.
        """
        pass

    def link_implicit_data_deps(self, src_task, dst_task):
        """Handle implicit data dependencies for a task.

        Args:
            src_task: The source task that produces data.
            dst_task: The destination task that depends on the source task's output.

        Note:
            This is a no-op implementation as this backend doesn't handle dependencies.
        """
        pass

    async def shutdown(self) -> None:
        """Shutdown the no-op execution backend.

        Performs cleanup operations. Since this is a no-op backend, no actual resources need to be
        cleaned up.
        """
        pass
