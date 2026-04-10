"""No-op execution backend for performance benchmarking.

Tasks are immediately marked as DONE without executing anything.
"""

import asyncio
import logging
from typing import Any, Callable

from ..base import BaseBackend
from ..constants import BackendMainStates, StateMapper


def _get_logger() -> logging.Logger:
    return logging.getLogger(__name__)


class NoopExecutionBackend(BaseBackend):
    """Backend that completes every task instantly.

    Useful for measuring Edge/bridge/client overhead without any actual
    task execution cost.
    """

    def __init__(self, name: str = "noop"):
        super().__init__(name=name)
        self.logger = _get_logger()
        self.tasks: dict[str, dict] = {}
        self._callback_func: Callable = lambda t, s: None
        self._initialized = False
        self._backend_state = BackendMainStates.INITIALIZED

    def __await__(self):
        return self._async_init().__await__()

    async def _async_init(self):
        if not self._initialized:
            StateMapper.register_backend_states_with_defaults(backend=self)
            StateMapper.register_backend_tasks_states_with_defaults(backend=self)
            self._backend_state = BackendMainStates.INITIALIZED
            self._initialized = True
            self.logger.info("Noop execution backend started")
        return self

    def get_task_states_map(self):
        return StateMapper(backend=self)

    async def submit_tasks(self, tasks: list[dict[str, Any]]) -> list[asyncio.Task]:
        if self._backend_state != BackendMainStates.RUNNING:
            self._backend_state = BackendMainStates.RUNNING

        submitted = []
        for task in tasks:
            task.update({
                "return_value": True,
                "stdout":       "",
                "stderr":       "",
                "exit_code":    0,
            })
            self.tasks[task["uid"]] = task
            future = asyncio.create_task(self._complete(task))
            submitted.append(future)
        return submitted

    async def _complete(self, task: dict) -> None:
        self._callback_func(task, "DONE")

    async def cancel_task(self, uid: str) -> bool:
        return uid in self.tasks

    async def cancel_all_tasks(self) -> int:
        n = len(self.tasks)
        self.tasks.clear()
        return n

    async def shutdown(self) -> None:
        self._backend_state = BackendMainStates.SHUTDOWN
        self.tasks.clear()
        self.logger.info("Noop execution backend shutdown")

    def build_task(self, uid, task_desc, task_specific_kwargs):
        pass

    def link_explicit_data_deps(self, src_task=None, dst_task=None,
                                file_name=None, file_path=None):
        pass

    def link_implicit_data_deps(self, src_task, dst_task):
        pass

    async def state(self) -> str:
        return self._backend_state.value

    def task_state_cb(self):
        pass

    async def __aenter__(self):
        if not self._initialized:
            await self._async_init()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.shutdown()
