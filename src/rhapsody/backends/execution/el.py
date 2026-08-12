"""Ensemble Launcher execution backend for distributed computing.

This module provides a backend that executes tasks via the Ensemble Launcher framework, supporting
MPI-based distributed execution environments.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import uuid
from typing import Any
from typing import Callable

from ensemble_launcher import EnsembleLauncher
from ensemble_launcher.config import LauncherConfig
from ensemble_launcher.config import MPIConfig
from ensemble_launcher.config import PolicyConfig
from ensemble_launcher.config import SystemConfig
from ensemble_launcher.ensemble import AsyncTask as AsyncELTask
from ensemble_launcher.ensemble import Task as ELTask
from ensemble_launcher.helper_functions import get_nodes
from ensemble_launcher.orchestrator import ClusterClient

from ..base import BaseBackend
from ..constants import BackendMainStates
from ..constants import StateMapper


class EnsembleBackend(BaseBackend):
    def __init__(
        self,
        name: str | None = None,
        child_executor_name: str = "async_mpi",
        return_stdout: bool = True,
        worker_logs: bool = True,
        master_logs: bool = True,
        gpu_selector: str = "ZE_AFFINITY_MASK",
        children_scheduler_policy: str = "fixed_leafs_children_policy",
        task_scheduler_policy: str = "large_resource_policy",
        enable_workstealing: bool = False,
        checkpoint_dir: str | None = None,
        mpi_flavour: str = "mpich",
        nlevels: int = 0,
        nleafs: int | None = None,
        cpus: list[int] | None = None,
        gpus: list[int] | None = None,
        client_only: bool = False,
        node_id: str = "global",
    ):
        super().__init__(name=name)

        self.logger = logging.getLogger(__name__)
        self._initialized = False
        self._backend_state = BackendMainStates.INITIALIZED
        self._callback_func: Callable = lambda t, s: None
        self._client_only = client_only
        self._node_id = node_id

        task_executor_name = ["async_loky", "async_mpi"]

        self._launcher_config = LauncherConfig(
            child_executor_name=child_executor_name,
            task_executor_name=task_executor_name,
            return_stdout=return_stdout,
            worker_logs=worker_logs,
            master_logs=master_logs,
            gpu_selector=gpu_selector,
            children_scheduler_policy=children_scheduler_policy,
            task_scheduler_policy=task_scheduler_policy,
            policy_config=PolicyConfig(nlevels=nlevels, leaf_nodes=nleafs or len(get_nodes())),
            enable_workstealing=enable_workstealing,
            cluster=True,
            checkpoint_dir=checkpoint_dir or os.path.join(os.getcwd(), f"ckpt_{uuid.uuid4()}"),
            mpi_config=MPIConfig(flavor=mpi_flavour),
        )
        cpus = cpus or list(range(os.cpu_count()))
        ngpus = len(gpus) if gpus is not None else 0
        gpus = gpus or []
        self._sys_config = SystemConfig(
            name="cluster", ncpus=len(cpus), cpus=cpus, ngpus=ngpus, gpus=gpus
        )
        self._el: EnsembleLauncher | None = None
        self._client: ClusterClient | None = None
        self.tasks: dict = {}

    def __await__(self):
        """Make EnsembleBackend awaitable."""
        return self._async_init().__await__()

    async def _async_init(self):
        if not self._initialized:
            try:
                self.logger.debug("Registering backend states...")
                StateMapper.register_backend_states_with_defaults(backend=self)

                self.logger.debug("Registering task states...")
                StateMapper.register_backend_tasks_states_with_defaults(backend=self)

                self._backend_state = BackendMainStates.INITIALIZED
                self.logger.debug(f"Backend state set to: {self._backend_state.value}")

                await self._initialize()
                self._initialized = True

                self.logger.info("Ensemble backend fully initialized and ready")

            except Exception as e:
                self.logger.exception(f"Ensemble backend initialization failed: {e}")
                self._initialized = False
                raise
        return self

    async def _initialize(self):
        if not self._client_only:
            self._el = EnsembleLauncher(
                ensemble_file={},
                system_config=self._sys_config,
                launcher_config=self._launcher_config,
            )
            await asyncio.to_thread(self._el.start, wait_time=5)

        self._client = ClusterClient(
            self._launcher_config.checkpoint_dir,
            node_id=self._node_id,
        )
        await asyncio.to_thread(self._client.start)

    def _ensure_initialized(self):
        if not self._initialized:
            raise RuntimeError(
                "EnsembleBackend must be awaited before use. "
                "Use: backend = await EnsembleBackend(...)"
            )

    async def _handle_task(self, task: dict) -> None:
        try:
            is_executable = task.get("executable", None) is not None
            self._callback_func(task, "RUNNING")
            el_task = self.build_task(task)
            fut = self._client.submit(el_task)
            task["future"] = fut
            result = await asyncio.wrap_future(fut)
            if is_executable:
                task["return_value"] = ""
                task["stdout"] = result.split(",")[0]  ## EL returns "stdout,stderr"
                task["stderr"] = result.split(",")[1]
            else:
                task["return_value"] = result
                task["stdout"] = ""
                task["stderr"] = ""
            self._callback_func(task, "DONE")
        except Exception as e:
            task["exception"] = e
            task["stdout"] = ""
            task["stderr"] = str(e)
            self._callback_func(task, "FAILED")

    async def submit_tasks(self, tasks: list[dict]) -> None:
        self._ensure_initialized()

        if self._backend_state == BackendMainStates.SHUTDOWN:
            raise RuntimeError("Cannot submit during shutdown")

        if self._backend_state != BackendMainStates.RUNNING:
            self._backend_state = BackendMainStates.RUNNING
            self.logger.debug(f"Backend state set to: {self._backend_state.value}")

        for task in tasks:
            future = asyncio.create_task(self._handle_task(task))
            self.tasks[task["uid"]] = task
            self.tasks[task["uid"]]["future"] = future

    async def shutdown(self) -> None:
        self._backend_state = BackendMainStates.SHUTDOWN
        self.logger.debug(f"Backend state set to: {self._backend_state.value}")

        try:
            if self._client is not None:
                self._client.teardown()
            if self._el is not None:
                self._el.stop()
        except Exception as e:
            self.logger.exception(f"Error during shutdown: {e}")
        finally:
            self._client = None
            self._el = None
            self.tasks.clear()
            self._initialized = False
            self.logger.info("Ensemble execution backend shutdown complete")

    async def state(self) -> str:
        return self._backend_state.value

    def task_state_cb(self, task: dict, state: str) -> None:
        self._callback_func(task, state)

    def register_callback(self, func: Callable[[dict[str, Any], str], None]) -> None:
        self._callback_func = func

    def get_task_states_map(self) -> Any:
        return StateMapper(backend=self)

    def build_task(self, task: dict) -> ELTask | AsyncELTask:
        backend_kwargs = task.get("task_backend_specific_kwargs", {})
        nnodes = backend_kwargs.get("nnodes", 1)
        ppn = backend_kwargs.get("ranks", 1) // nnodes
        ngpus_per_process = backend_kwargs.get("gpus_per_rank", 0)
        env = backend_kwargs.get("env", {})
        cpu_affinity = (
            list(map(int, backend_kwargs.get("cpu_affinity").split(",")))
            if "cpu_affinity" in backend_kwargs
            else []
        )
        gpu_affinity = (
            list(map(int, backend_kwargs.get("gpu_affinity").split(",")))
            if "gpu_affinity" in backend_kwargs
            else []
        )

        func = task.get("function")
        if func is not None and inspect.iscoroutinefunction(func):
            task_class = AsyncELTask
        else:
            task_class = ELTask

        is_executable = task.get("executable", None) is not None
        if is_executable:
            ## When ELTask.executable is str, it ignore args and kwargs.
            exec = task["executable"] + " " + " ".join([str(x) for x in task["arguments"]])
        else:
            exec = func

        return task_class(
            task_id=task["uid"],
            nnodes=nnodes,
            ppn=ppn,
            ngpus_per_process=ngpus_per_process,
            executable=exec,
            args=task["args"],
            kwargs=task["kwargs"],
            executor_name="async_mpi" if is_executable else "async_loky",
            env=env,
            cpu_affinity=cpu_affinity,
            gpu_affinity=gpu_affinity,
        )

    def link_implicit_data_deps(self, src_task: dict[str, Any], dst_task: dict[str, Any]) -> None:  # noqa: B027
        pass

    def link_explicit_data_deps(  # noqa: B027
        self,
        src_task: dict[str, Any] | None = None,
        dst_task: dict[str, Any] | None = None,
        file_name: str | None = None,
        file_path: str | None = None,
    ) -> None:
        pass

    async def cancel_task(self, uid: str) -> bool:
        if uid in self.tasks:
            future = self.tasks[uid].get("future")
            if future:
                return future.cancel()
        return False

    async def __aenter__(self):
        if not self._initialized:
            await self._async_init()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.shutdown()
