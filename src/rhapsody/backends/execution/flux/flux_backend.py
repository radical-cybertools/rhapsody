"""Flux execution backend for Rhapsody.

This module provides the FluxExecutionBackend which connects to a Flux resource manager instance to
execute tasks.
"""

from __future__ import annotations

import asyncio
import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from rhapsody.backends.base import BaseBackend
from rhapsody.backends.constants import BackendMainStates
from rhapsody.backends.constants import StateMapper
from rhapsody.backends.constants import TasksMainStates

from . import flux_sysmon
from .flux_helper import FluxHelper
from .flux_module import FluxModule
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
        sysmon: bool = False,
        sdexec: bool | str | None = None,
    ):
        """Initialize the Flux execution backend.

        Args:
            uri: Optional existing FLUX_URI. If None, a new Flux instance
                will be started using FluxService.
            launcher: Optional launcher prefix (e.g. MPI launcher) if
                starting a new Flux instance.  When unset and a partition
                is given, an ``srun`` launcher is derived from the
                partition spec (see :meth:`_partition_launcher`).
            name: Name of the backend.
            resources: Optional resource spec.  Supported key: ``partition``
                — a ``rhapsody_rm.partition_spec``-shaped dict
                (``{"nodelist": [Node…], "env": {…}}``).  The Flux instance
                is constrained to the partition's nodes via the derived
                launcher; the backend connects to it through the
                ssh-reachable URI (the instance's rank-0 broker runs on the
                partition's first node, not necessarily this host).
            sysmon: Enable flux's ``sysmon`` shell plugin for every task and
                record its summary as ``task["resource_usage"]`` (peak
                memory and CPU load; see :mod:`.flux_sysmon`).  The values
                are per job only when the instance runs jobs via sdexec;
                ``resource_usage["scope"]`` is ``"job"`` then, else
                ``"shell"`` (the broker's shared cgroup).
            sdexec: Passed to :class:`FluxService` when this backend starts
                its own instance.  Default (``None``): ``"auto"`` when
                *sysmon* is set, else ``False``.
        """
        super().__init__(name=name)

        self.logger = logger
        self._uri = uri
        self._launcher = launcher
        # Accept the documented ``partition`` key; reject any other resources
        # keys we don't yet support so silent failures are impossible.
        self._resources = dict(resources or {})
        partition = self._resources.pop("partition", None)
        if self._resources:
            raise NotImplementedError(
                f"FluxExecutionBackend: unsupported resources keys: {list(self._resources)}"
            )
        self._partition: dict = partition or {}
        self._sysmon = sysmon
        self._sdexec = sdexec if sdexec is not None else ("auto" if sysmon else False)
        self._sysmon_scope = "shell"
        self._sysmon_pool: ThreadPoolExecutor | None = None
        self._sysmon_local = threading.local()
        # Tasks that already got a terminal callback (exception, then finish).
        self._finished: set[str] = set()
        self._finished_lock = threading.Lock()  # journal thread vs cancel_task
        # Tasks with a cancel in flight -> journal events deferred until it resolves.
        self._cancelling: dict[str, list[Any]] = {}

        self._flux_service: FluxService | None = None
        self._flux_helper: FluxHelper | None = None

        self.tasks: dict[str, dict] = {}
        self._callback_func: callable = lambda t, s: None
        self._loop: asyncio.AbstractEventLoop | None = None

        self._initialized = False
        self._backend_state = BackendMainStates.INITIALIZED

    # ------------------------------------------------------------------
    # Launcher construction (rhapsody-partitions hook).
    #
    # Flux differs from Dragon here: a Flux client reaches its instance
    # through a URI, so the child process hosting this backend needs no
    # argv wrapper (and must not get one — the RemoteBackendProxy control
    # plane is an AF_UNIX socket, so the child has to stay on the driver's
    # node).  The partition is consumed *inside* the backend instead: the
    # (partition-constrained) Flux instance is bootstrapped by FluxService
    # under an srun launcher derived from the spec, and the backend
    # connects via the ssh-reachable URI.
    # ------------------------------------------------------------------

    @classmethod
    def build_launch_prefix(cls, partition: dict | None) -> list[str]:
        """Return the (empty) argv prefix for a proxied child host.

        Flux intentionally returns the base default: the partition
        constraint is applied via the ``srun`` launcher that
        :meth:`_partition_launcher` hands to ``FluxService``, not by
        wrapping the child process.
        """
        return []

    @staticmethod
    def _partition_launcher(partition: dict) -> str:
        """Build the ``srun`` prefix that pins ``flux start`` to the partition.

        Honours :func:`rhapsody_rm.partition_spec` output: one broker per
        partition node, constrained via ``--nodelist``.  ``--overlap`` lets
        the step share the allocation with other partitions' runtimes.
        """
        nodes = partition["nodelist"]
        hosts = ",".join(node.name for node in nodes)
        return (
            f"srun --overlap --nodes={len(nodes)} --ntasks={len(nodes)} "
            f"--ntasks-per-node=1 --nodelist={hosts}"
        )

    def __await__(self):
        """Make backend awaitable."""
        return self._async_init().__await__()

    async def _async_init(self):
        """Initialize the Flux connection asynchronously."""
        if not self._initialized:
            self._loop = asyncio.get_running_loop()
            try:
                self.logger.debug("Registering backend states...")
                StateMapper.register_backend_states_with_defaults(backend=self)

                self.logger.debug("Registering task states...")
                StateMapper.register_backend_tasks_states_with_defaults(backend=self)

                self._backend_state = BackendMainStates.INITIALIZED

                if not self._uri:
                    launcher = self._launcher
                    partition_launch = False
                    if not launcher and self._partition.get("nodelist"):
                        launcher = self._partition_launcher(self._partition)
                        partition_launch = True
                    self.logger.info(
                        "No FLUX_URI provided, starting a new FluxService (launcher: %s)",
                        launcher or "none",
                    )
                    self._flux_service = FluxService(launcher=launcher, sdexec=self._sdexec)
                    # Use asyncio.to_thread to avoid blocking the event loop
                    success = await asyncio.to_thread(self._flux_service.start, 60.0)
                    if not success:
                        raise RuntimeError("FluxService failed to start within timeout")
                    # A partition-launched instance has its rank-0 broker on
                    # the partition's first node — the local socket URI is not
                    # reachable from here; use the ssh-derived one instead.
                    if partition_launch:
                        self._uri = self._flux_service.r_uri
                    else:
                        self._uri = self._flux_service.uri

                self.logger.info("Connecting FluxHelper to %s", self._uri)
                self._flux_helper = FluxHelper(uri=self._uri, log=self.logger)
                self._flux_helper.register_cb(self._flux_event_cb)
                await asyncio.to_thread(self._flux_helper.start)

                if self._sysmon:
                    await asyncio.to_thread(self._init_sysmon)

                self._initialized = True
                self.logger.info("Flux execution backend started successfully")

            except Exception as e:
                self.logger.exception("Flux backend initialization failed: %s", e)
                self._initialized = False
                raise
        return self

    def telemetry_info(self) -> dict | None:
        """Return the instance URI for :class:`FluxTelemetryAdapter`.

        For a partition launch this is the ssh-reachable ``r_uri``.
        """
        if not self._uri:
            return None
        return {"kind": "flux", "uri": self._uri}

    def _init_sysmon(self) -> None:
        """Record whether sysmon values are per job and start the reader pool."""
        try:
            # Own handle: FluxHelper guards its handle with a lock the journal thread uses.
            config = FluxModule().core.Flux(self._uri).rpc("config.get").get()
            service = (config.get("exec") or {}).get("service", "rexec")
        except Exception:  # noqa: BLE001
            self.logger.debug("could not read instance config", exc_info=True)
            service = "rexec"
        self._sysmon_scope = "job" if service == "sdexec" else "shell"
        if self._sysmon_scope == "shell":
            self.logger.warning(
                "%s: flux instance runs jobs via %s, not sdexec — sysmon values "
                "describe the broker's shared cgroup, not the job "
                "(flux-framework/flux-core#7835)",
                self.name,
                service,
            )
        self._sysmon_pool = ThreadPoolExecutor(
            max_workers=4, thread_name_prefix=f"rhapsody-flux-sysmon-{self.name}"
        )

    def _complete_with_sysmon(self, task: dict, state: str, jobid: Any) -> None:
        """Attach the sysmon summary to *task*, then fire the state callback."""
        try:
            handle = getattr(self._sysmon_local, "handle", None)
            if handle is None:
                handle = FluxModule().core.Flux(self._uri)
                self._sysmon_local.handle = handle
            entries = flux_sysmon.read_entries(FluxModule().job, handle, jobid, timeout=2.0)
            usage = flux_sysmon.summarize(entries, self._sysmon_scope)
            if usage is not None:
                task["resource_usage"] = usage
        except Exception:  # noqa: BLE001
            self.logger.debug("sysmon read failed for %s", task.get("uid"), exc_info=True)
        self._fire(task, state)

    def _fire(self, task: dict, state: str) -> None:
        """Deliver a state change from a flux thread on the event loop.

        Session's task state manager must run on the loop: a future resolved
        from another thread does not wake the loop, so ``wait_tasks`` would
        only return at its timeout.
        """
        loop = self._loop
        if loop is not None and loop.is_running():
            loop.call_soon_threadsafe(self._invoke_callback, task, state)
        else:
            self._invoke_callback(task, state)

    def _invoke_callback(self, task: dict, state: str) -> None:
        try:
            self._callback_func(task, state)
        except Exception as e:
            self.logger.exception("Error in task callback for %s: %s", task.get("uid"), e)

    def get_task_states_map(self):
        """Return the state mapper for this backend."""
        return StateMapper(backend=self)

    def _mark_finished(self, uid: str) -> bool:
        """Record *uid* as terminal; False if it already was (one terminal state per task)."""
        with self._finished_lock:
            if uid in self._finished:
                return False
            self._finished.add(uid)
            return True

    def _flux_event_cb(self, task_uid: str, event: Any) -> None:
        """Handle events coming from the Flux journal.

        Maps Flux journal events to Rhapsody task states and invokes callbacks.
        """
        with self._finished_lock:
            if task_uid in self._cancelling:
                self._cancelling[task_uid].append(event)
                return
        if task_uid not in self.tasks or task_uid in self._finished:
            return
        task = self.tasks[task_uid]
        event_name = event.name

        state = None
        if event_name in ("submit", "alloc", "start"):
            state = TasksMainStates.RUNNING.value
        elif event_name == "finish":
            # The finish context 'status' is a wait(2) status (RFC 21), not an
            # exit code — decode it (negative = terminated by signal).
            status = getattr(event, "context", {}).get("status", 0)
            exit_code = os.waitstatus_to_exitcode(status)
            task["exit_code"] = exit_code
            if exit_code == 0:
                state = TasksMainStates.DONE.value
            else:
                state = TasksMainStates.FAILED.value
            if not self._mark_finished(task_uid):
                return
            pool = self._sysmon_pool
            if pool is not None:
                # Reading the output eventlog is an RPC — keep it off the
                # journal thread.
                try:
                    pool.submit(self._complete_with_sysmon, task, state, event.jobid)
                    return
                except RuntimeError:
                    pass  # pool shut down meanwhile: deliver without resource usage
        elif event_name == "exception":
            if getattr(event, "context", {}).get("severity", 0) != 0:
                return  # non-fatal: the job keeps running
            state = TasksMainStates.FAILED.value
            if not self._mark_finished(task_uid):
                return
            task["exit_code"] = 1

        if state:
            self._fire(task, state)

    async def submit_tasks(self, tasks: list[dict[str, Any]]) -> None:
        """Submit tasks to Flux."""
        if self._backend_state != BackendMainStates.RUNNING:
            self._backend_state = BackendMainStates.RUNNING

        specs = []
        for task in tasks:
            self.tasks[task["uid"]] = task
            with self._finished_lock:
                self._finished.discard(task["uid"])  # a resubmitted uid starts over
            task.pop("resource_usage", None)
            # Map canonical task dictionary values to the kwargs expected by spec_from_dict
            td = {
                "uid": task["uid"],
                "executable": task["executable"],
                "arguments": task.get("arguments", []),
                "ranks": task.get("ranks", 1),
                "timeout": task.get("timeout", 0.0),
                "environment": task.get("environment"),
                "sandbox": task.get("sandbox"),
                "sysmon": self._sysmon,
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
            # Defer journal events while the cancel is in flight: on success the
            # cancel's exception event must not report FAILED; on failure (e.g. the
            # job finished meanwhile) the deferred events still deliver its state.
            with self._finished_lock:
                if uid in self._finished or uid in self._cancelling:
                    return False
                self._cancelling[uid] = []
            try:
                await asyncio.to_thread(self._flux_helper.cancel, uid)
            except Exception as e:
                with self._finished_lock:
                    deferred = self._cancelling.pop(uid, [])
                self.logger.error("Failed to cancel flux task %s: %s", uid, e)
                for event in deferred:
                    self._flux_event_cb(uid, event)
                return False
            with self._finished_lock:
                self._cancelling.pop(uid, None)
                self._finished.add(uid)
            self._callback_func(self.tasks[uid], TasksMainStates.CANCELED.value)
            return True
        return False

    async def shutdown(self) -> None:
        """Shutdown the Flux backend and service."""
        self._backend_state = BackendMainStates.SHUTDOWN

        if self._sysmon_pool is not None:
            # Let pending sysmon reads fire their completion callbacks.
            await asyncio.to_thread(self._sysmon_pool.shutdown, True)
            self._sysmon_pool = None

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
