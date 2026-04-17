import argparse
import asyncio
import json
import logging
import os
import queue
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any
from typing import Callable
from typing import Optional

from ..base import BaseBackend
from ..constants import BackendMainStates
from ..constants import StateMapper

try:
    import multiprocessing as mp

    import dragon
    from dragon.data.ddict.ddict import DDict
    from dragon.infrastructure.gpu_desc import AccVendor
    from dragon.infrastructure.gpu_desc import find_accelerators
    from dragon.infrastructure.policy import Policy

    # Node Telemetry only
    from dragon.native.event import Event
    from dragon.native.machine import System
    from dragon.native.process import Popen
    from dragon.native.process import Process
    from dragon.native.process import ProcessTemplate
    from dragon.native.process_group import DragonUserCodeError
    from dragon.native.process_group import ProcessGroup
    from dragon.native.queue import Queue
    from dragon.telemetry import Telemetry
    from dragon.workflows.batch import Batch

except ImportError:  # pragma: no cover - environment without Dragon
    dragon = None
    Process = None
    ProcessTemplate = None
    ProcessGroup = None
    Popen = None
    Queue = None
    DDict = None
    System = None
    Policy = None
    Batch = None
    Event = None
    Telemetry = None
    AccVendor = None
    find_accelerators = None


def _get_logger() -> logging.Logger:
    """Get logger for dragon backend module.

    This function provides lazy logger evaluation, ensuring the logger is created after the user has
    configured logging, not at module import time.
    """
    return logging.getLogger(__name__)


def _bootstrap_dragon_runtime(
    num_nodes: Optional[int] = None,
    launcher_args: Optional[list] = None,
) -> None:
    """Bootstrap Dragon runtime if not already running as a Dragon head process.

    On the first invocation (plain ``python script.py``, ``DRAGON_MY_PUID`` absent):
      - Constructs ``sys.argv`` for the Dragon launcher (``num_nodes`` → ``-N``;
        ``launcher_args`` forwarded verbatim; Dragon's argparse handles all validation).
      - Auto-selects single-node or multi-node via ``determine_environment()``.
      - Starts Local Services and Global Services, spawns this script as the
        Dragon-managed head process, then ``sys.exit()``s.

    On the second invocation (Dragon head process, ``DRAGON_MY_PUID`` present):
      - Immediately returns. ``Batch()`` and all Dragon APIs are available.
    """
    if "DRAGON_MY_PUID" in os.environ:
        return  # Already running as Dragon-managed head process

    # dragon_single.main() reads sys.argv internally (no args_input path),
    # so argv manipulation is unavoidable. We prepend "dragon" so sys.argv[1:]
    # matches what the argparse parser expects: [<flags>, PROG, <REMAINDER>].
    argv = ["dragon"]
    if num_nodes is not None:
        argv += ["-N", str(num_nodes)]  # only transformation we own; rest delegated
    if launcher_args:
        argv += list(launcher_args)     # forwarded as-is; Dragon validates, not us
    argv += sys.argv  # sys.argv[0] = "script.py" → becomes PROG in launchargs parser

    sys.argv = argv

    # Auto-detect single vs multi-node (checks SLURM/PBS/SSH/K8s env)
    from dragon.launcher.launch_selector import determine_environment
    multi_mode = determine_environment()

    if not multi_mode:
        from dragon import _patch_multiprocessing  # idempotent — guard in monkeypatching.py
        _patch_multiprocessing()
        from dragon.launcher import dragon_single as _dl
    else:
        from dragon.launcher import dragon_multi_fe as _dl

    sys.exit(_dl.main())  # blocks until head process exits; propagates exit code



class TaskStateMapperV3:
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    DONE = "DONE"
    FAILED = "FAILED"
    CANCELED = "CANCELED"
    terminal_states = {DONE, FAILED, CANCELED}


# ============================================================================
# Main Backend Classes
# V3 Integrates with Dragon.Batch API (Most performant)
# ============================================================================


class DragonExecutionBackendV3(BaseBackend):
    """Dragon Batch backend using the streaming pipeline model.

    Tasks submitted via batch.function()/process()/job() are auto-dispatched by the Batch
    background thread. A single monitor thread polls each task with Task.get(block=False)
    and fires callbacks when results become available in the DDict.

    Note on working directory:
        DragonExecutionBackendV3 does not support a backend-level working directory.
        To set the working directory per task, use ``task_backend_specific_kwargs``
        with ``process_template`` (single process) or ``process_templates`` (MPI job)::

            ComputeTask(
                function=my_func,
                task_backend_specific_kwargs={
                    "process_template": {"cwd": "/path/to/dir"}
                },
            )

            # For MPI jobs:
            ComputeTask(
                function=my_func,
                task_backend_specific_kwargs={
                    "process_templates": [(nranks, {"cwd": "/path/to/dir"})]
                },
            )
    """

    def __init__(
        self,
        num_nodes: Optional[int] = None,
        pool_nodes: Optional[int] = None,
        disable_telemetry: bool = False,
        name: Optional[str] = "dragon",
        launcher_args: Optional[list[str]] = None,
    ):
        if not Batch:
            raise RuntimeError("Dragon Batch not available")

        # Auto-bootstrap: on first `python script.py` invocation, this starts the
        # Dragon runtime and re-spawns this script as the head process, then exits.
        # On the second invocation (Dragon head process), this is a no-op.
        _bootstrap_dragon_runtime(
            num_nodes=num_nodes,
            launcher_args=launcher_args,
        )

        # Everything below runs only in the Dragon head process (second invocation).
        super().__init__(name=name)

        self.logger = _get_logger()
        self.batch = Batch(
            num_nodes=num_nodes,
            pool_nodes=pool_nodes,
            disable_telem=disable_telemetry,
        )

        self._backend_state = BackendMainStates.INITIALIZED
        self._callback_func: Callable = lambda t, s: None
        self._task_registry: dict[str, Any] = {}
        self._task_states = TaskStateMapperV3()
        self._initialized = False
        self._cancelled_tasks: set[str] = set()
        self._monitored_batches = {}
        self._batch_monitor_thread = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

        self._shutdown_event = threading.Event()

        self.logger.info(
            f"DragonExecutionBackendV3: {self.batch.num_workers} workers, "
            f"{self.batch.num_managers} managers"
        )

    def __await__(self):
        return self._async_init().__await__()

    async def _async_init(self):
        """Unified async initialization with backend and task state registration.

        Pattern:
        1. Register backend states first
        2. Register task states
        3. Set backend state to INITIALIZED
        4. Initialize backend components (if needed)
        """
        if not self._initialized:
            try:
                self.logger.debug("Starting Dragon backend V3 async initialization...")

                # Step 1: Register backend states
                self.logger.debug("Registering backend states...")
                StateMapper.register_backend_states_with_defaults(backend=self)

                # Step 2: Register task states
                self.logger.debug("Registering task states...")
                StateMapper.register_backend_tasks_states_with_defaults(backend=self)

                self._initialized = True
                self.logger.info("Dragon backend V3 fully initialized and ready")

            except Exception as e:
                self.logger.exception(f"Dragon backend V3 initialization failed: {e}")
                self._initialized = False
                raise
        return self

    def _monitor_loop(self):
        """Single thread to monitor all active tasks.

        Tasks are auto-dispatched by the Batch background thread the moment they are created. This
        loop polls each registered task non-blocking and fires callbacks when results arrive.
        """
        self.logger.debug("Starting Dragon batch monitor loop (polling mode)")

        while not self._shutdown_event.is_set() or self._monitored_batches:
            try:
                # Iterate over a copy of keys to allow modification during iteration
                batch_tuids = list(self._monitored_batches.keys())

                if not batch_tuids:
                    # No active tasks, sleep briefly to avoid high CPU
                    time.sleep(0.01)
                    continue

                # Collect all completed tasks in this sweep, then deliver in one batch
                completed = []
                for tuid in batch_tuids:
                    if tuid not in self._monitored_batches:
                        continue
                    # Single DDict read: KeyError means not ready yet (avoids redundant __contains__)
                    try:
                        result, tb, raised, stdout, stderr = self.batch.results_ddict[tuid]
                    except KeyError:
                        continue
                    batch_task, uid = self._monitored_batches.pop(tuid)
                    completed.append((uid, result, tb, raised, stdout, stderr))

                # One cross-thread wakeup for the entire sweep batch
                if completed:
                    self._loop.call_soon_threadsafe(self._deliver_batch, completed)

                # Small sleep after each full sweep to prevent tight loop
                time.sleep(0.005)

            except Exception as e:
                self.logger.exception(f"Critical error in monitor loop: {e}")
                time.sleep(0.1)

        self.logger.debug("Dragon batch monitor loop stopped")

    def _deliver_batch(self, completions: list) -> None:
        """Deliver a batch of completed tasks. Runs on the asyncio event loop (via
        call_soon_threadsafe).

        Called once per monitor sweep with all tasks that completed in that sweep, reducing cross-
        thread wakeups from O(tasks) to O(sweeps).
        """
        for uid, result, tb, raised, stdout, stderr in completions:
            task_info = self._task_registry.pop(uid, None)
            if not task_info:
                continue
            if uid in self._cancelled_tasks:
                self._cancelled_tasks.discard(uid)
                continue
            task_desc = task_info["description"]
            if raised:
                task_desc["exception"] = result
                task_desc["stderr"] = tb if tb else str(result)
                if stdout:
                    task_desc["stdout"] = stdout
                self._callback_func(task_desc, "FAILED")
            else:
                task_desc["return_value"] = result
                if stdout:
                    task_desc["stdout"] = stdout
                if stderr:
                    task_desc["stderr"] = stderr
                self._callback_func(task_desc, "DONE")

    async def submit_tasks(self, tasks: list[dict]) -> None:
        """Submit tasks to the backend.

        :param tasks: List of task descriptions
        """
        if self._backend_state == BackendMainStates.SHUTDOWN:
            raise RuntimeError("Cannot submit during shutdown")

        # Set backend state to RUNNING when tasks are submitted
        if self._backend_state != BackendMainStates.RUNNING:
            self._backend_state = BackendMainStates.RUNNING
            self.logger.debug(f"Backend state set to: {self._backend_state.value}")

        # Capture event loop for cross-thread batch delivery
        if self._loop is None:
            self._loop = asyncio.get_running_loop()

        # Start monitor thread on first submission (lazy — avoids idle spin before tasks exist)
        if self._batch_monitor_thread is None or not self._batch_monitor_thread.is_alive():
            self._shutdown_event.clear()
            self._batch_monitor_thread = threading.Thread(
                target=self._monitor_loop, name="dragon_monitor_loop", daemon=True
            )
            self._batch_monitor_thread.start()

        # Build tasks
        batch_tasks_data = []
        for task in tasks:
            try:
                batch_task = await self.build_task(task)
                batch_tasks_data.append((task["uid"], batch_task))
            except Exception as e:
                self.logger.error(f"Failed to create task {task.get('uid')}: {e}", exc_info=True)
                task["exception"] = e
                self._callback_func(task, "FAILED")

        if not batch_tasks_data:
            return

        # Tasks are already in-flight — the Batch background thread auto-dispatches them
        # the moment they are created via batch.function()/process()/job().
        # Register each task individually for result monitoring.
        for uid, batch_task in batch_tasks_data:
            self._monitored_batches[batch_task.uid] = (batch_task, uid)
        self.logger.info(f"Submitted {len(batch_tasks_data)} tasks (streaming, auto-dispatched)")

    async def build_task(self, task: dict):
        """Translate AsyncFlow task to Dragon Batch task.

        Translation Priority (in order):
        1. If process_templates (list) provided → Job mode (ignore type='mpi', ignore ranks) [function/executable]
        2. If process_template (single) provided → Process mode [function/executable]
        3. If type='mpi' AND ranks provided (no templates) → Job mode (auto-build) [function/executable]
        4. If is_function (no templates, no MPI) → Function mode (native) [function only]
        5. If is_executable (no templates, no MPI) → Process mode (auto-build) [executable only]

        Execution Modes:
        - Function Native: batch.function() - direct Python function call
        - Function Process: batch.process() - function wrapped in ProcessTemplate
        - Function Job: batch.job() - function in MPI job with multiple ranks
        - Executable Process: batch.process() - single executable process
        - Executable Job: batch.job() - executable in MPI job with multiple ranks

        Setting cwd (working directory):
            Pass ``cwd`` inside ``process_template`` or each entry of ``process_templates``
            via ``task_backend_specific_kwargs``::

                ComputeTask(
                    function=my_func,
                    task_backend_specific_kwargs={
                        "process_template": {"cwd": "/path/to/dir"}
                    },
                )
        """
        # Fast path: extract everything upfront
        uid = task["uid"]
        is_function = bool(task.get("function"))
        target = task.get("function" if is_function else "executable")
        backend_kwargs = task.get("task_backend_specific_kwargs", {})
        name = task.get("name", uid)

        # For functions: use "args" and "kwargs"
        # For executables: use "arguments"
        if is_function:
            task_args = task.get("args", [])
            task_kwargs = task.get("kwargs", {})
        else:
            task_args = tuple(task.get("arguments", []))
            task_kwargs = None

        timeout = backend_kwargs.get("timeout", 1000000000.0)

        # Handle async functions
        if is_function and asyncio.iscoroutinefunction(target):
            original_target = target

            def target(*a, **kw):
                return asyncio.run(original_target(*a, **kw))

        # Get template configs once
        process_templates_config = backend_kwargs.get("process_templates")
        process_template_config = backend_kwargs.get("process_template")

        # Single decision tree - no redundant checks
        if process_templates_config is not None:
            # Priority 1: Job with user templates
            process_templates = [
                (
                    nranks,
                    ProcessTemplate(target, **{**tc, "args": task_args, "kwargs": task_kwargs}),
                )
                for nranks, tc in process_templates_config
            ]
            batch_task = self.batch.job(process_templates, name=name, timeout=timeout)
            execution_mode = "job"

        elif process_template_config is not None:
            # Priority 2: Process with user template
            batch_task = self.batch.process(
                ProcessTemplate(
                    target, **{**process_template_config, "args": task_args, "kwargs": task_kwargs}
                ),
                name=name,
                timeout=timeout,
            )
            execution_mode = "process"

        elif backend_kwargs.get("type") == "mpi":
            # Priority 3: Job auto-build
            batch_task = self.batch.job(
                [
                    (
                        backend_kwargs.get("ranks", 1),
                        ProcessTemplate(target, args=task_args, kwargs=task_kwargs),
                    )
                ],
                name=name,
                timeout=timeout,
            )
            execution_mode = "job"

        elif is_function:
            # Priority 4: Function native
            batch_task = self.batch.function(
                target, *task_args, name=name, timeout=timeout, **task_kwargs
            )
            execution_mode = "function"

        else:
            # Priority 5: Executable process auto-build
            batch_task = self.batch.process(
                ProcessTemplate(target, args=task_args, kwargs=task_kwargs),
                name=name,
                timeout=timeout,
            )
            execution_mode = "process"

        # Register and return
        self._task_registry[uid] = {
            "uid": uid,
            "description": task,
            "batch_task": batch_task,
        }

        self.logger.debug(f"Created {execution_mode} task: {uid}")

        return batch_task

    def link_implicit_data_deps(self, src_task, dst_task):
        pass

    async def state(self) -> str:
        """Get backend state.

        Returns:
            str: Current backend state (INITIALIZED, RUNNING, SHUTDOWN)
        """
        return self._backend_state.value

    def link_explicit_data_deps(self, src_task=None, dst_task=None, file_name=None, file_path=None):
        pass

    def task_state_cb(self, task: dict, state: str) -> None:
        self._callback_func(task, state)

    def get_task_states_map(self):
        return self._task_states

    async def cancel_task(self, uid: str) -> bool:
        if uid not in self._task_registry:
            raise ValueError(f"Task {uid} not found")

        # NOTE: dragon.batch does not expose nor support
        # process/function/job cancellation, we just notify
        # the asyncflow that the task is cancelled so not to block the flow
        task = self._task_registry[uid]["description"]
        self._callback_func(task, "CANCELED")
        self._cancelled_tasks.add(uid)

        return True

    async def shutdown(self) -> None:
        """Shutdown the backend and clean up resources."""
        if self._backend_state == BackendMainStates.SHUTDOWN:
            return

        # Set backend state to SHUTDOWN
        self._backend_state = BackendMainStates.SHUTDOWN
        self.logger.debug(f"Backend state set to: {self._backend_state.value}")
        self.logger.info("Shutting down V3 backend")
        self._shutdown_event.set()

        # Wait for monitor thread to finish if it exists
        if self._batch_monitor_thread and self._batch_monitor_thread.is_alive():
            try:
                self.logger.debug("Waiting for batch monitor thread to stop...")
                self._batch_monitor_thread.join(timeout=5.0)
                if self._batch_monitor_thread.is_alive():
                    self.logger.warning("Batch monitor thread did not stop within timeout")
            except Exception as e:
                self.logger.exception(f"Error stopping monitor thread: {e}")

        # Close Batch
        if self.batch:
            try:
                self.logger.debug("Closing batch...")
                self.batch.close()
                self.batch.join(timeout=10.0)
                self.logger.debug("Batch closed successfully")
            except Exception as e:
                self.logger.warning(f"Error closing batch gracefully: {e}")
                try:
                    self.logger.debug("Attempting to terminate batch...")
                    self.batch.terminate()
                except Exception as te:
                    self.logger.warning(f"Error terminating batch: {te}")

        self._task_registry.clear()
        self._state = "idle"
        self.logger.info("Shutdown V3 complete")

    # Batch features
    def fence(self):
        self.batch.fence()

    def create_ddict(self, *args, **kwargs):
        from dragon.data.ddict.ddict import DDict

        return DDict(*args, **kwargs)

    @classmethod
    async def create(
        cls,
        num_nodes: Optional[int] = None,
        pool_nodes: Optional[int] = None,
        disable_telemetry: bool = False,
    ):
        """Create and initialize a DragonExecutionBackendV3."""
        backend = cls(
            num_nodes=num_nodes,
            pool_nodes=pool_nodes,
            disable_telemetry=disable_telemetry,
        )
        return await backend


# ============================================================================
# ZMQ Worker — runs inside the Dragon head process (launched via dragon CLI)
# ============================================================================

class DragonExecutionBackendV3Worker(DragonExecutionBackendV3):
    """Dragon backend that receives tasks via ZMQ PULL and returns results via ZMQ PUSH.

    Inherits all Dragon Batch logic from DragonExecutionBackendV3.  Only the
    ZMQ transport layer is added on top.  Must be launched via the Dragon CLI
    (``dragon python -m rhapsody.backends.execution.dragon --worker ...``) so
    that ``DRAGON_MY_PUID`` is already set — ``_bootstrap_dragon_runtime()``
    becomes a no-op.
    """

    def __init__(
        self,
        endpoint_dir: str,
        num_nodes: Optional[int] = None,
        pool_nodes: Optional[int] = None,
        disable_telemetry: bool = False,
        name: str = "dragon",
    ):
        # Bind ZMQ and write endpoints.json BEFORE super().__init__() so the
        # client can connect while Batch() initialises across nodes (which can
        # take tens of seconds on multi-node).  Tasks sent in that window queue
        # up in ZMQ (HWM=0) and are drained once the task thread starts.
        self._endpoint_dir = endpoint_dir
        self._zmq_ctx = None
        self._task_socket = None
        self._result_socket = None
        self._zmq_ctrl_pub = None
        self._zmq_result_queue: queue.Queue = queue.Queue()
        self._zmq_task_thread: Optional[threading.Thread] = None
        self._zmq_result_thread: Optional[threading.Thread] = None
        self._bind_zmq_early()

        super().__init__(
            num_nodes=num_nodes,
            pool_nodes=pool_nodes,
            disable_telemetry=disable_telemetry,
            name=name,
        )

    def _bind_zmq_early(self) -> None:
        """Bind ZMQ sockets and write endpoints.json before Batch() is created."""
        try:
            import zmq
        except ImportError:
            raise ImportError("pyzmq is required for DragonExecutionBackendV3Worker. pip install pyzmq")

        multi_node = bool(os.environ.get("SLURM_JOB_ID") or os.environ.get("PBS_JOBID"))

        self._zmq_ctx = zmq.Context()
        self._zmq_ctrl_pub = self._zmq_ctx.socket(zmq.PAIR)
        self._zmq_ctrl_pub.bind("inproc://rhapsody-worker-ctrl")

        self._task_socket = self._zmq_ctx.socket(zmq.PULL)
        self._result_socket = self._zmq_ctx.socket(zmq.PUSH)
        self._task_socket.set_hwm(0)
        self._result_socket.set_hwm(0)

        if multi_node:
            task_port = self._task_socket.bind_to_random_port("tcp://0.0.0.0")
            result_port = self._result_socket.bind_to_random_port("tcp://0.0.0.0")
            fqdn = socket.getfqdn()
            endpoints = {
                "tasks": f"tcp://{fqdn}:{task_port}",
                "results": f"tcp://{fqdn}:{result_port}",
            }
        else:
            task_ep = f"ipc://{self._endpoint_dir}/tasks.ipc"
            result_ep = f"ipc://{self._endpoint_dir}/results.ipc"
            self._task_socket.bind(task_ep)
            self._result_socket.bind(result_ep)
            endpoints = {"tasks": task_ep, "results": result_ep}

        Path(self._endpoint_dir).mkdir(parents=True, exist_ok=True)
        with open(Path(self._endpoint_dir) / "endpoints.json", "w") as f:
            json.dump(endpoints, f)

        logging.getLogger(__name__).info(f"ZMQ worker bound — endpoints: {endpoints}")

    def __await__(self):
        return self._async_init().__await__()

    async def _async_init(self):
        await super()._async_init()
        self._loop = asyncio.get_running_loop()
        self._start_zmq_threads()
        # Install the ZMQ-forwarding wrapper.  The Session never registers a
        # callback on the Worker directly, so we do it here with a no-op inner
        # function.  register_callback wraps it to also push each result onto
        # _zmq_result_queue before calling the inner func.
        self.register_callback(lambda t, s: None)
        return self

    def _start_zmq_threads(self) -> None:
        """Start task/result threads. Sockets are already bound by _bind_zmq_early."""
        self._zmq_task_thread = threading.Thread(
            target=self._zmq_task_loop, name="dragon_zmq_tasks", daemon=True
        )
        self._zmq_result_thread = threading.Thread(
            target=self._zmq_result_loop, name="dragon_zmq_results", daemon=True
        )
        self._zmq_task_thread.start()
        self._zmq_result_thread.start()
        self.logger.info("ZMQ worker threads started")

    def register_callback(self, func: Callable) -> None:
        def _wrapped(task: dict, state: str) -> None:
            self._zmq_result_queue.put({
                "uid": task.get("uid"),
                "state": state,
                "return_value": task.get("return_value"),
                "stdout": task.get("stdout"),
                "stderr": task.get("stderr"),
                "exit_code": task.get("exit_code"),
                "exception": task.get("exception"),
            })
            func(task, state)
        super().register_callback(_wrapped)

    def _zmq_task_loop(self) -> None:
        import zmq
        import cloudpickle

        ctx = self._zmq_ctx
        ctrl = ctx.socket(zmq.PAIR)
        ctrl.connect("inproc://rhapsody-worker-ctrl")

        poller = zmq.Poller()
        poller.register(self._task_socket, zmq.POLLIN)
        poller.register(ctrl, zmq.POLLIN)

        while True:
            events = dict(poller.poll())
            if ctrl in events:
                break
            if self._task_socket in events:
                # Each message is a batch (list of task dicts); drain all queued messages.
                all_tasks = []
                while True:
                    try:
                        payload = cloudpickle.loads(self._task_socket.recv(zmq.NOBLOCK))
                        # Shutdown sentinel sent by the client
                        if isinstance(payload, dict) and payload.get("_rhapsody_cmd") == "shutdown":
                            self._shutdown_event.set()
                            ctrl.close()
                            return
                        all_tasks.extend(payload)  # payload is a list of task dicts
                    except zmq.Again:
                        break
                if all_tasks and self._loop is not None:
                    asyncio.run_coroutine_threadsafe(
                        super(DragonExecutionBackendV3Worker, self).submit_tasks(all_tasks),
                        self._loop,
                    )
        ctrl.close()

    def _zmq_result_loop(self) -> None:
        import cloudpickle

        while True:
            result = self._zmq_result_queue.get()
            if result is None:
                break
            try:
                self._result_socket.send(cloudpickle.dumps(result), flags=0)
            except Exception as e:
                self.logger.warning(f"ZMQ result send failed: {e}")

    async def shutdown(self) -> None:
        # Drain and stop ZMQ threads
        if self._zmq_ctrl_pub is not None:
            self._zmq_ctrl_pub.send(b"\x00")   # wake task loop
        self._zmq_result_queue.put(None)        # wake result loop
        if self._zmq_task_thread:
            self._zmq_task_thread.join(timeout=5)
        if self._zmq_result_thread:
            self._zmq_result_thread.join(timeout=5)
        if self._zmq_ctrl_pub:
            self._zmq_ctrl_pub.close()
        if self._task_socket:
            self._task_socket.close()
        if self._result_socket:
            self._result_socket.close()
        if self._zmq_ctx:
            self._zmq_ctx.term()
        await super().shutdown()


# ============================================================================
# ZMQ Client — runs in the user's clean process (no Dragon patches)
# ============================================================================

class DragonExecutionBackendV3Client(BaseBackend):
    """User-facing Dragon backend that runs outside the Dragon namespace.

    Launches a DragonExecutionBackendV3Worker subprocess via the ``dragon`` CLI
    and communicates with it over ZMQ PUSH/PULL sockets.  The user's process
    stays completely free of Dragon's multiprocessing patches, so
    ProcessPoolExecutor, Dask, and any other stdlib-based executor coexist
    without conflict.

    Usage::

        backend = await DragonExecutionBackendV3Client(
            num_nodes=4,
            launcher_args=["--wlm", "slurm", "--network-config", "/path/cfg.yaml"],
        )
        async with Session(backends=[backend, conc_b]):
            ...

    Run with ``python script.py`` (not ``dragon script.py``).
    """

    def __init__(
        self,
        num_nodes: Optional[int] = None,
        pool_nodes: Optional[int] = None,
        disable_telemetry: bool = False,
        name: str = "dragon",
        launcher_args: Optional[list] = None,
        coordination_dir: Optional[str] = None,
        startup_timeout: float = 300.0,
    ):
        super().__init__(name=name)
        self.logger = _get_logger()
        self._num_nodes = num_nodes
        self._pool_nodes = pool_nodes
        self._disable_telemetry = disable_telemetry
        self._launcher_args = launcher_args or []
        self._startup_timeout = startup_timeout
        self._coordination_dir = coordination_dir or os.path.join(
            os.path.expanduser("~"), ".rhapsody", "sessions", str(os.getpid())
        )
        self._worker_proc: Optional[subprocess.Popen] = None
        self._worker_log_out = None
        self._worker_log_err = None
        self._zmq_ctx = None
        self._task_socket = None
        self._result_socket = None
        self._zmq_ctrl_pub = None
        self._result_thread: Optional[threading.Thread] = None
        self._pending_tasks: dict[str, Any] = {}
        self._callback_func: Callable = lambda t, s: None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._initialized = False
        self._backend_state = BackendMainStates.INITIALIZED

    def __await__(self):
        return self._async_init().__await__()

    async def _async_init(self):
        if self._initialized:
            return self
        try:
            from ..constants import StateMapper
            StateMapper.register_backend_states_with_defaults(backend=self)
            StateMapper.register_backend_tasks_states_with_defaults(backend=self)

            self._loop = asyncio.get_running_loop()
            Path(self._coordination_dir).mkdir(parents=True, exist_ok=True)

            # Build dragon launch command.
            # Use "python -m rhapsody.backends.execution.dragon" so that:
            #   1. __package__ is set → relative imports work inside dragon.py
            #   2. the execution dir is not prepended to sys.path → no name collision
            #      between dragon.py and the dragon package.
            cmd = ["dragon"]
            if self._num_nodes is not None:
                cmd += ["-N", str(self._num_nodes)]
            cmd += self._launcher_args   # --wlm, --network-config, --transport, etc.
            cmd += [sys.executable, "-m", "rhapsody.backends.execution.dragon",
                    "--worker", "--endpoint-dir", self._coordination_dir]
            if self._pool_nodes is not None:
                cmd += ["--pool-nodes", str(self._pool_nodes)]
            if self._disable_telemetry:
                cmd += ["--disable-telemetry"]

            self.logger.info(f"Launching Dragon worker: {' '.join(cmd)}")
            # Redirect worker stdio to files — Dragon's SSH/HSTA writes to stdout/stderr
            # internally; if the parent process has None stdio (common in job launchers)
            # those writes crash with "'NoneType' object has no attribute 'write'".
            log_out = open(Path(self._coordination_dir) / "worker.stdout", "w")
            log_err = open(Path(self._coordination_dir) / "worker.stderr", "w")
            self._worker_log_out = log_out
            self._worker_log_err = log_err
            self.logger.info(
                f"Worker logs → {self._coordination_dir}/worker.{{stdout,stderr}}"
            )
            clean_env = {k: v for k, v in os.environ.items() if v is not None}
            self._worker_proc = subprocess.Popen(
                cmd, env=clean_env, stdout=log_out, stderr=log_err,
            )

            endpoints = await self._wait_for_endpoints()
            self._connect_zmq(endpoints)

            self._result_thread = threading.Thread(
                target=self._result_loop, name="dragon_client_results", daemon=True
            )
            self._result_thread.start()

            self._initialized = True
            self.logger.info("DragonExecutionBackendV3Client ready")
        except Exception:
            self.logger.exception("Client initialization failed")
            self._initialized = False
            raise
        return self

    async def _wait_for_endpoints(self) -> dict:
        endpoint_file = Path(self._coordination_dir) / "endpoints.json"
        deadline = time.monotonic() + self._startup_timeout
        while time.monotonic() < deadline:
            # Fail fast if the worker process already exited
            if self._worker_proc and self._worker_proc.poll() is not None:
                raise RuntimeError(
                    f"Dragon worker exited with code {self._worker_proc.returncode} "
                    f"before writing endpoints"
                )
            if endpoint_file.exists():
                try:
                    return json.loads(endpoint_file.read_text())
                except json.JSONDecodeError:
                    pass
            await asyncio.sleep(1.0)
        raise TimeoutError(
            f"Dragon worker did not write endpoints within {self._startup_timeout}s "
            f"(looked in {self._coordination_dir})"
        )

    def _connect_zmq(self, endpoints: dict) -> None:
        try:
            import zmq
        except ImportError:
            raise ImportError("pyzmq is required. pip install pyzmq")

        self._zmq_ctx = zmq.Context()

        self._zmq_ctrl_pub = self._zmq_ctx.socket(zmq.PAIR)
        self._zmq_ctrl_pub.bind("inproc://rhapsody-client-ctrl")

        self._task_socket = self._zmq_ctx.socket(zmq.PUSH)
        self._result_socket = self._zmq_ctx.socket(zmq.PULL)
        self._task_socket.set_hwm(0)
        self._result_socket.set_hwm(0)
        self._task_socket.connect(endpoints["tasks"])
        self._result_socket.connect(endpoints["results"])

    def register_callback(self, func: Callable) -> None:
        self._callback_func = func

    def get_task_states_map(self):
        from ..constants import StateMapper
        return StateMapper(backend=self)

    async def submit_tasks(self, tasks: list) -> None:
        import cloudpickle

        if self._backend_state != BackendMainStates.RUNNING:
            self._backend_state = BackendMainStates.RUNNING

        for task in tasks:
            self._pending_tasks[task["uid"]] = task
        # Send the entire batch as one message — one pickle, one ZMQ send.
        self._task_socket.send(cloudpickle.dumps(tasks), flags=1)  # NOBLOCK

    async def cancel_task(self, uid: str) -> bool:
        return self._pending_tasks.pop(uid, None) is not None

    def _result_loop(self) -> None:
        import zmq
        import cloudpickle

        ctx = self._zmq_ctx
        ctrl = ctx.socket(zmq.PAIR)
        ctrl.connect("inproc://rhapsody-client-ctrl")

        poller = zmq.Poller()
        poller.register(self._result_socket, zmq.POLLIN)
        poller.register(ctrl, zmq.POLLIN)

        while True:
            events = dict(poller.poll())
            if ctrl in events:
                break
            if self._result_socket in events:
                try:
                    result = cloudpickle.loads(self._result_socket.recv())
                    uid = result.get("uid")
                    task = self._pending_tasks.pop(uid, None)
                    if task is not None:
                        task.update({k: v for k, v in result.items() if k != "uid"})
                        self._loop.call_soon_threadsafe(
                            self._callback_func, task, result["state"]
                        )
                except Exception as e:
                    self.logger.warning(f"Result receive error: {e}")
        ctrl.close()

    async def state(self) -> str:
        return self._backend_state.value

    def build_task(self, uid, task_desc, task_specific_kwargs):
        pass

    def link_explicit_data_deps(self, src_task=None, dst_task=None, file_name=None, file_path=None):
        pass

    def link_implicit_data_deps(self, src_task, dst_task):
        pass

    def task_state_cb(self):
        pass

    async def shutdown(self) -> None:
        self._backend_state = BackendMainStates.SHUTDOWN

        # Tell the worker to shut down gracefully via a sentinel on the task socket.
        # The worker's task loop recognises {"_rhapsody_cmd": "shutdown"}, sets
        # _shutdown_event, and calls worker.shutdown() — letting Dragon exit cleanly
        # rather than being SIGTERMed (which triggers Dragon's abnormal-exit SSH cleanup).
        if self._task_socket is not None:
            try:
                import cloudpickle
                self._task_socket.send(
                    cloudpickle.dumps({"_rhapsody_cmd": "shutdown"}), flags=1
                )
            except Exception:
                pass

        # Stop the local result-receiver thread
        if self._zmq_ctrl_pub is not None:
            try:
                self._zmq_ctrl_pub.send(b"\x00")
            except Exception:
                pass
        if self._result_thread:
            self._result_thread.join(timeout=5)

        # Give the Dragon launcher a moment to receive the sentinel and begin its
        # own multi-node teardown, then detach.  Dragon's node cleanup (SSH/WLM)
        # can take 30-120s — blocking the user's process on it is unnecessary.
        if self._worker_proc and self._worker_proc.poll() is None:
            try:
                self._worker_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                # Still running — Dragon is doing its cleanup.  Detach and let it finish.
                self.logger.debug("Dragon launcher cleaning up nodes; detaching")

        # Close ZMQ after the worker is gone
        if self._zmq_ctrl_pub:
            self._zmq_ctrl_pub.close()
        if self._task_socket:
            self._task_socket.close()
        if self._result_socket:
            self._result_socket.close()
        if self._zmq_ctx:
            self._zmq_ctx.term()

        for fh in (self._worker_log_out, self._worker_log_err):
            try:
                if fh:
                    fh.close()
            except Exception:
                pass

        shutil.rmtree(self._coordination_dir, ignore_errors=True)
        self.logger.info("DragonExecutionBackendV3Client shutdown complete")

    async def __aenter__(self):
        if not self._initialized:
            await self._async_init()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.shutdown()


# ============================================================================
# Worker entry-point — invoked by dragon CLI: dragon python -m rhapsody.backends.execution.dragon --worker ...
# ============================================================================

def _worker_main(args) -> None:
    """Run the ZMQ worker inside the Dragon head process."""
    async def _run():
        worker = DragonExecutionBackendV3Worker(
            endpoint_dir=args.endpoint_dir,
            num_nodes=args.num_nodes,
            pool_nodes=args.pool_nodes,
            disable_telemetry=args.disable_telemetry,
        )
        await worker
        # Wait until shutdown — poll the threading event from async context
        loop = asyncio.get_running_loop()
        while not worker._shutdown_event.is_set():
            await asyncio.sleep(1)
        await worker.shutdown()

    asyncio.run(_run())


def main() -> None:
    parser = argparse.ArgumentParser(description="Rhapsody Dragon ZMQ worker")
    parser.add_argument("--worker", action="store_true",
                        help="Run as ZMQ worker inside Dragon head process")
    parser.add_argument("--endpoint-dir", default=None,
                        help="Directory for ZMQ endpoint coordination files")
    parser.add_argument("--num-nodes", type=int, default=None)
    parser.add_argument("--pool-nodes", type=int, default=None)
    parser.add_argument("--disable-telemetry", action="store_true", default=False)
    args = parser.parse_args()

    if args.worker:
        _worker_main(args)


if __name__ == "__main__":
    main()

