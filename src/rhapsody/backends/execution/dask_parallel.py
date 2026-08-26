"""Dask distributed execution backend for parallel and distributed computing.

This module provides a backend that executes tasks on Dask clusters, supporting both local and
distributed execution environments.

Execution model
----------------
Dask workers already natively distinguish sync and async callables: a submitted callable that
is a coroutine function runs directly on the worker's event loop, while a plain callable runs in
the worker's thread pool (see ``dask._task_spec.Task.is_coro`` /
``distributed.worker.Worker._maybe_deserialize_task``). Because of this, RHAPSODY submits
``task["function"]`` to Dask exactly as the caller defined it (optionally wrapped in
``functools.partial`` only to pre-bind keyword arguments) for *both* sync and async callables.
No RHAPSODY-side closure/adapter is used to "convert" an async callable into something Dask can
run — that used to be done via a `functools.wraps`-decorated local closure, which is exactly what
caused pickling failures (the closure's `__qualname__`/`__module__` were copied from the original
function, so pickling-by-reference resolved to a *different* object at that name and raised
``PicklingError: ... it's not the same object as ...``). Submitting the real callable directly
removes the synthetic function whose identity could ever mismatch.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import time
from dataclasses import dataclass
from functools import partial
from typing import Any
from typing import Callable

from rhapsody.api.errors import BackendError

from ..base import BaseBackend
from ..constants import BackendMainStates
from ..constants import StateMapper

try:
    import dask.distributed as dask
except ImportError:
    dask = None


def _get_logger() -> logging.Logger:
    """Get logger for dask backend module.

    This function provides lazy logger evaluation, ensuring the logger is created after the user has
    configured logging, not at module import time.
    """
    return logging.getLogger(__name__)


def _run_executable(
    executable: str,
    arguments: list[str],
    cwd: str | None = None,
    env: dict | None = None,
    shell: bool = False,
    capture_stdio: bool = False,
    output_dir: str | None = None,
    uid: str | None = None,
) -> tuple[str, str, int]:
    """Run a subprocess inside a Dask worker.

    Must be defined at module level for Dask pickling compatibility.

    Args:
        executable: Path to the executable.
        arguments: List of command-line arguments.
        cwd: Working directory for the subprocess.
        env: Environment variables dict. None inherits the worker environment.
        shell: Whether to execute through the shell.
        capture_stdio: If True, redirect stdout/stderr to files in output_dir.
        output_dir: Directory for output files when capture_stdio is True.
        uid: Task uid used to name output files when capture_stdio is True.

    Returns:
        Tuple of (stdout, stderr, returncode). When capture_stdio is True,
        stdout and stderr are file paths instead of decoded content.
    """
    import subprocess

    cmd = [executable] + list(arguments)
    shell_cmd = " ".join(cmd) if shell else cmd

    if capture_stdio and output_dir and uid:
        import os as _os

        stdout_path = _os.path.join(output_dir, f"{uid}.stdout")
        stderr_path = _os.path.join(output_dir, f"{uid}.stderr")
        with open(stdout_path, "wb") as out_f, open(stderr_path, "wb") as err_f:
            result = subprocess.run(
                shell_cmd, shell=shell, stdout=out_f, stderr=err_f, cwd=cwd, env=env
            )
        return stdout_path, stderr_path, result.returncode
    else:
        result = subprocess.run(shell_cmd, shell=shell, capture_output=True, cwd=cwd, env=env)
        return result.stdout.decode(), result.stderr.decode(), result.returncode


@dataclass
class _TaskRuntime:
    """Backend-private bookkeeping for a submitted task.

    Deliberately kept separate from the RHAPSODY task dict (``DaskExecutionBackend.tasks``), which
    is shared with and owned by the caller: nothing Dask/runtime-specific (the Dask ``Future``,
    submission bookkeeping) should be written onto that shared object.
    """

    uid: str
    kind: str  # "function" or "executable" — diagnostics only
    future: Any | None = None  # dask.distributed.Future, set once client.submit() succeeds
    submitted_at: float = 0.0


class DaskExecutionBackend(BaseBackend):
    """A Dask execution backend for distributed task execution.

    Handles task submission, cancellation, and proper async event loop handling
    for distributed task execution using Dask. Supports async functions, sync
    functions, and executable tasks.

    Client/cluster ownership: if the caller supplies ``client``, this backend never closes it
    (``shutdown()`` leaves it open). If the caller supplies only ``cluster``, this backend creates
    and owns a ``Client`` for it (closed on ``shutdown()``) but never touches the caller's cluster.
    If neither is supplied, this backend creates and owns both.

    Usage:
        backend = await DaskExecutionBackend(resources)
        # or
        async with DaskExecutionBackend(resources) as backend:
            await backend.submit_tasks(tasks)
    """

    def __init__(
        self,
        resources: dict | None = None,
        name: str = "dask",
        cluster: Any | None = None,
        client: Any | None = None,
    ):
        """Initialize the Dask execution backend (non-async setup only).

        Args:
            resources: Dictionary of resource requirements for tasks. Contains
                configuration parameters for the Dask client initialization.
            name: Name of the backend.
            cluster: Optional preconfigured Dask Cluster object. Not closed by
                shutdown() — the caller retains ownership.
            client: Optional preconfigured Dask Client object, which must have been
                created with asynchronous=True. Not closed by shutdown() — the
                caller retains ownership.
        """

        if dask is None:
            raise ImportError("Dask is required for DaskExecutionBackend.")

        super().__init__(name=name)

        self.logger = _get_logger()
        self.tasks: dict[str, dict[str, Any]] = {}
        self._runtime: dict[str, _TaskRuntime] = {}
        self._client = None
        self._callback_func: Callable = lambda t, s: None
        self._resources = resources or {}
        self._cluster_provided = cluster
        self._client_provided = client
        # Ownership: only close resources this backend created itself.
        self._owns_client = client is None
        self._initialized = False
        self._backend_state = BackendMainStates.INITIALIZED

    def __await__(self):
        """Make DaskExecutionBackend awaitable like Dask Client."""
        return self._async_init().__await__()

    async def _async_init(self):
        """Unified async initialization with backend and task state registration.

        Pattern:
        1. Register backend states first
        2. Register task states
        3. Set backend state to INITIALIZED
        4. Initialize backend components
        """
        if not self._initialized:
            try:
                # Step 1: Register backend states
                self.logger.debug("Registering backend states...")
                StateMapper.register_backend_states_with_defaults(backend=self)

                # Step 2: Register task states
                self.logger.debug("Registering task states...")
                StateMapper.register_backend_tasks_states_with_defaults(backend=self)

                # Step 3: Set backend state to INITIALIZED
                self._backend_state = BackendMainStates.INITIALIZED
                self.logger.debug(f"Backend state set to: {self._backend_state.value}")

                # Step 4: Initialize backend components
                await self._initialize()
                self._initialized = True

                self.logger.info("Dask backend fully initialized and ready")

            except Exception as e:
                self.logger.exception(f"Dask backend initialization failed: {e}")
                self._initialized = False
                raise
        return self

    async def _initialize(self) -> None:
        """Initialize the Dask client and set up worker environments.

        Raises:
            ValueError: If an externally-provided client is not asynchronous.
            Exception: If Dask client initialization fails.
        """
        try:
            if self._client_provided is not None:
                if not getattr(self._client_provided, "asynchronous", False):
                    raise ValueError(
                        "DaskExecutionBackend requires an externally-provided Client to be "
                        "created with asynchronous=True (e.g. Client(..., asynchronous=True)). "
                        f"Got a client with asynchronous="
                        f"{getattr(self._client_provided, 'asynchronous', None)!r}."
                    )
                self._client = self._client_provided
            elif self._cluster_provided is not None:
                self._client = await dask.Client(
                    self._cluster_provided, asynchronous=True, **self._resources
                )
            else:
                self._client = await dask.Client(asynchronous=True, **self._resources)

            dashboard_link = self._client.dashboard_link
            self.logger.info(f"Dask backend initialized with dashboard at {dashboard_link}")
        except Exception as e:
            self.logger.exception(f"Failed to initialize Dask client: {str(e)}")
            raise

    def register_callback(self, func: Callable) -> None:
        """Register a callback for task state changes.

        Args:
            func: Function to be called when task states change. Should accept
                task and state parameters. May be sync or async.
        """
        self._callback_func = func

    def get_task_states_map(self) -> StateMapper:
        """Retrieve a mapping of task IDs to their current states.

        Returns:
            StateMapper: Object containing the mapping of task states for this backend.
        """
        return StateMapper(backend=self)

    async def cancel_task(self, uid: str) -> bool:
        """Cancel a task by its UID.

        Args:
            uid (str): The UID of the task to cancel.

        Returns:
            bool: True if the task was found (still tracked, with a submitted Dask
            future) and cancellation was attempted, False for an unknown uid, a task
            that hasn't been submitted to Dask yet, or one that already reached a
            terminal state (and was purged from tracking).
        """
        self._ensure_initialized()
        runtime = self._runtime.get(uid)
        if runtime is None or runtime.future is None:
            return False
        try:
            await runtime.future.cancel()
        except Exception:
            self.logger.exception(f"Error cancelling task '{uid}'")
            return False
        return True

    async def submit_tasks(self, tasks: list[dict[str, Any]]) -> None:
        """Submit tasks to the Dask cluster.

        Dispatches each task to the appropriate submission method based on its type:
        executable tasks run via subprocess, function tasks (sync or async) are
        submitted directly to Dask workers.

        Args:
            tasks: List of task dictionaries containing:
                - uid: Unique task identifier
                - function: Callable to execute (sync or async)
                - args: Positional arguments
                - kwargs: Keyword arguments
                - executable: Path to executable (mutually exclusive with function)
                - arguments: CLI arguments for executable tasks
                - task_backend_specific_kwargs: Passed directly to client.submit()

        Raises:
            BackendError: If the Dask client is not in a usable state (the whole
                backend is unavailable — this is not attributable to any one task).
            ValueError: If a task specifies neither 'function' nor 'executable'.
        """
        self._ensure_initialized()

        if self._client is None or self._client.status != "running":
            raise BackendError(
                self.name,
                f"Dask client is not usable (status={getattr(self._client, 'status', None)!r})",
            )

        if self._backend_state != BackendMainStates.RUNNING:
            self._backend_state = BackendMainStates.RUNNING
            self.logger.debug(f"Backend state set to: {self._backend_state.value}")

        for task in tasks:
            is_func_task = bool(task.get("function"))
            is_exec_task = bool(task.get("executable"))
            if not is_func_task and not is_exec_task:
                raise ValueError("Task must specify either 'function' or 'executable'")

            uid = task["uid"]
            self.tasks[uid] = task
            self._runtime[uid] = _TaskRuntime(
                uid=uid,
                kind="executable" if is_exec_task else "function",
                submitted_at=time.monotonic(),
            )

            if is_exec_task:
                await self._submit_executable(task)
            else:
                await self._submit_function(task)

    async def _submit_and_track(
        self,
        task: dict[str, Any],
        fn: Callable,
        args: tuple,
        *,
        kind: str,
        fn_kwargs: dict[str, Any] | None = None,
    ) -> None:
        """Submit ``fn(*args, **fn_kwargs)`` to Dask and schedule its completion handler.

        This is the single low-level submission primitive shared by function and
        executable tasks. ``fn``/``args``/``fn_kwargs`` are the user-visible callable
        and its arguments; Dask submission options (resources, key, retries, ...) come
        only from ``task["task_backend_specific_kwargs"]``, kept strictly separate so a
        user's own function kwarg can never collide with a Dask-reserved submit() kwarg.

        Args:
            task: The RHAPSODY task dictionary (mutated in place with result/state fields).
            fn: The callable to submit (never a synthetic wrapper — see module docstring).
            args: Positional arguments to submit alongside ``fn``.
            kind: "function" or "executable" — controls result-shape handling in `_on_done`.
            fn_kwargs: Keyword arguments to submit alongside ``fn`` (executable path only;
                function-task kwargs are pre-bound via `functools.partial` by the caller).
        """
        uid = task["uid"]
        submit_kwargs = dict(task.get("task_backend_specific_kwargs", {}))
        if kind == "executable":
            submit_kwargs = {
                k: v for k, v in submit_kwargs.items() if k not in ("cwd", "shell", "env")
            }

        dask_resources = submit_kwargs.get("resources", {})
        if dask_resources and not await self._check_resources_satisfiable(dask_resources):
            task["exception"] = RuntimeError(
                f"No worker can satisfy resources {dask_resources}. "
                f"Workers must be started with matching --resources flags "
                f'(e.g. dask worker <scheduler> --resources "GPU=1").'
            )
            task["stdout"] = ""
            task["stderr"] = str(task["exception"])
            task["exit_code"] = 1
            await self._invoke_callback(task, "FAILED")
            self._purge(uid)
            return

        # Use the RHAPSODY uid as the Dask key unless the caller explicitly set one.
        # Without this, client.submit()'s default (tokenize(func, kwargs, *args) under
        # pure=True) can make two distinct tasks that call the same function with the
        # same arguments collide onto the same Dask key, silently sharing one Future.
        submit_kwargs.setdefault("key", uid)

        try:
            dask_future = self._client.submit(fn, *args, **(fn_kwargs or {}), **submit_kwargs)
        except Exception as e:
            task["exception"] = e
            task["stdout"] = ""
            task["stderr"] = str(e)
            await self._invoke_callback(task, "FAILED")
            self._purge(uid)
            return

        self._runtime[uid].future = dask_future
        asyncio.create_task(self._on_done(task, dask_future, kind))

    async def _on_done(self, task: dict[str, Any], f: Any, kind: str) -> None:
        """Shared completion handler for both function and executable Dask futures."""
        uid = task["uid"]
        try:
            await self._invoke_callback(task, "RUNNING")
            if kind == "executable":
                stdout, stderr, returncode = await f
                task["stdout"] = stdout
                task["stderr"] = stderr
                task["exit_code"] = returncode
                state = "DONE" if returncode == 0 else "FAILED"
            else:
                result = await f
                task["return_value"] = result
                task["stdout"] = ""
                task["stderr"] = ""
                state = "DONE"
            await self._invoke_callback(task, state)
        except dask.client.FutureCancelledError:
            await self._invoke_callback(task, "CANCELED")
        except Exception as e:
            task["exception"] = e
            task["stdout"] = ""
            task["stderr"] = str(e)
            await self._invoke_callback(task, "FAILED")
        finally:
            self._purge(uid)

    async def _invoke_callback(self, task: dict[str, Any], state: str) -> None:
        """Invoke the registered callback, tolerating both sync and async callables.

        A callback failure is caught and logged here so it can never corrupt task state or interrupt
        the completion handler that called it.
        """
        try:
            result = self._callback_func(task, state)
            if inspect.isawaitable(result):
                await result
        except Exception:
            self.logger.exception(
                f"Callback raised while handling state '{state}' for task '{task.get('uid')}'"
            )

    def _purge(self, uid: str) -> None:
        """Drop the backend-internal record for a task that has reached a terminal state.

        Safe to call once the terminal-state callback has already been invoked for this
        uid: results live on the original task dict, which the caller retains
        independently of this backend's own bookkeeping, and the callback has already
        resolved any waiter before this runs. After this, `cancel_task(uid)` returns
        False and `uid not in self.tasks`.
        """
        self.tasks.pop(uid, None)
        self._runtime.pop(uid, None)

    async def _submit_function(self, task: dict[str, Any]) -> None:
        """Submit a Python callable (sync or async) directly to Dask.

        Dask's own worker executor detects whether the submitted callable is a
        coroutine function and runs it on the worker's event loop if so, or in the
        worker's thread pool otherwise — no RHAPSODY-side adapter is needed for
        either case (see module docstring). The callable is submitted exactly as the
        caller defined it, optionally pre-bound via `functools.partial` for kwargs;
        it is never renamed or re-wrapped, so it can never claim an identity that
        doesn't match what pickling finds at that name.

        Args:
            task: Task dictionary containing the function and its parameters.
        """
        fn = task["function"]
        kwargs = task.get("kwargs") or {}
        if kwargs:
            # Bound via partial (not passed as client.submit(**kwargs)) because
            # client.submit reserves kwarg names like "resources"/"retries"/"key" for
            # its own submission options; a user function kwarg with the same name
            # must never collide with those.
            fn = partial(fn, **kwargs)
        args = tuple(task.get("args") or ())
        await self._submit_and_track(task, fn, args, kind="function")

    async def _submit_executable(self, task: dict[str, Any]) -> None:
        """Submit an executable task to run via subprocess inside a Dask worker.

        Args:
            task: Task dictionary containing executable path, arguments, and metadata.
        """
        bksp = task.get("task_backend_specific_kwargs", {})
        fn_kwargs = {
            "cwd": bksp.get("cwd"),
            "env": bksp.get("env"),
            "shell": bksp.get("shell", False),
            "capture_stdio": task.get("capture_stdio", False),
            "output_dir": self._work_dir,
            "uid": task["uid"],
        }
        args = (task["executable"], task.get("arguments", []))
        await self._submit_and_track(
            task, _run_executable, args, kind="executable", fn_kwargs=fn_kwargs
        )

    async def _check_resources_satisfiable(self, resources: dict) -> bool:
        """Return True if at least one connected worker can satisfy all resource constraints.

        Uses `client.scheduler.identity()` rather than `client.scheduler_info()`: for an
        asynchronous Client, `scheduler_info()` returns a cached snapshot captured once
        at connect time with an always-empty "workers" mapping (see its own docstring,
        which recommends this exact alternative), so it can never see workers that
        joined afterward or their resource advertisements.

        Args:
            resources: Dict of resource requirements (e.g. {"GPU": 1}).

        Returns:
            True if a qualifying worker exists, False otherwise.
        """
        info = await self._client.scheduler.identity(n_workers=-1)
        workers = info.get("workers", {})
        return any(
            all(w.get("resources", {}).get(k, 0) >= v for k, v in resources.items())
            for w in workers.values()
        )

    async def cancel_all_tasks(self) -> int:
        """Cancel all currently running/pending tasks.

        Returns:
            Number of tasks that were successfully cancelled
        """
        self._ensure_initialized()
        cancelled_count = 0
        task_uids = list(self.tasks.keys())

        for task_uid in task_uids:
            if await self.cancel_task(task_uid):
                cancelled_count += 1

        return cancelled_count

    def link_explicit_data_deps(
        self,
        src_task: dict[str, Any] | None = None,
        dst_task: dict[str, Any] | None = None,
        file_name: str | None = None,
        file_path: str | None = None,
    ) -> None:
        """Intentional no-op: Dask has no file-staging directive analogous to this hook.

        RHAPSODY's other backends that implement this (e.g. RADICAL-Pilot) use
        backend-native staging/transfer directives executed before a task runs. Dask
        has no equivalent at the flat `submit_tasks(list)` batch-submission level used
        here; Dask's native dependency mechanism is instead passing one task's Future
        as an argument to another `client.submit()` call, which requires a task-graph
        API this module doesn't currently expose. Should RHAPSODY grow an explicit
        dependency-graph API, this backend should wire dependencies that way rather
        than via file staging.

        Args:
            src_task: The source task that produces the dependency.
            dst_task: The destination task that depends on the source.
            file_name: Name of the file that represents the dependency.
            file_path: Full path to the file that represents the dependency.
        """
        pass

    def link_implicit_data_deps(self, src_task: dict[str, Any], dst_task: dict[str, Any]) -> None:
        """Intentional no-op — see `link_explicit_data_deps` for rationale.

        Args:
            src_task: The source task that produces data.
            dst_task: The destination task that depends on the source task's output.
        """
        pass

    async def state(self) -> str:
        """Get backend state.

        Returns:
            str: Current backend state (INITIALIZED, RUNNING, SHUTDOWN)
        """
        return self._backend_state.value

    async def task_state_cb(self, task: dict, state: str) -> None:
        """Callback function invoked when a task's state changes.

        Intentional no-op: state notification is delivered through the callback
        registered via `register_callback` (invoked from `_on_done`/`_invoke_callback`),
        matching the convention used by every sibling backend.

        Args:
            task: Dictionary containing task information and metadata.
            state: The new state of the task.
        """
        pass

    async def build_task(self, task: dict) -> None:
        """Intentional no-op: Dask has no separate "build description, then submit" phase.

        Task construction happens inline during `submit_tasks`/`_submit_function`/
        `_submit_executable` — there is no intermediate native task-description object
        to build ahead of time the way e.g. RADICAL-Pilot's `TaskDescription` requires.

        Args:
            task: Dictionary containing task definition, parameters, and metadata
                required for task construction.
        """
        pass

    async def shutdown(self) -> None:
        """Shutdown the Dask client and clean up resources.

        Cancels all outstanding tasks unconditionally. Closes the Dask client only if this backend
        created it (see class docstring on ownership); an externally provided client/cluster is left
        open for the caller to manage.
        """
        # Set backend state to SHUTDOWN
        self._backend_state = BackendMainStates.SHUTDOWN
        self.logger.debug(f"Backend state set to: {self._backend_state.value}")

        if self._client is not None:
            try:
                # Cancel all running tasks first, regardless of ownership.
                await self.cancel_all_tasks()

                if self._owns_client:
                    await self._client.close()
                    self.logger.info("Dask client shutdown complete")
                else:
                    self.logger.info(
                        "Externally-provided Dask client left open (not owned by this backend)"
                    )
            except Exception as e:
                self.logger.exception(f"Error during shutdown: {str(e)}")
            finally:
                self._client = None
                self.logger.info("Dask execution backend shutdown complete")

        # Always clean up state regardless of client presence
        self.tasks.clear()
        self._runtime.clear()
        self._initialized = False

    def _ensure_initialized(self):
        """Ensure the backend has been properly initialized."""
        if not self._initialized:
            raise RuntimeError(
                "DaskExecutionBackend must be awaited before use. "
                "Use: backend = await DaskExecutionBackend(resources)"
            )

    async def __aenter__(self):
        """Async context manager entry."""
        if not self._initialized:
            await self._async_init()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.shutdown()

    @classmethod
    async def create(
        cls,
        resources: dict | None = None,
        name: str = "dask",
        cluster: Any | None = None,
        client: Any | None = None,
    ) -> DaskExecutionBackend:
        """Alternative factory method for creating initialized backend.

        Args:
            resources: Configuration parameters for Dask client initialization.
            name: Name of the backend.
            cluster: Optional preconfigured Dask Cluster object.
            client: Optional preconfigured Dask Client object.

        Returns:
            Fully initialized DaskExecutionBackend instance.
        """
        backend = cls(resources=resources, name=name, cluster=cluster, client=client)
        return await backend
