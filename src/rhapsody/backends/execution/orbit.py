"""Orbit execution backend for remote task execution via ORBIT.

This module provides a backend that submits tasks to a remote HPC node
through the ORBIT bridge/plugin infrastructure.  The Endpoint node
runs a Rhapsody plugin with a local backend (e.g. Dragon V3) that
actually executes the work.

Internally delegates to ``RhapsodyClient`` so all transport-level
optimizations (template compression, pipelined batching, SSE-based
wait, batch notifications) are inherited automatically.
"""

import asyncio
import logging
from typing import Any
from typing import Callable

from ..base import BaseBackend
from ..constants import BackendMainStates
from ..constants import StateMapper

# ``radical.orbit`` is imported at module level so that tests can patch
# ``BridgeClient`` here.  When the package isn't installed we keep
# ``BridgeClient = None`` and remember the original ImportError; the
# actual chained re-raise happens in ``OrbitExecutionBackend.__init__``
# so the user sees the real cause (e.g. a downstream import failure
# inside ``radical.orbit`` itself, not just "package missing").
try:
    from radical.orbit import BridgeClient

    _radical_orbit_import_error = None
except ImportError as exc:
    BridgeClient = None
    _radical_orbit_import_error = exc

try:
    import radical.prof as rprof
except ImportError:
    rprof = None


class OrbitExecutionBackend(BaseBackend):
    """Execution backend that delegates to a remote ORBIT node.

    Uses ``radical.orbit.BridgeClient`` and ``RhapsodyClient`` for all
    communication — inheriting batching, template compression,
    pipelined submission, and SSE-based notifications.

    When tasks are submitted individually (one at a time), a batching
    layer collects them over a short time window (default 0.1 s) and
    flushes them as a single bulk request, dramatically reducing
    per-task HTTP round-trip overhead.

    Args:
        bridge_url:    URL of the ORBIT bridge
                       (e.g. ``"https://localhost:8000"``).  If omitted,
                       falls back to the ``RADICAL_ORBIT_BRIDGE_URL`` env var
                       (resolved by ``BridgeClient`` itself).
        endpoint_name:     Name of the endpoint to target.  If omitted, the
                       backend auto-selects the first connected endpoint
                       that advertises an enabled rhapsody plugin
                       (the synthetic ``'bridge'`` endpoint is always
                       skipped).  Raises ``RuntimeError`` from
                       ``await backend`` if no candidate is found.
        backends:      Backend names to request on the remote session
                       (default ``["dragon_v3"]``).
        name:          Backend name for Rhapsody registration
                       (default ``"orbit"``).
        batch_window:  Seconds to collect tasks before flushing
                       (default 0.25).  Set to 0 to disable batching.
        batch_limit:   Max tasks per batch — triggers an immediate flush
                       when reached (default 1024).
        notify_batch_window:  Endpoint-side notification batch window
                       (seconds).  ``None`` uses server default.
        notify_batch_size:    Endpoint-side notification batch size.
                       ``None`` uses server default.
    """

    _DEFAULT_BATCH_WINDOW = 0.25
    _ENDPOINTS_TO_SKIP = ["bridge"]
    _PLUGIN_NAME = "rhapsody"
    _TERMINAL_STATES = frozenset({"DONE", "FAILED", "CANCELED", "COMPLETED"})

    def __init__(
        self,
        bridge_url: str | None = None,
        endpoint_name: str | None = None,
        backends: list[str] | None = None,
        name: str = "orbit",
        plugin_name: str = _PLUGIN_NAME,
        batch_window: float | None = None,
        batch_limit: int = 1024,
        notify_batch_window: float | None = None,
        notify_batch_size: int | None = None,
    ):
        super().__init__(name=name)

        if BridgeClient is None:
            raise ImportError(
                f"OrbitExecutionBackend: cannot import radical.orbit: {_radical_orbit_import_error}"
            ) from _radical_orbit_import_error

        self.logger = logging.getLogger(__name__)
        self._bridge_url = bridge_url
        self._endpoint_name = endpoint_name
        self._plugin_name = plugin_name
        self._remote_backends = backends or ["dragon_v3"]
        self._notify_batch_window = notify_batch_window
        self._notify_batch_size = notify_batch_size
        self._endpoint_python_mm: tuple | None = None
        self._endpoint_python_lookup_done = False

        self._bc = None  # BridgeClient
        self._rh = None  # RhapsodyClient (from get_rhapsody_handle)
        self._tasks: dict[str, dict] = {}
        # uids for which a terminal state callback has already fired, so the
        # SSE and local-cancel paths don't double-fire the same terminal state
        self._terminal_fired: set[str] = set()

        self._callback_func: Callable = lambda t, s: None
        self._initialized = False
        self._backend_state = BackendMainStates.INITIALIZED
        self._loop: asyncio.AbstractEventLoop | None = None

        # -- submission batching --
        self._batch_window = (
            batch_window if batch_window is not None else self._DEFAULT_BATCH_WINDOW
        )
        self._batch_limit = batch_limit
        self._batch_buffer: list[dict] = []
        self._batch_lock = asyncio.Lock()
        self._flush_handle: asyncio.TimerHandle | None = None

        # -- profiling --
        self._prof = rprof.Profiler("client.task", ns="radical.orbit") if rprof else None

    # ------------------------------------------------------------------
    # Async init
    # ------------------------------------------------------------------

    def __await__(self):
        return self._async_init().__await__()

    async def _async_init(self):
        if self._initialized:
            return self

        self._loop = asyncio.get_running_loop()

        # Register states
        StateMapper.register_backend_states_with_defaults(backend=self)
        StateMapper.register_backend_tasks_states_with_defaults(backend=self)

        # Runs blocking network I/O (BridgeClient, endpoint/plugin queries),
        # so keep it off the event loop.
        self._rh = await asyncio.to_thread(self._get_rhapsody_handle)

        # Register persistent notification callback for task completions
        self._rh.register_notification_callback(self._on_task_notification, topic="task_status")
        self._rh.register_notification_callback(
            self._on_task_notification, topic="task_status_batch"
        )

        self._initialized = True
        self.logger.info(
            "Orbit backend ready: %s/%s (session %s)",
            self._endpoint_name,
            self._plugin_name,
            self._rh.sid,
        )
        return self

    # ------------------------------------------------------------------
    # Notification handling
    # ------------------------------------------------------------------

    def _on_task_notification(self, endpoint, plugin, topic, data):
        """SSE callback: update local tasks and fire Rhapsody callback."""
        if topic == "task_status_batch":
            for t in data.get("tasks", []):
                self._apply_task_update(t)
        else:
            self._apply_task_update(data)

    def _apply_task_update(self, body: dict):
        """Apply a single task status update from SSE."""
        if not body:
            return

        uid = body.get("uid")
        state = body.get("state", "")

        if uid not in self._tasks:
            return

        task = self._tasks[uid]

        # Decode base64-encoded return values (bytes results)
        if body.get("_return_value_encoding") == "base64":
            import base64

            body["return_value"] = base64.b64decode(body["return_value"])
            del body["_return_value_encoding"]

        # A remote failure may serialize its exception as a string or dict;
        # coerce it to a real exception so session.py raises it instead of
        # silently treating the failed task as successful.
        exc = body.get("exception", body.get("error"))
        if exc is not None and not isinstance(exc, BaseException):
            body["exception"] = RuntimeError(str(exc))

        # Update local task dict with remote results
        for key in (
            "state",
            "stdout",
            "stderr",
            "exit_code",
            "return_value",
            "exception",
            "traceback",
            "error",
        ):
            if key in body:
                task[key] = body[key]

        # Profile task completion on client side
        if self._prof:
            self._prof.prof("task_complete", uid=uid, state=state)

        # Fire Rhapsody state callback (thread-safe)
        if self._loop and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self._fire_callback, task, state)

    def _fire_callback(self, task: dict, state: str) -> None:
        """Invoke the Rhapsody state callback, at most once per terminal state.

        Both the SSE notification path and the local cancel path can report
        the same terminal state for a task (e.g. a cancel fires the callback
        locally *and* the endpoint emits a matching CANCELED notification).
        This guard ensures consumers that resolve a future on the terminal
        callback see it exactly once.

        Must run on the event loop thread — ``_terminal_fired`` is accessed
        without a lock, and every caller (SSE via ``call_soon_threadsafe``,
        cancel paths from coroutine bodies) already runs there.
        """
        uid = task.get("uid")
        if uid and str(state).upper() in self._TERMINAL_STATES:
            if uid in self._terminal_fired:
                return
            self._terminal_fired.add(uid)
        self._callback_func(task, state)

    # ------------------------------------------------------------------
    # Endpoint auto-selection and Plugin retrieval
    # ------------------------------------------------------------------

    def _get_rhapsody_handle(self) -> Any:
        """Either create an RhapsodyClient for the named endpoint, or pick the first endpoint that
        advertises an enabled rhapsody plugin.

        Plugins hosted by the
        Bridge are skipped.
        Raises ``RuntimeError`` if no candidate is found.
        """

        self._bc = BridgeClient(url=self._bridge_url)
        self._bridge_url = self._bc.url

        # find a suitable endpoint and load rhapsody plugin
        if not self._endpoint_name:
            for eid in self._bc.list_endpoints():
                if eid in self._ENDPOINTS_TO_SKIP:
                    continue

                # An offline or misbehaving endpoint must not abort the whole
                # auto-selection scan — skip it and keep probing the rest.
                try:
                    plugins = self._bc.get_endpoint_client(eid).list_plugins()
                    info = plugins.get(self._plugin_name)
                    if info and info.get("enabled"):
                        self.logger.info(
                            "auto-selected endpoint %r (plugin %r)", eid, self._plugin_name
                        )
                        self._endpoint_name = eid
                        break
                except Exception as exc:
                    self.logger.debug("failed to query plugins on endpoint %r: %s", eid, exc)

        if not self._endpoint_name:
            raise RuntimeError(
                f"no endpoint advertises an enabled {self._plugin_name!r} plugin "
                f"on bridge {self._bridge_url}"
            )

        ec = self._bc.get_endpoint_client(self._endpoint_name)

        kwargs = {"backends": self._remote_backends}
        if self._notify_batch_window is not None:
            kwargs["notify_batch_window"] = self._notify_batch_window
        if self._notify_batch_size is not None:
            kwargs["notify_batch_size"] = self._notify_batch_size

        return ec.get_plugin(self._plugin_name, **kwargs)

    # ------------------------------------------------------------------
    # Python-version compatibility for cloudpickled function tasks
    # ------------------------------------------------------------------

    async def _check_python_compat(self, tasks: list) -> None:
        """Raise if any cloudpickled task in *tasks* would hit a Python version mismatch on the
        remote endpoint.  No check is performed if the batch contains only executable or import-path
        tasks, or if the endpoint's Python version could not be determined.

        Cloudpickle serializes function bytecode using ``CodeType``,
        whose tuple shape changed between Python 3.10 and 3.11 — any
        cross-minor-version skew fails deserialization on the endpoint.
        """
        import sys

        def needs_compat(t: dict) -> bool:
            fn = t.get("function")
            return (isinstance(fn, str) and fn.startswith("cloudpickle::")) or bool(
                t.get("_pickled_fields")
            )

        if not any(needs_compat(t) for t in tasks):
            return

        if not self._endpoint_python_lookup_done:
            # Only the remote lookup is transient (network / plugin not yet
            # ready) — on failure we leave the check armed so a later
            # submission retries.  A successful lookup settles the check for
            # good, even if the reported version can't be parsed (that's
            # deterministic and retrying wouldn't help).
            info = None

            def _lookup():
                ec = self._bc.get_endpoint_client(self._endpoint_name)
                return ec.get_plugin("sysinfo").host_role()

            try:
                # Blocking network I/O — run off the event loop.
                info = await asyncio.to_thread(_lookup)
            except Exception as exc:
                self.logger.debug(f"pickle compat lookup failed, will retry: {exc}")

            if info is not None:
                self._endpoint_python_lookup_done = True
                pyver = info.get("python_version") or ""
                parts = pyver.split(".")
                try:
                    if len(parts) >= 2:
                        self._endpoint_python_mm = (int(parts[0]), int(parts[1]))
                except ValueError:
                    pass
                if not self._endpoint_python_mm:
                    self.logger.debug(f"skip pickle compat check ({pyver})")

        if not self._endpoint_python_mm:
            return

        client_mm = (sys.version_info.major, sys.version_info.minor)
        if self._endpoint_python_mm != client_mm:
            raise RuntimeError(
                f"function tasks cannot be submitted to endpoint "
                f"{self._endpoint_name!r}: client Python "
                f"{client_mm[0]}.{client_mm[1]} != endpoint Python "
                f"{self._endpoint_python_mm[0]}.{self._endpoint_python_mm[1]}.  "
                f"cloudpickle is not portable across Python minor "
                f"versions — align the venvs, or use executable / "
                f"import-path tasks for this endpoint."
            )

    # ------------------------------------------------------------------
    # BaseBackend interface
    # ------------------------------------------------------------------

    async def submit_tasks(self, tasks: list[dict[str, Any]]) -> None:
        """Submit tasks to the remote endpoint for execution.

        When batching is enabled (batch_window > 0), tasks are collected
        in an internal buffer and flushed after the window expires or the
        buffer reaches ``batch_limit``.  This turns many small individual
        submissions into fewer bulk HTTP requests.

        When batching is disabled (batch_window == 0), delegates directly
        to ``RhapsodyClient.submit_tasks()``.
        """
        if not self._initialized:
            await self._async_init()

        if self._backend_state != BackendMainStates.RUNNING:
            self._backend_state = BackendMainStates.RUNNING

        # Fail fast on cross-Python cloudpickle skew *before* registering or
        # buffering, so the error always propagates to this caller.  When
        # batching defers the flush, the check would otherwise run inside an
        # orphaned ``ensure_future`` task and never reach the awaiter.
        await self._check_python_compat(tasks)

        # Assign UIDs, register locally, emit submit prof events — single pass
        import uuid

        prof = self._prof
        for task in tasks:
            task.setdefault("uid", f"task.{uuid.uuid4().hex[:8]}")
            self._tasks[task["uid"]] = task
            if prof:
                prof.prof("task_submit", uid=task["uid"])

        # No batching — submit immediately
        if self._batch_window <= 0:
            task_dicts = [dict(t) for t in tasks]
            await asyncio.to_thread(self._rh.submit_tasks, task_dicts)
            return

        # Batching — collect and schedule flush
        async with self._batch_lock:
            self._batch_buffer.extend(dict(t) for t in tasks)

            if len(self._batch_buffer) >= self._batch_limit:
                # Buffer full — flush now
                await self._flush_batch()
            elif self._flush_handle is None:
                # Start the timer for the first task in this window
                loop = asyncio.get_running_loop()
                self._flush_handle = loop.call_later(self._batch_window, self._trigger_flush)

    def _trigger_flush(self):
        """Timer callback — schedule the async flush on the event loop."""
        asyncio.ensure_future(self._locked_flush())

    async def _locked_flush(self):
        """Flush the batch buffer under the batch lock."""
        async with self._batch_lock:
            await self._flush_batch()

    async def _flush_batch(self):
        """Send all buffered tasks in one bulk request.

        Must be called while holding ``_batch_lock``.
        """
        if not self._batch_buffer:
            return

        batch = self._batch_buffer
        self._batch_buffer = []

        if self._flush_handle is not None:
            self._flush_handle.cancel()
            self._flush_handle = None

        # NOTE: compat is validated up front in submit_tasks(); buffered tasks
        # are already known-good by the time they reach the flush.

        prof = self._prof
        if prof:
            for t in batch:
                prof.prof("task_batch_flush", uid=t.get("uid", "?"))

        self.logger.debug("Flushing batch of %d tasks", len(batch))
        try:
            await asyncio.to_thread(self._rh.submit_tasks, batch)
        except Exception as exc:
            # The flush runs as a detached background task; an unhandled error
            # here would leave the batch's tasks hung forever.  Fail them
            # explicitly so the failure reaches the client.
            self.logger.error("Failed to submit batch of %d tasks: %s", len(batch), exc)
            for t in batch:
                task = self._tasks.get(t.get("uid"))
                if task is not None:
                    task["exception"] = exc
                    task["state"] = "FAILED"
                    self._fire_callback(task, "FAILED")
            raise

    async def cancel_task(self, uid: str) -> bool:
        """Cancel a single task on the remote endpoint."""
        if not self._initialized:
            await self._async_init()

        if uid not in self._tasks:
            return False

        await asyncio.to_thread(self._rh.cancel_task, uid)

        task = self._tasks[uid]
        task["state"] = "CANCELED"
        self._fire_callback(task, "CANCELED")
        return True

    async def cancel_all_tasks(self) -> int:
        """Cancel all non-terminal tasks on the remote endpoint.

        Mirrors ``cancel_task``: marks each non-terminal local task CANCELED
        and fires its state callback, so consumers that resolve futures via
        callbacks don't hang.  ``_fire_callback`` dedups against the matching
        CANCELED SSE notification, so each task's terminal callback fires once.
        """
        if not self._initialized:
            await self._async_init()

        result = await asyncio.to_thread(self._rh.cancel_all_tasks)

        for task in list(self._tasks.values()):
            if str(task.get("state", "")).upper() not in self._TERMINAL_STATES:
                task["state"] = "CANCELED"
                self._fire_callback(task, "CANCELED")

        return result.get("canceled", 0)

    async def shutdown(self) -> None:
        """Flush pending tasks, close session and BridgeClient."""
        try:
            await self._locked_flush()
        except Exception as e:
            # Never let a flush failure skip client cleanup below.
            self.logger.warning("Failed to flush pending tasks during shutdown: %s", e)
        self._backend_state = BackendMainStates.SHUTDOWN

        # close() calls perform blocking network I/O — run them off the loop.
        if self._rh:
            try:
                await asyncio.to_thread(self._rh.close)
            except Exception as e:
                self.logger.warning("Failed to close session: %s", e)
            self._rh = None

        if self._bc:
            try:
                await asyncio.to_thread(self._bc.close)
            except Exception as e:
                self.logger.warning("Failed to close bridge client: %s", e)
            self._bc = None

        if self._prof:
            self._prof.close()

        self.logger.info("Orbit execution backend shutdown complete")

    async def state(self) -> str:
        return self._backend_state.value

    def task_state_cb(self, task, state):
        pass

    def get_task_states_map(self):
        return StateMapper(backend=self)

    def build_task(self, uid, task_desc, task_specific_kwargs):
        pass

    def link_explicit_data_deps(self, src_task=None, dst_task=None, file_name=None, file_path=None):
        pass

    def link_implicit_data_deps(self, src_task, dst_task):
        pass

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    async def __aenter__(self):
        if not self._initialized:
            await self._async_init()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.shutdown()
