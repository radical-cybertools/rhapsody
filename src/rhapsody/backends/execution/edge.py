"""Edge execution backend for remote task execution via RADICAL Edge.

This module provides a backend that submits tasks to a remote HPC node
through the RADICAL Edge bridge/plugin infrastructure.  The Edge node
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

# ``radical.edge`` is imported at module level so that tests can patch
# ``BridgeClient`` here.  When the package isn't installed we keep
# ``BridgeClient = None`` and remember the original ImportError; the
# actual chained re-raise happens in ``EdgeExecutionBackend.__init__``
# so the user sees the real cause (e.g. a downstream import failure
# inside ``radical.edge`` itself, not just "package missing").
try:
    from radical.edge import BridgeClient
    _radical_edge_import_error = None
except ImportError as exc:
    BridgeClient = None
    _radical_edge_import_error = exc

try:
    import radical.prof as rprof
except ImportError:
    rprof = None


class EdgeExecutionBackend(BaseBackend):
    """Execution backend that delegates to a remote RADICAL Edge node.

    Uses ``radical.edge.BridgeClient`` and ``RhapsodyClient`` for all
    communication — inheriting batching, template compression,
    pipelined submission, and SSE-based notifications.

    When tasks are submitted individually (one at a time), a batching
    layer collects them over a short time window (default 0.1 s) and
    flushes them as a single bulk request, dramatically reducing
    per-task HTTP round-trip overhead.

    Args:
        bridge_url:    URL of the RADICAL Edge bridge
                       (e.g. ``"https://localhost:8000"``).  If omitted,
                       falls back to the ``RADICAL_BRIDGE_URL`` env var
                       (resolved by ``BridgeClient`` itself).
        edge_name:     Name of the edge to target.  If omitted, the
                       backend auto-selects the first connected edge
                       that advertises an enabled rhapsody plugin
                       (the synthetic ``'bridge'`` edge is always
                       skipped).  Raises ``RuntimeError`` from
                       ``await backend`` if no candidate is found.
        backends:      Backend names to request on the remote session
                       (default ``["dragon_v3"]``).
        name:          Backend name for Rhapsody registration
                       (default ``"edge"``).
        batch_window:  Seconds to collect tasks before flushing
                       (default 0.05).  Set to 0 to disable batching.
        batch_limit:   Max tasks per batch — triggers an immediate flush
                       when reached (default 1024).
        notify_batch_window:  Edge-side notification batch window
                       (seconds).  ``None`` uses server default.
        notify_batch_size:    Edge-side notification batch size.
                       ``None`` uses server default.
    """

    _DEFAULT_BATCH_WINDOW = 0.25
    _EDGES_TO_SKIP        = ['bridge']
    _PLUGIN_NAME          = 'rhapsody'


    def __init__(
        self,
        bridge_url: str | None = None,
        edge_name: str | None = None,
        backends: list[str] | None = None,
        name: str = "edge",
        plugin_name: str = _PLUGIN_NAME,
        batch_window: float | None = None,
        batch_limit: int = 1024,
        notify_batch_window: float | None = None,
        notify_batch_size: int | None = None,
    ):
        super().__init__(name=name)

        if BridgeClient is None:
            raise ImportError(
                f"EdgeExecutionBackend: cannot import radical.edge: "
                f"{_radical_edge_import_error}") from _radical_edge_import_error

        self.logger           = logging.getLogger(__name__)
        self._bridge_url      = bridge_url
        self._edge_name       = edge_name
        self._plugin_name          = plugin_name
        self._remote_backends      = backends or ['dragon_v3']
        self._notify_batch_window  = notify_batch_window
        self._notify_batch_size    = notify_batch_size
        self._edge_python_mm: tuple | None = None
        self._edge_python_lookup_done = False

        self._bc   = None   # BridgeClient
        self._rh   = None   # RhapsodyClient (from get_rhapsody_handle)
        self._tasks: dict[str, dict] = {}

        self._callback_func: Callable = lambda t, s: None
        self._initialized   = False
        self._backend_state = BackendMainStates.INITIALIZED
        self._loop: asyncio.AbstractEventLoop | None = None

        # -- submission batching --
        self._batch_window = batch_window if batch_window is not None \
                             else self._DEFAULT_BATCH_WINDOW
        self._batch_limit  = batch_limit
        self._batch_buffer: list[dict] = []
        self._batch_lock   = asyncio.Lock()
        self._flush_handle: asyncio.TimerHandle | None = None

        # -- profiling --
        self._prof = rprof.Profiler('client.task', ns='radical.edge') \
                     if rprof else None

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

        self._rh = self._get_rhapsody_handle()

        # Register persistent notification callback for task completions
        self._rh.register_notification_callback(
            self._on_task_notification, topic="task_status")
        self._rh.register_notification_callback(
            self._on_task_notification, topic="task_status_batch")

        self._initialized = True
        self.logger.info("Edge backend ready: %s/%s (session %s)",
                         self._edge_name, self._plugin_name,
                         self._rh.sid)
        return self

    # ------------------------------------------------------------------
    # Notification handling
    # ------------------------------------------------------------------

    def _on_task_notification(self, edge, plugin, topic, data):
        """SSE callback: update local tasks and fire Rhapsody callback."""
        if topic == 'task_status_batch':
            for t in data.get('tasks', []):
                self._apply_task_update(t)
        else:
            self._apply_task_update(data)

    def _apply_task_update(self, body: dict):
        """Apply a single task status update from SSE."""
        uid   = body.get('uid')
        state = body.get('state', '')

        if uid not in self._tasks:
            return

        task = self._tasks[uid]

        # Decode base64-encoded return values (bytes results)
        if body.get('_return_value_encoding') == 'base64':
            import base64
            body['return_value'] = base64.b64decode(body['return_value'])
            del body['_return_value_encoding']

        # Update local task dict with remote results
        for key in ('state', 'stdout', 'stderr', 'exit_code',
                    'return_value', 'exception', 'traceback', 'error'):
            if key in body:
                task[key] = body[key]

        # Profile task completion on client side
        if self._prof:
            self._prof.prof('task_complete', uid=uid, state=state)

        # Fire Rhapsody state callback (thread-safe)
        if self._loop and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(
                self._callback_func, task, state)

    # ------------------------------------------------------------------
    # Edge auto-selection and Plugin retrieval
    # ------------------------------------------------------------------

    def _get_rhapsody_handle(self) -> "RhapsodyClient":
        """Either create an RhapsodyClient for the named edge, or pick the first
        edge that advertises an enabled rhapsody plugin. Plugins hosted by the
        Bridge are skipped.
        Raises ``RuntimeError`` if no candidate is found.
        """

        self._bc = BridgeClient(url=self._bridge_url)
        self._bridge_url = self._bc.url

        # find a suitable edge and load rhapsody plugin
        if not self._edge_name:
            for eid in self._bc.list_edges():
                if eid in self._EDGES_TO_SKIP:
                    continue

                plugins = self._bc.get_edge_client(eid).list_plugins()
                info    = plugins.get(self._plugin_name)
                if info and info.get('enabled'):
                    self.logger.info("auto-selected edge %r (plugin %r)",
                                     eid, self._plugin_name)
                    self._edge_name = eid
                    break

        if not self._edge_name:
            raise RuntimeError(
                f"no edge advertises an enabled {self._plugin_name!r} plugin "
                f"on bridge {self._bridge_url}")

        ec = self._bc.get_edge_client(self._edge_name)

        kwargs = {'backends': self._remote_backends}
        if self._notify_batch_window is not None:
            kwargs['notify_batch_window'] = self._notify_batch_window
        if self._notify_batch_size is not None:
            kwargs['notify_batch_size'] = self._notify_batch_size

        return ec.get_plugin(self._plugin_name, **kwargs)


    # ------------------------------------------------------------------
    # Python-version compatibility for cloudpickled function tasks
    # ------------------------------------------------------------------

    def _check_python_compat(self, tasks: list) -> None:
        """Raise if any cloudpickled task in *tasks* would hit a Python
        version mismatch on the remote edge.  No check is performed
        if the batch contains only executable or import-path tasks,
        or if the edge's Python version could not be determined.

        Cloudpickle serializes function bytecode using ``CodeType``,
        whose tuple shape changed between Python 3.10 and 3.11 — any
        cross-minor-version skew fails deserialization on the edge.
        """
        import sys

        def needs_compat(t: dict) -> bool:
            fn = t.get('function')
            return ((isinstance(fn, str) and fn.startswith('cloudpickle::'))
                    or bool(t.get('_pickled_fields')))

        if not any(needs_compat(t) for t in tasks):
            return

        if not self._edge_python_lookup_done:
            self._edge_python_lookup_done = True
            pyver = ''
            pyexc = ''
            try:
                ec = self._bc.get_edge_client(self._edge_name)
                info = ec.get_plugin('sysinfo').host_role()
                pyver = info.get('python_version') or ''
                parts = pyver.split('.')
                if len(parts) >= 2:
                    self._edge_python_mm = (int(parts[0]), int(parts[1]))
            except Exception as exc:
                pyexc = exc

            if not self._edge_python_mm:
                self.logger.debug(f"skip pickle compat check {pyver} ({pyexc})")

        if not self._edge_python_mm:
            return

        client_mm = (sys.version_info.major, sys.version_info.minor)
        if self._edge_python_mm != client_mm:
            raise RuntimeError(
                f"function tasks cannot be submitted to edge "
                f"{self._edge_name!r}: client Python "
                f"{client_mm[0]}.{client_mm[1]} != edge Python "
                f"{self._edge_python_mm[0]}.{self._edge_python_mm[1]}.  "
                f"cloudpickle is not portable across Python minor "
                f"versions — align the venvs, or use executable / "
                f"import-path tasks for this edge.")

    # ------------------------------------------------------------------
    # BaseBackend interface
    # ------------------------------------------------------------------

    async def submit_tasks(self, tasks: list[dict[str, Any]]) -> None:
        """Submit tasks to the remote edge for execution.

        When batching is enabled (batch_window > 0), tasks are collected
        in an internal buffer and flushed after the window expires or the
        buffer reaches ``batch_limit``.  This turns many small individual
        submissions into fewer bulk HTTP requests.

        When batching is disabled (batch_window == 0), delegates directly
        to ``RhapsodyClient.submit_tasks()``.
        """
        if self._backend_state != BackendMainStates.RUNNING:
            self._backend_state = BackendMainStates.RUNNING

        # Assign UIDs, register locally, emit submit prof events — single pass
        import uuid
        prof = self._prof
        for task in tasks:
            task.setdefault('uid', f"task.{uuid.uuid4().hex[:8]}")
            self._tasks[task['uid']] = task
            if prof:
                prof.prof('task_submit', uid=task['uid'])

        # No batching — submit immediately
        if self._batch_window <= 0:
            task_dicts = [dict(t) for t in tasks]
            self._check_python_compat(task_dicts)
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
                self._flush_handle = loop.call_later(
                    self._batch_window, self._trigger_flush)

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

        self._check_python_compat(batch)

        prof = self._prof
        if prof:
            for t in batch:
                prof.prof('task_batch_flush', uid=t.get('uid', '?'))

        self.logger.debug("Flushing batch of %d tasks", len(batch))
        await asyncio.to_thread(self._rh.submit_tasks, batch)

    async def cancel_task(self, uid: str) -> bool:
        """Cancel a single task on the remote edge."""
        if uid not in self._tasks:
            return False

        await asyncio.to_thread(self._rh.cancel_task, uid)

        task = self._tasks[uid]
        task['state'] = 'CANCELED'
        self._callback_func(task, 'CANCELED')
        return True

    async def cancel_all_tasks(self) -> int:
        """Cancel all non-terminal tasks on the remote edge."""
        result = await asyncio.to_thread(self._rh.cancel_all_tasks)
        return result.get('canceled', 0)

    async def shutdown(self) -> None:
        """Flush pending tasks, close session and BridgeClient."""
        await self._locked_flush()
        self._backend_state = BackendMainStates.SHUTDOWN

        if self._rh:
            try:
                self._rh.close()
            except Exception as e:
                self.logger.warning("Failed to close session: %s", e)
            self._rh = None

        if self._bc:
            try:
                self._bc.close()
            except Exception as e:
                self.logger.warning("Failed to close bridge client: %s", e)
            self._bc = None

        if self._prof:
            self._prof.close()

        self.logger.info("Edge execution backend shutdown complete")

    async def state(self) -> str:
        return self._backend_state.value

    def task_state_cb(self, task, state):
        pass

    def get_task_states_map(self):
        return StateMapper(backend=self)

    def build_task(self, uid, task_desc, task_specific_kwargs):
        pass

    def link_explicit_data_deps(self, src_task=None, dst_task=None,
                                file_name=None, file_path=None):
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
