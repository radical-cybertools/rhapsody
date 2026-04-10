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
from typing import Any, Callable

from ..base import BaseBackend
from ..constants import BackendMainStates, StateMapper

try:
    from radical.edge import BridgeClient
except ImportError:
    BridgeClient = None

try:
    import radical.prof as rprof
except ImportError:
    rprof = None


def _get_logger() -> logging.Logger:
    return logging.getLogger(__name__)


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
                       (e.g. ``"https://localhost:8000"``).
        edge_name:     Name of the edge to target.
        plugin_name:   Name of the Rhapsody plugin on the edge
                       (default ``"rhapsody"``).
        backends:      Backend names to request on the remote session
                       (default ``["dragon_v3"]``).
        name:          Backend name for Rhapsody registration
                       (default ``"edge"``).
        batch_window:  Seconds to collect tasks before flushing
                       (default 0.1).  Set to 0 to disable batching.
        batch_limit:   Max tasks per batch — triggers an immediate flush
                       when reached (default 1024).
    """

    def __init__(
        self,
        bridge_url: str,
        edge_name: str,
        plugin_name: str = "rhapsody",
        backends: list[str] | None = None,
        name: str = "edge",
        batch_window: float = 0.1,
        batch_limit: int = 1024,
    ):
        super().__init__(name=name)

        if BridgeClient is None:
            raise ImportError(
                "EdgeExecutionBackend requires 'radical.edge'. "
                "Install it with: pip install radical.edge")

        self.logger           = _get_logger()
        self._bridge_url      = bridge_url
        self._edge_name       = edge_name
        self._plugin_name     = plugin_name
        self._remote_backends = backends or ['dragon_v3']

        self._bc   = None   # BridgeClient
        self._rh   = None   # RhapsodyClient (from get_plugin)
        self._tasks: dict[str, dict] = {}

        self._callback_func: Callable = lambda t, s: None
        self._initialized   = False
        self._backend_state = BackendMainStates.INITIALIZED
        self._loop: asyncio.AbstractEventLoop | None = None

        # -- submission batching --
        self._batch_window = batch_window
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
        self._backend_state = BackendMainStates.INITIALIZED

        # Create BridgeClient → EdgeClient → RhapsodyClient
        # cert=None disables TLS verification (default for self-signed)
        self._bc = BridgeClient(url=self._bridge_url)
        ec = self._bc.get_edge_client(self._edge_name)
        self._rh = ec.get_plugin(self._plugin_name,
                                 backends=self._remote_backends)

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

        # Ensure all tasks have UIDs before registration
        import uuid
        for task in tasks:
            if 'uid' not in task:
                task['uid'] = f"task.{uuid.uuid4().hex[:8]}"

        # Register tasks locally for notification tracking
        prof = self._prof
        for task in tasks:
            self._tasks[task['uid']] = task
            if prof:
                prof.prof('task_submit', uid=task['uid'])

        # No batching — submit immediately
        if self._batch_window <= 0:
            task_dicts = [dict(t) for t in tasks]
            if prof:
                for t in tasks:
                    prof.prof('task_batch_flush', uid=t['uid'])
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
        asyncio.ensure_future(self._timed_flush())

    async def _timed_flush(self):
        """Flush the batch buffer (called from timer)."""
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

        prof = self._prof
        if prof:
            for t in batch:
                prof.prof('task_batch_flush', uid=t.get('uid', '?'))

        self.logger.debug("Flushing batch of %d tasks", len(batch))
        await asyncio.to_thread(self._rh.submit_tasks, batch)

    async def _force_flush(self):
        """Force-flush any pending batched tasks."""
        async with self._batch_lock:
            await self._flush_batch()

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
        await self._force_flush()
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
