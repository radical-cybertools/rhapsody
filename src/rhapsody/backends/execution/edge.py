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


def _get_logger() -> logging.Logger:
    return logging.getLogger(__name__)


class EdgeExecutionBackend(BaseBackend):
    """Execution backend that delegates to a remote RADICAL Edge node.

    Uses ``radical.edge.BridgeClient`` and ``RhapsodyClient`` for all
    communication — inheriting batching, template compression,
    pipelined submission, and SSE-based notifications.

    Args:
        bridge_url:  URL of the RADICAL Edge bridge
                     (e.g. ``"https://localhost:8000"``).
        edge_name:   Name of the edge to target.
        plugin_name: Name of the Rhapsody plugin on the edge
                     (default ``"rhapsody"``).
        backends:    Backend names to request on the remote session
                     (default ``["dragon_v3"]``).
        name:        Backend name for Rhapsody registration
                     (default ``"edge"``).
    """

    def __init__(
        self,
        bridge_url: str,
        edge_name: str,
        plugin_name: str = "rhapsody",
        backends: list[str] | None = None,
        name: str = "edge",
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
                    'return_value', 'exception', 'error'):
            if key in body:
                task[key] = body[key]

        # Fire Rhapsody state callback (thread-safe)
        if self._loop and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(
                self._callback_func, task, state)

    # ------------------------------------------------------------------
    # BaseBackend interface
    # ------------------------------------------------------------------

    async def submit_tasks(self, tasks: list[dict[str, Any]]) -> None:
        """Submit tasks to the remote edge for execution.

        Delegates to ``RhapsodyClient.submit_tasks()`` which handles
        serialization, template compression, size-aware batching, and
        concurrent pipelined submission.
        """
        if self._backend_state != BackendMainStates.RUNNING:
            self._backend_state = BackendMainStates.RUNNING

        # Ensure all tasks have UIDs before registration
        import uuid
        for task in tasks:
            if 'uid' not in task:
                task['uid'] = f"task.{uuid.uuid4().hex[:8]}"

        # Register tasks locally for notification tracking
        task_dicts = []
        for task in tasks:
            self._tasks[task['uid']] = task
            task_dicts.append(dict(task))

        # Delegate to RhapsodyClient — all optimizations apply
        await asyncio.to_thread(self._rh.submit_tasks, task_dicts)

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
        """Close the RhapsodyClient session and BridgeClient."""
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
