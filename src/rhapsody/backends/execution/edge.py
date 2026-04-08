"""Edge execution backend for remote task execution via RADICAL Edge.

This module provides a backend that submits tasks to a remote HPC node
through the RADICAL Edge bridge/plugin infrastructure.  The Edge node
runs a Rhapsody plugin with a local backend (e.g. Dragon V3) that
actually executes the work.

Task flow:
  EdgeExecutionBackend.submit_tasks()
    -> serialize task dicts (cloudpickle for callables)
    -> POST /rhapsody/submit/{sid}  (via bridge -> edge)
    -> remote RhapsodySession deserializes and delegates to local backend
    -> SSE notifications stream back state changes
    -> callback fires locally when task reaches terminal state
"""

import asyncio
import base64
import json
import logging
import threading
from typing import Any, Callable

from ..base import BaseBackend
from ..constants import BackendMainStates, StateMapper

try:
    import cloudpickle
except ImportError:
    cloudpickle = None

try:
    import httpx
except ImportError:
    httpx = None

try:
    import sseclient
except ImportError:
    sseclient = None


def _get_logger() -> logging.Logger:
    return logging.getLogger(__name__)


class EdgeExecutionBackend(BaseBackend):
    """Execution backend that delegates to a remote RADICAL Edge node.

    Args:
        bridge_url:  URL of the RADICAL Edge bridge
                     (e.g. ``"https://localhost:8000"``).
        edge_name:   Name of the edge to target.
        plugin_name: Name of the Rhapsody plugin on the edge
                     (default ``"rhapsody"``).
        backends:    Backend names to request on the remote session
                     (default ``["dragon_v3"]``).
        verify_ssl:  Verify TLS certificates (default ``False`` for
                     self-signed certs typical in HPC).
        name:        Backend name for Rhapsody registration
                     (default ``"edge"``).
    """

    def __init__(
        self,
        bridge_url: str,
        edge_name: str,
        plugin_name: str = "rhapsody",
        backends: list[str] | None = None,
        verify_ssl: bool = False,
        name: str = "edge",
    ):
        super().__init__(name=name)

        if httpx is None:
            raise ImportError("EdgeExecutionBackend requires 'httpx'. "
                              "Install it with: pip install httpx")

        self.logger         = _get_logger()
        self._bridge_url    = bridge_url.rstrip('/')
        self._edge_name     = edge_name
        self._plugin_name   = plugin_name
        self._remote_backends = backends or ['dragon_v3']
        self._verify_ssl    = verify_ssl

        self._http           = httpx.Client(verify=verify_ssl, timeout=300)
        self._sid: str | None = None
        self._base_url: str | None = None

        self._tasks: dict[str, dict]   = {}
        self._callback_func: Callable  = lambda t, s: None
        self._initialized              = False
        self._backend_state            = BackendMainStates.INITIALIZED

        self._sse_thread: threading.Thread | None = None
        self._sse_stop   = threading.Event()
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

        # Build base URL for the plugin on this edge
        self._base_url = (f"{self._bridge_url}/{self._edge_name}"
                          f"/{self._plugin_name}")

        # Register a remote session
        resp = self._http.post(
            f"{self._base_url}/register_session",
            json={"backends": self._remote_backends},
        )
        resp.raise_for_status()
        self._sid = resp.json()["sid"]
        self.logger.info("Remote session registered: %s", self._sid)

        # Start SSE listener for notifications
        self._start_sse_listener()

        self._initialized = True
        return self

    # ------------------------------------------------------------------
    # Task serialization
    # ------------------------------------------------------------------

    @staticmethod
    def _serialize_task(task: dict) -> dict:
        """Prepare a task dict for JSON transport over REST.

        Callables in ``function``, ``args``, ``kwargs`` are encoded with
        cloudpickle + base64.  Plain JSON-serializable values pass through.
        """
        td = dict(task)
        pickled_fields = []

        # Serialize callable function
        fn = td.get('function')
        if callable(fn):
            if cloudpickle is None:
                raise RuntimeError(
                    "cloudpickle is required to serialize function tasks. "
                    "Install it with: pip install cloudpickle")
            encoded = base64.b64encode(
                cloudpickle.dumps(fn)).decode('ascii')
            td['function'] = 'cloudpickle::' + encoded
            pickled_fields.append('function')

        # Serialize args/kwargs if they contain non-JSON types
        for field in ('args', 'kwargs'):
            val = td.get(field)
            if val is None:
                continue
            try:
                json.dumps(val)
            except (TypeError, ValueError):
                if cloudpickle is None:
                    raise RuntimeError(
                        f"cloudpickle required to serialize '{field}'")
                encoded = base64.b64encode(
                    cloudpickle.dumps(val)).decode('ascii')
                td[field] = 'cloudpickle::' + encoded
                pickled_fields.append(field)

        if pickled_fields:
            td['_pickled_fields'] = pickled_fields

        # Remove non-serializable internal fields and the local backend
        # name so the remote session routes to its own default backend.
        td.pop('future', None)
        td.pop('_future', None)
        td.pop('backend', None)

        return td

    # ------------------------------------------------------------------
    # SSE notification listener
    # ------------------------------------------------------------------

    def _start_sse_listener(self):
        """Start a background thread that listens for SSE notifications."""
        if sseclient is None:
            self.logger.warning("sseclient not installed — no live "
                                "notifications (pip install sseclient-py)")
            return

        self._sse_stop.clear()
        self._sse_thread = threading.Thread(
            target=self._sse_loop, daemon=True, name="edge-sse")
        self._sse_thread.start()

    def _sse_loop(self):
        """SSE listener loop (runs in background thread)."""
        import requests as req   # use requests for streaming (httpx SSE
                                 # support is limited)
        url = f"{self._bridge_url}/events"
        try:
            resp = req.get(url, stream=True,
                           verify=self._verify_ssl, timeout=None)
            client = sseclient.SSEClient(resp)
            for event in client.events():
                if self._sse_stop.is_set():
                    break
                try:
                    msg = json.loads(event.data)
                except (json.JSONDecodeError, TypeError):
                    continue
                if msg.get('topic') == 'notification':
                    self._handle_notification(msg['data'])
        except Exception as e:
            if not self._sse_stop.is_set():
                self.logger.warning("SSE listener error: %s", e)

    def _handle_notification(self, data: dict):
        """Process an SSE notification from the remote plugin."""
        edge   = data.get('edge')
        plugin = data.get('plugin')
        topic  = data.get('topic')
        body   = data.get('data', {})

        # Filter for our edge/plugin
        if edge != self._edge_name or plugin != self._plugin_name:
            return
        if topic != 'task_status':
            return

        uid   = body.get('uid')
        state = body.get('state', '')

        if uid not in self._tasks:
            return

        task = self._tasks[uid]

        # Update local task dict with remote results
        for key in ('state', 'stdout', 'stderr', 'exit_code',
                    'return_value', 'exception', 'error'):
            if key in body:
                task[key] = body[key]

        # Fire callback (thread-safe delivery to the event loop)
        if self._loop and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(
                self._callback_func, task, state)

    # ------------------------------------------------------------------
    # BaseBackend interface
    # ------------------------------------------------------------------

    async def submit_tasks(self, tasks: list[dict[str, Any]]) -> None:
        """Submit tasks to the remote edge for execution."""
        if self._backend_state != BackendMainStates.RUNNING:
            self._backend_state = BackendMainStates.RUNNING

        serialized = []
        for task in tasks:
            td = self._serialize_task(task)
            self._tasks[task['uid']] = task
            serialized.append(td)

        resp = self._http.post(
            f"{self._base_url}/submit/{self._sid}",
            json={"tasks": serialized},
        )
        resp.raise_for_status()

        # Update local UIDs if the remote assigned different ones
        remote_results = resp.json()
        if isinstance(remote_results, list):
            for local, remote in zip(tasks, remote_results):
                remote_uid = remote.get('uid')
                if remote_uid and remote_uid != local['uid']:
                    self._tasks[remote_uid] = self._tasks.pop(local['uid'])
                    local['uid'] = remote_uid

    async def cancel_task(self, uid: str) -> bool:
        """Cancel a single task on the remote edge."""
        if uid not in self._tasks:
            return False

        resp = self._http.post(
            f"{self._base_url}/cancel/{self._sid}/{uid}")
        resp.raise_for_status()

        task = self._tasks[uid]
        task['state'] = 'CANCELED'
        self._callback_func(task, 'CANCELED')
        return True

    async def cancel_all_tasks(self) -> int:
        """Cancel all non-terminal tasks on the remote edge."""
        resp = self._http.post(
            f"{self._base_url}/cancel_all/{self._sid}")
        resp.raise_for_status()
        return resp.json().get('canceled', 0)

    async def shutdown(self) -> None:
        """Unregister the remote session and stop the SSE listener."""
        self._backend_state = BackendMainStates.SHUTDOWN

        # Stop SSE listener
        self._sse_stop.set()
        if self._sse_thread and self._sse_thread.is_alive():
            self._sse_thread.join(timeout=5)

        # Unregister remote session
        if self._sid and self._base_url:
            try:
                self._http.post(
                    f"{self._base_url}/unregister_session/{self._sid}")
            except Exception as e:
                self.logger.warning("Failed to unregister session: %s", e)

        self._http.close()
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
