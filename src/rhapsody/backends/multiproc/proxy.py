"""Driver-side proxy for a backend hosted in a child process.

``RemoteBackendProxy`` is a :class:`BaseBackend` (and so plugs into
:class:`Session` with no Session-side changes — verified facts 1, 6) that
forwards ``submit_tasks`` / ``cancel_task`` / ``shutdown`` over the local
control plane and, on incoming ``COMPLETE``, **merges the child's result fields
back into the driver-side task object** before firing the registered callback
— the cross-process analogue of fact 3.

Patterns mirror the dragon-isolation prototype (commit ``660dc45``,
``DragonExecutionBackendV3Client``), generalized to any backend and using
``multiprocessing.connection`` instead of ZMQ.
"""

from __future__ import annotations

import asyncio
import logging
import subprocess
import threading
from typing import Any
from typing import Callable

from ..base import BaseBackend
from ..constants import BackendMainStates
from ..discovery import BackendRegistry
from .spawn import SpawnedHost
from .spawn import spawn_backend_host
from .wire import OP_CANCEL
from .wire import OP_COMPLETE
from .wire import OP_SHUTDOWN
from .wire import OP_STATE
from .wire import OP_SUBMIT
from .wire import TerminalStatesShim
from .wire import merge_envelope_into_task
from .wire import recv_msg
from .wire import send_msg

logger = logging.getLogger(__name__)


class RemoteBackendProxy(BaseBackend):
    """Driver-side proxy that fronts a backend running in a child process.

    Usage::

        proxy = await RemoteBackendProxy(
            name="p0",                # unique partition-backend name
            backend="concurrent",     # name registered with BackendRegistry
            resources={"partition": {...}},   # optional; RM contract dict
            extra_env={"SLURM_NODELIST": "..."},
            launch_prefix=None,       # e.g. ["dragon"] for Phase 3
        )
        Session(backends=[proxy, ...])
    """

    def __init__(
        self,
        name: str,
        backend: str,
        resources: dict | None = None,
        extra_env: dict | None = None,
        launch_prefix: list[str] | None = None,
    ):
        super().__init__(name=name)
        self._backend_name = backend
        self._resources = resources or {}
        # ``None`` (not ``[]``/``{}``) means "derive from the backend class +
        # partition spec at init time".  Explicit values override.
        self._extra_env_override: dict | None = extra_env
        self._launch_prefix_override: list[str] | None = (
            list(launch_prefix) if launch_prefix is not None else None
        )
        # Resolved at init time (after derivation).
        self._extra_env: dict = {}
        self._launch_prefix: list[str] = []

        self._callback_func: Callable = lambda t, s: None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._initialized = False
        self._backend_state = BackendMainStates.INITIALIZED

        self._pending: dict[str, dict] = {}     # uid -> driver-side task object
        self._terminal_states: tuple = ("DONE", "FAILED", "CANCELED")
        self._host: SpawnedHost | None = None
        self._reader_thread: threading.Thread | None = None
        self._closed = False

    # ------------------------------------------------------------------ init
    def __await__(self):
        return self._async_init().__await__()

    async def _async_init(self):
        if self._initialized:
            return self

        self._loop = asyncio.get_running_loop()

        # Derive the launcher prefix and child env from the backend class +
        # partition spec, unless the user passed explicit overrides.  This is
        # where launcher-specific knowledge (Dragon's --hostlist, ports, etc.)
        # lives on the backend, keeping the proxy and call site Dragon-blind.
        partition = (self._resources or {}).get("partition")
        if self._launch_prefix_override is not None:
            self._launch_prefix = self._launch_prefix_override
        else:
            backend_cls = BackendRegistry.get_backend_class(self._backend_name)
            self._launch_prefix = list(backend_cls.build_launch_prefix(partition))
        if self._extra_env_override is not None:
            self._extra_env = self._extra_env_override
        else:
            self._extra_env = dict((partition or {}).get("env") or {})

        # spawn_backend_host blocks on listener.accept(); run in executor so
        # the driver loop keeps spinning.
        self._host = await self._loop.run_in_executor(
            None,
            spawn_backend_host,
            self._backend_name,
            self._resources,
            self._extra_env,
            self._launch_prefix,
        )
        self._terminal_states = self._host.terminal_states

        self._reader_thread = threading.Thread(
            target=self._reader_loop,
            name=f"rhapsody-mp-proxy-{self.name}",
            daemon=True,
        )
        self._reader_thread.start()

        self._initialized = True
        logger.info("RemoteBackendProxy[%s -> %s] ready", self.name, self._backend_name)
        return self

    # ----------------------------------------------------------- BaseBackend
    async def submit_tasks(self, tasks: list[dict]) -> None:
        """Track tasks by uid (we need the *driver-side* objects for merge-back)
        then send a SUBMIT batch to the child."""
        if self._backend_state != BackendMainStates.RUNNING:
            self._backend_state = BackendMainStates.RUNNING
        for task in tasks:
            self._pending[task["uid"]] = task
        send_msg(self._host.conn, {"op": OP_SUBMIT, "tasks": list(tasks)})

    async def cancel_task(self, uid: str) -> bool:
        if uid not in self._pending:
            return False
        send_msg(self._host.conn, {"op": OP_CANCEL, "uid": uid})
        return True

    async def shutdown(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._backend_state = BackendMainStates.SHUTDOWN
        try:
            send_msg(self._host.conn, {"op": OP_SHUTDOWN})
        except Exception:
            pass

        # Give the child a chance to exit cleanly, then close.
        try:
            self._host.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self._host.proc.terminate()
            try:
                self._host.proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self._host.proc.kill()

        try:
            self._host.conn.close()
        except Exception:
            pass
        try:
            self._host.listener.close()
        except Exception:
            pass

        if self._reader_thread:
            self._reader_thread.join(timeout=2)

        from .spawn import _cleanup_sockdir
        _cleanup_sockdir(self._host.sockdir)

    async def state(self) -> str:
        return self._backend_state.value

    def get_task_states_map(self):
        # Session reads only ``.terminal_states`` (verified fact 6).
        return TerminalStatesShim(self._terminal_states)

    # ABC requirements that the existing engine never invokes (fact 7) ----
    def build_task(self, *args: Any, **kwargs: Any) -> None:
        pass

    def task_state_cb(self, *args: Any, **kwargs: Any) -> None:
        pass

    def link_explicit_data_deps(self, *args: Any, **kwargs: Any) -> None:
        pass

    def link_implicit_data_deps(self, *args: Any, **kwargs: Any) -> None:
        pass

    # --------------------------------------------------------- async ctx mgr
    async def __aenter__(self):
        if not self._initialized:
            await self._async_init()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.shutdown()

    # --------------------------------------------------- reader thread loop
    def _reader_loop(self) -> None:
        """Drain STATE / COMPLETE / ERROR notifications from the child.

        Runs in a daemon thread; on each terminal completion, merges result
        fields into the driver-side task object then calls the registered
        callback (``Session.update_task``, which is itself thread-safe per
        verified fact 5 — but we hop through the loop explicitly to match the
        prototype and keep callback ordering predictable per loop).
        """
        conn = self._host.conn
        while True:
            try:
                msg = recv_msg(conn)
            except (EOFError, OSError):
                return
            except Exception as exc:
                logger.warning("proxy[%s] reader recv failed: %s", self.name, exc)
                return

            op = msg.get("op")
            if op == OP_STATE:
                uid = msg["uid"]
                state = msg["state"]
                task = self._pending.get(uid)
                if task is None:
                    continue
                task["state"] = state
                self._dispatch_callback(task, state)
            elif op == OP_COMPLETE:
                uid = msg["uid"]
                state = msg["state"]
                envelope = msg["envelope"]
                task = self._pending.pop(uid, None)
                if task is None:
                    continue
                try:
                    merge_envelope_into_task(task, envelope)
                except Exception as exc:
                    logger.exception("proxy[%s] merge failed for %s: %s",
                                     self.name, uid, exc)
                    task["state"] = state
                task["state"] = state  # ensure state set even if envelope omitted it
                self._dispatch_callback(task, state)
            elif op == "ERROR":
                logger.error("proxy[%s] child reported ERROR: %s",
                             self.name, msg.get("detail"))
            else:
                logger.warning("proxy[%s] unknown op: %r", self.name, op)

    def _dispatch_callback(self, task: dict, state: str) -> None:
        loop = self._loop
        cb = self._callback_func
        if loop is None or loop.is_closed():
            # Loop gone — best effort direct call (matches Session fallback).
            try:
                cb(task, state)
            except Exception:
                logger.exception("proxy[%s] callback (no-loop) failed", self.name)
            return
        loop.call_soon_threadsafe(cb, task, state)
