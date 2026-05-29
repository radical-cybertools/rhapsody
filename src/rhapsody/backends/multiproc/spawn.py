"""Spawn helper for backend-host child processes.

Creates a local AF_UNIX :class:`multiprocessing.connection.Listener`, launches
the host as ``python -m rhapsody.backends.multiproc.host`` (optionally under a
runtime launcher prefix, e.g. ``["dragon"]`` for Phase 3), accepts the child's
connection, performs the ``CONFIG`` → ``READY`` handshake, and returns the
artifacts the proxy needs to drive the child.

Address/authkey are passed via env vars (not argv) so the authkey doesn't
appear in process listings.  The partition env, if any, is merged into the
child's process environment — this is where the RM contract's ``env`` lands.
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from multiprocessing.connection import Connection
from multiprocessing.connection import Listener

from .wire import OP_CONFIG
from .wire import OP_READY
from .wire import recv_msg
from .wire import send_msg

_HOST_ENTRY = "rhapsody.backends.multiproc.host"

logger = logging.getLogger(__name__)


@dataclass
class SpawnedHost:
    """Bundle returned by :func:`spawn_backend_host` — everything the proxy
    needs for both runtime use and teardown."""

    proc: subprocess.Popen
    listener: Listener
    conn: Connection
    address: str
    sockdir: str
    terminal_states: tuple


def spawn_backend_host(
    backend_name: str,
    resources: dict | None = None,
    extra_env: dict | None = None,
    launch_prefix: list[str] | None = None,
) -> SpawnedHost:
    """Spawn a backend-host child and complete the CONFIG/READY handshake.

    Args:
        backend_name:  Name registered with :class:`BackendRegistry` (e.g.
            ``"concurrent"``, ``"noop"``, ``"dragon_v3"``).
        resources:     Passed verbatim to the child backend's constructor as
            ``resources={...}`` (verified fact 8).  May include
            ``{"partition": {"nodelist": [...], "env": {...}}}`` per
            ``rhapsody_rm/CONTRACT.md``.
        extra_env:     Extra env vars for the child process — the partition
            ``env`` from the RM contract goes here.
        launch_prefix: Argv prefix to wrap the python invocation (e.g.
            ``["dragon"]`` for Phase 3); defaults to no prefix.

    Returns:
        :class:`SpawnedHost` carrying the live ``proc``, ``listener``, ``conn``,
        plus the child's reported ``terminal_states`` (from ``READY``).

    Note:
        ``listener.accept()`` blocks until the child connects; callers running
        on an asyncio loop should invoke this via ``loop.run_in_executor``.
    """
    sockdir = tempfile.mkdtemp(prefix="rhapsody-mp-")
    address = os.path.join(sockdir, "sock")
    authkey = os.urandom(16)

    listener = Listener(address, family="AF_UNIX", authkey=authkey)

    child_env = {k: v for k, v in os.environ.items() if v is not None}
    if extra_env:
        child_env.update({str(k): str(v) for k, v in extra_env.items()})
    child_env["RHAPSODY_MP_ADDRESS"] = address
    child_env["RHAPSODY_MP_AUTHKEY"] = authkey.hex()

    cmd = list(launch_prefix or []) + [sys.executable, "-m", _HOST_ENTRY]
    logger.info("spawning backend-host: %s (backend=%s)", " ".join(cmd), backend_name)

    try:
        proc = subprocess.Popen(cmd, env=child_env)
    except Exception:
        listener.close()
        _cleanup_sockdir(sockdir)
        raise

    try:
        conn = listener.accept()           # blocks until child connects
        send_msg(conn, {
            "op": OP_CONFIG,
            "backend": backend_name,
            "resources": resources or {},
        })
        ready = recv_msg(conn)
        if ready.get("op") != OP_READY:
            raise RuntimeError(f"expected READY from child, got {ready!r}")
        terminal_states = tuple(ready.get("terminal_states", ("DONE", "FAILED", "CANCELED")))
    except Exception:
        try:
            proc.terminate()
        except Exception:
            pass
        listener.close()
        _cleanup_sockdir(sockdir)
        raise

    return SpawnedHost(
        proc=proc,
        listener=listener,
        conn=conn,
        address=address,
        sockdir=sockdir,
        terminal_states=terminal_states,
    )


def _cleanup_sockdir(sockdir: str) -> None:
    """Best-effort removal of the AF_UNIX socket file and its tempdir."""
    try:
        sock = os.path.join(sockdir, "sock")
        if os.path.exists(sock):
            os.unlink(sock)
        os.rmdir(sockdir)
    except OSError:
        pass
