"""Backend-host child process — runs the real :class:`BaseBackend`.

Entry point: ``python -m rhapsody.backends.multiproc.host``.  Reads the
control-plane address/authkey from env (set by ``spawn.spawn_backend_host``),
connects back to the driver, performs the ``CONFIG`` → ``READY`` handshake,
constructs the named backend via :func:`get_backend`, and forwards lifecycle
events back to the driver.

The structure mirrors the dragon-isolation prototype (commit ``660dc45``,
``DragonExecutionBackendV3Worker`` + ``_worker_main`` / ``main``), generalized
to any backend and using ``multiprocessing.connection`` instead of ZMQ.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import threading
from multiprocessing.connection import Client

from ..discovery import get_backend
from .wire import OP_CANCEL
from .wire import OP_COMPLETE
from .wire import OP_CONFIG
from .wire import OP_ERROR
from .wire import OP_READY
from .wire import OP_SHUTDOWN
from .wire import OP_STATE
from .wire import OP_SUBMIT
from .wire import inline_envelope
from .wire import recv_msg
from .wire import send_msg

logger = logging.getLogger(__name__)


async def _run() -> int:
    address = os.environ.get("RHAPSODY_MP_ADDRESS")
    authkey_hex = os.environ.get("RHAPSODY_MP_AUTHKEY")
    if not address or not authkey_hex:
        sys.stderr.write(
            "rhapsody.backends.multiproc.host: missing RHAPSODY_MP_ADDRESS/AUTHKEY env\n"
        )
        return 2

    authkey = bytes.fromhex(authkey_hex)

    # Connect (blocking, fast — listener is already accepting on the driver side).
    conn = Client(address, family="AF_UNIX", authkey=authkey)

    # CONFIG handshake: driver tells us which backend to build and with what
    # resources.  ``resources`` may carry the partition spec per
    # rhapsody_rm/CONTRACT.md.
    cfg = recv_msg(conn)
    if cfg.get("op") != OP_CONFIG:
        send_msg(conn, {"op": OP_ERROR, "detail": f"expected CONFIG, got {cfg!r}"})
        return 2
    backend_name = cfg["backend"]
    resources = cfg.get("resources") or {}

    backend = None
    try:
        # Construct + init the real backend (verified facts 7, 9).
        backend = get_backend(backend_name, resources=resources)
        backend = await backend           # backends are awaitable for init

        # Terminal states are needed by both sides — driver to know when to
        # resolve futures, child to know whether to send STATE vs COMPLETE.
        terminal_states = tuple(backend.get_task_states_map().terminal_states)

        loop = asyncio.get_running_loop()
        stop_event = asyncio.Event()
        send_lock = threading.Lock()

        def _safe_send(msg: dict) -> None:
            # All sends happen on the loop thread (callbacks fire in the loop's
            # context for the existing backends we target in v1).  Lock is
            # cheap insurance for backends that may invoke the callback off
            # the loop (mp.connection is full-duplex for socket-based
            # transports — concurrent send+recv is fine; concurrent SEND+SEND
            # is what the lock guards against).
            with send_lock:
                send_msg(conn, msg)

        def _callback(task: dict, state: str) -> None:
            uid = task.get("uid")
            if state in terminal_states:
                _safe_send({
                    "op": OP_COMPLETE,
                    "uid": uid,
                    "state": state,
                    "envelope": inline_envelope(task),
                })
            else:
                _safe_send({"op": OP_STATE, "uid": uid, "state": state})

        backend.register_callback(_callback)

        # READY: tells the driver the child is up and what its terminal states
        # are.  Sending ``terminal_states`` here is the minimal subset of the
        # state-map handshake; full StateMapper registration is D4.
        _safe_send({"op": OP_READY, "terminal_states": list(terminal_states)})

        # Command reader thread: blocking recv on conn, dispatch to the loop.
        def _reader() -> None:
            while True:
                try:
                    msg = recv_msg(conn)
                except (EOFError, OSError):
                    loop.call_soon_threadsafe(stop_event.set)
                    return
                except Exception as exc:
                    logger.warning("host reader recv failed: %s", exc)
                    loop.call_soon_threadsafe(stop_event.set)
                    return

                op = msg.get("op")
                if op == OP_SUBMIT:
                    tasks = msg.get("tasks") or []
                    asyncio.run_coroutine_threadsafe(backend.submit_tasks(tasks), loop)
                elif op == OP_CANCEL:
                    uid = msg.get("uid")
                    asyncio.run_coroutine_threadsafe(backend.cancel_task(uid), loop)
                elif op == OP_SHUTDOWN:
                    loop.call_soon_threadsafe(stop_event.set)
                    return
                else:
                    logger.warning("host: unknown op %r", op)

        threading.Thread(target=_reader, name="rhapsody-mp-host-reader",
                         daemon=True).start()

        # Run until the driver tells us to shut down (or the connection drops).
        await stop_event.wait()

    finally:
        if backend is not None:
            try:
                await backend.shutdown()
            except Exception:
                logger.exception("host: backend.shutdown raised")
        try:
            conn.close()
        except Exception:
            pass

    return 0


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("RHAPSODY_MP_LOGLEVEL", "WARNING"),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    rc = asyncio.run(_run())
    sys.exit(rc)


if __name__ == "__main__":
    main()
