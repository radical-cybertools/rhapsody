"""Control-plane wire format for rhapsody.backends.multiproc.

Used **directly** by ``proxy.py`` and ``host.py`` — there is no Transport
abstraction yet (decision §3.6 in ``rhapsody-partitions.md``: localized single
implementation; extract a seam only when a second transport appears).

Messages travel over a :class:`multiprocessing.connection.Connection` and are
framed with ``send_bytes`` / ``recv_bytes`` after being serialized with
cloudpickle (so task functions and arbitrary ``return_value`` / ``exception``
payloads ride through, matching the edge backend's choice — same Python-version-
skew caveat applies).

Every ``COMPLETE`` carries a *result envelope* (decision §3.7: hard v1
requirement, **shape only**).  v1 only ever emits ``kind="inline"``; the
``handle`` branch is reserved in the wire format so D5 is a non-breaking add.
"""

from __future__ import annotations

from typing import Any

import cloudpickle

# ---------------------------------------------------------------------------
# Operation codes (stable contract — see §6.3 of the plan).
# ---------------------------------------------------------------------------
# driver → child
OP_CONFIG = "CONFIG"
OP_SUBMIT = "SUBMIT"
OP_CANCEL = "CANCEL"
OP_SHUTDOWN = "SHUTDOWN"

# child → driver
OP_READY = "READY"
OP_STATE = "STATE"
OP_COMPLETE = "COMPLETE"
OP_ERROR = "ERROR"

# Result fields that survive the cross-process merge.  Mirrors the fields
# initialised by ``api.task.BaseTask.__init__`` and populated by backends
# during execution (verified fact 3).  Non-result keys on the child's task
# (notably ``concurrent``'s ``"future"`` key, which holds an ``asyncio.Task``
# and is NOT picklable) are excluded by sending only this subset back.
RESULT_FIELDS = (
    "state",
    "stdout",
    "stderr",
    "exit_code",
    "return_value",
    "response",
    "exception",
)


def send_msg(conn, msg: dict) -> None:
    """Serialize ``msg`` with cloudpickle and send over ``conn``.

    The whole message is one cloudpickled blob, framed by ``send_bytes``.
    """
    conn.send_bytes(cloudpickle.dumps(msg))


def recv_msg(conn) -> dict:
    """Receive one message from ``conn`` and cloudpickle-load it.

    Raises :class:`EOFError` when the peer closes.
    """
    return cloudpickle.loads(conn.recv_bytes())


def inline_envelope(task: dict) -> dict:
    """Build a ``kind="inline"`` envelope from ``task``'s result fields.

    Only :data:`RESULT_FIELDS` are extracted — the child's task object may carry
    backend-internal keys (e.g. concurrent's ``"future"``) that don't pickle.
    """
    payload = {k: task.get(k) for k in RESULT_FIELDS}
    return {"kind": "inline", "payload": payload}


def merge_envelope_into_task(task: dict, envelope: dict) -> None:
    """Merge a result envelope into a driver-side task object in-place.

    v1 only handles ``kind="inline"``; ``"handle"`` raises NotImplementedError
    (deferred D5).  This is the cross-process analogue of how an in-process
    backend mutates the task before firing the callback (fact 3).
    """
    kind = envelope.get("kind")
    if kind == "inline":
        for k, v in envelope["payload"].items():
            task[k] = v
        return
    if kind == "handle":
        raise NotImplementedError(
            "handle-kind envelopes are deferred (see rhapsody-partitions.md D5)"
        )
    raise ValueError(f"unknown envelope kind: {kind!r}")


# ---------------------------------------------------------------------------
# Tiny shim returned by RemoteBackendProxy.get_task_states_map() — Session
# only reads ``.terminal_states`` from it (verified fact 6).  Using a shim
# (rather than the global StateMapper registry) keeps the proxy free of
# class-name registry games; the full StateMapper registration is D4.
# ---------------------------------------------------------------------------
class TerminalStatesShim:
    """Minimal object exposing ``terminal_states`` for ``Session.add_backend``."""

    __slots__ = ("terminal_states",)

    def __init__(self, terminal_states: tuple[Any, ...]):
        self.terminal_states = tuple(terminal_states)
