"""Tests for ``rhapsody.backends.multiproc`` — RemoteBackendProxy + backend-host.

These spawn real ``python -m rhapsody.backends.multiproc.host`` subprocesses, so
they exercise the full cross-process path (CONFIG/READY handshake, SUBMIT/
STATE/COMPLETE, envelope merge-back, clean shutdown).  They do not need any
cluster — they use the ``concurrent`` backend in each child.

The child must import the *source* tree, not the older installed ``rhapsody``
package in site-packages.  The ``_propagate_pythonpath`` autouse fixture
prepends ``src/`` to ``PYTHONPATH`` so the child resolves the new
``rhapsody.backends.multiproc`` package.
"""

from __future__ import annotations

import os

import pytest

from rhapsody import ComputeTask
from rhapsody import Session
from rhapsody.backends.multiproc import RemoteBackendProxy

_REPO_SRC = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "src")
)


@pytest.fixture(autouse=True)
def _propagate_pythonpath(monkeypatch):
    """Ensure the spawned host can ``import rhapsody.backends.multiproc``.

    The child inherits the parent's env (``os.environ``) verbatim; prepending
    the source dir to ``PYTHONPATH`` makes the source take precedence over any
    older installed copy in site-packages.
    """
    current = os.environ.get("PYTHONPATH", "")
    parts = current.split(os.pathsep) if current else []
    if _REPO_SRC not in parts:
        parts.insert(0, _REPO_SRC)
        monkeypatch.setenv("PYTHONPATH", os.pathsep.join(parts))


# ---------------------------------------------------------------------------
# Phase 1 — proxy round-trip against a single proxied partition.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_proxy_command_roundtrip():
    """An executable task echoes through the proxy and returns stdout/exit_code."""
    proxy = await RemoteBackendProxy(name="p0", backend="concurrent")
    try:
        async with Session(backends=[proxy]) as session:
            task = ComputeTask(
                executable="/bin/echo", arguments=["hello"], backend="p0",
            )
            await session.submit_tasks([task])
            await session.wait_tasks([task], timeout=20)

            assert task["state"] == "DONE"
            assert task["exit_code"] == 0
            assert "hello" in (task.get("stdout") or "")
    finally:
        await proxy.shutdown()


@pytest.mark.asyncio
async def test_proxy_function_roundtrip():
    """A lambda function task round-trips its return_value through the envelope."""
    proxy = await RemoteBackendProxy(name="p0", backend="concurrent")
    try:
        async with Session(backends=[proxy]) as session:
            task = ComputeTask(
                function=(lambda x, y: x * y), args=(6, 7), backend="p0",
            )
            await session.submit_tasks([task])
            await session.wait_tasks([task], timeout=20)

            assert task["state"] == "DONE"
            assert task["return_value"] == 42
            assert task["exit_code"] == 0
    finally:
        await proxy.shutdown()


@pytest.mark.asyncio
async def test_proxy_clean_shutdown_is_idempotent():
    """Shutdown is safe to call twice and tears down the child cleanly."""
    proxy = await RemoteBackendProxy(name="p0", backend="concurrent")
    await proxy.shutdown()
    await proxy.shutdown()  # no-op the second time


# ---------------------------------------------------------------------------
# Phase 2 — multiple proxied partitions + cross-partition dependency.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_two_partitions_with_cross_dependency():
    """Cross-partition dep is plain user async/await: result from p0 → arg to p1.

    No driver-side cut-edge machinery is invoked — the result returns to the
    driver via ``COMPLETE``, the user reads it, and submits the dependent task
    to the other partition with the value materialized as an arg.
    """
    p0 = await RemoteBackendProxy(name="p0", backend="concurrent")
    p1 = await RemoteBackendProxy(name="p1", backend="concurrent")
    try:
        async with Session(backends=[p0, p1]) as session:
            # Produce on p0
            t0 = ComputeTask(function=(lambda: 17), backend="p0")
            await session.submit_tasks([t0])
            await session.wait_tasks([t0], timeout=20)
            assert t0["return_value"] == 17

            # Consume on p1, using t0's result
            t1 = ComputeTask(
                function=(lambda x: x + 25),
                args=(t0["return_value"],),
                backend="p1",
            )
            await session.submit_tasks([t1])
            await session.wait_tasks([t1], timeout=20)
            assert t1["return_value"] == 42
    finally:
        await p0.shutdown()
        await p1.shutdown()


@pytest.mark.asyncio
async def test_session_terminal_states_picked_up_from_proxy():
    """``Session.add_backend`` reads ``terminal_states`` from the proxy shim;
    if it didn't, the proxied task's future would never resolve."""
    proxy = await RemoteBackendProxy(name="p0", backend="concurrent")
    try:
        async with Session(backends=[proxy]) as session:
            # If terminal_states were missing, this would time out:
            task = ComputeTask(executable="/bin/true", backend="p0")
            await session.submit_tasks([task])
            await session.wait_tasks([task], timeout=10)
            assert task["state"] == "DONE"
    finally:
        await proxy.shutdown()
