"""Telemetry for flux-backed sessions on a single host.

Partition-launched flux instances (fake ``srun`` shim, see
``test_flux_backend_partitioned.py``) give multi-broker instances on
localhost, reached via the ssh ``r_uri``.  The tests assert that

- task lifecycle events flow for flux tasks, and
- ``FluxTelemetryAdapter`` delivers ``ResourceUpdate`` events from every
  broker rank — for a directly attached backend and for partitions hosted
  behind ``RemoteBackendProxy``.
"""

import asyncio
import os
import socket
import stat
from types import SimpleNamespace

import pytest

pytest.importorskip("opentelemetry", reason="opentelemetry-sdk not installed")

from test_flux_backend_partitioned import _FAKE_SRUN
from test_flux_backend_partitioned import skip_if_no_ssh_to_self  # noqa: F401

from rhapsody import ComputeTask
from rhapsody import Session
from rhapsody.telemetry.events import ResourceUpdate

_REPO_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src"))


@pytest.fixture
def fake_srun(tmp_path, monkeypatch):
    shim = tmp_path / "srun"
    shim.write_text(_FAKE_SRUN)
    shim.chmod(shim.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")
    # Proxy children and flux-exec'd samplers must import the source tree.
    parts = [p for p in os.environ.get("PYTHONPATH", "").split(os.pathsep) if p]
    if _REPO_SRC not in parts:
        monkeypatch.setenv("PYTHONPATH", os.pathsep.join([_REPO_SRC, *parts]))


def _partition(n):
    # SimpleNamespace pickles by reference, so the spec survives the proxy wire.
    host = socket.gethostname()
    return {"nodelist": [SimpleNamespace(name=host) for _ in range(n)], "env": {}}


def _ranks(events, backend):
    return {
        e.attributes.get("flux_rank")
        for e in events
        if isinstance(e, ResourceUpdate) and e.backend == backend and e.resource_scope == "per_node"
    }


async def _wait_for(pred, timeout=30.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline and not pred():
        await asyncio.sleep(0.1)


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.usefixtures("skip_if_no_flux", "skip_if_no_ssh_to_self", "fake_srun")
async def test_flux_session_telemetry():
    """Task events and per-rank resource events for a 2-broker flux backend."""
    from rhapsody.backends.execution.flux.flux_backend import FluxExecutionBackend

    backend = await FluxExecutionBackend(name="flux0", resources={"partition": _partition(2)})
    events = []
    async with Session(backends=[backend]) as session:
        telemetry = await session.start_telemetry(resource_poll_interval=0.5)
        telemetry.subscribe(events.append)

        tasks = [
            ComputeTask(executable="/bin/echo", arguments=[str(i)], backend="flux0")
            for i in range(4)
        ]
        await session.submit_tasks(tasks)
        await session.wait_tasks(tasks, timeout=60)
        assert all(t["state"] == "DONE" for t in tasks)

        await _wait_for(lambda: _ranks(events, "flux0") == {0, 1})

    assert _ranks(events, "flux0") == {0, 1}

    uids = {t["uid"] for t in tasks}
    for kind in ("TaskCreated", "TaskSubmitted", "TaskStarted", "TaskCompleted"):
        seen = {e.task_id for e in events if e.event_type == kind}
        assert uids <= seen, (kind, uids - seen)
    assert all(e.backend == "flux0" for e in events if e.task_id in uids), {
        e.backend for e in events if e.task_id in uids
    }


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.usefixtures("skip_if_no_flux", "skip_if_no_ssh_to_self", "fake_srun")
async def test_flux_partitions_behind_proxy_telemetry():
    """Two proxied flux partitions each get their own resource adapter."""
    from rhapsody.backends.multiproc import RemoteBackendProxy

    proxies = await asyncio.gather(
        *(
            RemoteBackendProxy(name=name, backend="flux", resources={"partition": _partition(2)})
            for name in ("p0", "p1")
        )
    )
    events = []
    try:
        async with Session(backends=list(proxies)) as session:
            telemetry = await session.start_telemetry(resource_poll_interval=0.5)
            telemetry.subscribe(events.append)

            tasks = [
                ComputeTask(executable="/bin/true", backend=name)
                for name in ("p0", "p1")
                for _ in range(2)
            ]
            await session.submit_tasks(tasks)
            await session.wait_tasks(tasks, timeout=60)
            assert all(t["state"] == "DONE" for t in tasks)

            await _wait_for(lambda: _ranks(events, "p0") == _ranks(events, "p1") == {0, 1})
    finally:
        for proxy in proxies:
            await proxy.shutdown()

    assert _ranks(events, "p0") == {0, 1}
    assert _ranks(events, "p1") == {0, 1}
