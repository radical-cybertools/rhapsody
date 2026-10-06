"""Unit tests for FluxTelemetryAdapter and flux adapter selection.

``flux exec`` is replaced by a small Python child that prints canned sampler
lines, so the subprocess, reader-thread and stop paths run for real without a
flux instance.
"""

import asyncio
import json
import logging
import sys
import time
from types import SimpleNamespace
from unittest.mock import patch

import pytest

pytest.importorskip("opentelemetry", reason="opentelemetry-sdk not installed")

from rhapsody.backends.execution.flux.flux_backend import FluxExecutionBackend
from rhapsody.backends.multiproc import RemoteBackendProxy
from rhapsody.telemetry.adapters.flux import FluxTelemetryAdapter
from rhapsody.telemetry.events import ResourceUpdate
from rhapsody.telemetry.manager import TelemetryManager

from .conftest import AdapterCapabilities
from .conftest import assert_resource_update_contract

FLUX_CAPS = AdapterCapabilities(name="flux", has_gpu_aggregate=True, has_gpu_per_device=True)
FLUX_CAPS_NO_GPU = AdapterCapabilities(name="flux")


def _line(rank, node_id="node1", gpus=None, t=None):
    gpus = {"0": 30.0, "1": 70.0} if gpus is None else gpus
    return json.dumps(
        {
            "node_id": node_id,
            "rank": rank,
            "time": t if t is not None else time.time(),
            "node": {
                "cpu_percent": 12.5,
                "memory_percent": 40.0,
                "gpu_percent": max(gpus.values()) if gpus else None,
                "disk_read_bytes": 1.0,
                "disk_write_bytes": 2.0,
                "net_sent_bytes": 3.0,
                "net_recv_bytes": 4.0,
            },
            "gpus": gpus,
        }
    )


def _fake_exec(lines, then="exit"):
    """Return a command that prints ``lines`` and then exits, sleeps, or ignores SIGTERM."""
    body = f"import sys, time, signal\nfor l in {lines!r}:\n    print(l, flush=True)\n"
    if then == "sleep":
        body += "time.sleep(60)\n"
    elif then == "ignore_term":
        body += "signal.signal(signal.SIGTERM, signal.SIG_IGN)\ntime.sleep(60)\n"
    return [sys.executable, "-c", body]


@pytest.fixture
async def manager():
    m = TelemetryManager(session_id="test-session")
    await m.start()
    yield m
    if m._running:
        await m.stop()


def _collect(manager):
    received = []
    manager.subscribe(lambda e: received.append(e) if isinstance(e, ResourceUpdate) else None)
    return received


def _adapter(cmd, **kwargs):
    adapter = FluxTelemetryAdapter(
        session_id="test-session", backend_name="flux0", uri="ssh://h/x", **kwargs
    )
    adapter._command = lambda: cmd
    return adapter


async def _wait_for(pred, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return
        await asyncio.sleep(0.02)


class TestFluxTelemetryAdapter:
    async def test_command_runs_sampler_on_all_ranks(self):
        adapter = FluxTelemetryAdapter(
            session_id="s", backend_name="flux0", uri="ssh://h/x", interval=2.0
        )
        cmd = adapter._command()
        assert cmd[:4] == ["flux", "exec", "-r", "all"]
        assert cmd[4:] == [
            sys.executable,
            "-m",
            "rhapsody.telemetry.node_sampler",
            "--interval",
            "2.0",
        ]

    async def test_sets_flux_uri(self, manager):
        adapter = FluxTelemetryAdapter(session_id="s", backend_name="flux0", uri="ssh://h/x")
        manager.register_adapter(adapter)
        with patch("subprocess.Popen") as popen:
            adapter.start()
        assert popen.call_args.kwargs["env"]["FLUX_URI"] == "ssh://h/x"

    async def test_lines_become_events(self, manager):
        received = _collect(manager)
        t0 = time.time() - 10
        adapter = _adapter(_fake_exec([_line(0, "n0", t=t0), _line(1, "n1", gpus={})]))
        manager.register_adapter(adapter)
        adapter.start()
        await _wait_for(lambda: len(received) >= 4)
        adapter.stop()

        for ev in received:
            assert_resource_update_contract(
                ev, FLUX_CAPS if ev.node_id == "n0" else FLUX_CAPS_NO_GPU
            )
            assert ev.backend == "flux0"
            assert ev.session_id == "test-session"

        per_node = {e.node_id: e for e in received if e.resource_scope == "per_node"}
        assert set(per_node) == {"n0", "n1"}
        assert per_node["n0"].attributes == {"flux_rank": 0}
        assert per_node["n1"].attributes == {"flux_rank": 1}
        assert per_node["n0"].event_time == t0
        assert per_node["n0"].gpu_percent == 70.0
        assert per_node["n1"].gpu_percent is None

        per_gpu = sorted(
            (e for e in received if e.resource_scope == "per_gpu"), key=lambda e: e.gpu_id
        )
        assert [(e.node_id, e.gpu_id, e.gpu_percent) for e in per_gpu] == [
            ("n0", 0, 30.0),
            ("n0", 1, 70.0),
        ]

    async def test_malformed_lines_are_skipped(self, manager):
        received = _collect(manager)
        adapter = _adapter(_fake_exec(["not json", '{"node_id": "x"}', _line(None, gpus={})]))
        manager.register_adapter(adapter)
        adapter.start()
        await _wait_for(lambda: len(received) >= 1)
        await asyncio.sleep(0.1)
        adapter.stop()

        assert len(received) == 1
        assert_resource_update_contract(received[0], FLUX_CAPS_NO_GPU)
        assert received[0].node_id == "node1"
        assert received[0].attributes == {}

    async def test_early_exit_warns(self, manager, caplog):
        adapter = _adapter(_fake_exec([]))
        manager.register_adapter(adapter)
        with caplog.at_level(logging.WARNING, logger="rhapsody.telemetry.adapters.flux"):
            adapter.start()
            await _wait_for(lambda: "exited early" in caplog.text)
        adapter.stop()
        assert "exited early" in caplog.text

    async def test_stop_terminates_samplers(self, manager, caplog):
        adapter = _adapter(_fake_exec([], then="sleep"))
        manager.register_adapter(adapter)
        adapter.start()
        t0 = time.monotonic()
        with caplog.at_level(logging.WARNING, logger="rhapsody.telemetry.adapters.flux"):
            adapter.stop()
            await asyncio.sleep(0.1)
        assert time.monotonic() - t0 < 3
        assert adapter._proc.poll() is not None
        assert "exited early" not in caplog.text

    async def test_stop_kills_when_term_ignored(self, manager):
        adapter = _adapter(_fake_exec(["ready"], then="ignore_term"), stop_timeout=0.5)
        manager.register_adapter(adapter)
        adapter.start()
        await asyncio.sleep(0.3)  # let the child install its SIGTERM handler
        adapter.stop()
        assert adapter._proc.poll() is not None

    async def test_missing_flux_is_not_fatal(self, manager, caplog):
        adapter = _adapter(["/nonexistent/flux", "exec"])
        manager.register_adapter(adapter)
        with caplog.at_level(logging.WARNING, logger="rhapsody.telemetry.adapters.flux"):
            adapter.start()
        assert "Could not start flux node samplers" in caplog.text
        adapter.stop()

    def test_stop_before_start_is_safe(self):
        FluxTelemetryAdapter(session_id="s", backend_name="b", uri="u").stop()


class TestFluxAdapterSelection:
    def _attach(self, backend, name="b0"):
        m = TelemetryManager(session_id="s")
        m.attach_backend(backend, session_id="s", backend_name=name)
        return m._adapters

    def test_flux_backend_info(self):
        backend = FluxExecutionBackend(uri="ssh://h/x")
        assert backend.telemetry_info() == {"kind": "flux", "uri": "ssh://h/x"}
        assert FluxExecutionBackend().telemetry_info() is None

    def test_flux_backend_gets_flux_adapter(self):
        adapters = self._attach(FluxExecutionBackend(uri="local:///tmp/f"))
        assert len(adapters) == 1
        assert isinstance(adapters[0], FluxTelemetryAdapter)
        assert adapters[0]._uri == "local:///tmp/f"

    def test_proxied_flux_gets_flux_adapter(self):
        proxy = RemoteBackendProxy(name="part0", backend="flux")
        proxy._host = SimpleNamespace(telemetry_info={"kind": "flux", "uri": "ssh://n1/x"})
        adapters = self._attach(proxy, name="part0")
        assert len(adapters) == 1
        assert isinstance(adapters[0], FluxTelemetryAdapter)
        assert adapters[0]._uri == "ssh://n1/x"
        assert adapters[0]._backend_name == "part0"

    def test_proxy_without_info_gets_no_adapter(self):
        proxy = RemoteBackendProxy(name="part0", backend="concurrent")
        assert proxy.telemetry_info() is None
        proxy._host = SimpleNamespace(telemetry_info=None)
        assert self._attach(proxy) == []

    def test_failing_telemetry_info_falls_through(self, caplog):
        class Broken:
            def telemetry_info(self):
                raise RuntimeError("boom")

        with caplog.at_level(logging.WARNING, logger="rhapsody.telemetry.manager"):
            assert self._attach(Broken()) == []
        assert "telemetry_info() failed" in caplog.text
