"""Unit tests for the shared node sampler and its ``python -m`` entry point."""

import json
import os
import signal
import subprocess
import sys
import time

import pytest

pytest.importorskip("psutil", reason="psutil not installed")

from rhapsody.telemetry.node_sampler import NodeSampler
from rhapsody.telemetry.node_sampler import resource_events

_REPO_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "src"))

NODE_KEYS = {
    "cpu_percent",
    "memory_percent",
    "gpu_percent",
    "disk_read_bytes",
    "disk_write_bytes",
    "net_sent_bytes",
    "net_recv_bytes",
}


def test_sample_shape():
    sampler = NodeSampler("n0")
    node, gpus = sampler.sample()
    assert set(node) == NODE_KEYS
    assert isinstance(node["cpu_percent"], float)
    assert isinstance(node["memory_percent"], float)
    assert all(isinstance(k, int) and isinstance(v, float) for k, v in gpus.items())


def test_resource_events_node_and_gpus():
    node = dict.fromkeys(NODE_KEYS, 1.0)
    events = resource_events(
        "s", "b", "n0", node, {0: 10.0, 1: 20.0}, event_time=5.0, attributes={"flux_rank": 3}
    )
    assert [e.resource_scope for e in events] == ["per_node", "per_gpu", "per_gpu"]
    assert all(e.event_time == 5.0 and e.attributes == {"flux_rank": 3} for e in events)
    assert all(e.node_id == "n0" and e.backend == "b" for e in events)
    assert [(e.gpu_id, e.gpu_percent) for e in events[1:]] == [(0, 10.0), (1, 20.0)]
    assert events[1].cpu_percent is None and events[1].disk_read_bytes is None


def test_main_prints_json_and_stops_on_sigterm():
    env = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join([_REPO_SRC, os.environ.get("PYTHONPATH", "")]),
    }
    proc = subprocess.Popen(
        [sys.executable, "-m", "rhapsody.telemetry.node_sampler", "--interval", "0.1"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    try:
        first = json.loads(proc.stdout.readline())
        second = json.loads(proc.stdout.readline())
        proc.send_signal(signal.SIGTERM)
        t0 = time.monotonic()
        rc = proc.wait(timeout=5)
    finally:
        if proc.poll() is None:
            proc.kill()
    assert rc == 0
    assert time.monotonic() - t0 < 2
    assert set(first) == {"node_id", "rank", "time", "node", "gpus"}
    assert set(first["node"]) == NODE_KEYS
    assert first["rank"] is None or isinstance(first["rank"], int)
    assert second["time"] > first["time"]
    assert "found in sys.modules" not in proc.stderr.read()
