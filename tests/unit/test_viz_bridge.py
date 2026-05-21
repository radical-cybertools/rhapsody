"""Smoke tests for the dev viz: fake_dragon_stream.py and the SSE bridge.

These are integration-light tests — they shell out to the scripts in
examples/telemetry/viz/ and assert structural properties of what comes out.
The goal is to catch regressions in the synthetic data format and the
SSE wire format without standing up a real Dragon session.
"""

from __future__ import annotations

import http.client
import json
import socket
import subprocess
import sys
import threading
import time
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
GEN  = REPO / "examples" / "telemetry" / "viz" / "fake_dragon_stream.py"
BRIDGE = REPO / "examples" / "telemetry" / "viz" / "bridge.py"

# Allow `from bridge import LiveVizBridge` for in-process tests without
# requiring the example to be installed as a package.
sys.path.insert(0, str(REPO / "examples" / "telemetry" / "viz"))


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _wait_listening(port: int, timeout: float = 3.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return True
        except OSError:
            time.sleep(0.05)
    return False


def test_generator_emits_well_formed_events(tmp_path):
    out = tmp_path / "sample.jsonl"
    rc = subprocess.run(
        [sys.executable, str(GEN), "--tasks", "20", "--seed", "1", "--out", str(out)],
        check=True,
        capture_output=True,
    )
    assert rc.returncode == 0
    lines = out.read_text().splitlines()
    assert len(lines) > 20  # at least one event per task plus session events
    events = [json.loads(l) for l in lines if l.strip()]
    types = {e["event_type"] for e in events}
    assert "SessionStarted" in types
    assert "ResourceLayout" in types
    assert "TaskStarted" in types
    assert "TaskCompleted" in types

    # TaskStarted carries placement
    started = next(e for e in events if e["event_type"] == "TaskStarted")
    placement = started["attributes"].get("placement")
    assert placement is not None
    assert "node_id" in placement
    assert "core_ids" in placement
    assert "gpu_ids" in placement

    # event_time across the stream is not strictly monotonic — completions
    # naturally interleave with submissions in real telemetry too. We just
    # require that every event has a sane timestamp.
    for e in events:
        assert isinstance(e.get("event_time"), (int, float))
        assert e["event_time"] > 0


def test_bridge_replay_streams_every_line(tmp_path):
    sample = tmp_path / "sample.jsonl"
    subprocess.run(
        [sys.executable, str(GEN), "--tasks", "10", "--seed", "2", "--out", str(sample)],
        check=True,
        capture_output=True,
    )
    n_lines = len([l for l in sample.read_text().splitlines() if l.strip()])
    assert n_lines > 0

    port = _free_port()
    proc = subprocess.Popen(
        [sys.executable, str(BRIDGE), "--host", "127.0.0.1", "--port", str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        assert _wait_listening(port), "bridge did not start listening"
        url = (
            f"http://127.0.0.1:{port}/replay?"
            f"file={sample}&speed=inf"
        )
        with urllib.request.urlopen(url, timeout=4.0) as resp:
            body = resp.read().decode("utf-8")
        data_lines = [l for l in body.splitlines() if l.startswith("data: ")]
        assert len(data_lines) == n_lines, (
            f"expected {n_lines} SSE data lines, got {len(data_lines)}"
        )
        # Each data line should be a valid JSON event
        for d in data_lines[:5]:
            parsed = json.loads(d[len("data: "):])
            assert "event_type" in parsed
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=3.0)
        except subprocess.TimeoutExpired:
            proc.kill()


def test_live_bridge_streams_subscriber_events():
    """LiveVizBridge.emit() → /live SSE: events arrive without any file I/O."""
    from bridge import LiveVizBridge  # type: ignore[import-not-found]

    port = _free_port()
    bridge = LiveVizBridge(port=port)
    bridge.start()
    try:
        assert _wait_listening(port), "bridge did not start listening"

        received: list[str] = []
        reader_done = threading.Event()

        def reader():
            try:
                conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5.0)
                conn.request("GET", "/live")
                resp = conn.getresponse()
                # Read line-by-line; collect the JSON payloads of SSE
                # ``data:`` frames until we have what we expect.
                fp = resp.fp
                while len(received) < 3:
                    raw = fp.readline()
                    if not raw:
                        break
                    s = raw.decode("utf-8", errors="replace").rstrip("\r\n")
                    if s.startswith("data: "):
                        received.append(s[len("data: "):])
                conn.close()
            finally:
                reader_done.set()

        t = threading.Thread(target=reader, daemon=True)
        t.start()
        # Give the reader time to connect and register its queue.
        time.sleep(0.3)

        for i in range(3):
            bridge.emit({
                "event_type": "TaskStarted",
                "task_id": f"t{i}",
                "event_time": time.time(),
                "attributes": {"placement": {"node_id": "n0", "core_ids": [i]}},
            })

        assert reader_done.wait(timeout=3.0), "reader did not finish in time"
        assert len(received) == 3, f"expected 3 SSE data lines, got {len(received)}"
        parsed = [json.loads(r) for r in received]
        assert [p["task_id"] for p in parsed] == ["t0", "t1", "t2"]
        assert all(p["event_type"] == "TaskStarted" for p in parsed)
    finally:
        bridge.stop()


def test_live_bridge_replays_backlog_to_late_joiner():
    """Events emitted BEFORE a client connects must be replayed on connect.

    Covers the common race where the demo emits SessionStarted / ResourceLayout
    / TaskStarted events while the browser is still loading viz.js. Without
    the backlog, those events go to zero subscribers and the viewer renders
    nothing.
    """
    from bridge import LiveVizBridge  # type: ignore[import-not-found]

    port = _free_port()
    bridge = LiveVizBridge(port=port)
    bridge.start()
    try:
        assert _wait_listening(port)

        # Emit two "metadata" events BEFORE any client is connected.
        bridge.emit({
            "event_type": "SessionStarted",
            "event_time": time.time(),
        })
        bridge.emit({
            "event_type": "ResourceLayout",
            "event_time": time.time(),
            "attributes": {"nodes": [{"id": "n0", "cores": 8, "gpus": 0}]},
        })

        received: list[str] = []
        reader_done = threading.Event()

        def reader():
            try:
                conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5.0)
                conn.request("GET", "/live")
                resp = conn.getresponse()
                fp = resp.fp
                while len(received) < 3:
                    raw = fp.readline()
                    if not raw:
                        break
                    s = raw.decode("utf-8", errors="replace").rstrip("\r\n")
                    if s.startswith("data: "):
                        received.append(s[len("data: "):])
                conn.close()
            finally:
                reader_done.set()

        t = threading.Thread(target=reader, daemon=True)
        t.start()
        # Brief wait so the reader connects, then emit one live event.
        time.sleep(0.3)
        bridge.emit({"event_type": "TaskStarted", "task_id": "late-1"})

        assert reader_done.wait(timeout=3.0)
        types = [json.loads(r).get("event_type") for r in received]
        # Backlog is replayed first, then the live event.
        assert types == ["SessionStarted", "ResourceLayout", "TaskStarted"], types
    finally:
        bridge.stop()


def test_live_bridge_backlog_excludes_completed_tasks():
    """A task that has completed before client connect must not be replayed.

    Replaying a finished task to a late joiner only shows its tile falling
    out of frame with no preceding flight — meaningless. So the backlog
    keeps session-scope events and *active* task events only; terminal
    events drop the task's whole history.
    """
    from bridge import LiveVizBridge  # type: ignore[import-not-found]

    port = _free_port()
    bridge = LiveVizBridge(port=port)
    bridge.start()
    try:
        assert _wait_listening(port)

        # Session bootstrap.
        bridge.emit({"event_type": "SessionStarted", "event_time": 1.0})
        bridge.emit({
            "event_type": "ResourceLayout",
            "event_time": 1.0,
            "attributes": {"nodes": [{"id": "n0", "cores": 4, "gpus": 0}]},
        })

        # A task that runs to completion BEFORE the client connects.
        for et in ("TaskCreated", "TaskSubmitted", "TaskStarted", "TaskCompleted"):
            bridge.emit({"event_type": et, "task_id": "done-1", "event_time": 2.0})

        # A task that is currently in flight.
        for et in ("TaskCreated", "TaskSubmitted", "TaskStarted"):
            bridge.emit({"event_type": et, "task_id": "live-1", "event_time": 3.0})

        received: list[dict] = []
        reader_done = threading.Event()

        def reader():
            try:
                conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5.0)
                conn.request("GET", "/live")
                resp = conn.getresponse()
                fp = resp.fp
                # 2 session events + 3 active-task events = 5 expected
                while len(received) < 5:
                    raw = fp.readline()
                    if not raw:
                        break
                    s = raw.decode("utf-8", errors="replace").rstrip("\r\n")
                    if s.startswith("data: "):
                        try:
                            received.append(json.loads(s[len("data: "):]))
                        except Exception:
                            pass
                conn.close()
            finally:
                reader_done.set()

        t = threading.Thread(target=reader, daemon=True)
        t.start()
        assert reader_done.wait(timeout=3.0)

        types = [(e.get("event_type"), e.get("task_id")) for e in received]
        # No "done-1" anywhere.
        assert all(tid != "done-1" for _, tid in types), types
        # Both session events present.
        assert ("SessionStarted", None) in types
        assert ("ResourceLayout", None) in types
        # All three live events for the in-flight task present.
        live = [t for t in types if t[1] == "live-1"]
        assert [et for et, _ in live] == ["TaskCreated", "TaskSubmitted", "TaskStarted"], live
    finally:
        bridge.stop()


def test_live_bridge_sends_shutdown_event_on_stop():
    """bridge.stop() must signal every connected /live client so the
    viewer can drop its state back to the "pending" placeholder
    instead of leaving a stale slot grid on screen."""
    from bridge import LiveVizBridge  # type: ignore[import-not-found]

    port = _free_port()
    bridge = LiveVizBridge(port=port)
    bridge.start()

    saw_shutdown_with_data = threading.Event()
    connected   = threading.Event()
    error: list[str] = []

    def reader():
        try:
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5.0)
            conn.request("GET", "/live")
            resp = conn.getresponse()
            connected.set()
            fp = resp.fp
            saw_event_line = False
            while True:
                raw = fp.readline()
                if not raw:
                    break
                s = raw.decode("utf-8", errors="replace").rstrip("\r\n")
                if s == "event: shutdown":
                    saw_event_line = True
                    continue
                if saw_event_line and s.startswith("data:"):
                    # A data line (even empty) is required by the SSE
                    # spec for the named event to actually dispatch in
                    # the browser. Without this, ``event: shutdown``
                    # alone is silently dropped.
                    saw_shutdown_with_data.set()
                    break
                if saw_event_line and s == "":
                    # Blank line before any data line — would never
                    # dispatch in a real browser.  Don't set the flag.
                    saw_event_line = False
            conn.close()
        except Exception as e:
            error.append(repr(e))

    t = threading.Thread(target=reader, daemon=True)
    t.start()
    try:
        assert connected.wait(timeout=3.0), "reader never connected"
        # Give the handler a moment to register its queue.
        time.sleep(0.2)
        bridge.stop()
        assert saw_shutdown_with_data.wait(timeout=3.0), (
            f"shutdown event with data not received; errors: {error}"
        )
    finally:
        bridge.stop()  # idempotent
        t.join(timeout=2.0)


def test_live_bridge_drops_on_overflow_without_blocking():
    """A slow/disconnected viewer must never back up the emitter."""
    from bridge import LiveVizBridge  # type: ignore[import-not-found]

    bridge = LiveVizBridge(port=_free_port(), queue_size=5)
    bridge.start()
    try:
        # Register a client queue but never read from it.
        q, _backlog = bridge._register_client()
        # Emit far more than the queue can hold; emit must not block or raise.
        start = time.monotonic()
        for i in range(500):
            bridge.emit({"event_type": "TaskStarted", "task_id": f"t{i}"})
        elapsed = time.monotonic() - start
        assert elapsed < 0.5, f"emit blocked under overflow ({elapsed:.3f}s)"
        # Queue capped at maxsize — older items dropped, no exceptions raised.
        assert q.qsize() <= 5
    finally:
        bridge.stop()


def test_bridge_serves_static_index():
    port = _free_port()
    proc = subprocess.Popen(
        [sys.executable, str(BRIDGE), "--host", "127.0.0.1", "--port", str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        assert _wait_listening(port)
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=2.0) as r:
            body = r.read().decode("utf-8")
        assert "<canvas" in body and "viz.js" in body
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/viz.js", timeout=2.0) as r:
            js = r.read().decode("utf-8")
        assert "EventSource" in js
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=3.0)
        except subprocess.TimeoutExpired:
            proc.kill()
