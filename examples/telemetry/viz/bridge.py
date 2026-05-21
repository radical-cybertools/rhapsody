#!/usr/bin/env python3
"""SSE bridge for the RHAPSODY live-viz.

Two endpoints share one stdlib HTTP server (``http.server``):

  GET /              - serves index.html + viz.js (static)
  GET /replay?file=  - paces a JSONL through SSE  (post-mortem replay)
  GET /live          - streams events pushed via LiveVizBridge.emit()

For **live** viewing, instantiate :class:`LiveVizBridge` inside the
rhapsody session script and register its ``emit`` method as a subscriber
on the :class:`~rhapsody.telemetry.manager.TelemetryManager`::

    from examples.telemetry.viz.bridge import LiveVizBridge

    bridge = LiveVizBridge(port=8765)
    bridge.start()

    telemetry = await session.start_telemetry()   # no checkpoint_path!
    telemetry.subscribe(bridge.emit)

The browser connects to ``http://127.0.0.1:8765/?live`` and events flow:
``manager.emit() → dispatch loop → bridge.emit() → per-client Queue →
SSE write``.  No file system involved.

For **replay**, run this script from the CLI::

    python examples/telemetry/viz/bridge.py --replay path.jsonl --open

Server-Sent Events were chosen over WebSockets because (a) one-way is
enough for the viewer, (b) the wire is plain text-over-HTTP, so
``curl http://localhost:8765/replay?file=...`` debugs instantly, and
(c) browser reconnect-on-drop is free.
"""

from __future__ import annotations

import argparse
import collections
import dataclasses
import json
import os
import queue
import sys
import threading
import time
import urllib.parse
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent


# ----- SSE framing ---------------------------------------------------------


def _sse_chunk(payload: str, event: str | None = None) -> bytes:
    out: list[str] = []
    if event:
        out.append(f"event: {event}")
    # SSE spec: an event with an empty data buffer is NOT dispatched by
    # the browser.  Always emit at least one ``data:`` line so named
    # events (``event: shutdown``, ``event: eof``) actually fire their
    # listeners even when the payload is empty.
    if not payload:
        out.append("data:")
    else:
        for line in payload.splitlines():
            out.append(f"data: {line}")
    out.append("")
    out.append("")
    return ("\n".join(out)).encode("utf-8")


def _stream_existing(path: str):
    """Yield every existing line in ``path`` (one shot)."""
    with open(path) as fh:
        for line in fh:
            line = line.rstrip("\n")
            if line:
                yield line


# ----- Event → JSON line ---------------------------------------------------


def _serialize(event: Any) -> str:
    """Serialize a BaseEvent dataclass (or a plain dict) to one JSON line.

    Matches the format the JSONL checkpoint writer uses, so the browser
    has a single code path for live and replay.
    """
    if isinstance(event, dict):
        d = dict(event)
    else:
        try:
            d = {f.name: getattr(event, f.name) for f in dataclasses.fields(event)}
        except TypeError:
            d = {"event_type": str(type(event).__name__), "value": str(event)}
    d.setdefault("name", d.get("event_type") or "")
    d.setdefault("section", "event")
    return json.dumps(d, default=str)


# ----- LiveVizBridge -------------------------------------------------------


class LiveVizBridge:
    """In-process SSE bridge.

    Lifecycle:

    1. ``bridge = LiveVizBridge(port=8765)``
    2. ``bridge.start()``   — non-blocking; spawns a daemon HTTP thread.
    3. ``telemetry.subscribe(bridge.emit)``
    4. (session runs)
    5. ``bridge.stop()``    — closes connections, joins the thread.

    Thread safety:
      - ``emit()`` is called by the telemetry dispatch loop.  It does only
        thread-safe queue ops (``put_nowait``) and never blocks.  Slow
        viewers cause events to be dropped, never back-pressured upstream.
      - Each ``/live`` request runs on its own HTTP worker thread and
        blocks on ``queue.get()``.  A ``None`` sentinel pushed by
        ``stop()`` wakes them for clean shutdown.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 8765,
        *,
        queue_size: int = 10_000,
        backlog_size: int = 5_000,
    ) -> None:
        self.host = host
        self.port = port
        self._queue_size = queue_size
        self._clients: list[queue.Queue] = []
        # Backlog for late-joining /live clients.  Split in two so the
        # filtering rules are obvious:
        #
        #   _session_backlog : SessionStarted / ResourceLayout / SessionEnded
        #                      — small, kept for the bridge's lifetime so
        #                      topology is always replayed
        #
        #   _task_backlog    : task-scope events for tasks that have NOT
        #                      yet completed.  A terminal event drops the
        #                      task's whole history and is itself not stored
        #                      — replaying "Created → Started → Completed"
        #                      to a new client just shows tiles falling
        #                      out of frame with no context.
        self._session_backlog: list[str] = []
        self._task_backlog: collections.deque[tuple[str, str]] = collections.deque(
            maxlen=backlog_size
        )
        self._lock = threading.Lock()
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self._stopped = threading.Event()

    # ---- public API ------------------------------------------------------

    def emit(self, event: Any) -> None:
        """Subscriber callback.  Thread-safe and non-blocking."""
        try:
            line = _serialize(event)
        except Exception:
            return
        # Cheap to read off the source object — avoids re-parsing JSON.
        if isinstance(event, dict):
            etype = event.get("event_type")
            task_id = event.get("task_id")
        else:
            etype = getattr(event, "event_type", None)
            task_id = getattr(event, "task_id", None)
        with self._lock:
            if etype in ("TaskCompleted", "TaskFailed", "TaskCanceled") and task_id:
                # Drop the task's history from the backlog and skip storing
                # the terminal event itself.  Late joiners get a clean
                # "currently active tasks only" replay.
                if any(tid == task_id for tid, _ in self._task_backlog):
                    self._task_backlog = collections.deque(
                        ((tid, ln) for tid, ln in self._task_backlog if tid != task_id),
                        maxlen=self._task_backlog.maxlen,
                    )
            elif etype in ("SessionStarted", "SessionEnded", "ResourceLayout"):
                # Session-scope: kept for the bridge's lifetime so late
                # joiners always see the topology + session state.
                self._session_backlog.append(line)
            elif task_id:
                self._task_backlog.append((task_id, line))
            # Live-stream every event to currently connected clients
            # regardless of backlog policy — including events we don't
            # store (e.g. ResourceUpdate, custom user events).
            for q in self._clients:
                try:
                    q.put_nowait(line)
                except queue.Full:
                    # Drop on overflow — a slow viewer must never back up
                    # the telemetry dispatch loop.
                    pass

    def start(self) -> None:
        """Start the HTTP server on a daemon thread.  Idempotent."""
        if self._server is not None:
            return
        self._server = ThreadingHTTPServer(
            (self.host, self.port), self._make_handler()
        )
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name="viz-bridge",
            daemon=True,
        )
        self._thread.start()
        sys.stderr.write(f"[bridge] serving viz at http://{self.host}:{self.port}/\n")

    def stop(self) -> None:
        """Wake any blocked /live handlers, shut the server, drop clients."""
        if self._stopped.is_set():
            return
        self._stopped.set()
        with self._lock:
            for q in self._clients:
                try:
                    q.put_nowait(None)  # sentinel — wakes handler
                except queue.Full:
                    pass
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        self._thread = None

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}/"

    # ---- per-client bookkeeping -----------------------------------------

    def _register_client(self) -> tuple[queue.Queue, list[str]]:
        """Atomically register a new client and snapshot the current backlog.

        Returns ``(queue, backlog_snapshot)``.  Both are taken inside one lock
        so no event is either lost or duplicated across the handover from
        backlog-replay to live-streaming: every event emitted up to the
        snapshot is in the snapshot, and every event emitted after is in
        the queue.
        """
        q: queue.Queue = queue.Queue(maxsize=self._queue_size)
        with self._lock:
            # Session events first (the viewer relies on ResourceLayout
            # to size its grid), then the in-flight task events.
            snapshot = list(self._session_backlog) + [ln for _, ln in self._task_backlog]
            self._clients.append(q)
        return q, snapshot

    def _unregister_client(self, q: queue.Queue) -> None:
        with self._lock:
            try:
                self._clients.remove(q)
            except ValueError:
                pass

    # ---- request handler -------------------------------------------------

    def _make_handler(self):
        bridge = self

        class Handler(BaseHTTPRequestHandler):
            # Quieter logs — one line per request.
            def log_message(self, format, *args):  # noqa: A002 — base class API
                sys.stderr.write(
                    f"[bridge] {self.address_string()} - {format % args}\n"
                )

            # ---- /static
            def _serve_static(self, rel_path: str) -> None:
                if rel_path in ("", "/"):
                    rel_path = "index.html"
                rel_path = rel_path.lstrip("/")
                candidate = (HERE / rel_path).resolve()
                if HERE not in candidate.parents and candidate != HERE:
                    self.send_error(403)
                    return
                if not candidate.is_file():
                    self.send_error(404)
                    return
                ext = candidate.suffix.lower()
                ctype = {
                    ".html": "text/html; charset=utf-8",
                    ".js":   "application/javascript; charset=utf-8",
                    ".css":  "text/css; charset=utf-8",
                    ".json": "application/json",
                }.get(ext, "application/octet-stream")
                data = candidate.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(data)))
                # Dev-time setup — never let the browser cache the viz
                # assets, or a stale viz.js will mask any future edit.
                self.send_header("Cache-Control", "no-store, max-age=0")
                self.end_headers()
                self.wfile.write(data)

            # ---- /replay (still file-backed — post-mortem only)
            def _serve_replay(self, qs: dict[str, list[str]]) -> None:
                path = (qs.get("file") or [""])[0]
                if not path or not os.path.isfile(path):
                    self.send_error(404, "file not found")
                    return
                try:
                    speed = float((qs.get("speed") or ["1.0"])[0])
                except ValueError:
                    speed = 1.0
                self._begin_sse()
                prev_time: float | None = None
                try:
                    for raw in _stream_existing(path):
                        et: float | None = None
                        try:
                            et = float(json.loads(raw).get("event_time") or 0.0)
                        except Exception:
                            et = None
                        if (
                            speed != float("inf")
                            and prev_time is not None
                            and et is not None
                        ):
                            gap = max(0.0, (et - prev_time) / max(speed, 1e-9))
                            if gap > 0:
                                time.sleep(gap)
                        if et is not None:
                            prev_time = et
                        try:
                            self.wfile.write(_sse_chunk(raw))
                            self.wfile.flush()
                        except (BrokenPipeError, ConnectionResetError):
                            return
                    try:
                        self.wfile.write(_sse_chunk("", event="eof"))
                        self.wfile.flush()
                    except (BrokenPipeError, ConnectionResetError):
                        pass
                finally:
                    # Tell the server to close after this request — clients
                    # otherwise sit waiting on a keep-alive socket.
                    self.close_connection = True

            # ---- /live (in-memory subscriber feed)
            def _serve_live(self) -> None:
                q, backlog = bridge._register_client()
                self._begin_sse()
                # Replay the backlog first so a late-joining client still
                # sees SessionStarted + ResourceLayout + recent task events.
                # Tag each frame with ``event: backlog`` so the viewer
                # snaps the slot state instead of animating — replaying
                # a fade-in for a task that's been running for minutes
                # would be misleading.
                for raw in backlog:
                    try:
                        self.wfile.write(_sse_chunk(raw, event="backlog"))
                    except (BrokenPipeError, ConnectionResetError):
                        bridge._unregister_client(q)
                        return
                if backlog:
                    try:
                        self.wfile.flush()
                    except (BrokenPipeError, ConnectionResetError):
                        bridge._unregister_client(q)
                        return
                try:
                    while not bridge._stopped.is_set():
                        try:
                            line = q.get(timeout=15.0)
                        except queue.Empty:
                            # SSE comment frame as heartbeat — keeps
                            # intermediate proxies from killing the conn.
                            try:
                                self.wfile.write(b":heartbeat\n\n")
                                self.wfile.flush()
                            except (BrokenPipeError, ConnectionResetError):
                                return
                            continue
                        if line is None:
                            # Sentinel from stop(): tell the viewer the
                            # session is over so it can drop its state
                            # back to the "pending" placeholder.  Failure
                            # to write is non-fatal — we're shutting down
                            # anyway.
                            try:
                                self.wfile.write(_sse_chunk("", event="shutdown"))
                                self.wfile.flush()
                            except (BrokenPipeError, ConnectionResetError):
                                pass
                            return
                        try:
                            self.wfile.write(_sse_chunk(line))
                            self.wfile.flush()
                        except (BrokenPipeError, ConnectionResetError):
                            return
                finally:
                    bridge._unregister_client(q)

            # ---- helpers
            def _begin_sse(self) -> None:
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream; charset=utf-8")
                self.send_header("Cache-Control", "no-cache, no-transform")
                self.send_header("Connection", "keep-alive")
                self.send_header("X-Accel-Buffering", "no")
                self.end_headers()

            def do_GET(self):  # noqa: N802 — required by base class
                parsed = urllib.parse.urlparse(self.path)
                qs = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
                route = parsed.path
                if route == "/replay":
                    self._serve_replay(qs)
                elif route == "/live":
                    self._serve_live()
                else:
                    self._serve_static(route)

        return Handler


# ----- CLI (replay-only) ---------------------------------------------------


def main() -> int:
    p = argparse.ArgumentParser(
        description="SSE bridge for the rhapsody live-viz (replay mode)"
    )
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8765)
    p.add_argument(
        "--open", dest="open_browser", action="store_true",
        help="open the viewer in the default browser at startup",
    )
    p.add_argument(
        "--replay",
        help="JSONL path to expose at /replay (the URL the browser will use)",
    )
    args = p.parse_args()

    bridge = LiveVizBridge(host=args.host, port=args.port)
    bridge.start()

    if args.open_browser:
        url = bridge.url
        if args.replay:
            url += "?replay=" + urllib.parse.quote(os.path.abspath(args.replay))
        try:
            webbrowser.open(url)
        except Exception:
            pass

    sys.stderr.write(
        "[bridge]   /replay?file=<path>   post-mortem replay\n"
        "[bridge]   /live                 in-process subscriber feed (see LiveVizBridge)\n"
    )

    try:
        # Block forever — actual serving runs on the daemon thread.
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        pass
    finally:
        bridge.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
