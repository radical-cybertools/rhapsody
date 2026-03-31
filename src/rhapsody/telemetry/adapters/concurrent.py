"""Telemetry adapter for ConcurrentExecutionBackend.

Uses psutil for CPU, memory, disk I/O, and network I/O metrics on the local node.
GPU utilization is collected via nvidia-smi if available; falls back to None gracefully.

The concurrent backend runs on a single local node with no native telemetry
infrastructure, so psutil is the only correct option here.

The collection loop runs in a **daemon thread** (not an asyncio task) so it is
immune to any event-loop interference from the Dragon runtime when RHAPSODY is
launched via `dragon -T N`.  Events are posted back to the asyncio loop via
`loop.call_soon_threadsafe`, keeping the emit path thread-safe.
"""

from __future__ import annotations

import asyncio
import logging
import socket
import subprocess
import threading
import time
from typing import TYPE_CHECKING

from rhapsody.telemetry.adapters.base import TelemetryAdapter
from rhapsody.telemetry.events import ResourceUpdate
from rhapsody.telemetry.events import make_event

if TYPE_CHECKING:
    from rhapsody.telemetry.manager import TelemetryManager

logger = logging.getLogger(__name__)


class ConcurrentTelemetryAdapter(TelemetryAdapter):
    """Collects node-level resource metrics for the concurrent backend via psutil.

    Runs a daemon thread that wakes up every *interval* seconds, reads CPU /
    memory / disk / network counters via psutil, then posts a ResourceUpdate event
    back to the asyncio event loop via ``loop.call_soon_threadsafe``.  Using a
    thread (rather than an asyncio task) makes the adapter robust under Dragon's
    runtime, which can interfere with ``asyncio.sleep`` inside tasks.

    Args:
        session_id:   Session identifier to attach to emitted events.
        backend_name: Backend name to attach to emitted events.
        interval:     Polling interval in seconds (default: 5.0).
    """

    def __init__(
        self,
        session_id: str,
        backend_name: str,
        interval: float = 5.0,
    ) -> None:
        self._session_id = session_id
        self._backend_name = backend_name
        self._interval = interval
        self._manager: TelemetryManager | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._running = False
        self._node_id = socket.gethostname()

    def start(self, manager: TelemetryManager) -> None:
        self._manager = manager
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = asyncio.get_event_loop()
        self._running = True
        self._thread = threading.Thread(
            target=self._collect_loop,
            name="telemetry-concurrent-adapter",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        # Thread is a daemon — it will exit when the process exits; no join needed.

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _collect_loop(self) -> None:
        """Blocking loop that runs in the daemon thread."""
        try:
            import psutil
        except ImportError:
            logger.warning("psutil not available — ConcurrentTelemetryAdapter is a no-op")
            return

        # Warm up cpu_percent (first call always returns 0.0 on Linux)
        psutil.cpu_percent(interval=None)

        # Seed disk / net baselines for delta calculations
        prev_disk: tuple | None = None
        prev_net: tuple | None = None
        try:
            dc = psutil.disk_io_counters()
            prev_disk = (dc.read_bytes, dc.write_bytes) if dc else None
        except Exception:  # noqa: S110
            pass
        try:
            nc = psutil.net_io_counters()
            prev_net = (nc.bytes_sent, nc.bytes_recv) if nc else None
        except Exception:  # noqa: S110
            pass

        while self._running:
            time.sleep(self._interval)  # plain OS sleep — no event loop involved
            if not self._running:
                break
            try:
                cpu = psutil.cpu_percent(interval=None)
                mem = psutil.virtual_memory().percent
                gpu = self._try_gpu_percent()

                disk_read = disk_write = net_sent = net_recv = None

                try:
                    dc = psutil.disk_io_counters()
                    if dc and prev_disk is not None:
                        disk_read = float(dc.read_bytes - prev_disk[0])
                        disk_write = float(dc.write_bytes - prev_disk[1])
                    prev_disk = (dc.read_bytes, dc.write_bytes) if dc else None
                except Exception:  # noqa: S110
                    pass

                try:
                    nc = psutil.net_io_counters()
                    if nc and prev_net is not None:
                        net_sent = float(nc.bytes_sent - prev_net[0])
                        net_recv = float(nc.bytes_recv - prev_net[1])
                    prev_net = (nc.bytes_sent, nc.bytes_recv) if nc else None
                except Exception:  # noqa: S110
                    pass

                event = make_event(
                    ResourceUpdate,
                    session_id=self._session_id,
                    backend=self._backend_name,
                    node_id=self._node_id,
                    cpu_percent=cpu,
                    memory_percent=mem,
                    gpu_percent=gpu,
                    disk_read_bytes=disk_read,
                    disk_write_bytes=disk_write,
                    net_sent_bytes=net_sent,
                    net_recv_bytes=net_recv,
                )
                # Post back to the asyncio loop — thread-safe
                if self._loop and self._loop.is_running():
                    self._loop.call_soon_threadsafe(self._manager.emit, event)
            except Exception:
                logger.debug("ConcurrentTelemetryAdapter collection error", exc_info=True)

    def _try_gpu_percent(self) -> float | None:
        """Try to get GPU utilization via nvidia-smi.

        Returns None if unavailable.
        """
        try:
            result = subprocess.run(
                ["nvidia-smi", "--query-gpu=utilization.gpu", "--format=csv,noheader,nounits"],  # noqa: S607
                capture_output=True,
                text=True,
                timeout=1,
            )
            if result.returncode == 0:
                lines = [ln.strip() for ln in result.stdout.strip().splitlines() if ln.strip()]
                if lines:
                    return float(lines[0])
        except Exception:  # noqa: S110
            pass
        return None
