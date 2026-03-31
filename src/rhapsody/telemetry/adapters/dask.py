"""Telemetry adapter for DaskExecutionBackend.

Relies purely on Dask's native scheduler_info() API — no psutil, no extra deps.

Per-worker metrics exposed by Dask:
    workers[addr]["metrics"]["cpu"]    → CPU utilization %
    workers[addr]["metrics"]["memory"] → bytes used
    workers[addr]["memory_limit"]      → bytes total
    workers[addr]["host"]              → hostname (used as node_id)

GPU utilization is not exposed by Dask scheduler_info — gpu_percent is always None.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING
from typing import Any

from rhapsody.telemetry.adapters.base import TelemetryAdapter
from rhapsody.telemetry.events import ResourceUpdate
from rhapsody.telemetry.events import make_event

if TYPE_CHECKING:
    from rhapsody.telemetry.manager import TelemetryManager

logger = logging.getLogger(__name__)


class DaskTelemetryAdapter(TelemetryAdapter):
    """Collects per-worker resource metrics from Dask's scheduler.

    Args:
        client:       An active Dask ``distributed.Client`` instance.
        session_id:   Session identifier for emitted events.
        backend_name: Backend name for emitted events.
        interval:     Polling interval in seconds (default: 10.0).
    """

    def __init__(
        self,
        client: Any,
        session_id: str,
        backend_name: str,
        interval: float = 10.0,
    ) -> None:
        self._client = client
        self._session_id = session_id
        self._backend_name = backend_name
        self._interval = interval
        self._manager: TelemetryManager | None = None
        self._task: asyncio.Task | None = None
        self._running = False

    def start(self, manager: TelemetryManager) -> None:
        self._manager = manager
        self._running = True
        self._task = asyncio.create_task(self._collect_loop(), name="telemetry-dask-adapter")

    def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()

    async def _collect_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self._interval)
            try:
                workers = self._client.scheduler_info().get("workers", {})
                for addr, w in workers.items():
                    m = w.get("metrics", {})
                    mem_limit = w.get("memory_limit", 1) or 1
                    cpu = m.get("cpu", 0.0)
                    mem_pct = (m.get("memory", 0) / mem_limit) * 100.0
                    node_id = w.get("host", addr)
                    self._manager.emit(
                        make_event(
                            ResourceUpdate,
                            session_id=self._session_id,
                            backend=self._backend_name,
                            node_id=node_id,
                            cpu_percent=cpu,
                            memory_percent=mem_pct,
                            gpu_percent=None,  # Dask does not expose GPU via scheduler_info
                        )
                    )
            except Exception:
                logger.debug("DaskTelemetryAdapter collection error", exc_info=True)
