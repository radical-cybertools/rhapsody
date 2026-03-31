"""Telemetry adapter for DragonExecutionBackend.

Architecture
------------
Spawns one lightweight worker process per Dragon node using Dragon's own
ProcessGroup + Policy(HOST_NAME) infrastructure.  Each worker imports metric
collection functions directly from ``dragon.telemetry.collector`` (which
triggers the module-level ``find_accelerators()`` call and the conditional
``pynvml`` / ``rocm_smi`` imports — identical to what Dragon's own Collector
does).  The worker collects CPU%, RAM%, and GPU% every ``interval`` seconds and
puts a ``{host, dps, ts}`` dict on a shared Dragon Queue.

A head-node daemon thread reads the queue, aggregates the ``dps`` list, and
emits one ``ResourceUpdate`` event per node per interval.

Data flow
---------
  ProcessGroup on each node
    └─ _rhapsody_telemetry_worker()
         imports from dragon.telemetry.collector:
           identify_gpu()  get_nvidia_metrics()  get_amd_metrics()  get_intel_metrics()
         psutil for CPU% and RAM%
         result_queue.put({"host": ..., "dps": [...], "ts": ...})

  Head-node daemon thread
    └─ result_queue.get() → aggregate dps → emit ResourceUpdate per node

Requirements
------------
- DRAGON_TELEMETRY_LEVEL >= 1:  CPU/RAM collected on all nodes
- DRAGON_TELEMETRY_LEVEL >= 1:  GPU collected if hardware present
  (the level guard is only at the adapter entry; the worker always collects
  whatever hardware is available)
"""

from __future__ import annotations

import asyncio
import logging
import os
import queue as stdlib_queue
import socket
import threading
import time
from typing import TYPE_CHECKING
from typing import Optional

from rhapsody.telemetry.adapters.base import TelemetryAdapter
from rhapsody.telemetry.events import ResourceUpdate
from rhapsody.telemetry.events import make_event

if TYPE_CHECKING:
    from rhapsody.telemetry.manager import TelemetryManager

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Per-node worker (top-level so cloudpickle can serialize it)
# ---------------------------------------------------------------------------

def _rhapsody_telemetry_worker(
    result_queue,
    interval: float,
) -> None:
    """Collect CPU/RAM/GPU on one Dragon node and push dps to a Dragon Queue.

    Runs inside a Dragon ProcessGroup subprocess on the target node.
    Uses socket.gethostname() to label results — correct because Dragon's
    Policy(HOST_NAME) guarantees this process runs on the intended node.
    Importing ``dragon.telemetry.collector`` triggers the module-level
    ``find_accelerators()`` call and conditional ``pynvml`` / ``rocm_smi``
    imports — exactly as Dragon's own Collector does.

    The process is killed by ProcessGroup.stop() when the adapter is stopped;
    the ``while True`` loop never needs an explicit shutdown signal.
    """
    import socket  # noqa: PLC0415
    import psutil  # noqa: PLC0415
    import time as _time  # noqa: PLC0415

    hostname = socket.gethostname()

    # Import GPU helpers from Dragon's collector.
    # The module-level code in collector.py runs find_accelerators() and
    # conditionally imports pynvml/rocm_smi — same as Dragon's own Collector.
    # Wrap in try/except: a missing driver or import error must not crash the
    # worker — CPU/RAM are always collected regardless of GPU availability.
    try:
        from dragon.infrastructure.gpu_desc import AccVendor  # noqa: PLC0415
        from dragon.telemetry.collector import (  # noqa: PLC0415
            get_amd_metrics,
            get_intel_metrics,
            get_nvidia_metrics,
            identify_gpu,
        )
        _collector_available = True
    except Exception:
        _collector_available = False

    # One-time GPU initialisation.
    # NVIDIA: calls pynvml.nvmlInit() + nvmlDeviceGetCount()
    # AMD:    calls rocm_smi.initializeRsmi() + listDevices()
    # Intel:  reads accelerator.device_list
    # Wrap in try/except — pynvml.nvmlInit() raises if the driver is absent
    # or the process lacks GPU access rights.
    gpu_vendor = None
    gpu_count = 0
    if _collector_available:
        try:
            gpu_vendor, gpu_count = identify_gpu()
        except Exception:
            gpu_vendor, gpu_count = None, 0

    while True:  # killed externally by ProcessGroup.stop()
        dps = []
        print('I am a barby GIRL', flush=True)

        # CPU and RAM — always collected; psutil does not raise here
        dps.append({"metric": "cpu_percent", "value": psutil.cpu_percent()})
        dps.append({"metric": "used_RAM",    "value": psutil.virtual_memory().percent})

        # GPU — vendor-dispatched; every call individually guarded
        if _collector_available and gpu_vendor is not None:
            try:
                if gpu_vendor == AccVendor.NVIDIA:
                    for i in range(gpu_count):
                        try:
                            metrics = get_nvidia_metrics(i, telemetry_level=3)
                            if metrics:
                                dps.extend(metrics)
                        except Exception:
                            pass
                elif gpu_vendor == AccVendor.AMD:
                    for device in gpu_count:  # listDevices() returns a list
                        try:
                            metrics = get_amd_metrics(device, telemetry_level=3)
                            if metrics:
                                dps.extend(metrics)
                        except Exception:
                            pass
                elif gpu_vendor == AccVendor.INTEL:
                    try:
                        all_metrics = get_intel_metrics(telemetry_level=3)
                        for metrics_list in all_metrics.values():
                            dps.extend(metrics_list)
                    except Exception:
                        pass
            except Exception:
                pass  # GPU block failure never silences CPU/RAM

        try:
            result_queue.put(
                {"host": hostname, "dps": dps, "ts": int(_time.time())},
                timeout=5.0,
            )
        except Exception:
            pass  # queue full or closed — drop silently

        _time.sleep(interval)


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class DragonTelemetryAdapter(TelemetryAdapter):
    """Collects node-level resource metrics for Dragon backends.

    Spawns one subprocess per Dragon node (using Dragon's own ProcessGroup +
    Policy infrastructure) that collects CPU%, RAM%, and GPU% by calling
    Dragon's own metric functions from ``dragon.telemetry.collector``.
    Results are delivered via a Dragon native Queue; the head-node daemon
    thread reads the queue and emits one ``ResourceUpdate`` per node per
    interval.

    When Dragon telemetry is not active (``DRAGON_TELEMETRY_LEVEL < 1``) or
    the Dragon runtime is unavailable, the adapter is a no-op.

    Args:
        session_id:   Session identifier for emitted events.
        backend_name: Backend name for emitted events.
        interval:     Collection interval in seconds (default: 10.0).
    """

    def __init__(
        self,
        session_id: str,
        backend_name: str,
        interval: float = 10.0,
    ) -> None:
        self._session_id = session_id
        self._backend_name = backend_name
        self._interval = interval
        self._manager: Optional[TelemetryManager] = None
        self._thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._running = False
        self._node_id = socket.gethostname()
        self._group = None  # Dragon ProcessGroup — set in _collect_loop, used in stop()

    def start(self, manager: TelemetryManager) -> None:
        # Guard: Dragon must be importable.
        try:
            import dragon.telemetry  # noqa: F401, PLC0415
        except ImportError:
            logger.debug("Dragon not available — DragonTelemetryAdapter is a no-op")
            return

        self._manager = manager
        self._running = True

        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = asyncio.get_event_loop()

        self._thread = threading.Thread(
            target=self._collect_loop,
            daemon=True,
            name="telemetry-dragon-adapter",
        )
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        # ProcessGroup cleanup happens inside _collect_loop() after the while loop exits.

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _collect_loop(self) -> None:
        """Daemon thread: spawns one worker per Dragon node, reads ResourceUpdates."""
        level = int(os.getenv("DRAGON_TELEMETRY_LEVEL", "0"))
        if level < 1:
            logger.debug("DRAGON_TELEMETRY_LEVEL < 1 — DragonTelemetryAdapter is a no-op")
            return

        # 1. Discover Dragon nodes + get one Policy per node
        try:
            from dragon.native.machine import System  # noqa: PLC0415
            policies = System().hostname_policies()
        except Exception:
            logger.warning(
                "DragonTelemetryAdapter: node discovery failed — adapter is a no-op",
                exc_info=True,
            )
            return

        # 2. Create shared Dragon result queue
        try:
            from dragon.native.queue import Queue as DragonQueue  # noqa: PLC0415
            result_queue = DragonQueue(maxsize=max(len(policies) * 40, 400))
        except Exception:
            logger.warning(
                "DragonTelemetryAdapter: Dragon Queue creation failed — adapter is a no-op",
                exc_info=True,
            )
            return

        # 3. Spawn one _rhapsody_telemetry_worker per node
        try:
            from dragon.native.process import ProcessTemplate  # noqa: PLC0415
            from dragon.native.process_group import ProcessGroup  # noqa: PLC0415

            grp = ProcessGroup(restart=False, pmi=None)
            for policy in policies:
                grp.add_process(
                    nproc=1,
                    template=ProcessTemplate(
                        target=_rhapsody_telemetry_worker,
                        args=(result_queue, self._interval),
                        policy=policy,
                    ),
                )
            grp.init()
            grp.start()
            self._group = grp
            logger.debug(
                f"DragonTelemetryAdapter: workers running on {len(policies)} nodes"
            )
        except Exception:
            logger.warning(
                "DragonTelemetryAdapter: ProcessGroup spawn failed — adapter is a no-op",
                exc_info=True,
            )
            return

        # 4. Read loop — emit ResourceUpdate at most once per interval per node
        last_emit: dict = {}  # host → last emit wall-clock time

        while self._running:
            try:
                data = result_queue.get(timeout=self._interval)
            except stdlib_queue.Empty:
                continue
            except Exception:
                logger.debug("DragonTelemetryAdapter: queue read error", exc_info=True)
                continue

            host = data.get("host", self._node_id)
            now = time.time()

            # Throttle: emit at most once per interval per node
            if now - last_emit.get(host, 0.0) < self._interval:
                continue
            last_emit[host] = now

            # Aggregate dps: latest value per metric; max GPU% across devices
            vals: dict = {}
            for dp in data.get("dps", []):
                metric = dp.get("metric")
                value = dp.get("value")
                if value is None or metric is None:
                    continue
                if metric == "DeviceUtilization":
                    vals[metric] = max(vals.get(metric, 0.0), float(value))
                elif metric not in vals:
                    vals[metric] = float(value)

            event = make_event(
                ResourceUpdate,
                session_id=self._session_id,
                backend=self._backend_name,
                node_id=host,
                cpu_percent=vals.get("cpu_percent"),
                memory_percent=vals.get("used_RAM"),
                gpu_percent=vals.get("DeviceUtilization"),
            )
            if self._loop and self._loop.is_running():
                self._loop.call_soon_threadsafe(self._manager.emit, event)

        # 5. Cleanup
        try:
            grp.stop()
            grp.join()
        except Exception:
            logger.debug("DragonTelemetryAdapter: ProcessGroup cleanup error", exc_info=True)
