"""Telemetry adapter for ConcurrentExecutionBackend.

Uses psutil for CPU, memory, disk I/O, and network I/O metrics on the local node.
GPU utilization is collected via nvidia-smi if available; falls back to None gracefully.

The concurrent backend runs on a single local node with no native telemetry
infrastructure, so psutil is the only correct option here.

The collection loop runs in a **daemon thread** (not an asyncio task) so it is
immune to any event-loop interference from the Dragon runtime when RHAPSODY is
launched via `dragon -T N`.  Events are posted back to the asyncio loop via
`loop.call_soon_threadsafe`, keeping the emit path thread-safe.

The metrics are node-level, so all adapters in one process that poll at the
same interval share one collection thread (:class:`_NodeSampler`): a process
that hosts many sessions (an ORBIT endpoint) polls the node once per interval,
not once per session.
"""

from __future__ import annotations

import asyncio
import logging
import socket
import threading
import time
from typing import TYPE_CHECKING
from typing import Any

from rhapsody.telemetry.adapters.base import TelemetryAdapter
from rhapsody.telemetry.events import ResourceUpdate
from rhapsody.telemetry.events import make_event

if TYPE_CHECKING:
    from rhapsody.telemetry.manager import TelemetryManager

logger = logging.getLogger(__name__)


class _NodeSampler:
    """One node-level collection thread per process and polling interval.

    Polls psutil / NVML every *interval* seconds and hands each sample to every
    subscribed adapter.  The thread starts with the first subscriber and ends
    after the last one unsubscribes.
    """

    _lock = threading.Lock()
    _samplers: dict[float, _NodeSampler] = {}

    def __init__(self, interval: float) -> None:
        self._interval = interval
        self._subscribers: list[ConcurrentTelemetryAdapter] = []
        self._last: dict[str, Any] | None = None
        self._node_id = socket.gethostname()
        self._thread = threading.Thread(
            target=self._collect_loop,
            name="telemetry-concurrent-adapter",
            daemon=True,
        )

    @classmethod
    def subscribe(cls, adapter: ConcurrentTelemetryAdapter) -> _NodeSampler:
        """Add *adapter* to the sampler for its interval, starting one if needed."""
        with cls._lock:
            sampler = cls._samplers.get(adapter._interval)
            if sampler is None:
                sampler = cls(adapter._interval)
                cls._samplers[adapter._interval] = sampler
                sampler._subscribers.append(adapter)
                sampler._thread.start()
            else:
                sampler._subscribers.append(adapter)
                # coverage starts at t=0 for a late subscriber too
                if sampler._last is not None:
                    adapter._emit_sample(sampler._last)
            return sampler

    @classmethod
    def unsubscribe(cls, adapter: ConcurrentTelemetryAdapter) -> None:
        """Remove *adapter*; the last one to leave stops the thread."""
        with cls._lock:
            sampler = cls._samplers.get(adapter._interval)
            if sampler is None or adapter not in sampler._subscribers:
                return
            sampler._subscribers.remove(adapter)
            if not sampler._subscribers:
                del cls._samplers[adapter._interval]

    def _running(self) -> bool:
        with self._lock:
            return self._samplers.get(self._interval) is self

    def _publish(self, sample: dict[str, Any]) -> None:
        with self._lock:
            self._last = sample
            subscribers = list(self._subscribers)
        for adapter in subscribers:
            adapter._emit_sample(sample)

    def _collect_loop(self) -> None:
        """Blocking loop that runs in the daemon thread."""
        try:
            import psutil
        except ImportError:
            logger.warning("psutil not available — ConcurrentTelemetryAdapter is a no-op")
            return

        # Warm up cpu_percent (first call always returns 0.0 on Linux)
        psutil.cpu_percent(interval=None)

        # GPU setup via pynvml — initialized once, not on every poll
        _nvml_ok = False
        _gpu_count = 0
        _gpu_failed: set[int] = set()
        try:
            import pynvml

            pynvml.nvmlInit()
            _gpu_count = pynvml.nvmlDeviceGetCount()
            _nvml_ok = True
        except ImportError:
            pass  # pynvml not installed — GPU metrics skipped silently
        except Exception as _exc:
            logger.warning(
                "pynvml initialization failed on %s — GPU metrics disabled: %s",
                self._node_id,
                _exc,
            )

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

        def _poll() -> None:
            """Collect one sample and hand it to the subscribers."""
            nonlocal prev_disk, prev_net
            try:
                cpu = psutil.cpu_percent(interval=None)
                mem = psutil.virtual_memory().percent

                # Per-device GPU utilization via pynvml
                per_device_gpu: dict[int, float] = {}
                if _nvml_ok:
                    for dev_idx in range(_gpu_count):
                        if dev_idx in _gpu_failed:
                            continue
                        try:
                            handle = pynvml.nvmlDeviceGetHandleByIndex(dev_idx)
                            util = pynvml.nvmlDeviceGetUtilizationRates(handle)
                            per_device_gpu[dev_idx] = float(util.gpu)
                        except Exception as _exc:
                            _gpu_failed.add(dev_idx)
                            logger.warning(
                                "GPU %d unavailable on %s (%s) — disabling",
                                dev_idx,
                                self._node_id,
                                _exc,
                            )

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

                self._publish(
                    {
                        "node_id": self._node_id,
                        "cpu_percent": cpu,
                        "memory_percent": mem,
                        "per_device_gpu": per_device_gpu,
                        "disk_read_bytes": disk_read,
                        "disk_write_bytes": disk_write,
                        "net_sent_bytes": net_sent,
                        "net_recv_bytes": net_recv,
                    }
                )

            except Exception:
                logger.debug("ConcurrentTelemetryAdapter collection error", exc_info=True)

        # Emit one sample immediately so resource coverage starts at t=0,
        # not one full interval later. Disk/net deltas are None on this first
        # call (baselines were just seeded above) which is correct.
        _poll()

        while self._running():
            time.sleep(self._interval)  # plain OS sleep — no event loop involved
            if not self._running():
                break
            _poll()


class ConcurrentTelemetryAdapter(TelemetryAdapter):
    """Collects node-level resource metrics for the concurrent backend via psutil.

    Subscribes to the process's collection thread for its *interval*, which
    wakes up every *interval* seconds, reads CPU / memory / disk / network
    counters via psutil; each sample is posted as a ResourceUpdate event back
    to this adapter's asyncio event loop via ``loop.call_soon_threadsafe``.
    Using a thread (rather than an asyncio task) makes the adapter robust under
    Dragon's runtime, which can interfere with ``asyncio.sleep`` inside tasks.

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

    def start(self) -> None:
        if self._running:
            return  # already polling: a second start must not subscribe twice
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = asyncio.get_event_loop()
        self._running = True
        self._thread = _NodeSampler.subscribe(self)._thread

    def stop(self) -> None:
        self._running = False
        _NodeSampler.unsubscribe(self)
        # Thread is a daemon — it ends with its last subscriber; no join needed.

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _emit_sample(self, sample: dict[str, Any]) -> None:
        """Post one node sample as ResourceUpdate events (called from the sampler)."""
        if not (self._running and self._loop and self._loop.is_running()):
            return
        per_device_gpu = sample["per_device_gpu"]
        gpu_aggregate = max(per_device_gpu.values()) if per_device_gpu else None

        # Node-level aggregate event (gpu_id=None — backward-compatible)
        node_event = make_event(
            ResourceUpdate,
            session_id=self._session_id,
            backend=self._backend_name,
            node_id=sample["node_id"],
            cpu_percent=sample["cpu_percent"],
            memory_percent=sample["memory_percent"],
            gpu_percent=gpu_aggregate,
            disk_read_bytes=sample["disk_read_bytes"],
            disk_write_bytes=sample["disk_write_bytes"],
            net_sent_bytes=sample["net_sent_bytes"],
            net_recv_bytes=sample["net_recv_bytes"],
        )
        self._loop.call_soon_threadsafe(self._manager.emit, node_event)

        # Per-device GPU events (cpu/mem/disk/net are per_node scope only)
        for dev_idx, gpu_pct in per_device_gpu.items():
            gpu_event = make_event(
                ResourceUpdate,
                session_id=self._session_id,
                backend=self._backend_name,
                node_id=sample["node_id"],
                cpu_percent=None,
                memory_percent=None,
                gpu_percent=gpu_pct,
                gpu_id=dev_idx,
                disk_read_bytes=None,
                disk_write_bytes=None,
                net_sent_bytes=None,
                net_recv_bytes=None,
            )
            self._loop.call_soon_threadsafe(self._manager.emit, gpu_event)
