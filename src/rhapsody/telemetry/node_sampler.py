"""Per-node resource sampling shared by the concurrent and flux telemetry adapters.

:class:`NodeSampler` reads CPU / memory / disk I/O / network I/O via psutil and
per-device GPU utilization via pynvml (skipped silently when pynvml is absent).
:func:`resource_events` turns one sample into ``ResourceUpdate`` events.

Run as a module, this file is the remote sampler the flux adapter starts on
every broker rank via ``flux exec``: it prints one JSON line per sample to
stdout until it receives SIGTERM::

    python -m rhapsody.telemetry.node_sampler --interval 5
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import socket
import subprocess
import sys
import threading
import time

from rhapsody.telemetry.events import BaseEvent
from rhapsody.telemetry.events import ResourceUpdate
from rhapsody.telemetry.events import make_event

logger = logging.getLogger(__name__)


class NodeSampler:
    """Stateful sampler for the local node.

    Raises ``ImportError`` at construction when psutil is not available.

    Args:
        node_id: Node identifier used in log messages.
    """

    def __init__(self, node_id: str) -> None:
        import psutil

        self._psutil = psutil
        self._node_id = node_id

        # Warm up cpu_percent (first call always returns 0.0 on Linux)
        psutil.cpu_percent(interval=None)

        # GPU setup via pynvml — initialized once, not on every poll
        self._nvml = None
        self._gpu_count = 0
        self._gpu_failed: set[int] = set()
        try:
            import pynvml

            pynvml.nvmlInit()
            self._gpu_count = pynvml.nvmlDeviceGetCount()
            self._nvml = pynvml
        except ImportError:
            pass  # pynvml not installed — GPU metrics skipped silently
        except Exception as _exc:
            logger.warning(
                "pynvml initialization failed on %s — GPU metrics disabled: %s",
                node_id,
                _exc,
            )

        # Seed disk / net baselines for delta calculations
        self._prev_disk: tuple | None = None
        self._prev_net: tuple | None = None
        try:
            dc = psutil.disk_io_counters()
            self._prev_disk = (dc.read_bytes, dc.write_bytes) if dc else None
        except Exception:  # noqa: S110
            pass
        try:
            nc = psutil.net_io_counters()
            self._prev_net = (nc.bytes_sent, nc.bytes_recv) if nc else None
        except Exception:  # noqa: S110
            pass

    def sample(self) -> tuple[dict, dict[int, float]]:
        """Collect one sample.

        Returns:
            ``(node, gpus)``: ``node`` holds the per-node ``ResourceUpdate``
            fields (disk/net are byte deltas since the previous call, None on
            the first call); ``gpus`` maps device index to utilization %.
        """
        psutil = self._psutil
        cpu = psutil.cpu_percent(interval=None)
        mem = psutil.virtual_memory().percent

        # Per-device GPU utilization via pynvml
        gpus: dict[int, float] = {}
        if self._nvml is not None:
            for dev_idx in range(self._gpu_count):
                if dev_idx in self._gpu_failed:
                    continue
                try:
                    handle = self._nvml.nvmlDeviceGetHandleByIndex(dev_idx)
                    util = self._nvml.nvmlDeviceGetUtilizationRates(handle)
                    gpus[dev_idx] = float(util.gpu)
                except Exception as _exc:
                    self._gpu_failed.add(dev_idx)
                    logger.warning(
                        "GPU %d unavailable on %s (%s) — disabling",
                        dev_idx,
                        self._node_id,
                        _exc,
                    )

        disk_read = disk_write = net_sent = net_recv = None

        try:
            dc = psutil.disk_io_counters()
            if dc and self._prev_disk is not None:
                disk_read = float(dc.read_bytes - self._prev_disk[0])
                disk_write = float(dc.write_bytes - self._prev_disk[1])
            self._prev_disk = (dc.read_bytes, dc.write_bytes) if dc else None
        except Exception:  # noqa: S110
            pass

        try:
            nc = psutil.net_io_counters()
            if nc and self._prev_net is not None:
                net_sent = float(nc.bytes_sent - self._prev_net[0])
                net_recv = float(nc.bytes_recv - self._prev_net[1])
            self._prev_net = (nc.bytes_sent, nc.bytes_recv) if nc else None
        except Exception:  # noqa: S110
            pass

        node = {
            "cpu_percent": cpu,
            "memory_percent": mem,
            "gpu_percent": max(gpus.values()) if gpus else None,
            "disk_read_bytes": disk_read,
            "disk_write_bytes": disk_write,
            "net_sent_bytes": net_sent,
            "net_recv_bytes": net_recv,
        }
        return node, gpus


def resource_events(
    session_id: str,
    backend_name: str,
    node_id: str,
    node: dict,
    gpus: dict[int, float],
    **kwargs,
) -> list[BaseEvent]:
    """Build the per-node event plus one per-GPU event for one sample.

    Extra ``kwargs`` (e.g. ``event_time``, ``attributes``) go to every event.
    """
    # Node-level aggregate event (gpu_id=None — backward-compatible)
    events = [
        make_event(
            ResourceUpdate,
            session_id=session_id,
            backend=backend_name,
            node_id=node_id,
            **node,
            **kwargs,
        )
    ]
    # Per-device GPU events (cpu/mem/disk/net are per_node scope only)
    for dev_idx, gpu_pct in gpus.items():
        events.append(
            make_event(
                ResourceUpdate,
                session_id=session_id,
                backend=backend_name,
                node_id=node_id,
                gpu_percent=gpu_pct,
                gpu_id=dev_idx,
                **kwargs,
            )
        )
    return events


def _flux_rank() -> int | None:
    """Return the rank of the local flux broker, None outside flux."""
    try:
        out = subprocess.run(
            ["flux", "getattr", "rank"],  # noqa: S607
            capture_output=True,
            text=True,
            timeout=10,
        )
        return int(out.stdout.strip()) if out.returncode == 0 else None
    except Exception:
        return None


def main(argv: list[str] | None = None) -> int:
    """Print one JSON sample line per interval until SIGTERM / SIGINT."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interval", type=float, default=5.0)
    args = parser.parse_args(argv)

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())

    node_id = socket.gethostname()
    rank = _flux_rank()
    try:
        sampler = NodeSampler(node_id)
    except ImportError:
        print("psutil not available — node sampler exits", file=sys.stderr)
        return 1

    # The first sample comes one interval after the cpu_percent warm-up in
    # NodeSampler(); an immediate sample would cover a near-zero window.
    while not stop.wait(args.interval):
        try:
            node, gpus = sampler.sample()
            line = {
                "node_id": node_id,
                "rank": rank,
                "time": time.time(),
                "node": node,
                "gpus": {str(k): v for k, v in gpus.items()},
            }
            print(json.dumps(line), flush=True)
        except BrokenPipeError:
            return 0
        except Exception as exc:
            print(f"sample failed: {exc}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
