"""Telemetry adapter for FluxExecutionBackend.

Starts :mod:`rhapsody.telemetry.node_sampler` on every broker rank of the flux
instance via ``flux exec -r all``.  ``flux exec`` runs the samplers directly
under the brokers, outside the scheduler, so no cores are reserved for
telemetry.  The samplers print one JSON line per sample; ``flux exec`` streams
them back here, where a daemon thread turns each line into ``ResourceUpdate``
events and posts them to the asyncio loop via ``loop.call_soon_threadsafe``.

The adapter only needs the instance URI, so it works for a local instance
(``local://``) and for a partition instance on other nodes (``ssh://``) alike.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import subprocess
import sys
import threading
from typing import TYPE_CHECKING

from rhapsody.telemetry.adapters.base import TelemetryAdapter
from rhapsody.telemetry.node_sampler import resource_events

if TYPE_CHECKING:
    from rhapsody.telemetry.manager import TelemetryManager

logger = logging.getLogger(__name__)


class FluxTelemetryAdapter(TelemetryAdapter):
    """Collects per-node resource metrics from all ranks of a flux instance.

    Args:
        session_id:   Session identifier to attach to emitted events.
        backend_name: Backend name to attach to emitted events.
        uri:          FLUX_URI of the instance to sample.
        interval:     Polling interval in seconds (default: 5.0).
        stop_timeout: Seconds to wait for ``flux exec`` after SIGTERM before
                      killing it (default: 5.0).
    """

    def __init__(
        self,
        session_id: str,
        backend_name: str,
        uri: str,
        interval: float = 5.0,
        stop_timeout: float = 5.0,
    ) -> None:
        self._session_id = session_id
        self._backend_name = backend_name
        self._uri = uri
        self._interval = interval
        self._stop_timeout = stop_timeout
        self._manager: TelemetryManager | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._proc: subprocess.Popen | None = None
        self._threads: list[threading.Thread] = []
        self._running = False

    def _command(self) -> list[str]:
        return [
            "flux",
            "exec",
            "-r",
            "all",
            sys.executable,
            "-m",
            "rhapsody.telemetry.node_sampler",
            "--interval",
            str(self._interval),
        ]

    def start(self) -> None:
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = asyncio.get_event_loop()
        try:
            self._proc = subprocess.Popen(  # noqa: S603
                self._command(),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                stdin=subprocess.DEVNULL,
                text=True,
                env={**os.environ, "FLUX_URI": self._uri},
            )
        except OSError as exc:
            logger.warning("Could not start flux node samplers on %s: %s", self._uri, exc)
            return
        self._running = True
        self._threads = [
            threading.Thread(
                target=self._read_stdout,
                args=(self._proc,),
                name=f"telemetry-flux-adapter-{self._backend_name}",
                daemon=True,
            ),
            threading.Thread(
                target=self._read_stderr,
                args=(self._proc,),
                name=f"telemetry-flux-adapter-{self._backend_name}-err",
                daemon=True,
            ),
        ]
        for thread in self._threads:
            thread.start()

    def stop(self) -> None:
        self._running = False
        proc = self._proc
        if proc is None or proc.poll() is not None:
            return
        # flux exec forwards SIGTERM to the samplers on all ranks
        proc.terminate()
        try:
            proc.wait(timeout=self._stop_timeout)
        except subprocess.TimeoutExpired:
            logger.warning("flux node samplers on %s did not stop — killing", self._uri)
            proc.kill()
            proc.wait()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _handle_line(self, line: str) -> None:
        """Turn one sampler JSON line into events and post them to the loop."""
        try:
            rec = json.loads(line)
            attributes = {"flux_rank": rec["rank"]} if rec.get("rank") is not None else {}
            events = resource_events(
                self._session_id,
                self._backend_name,
                rec["node_id"],
                rec["node"],
                {int(k): float(v) for k, v in rec.get("gpus", {}).items()},
                event_time=float(rec["time"]),
                attributes=attributes,
            )
        except Exception:
            logger.debug("Ignoring malformed flux sampler line: %r", line, exc_info=True)
            return
        if self._loop and self._loop.is_running():
            for event in events:
                self._loop.call_soon_threadsafe(self._manager.emit, event)

    def _read_stdout(self, proc: subprocess.Popen) -> None:
        for line in proc.stdout or ():
            if line.strip():
                self._handle_line(line)
        rc = proc.wait()
        if self._running:
            logger.warning(
                "flux node samplers on %s exited early (rc=%s) — no further resource telemetry",
                self._uri,
                rc,
            )

    def _read_stderr(self, proc: subprocess.Popen) -> None:
        for line in proc.stderr or ():
            logger.debug("flux node sampler: %s", line.rstrip())
