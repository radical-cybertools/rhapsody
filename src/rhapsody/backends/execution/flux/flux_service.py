"""Flux instance lifecycle manager.

This module provides :class:`FluxService`, which starts and manages a
``flux start`` subprocess, waits for the ``FLUX_URI`` to become available,
and exposes both a local URI (for same-host connections) and an SSH-reachable
remote URI.
"""

from __future__ import annotations

import logging
import subprocess
import threading
import uuid
from typing import Any
from urllib.parse import urlparse
from urllib.parse import urlunparse

from .flux_module import FluxModule

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------------------
#
class FluxService:
    """Start and manage a local Flux instance.

    :class:`FluxService` launches ``flux start`` as a background subprocess,
    watches its output for the ``FLUX_URI`` announcement, and signals readiness
    via a :class:`threading.Event`.

    Args:
        uid: Optional identifier for log messages.  Defaults to a random UUID.
        launcher: Optional prefix command (e.g. an MPI launcher) prepended
            before ``flux start``.

    Example::

        fs = FluxService()
        fs.start(timeout=30)
        print(fs.uri)    # local socket URI
        print(fs.r_uri)  # ssh://hostname/... URI
        fs.stop()
    """

    # --------------------------------------------------------------------------
    #
    def __init__(
        self,
        uid: str | None = None,
        launcher: str | None = None,
    ) -> None:
        self._uid: str = uid or str(uuid.uuid4())
        self._launcher: str = launcher or ""

        self._fm = FluxModule()
        self._uri: str | None = None
        self._r_uri: str | None = None
        self._host: str | None = None
        self._proc: subprocess.Popen | None = None
        self._ready: threading.Event = threading.Event()

        self._fm.verify()

    # --------------------------------------------------------------------------
    # Properties
    # --------------------------------------------------------------------------

    @property
    def uid(self) -> str:
        """Unique identifier for this Flux service instance."""
        return self._uid

    @property
    def uri(self) -> str | None:
        """Local ``FLUX_URI`` (unix domain socket), or ``None`` before start."""
        return self._uri

    @property
    def r_uri(self) -> str | None:
        """SSH-reachable URI derived from the local URI, or ``None`` before start."""
        return self._r_uri

    # --------------------------------------------------------------------------
    #
    def _reader(self, stream: Any) -> None:
        """Read *stream* line-by-line and forward each line to :meth:`_on_line`."""
        try:
            for raw in stream:
                line = raw.decode(errors="replace").rstrip("\n")
                logger.debug("%s: flux io: %s", self._uid, line)
                self._on_line(line)
        except Exception:  # noqa: BLE001, S110
            pass

    # --------------------------------------------------------------------------
    #
    def _on_line(self, line: str) -> None:
        """Process a single output line from the ``flux start`` subprocess."""
        if not line.startswith("FLUX_URI="):
            return

        # Expected format: "FLUX_URI=<uri> FLUX_HOST=<hostname>"
        parts = line.strip().split(" ", 1)
        try:
            self._uri = parts[0].split("=", 1)[1]
            self._host = parts[1].split("=", 1)[1] if len(parts) > 1 else None
        except IndexError:
            logger.warning("%s: could not parse flux banner: %s", self._uid, line)
            return

        if self._host:
            parsed = urlparse(self._uri)
            self._r_uri = urlunparse(parsed._replace(scheme="ssh", netloc=self._host))
        else:
            self._r_uri = self._uri

        logger.info("%s: flux uri:   %s", self._uid, self._uri)
        logger.info("%s: flux r_uri: %s", self._uid, self._r_uri)
        self._ready.set()

    # --------------------------------------------------------------------------
    #
    def start(self, timeout: float | None = None) -> bool:
        """Start the ``flux start`` subprocess and wait for it to become ready.

        Args:
            timeout: Seconds to wait for the Flux instance to announce its URI.
                Pass a negative number to wait indefinitely.  Pass ``None`` to
                return immediately (non-blocking).

        Returns:
            ``True`` if the instance is ready within *timeout*, ``False``
            otherwise.
        """
        fcmd = (
            "echo FLUX_URI=\\$FLUX_URI FLUX_HOST=\\$(hostname) && flux resource list && sleep inf"
        )
        cmd_str = f'{self._launcher} {self._fm.exe} start bash -c "{fcmd}"'.strip()

        logger.info("%s: starting flux instance: %s", self._uid, cmd_str)

        self._proc = subprocess.Popen(  # noqa: S602
            cmd_str,
            shell=True,  # noqa: S602
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        # Read both streams in daemon threads so we never block
        for stream in (self._proc.stdout, self._proc.stderr):
            t = threading.Thread(target=self._reader, args=(stream,), daemon=True)
            t.start()

        # Also watch for the process dying unexpectedly
        threading.Thread(target=self._wait_proc, daemon=True).start()

        return self.ready(timeout=timeout)

    # --------------------------------------------------------------------------
    #
    def _wait_proc(self) -> None:
        """Wait for the subprocess to exit and log the result."""
        if self._proc is None:
            return
        rc = self._proc.wait()
        logger.info("%s: flux instance exited (rc=%s)", self._uid, rc)

    # --------------------------------------------------------------------------
    #
    def ready(self, timeout: float | None = None) -> bool:
        """Check (or wait for) readiness.

        Args:
            timeout: Seconds to wait.  Negative → wait indefinitely.
                ``None`` → return immediately.

        Returns:
            ``True`` if ready.
        """
        if timeout is None:
            return self._ready.is_set()
        if timeout < 0:
            self._ready.wait()
        else:
            self._ready.wait(timeout=timeout)
        return self._ready.is_set()

    # --------------------------------------------------------------------------
    #
    def stop(self) -> None:
        """Terminate the Flux subprocess."""
        if self._proc is not None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self._proc.kill()

        self._proc = None
        self._uri = None
        self._r_uri = None
        self._ready.clear()

        logger.info("%s: flux instance stopped", self._uid)
