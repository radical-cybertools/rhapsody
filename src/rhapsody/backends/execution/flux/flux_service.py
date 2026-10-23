"""Flux instance lifecycle manager.

This module provides :class:`FluxService`, which starts and manages a
``flux start`` subprocess, waits for the ``FLUX_URI`` to become available,
and exposes both a local URI (for same-host connections) and an SSH-reachable
remote URI.
"""

from __future__ import annotations

import logging
import os
import re
import subprocess
import threading
import uuid
from typing import Any
from urllib.parse import urlparse
from urllib.parse import urlunparse

from .flux_module import FluxModule

logger = logging.getLogger(__name__)

# Broker config that makes job-exec launch each job shell as a systemd unit
# with its own cgroup (flux-config-exec(5), flux-config-systemd(5)).
_SDEXEC_CONF = ("--conf=systemd.enable=true", "--conf=exec.service=sdexec")

# systemd's environment rules (strictest variant, systemd v239): see
# flux-framework/flux-core#7836.
_ENV_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_ENV_BAD_CHARS_RE = re.compile(r"[\x00-\x08\x0b-\x1f\x7f]")

# Printed by the instance's initial program when the sdexec smoke job fails.
_SDEXEC_FAILED = "RHAPSODY_FLUX_SDEXEC_FAILED"


def systemd_safe_env(env: dict[str, str]) -> dict[str, str]:
    """Return *env* without the entries systemd rejects for transient units.

    Under sdexec the broker environment is passed to every job unit, and
    systemd refuses the whole block if a single entry is invalid (exported
    bash functions such as ``BASH_FUNC_module%%``, values with control
    characters such as a colored ``PS1``).
    """
    clean = {}
    for name, value in env.items():
        if not _ENV_NAME_RE.match(name) or _ENV_BAD_CHARS_RE.search(value):
            continue
        try:
            value.encode("utf-8")
        except UnicodeEncodeError:  # undecodable bytes (surrogateescape)
            continue
        clean[name] = value
    return clean


def user_systemd_available() -> bool:
    """Return True if a user systemd bus is reachable (where flux's sdbus looks)."""
    addr = os.environ.get("DBUS_SESSION_BUS_ADDRESS", "")
    if addr.startswith("unix:path="):
        return os.path.exists(addr[len("unix:path=") :].split(",")[0])
    runtime_dir = os.environ.get("XDG_RUNTIME_DIR")
    return bool(runtime_dir) and os.path.exists(os.path.join(runtime_dir, "bus"))


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
        sdexec: Run jobs under systemd (``exec.service = "sdexec"``) so each
            job gets its own cgroup.  ``"auto"`` enables it when a user
            systemd bus is reachable and ``flux start`` supports ``--conf``;
            ``True`` requires it; ``False`` (default) uses flux's default
            ``rexec``.  If the instance cannot run a job under sdexec, the
            service restarts without it (``"auto"``) or fails (``True``).

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
        sdexec: bool | str = False,
    ) -> None:
        if sdexec not in (True, False, "auto"):
            raise ValueError(f"sdexec must be True, False or 'auto', got {sdexec!r}")
        self._uid: str = uid or str(uuid.uuid4())
        self._launcher: str = launcher or ""
        self._sdexec_mode = sdexec
        self._sdexec: bool = False

        self._fm = FluxModule()
        self._uri: str | None = None
        self._r_uri: str | None = None
        self._host: str | None = None
        self._proc: subprocess.Popen | None = None
        self._ready: threading.Event = threading.Event()
        # Set on readiness, sdexec smoke-job failure, or process exit.
        self._settled: threading.Event = threading.Event()
        self._sdexec_failed: bool = False

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

    @property
    def sdexec(self) -> bool:
        """True if the running instance launches jobs via sdexec."""
        return self._sdexec

    # --------------------------------------------------------------------------
    #
    def _reader(self, stream: Any, proc: subprocess.Popen) -> None:
        """Read *stream* line-by-line and forward each line to :meth:`_on_line`."""
        try:
            for raw in stream:
                line = raw.decode(errors="replace").rstrip("\n")
                logger.debug("%s: flux io: %s", self._uid, line)
                if proc is not self._proc:
                    continue  # output of a replaced process (sdexec → rexec relaunch)
                self._on_line(line)
        except Exception:  # noqa: BLE001, S110
            pass

    # --------------------------------------------------------------------------
    #
    def _on_line(self, line: str) -> None:
        """Process a single output line from the ``flux start`` subprocess."""
        if line.startswith(_SDEXEC_FAILED):
            self._sdexec_failed = True
            self._settled.set()
            return
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
        self._settled.set()

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

        Raises:
            RuntimeError: If ``sdexec=True`` and the instance cannot run jobs
                under sdexec.
        """
        use_sdexec = self._sdexec_mode is True or (
            self._sdexec_mode == "auto" and self._sdexec_possible()
        )
        if not use_sdexec or timeout is None:
            # Non-blocking start cannot detect an sdexec failure — no fallback.
            return self._launch(use_sdexec, timeout)

        if self._launch(True, timeout):
            return True
        if not self._sdexec_failed:
            # Startup timeout or exit: not an sdexec problem, a rexec retry won't help.
            return False
        if self._sdexec_mode is True:
            self.stop()
            raise RuntimeError(f"{self._uid}: flux instance cannot run jobs under sdexec")
        logger.warning(
            "%s: flux instance cannot run jobs under sdexec — restarting with rexec "
            "(sysmon values will cover the broker's cgroup, not the job's)",
            self._uid,
        )
        self.stop()
        return self._launch(False, timeout)

    # --------------------------------------------------------------------------
    #
    def _sdexec_possible(self) -> bool:
        """Check the preconditions for ``sdexec="auto"``."""
        if not user_systemd_available():
            logger.info("%s: no user systemd bus — not using sdexec", self._uid)
            return False
        try:
            out = subprocess.run(
                [self._fm.exe, "start", "--help"],
                capture_output=True,
                text=True,
                timeout=30,
            )
        except Exception:  # noqa: BLE001
            return False
        if "--conf=" not in out.stdout + out.stderr:
            logger.info("%s: 'flux start' has no --conf — not using sdexec", self._uid)
            return False
        return True

    # --------------------------------------------------------------------------
    #
    def _launch(self, sdexec: bool, timeout: float | None) -> bool:
        """Start one ``flux start`` subprocess and wait for it to settle."""
        self._sdexec = sdexec
        self._sdexec_failed = False
        self._settled.clear()

        banner = (
            "echo FLUX_URI=\\$FLUX_URI FLUX_HOST=\\$(hostname) && flux resource list && sleep inf"
        )
        if sdexec:
            # Smoke job on every node: under sdexec a bad broker environment
            # or a missing user systemd only shows when a job is launched.
            fcmd = (
                "if flux run -N \\$(flux getattr size) true; then "
                f"{banner}; else echo {_SDEXEC_FAILED}; fi"
            )
            conf = " " + " ".join(_SDEXEC_CONF)
            env = systemd_safe_env(dict(os.environ))
        else:
            fcmd = banner
            conf = ""
            env = None
        cmd_str = f'{self._launcher} {self._fm.exe} start{conf} bash -c "{fcmd}"'.strip()

        logger.info("%s: starting flux instance: %s", self._uid, cmd_str)

        self._proc = subprocess.Popen(  # noqa: S602
            cmd_str,
            shell=True,  # noqa: S602
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )

        # Read both streams in daemon threads so we never block
        readers = []
        for stream in (self._proc.stdout, self._proc.stderr):
            t = threading.Thread(target=self._reader, args=(stream, self._proc), daemon=True)
            t.start()
            readers.append(t)

        # Also watch for the process dying unexpectedly
        threading.Thread(target=self._wait_proc, args=(self._proc, readers), daemon=True).start()

        if timeout is None:
            return self.ready()
        self._settled.wait(timeout=None if timeout < 0 else timeout)
        return self._ready.is_set()

    # --------------------------------------------------------------------------
    #
    def _wait_proc(self, proc: subprocess.Popen, readers: list[threading.Thread]) -> None:
        """Wait for the subprocess to exit and log the result."""
        rc = proc.wait()
        logger.info("%s: flux instance exited (rc=%s)", self._uid, rc)
        # Let the readers handle the last lines (e.g. the sdexec failure marker)
        # before start() looks at the outcome.
        for t in readers:
            t.join(timeout=5)
        if proc is self._proc:
            self._settled.set()

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
