"""Flux Python-module loader and jobspec factory functions.

This module handles discovery and lazy import of the Flux Python bindings
(``flux`` and ``flux.job``), with a fallback that shells out to the ``flux``
executable to locate the correct Python path when the bindings are not on
``sys.path`` by default.  It also provides convenience factory functions for
building ``flux.job.JobspecV1`` objects from simple Python data structures.
"""

from __future__ import annotations

import importlib
import math
import os
import shlex
import shutil
import subprocess
import sys
import uuid
from typing import Any


# ------------------------------------------------------------------------------
#
def _short_uid() -> str:
    """Return a short random identifier."""
    return str(uuid.uuid4()).replace("-", "")[:12]


# ------------------------------------------------------------------------------
#
class FluxModule:
    """Lazy loader for the Flux Python bindings.

    The bindings are imported once and cached at the class level so that
    multiple ``FluxModule`` instances share the same underlying objects.

    Two import strategies are attempted in order:

    1. Plain ``importlib.import_module('flux')`` / ``importlib.import_module('flux.job')``.
    2. Shell out to ``flux python -c "import flux; print(flux.__file__)"`` to
       discover the correct ``sys.path`` entry, then retry.

    Raises:
        RuntimeError: If ``verify()`` is called and any required component
            (core module, job module, or ``flux`` executable) is not available.
    """

    _flux_core: Any = None
    _flux_job: Any = None
    _flux_exc: Exception | None = None
    _flux_exe: str | None = None

    # --------------------------------------------------------------------------
    #
    def __init__(self) -> None:
        """Import the flux module if not already cached."""

        # Already initialised by a previous instance — nothing to do.
        if self._flux_core is not None or self._flux_exc is not None:
            return

        flux: Any = None
        flux_job: Any = None
        flux_exc: Exception | None = None

        # Strategy 1: normal import
        try:
            flux = importlib.import_module("flux")
            flux_job = importlib.import_module("flux.job")
        except Exception as exc:  # noqa: BLE001
            flux_exc = exc

        # Strategy 2: discover path via `flux python`
        if flux is None or flux_job is None:
            to_pop: str | None = None
            try:
                result = subprocess.run(
                    ["flux", "python", "-c", "import flux; print(flux.__file__)"],  # noqa: S607
                    capture_output=True,
                    text=True,
                    check=True,
                )
                flux_path = os.path.dirname(result.stdout.strip())
                mod_path = os.path.dirname(flux_path)
                sys.path.append(mod_path)
                to_pop = mod_path

                flux = importlib.import_module("flux")
                flux_job = importlib.import_module("flux.job")
                flux_exc = None

            except Exception as exc:  # noqa: BLE001
                flux_exc = exc

            finally:
                if to_pop and to_pop in sys.path:
                    sys.path.remove(to_pop)

        FluxModule._flux_core = flux
        FluxModule._flux_job = flux_job
        FluxModule._flux_exc = flux_exc
        FluxModule._flux_exe = shutil.which("flux")

    # --------------------------------------------------------------------------
    #
    def verify(self) -> None:
        """Assert that all required Flux components are available.

        Raises:
            RuntimeError: If the ``flux`` Python module, ``flux.job`` module,
                or the ``flux`` executable cannot be found.
        """
        if self._flux_core is None:
            raise RuntimeError("flux Python module not found") from self._flux_exc

        if self._flux_job is None:
            raise RuntimeError("flux.job Python module not found") from self._flux_exc

        if self._flux_exe is None:
            raise RuntimeError("flux executable not found on PATH") from self._flux_exc

        if not hasattr(self._flux_job, "JournalConsumer"):
            raise RuntimeError("flux.job.JournalConsumer not available — v1 API required")

    # --------------------------------------------------------------------------
    # Properties
    # --------------------------------------------------------------------------

    @property
    def core(self) -> Any:
        """The imported ``flux`` module."""
        return self._flux_core

    @property
    def job(self) -> Any:
        """The imported ``flux.job`` module."""
        return self._flux_job

    @property
    def exc(self) -> Exception | None:
        """The exception raised during import, if any."""
        return self._flux_exc

    @property
    def exe(self) -> str | None:
        """Absolute path to the ``flux`` executable, or ``None``."""
        return self._flux_exe


# ------------------------------------------------------------------------------
#
def spec_from_command(cmd: str) -> Any:
    """Build a ``JobspecV1`` from a shell command string.

    Args:
        cmd: Shell command string (e.g. ``"/bin/echo hello"``).  It is
            split with :func:`shlex.split` before being passed to Flux.

    Returns:
        A ``flux.job.JobspecV1`` with a randomly generated ``uid`` stored in
        ``spec.attributes['user']['uid']``.
    """
    fm = FluxModule()
    spec = fm.job.JobspecV1.from_command(shlex.split(cmd))
    if "user" not in spec.attributes:
        spec.attributes["user"] = {}
    spec.attributes["user"]["uid"] = _short_uid()
    return spec


# ------------------------------------------------------------------------------
#
def spec_from_dict(td: dict) -> Any:
    """Build a ``JobspecV1`` from a plain task description dictionary.

    The dictionary may contain the following keys (all optional unless noted):

    * ``executable`` (**required**): path or name of the executable.
    * ``arguments``: list of command-line arguments.
    * ``uid``: caller-supplied task identifier; a random one is generated
      when absent.
    * ``priority``: integer priority, clamped to ``[1, 31]``.
    * ``timeout``: wall-time limit in seconds (default ``0.0`` = unlimited).
    * ``ranks``: number of MPI ranks / task slots (default ``1``).
    * ``cores_per_rank``: CPU cores per slot (default ``1``).
    * ``gpus_per_rank``: GPUs per slot (default ``0``).
    * ``environment``: mapping of environment variables.
    * ``sandbox``: working directory (``cwd``).
    * ``shell``: launch shell.
    * ``stdin``, ``stdout``, ``stderr``: I/O redirection paths.
    * ``use_mpi``: when ``True`` (default) and ``ranks > 1``, sets
      ``exit-on-error = 1`` so that all ranks exit if one fails.

    Args:
        td: Task description dictionary.

    Returns:
        A fully constructed ``flux.job.JobspecV1``.
    """
    fm = FluxModule()

    version = 1
    user = {
        "uid": td.get("uid", _short_uid()),
        "priority": td.get("priority"),
    }
    system: dict = {"duration": td.get("timeout", 0.0)}
    tasks = [
        {
            "command": [td["executable"]] + td.get("arguments", []),
            "slot": "task",
            "count": {"per_slot": 1},
        }
    ]

    # Normalise priority to [1, 31]
    if user["priority"] is not None:
        user["priority"] = min(max(int(user["priority"]), 1), 31)

    if "environment" in td:
        system["environment"] = td["environment"]
    if "sandbox" in td:
        system["cwd"] = td["sandbox"]
    if "shell" in td:
        system["shell"] = td["shell"]

    attributes = {"system": system, "user": user}

    n_ranks = td.get("ranks", 1)
    resources = [
        {
            "count": n_ranks,
            "type": "slot",
            "label": "task",
            "with": [
                {
                    "count": int(td.get("cores_per_rank", 1)),
                    "type": "core",
                }
            ],
        }
    ]

    gpr = td.get("gpus_per_rank", 0)
    if gpr:
        resources[0]["with"].append(
            {
                "count": math.ceil(gpr),  # Flux requires integer GPU counts
                "type": "gpu",
            }
        )

    spec = fm.job.JobspecV1(
        resources=resources,
        attributes=attributes,
        tasks=tasks,
        version=version,
    )

    if n_ranks > 1:
        if td.get("use_mpi", True):
            spec.setattr_shell_option("exit-on-error", 1)
        else:
            spec.setattr_shell_option("exit-timeout", "none")

    if td.get("stdin"):
        spec.stdin = td["stdin"]
    if td.get("stdout"):
        spec.stdout = td["stdout"]
    if td.get("stderr"):
        spec.stderr = td["stderr"]

    return spec
