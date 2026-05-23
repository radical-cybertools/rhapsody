"""Pytest configuration for the Dragon CI contract suite.

Run with::

    dragon python -m pytest -c tests/dragon_ci/pytest.ini \
        --rootdir=tests/dragon_ci tests/dragon_ci/

The ``-c`` / ``--rootdir`` flags keep pytest from picking up the parent
``tests/conftest.py`` (which imports Rhapsody).

Worker-importability note
-------------------------
cloudpickle pickles top-level functions **by reference** (just the
``module.qualname`` pair). Dragon workers re-import the module by that
name, and their ``sys.path`` is NOT inherited from the parent — only the
``PYTHONPATH`` env var is. So a helper that the parent can import only
because of a runtime ``sys.path.insert`` will fail in the worker, the
result is never written to the DDict, and ``task.get()`` blocks forever
(see ``test_worker_failure_modes.py``).

To avoid the trap, this conftest puts the suite directory on both
``sys.path`` (for the parent) and ``PYTHONPATH`` (for workers). All
callables passed to Batch tasks must live in ``_dragon_ci_helpers``,
not in a ``test_*.py`` file.
"""

from __future__ import annotations

import os
import sys

import pytest
import pytest_asyncio


# --- Make _dragon_ci_helpers importable from workers ----------------------

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)
os.environ["PYTHONPATH"] = _HERE + os.pathsep + os.environ.get("PYTHONPATH", "")


# --- Dragon must be importable -------------------------------------------

pytest.importorskip("dragon", reason="run under `dragon python`")


# --- Environment detection (single pass at import time) ------------------


def _safe(fn, default):
    try:
        return fn()
    except Exception:
        return default


def _detect_nnodes() -> int:
    from dragon.native.machine import System

    return int(System().nnodes)


def _detect_ngpus() -> int:
    from dragon.telemetry.collector import identify_gpu

    _vendor, count = identify_gpu()
    # AMD's identify_gpu returns a list; normalize to length.
    return len(count) if isinstance(count, list) else int(count or 0)


def _has_telemetry_collector() -> bool:
    import dragon.telemetry.collector  # noqa: F401

    return True


_NNODES = _safe(_detect_nnodes, 0)
_NGPUS = _safe(_detect_ngpus, 0)
_HAS_TELEM = _safe(_has_telemetry_collector, False)
# PMI/PMIx launch failures abort the entire Dragon runtime, so we cannot
# probe at runtime. Gate on explicit opt-in instead.
_HAS_PMI = os.environ.get("DRAGON_CI_HAS_PMI", "").lower() not in ("", "0", "false")


# --- Auto-skip markers ----------------------------------------------------

# (marker_name, predicate-that-must-be-true-to-run, reason)
_SKIP_RULES = (
    ("requires_multi_node", _NNODES >= 2, f"requires >= 2 Dragon nodes (have {_NNODES})"),
    ("requires_gpu", _NGPUS >= 1, "requires at least one GPU"),
    ("requires_mpi", _HAS_PMI, "set DRAGON_CI_HAS_PMI=1 to enable MPI/PMIx tests"),
    ("requires_telemetry_collector", _HAS_TELEM, "dragon.telemetry.collector not importable"),
)


def pytest_collection_modifyitems(config, items):
    for marker, ok, reason in _SKIP_RULES:
        if ok:
            continue
        skip = pytest.mark.skip(reason=reason)
        for item in items:
            if marker in item.keywords:
                item.add_marker(skip)


# --- Batch fixtures -------------------------------------------------------


def _new_batch():
    """Construct a Batch with Dragon's required mp start method set."""
    import multiprocessing as mp

    from dragon.workflows.batch import Batch

    try:
        if mp.get_start_method(allow_none=True) != "dragon":
            mp.set_start_method("dragon", force=True)
    except RuntimeError:
        pass
    return Batch(disable_telem=True)


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def batch():
    """One Batch shared across the session.

    Async-fixture so construction happens inside an asyncio loop, mirroring
    Rhapsody's own V3 tests. Tests must NOT kill workers or call ``close()``
    on this Batch — use ``fresh_batch`` for that.
    """
    b = _new_batch()
    try:
        yield b
    finally:
        try:
            b.join(timeout=30.0)
        except Exception:
            pass


@pytest_asyncio.fixture(loop_scope="session")
async def fresh_batch():
    """A throwaway Batch for tests that intentionally kill workers — the
    session ``batch`` cannot recover once a worker is SIGKILL'd.

    Skips the graceful ``join()`` because dead workers make it block for
    the full timeout; ``terminate()`` can also raise ``DragonUserCodeError``
    on dead workers — both are swallowed.
    """
    b = _new_batch()
    try:
        yield b
    finally:
        try:
            b.terminate()
        except Exception:
            pass
