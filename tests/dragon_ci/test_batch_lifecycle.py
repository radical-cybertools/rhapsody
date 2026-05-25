"""Contract: ``dragon.workflows.batch.Batch`` construction, attributes, teardown.

Rhapsody's V3 backend constructs ``Batch`` with the kwargs below, reads
``num_workers`` / ``num_managers`` for logging, and shuts down via
``close()`` then ``join(timeout=...)`` with ``terminate()`` as a fallback.
``fence()`` is exposed to user code as a barrier.
"""

from __future__ import annotations

import inspect

import pytest
from dragon.workflows.batch import Batch

_BATCH_CTOR_KWARGS = ("num_nodes", "pool_nodes", "disable_telem",
                      "scheduler_workers", "results_ddict_mem")
_BATCH_METHODS = ("function", "process", "job", "fence", "close", "join", "terminate")


@pytest.mark.parametrize("kwarg", _BATCH_CTOR_KWARGS)
def test_batch_ctor_kwarg(kwarg):
    assert kwarg in inspect.signature(Batch.__init__).parameters, (
        f"Batch.__init__ no longer accepts {kwarg!r}"
    )


@pytest.mark.parametrize("method", _BATCH_METHODS)
def test_batch_method_present(method):
    assert callable(getattr(Batch, method, None)), f"Batch.{method}() removed"


def test_batch_num_workers_and_num_managers(batch):
    """Rhapsody logs ``batch.num_workers`` and ``batch.num_managers`` at startup."""
    assert isinstance(batch.num_workers, int) and batch.num_workers >= 1
    assert isinstance(batch.num_managers, int) and batch.num_managers >= 1


def test_batch_results_ddict_supports_lookup(batch):
    """Rhapsody reads ``batch.results_ddict[task.uid]`` directly."""
    rd = batch.results_ddict
    assert hasattr(rd, "__contains__") and hasattr(rd, "__getitem__")


def test_batch_fence_callable_on_idle_batch(batch):
    """Rhapsody exposes ``Batch.fence()`` to user code as a barrier. On an
    idle Batch it must complete without raising."""
    batch.fence()


def test_batch_close_join_terminate_lifecycle():
    """A throwaway Batch must accept close()+join()+terminate() without raising.

    Combined into one test so we only pay one Batch startup. Note ``close()``
    is deprecated as of Dragon 0.14 (no-op); only ``join()`` is real teardown.
    """
    b = Batch(disable_telem=True)
    b.close()
    b.join(timeout=30.0)
    b.terminate()
