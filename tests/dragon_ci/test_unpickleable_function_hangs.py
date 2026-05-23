"""Documented Dragon bug — kept failing until Dragon fixes it.

A helper module made importable in the parent only via runtime
``sys.path.insert`` (not via ``PYTHONPATH``) cannot be re-imported by the
worker. cloudpickle pickles the helper by reference, the worker dies
during unpickling, and ``task.get()`` blocks forever — same root cause
as the SIGKILL hang.

This is the silent-hang failure mode pytest users hit when they define a
helper at the top of a ``test_*.py`` file and pass it to
``batch.function()``. The conftest works around it by adding this
directory to ``PYTHONPATH``; the test below disables that workaround
locally to reproduce the bug.

Lives in its own file for the same reason as the other ``*_hangs.py``
files: ``method="thread"`` leaks the test thread which breaks
subsequent tests in the same pytest invocation.
"""

import os
import sys

import pytest

from dragon.workflows.batch import Batch


@pytest.mark.timeout(5, method="thread")
def test_unpickleable_function_hangs(fresh_batch: Batch):
    here = os.path.dirname(os.path.abspath(__file__))
    offpath = os.path.join(here, "_offpath")
    assert offpath not in os.environ.get("PYTHONPATH", "").split(os.pathsep), (
        "test setup error: _offpath ended up on PYTHONPATH"
    )

    sys.path.insert(0, offpath)
    try:
        from marker_helper import offpath_add  # noqa: PLC0415
    finally:
        sys.path.remove(offpath)

    fresh_batch.function(offpath_add, 3, 4).get(timeout=2.0)
