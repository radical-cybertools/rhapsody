"""Documented Dragon bug — **test disabled**.

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

The test is **skipped** because letting it hit ``pytest.mark.timeout``
produces a hard failure that doesn't compose with ``pytest.mark.xfail``
(pytest-timeout raises a ``BaseException`` subclass that xfail does not
catch). To check whether Dragon has fixed the bug, remove the
``pytest.mark.skip`` decorator and run this file under
``dragon python -m pytest`` — the test will either return cleanly (Dragon
fixed it, re-enable permanently) or pytest-timeout will fire (still broken).

Lives in its own file for the same reason as the other ``*_hangs.py``
files: ``method="thread"`` leaks the test thread which breaks
subsequent tests in the same pytest invocation.
"""

import os
import sys
import warnings

import pytest
from dragon.workflows.batch import Batch

warnings.warn(
    "test_unpickleable_function_hangs is DISABLED — Dragon bug (worker dies "
    "during cloudpickle unpickling without diagnostic; task.get() blocks "
    "forever) is not actively checked. Re-enable to retest.",
    UserWarning,
    stacklevel=2,
)


@pytest.mark.skip(
    reason="DISABLED — Dragon bug: worker silently dies during cloudpickle "
           "unpickling; task.get() blocks forever. Re-enable to check if fixed."
)
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
