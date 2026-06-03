"""Documented Dragon bug — **test disabled**.

``SIGKILL`` inside a function task denies Dragon a clean exit signal, so
the result-DDict entry is never written and the dispatcher has no way to
learn the worker died. ``task.get()`` then blocks indefinitely — its
``timeout=`` kwarg only governs manager selection, not the blocking
DDict read.

The test below is **skipped** because letting it hit ``pytest.mark.timeout``
produces a hard failure that doesn't compose with ``pytest.mark.xfail``
(pytest-timeout raises a ``BaseException`` subclass that xfail does not
catch). To check whether Dragon has fixed the bug, remove the
``pytest.mark.skip`` decorator and run this file under
``dragon python -m pytest`` — the test will either return cleanly (Dragon
fixed it, re-enable permanently) or pytest-timeout will fire (still broken).

Lives in its own file because ``method="thread"`` leaks the test thread
(still wedged inside Dragon's blocking read) and the leaked thread holds
resources that break any subsequent test in the same pytest invocation.
"""

import warnings

import pytest
from _dragon_ci_helpers import fn_kill_self  # noqa: E402
from dragon.workflows.batch import Batch

warnings.warn(
    "test_sigkill_in_worker_hangs is DISABLED — Dragon bug (abnormal worker "
    "termination is not detected; task.get() blocks indefinitely and its "
    "timeout= kwarg is not honored on the blocking DDict read) is not "
    "actively checked. Re-enable to retest.",
    UserWarning,
    stacklevel=2,
)


@pytest.mark.skip(
    reason="DISABLED — Dragon bug: SIGKILL'd worker is undetected, task.get() "
           "blocks forever, timeout= is ignored. Re-enable to check if fixed."
)
@pytest.mark.timeout(5, method="thread")
def test_sigkill_in_worker_hangs(fresh_batch: Batch):
    fresh_batch.function(fn_kill_self, 1).get(timeout=2.0)
