"""Documented Dragon bug — kept failing until Dragon fixes it.

``SIGKILL`` inside a function task denies Dragon a clean exit signal, so
the result-DDict entry is never written and the dispatcher has no way to
learn the worker died. ``task.get()`` then blocks indefinitely — its
``timeout=`` kwarg only governs manager selection, not the blocking
DDict read.

When Dragon adds abnormal-worker-death detection (or finally honours
``task.get(timeout=)``), the call below will return, pytest-timeout will
stop firing, and the test goes green.

Lives in its own file because ``method="thread"`` leaks the test thread
(still wedged inside Dragon's blocking read) and the leaked thread holds
resources that break any subsequent test in the same pytest invocation.
"""

import pytest

from dragon.workflows.batch import Batch

from _dragon_ci_helpers import fn_kill_self  # noqa: E402


@pytest.mark.timeout(5, method="thread")
def test_sigkill_in_worker_hangs(fresh_batch: Batch):
    fresh_batch.function(fn_kill_self, 1).get(timeout=2.0)
