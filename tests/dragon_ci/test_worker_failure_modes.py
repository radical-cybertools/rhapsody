"""Contract: Dragon's worker-failure detection — the *clean* paths.

Two failure modes Dragon DOES detect, surfaced through ``task.get()``:

1. **Caught exception** in the user function → the exception instance and
   its traceback land in the results-DDict 5-tuple (``raised=True``),
   and ``task.get()`` re-raises it.
2. **Clean process exit** (``sys.exit(N)``) → no result is written, but
   Dragon's dispatcher notices and synthesises
   ``RuntimeError("function worker exited without producing output: …")``.

The silent-hang failure modes (abnormal worker death, unpickleable
function) live in their own files because they leak threads that break
subsequent tests in the same pytest invocation:

- ``test_sigkill_in_worker_hangs.py``
- ``test_unpickleable_function_hangs.py``
- ``test_ddict_unknown_key_blocks.py``
"""

from __future__ import annotations

import pytest

from dragon.workflows.batch import Batch

from _dragon_ci_helpers import fn_assert_false, fn_sys_exit_one  # noqa: E402


def test_caught_exception_is_reraised_and_tuple_recorded(batch: Batch):
    task = batch.function(fn_assert_false, 1)
    with pytest.raises(AssertionError, match="intentional dragon_ci probe"):
        task.get(timeout=10.0)
    result, tb, raised, _stdout, _stderr = batch.results_ddict[task.uid]
    assert raised is True
    assert isinstance(result, AssertionError)
    assert isinstance(tb, str) and "AssertionError" in tb


def test_sys_exit_in_worker_raises_runtime_error(batch: Batch):
    """Clean ``sys.exit`` surfaces as ``RuntimeError`` with that exact phrase."""
    task = batch.function(fn_sys_exit_one, 1)
    with pytest.raises(RuntimeError, match="exited without producing output"):
        task.get(timeout=10.0)
