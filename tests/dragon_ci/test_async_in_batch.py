"""Contract: async-coroutine target dispatch via ``Batch.function``.

Rhapsody V3's ``build_task`` detects ``asyncio.iscoroutinefunction(target)``
and wraps the coroutine in a ``asyncio.run(...)`` shim before passing it to
``batch.function``. This test pins that pattern, and also verifies the
underlying primitives Rhapsody depends on:

- ``asyncio.iscoroutinefunction`` recognises the user's function,
- ``Batch.function`` accepts a plain (non-coroutine) callable,
- a coroutine result reaches ``batch.results_ddict[uid]`` unchanged after
  the wrapper applies ``asyncio.run``.

Helpers live in ``_dragon_ci_helpers`` — see the conftest module docstring
for why callables passed to Batch tasks cannot be defined in this file.
"""

from __future__ import annotations

import asyncio

from _dragon_ci_helpers import async_double  # noqa: E402
from _dragon_ci_helpers import async_run_shim  # noqa: E402
from dragon.workflows.batch import Batch


def test_iscoroutinefunction_detects_async_def():
    assert asyncio.iscoroutinefunction(async_double)
    assert not asyncio.iscoroutinefunction(async_run_shim)


def test_async_result_reaches_results_ddict(batch: Batch):
    """The shimmed coroutine result must surface as a plain int in the DDict."""
    task = batch.function(async_run_shim, 21)
    assert task.get(timeout=60.0) == 42

    result, _tb, raised, _stdout, _stderr = batch.results_ddict[task.uid]
    assert raised is False
    assert result == 42
