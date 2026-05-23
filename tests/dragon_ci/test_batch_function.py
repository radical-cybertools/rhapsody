"""Contract: ``Batch.function()`` mode and the results-DDict tuple shape.

Rhapsody's V3 backend reads results directly from the Batch-owned DDict::

    result, tb, raised, stdout, stderr = batch.results_ddict[task.uid]

so the tuple layout, the truthiness of ``raised``, and the types of
``stdout``/``stderr`` are all part of the contract. ``Function.get()`` is the
documented public alternative and is also pinned.

Helpers live in ``_dragon_ci_helpers`` — see the conftest module docstring
for why callables passed to Batch tasks cannot be defined in this file.
"""

from __future__ import annotations

import pytest

from dragon.workflows.batch import Batch
from dragon.workflows.batch.batch import Function, TaskNotReadyError

from _dragon_ci_helpers import (  # noqa: E402  (must be importable from workers)
    add, kwfn, print_and_return, raise_value_error, slow_double,
)


def test_function_returns_function_handle(batch: Batch):
    """``batch.function()`` returns a ``Function`` with ``.uid`` and ``.get``."""
    task = batch.function(add, 2, 3)
    assert isinstance(task, Function)
    assert task.uid and callable(task.get)
    assert task.get(timeout=60.0) == 5


def test_function_get_reraises_exception(batch: Batch):
    task = batch.function(raise_value_error, "intentional failure")
    with pytest.raises(ValueError, match="intentional failure"):
        task.get(timeout=60.0)


def test_function_supports_kwargs(batch: Batch):
    """``Batch.function`` must forward ``**kwargs`` to the target."""
    assert batch.function(kwfn, 6, mult=7).get(timeout=60.0) == 42


def test_function_get_non_blocking_raises_when_not_ready(batch: Batch):
    """``Function.get(block=False)`` raises ``TaskNotReadyError`` pre-completion."""
    task = batch.function(slow_double, 21, 1.0)
    with pytest.raises(TaskNotReadyError):
        task.get(block=False)
    assert task.get(timeout=60.0) == 42


def test_results_ddict_five_tuple_on_success(batch: Batch):
    """Rhapsody unpacks exactly five fields. Order and shape are the contract."""
    task = batch.function(add, 1, 1)
    task.get(timeout=60.0)
    entry = batch.results_ddict[task.uid]
    assert isinstance(entry, tuple) and len(entry) == 5, (
        f"results_ddict tuple shape changed: {entry!r}"
    )
    result, tb, raised, stdout, stderr = entry
    assert result == 2
    assert tb is None and raised is False
    assert isinstance(stdout, str) and isinstance(stderr, str)


def test_results_ddict_five_tuple_on_failure(batch: Batch):
    """On exception: ``raised=True``, ``result`` holds the exception, ``tb`` is str."""
    task = batch.function(raise_value_error, "boom")
    with pytest.raises(ValueError):
        task.get(timeout=60.0)
    result, tb, raised, stdout, stderr = batch.results_ddict[task.uid]
    assert raised is True
    assert isinstance(result, BaseException)
    assert isinstance(tb, str) and "ValueError" in tb
    assert isinstance(stdout, str) and isinstance(stderr, str)


def test_function_captures_stdout_and_stderr(batch: Batch):
    task = batch.function(print_and_return, 7, "hello-from-function")
    assert task.get(timeout=60.0) == 7
    _r, _tb, _raised, stdout, stderr = batch.results_ddict[task.uid]
    assert "hello-from-function" in stdout
    assert "err msg" in stderr
