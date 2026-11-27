"""Contract: ``Batch.function()`` mode and the task-handle result API.

Rhapsody's Dragon backend collects each result through the task handle::

    result = task.get(block=True)      # re-raises the task's exception
    tb = task.traceback                # str after a raised exception
    stdout = task.get_stdout(block=False) or ""   # None unless stdio paths are set
    stderr = task.get_stderr(block=False) or ""

and ``capture_stdio`` sets ``batch.options(stdout=path, stderr=path)``. Those calls and their return types are the contract. Rhapsody does not read
``batch.results_ddict`` (its entry layout changed from a 5- to a 3-tuple).

Helpers live in ``_dragon_ci_helpers`` — see the conftest module docstring
for why callables passed to Batch tasks cannot be defined in this file.
"""

from __future__ import annotations

import pytest
from _dragon_ci_helpers import add  # noqa: E402  (must be importable from workers)
from _dragon_ci_helpers import kwfn  # noqa: E402  (must be importable from workers)
from _dragon_ci_helpers import print_and_return  # noqa: E402  (must be importable from workers)
from _dragon_ci_helpers import raise_value_error  # noqa: E402  (must be importable from workers)
from _dragon_ci_helpers import slow_double  # noqa: E402  (must be importable from workers)
from dragon.workflows.batch import Batch
from dragon.workflows.batch.batch import Function
from dragon.workflows.batch.batch import TaskNotReadyError


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


def test_function_get_blocking_returns_result(batch: Batch):
    """Rhapsody calls ``get(block=True)``, then reads ``traceback`` only on a raise."""
    task = batch.function(add, 1, 1)
    assert task.get(block=True) == 2


def test_function_traceback_after_exception(batch: Batch):
    """After ``get()`` re-raises, ``task.traceback`` is a str naming the exception."""
    task = batch.function(raise_value_error, "boom")
    with pytest.raises(ValueError):
        task.get(block=True)
    assert isinstance(task.traceback, str) and "ValueError" in task.traceback


def test_function_default_stdio_is_not_captured(batch: Batch):
    """Without stdio paths, a function's output is not captured (Dragon 0.15.0).

    ``get_stdout()``/``get_stderr()`` return None, and Rhapsody reports "". If this fails,
    Dragon captures function output by default again.
    """
    task = batch.function(print_and_return, 7, "hello-from-function")
    assert task.get(timeout=60.0) == 7
    assert task.get_stdout(block=False) is None
    assert task.get_stderr(block=False) is None


def test_function_options_stdio_paths_capture_output(batch: Batch, tmp_path):
    """Rhapsody's ``capture_stdio``: ``batch.options(stdout=, stderr=)`` writes the files."""
    out, err = tmp_path / "task.stdout", tmp_path / "task.stderr"
    task = batch.options(stdout=str(out), stderr=str(err)).function(
        print_and_return, 7, "hello-from-function"
    )
    assert task.get(timeout=60.0) == 7
    assert task.stdout_path and task.stderr_path
    assert "hello-from-function" in out.read_text()
    assert "err msg" in err.read_text()
