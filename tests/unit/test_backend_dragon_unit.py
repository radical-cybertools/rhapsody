"""Dragon-backend behavior that must hold WITHOUT a dragon install.

The full dragon test module skips when the runtime is absent, so the
contracts fixed for the remote DT demo are pinned here on a bare
instance: cancel of an untracked task is idempotent, and a failed
delivery attaches the remote traceback to the exception the user gets.
"""

import logging

import pytest

from rhapsody.backends.execution.dragon import DragonExecutionBackend


def bare_backend():
    """The slice of the backend the methods under test touch."""

    backend = object.__new__(DragonExecutionBackend)
    backend.logger = logging.getLogger("test.dragon.unit")
    backend._task_registry = {}

    delivered = []
    backend._callback_func = lambda task, state: delivered.append((dict(task), state))

    return backend, delivered


@pytest.mark.asyncio
async def test_cancel_of_an_untracked_task_is_a_noop():
    """A cancel can race a task that already finished or was never
    dispatched -- ROSE cancels its losing candidate branch exactly this
    way.  Raising here broke the caller's teardown and turned a clean
    branch-cancel into a DependencyFailureError on the surviving
    pipeline."""

    backend, delivered = bare_backend()

    assert await backend.cancel_task("task.000000") is False
    assert delivered == []


def test_a_failed_delivery_attaches_the_remote_traceback(caplog):
    """The worker's traceback reaches the user as a note on the task's exception, and as "traceback"
    for ORBIT to forward.

    A failure is the user's to report: nothing above DEBUG.
    """

    backend, delivered = bare_backend()
    backend._task_registry["task.000001"] = {"description": {"uid": "task.000001"}}
    tb = "Traceback (most recent call last):\n  ...\nValueError: boom happened\n"

    with caplog.at_level(logging.INFO, logger="test.dragon.unit"):
        backend._deliver_batch([("task.000001", ValueError("boom happened"), tb, True, "", "")])

    assert len(delivered) == 1
    task, state = delivered[0]
    assert state == "FAILED"
    exc = task["exception"]
    assert isinstance(exc, ValueError) and str(exc) == "boom happened"
    assert exc.__notes__ == [f"Remote traceback:\n{tb}"]
    assert task["traceback"] == tb
    assert "Traceback" in task["stderr"]
    assert caplog.records == []


def test_a_failed_delivery_without_traceback_adds_no_note():
    backend, delivered = bare_backend()
    backend._task_registry["task.000002"] = {"description": {"uid": "task.000002"}}

    backend._deliver_batch([("task.000002", ValueError("boom"), None, True, "", "")])

    task, state = delivered[0]
    assert state == "FAILED"
    assert not getattr(task["exception"], "__notes__", None)
    assert "traceback" not in task
