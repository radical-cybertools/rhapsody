"""Dragon-backend behavior that must hold WITHOUT a dragon install.

The full dragon test module skips when the runtime is absent, so the
contracts fixed for the remote DT demo are pinned here on a bare
instance: cancel of an untracked task is idempotent, and a failed
delivery logs the remote traceback endpoint-side.
"""

import logging

import pytest

from rhapsody.backends.execution.dragon import DragonExecutionBackend


def bare_backend():
    """The slice of the backend the methods under test touch."""

    backend = object.__new__(DragonExecutionBackend)
    backend.logger = logging.getLogger("test.dragon.unit")
    backend._task_registry = {}
    backend._cancelled_tasks = set()

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


def test_a_failed_delivery_logs_the_remote_traceback(caplog):
    """The worker captures the traceback and the consumer side only ever sees the exception message
    -- the endpoint log is where it must surface."""

    backend, delivered = bare_backend()
    backend._task_registry["task.000001"] = {"description": {"uid": "task.000001"}}

    with caplog.at_level(logging.ERROR, logger="test.dragon.unit"):
        backend._deliver_batch(
            [
                (
                    "task.000001",
                    "boom happened",
                    "Traceback (most recent call last):\n  ...",
                    True,
                    "",
                    "",
                ),
            ]
        )

    assert len(delivered) == 1
    task, state = delivered[0]
    assert state == "FAILED"
    assert task["exception"] == "boom happened"
    assert "Traceback" in task["stderr"]

    assert "task.000001 failed: boom happened" in caplog.text
    assert "Traceback (most recent call last):" in caplog.text
