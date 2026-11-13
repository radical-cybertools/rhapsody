"""DragonExecutionBackend monitor-thread handoff, tested without a Dragon install.

The monitor loop runs on its own thread (``dragon_monitor_loop``) and must hand every task state
change to the asyncio event loop, so consumer callbacks never run off the loop. These tests drive
the real monitor thread against a fake Batch, so they also run where Dragon is absent (the main
Dragon test module skips there).
"""

import asyncio
import queue
import signal
import threading
import time
from unittest.mock import patch

import pytest

from rhapsody.backends.execution import dragon as dragon_module
from rhapsody.backends.execution.dragon import DragonExecutionBackend


class FakeTaskCancelledError(Exception):
    """Stands in for dragon.workflows.batch.TaskCancelledError."""


class FakeBatch:
    """Batch whose poll() returns the tuids the test marks as completed."""

    num_workers = 1
    num_managers = 1

    def __init__(self):
        self.completed_tuids = queue.Queue()

    def poll(self, timeout=None):
        try:
            return self.completed_tuids.get(timeout=timeout)
        except queue.Empty:
            return None


class FakeCancelledBatchTask:
    """Batch task whose result is Dragon's TaskCancelledError.

    get() raises TaskCancelledError once ``release`` is set; it is set from the start unless a
    test needs to hold the monitor thread inside get(). cancel() returns True once
    ``cancel_release`` is set, also set from the start unless a test needs to hold cancel_task()
    while Dragon handles the cancel.
    """

    def __init__(self, tuid):
        self.uid = tuid
        self.get_entered = threading.Event()
        self.release = threading.Event()
        self.release.set()
        self.cancel_entered = threading.Event()
        self.cancel_release = threading.Event()
        self.cancel_release.set()

    def get(self, block=True):
        self.get_entered.set()
        self.release.wait(5.0)
        raise FakeTaskCancelledError(self.uid)

    def cancel(self):
        self.cancel_entered.set()
        self.cancel_release.wait(5.0)
        return True


@pytest.fixture
def backend():
    """DragonExecutionBackend on a FakeBatch; stops the monitor thread on teardown."""
    # TaskCancelledError stays patched while the monitor thread runs: it is looked up on raise.
    with (
        patch.object(dragon_module, "Batch", return_value=FakeBatch()),
        patch.object(dragon_module, "TaskCancelledError", FakeTaskCancelledError),
    ):
        backend = DragonExecutionBackend()
        yield backend
        backend._shutdown_event.set()
        if backend._batch_monitor_thread is not None:
            backend._batch_monitor_thread.join(timeout=5.0)


def _track(backend, uid, batch_task, is_native_function=True, timeout=None, submitted_at=None):
    """Register a dispatched task the way build_task() does; return its description."""
    task_desc = {"uid": uid}
    backend._task_registry[uid] = {
        "uid": uid,
        "description": task_desc,
        "batch_task": batch_task,
        "is_native_function": is_native_function,
        "timeout": timeout,
        "submitted_at": time.monotonic() if submitted_at is None else submitted_at,
    }
    backend._monitored_batches[batch_task.uid] = (batch_task, uid)
    return task_desc


def _record_callbacks(backend):
    """Register a callback recording (task, state, thread); return the records and an Event."""
    delivered = []
    on_delivery = threading.Event()

    def callback(task, state):
        delivered.append((task, state, threading.current_thread()))
        on_delivery.set()

    backend.register_callback(callback)
    return delivered, on_delivery


async def test_cancel_no_one_asked_for_is_a_failure_delivered_on_the_event_loop_thread(backend):
    """A TaskCancelledError for a task cancel_task() never asked to cancel ends FAILED, once, on the
    event-loop thread.

    CANCELED is only for a requested cancel. Calling the callback straight from dragon_monitor_loop
    would run the consumer's state handlers off the loop; it must take the same call_soon_threadsafe
    path as DONE/FAILED.
    """
    uid = "task.dragon-side-cancel"
    batch_task = FakeCancelledBatchTask("dragon-tuid-dragon-side-cancel")
    task_desc = _track(backend, uid, batch_task)
    delivered, on_delivery = _record_callbacks(backend)

    # Captures the running loop and starts dragon_monitor_loop, as the first submission does.
    await backend.submit_tasks([])
    backend.batch.completed_tuids.put(batch_task.uid)

    assert await asyncio.to_thread(on_delivery.wait, 5.0)
    [(task, state, thread)] = delivered
    assert (task, state) == (task_desc, "FAILED")
    assert isinstance(task["exception"], FakeTaskCancelledError)
    assert thread is threading.current_thread()
    assert uid not in backend._task_registry


async def test_requested_cancel_seen_first_by_the_monitor_is_canceled(backend):
    """The monitor delivers a requested cancel's TaskCancelledError while cancel_task() still waits
    for Dragon's answer: CANCELED is reported once, and cancel_task() returns True."""
    uid = "task.monitor-first"
    batch_task = FakeCancelledBatchTask("dragon-tuid-monitor-first")
    batch_task.cancel_release.clear()
    task_desc = _track(backend, uid, batch_task)
    delivered, on_delivery = _record_callbacks(backend)

    await backend.submit_tasks([])
    cancel = asyncio.create_task(backend.cancel_task(uid))
    assert await asyncio.to_thread(batch_task.cancel_entered.wait, 5.0)
    backend.batch.completed_tuids.put(batch_task.uid)
    assert await asyncio.to_thread(on_delivery.wait, 5.0)

    batch_task.cancel_release.set()
    assert await cancel is True
    assert [(task, state) for task, state, _ in delivered] == [(task_desc, "CANCELED")]
    assert "exception" not in task_desc
    assert uid not in backend._cancel_requested


async def test_requested_cancel_is_canceled_when_cancel_task_is_itself_cancelled(backend):
    """If the cancel_task() coroutine is cancelled while Dragon handles the cancel, the
    TaskCancelledError that cancel causes is still reported CANCELED, not FAILED."""
    uid = "task.cancel-task-cancelled"
    batch_task = FakeCancelledBatchTask("dragon-tuid-cancel-task-cancelled")
    batch_task.cancel_release.clear()
    task_desc = _track(backend, uid, batch_task)
    delivered, on_delivery = _record_callbacks(backend)

    await backend.submit_tasks([])
    cancel = asyncio.create_task(backend.cancel_task(uid))
    assert await asyncio.to_thread(batch_task.cancel_entered.wait, 5.0)
    cancel.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancel
    batch_task.cancel_release.set()

    backend.batch.completed_tuids.put(batch_task.uid)
    assert await asyncio.to_thread(on_delivery.wait, 5.0)
    assert [(task, state) for task, state, _ in delivered] == [(task_desc, "CANCELED")]


async def test_dragon_side_cancel_after_user_cancel_is_not_reported_twice(backend):
    """A cancel_task() that reports CANCELED before the monitor sees the TaskCancelledError it
    caused reports CANCELED exactly once.

    The monitor thread is held inside get() while cancel_task() reports CANCELED and drops the task;
    the TaskCancelledError delivered afterwards must find nothing left to report.
    """
    uid = "task.user-cancel-race"
    batch_task = FakeCancelledBatchTask("dragon-tuid-user-cancel-race")
    batch_task.release.clear()
    task_desc = _track(backend, uid, batch_task)
    delivered, _ = _record_callbacks(backend)

    await backend.submit_tasks([])
    backend.batch.completed_tuids.put(batch_task.uid)
    assert await asyncio.to_thread(batch_task.get_entered.wait, 5.0)

    assert await backend.cancel_task(uid) is True

    # Let get() raise, then stop the monitor so its last sweep has been handed to the loop.
    batch_task.release.set()
    backend._shutdown_event.set()
    await asyncio.to_thread(backend._batch_monitor_thread.join, 5.0)
    await asyncio.sleep(0.05)

    assert [(task, state) for task, state, _ in delivered] == [(task_desc, "CANCELED")]
    assert uid not in backend._task_registry
    assert uid not in backend._cancel_requested


class FakeDoneBatchTask:
    """Batch task that completed with ``value``; ``stdout_error`` makes get_stdout() raise."""

    traceback = None

    def __init__(self, tuid, value, stdout_error=None):
        self.uid = tuid
        self.value = value
        self.stdout_error = stdout_error

    def get(self, block=True):
        return self.value

    def get_stdout(self, block=False):
        if self.stdout_error is not None:
            raise self.stdout_error
        return ""

    def get_stderr(self, block=False):
        return ""


async def test_sweep_resolved_before_an_error_is_still_delivered(backend):
    """A tuid that raises mid-sweep does not drop the DONE/FAILED already resolved in it."""
    done_task = FakeDoneBatchTask("dragon-tuid-done", 7)
    cancelled_task = FakeCancelledBatchTask("dragon-tuid-cancelled")
    broken_task = FakeDoneBatchTask("dragon-tuid-broken", 0, stdout_error=RuntimeError("boom"))
    done_desc = _track(backend, "task.done", done_task)
    _track(backend, "task.cancelled", cancelled_task)
    _track(backend, "task.broken", broken_task)
    delivered, _ = _record_callbacks(backend)

    # Queued before the monitor starts, so one sweep drains all three in this order.
    for batch_task in (done_task, cancelled_task, broken_task):
        backend.batch.completed_tuids.put(batch_task.uid)
    await backend.submit_tasks([])

    backend._shutdown_event.set()
    await asyncio.to_thread(backend._batch_monitor_thread.join, 5.0)
    await asyncio.sleep(0.05)

    states = {task["uid"]: state for task, state, _ in delivered}
    assert states == {"task.done": "DONE", "task.cancelled": "FAILED"}
    assert done_desc["return_value"] == 7


# ----------------------------------------------------------------------------
# Process and job timeouts. Dragon reports only exit codes when a task's walltime expires.
# ----------------------------------------------------------------------------

SIGINT, SIGTERM, SIGKILL = -signal.SIGINT, -signal.SIGTERM, -signal.SIGKILL


async def _deliver_process_result(backend, result, timeout, ran_for):
    """Deliver a process/job task that returned ``result`` ``ran_for`` s after its submission."""
    uid = "task.process"
    batch_task = FakeDoneBatchTask("dragon-tuid-process", result)
    task_desc = _track(
        backend,
        uid,
        batch_task,
        is_native_function=False,
        timeout=timeout,
        submitted_at=time.monotonic() - ran_for,
    )
    delivered, on_delivery = _record_callbacks(backend)
    await backend.submit_tasks([])
    backend.batch.completed_tuids.put(batch_task.uid)
    assert await asyncio.to_thread(on_delivery.wait, 5.0)
    [(task, state, _)] = delivered
    assert task is task_desc
    return task, state


@pytest.mark.parametrize(
    ("result", "exit_code", "codes_text"),
    [
        (SIGINT, SIGINT, f"exit code {SIGINT}"),
        (SIGTERM, SIGTERM, f"exit code {SIGTERM}"),
        (SIGKILL, SIGKILL, f"exit code {SIGKILL}"),
        ([SIGINT, SIGINT], SIGINT, f"exit codes [{SIGINT}, {SIGINT}]"),
        # A rank died and the others waited for it until the timeout.
        ([1, SIGINT], 1, f"exit codes [1, {SIGINT}]"),
    ],
)
async def test_process_stopped_at_its_timeout_fails_with_timeout_error(
    backend, result, exit_code, codes_text
):
    """A process or job task Dragon stopped at its timeout ends FAILED with a TimeoutError and keeps
    its exit code."""
    task, state = await _deliver_process_result(backend, result, timeout=5, ran_for=6)

    assert state == "FAILED"
    assert isinstance(task["exception"], TimeoutError)
    assert str(task["exception"]) == (
        f"task task.process exceeded its timeout of 5 s ({codes_text})"
    )
    assert task["exit_code"] == exit_code
    assert task["return_value"] == result


@pytest.mark.parametrize(
    ("result", "timeout", "ran_for"),
    [
        (SIGKILL, 60, 1),  # killed well before its timeout
        (-signal.SIGSEGV, 5, 6),  # crashed, not stopped by Dragon
        (SIGINT, None, 6),  # no timeout to expire
        (1, 5, 6),  # an ordinary failure
    ],
)
async def test_other_process_failures_are_not_timeouts(backend, result, timeout, ran_for):
    """A failed process or job task is reported as timed out only when it ended by a walltime signal
    after its timeout."""
    task, state = await _deliver_process_result(backend, result, timeout=timeout, ran_for=ran_for)

    assert state == "FAILED"
    assert "exception" not in task
    assert task["exit_code"] == result


async def test_clean_exit_after_the_timeout_is_done(backend):
    """A task that exits 0 is DONE, however long it took to report."""
    task, state = await _deliver_process_result(backend, 0, timeout=5, ran_for=6)

    assert state == "DONE"
    assert "exception" not in task
