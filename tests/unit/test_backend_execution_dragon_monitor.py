"""DragonExecutionBackend monitor-thread handoff, tested without a Dragon install.

The monitor loop runs on its own thread (``dragon_monitor_loop``) and must hand every task state
change to the asyncio event loop, so consumer callbacks never run off the loop. These tests drive
the real monitor thread against a fake Batch, so they also run where Dragon is absent (the main
Dragon test module skips there).
"""

import asyncio
import queue
import threading
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
    """Batch task Dragon cancelled on its own (timeout / upstream cancel).

    get() raises TaskCancelledError once ``release`` is set; it is set from the start unless a
    test needs to hold the monitor thread inside get().
    """

    def __init__(self, tuid):
        self.uid = tuid
        self.get_entered = threading.Event()
        self.release = threading.Event()
        self.release.set()

    def get(self, block=True):
        self.get_entered.set()
        self.release.wait(5.0)
        raise FakeTaskCancelledError(self.uid)

    def cancel(self):
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


def _track(backend, uid, batch_task):
    """Register a dispatched task the way build_task() does; return its description."""
    task_desc = {"uid": uid}
    backend._task_registry[uid] = {
        "uid": uid,
        "description": task_desc,
        "batch_task": batch_task,
        "is_native_function": True,
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


async def test_dragon_side_cancel_is_delivered_on_the_event_loop_thread(backend):
    """CANCELED for a task Dragon cancelled itself is reported once, on the event-loop thread.

    Dragon raises TaskCancelledError from get() after a timeout or an upstream cancel. Calling the
    callback straight from dragon_monitor_loop would run the consumer's state handlers off the loop;
    it must take the same call_soon_threadsafe path as DONE/FAILED.
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
    assert (task, state) == (task_desc, "CANCELED")
    assert thread is threading.current_thread()
    assert uid not in backend._task_registry


async def test_dragon_side_cancel_after_user_cancel_is_not_reported_twice(backend):
    """A user cancel_task() racing Dragon's own cancellation reports CANCELED exactly once.

    The monitor thread is held inside get() while cancel_task() reports CANCELED and drops the task;
    the Dragon-side cancellation delivered afterwards must find nothing left to report.
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
