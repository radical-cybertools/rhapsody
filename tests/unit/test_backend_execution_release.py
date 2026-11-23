"""A backend lets go of a task once it has reported the task's final state.

One backend can run hundreds of thousands of tasks. A task it keeps after the task ended costs
memory, and so does the handler (an asyncio Task) that ran it. On a thread pool the handlers cost
time too: in Python 3.12 ``asyncio.run`` in a worker thread walks every Task still alive in the
process, so each task would cost more than the one before.

The caller keeps the task dicts in these tests, as an engine that hands them back to its users
does: what the backend leaves in a dict must not keep anything alive either.
"""

import asyncio
import gc
import sys
import threading
import weakref

import pytest

from rhapsody.backends.execution.concurrent import ConcurrentExecutionBackend
from rhapsody.backends.execution.noop import NoopExecutionBackend

TERMINAL = {"DONE", "FAILED", "CANCELED"}

needs_eager_tasks = pytest.mark.skipif(
    sys.version_info < (3, 12), reason="eager tasks are new in Python 3.12"
)


class _Recorder:
    """A backend callback: every state change, and a future for each task's final state."""

    def __init__(self):
        self.calls = []
        self._ended = {}

    def __call__(self, task, state):
        self.calls.append((task, state))
        if state in TERMINAL:
            ended = self._future(task)
            if not ended.done():
                ended.set_result(state)

    def _future(self, task):
        if id(task) not in self._ended:
            self._ended[id(task)] = asyncio.get_running_loop().create_future()
        return self._ended[id(task)]

    async def ended(self, task, timeout=10.0):
        """The task's final state, once the backend reported it."""
        return await asyncio.wait_for(self._future(task), timeout)

    def states(self, task):
        return [state for t, state in self.calls if t is task]


async def _double(x):
    return 2 * x


def _fail():
    raise ValueError("no")


def _handlers_alive(refs):
    gc.collect()
    return sum(ref() is not None for ref in refs)


@pytest.fixture
def recorder():
    return _Recorder()


@pytest.fixture(params=[False, True], ids=["plain", "observe_peaks"])
async def backend(recorder, request):
    backend = await ConcurrentExecutionBackend(observe_peaks=request.param)
    backend.register_callback(recorder)
    yield backend
    await backend.shutdown()


@pytest.fixture
async def eager_tasks():
    loop = asyncio.get_running_loop()
    loop.set_task_factory(asyncio.eager_task_factory)
    yield
    loop.set_task_factory(None)


async def test_many_tasks_keep_the_concurrent_backend_bounded(backend, recorder):
    functions = [{"uid": f"double.{i}", "function": _double, "args": [i]} for i in range(300)]
    failures = [{"uid": f"fail.{i}", "function": _fail} for i in range(300)]
    commands = [
        {"uid": "echo", "executable": "/bin/echo", "arguments": ["hi"]},
        {"uid": "exit", "executable": "/bin/sh", "arguments": ["-c", "exit 3"]},
    ]
    tasks = functions + failures + commands

    handlers = [weakref.ref(h) for h in await backend.submit_tasks(tasks)]
    for task in tasks:
        await recorder.ended(task)

    assert backend.tasks == {}
    assert [t["uid"] for t in tasks if "future" in t] == []
    assert _handlers_alive(handlers) == 0

    # What a caller reads once a task ended is all still there
    for i, task in enumerate(functions):
        assert recorder.states(task) == ["RUNNING", "DONE"]
        assert task["return_value"] == 2 * i
    for task in failures:
        assert recorder.states(task) == ["RUNNING", "FAILED"]
        assert isinstance(task["exception"], ValueError)
    assert recorder.states(commands[0])[-1] == "DONE"
    assert commands[0]["stdout"] == "hi\n"
    assert recorder.states(commands[1])[-1] == "FAILED"
    assert commands[1]["exit_code"] == 3


async def test_a_cancelled_task_is_released(backend, recorder):
    running = {"uid": "running", "executable": "/bin/sleep", "arguments": ["10"]}
    await backend.submit_tasks([running])
    await asyncio.sleep(0.2)
    # Cancelled before its handler starts, so the handler never runs its own clean-up
    queued = {"uid": "queued", "executable": "/bin/sleep", "arguments": ["10"]}
    [handler] = await backend.submit_tasks([queued])
    handler = weakref.ref(handler)

    assert await backend.cancel_task("queued")
    assert await backend.cancel_task("running")

    assert backend.tasks == {}
    assert "future" not in running and "future" not in queued
    assert _handlers_alive([handler]) == 0
    assert recorder.states(queued) == ["CANCELED"]
    assert recorder.states(running) == ["RUNNING", "CANCELED"]
    # A task that ended cannot be cancelled
    assert not await backend.cancel_task("running")


async def test_a_task_that_ends_leaves_a_later_task_of_its_uid(backend, recorder):
    first_go, second_go = threading.Event(), threading.Event()
    first = {"uid": "same", "function": first_go.wait, "args": [10]}
    second = {"uid": "same", "function": second_go.wait, "args": [10]}
    await backend.submit_tasks([first])
    await backend.submit_tasks([second])
    try:
        first_go.set()
        assert await recorder.ended(first) == "DONE"

        # The uid still names the later task, which can still be cancelled
        assert backend.tasks["same"] is second
        assert await backend.cancel_task("same")
    finally:
        second_go.set()

    assert recorder.states(second) == ["RUNNING", "CANCELED"]
    assert backend.tasks == {}


async def test_a_task_submitted_again_while_it_runs_stays_cancellable(backend, recorder):
    events = []

    def wait():
        event = threading.Event()
        events.append(event)
        return event.wait(10)

    task = {"uid": "again", "function": wait}
    await backend.submit_tasks([task])
    while len(events) < 1:
        await asyncio.sleep(0.01)
    [second] = await backend.submit_tasks([task])
    try:
        while len(events) < 2:
            await asyncio.sleep(0.01)
        events[0].set()
        assert await recorder.ended(task) == "DONE"

        # The first run ended; the second still runs, and is still the task's
        assert backend.tasks["again"] is task
        assert task["future"] is second
        assert await backend.cancel_task("again")
    finally:
        for event in events:
            event.set()
    assert backend.tasks == {}
    assert "future" not in task


async def test_cancel_all_tasks_counts_the_tasks_that_still_run(backend, recorder):
    ended = [{"uid": f"double.{i}", "function": _double, "args": [i]} for i in range(5)]
    await backend.submit_tasks(ended)
    for task in ended:
        await recorder.ended(task)

    go = threading.Event()
    running = {"uid": "running", "function": go.wait, "args": [10]}
    await backend.submit_tasks([running])
    await asyncio.sleep(0.1)
    # Cancelled before its handler starts
    queued = {"uid": "queued", "function": go.wait, "args": [10]}
    await backend.submit_tasks([queued])
    try:
        assert await backend.cancel_all_tasks() == 2
    finally:
        go.set()
    assert backend.tasks == {}
    assert "future" not in running and "future" not in queued


@needs_eager_tasks
async def test_a_task_whose_handler_ends_eagerly_is_released(eager_tasks, backend, recorder):
    # No function and no executable: it fails before its handler first waits, so an eager task
    # factory runs the whole handler inside submit_tasks
    broken = {"uid": "broken"}
    works = {"uid": "works", "function": _double, "args": [2]}

    handlers = [weakref.ref(h) for h in await backend.submit_tasks([broken, works])]
    assert recorder.states(broken) == ["RUNNING", "FAILED"]
    assert await recorder.ended(works) == "DONE"

    assert backend.tasks == {}
    assert "future" not in broken and "future" not in works
    # The failure's traceback keeps the frames it was raised under, submit_tasks's among them,
    # as any exception does: Python keeps those, not the backend
    broken["exception"].__traceback__ = None
    assert _handlers_alive(handlers) == 0
    assert not await backend.cancel_task("broken")


async def test_many_tasks_keep_the_noop_backend_bounded(recorder):
    backend = await NoopExecutionBackend()
    backend.register_callback(recorder)
    tasks = [{"uid": f"noop.{i}"} for i in range(200)]
    await backend.submit_tasks(tasks)
    for task in tasks:
        await recorder.ended(task)
    assert backend.tasks == {}
    assert backend._futures == {}
    await backend.shutdown()


@needs_eager_tasks
async def test_noop_tasks_that_end_eagerly_are_released(eager_tasks, recorder):
    backend = await NoopExecutionBackend()
    backend.register_callback(recorder)
    tasks = [{"uid": f"noop.{i}"} for i in range(200)]
    await backend.submit_tasks(tasks)
    assert [recorder.states(t) for t in tasks] == [["DONE"]] * len(tasks)
    assert backend.tasks == {}
    assert backend._futures == {}
    await backend.shutdown()
