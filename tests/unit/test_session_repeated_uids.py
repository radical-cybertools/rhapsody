"""A uid names one task in a Session: what a repeat of a uid does.

A task submitted with the uid of a running task, or of an ended task the Session keeps, is a
repeat. It attaches to that task: it gets the task's future and results, and never reaches a
backend.
"""

import asyncio
import logging
import os
import time
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest

from rhapsody import ComputeTask
from rhapsody.api import Session
from rhapsody.api import TaskStateManager
from rhapsody.backends import ConcurrentExecutionBackend


class RecordingBackend:
    """A backend that records what it receives and reports states when a test says so."""

    def __init__(self, name="recording"):
        self.name = name
        self.attached_to = []
        self.is_attached = False
        self.received = []
        self._callback = None

    def register_callback(self, func):
        self._callback = func

    def get_task_states_map(self):
        return SimpleNamespace(terminal_states=("DONE", "FAILED", "CANCELED"))

    async def submit_tasks(self, tasks):
        self.received.extend(tasks)

    def report(self, task, state, **fields):
        task.update(fields)
        self._callback(task, state)

    async def shutdown(self):
        pass


def double(x):
    return 2 * x


def triple(x):
    return 3 * x


def failing_call():
    """An exception raised for real, so that it carries a traceback."""
    try:
        raise ValueError("simulation diverged")
    except ValueError as exc:
        return exc


@pytest.fixture
def backend():
    return RecordingBackend()


@pytest.fixture
def session(backend, tmp_path):
    return Session(backends=[backend], work_dir=str(tmp_path))


@pytest.mark.asyncio
async def test_a_repeat_of_a_running_task_shares_its_future_and_gets_its_results(backend, session):
    first = ComputeTask(uid="sim-1", function=double, args=(2,))
    repeat = ComputeTask(uid="sim-1", function=double, args=(2,))

    (first_fut,) = await session.submit_tasks([first])
    backend.report(first, "RUNNING")
    (repeat_fut,) = await session.submit_tasks([repeat])

    assert backend.received == [first]
    assert backend.received[0] is first
    assert repeat_fut is first_fut
    assert repeat.state == "RUNNING"

    backend.report(first, "DONE", return_value=4, stdout="4", exit_code=0)

    assert await repeat is first
    assert repeat.state == "DONE"
    assert repeat.return_value == 4
    assert repeat.stdout == "4"
    assert await session.wait_tasks([repeat], timeout=1) == [repeat]


@pytest.mark.asyncio
async def test_a_repeat_of_an_ended_task_gets_its_outcome_and_runs_nothing(backend, session):
    first = ComputeTask(uid="sim-1", function=double, args=(2,))
    await session.submit_tasks([first])
    backend.report(first, "DONE", return_value=4, exit_code=0)
    await first

    repeat = ComputeTask(uid="sim-1", function=double, args=(2,))
    (fut,) = await session.submit_tasks([repeat])

    assert backend.received == [first]
    assert backend.received[0] is first
    assert fut.done()
    assert await repeat is repeat
    assert repeat.state == "DONE"
    assert repeat.return_value == 4
    assert await session.wait_tasks([repeat], timeout=1) == [repeat]


@pytest.mark.asyncio
async def test_a_repeat_of_a_failed_task_gets_its_exception_without_the_traceback(backend, session):
    first = ComputeTask(uid="sim-1", function=double, args=(2,))
    (first_fut,) = await session.submit_tasks([first])
    error = failing_call()
    backend.report(first, "FAILED", exception=error, stderr="diverged")
    with pytest.raises(ValueError, match="simulation diverged"):
        await first_fut

    repeat = ComputeTask(uid="sim-1", function=double, args=(2,))
    (fut,) = await session.submit_tasks([repeat])

    assert backend.received == [first]
    assert backend.received[0] is first
    assert repeat.state == "FAILED"
    assert repeat.stderr == "diverged"
    assert type(repeat.exception) is ValueError
    assert repeat.exception.args == error.args
    assert repeat.exception.__traceback__ is None
    assert first.exception is error
    assert error.__traceback__ is not None
    with pytest.raises(ValueError, match="simulation diverged"):
        await fut


@pytest.mark.asyncio
async def test_a_repeat_of_a_failed_executable_raises_its_exit_code(backend, session):
    first = ComputeTask(uid="cmd-1", executable="false")
    await session.submit_tasks([first])
    backend.report(first, "FAILED", exit_code=1, stderr="no")

    repeat = ComputeTask(uid="cmd-1", executable="false")
    (fut,) = await session.submit_tasks([repeat])

    assert backend.received == [first]
    assert backend.received[0] is first
    assert repeat.exit_code == 1
    with pytest.raises(Exception, match="exit code 1"):
        await fut


@pytest.mark.asyncio
async def test_a_repeat_of_a_cancelled_task_is_cancelled_too(backend, session):
    first = ComputeTask(uid="sim-1", function=double, args=(2,))
    await session.submit_tasks([first])
    backend.report(first, "CANCELED")

    repeat = ComputeTask(uid="sim-1", function=double, args=(2,))
    await session.submit_tasks([repeat])

    assert backend.received == [first]
    assert backend.received[0] is first
    assert repeat.state == "CANCELED"


@pytest.mark.asyncio
async def test_a_task_that_fails_while_repeated_fails_both(backend, session):
    first = ComputeTask(uid="sim-1", function=double, args=(2,))
    repeat = ComputeTask(uid="sim-1", function=double, args=(2,))
    await session.submit_tasks([first])
    await session.submit_tasks([repeat])

    error = failing_call()
    backend.report(first, "FAILED", exception=error)

    for task in (first, repeat):
        with pytest.raises(ValueError, match="simulation diverged"):
            await task
    assert repeat.exception is error


@pytest.mark.asyncio
async def test_two_tasks_with_one_uid_in_one_call_run_once(backend, session):
    first = ComputeTask(uid="sim-1", function=double, args=(2,))
    repeat = ComputeTask(uid="sim-1", function=double, args=(2,))

    first_fut, repeat_fut = await session.submit_tasks([first, repeat])

    assert backend.received == [first]
    assert backend.received[0] is first
    assert repeat_fut is first_fut
    backend.report(first, "DONE", return_value=4)
    await repeat
    assert repeat.return_value == 4


@pytest.mark.asyncio
async def test_the_same_task_submitted_again_runs_once(backend, session):
    task = ComputeTask(uid="sim-1", function=double, args=(2,))
    (fut,) = await session.submit_tasks([task])
    assert await session.submit_tasks([task]) == [fut]

    error = failing_call()
    backend.report(task, "FAILED", exception=error)
    with pytest.raises(ValueError):
        await fut

    (again,) = await session.submit_tasks([task])
    assert backend.received == [task]
    assert task.exception is error  # its own exception, with its traceback, stays
    with pytest.raises(ValueError):
        await again


@pytest.mark.asyncio
async def test_a_repeat_with_another_function_warns_and_gets_the_outcome(backend, session, caplog):
    first = ComputeTask(uid="sim-1", function=double, args=(2,))
    await session.submit_tasks([first])
    other = ComputeTask(uid="sim-1", function=triple, args=(2,))

    with caplog.at_level(logging.WARNING, logger="rhapsody.api.session"):
        await session.submit_tasks([other])

    assert "another function or executable" in caplog.text
    assert backend.received == [first]
    assert backend.received[0] is first
    backend.report(first, "DONE", return_value=4)
    await other
    assert other.return_value == 4

    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="rhapsody.api.session"):
        await session.submit_tasks([ComputeTask(uid="sim-1", function=double, args=(2,))])
    assert caplog.text == ""
    later = ComputeTask(uid="sim-1", executable="/bin/echo")
    with caplog.at_level(logging.WARNING, logger="rhapsody.api.session"):
        await session.submit_tasks([later])
    assert "another function or executable" in caplog.text
    assert later.return_value == 4
    assert len(backend.received) == 1


@pytest.mark.asyncio
async def test_a_plain_dict_repeat_attaches(backend, session):
    first = {"uid": "sim-1", "function": double, "args": (2,)}
    repeat = {"uid": "sim-1", "function": double, "args": (2,)}
    first_fut, repeat_fut = await session.submit_tasks([first, repeat])

    assert repeat_fut is first_fut
    backend.report(first, "DONE", return_value=4)
    assert await repeat_fut is first
    assert repeat["return_value"] == 4


@pytest.mark.asyncio
async def test_repeats_send_no_telemetry_task_events(backend, session):
    created, submitted, queued, changes = [], [], [], []
    session._telemetry = SimpleNamespace(
        _on_task_created=lambda t: created.append(t["uid"]),
        _on_task_submitted=lambda t: submitted.append(t["uid"]),
        _on_task_queued=lambda t: queued.append(t["uid"]),
    )
    session._state_manager._telemetry_observer = lambda t, s: changes.append((id(t), s))
    first = ComputeTask(uid="sim-1", function=double, args=(2,))
    repeat = ComputeTask(uid="sim-1", function=double, args=(2,))

    await session.submit_tasks([first])
    await session.submit_tasks([repeat])
    backend.report(first, "DONE", return_value=4)
    await session.submit_tasks([ComputeTask(uid="sim-1", function=double, args=(2,))])
    session._telemetry = None

    assert created == submitted == queued == ["sim-1"]
    assert changes == [(id(first), "DONE")]


@pytest.mark.asyncio
async def test_the_session_keeps_the_last_retained_ended_tasks(backend, tmp_path, caplog):
    session = Session(backends=[backend], work_dir=str(tmp_path), retained_tasks=2)
    tasks = [ComputeTask(uid=f"sim-{i}", function=double, args=(i,)) for i in range(3)]
    await session.submit_tasks(tasks)
    with caplog.at_level(logging.INFO, logger="rhapsody.api.session"):
        for i, task in enumerate(tasks):
            backend.report(task, "DONE", return_value=2 * i)

    assert caplog.text.count("keeps the last 2 ended tasks") == 1
    newest = ComputeTask(uid="sim-2", function=double, args=(2,))
    oldest = ComputeTask(uid="sim-0", function=double, args=(0,))
    await session.submit_tasks([newest, oldest])

    assert newest.return_value == 4
    assert backend.received == tasks + [oldest]
    assert backend.received[-1] is oldest


@pytest.mark.asyncio
async def test_retained_tasks_zero_keeps_no_ended_task(backend, tmp_path):
    session = Session(backends=[backend], work_dir=str(tmp_path), retained_tasks=0)
    first = ComputeTask(uid="sim-1", function=double, args=(2,))
    await session.submit_tasks([first, ComputeTask(uid="sim-1", function=double, args=(2,))])
    assert len(backend.received) == 1

    backend.report(first, "DONE", return_value=4)
    again = ComputeTask(uid="sim-1", function=double, args=(2,))
    await session.submit_tasks([again])

    assert backend.received == [first, again]
    assert backend.received[1] is again


@pytest.mark.asyncio
async def test_retained_tasks_none_keeps_every_ended_task(backend, tmp_path):
    session = Session(backends=[backend], work_dir=str(tmp_path), retained_tasks=None)
    tasks = [ComputeTask(uid=f"sim-{i}", function=double, args=(i,)) for i in range(50)]
    await session.submit_tasks(tasks)
    for task in tasks:
        backend.report(task, "DONE", return_value=0)

    repeats = [ComputeTask(uid=f"sim-{i}", function=double, args=(i,)) for i in range(50)]
    await session.submit_tasks(repeats)
    assert len(backend.received) == 50


@pytest.mark.parametrize("retained", [-1, 1.5, True, "10"])
def test_retained_tasks_must_be_none_or_a_count(retained):
    with pytest.raises(ValueError, match="retained must be None or an int >= 0"):
        Session(retained_tasks=retained)
    with pytest.raises(ValueError):
        TaskStateManager(retained=retained)


@pytest.mark.asyncio
async def test_waiting_on_an_ended_task_keeps_no_future(backend, session):
    task = ComputeTask(uid="sim-1", function=double, args=(2,))
    await session.submit_tasks([task])
    backend.report(task, "DONE", return_value=4)

    await session.wait_tasks([task])
    await session.wait_tasks([task])

    assert session._state_manager._task_futures == {}
    assert session._state_manager._running == {}


@pytest.mark.asyncio
async def test_a_failed_submit_forgets_its_tasks(backend, session):
    good = ComputeTask(uid="sim-1", function=double, args=(2,))
    bad = ComputeTask(uid="sim-2", function=double, args=(2,), backend="nowhere")
    with pytest.raises(ValueError, match="not found in Session"):
        await session.submit_tasks([good, bad])
    assert backend.received == []

    again = ComputeTask(uid="sim-1", function=double, args=(2,))
    await session.submit_tasks([again])
    assert backend.received == [again]
    assert backend.received[0] is again


# ---------------------------------------------------------------------------
# Every backend that runs in CI: a repeat never runs the task a second time.
# Backends that need a runtime (Dragon, RADICAL-Pilot, ORBIT, vLLM) never see a repeat
# either: the Session attaches it before routing, as the tests above show.
# ---------------------------------------------------------------------------


def count_run(marker_dir, value):
    """Record one run in ``marker_dir`` and return ``value`` after a pause."""
    with open(os.path.join(marker_dir, f"{os.getpid()}-{time.monotonic_ns()}"), "w"):
        pass
    time.sleep(0.3)
    return value


async def _concurrent_threads():
    return await ConcurrentExecutionBackend(ThreadPoolExecutor(max_workers=2))


async def _concurrent_processes():
    return await ConcurrentExecutionBackend(ProcessPoolExecutor(max_workers=2))


async def _dask():
    pytest.importorskip("dask.distributed")
    from rhapsody.backends import DaskExecutionBackend

    return await DaskExecutionBackend({"n_workers": 1, "threads_per_worker": 2, "processes": False})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "make_backend",
    [_concurrent_threads, _concurrent_processes, _dask],
    ids=["concurrent-threads", "concurrent-processes", "dask"],
)
async def test_a_repeat_runs_nothing_on_a_real_backend(make_backend, tmp_path):
    marker_dir = tmp_path / "runs"
    marker_dir.mkdir()
    real = await make_backend()
    async with Session(backends=[real], work_dir=str(tmp_path)) as session:

        def task():
            return ComputeTask(uid="sim-1", function=count_run, args=(str(marker_dir), 42))

        first = task()
        await session.submit_tasks([first])
        while_running = task()
        await session.submit_tasks([while_running])
        await asyncio.wait_for(session.wait_tasks([first, while_running]), timeout=60)
        after_end = task()
        await session.submit_tasks([after_end])
        await asyncio.wait_for(after_end, timeout=5)

    assert len(os.listdir(marker_dir)) == 1
    for t in (first, while_running, after_end):
        assert t.state == "DONE"
        assert t.return_value == 42
