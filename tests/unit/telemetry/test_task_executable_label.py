"""The executable label on task telemetry for ComputeTask function and executable tasks.

ComputeTask always stores both an ``executable`` and a ``function`` key, one of them None. The
label must come from whichever is set, never from ``str(None)``.
"""

import functools
from concurrent.futures import ThreadPoolExecutor

import pytest

pytest.importorskip("opentelemetry", reason="opentelemetry-sdk not installed")

from rhapsody.api.session import Session
from rhapsody.api.task import ComputeTask
from rhapsody.backends.execution.concurrent import ConcurrentExecutionBackend
from rhapsody.telemetry.manager import _task_executable

TASK_EVENTS = {"TaskCreated", "TaskSubmitted", "TaskQueued", "TaskStarted", "TaskCompleted"}


def work(x=0):
    return x


def test_function_task_is_labelled_with_the_function_name():
    assert _task_executable(ComputeTask(function=work)) == "work"
    assert _task_executable(ComputeTask(function=functools.partial(work, 1))) == "work"


def test_executable_task_is_labelled_with_the_executable():
    assert _task_executable(ComputeTask(executable="/bin/echo", arguments=["hi"])) == "/bin/echo"


@pytest.fixture
async def backend():
    b = await ConcurrentExecutionBackend(executor=ThreadPoolExecutor(max_workers=2))
    yield b
    await b.shutdown()


async def test_session_events_and_spans_carry_the_label(backend):
    """Every lifecycle event and the task span of a function task carry the function name."""
    session = Session(backends=[backend])
    telemetry = await session.start_telemetry()
    received = []
    telemetry.subscribe(received.append)

    fn_task = ComputeTask(function=work, args=(1,))
    exe_task = ComputeTask(executable="/bin/echo", arguments=["hi"])
    tasks = [fn_task, exe_task]
    try:
        await session.submit_tasks(tasks)
        await session.wait_tasks(tasks, timeout=10.0)
    finally:
        await session.close()

    labels: dict[str, dict[str, str]] = {}
    for e in received:
        if e.event_type in TASK_EVENTS:
            labels.setdefault(e.task_id, {})[e.event_type] = e.attributes["executable"]
    assert labels[fn_task["uid"]] == dict.fromkeys(TASK_EVENTS, "work")
    assert labels[exe_task["uid"]] == dict.fromkeys(TASK_EVENTS, "/bin/echo")

    span_labels = sorted(
        s.attributes["executable"] for s in telemetry.read_traces() if s.name == "task"
    )
    assert span_labels == ["/bin/echo", "work"]
