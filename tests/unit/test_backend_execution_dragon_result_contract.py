"""The result contract of the Dragon execution backends, tested without a Dragon install.

What a consumer such as AsyncFlow relies on:

- A task that runs past its ``timeout`` ends FAILED, with a ``TimeoutError`` as its exception.
- A task cancelled through ``cancel_task()`` ends CANCELED, and ``cancel_task()`` returns True.
- ``cancel_task()`` returns False, and reports nothing, for an unknown uid or a task that has
  already ended. It never raises for those.
- Every task gets exactly one terminal report.

Each backend runs over a fake of its Dragon runtime, selected by the ``contract_backend``
fixture's parameter. To add a backend, write a context manager that yields it over its fake
runtime and add it to ``CONTRACT_BACKENDS``. The tests use only the backend API:
``submit_tasks``, ``cancel_task`` and ``register_callback``.
"""

import asyncio
import contextlib
import itertools
import queue
import signal
import threading
import time
from unittest.mock import patch

import pytest

from rhapsody.backends.execution import dragon as dragon_module

TERMINAL = {"DONE", "FAILED", "CANCELED"}

# Long enough that only a timeout or a cancel ends a task first.
LONG = 30.0
# Short enough to keep the timeout tests quick.
TIMEOUT = 0.3


def sleep_for(seconds):
    """A function task that runs for ``seconds``."""
    time.sleep(seconds)
    return seconds


# ============================================================================
# Batch: DragonExecutionBackend over a fake dragon.workflows.batch.Batch
# ============================================================================


class FakeTaskCancelledError(Exception):
    """Stands in for dragon.workflows.batch.TaskCancelledError."""


class FakeProcessTemplate:
    """Stands in for dragon.native.process.ProcessTemplate."""

    def __init__(self, target, args=(), kwargs=None, **options):
        self.target = target
        self.args = tuple(args)


class FakeBatchTask:
    """A Batch task that behaves as Dragon 0.15.0's did on a real runtime.

    It runs for ``args[0]`` seconds (a ``sleep_for`` function, or a ``sleep`` or ``trap-sigint``
    executable), and an executable named ``false`` exits 1 at once. When the timeout expires
    first, a function task raises TimeoutError, and a process or job task returns -SIGINT for
    each process, the exit code Dragon's walltime kill gave, or 130 for ``trap-sigint``, which
    catches SIGINT and exits as a bash trap does. A cancel accepted before the end makes
    ``get()`` raise TaskCancelledError.
    """

    traceback = None
    stdout_path = None
    stderr_path = None

    def __init__(self, batch, kind, target, args, timeout, nranks=1):
        self.uid = f"fake-tuid-{next(batch.tuids)}"
        self._batch = batch
        self._lock = threading.Lock()
        self._finished = threading.Event()
        self._outcome = None
        if kind != "function" and target == "false":
            duration, value = 0.0, 1
        else:
            duration = float(args[0]) if args else 0.0
            value = duration if kind == "function" else 0
        if nranks > 1:
            value = [value] * nranks
        if timeout < duration:
            if kind == "function":
                value = TimeoutError(f"task timed out before completing: tuid={self.uid}")
            else:
                code = 128 + signal.SIGINT if target == "trap-sigint" else -signal.SIGINT
                value = [code] * nranks if nranks > 1 else code
            duration = timeout
        self._timer = threading.Timer(duration, self._finish, args=(value,))
        self._timer.daemon = True
        self._timer.start()

    def _finish(self, outcome):
        with self._lock:
            if self._finished.is_set():
                return False
            self._outcome = outcome
            self._finished.set()
        self._batch.completed.put(self.uid)
        return True

    def cancel(self):
        self._timer.cancel()
        return self._finish(FakeTaskCancelledError(f"task {self.uid} was cancelled"))

    def get(self, block=True):
        self._finished.wait()
        if isinstance(self._outcome, BaseException):
            raise self._outcome
        return self._outcome

    def get_stdout(self, block=False):
        return ""

    def get_stderr(self, block=False):
        return ""


class FakeBatchOptions:
    def __init__(self, batch, timeout):
        self._batch = batch
        self._timeout = timeout

    def function(self, target, *args, **kwargs):
        return FakeBatchTask(self._batch, "function", target, args, self._timeout)

    def process(self, template):
        return FakeBatchTask(self._batch, "process", template.target, template.args, self._timeout)

    def job(self, templates):
        nranks = sum(n for n, _ in templates)
        template = templates[0][1]
        return FakeBatchTask(
            self._batch, "job", template.target, template.args, self._timeout, nranks
        )


class FakeBatch:
    num_workers = 4
    num_managers = 1

    def __init__(self, **kwargs):
        self.completed = queue.Queue()
        self.tuids = itertools.count()

    def options(self, timeout, **kwargs):
        return FakeBatchOptions(self, timeout)

    def poll(self, timeout=None):
        try:
            return self.completed.get(timeout=timeout)
        except queue.Empty:
            return None

    def join(self, timeout=None):
        pass

    def destroy(self, timeout=None):
        pass

    def log_dir(self):
        raise RuntimeError("task_logs disabled")


@contextlib.asynccontextmanager
async def batch_backend():
    with (
        patch.object(dragon_module, "Batch", FakeBatch),
        patch.object(dragon_module, "TaskCancelledError", FakeTaskCancelledError),
        patch.object(dragon_module, "ProcessTemplate", FakeProcessTemplate),
    ):
        backend = await dragon_module.DragonExecutionBackend()
        try:
            yield backend
        finally:
            await backend.shutdown()


# ============================================================================
# The contract
# ============================================================================

# name -> async context manager yielding a backend over its fake runtime. The native backend
# (dragon_native.py) joins here, with its own fake, once it is on main.
CONTRACT_BACKENDS = {"batch": batch_backend}


@pytest.fixture(params=sorted(CONTRACT_BACKENDS))
async def contract_backend(request):
    async with CONTRACT_BACKENDS[request.param]() as backend:
        yield backend


class Reports:
    """Every state report the backend makes, and a way to wait for a task's terminal one."""

    def __init__(self, backend):
        self.states: dict[str, list[str]] = {}
        self._changed = asyncio.Event()
        backend.register_callback(self._record)

    def _record(self, task, state):
        self.states.setdefault(task["uid"], []).append(state)
        self._changed.set()

    def terminal(self, uid):
        return [s for s in self.states.get(uid, []) if s in TERMINAL]

    async def wait_terminal(self, uid, timeout=10.0):
        async def _wait():
            while not self.terminal(uid):
                self._changed.clear()
                await self._changed.wait()

        await asyncio.wait_for(_wait(), timeout)
        # Anything else in flight for this task would arrive within a few loop turns.
        await asyncio.sleep(0.1)
        return self.terminal(uid)


_uids = itertools.count()


def make_task(kind, seconds, timeout=None):
    """A task dict, as Session passes it, of one kind that runs for ``seconds``."""
    specific = {} if timeout is None else {"timeout": timeout}
    task = {"uid": f"task.contract-{next(_uids):04d}", "name": kind}
    if kind == "executable":
        task.update(executable="sleep", arguments=[str(seconds)])
    else:
        task.update(function=sleep_for, args=[seconds], kwargs={})
    if kind == "function in a process":
        specific["process_template"] = {}
    elif kind == "job":
        task.update(function=None, executable="sleep", arguments=[str(seconds)])
        specific.update(process_templates=[(2, {})], pmi="none")
    task["task_backend_specific_kwargs"] = specific
    return task


KINDS = ["function", "executable", "function in a process", "job"]


@pytest.mark.parametrize("kind", KINDS)
async def test_timeout_ends_failed_with_timeout_error(contract_backend, kind):
    reports = Reports(contract_backend)
    task = make_task(kind, LONG, timeout=TIMEOUT)

    await contract_backend.submit_tasks([task])

    assert await reports.wait_terminal(task["uid"]) == ["FAILED"]
    assert isinstance(task.get("exception"), TimeoutError)


@pytest.mark.parametrize("kind", KINDS)
async def test_requested_cancel_ends_canceled(contract_backend, kind):
    reports = Reports(contract_backend)
    task = make_task(kind, LONG)
    await contract_backend.submit_tasks([task])

    assert await contract_backend.cancel_task(task["uid"]) is True

    assert await reports.wait_terminal(task["uid"]) == ["CANCELED"]
    assert not isinstance(task.get("exception"), TimeoutError)


async def test_timeout_of_a_process_that_catches_sigint_ends_failed_with_timeout_error(
    contract_backend,
):
    """A process that catches the signal stopping it at its timeout, and exits as a shell does,
    still timed out."""
    reports = Reports(contract_backend)
    task = make_task("executable", LONG, timeout=TIMEOUT)
    task["executable"] = "trap-sigint"
    await contract_backend.submit_tasks([task])

    assert await reports.wait_terminal(task["uid"]) == ["FAILED"]
    assert isinstance(task.get("exception"), TimeoutError)


async def test_requested_cancel_before_the_timeout_ends_canceled(contract_backend):
    """A task with a timeout cancelled before it expires is CANCELED, not timed out."""
    reports = Reports(contract_backend)
    task = make_task("executable", LONG, timeout=5.0)
    await contract_backend.submit_tasks([task])

    assert await contract_backend.cancel_task(task["uid"]) is True

    assert await reports.wait_terminal(task["uid"]) == ["CANCELED"]


async def test_cancel_of_an_unknown_uid_returns_false(contract_backend):
    reports = Reports(contract_backend)

    assert await contract_backend.cancel_task("task.no-such-uid") is False
    assert reports.states == {}


async def test_cancel_of_an_ended_task_returns_false(contract_backend):
    reports = Reports(contract_backend)
    task = make_task("function", 0.0)
    await contract_backend.submit_tasks([task])
    assert await reports.wait_terminal(task["uid"]) == ["DONE"]

    assert await contract_backend.cancel_task(task["uid"]) is False

    assert reports.terminal(task["uid"]) == ["DONE"]


async def test_failure_within_the_timeout_is_not_a_timeout(contract_backend):
    reports = Reports(contract_backend)
    task = make_task("executable", 0.0, timeout=5.0)
    task.update(executable="false", arguments=[])
    await contract_backend.submit_tasks([task])

    assert await reports.wait_terminal(task["uid"]) == ["FAILED"]
    assert task.get("exit_code") == 1
    assert not isinstance(task.get("exception"), TimeoutError)
