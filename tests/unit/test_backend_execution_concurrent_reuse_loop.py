"""ConcurrentExecutionBackend(reuse_event_loop=True): one event loop per worker thread.

By default an async function task on a thread pool runs with ``asyncio.run``, which creates
and closes an event loop per task. Its clean-up calls ``asyncio.all_tasks()``, which walks
every asyncio Task of the process, so a task costs more the more Tasks are alive. With
``reuse_event_loop=True`` each worker thread keeps one loop, and the backend does the
clean-up that needs no such walk: it cancels the tasks a call left running.
"""

import asyncio
import multiprocessing
import threading
import time
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor

import pytest

from rhapsody import ComputeTask
from rhapsody.backends.execution.concurrent import ConcurrentExecutionBackend


async def _loop_id():
    return id(asyncio.get_running_loop())


async def _run(backend, func, *args):
    result, state = await backend._execute_function(ComputeTask(function=func, args=list(args)))
    assert state == "DONE", result
    return result["return_value"]


def _backend(**kwargs):
    return ConcurrentExecutionBackend(executor=ThreadPoolExecutor(max_workers=1), **kwargs)


def _loop_recorder():
    """A task that records its running loop, and the list it records into.

    The list keeps every loop alive, so loops compare by identity: comparing their ``id()`` would
    not do, as CPython gives a freed loop's id to the next object.
    """
    seen = []

    async def record_loop():
        seen.append(asyncio.get_running_loop())
        return len(seen)

    return seen, record_loop


async def test_default_runs_each_task_on_a_new_loop():
    seen, record_loop = _loop_recorder()
    backend = _backend()
    try:
        await _run(backend, record_loop)
        await _run(backend, record_loop)
        assert len(seen) == 2
        assert seen[0] is not seen[1]  # asyncio.run: a loop per task
    finally:
        await backend.shutdown()


async def test_reuse_runs_a_thread_s_tasks_on_one_loop():
    seen, record_loop = _loop_recorder()
    backend = _backend(reuse_event_loop=True)
    try:
        await _run(backend, record_loop)
        await _run(backend, record_loop)
        assert len(seen) == 2
        assert seen[0] is seen[1]  # one worker thread, one loop
    finally:
        await backend.shutdown()


async def test_reuse_leaves_sync_functions_without_a_loop():
    def running_loop():
        try:
            asyncio.get_running_loop()
            return True
        except RuntimeError:
            return False

    backend = _backend(reuse_event_loop=True)
    try:
        await _run(backend, _loop_id)
        assert await _run(backend, running_loop) is False
    finally:
        await backend.shutdown()


async def test_reuse_cancels_what_a_task_left_running():
    cancelled = threading.Event()

    async def leaves_a_task():
        async def forever():
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                cancelled.set()
                raise

        asyncio.get_running_loop().create_task(forever())
        await asyncio.sleep(0)
        return "done"

    backend = _backend(reuse_event_loop=True)
    try:
        assert await _run(backend, leaves_a_task) == "done"
        assert cancelled.is_set()
    finally:
        await backend.shutdown()


async def test_reuse_reports_an_error_a_left_task_raised():
    seen = []

    async def leaves_a_failing_task():
        loop = asyncio.get_running_loop()
        loop.set_exception_handler(lambda _loop, ctx: seen.append(ctx.get("exception")))

        async def fails_on_cancel():
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                raise ValueError("raised while cancelled") from None

        loop.create_task(fails_on_cancel())
        await asyncio.sleep(0)
        return "done"

    backend = _backend(reuse_event_loop=True)
    try:
        assert await _run(backend, leaves_a_failing_task) == "done"
        assert any(isinstance(e, ValueError) for e in seen)
    finally:
        await backend.shutdown()


async def test_reuse_survives_a_task_that_stops_its_loop():
    async def stops_its_loop():
        loop = asyncio.get_running_loop()
        loop.call_soon(loop.stop)
        await asyncio.sleep(0.01)
        return "unreached"

    backend = _backend(reuse_event_loop=True)
    try:
        result, state = await backend._execute_function(ComputeTask(function=stops_its_loop))
        assert state == "FAILED" or result.get("exception") is not None
    except RuntimeError:
        pass
    try:
        assert isinstance(await _run(backend, _loop_id), int)
    finally:
        await backend.shutdown()


async def test_shutdown_closes_the_threads_loops():
    backend = _backend(reuse_event_loop=True)
    await _run(backend, _loop_id)
    loops = backend._thread_loops.loops()
    assert loops and not any(lp.is_closed() for lp in loops)
    await backend.shutdown()
    assert all(lp.is_closed() for lp in loops)


async def test_reuse_with_processes_keeps_asyncio_run():
    pytest.importorskip("cloudpickle", reason="cloudpickle not installed")
    executor = ProcessPoolExecutor(max_workers=1, mp_context=multiprocessing.get_context("spawn"))
    backend = ConcurrentExecutionBackend(executor=executor, reuse_event_loop=True)
    try:
        assert isinstance(await _run(backend, _loop_id), int)
        assert backend._thread_loops is None
    finally:
        await backend.shutdown()


async def test_reuse_is_not_slowed_by_live_tasks():
    """With many Tasks alive in the process, a worker's call on its own loop costs about what it
    costs without them; ``asyncio.run`` walks them all."""

    async def noop():
        return None

    def timed(run, n=200):
        t = time.perf_counter()
        for _ in range(n):
            run(noop, (), {})
        return time.perf_counter() - t

    stop = asyncio.Event()
    holders = [asyncio.ensure_future(stop.wait()) for _ in range(20_000)]
    backend = _backend(reuse_event_loop=True)
    try:
        await asyncio.sleep(0)
        loop = asyncio.get_running_loop()
        per_task = await loop.run_in_executor(
            backend.executor, timed, ConcurrentExecutionBackend._run_in_thread
        )
        reused = await loop.run_in_executor(backend.executor, timed, backend._run_on_thread_loop)
        assert reused < per_task / 3, (reused, per_task)
    finally:
        stop.set()
        await asyncio.gather(*holders)
        await backend.shutdown()
