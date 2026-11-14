"""ConcurrentExecutionBackend(observe_peaks=True): each task's usage as task["observed"]."""

import asyncio
import inspect
import os
import socket
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor

import pytest

from rhapsody.backends.execution.concurrent import ConcurrentExecutionBackend

MIB = 1 << 20
SLACK = 16 * MIB  # Linux >= 6.2 keeps RSS in per-CPU counters, which can read a few MiB low
LINUX = sys.platform.startswith("linux")


async def _run(backend, tasks, timeout=60.0):
    """Submit tasks; return {uid: (final state, task)}."""
    finals = {}
    done = asyncio.Event()

    def callback(task, state):
        if state in ("DONE", "FAILED", "CANCELED"):
            finals[task["uid"]] = (state, task)
            if len(finals) == len(tasks):
                done.set()

    backend.register_callback(callback)
    await backend.submit_tasks(tasks)
    await asyncio.wait_for(done.wait(), timeout)
    return finals


def _alloc(uid, mib, code=0, **extra):
    script = f"import sys; b = b'x' * ({mib} << 20); sys.exit({code})"
    return {"uid": uid, "executable": sys.executable, "arguments": ["-c", script], **extra}


async def _hold(mib):
    return len(b"x" * (mib * MIB))


@pytest.mark.asyncio
async def test_executables_report_peak_window_and_exit(tmp_path):
    backend = await ConcurrentExecutionBackend(ThreadPoolExecutor(2), observe_peaks=True)
    backend._work_dir = str(tmp_path)
    before = time.time()
    try:
        finals = await _run(
            backend,
            [
                _alloc("ok", 200),
                _alloc("bad", 100, code=3),
                {
                    "uid": "sh",
                    "executable": "exit",
                    "arguments": ["5"],
                    "task_backend_specific_kwargs": {"shell": True},
                },
            ],
        )
    finally:
        await backend.shutdown()

    state, task = finals["ok"]
    assert state == "DONE" and task["exit_code"] == 0
    observed = task["observed"]
    assert 200 * MIB - SLACK <= observed["host_mem"] * (1 << 30) <= 264 * MIB
    assert before <= observed["started"] <= observed["ended"] <= time.time()
    assert observed["host"] == socket.gethostname()
    assert observed["cpu_seconds"] >= 0
    state, task = finals["bad"]
    assert state == "FAILED" and task["exit_code"] == 3
    assert task["observed"]["host_mem"] * (1 << 30) >= 100 * MIB - SLACK
    state, task = finals["sh"]
    assert state == "FAILED" and task["exit_code"] == 5  # through /bin/sh -c
    assert task["observed"]["started"] <= task["observed"]["ended"]
    assert os.listdir(tmp_path) == []  # records read, roots removed


@pytest.mark.asyncio
async def test_functions_in_threads_report_times_only():
    backend = await ConcurrentExecutionBackend(ThreadPoolExecutor(2), observe_peaks=True)
    try:
        finals = await _run(backend, [{"uid": "f", "function": _hold, "args": [64]}])
    finally:
        await backend.shutdown()
    state, task = finals["f"]
    assert state == "DONE" and task["return_value"] == 64 * MIB
    assert task["observed"]["host_mem"] is None
    assert task["observed"]["started"] <= task["observed"]["ended"]
    assert task["observed"]["cpu_seconds"] >= 0


@pytest.mark.asyncio
async def test_functions_in_processes_report_times_and_linux_memory():
    pytest.importorskip("cloudpickle")
    backend = await ConcurrentExecutionBackend(ProcessPoolExecutor(1), observe_peaks=True)
    try:
        finals = await _run(backend, [{"uid": "f", "function": _hold, "args": [256]}])
    finally:
        await backend.shutdown()
    state, task = finals["f"]
    assert state == "DONE" and task["return_value"] == 256 * MIB
    observed = task["observed"]
    assert observed["started"] <= observed["ended"]
    assert observed["cpu_seconds"] >= 0
    if LINUX:
        assert observed["approximate"] is True
        assert observed["host_mem"] * (1 << 30) >= 256 * MIB - SLACK
    else:
        assert observed["host_mem"] is None


async def _spin_then_fail(seconds):
    end = time.thread_time() + seconds  # this thread's CPU time
    while time.thread_time() < end:
        pass
    raise ValueError("failed after spinning")


@pytest.mark.asyncio
@pytest.mark.parametrize("pool", ["threads", "processes"])
async def test_a_function_that_raises_still_reports_its_window(pool):
    if pool == "processes":
        pytest.importorskip("cloudpickle")
        executor = ProcessPoolExecutor(1)
    else:
        executor = ThreadPoolExecutor(1)
    backend = await ConcurrentExecutionBackend(executor, observe_peaks=True)
    try:
        finals = await _run(backend, [{"uid": "f", "function": _spin_then_fail, "args": [0.2]}])
    finally:
        await backend.shutdown()
    state, task = finals["f"]
    assert state == "FAILED" and isinstance(task["exception"], ValueError)
    observed = task["observed"]
    assert observed["ended"] - observed["started"] >= 0.2
    assert observed["cpu_seconds"] >= 0.2  # the worker process's, or the thread's
    assert observed["host"] == socket.gethostname()


@pytest.mark.asyncio
async def test_off_by_default_nothing_observed(tmp_path):
    backend = await ConcurrentExecutionBackend(ThreadPoolExecutor(2))
    backend._work_dir = str(tmp_path)
    try:
        finals = await _run(backend, [_alloc("x", 8), {"uid": "f", "function": _hold, "args": [1]}])
    finally:
        await backend.shutdown()
    assert all("observed" not in task for _, task in finals.values())
    assert os.listdir(tmp_path) == []


# ============================================================================
# Review findings: each test fails without its fix
# ============================================================================


def test_positional_arguments_keep_their_v070_meaning():
    params = inspect.signature(ConcurrentExecutionBackend.__init__).parameters
    positional = [n for n, p in params.items() if p.kind is p.POSITIONAL_OR_KEYWORD]
    assert positional == ["self", "executor", "name"]
    assert params["observe_peaks"].kind is inspect.Parameter.KEYWORD_ONLY
    params = inspect.signature(ConcurrentExecutionBackend.create).parameters
    assert params["observe_peaks"].kind is inspect.Parameter.KEYWORD_ONLY


@pytest.mark.asyncio
async def test_a_relative_work_dir_and_a_task_cwd_keep_the_records(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "taskdir").mkdir()
    backend = await ConcurrentExecutionBackend(ThreadPoolExecutor(1), observe_peaks=True)
    backend._work_dir = "work"
    try:
        task = _alloc("t", 8, task_backend_specific_kwargs={"cwd": str(tmp_path / "taskdir")})
        finals = await _run(backend, [task])
    finally:
        await backend.shutdown()
    state, task = finals["t"]
    assert state == "DONE" and task["observed"]["host_mem"] > 0
    assert not list((tmp_path / "taskdir").rglob("*.json"))


@pytest.mark.asyncio
async def test_the_task_environment_does_not_break_the_launcher(tmp_path):
    backend = await ConcurrentExecutionBackend(ThreadPoolExecutor(1), observe_peaks=True)
    backend._work_dir = str(tmp_path)
    env = {**os.environ, "PYTHONHOME": "/nonexistent"}
    try:
        task = {
            "uid": "e",
            "executable": "/bin/echo",
            "arguments": ["hi"],
            "task_backend_specific_kwargs": {"env": env},
        }
        finals = await _run(backend, [task])
    finally:
        await backend.shutdown()
    state, task = finals["e"]
    assert state == "DONE" and task["stdout"] == "hi\n" and "observed" in task


SHARED = ValueError("one stored error")


async def _raise_shared(seconds):
    await asyncio.sleep(seconds)
    raise SHARED


@pytest.mark.asyncio
async def test_tasks_raising_one_shared_error_keep_their_own_measurements():
    backend = await ConcurrentExecutionBackend(ThreadPoolExecutor(2), observe_peaks=True)
    try:
        finals = await _run(
            backend,
            [
                {"uid": "a", "function": _raise_shared, "args": [0.01]},
                {"uid": "b", "function": _raise_shared, "args": [0.2]},
            ],
        )
    finally:
        await backend.shutdown()
    (sa, a), (sb, b) = finals["a"], finals["b"]
    assert sa == sb == "FAILED" and a["exception"] is b["exception"] is SHARED
    assert vars(SHARED) == {}  # nothing written on the user's exception
    window = {t["uid"]: t["observed"]["ended"] - t["observed"]["started"] for t in (a, b)}
    assert window["a"] < 0.15 <= window["b"]
