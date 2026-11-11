"""Unit tests for ConcurrentExecutionBackend."""

import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from rhapsody import ComputeTask
from rhapsody.backends.execution.concurrent import ConcurrentExecutionBackend


@pytest.fixture(autouse=True)
def no_leftover_worker_processes():
    """Fail any test in this module that leaves worker processes running.

    When the interpreter exits, concurrent.futures waits for the workers of every process pool that
    was not shut down. If one of those workers is stuck, pytest prints its summary and then never
    exits.
    """
    before = set(multiprocessing.active_children())
    yield
    leftover = [p for p in multiprocessing.active_children() if p not in before]
    if leftover:
        pytest.fail(f"test left worker processes running: {leftover}")


# ---------------------------------------------------------------------------
# cwd tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_concurrent_execute_command_exec_respects_cwd():
    """_execute_command passes cwd from task_backend_specific_kwargs to create_subprocess_exec."""
    backend = ConcurrentExecutionBackend()

    task = ComputeTask(
        executable="/bin/pwd",
        task_backend_specific_kwargs={"cwd": "/tmp"},
    )

    mock_process = MagicMock()
    mock_process.communicate = AsyncMock(return_value=(b"/tmp\n", b""))
    mock_process.returncode = 0

    with patch(
        "rhapsody.backends.execution.concurrent.asyncio.create_subprocess_exec",
        new_callable=AsyncMock,
        return_value=mock_process,
    ) as mock_exec:
        result_task, state = await backend._execute_command(task)

    kwargs = mock_exec.call_args[1]
    assert kwargs.get("cwd") == "/tmp"
    assert state == "DONE"


@pytest.mark.asyncio
async def test_concurrent_execute_command_shell_respects_cwd_from_bksp():
    """_execute_command passes cwd from task_backend_specific_kwargs to create_subprocess_shell."""
    backend = ConcurrentExecutionBackend()

    task = ComputeTask(
        executable="pwd",
        task_backend_specific_kwargs={"shell": True, "cwd": "/tmp"},
    )

    mock_process = MagicMock()
    mock_process.communicate = AsyncMock(return_value=(b"/tmp\n", b""))
    mock_process.returncode = 0

    with patch(
        "rhapsody.backends.execution.concurrent.asyncio.create_subprocess_shell",
        new_callable=AsyncMock,
        return_value=mock_process,
    ) as mock_shell:
        result_task, state = await backend._execute_command(task)

    kwargs = mock_shell.call_args[1]
    assert kwargs.get("cwd") == "/tmp"
    assert state == "DONE"


@pytest.mark.asyncio
async def test_concurrent_execute_command_no_cwd():
    """_execute_command passes cwd=None when no cwd is set (no crash)."""
    backend = ConcurrentExecutionBackend()

    task = ComputeTask(executable="/bin/pwd")

    mock_process = MagicMock()
    mock_process.communicate = AsyncMock(return_value=(b"/some/dir\n", b""))
    mock_process.returncode = 0

    with patch(
        "rhapsody.backends.execution.concurrent.asyncio.create_subprocess_exec",
        new_callable=AsyncMock,
        return_value=mock_process,
    ) as mock_exec:
        await backend._execute_command(task)

    kwargs = mock_exec.call_args[1]
    assert kwargs.get("cwd") is None


# ---------------------------------------------------------------------------
# env tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_concurrent_execute_command_exec_respects_env():
    """_execute_command passes env from task_backend_specific_kwargs to create_subprocess_exec."""
    backend = ConcurrentExecutionBackend()

    task = ComputeTask(
        executable="/bin/printenv",
        task_backend_specific_kwargs={"env": {"MY_VAR": "hello"}},
    )

    mock_process = MagicMock()
    mock_process.communicate = AsyncMock(return_value=(b"hello\n", b""))
    mock_process.returncode = 0

    with patch(
        "rhapsody.backends.execution.concurrent.asyncio.create_subprocess_exec",
        new_callable=AsyncMock,
        return_value=mock_process,
    ) as mock_exec:
        result_task, state = await backend._execute_command(task)

    kwargs = mock_exec.call_args[1]
    assert kwargs.get("env") == {"MY_VAR": "hello"}
    assert state == "DONE"


@pytest.mark.asyncio
async def test_concurrent_execute_command_shell_respects_env():
    """_execute_command passes env from task_backend_specific_kwargs to create_subprocess_shell."""
    backend = ConcurrentExecutionBackend()

    task = ComputeTask(
        executable="printenv MY_VAR",
        task_backend_specific_kwargs={"shell": True, "env": {"MY_VAR": "world"}},
    )

    mock_process = MagicMock()
    mock_process.communicate = AsyncMock(return_value=(b"world\n", b""))
    mock_process.returncode = 0

    with patch(
        "rhapsody.backends.execution.concurrent.asyncio.create_subprocess_shell",
        new_callable=AsyncMock,
        return_value=mock_process,
    ) as mock_shell:
        result_task, state = await backend._execute_command(task)

    kwargs = mock_shell.call_args[1]
    assert kwargs.get("env") == {"MY_VAR": "world"}
    assert state == "DONE"


@pytest.mark.asyncio
async def test_concurrent_execute_command_no_env():
    """_execute_command passes env=None when no env is set (inherits parent process)."""
    backend = ConcurrentExecutionBackend()

    task = ComputeTask(executable="/bin/pwd")

    mock_process = MagicMock()
    mock_process.communicate = AsyncMock(return_value=(b"/some/dir\n", b""))
    mock_process.returncode = 0

    with patch(
        "rhapsody.backends.execution.concurrent.asyncio.create_subprocess_exec",
        new_callable=AsyncMock,
        return_value=mock_process,
    ) as mock_exec:
        await backend._execute_command(task)

    kwargs = mock_exec.call_args[1]
    assert kwargs.get("env") is None


# ---------------------------------------------------------------------------
# Regular function execution tests (Bug 2)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_execute_function_regular_sync_function_in_thread():
    """_execute_function runs a regular (non-async) function in a ThreadPoolExecutor."""
    backend = ConcurrentExecutionBackend(executor=ThreadPoolExecutor())

    def add(a, b):
        return a + b

    task = ComputeTask(function=add, args=[3, 4])
    result_task, state = await backend._execute_function(task)

    assert state == "DONE"
    assert result_task["return_value"] == 7


@pytest.mark.asyncio
async def test_execute_function_async_function_in_thread():
    """_execute_function still works for async functions in a ThreadPoolExecutor (regression)."""
    backend = ConcurrentExecutionBackend(executor=ThreadPoolExecutor())

    async def async_multiply(a, b):
        return a * b

    task = ComputeTask(function=async_multiply, args=[3, 4])
    result_task, state = await backend._execute_function(task)

    assert state == "DONE"
    assert result_task["return_value"] == 12


@pytest.fixture
async def process_backend():
    """A ConcurrentExecutionBackend on a one-worker process pool, shut down after the test.

    The worker is started with "spawn". On Linux before Python 3.14 the default is "fork", and
    forking the multi-threaded pytest process can leave the child deadlocked.
    """
    pytest.importorskip("cloudpickle", reason="cloudpickle not installed")

    executor = ProcessPoolExecutor(max_workers=1, mp_context=multiprocessing.get_context("spawn"))
    backend = ConcurrentExecutionBackend(executor=executor)
    yield backend
    await backend.shutdown()


@pytest.mark.asyncio
async def test_execute_function_regular_sync_function_in_process(process_backend):
    """_execute_function runs a regular (non-async) function in a ProcessPoolExecutor."""

    def multiply(a, b):
        return a * b

    task = ComputeTask(function=multiply, args=[5, 6])
    result_task, state = await process_backend._execute_function(task)

    assert state == "DONE"
    assert result_task["return_value"] == 30


@pytest.mark.asyncio
async def test_execute_function_async_function_in_process(process_backend):
    """_execute_function still works for async functions in a ProcessPoolExecutor (regression)."""

    async def async_add(a, b):
        return a + b

    task = ComputeTask(function=async_add, args=[10, 20])
    result_task, state = await process_backend._execute_function(task)

    assert state == "DONE"
    assert result_task["return_value"] == 30


# ---------------------------------------------------------------------------
# capture_stdio tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_capture_stdio_writes_files(tmp_path):
    """capture_stdio=True writes stdout/stderr to files and stores paths in task."""
    backend = ConcurrentExecutionBackend()
    backend._work_dir = str(tmp_path)

    task = ComputeTask(
        executable="/bin/bash",
        arguments=["-c", "echo hello; echo err >&2"],
        capture_stdio=True,
    )

    result_task, state = await backend._execute_command(task)

    assert state == "DONE"
    stdout_path = result_task["stdout"]
    stderr_path = result_task["stderr"]
    assert stdout_path.endswith(".stdout")
    assert stderr_path.endswith(".stderr")
    assert open(stdout_path).read() == "hello\n"
    assert open(stderr_path).read() == "err\n"


@pytest.mark.asyncio
async def test_capture_stdio_false_returns_strings(tmp_path):
    """Without capture_stdio, stdout/stderr are decoded strings (default behaviour)."""
    backend = ConcurrentExecutionBackend()
    backend._work_dir = str(tmp_path)

    task = ComputeTask(executable="/bin/echo", arguments=["world"])

    mock_process = MagicMock()
    mock_process.communicate = AsyncMock(return_value=(b"world\n", b""))
    mock_process.returncode = 0

    with patch(
        "rhapsody.backends.execution.concurrent.asyncio.create_subprocess_exec",
        new_callable=AsyncMock,
        return_value=mock_process,
    ):
        result_task, state = await backend._execute_command(task)

    assert state == "DONE"
    assert result_task["stdout"] == "world\n"
    assert result_task["stderr"] == ""


@pytest.mark.asyncio
async def test_capture_stdio_nonzero_exit_returns_failed(tmp_path):
    """capture_stdio=True with a failing command returns FAILED state."""
    backend = ConcurrentExecutionBackend()
    backend._work_dir = str(tmp_path)

    task = ComputeTask(
        executable="/bin/bash",
        arguments=["-c", "exit 1"],
        capture_stdio=True,
    )

    result_task, state = await backend._execute_command(task)

    assert state == "FAILED"
    assert result_task["exit_code"] == 1


# ---------------------------------------------------------------------------
# result_contract tests — stdout/stderr must be str after any DONE/FAILED
# ---------------------------------------------------------------------------


@pytest.mark.result_contract
@pytest.mark.asyncio
async def test_concurrent_exception_path_stdout_is_empty_string():
    """When _execute_task catches an exception, stdout must be '' not None."""
    backend = ConcurrentExecutionBackend()

    async def boom():
        raise RuntimeError("intentional")

    task = ComputeTask(function=boom, args=[])
    result_task, state = await backend._execute_task(task)

    assert state == "FAILED"
    assert isinstance(result_task["stdout"], str)


@pytest.mark.result_contract
@pytest.mark.asyncio
async def test_concurrent_command_empty_output_stdout_is_string():
    """_execute_command with empty stdout/stderr must write '' not None."""
    backend = ConcurrentExecutionBackend()
    task = ComputeTask(executable="/bin/true")

    mock_process = MagicMock()
    mock_process.communicate = AsyncMock(return_value=(b"", b""))
    mock_process.returncode = 0

    with patch(
        "rhapsody.backends.execution.concurrent.asyncio.create_subprocess_exec",
        new_callable=AsyncMock,
        return_value=mock_process,
    ):
        result_task, state = await backend._execute_command(task)

    assert state == "DONE"
    assert isinstance(result_task["stdout"], str)
    assert isinstance(result_task["stderr"], str)
