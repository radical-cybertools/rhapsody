"""Unit tests for ConcurrentExecutionBackend."""

from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from rhapsody import ComputeTask
from rhapsody.backends.execution.concurrent import ConcurrentExecutionBackend

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


@pytest.mark.asyncio
async def test_execute_function_regular_sync_function_in_process():
    """_execute_function runs a regular (non-async) function in a ProcessPoolExecutor."""
    cloudpickle = pytest.importorskip("cloudpickle", reason="cloudpickle not installed")

    backend = ConcurrentExecutionBackend(executor=ProcessPoolExecutor())

    def multiply(a, b):
        return a * b

    task = ComputeTask(function=multiply, args=[5, 6])
    result_task, state = await backend._execute_function(task)

    assert state == "DONE"
    assert result_task["return_value"] == 30


@pytest.mark.asyncio
async def test_execute_function_async_function_in_process():
    """_execute_function still works for async functions in a ProcessPoolExecutor (regression)."""
    cloudpickle = pytest.importorskip("cloudpickle", reason="cloudpickle not installed")

    backend = ConcurrentExecutionBackend(executor=ProcessPoolExecutor())

    async def async_add(a, b):
        return a + b

    task = ComputeTask(function=async_add, args=[10, 20])
    result_task, state = await backend._execute_function(task)

    assert state == "DONE"
    assert result_task["return_value"] == 30
