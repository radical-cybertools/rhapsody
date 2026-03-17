"""Unit tests for ConcurrentExecutionBackend."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from rhapsody import ComputeTask
from rhapsody.backends.execution.concurrent import ConcurrentExecutionBackend


# ---------------------------------------------------------------------------
# cwd tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_concurrent_execute_command_exec_respects_cwd():
    """_execute_command passes task-level cwd to create_subprocess_exec."""
    backend = ConcurrentExecutionBackend()

    task = ComputeTask(executable="/bin/pwd", cwd="/tmp")

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
async def test_concurrent_execute_command_bksp_cwd_overrides_task_cwd():
    """task_backend_specific_kwargs cwd takes priority over task-level cwd."""
    backend = ConcurrentExecutionBackend()

    task = ComputeTask(
        executable="/bin/pwd",
        cwd="/home",
        task_backend_specific_kwargs={"cwd": "/var"},
    )

    mock_process = MagicMock()
    mock_process.communicate = AsyncMock(return_value=(b"/var\n", b""))
    mock_process.returncode = 0

    with patch(
        "rhapsody.backends.execution.concurrent.asyncio.create_subprocess_exec",
        new_callable=AsyncMock,
        return_value=mock_process,
    ) as mock_exec:
        await backend._execute_command(task)

    kwargs = mock_exec.call_args[1]
    assert kwargs.get("cwd") == "/var"


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
