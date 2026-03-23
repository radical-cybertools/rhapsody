"""Integration tests for the Flux execution backend.
These tests require a live Flux instance.
"""

import asyncio
import pytest

from rhapsody.backends.constants import BackendMainStates
from rhapsody.backends.constants import TasksMainStates
from rhapsody.backends.execution.flux import FluxExecutionBackend


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.usefixtures("skip_if_no_flux")
async def test_flux_backend_submit_and_callback():
    """Submit 1 task, assert callback fires with DONE state."""
    # Start the backend via context manager which will start FluxService
    backend = FluxExecutionBackend()
    await backend._async_init()

    # We need a futures queue to track callback states
    states = []
    done_event = asyncio.Event()

    def _callback(task: dict, state: str) -> None:
        states.append(state)
        if state in (TasksMainStates.DONE.value, TasksMainStates.FAILED.value, TasksMainStates.CANCELED.value):
            # Safe to set event to continue
            # If we call set from a different thread, we should use call_soon_threadsafe
            # But here _callback might be called from an event loop thread or not.
            # _handle_events runs in a daemon thread, so we must use get_running_loop() and call_soon_threadsafe.
            # The asyncio.Event is tied to the current loop context.
            pass

    # A better way to track is to just poll the list since the callback comes from a thread
    backend.register_callback(_callback)

    task = {
        "uid": "int_test_task_1",
        "executable": "/bin/echo",
        "arguments": ["hello world"],
        "timeout": 10.0
    }

    try:
        await backend.submit_tasks([task])
        
        # Wait up to 10 seconds for the task to finish
        for _ in range(100):
            if TasksMainStates.DONE.value in states or TasksMainStates.FAILED.value in states:
                break
            await asyncio.sleep(0.1)

        assert TasksMainStates.DONE.value in states
    finally:
        await backend.shutdown()


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.usefixtures("skip_if_no_flux")
async def test_flux_backend_cancel():
    """Submit 1 long-running task, cancel it, assert CANCELED callback."""
    backend = FluxExecutionBackend()
    await backend._async_init()

    states = []

    def _callback(task: dict, state: str) -> None:
        states.append(state)

    backend.register_callback(_callback)

    # Use a long-running sleep command so we can cancel it while it's running
    task = {
        "uid": "int_test_task_2",
        "executable": "/bin/sleep",
        "arguments": ["60"],
        "timeout": 70.0
    }

    try:
        await backend.submit_tasks([task])
        
        # Wait briefly for it to start
        for _ in range(20):
            if TasksMainStates.RUNNING.value in states:
                break
            await asyncio.sleep(0.1)

        # Cancel it
        success = await backend.cancel_task(task["uid"])
        assert success is True

        # Wait for cancellation callback
        for _ in range(50):
            if TasksMainStates.CANCELED.value in states:
                break
            await asyncio.sleep(0.1)

        assert TasksMainStates.CANCELED.value in states

    finally:
        await backend.shutdown()
