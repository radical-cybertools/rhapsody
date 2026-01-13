
import asyncio
import os
import pytest
from rhapsody import ComputeTask
from rhapsody.api.session import Session, TaskStateManager
from rhapsody.backends import ConcurrentExecutionBackend

def test_session_class_import():
    """Test that Session class can be imported."""
    assert Session is not None

def test_session_instantiation():
    """Test that Session class can be instantiated."""
    session = Session()
    assert session is not None
    assert session.backends == []

def test_session_path_attribute():
    """Test that Session sets work_dir attribute to current working directory."""
    original_cwd = os.getcwd()
    session = Session()
    assert hasattr(session, "work_dir")
    assert session.work_dir == original_cwd

@pytest.mark.asyncio
async def test_session_submit_and_wait_flow():
    """Test complete flow: submit -> wait -> verify."""
    backend = await ConcurrentExecutionBackend()
    session = Session(backends=[backend])

    tasks = [
        ComputeTask(uid="task_1", executable="echo", arguments=["hello"]),
        ComputeTask(uid="task_2", executable="false"), # Will fail
    ]
    
    async with session:
        await session.submit_tasks(tasks)
        
        # Tasks should have state updated eventually
        await session.wait_tasks(tasks, timeout=5.0)
        
        # Verify states
        task1 = next(t for t in tasks if t.uid == "task_1")
        task2 = next(t for t in tasks if t.uid == "task_2")
        
        assert task1.state == "DONE"
        assert task2.state == "FAILED"

@pytest.mark.asyncio
async def test_session_wait_timeout():
    """Test that Session.wait_tasks respects timeout."""
    backend = await ConcurrentExecutionBackend()
    session = Session(backends=[backend])

    tasks = [
        ComputeTask(uid="slow_task", executable="sleep", arguments=["5"]),
    ]
    
    async with session:
        await session.submit_tasks(tasks)
        
        with pytest.raises(asyncio.TimeoutError):
            await session.wait_tasks(tasks, timeout=0.1)
            
        # Cancel to clean up
        await backend.cancel_task(tasks[0].uid)

@pytest.mark.asyncio
async def test_session_callbacks_registered():
    """Test that Session registers its state manager callback with backends."""
    backend = await ConcurrentExecutionBackend()
    session = Session(backends=[backend])
    
    # Check if callback is registered (implementation detail check)
    # The session registers self._state_manager.update_task
    assert backend._callback_func == session._state_manager.update_task
    await session.close()
