
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
    async def test_session_returns_futures(self):
        """Test that submit_tasks returns native futures."""
        backend = await ConcurrentExecutionBackend()
        session = Session(backends=[backend])
        task = ComputeTask(executable="echo", arguments=["hi"])
        
        async with session:
            futures = await session.submit_tasks([task])
            assert isinstance(futures, list)
            assert len(futures) == 1
            assert isinstance(futures[0], asyncio.Future)
            
            # Wait for completion via future
            result = await futures[0]
            assert result == task
            assert task.state == "DONE"

    @pytest.mark.asyncio
    async def test_session_direct_task_await(self):
        """Test that task objects can be awaited directly."""
        backend = await ConcurrentExecutionBackend()
        session = Session(backends=[backend])
        task = ComputeTask(executable="echo", arguments=["hi"])
        
        async with session:
            await session.submit_tasks([task])
            
            # Direct await on task
            result = await task
            assert result == task
            assert task.state == "DONE"

    @pytest.mark.asyncio
    async def test_session_gather_tasks(self):
        """Test that asyncio.gather works on tasks or futures."""
        backend = await ConcurrentExecutionBackend()
        session = Session(backends=[backend])
        tasks = [
            ComputeTask(uid=f"t{i}", executable="echo", arguments=[str(i)])
            for i in range(3)
        ]
        
        async with session:
            futures = await session.submit_tasks(tasks)
            
            # Gather futures
            results = await asyncio.gather(*futures)
            assert len(results) == 3
            assert all(t.state == "DONE" for t in tasks)
            
            # Reset tasks for another test
            tasks2 = [
                ComputeTask(uid=f"t2_{i}", executable="echo", arguments=[str(i)])
                for i in range(3)
            ]
            await session.submit_tasks(tasks2)
            
            # Gather tasks directly
            results2 = await asyncio.gather(*tasks2)
            assert len(results2) == 3
            assert all(t.state == "DONE" for t in tasks2)

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
