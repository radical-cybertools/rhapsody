import asyncio
from rhapsody.backends import Session
from rhapsody.backends.execution.concurrent import ConcurrentExecutionBackend


async def main():
    # Create a session

    session = Session()

    # Get a backend (concurrent backend by default)
    backend = await ConcurrentExecutionBackend()

    # Define tasks
    tasks = [
        {
            "uid": "task_1",
            "executable": "echo",
            "arguments": ["Hello from task 1 - $HOSTNAME"],
            "task_backend_specific_kwargs": {"shell": True}
        },
        {
            "uid": "task_2",
            "executable": "/bin/echo",
            "arguments": ["Hello", "from", "task", "2"]
        }
    ]

    # Submit tasks
    await backend.submit_tasks(tasks)

    # Wait for all tasks to complete (no manual callback needed!)
    completed_tasks = await backend.wait_tasks(tasks)

    # Access task results
    for uid, task in completed_tasks.items():
        print(f"Task {uid}:")
        print(f"State: {task['state']}")
        if task['state'] == 'DONE':
            print(f"Output: {task.get('stdout', '').strip()}")
        else:
            print(f"Output: {task.get('stderr', '').strip()}")

    # Cleanup
    await backend.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
