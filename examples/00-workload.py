import asyncio
import rhapsody
import logging

from rhapsody.backends.execution.dragon import DragonExecutionBackendV3


rhapsody.enable_logging(level=logging.DEBUG)

async def main():
    # Get a backend (concurrent backend by default)
    backend = await DragonExecutionBackendV3()

    # Define tasks
    tasks = [
        {
            "uid": "task_1",
            "executable": "/bin/bash",
            "arguments": ["-c", "echo Hello from task 1 on $HOSTNAME"],
            "task_backend_specific_kwargs": {"shell": True}
        },
        {
            "uid": "task_2",
            "executable": "/bin/bash",
            "arguments": ["-c", "echo Hello from task 1 on $HOSTNAME"],
            "task_backend_specific_kwargs": {"shell": True}
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
