import asyncio
import logging

import rhapsody
from rhapsody.backends import DragonExecutionBackendV3


rhapsody.enable_logging(level=logging.DEBUG)

async def main():
    # Get a backend (concurrent backend by default)
    backend = await DragonExecutionBackendV3()

    # Define tasks (UIDs auto-generated!)
    tasks = [
        rhapsody.ComputeTask(
            executable="/bin/bash",
            arguments=["-c", "echo Hello from task 1 on $HOSTNAME"],
            shell=True
        ),
        rhapsody.ComputeTask(
            executable="/bin/bash",
            arguments=["-c", "echo Hello from task 2 on $HOSTNAME"],
            shell=True
        ),
    ]

    # Submit tasks
    await backend.submit_tasks(tasks)

    # Wait for all tasks to complete (no manual callback needed!)
    await backend.wait_tasks(tasks)

    # Access task results - tasks are updated in-place
    for task in tasks:
        print(f"Task {task.uid} in {task.state} state.")
        print(f"Output: {task.stdout.strip() or task.stderr}")

    # Cleanup
    await backend.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
