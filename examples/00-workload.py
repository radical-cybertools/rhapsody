import asyncio
from rhapsody.backends import Session
from rhapsody.backends.execution.dask_parallel import DaskExecutionBackend


async def main():
    # Create a session

    session = Session()

    # Get a backend (concurrent backend by default)
    backend = await DaskExecutionBackend()

    # Set up a callback to track task results
    results = []
    def callback(task, state):
        results.append((task["uid"], state))

    backend.register_callback(callback)

    # Define tasks
    tasks = [
        {
            "uid": "task_1",
            "executable": "/bin/bash -c 'echo $HOSTNAME'"
        },
        {
            "uid": "task_2",
            "executable": "/bin/bash -c 'echo $HOSTNAME'"
        }
    ]

    # Submit and execute tasks
    await backend.submit_tasks(tasks)

    # Wait for all tasks to complete
    final_states = await backend.wait_tasks(results, len(tasks))

    # Optional: Check for failures
    failed_tasks = [uid for uid, state in final_states.items() if state == 'FAILED']
    if failed_tasks:
        print(f"Warning: {len(failed_tasks)} task(s) failed: {failed_tasks}")

    # Cleanup
    await backend.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
