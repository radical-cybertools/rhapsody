import asyncio
import rhapsody
from rhapsody.backends import Session

async def main():
    # Create a session
    session = Session()

    # Get a backend (concurrent backend by default)
    backend = rhapsody.get_backend("concurrent")

    # Set up a callback to track task results
    results = []
    def callback(task, state):
        results.append((task["uid"], state))

    backend.register_callback(callback)

    # Define tasks
    tasks = [
        {
            "uid": "task_1",
            "executable": "echo",
            "arguments": ["Hello, RHAPSODY!"]
        },
        {
            "uid": "task_2",
            "executable": "python",
            "arguments": ["-c", "print('Task 2 complete')"]
        }
    ]

    # Submit and execute tasks
    task_futures = await backend.submit_tasks(tasks)

    # Wait for all tasks to complete
    await asyncio.gather(*task_futures)

    # Check results
    for task_uid, state in results:
        print(f"Task {task_uid}: {state}")

    # Cleanup
    await backend.shutdown()

if __name__ == "__main__":
    asyncio.run(main())

