import asyncio
import logging

import rhapsody
from rhapsody.api import ComputeTask, Session
from rhapsody.backends import DragonExecutionBackendV3

rhapsody.enable_logging(level=logging.DEBUG)


async def main():
    # Initialize backend
    backend = await DragonExecutionBackendV3()
    session = Session(backends=[backend])

    async def single_function():
        import socket
        return socket.gethostname()

    async def parallel_function():
        import socket
        return socket.gethostname()

    async def native_function():
        import socket
        return socket.gethostname()

    tasks = [

        # Native function (no backend-specific config)
        ComputeTask(
            function=native_function,
        ),

        # Single-process executable
        ComputeTask(
            executable='/bin/bash',
            arguments=['-c', 'echo $HOSTNAME'],
            task_backend_specific_kwargs={
                "process_template": {}
            },
        ),

        # Parallel Job executable (2+2 processes)
        ComputeTask(
            executable='/bin/bash',
            arguments=['-c', 'echo $HOSTNAME'],
            task_backend_specific_kwargs={
                "process_templates": [(2, {}), (2, {})]
            },
        ),

        # Single-process function
        ComputeTask(
            function=single_function,
            task_backend_specific_kwargs={
                "process_template": {}
            },
        ),

        # Parallel Job function (2+2 processes)
        ComputeTask(
            function=parallel_function,
            task_backend_specific_kwargs={
                "process_templates": [(2, {}), (2, {})]
            },
        )
    ]

    async with session:
        # returns futures
        futures = await session.submit_tasks(tasks)

        print(f"Submitted {len(tasks)} tasks. Received {len(futures)} futures.")

        await asyncio.gather(*futures)  # or tasks both works

        for t in tasks:
            print(f"Task {t.uid}: {t.state} (output: {t.stdout.strip() if t.stdout else t.return_value})")

if __name__ == "__main__":
    asyncio.run(main())
