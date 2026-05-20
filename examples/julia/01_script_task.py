"""Demo (a): run a Julia script as a Rhapsody task.

Nothing Julia-specific on the Rhapsody side -- we just point a ComputeTask
at the `julia` executable and pass the script path as an argument.

Run:
    python examples/julia/01_script_task.py            # Concurrent
    dragon examples/julia/01_script_task.py dragon     # Dragon (CLI arg picks backend)
"""

import asyncio
import sys
import os

from rhapsody.api import ComputeTask, Session

HERE = os.path.dirname(os.path.abspath(__file__))


async def main():

    if len(sys.argv) > 1 and sys.argv[1] == "dragon":
        from rhapsody.backends import DragonExecutionBackendV3 as Backend
    else:
        from rhapsody.backends import ConcurrentExecutionBackend as Backend

    backend = await Backend()
    session = Session([backend])

    task = ComputeTask(
        executable="julia",
        arguments=[os.path.join(HERE, "hello.jl"), "world"],
    )

    await session.submit_tasks([task])
    await session.wait_tasks([task])

    print(f"Task {task.uid} -> {task.state}")
    print(f"stdout: {(task.stdout or '').strip()}")
    if task.stderr:
        print(f"stderr: {task.stderr.strip()}")

    await backend.shutdown()


if __name__ == "__main__":
    asyncio.run(main())

