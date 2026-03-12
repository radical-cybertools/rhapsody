"""Example: DaskExecutionBackend with sync functions, async functions, and executables.

Runs a LocalCluster automatically. Pass cluster= or client= to target SLURM, Kubernetes,
or any other Dask-compatible cluster — no other code changes needed.
"""

import asyncio

from rhapsody.api import ComputeTask, Session
from rhapsody.backends import DaskExecutionBackend


def compute_square_sync(n):
    """Sync function — dispatched natively to Dask workers."""
    return {"input": n, "result": n * n}


async def compute_square_async(n):
    """Async function — wrapped transparently, name visible in Dask dashboard."""
    return {"input": n, "result": n * n}


async def main():
    async with DaskExecutionBackend() as backend:
        session = Session(backends=[backend])

        tasks = []
        tasks += [ComputeTask(function=compute_square_sync, args=(i,)) for i in range(10)]
        tasks += [ComputeTask(function=compute_square_async, args=(i,)) for i in range(10)]
        tasks += [ComputeTask(executable="/bin/echo", arguments=[f"task {i}"]) for i in range(10)]

        async with session:
            await session.submit_tasks(tasks)
            await session.wait_tasks(tasks)

    done = sum(1 for t in tasks if t.state == "DONE")
    failed = sum(1 for t in tasks if t.state == "FAILED")
    print(f"Done: {done}, Failed: {failed}")
    for t in tasks:
        result = t.return_value if t.return_value is not None else t.stdout.strip()
        print(f"  {t.uid} -> {result}")


if __name__ == "__main__":
    asyncio.run(main())
