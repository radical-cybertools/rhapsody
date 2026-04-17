import asyncio
import logging

import rhapsody
from rhapsody.api import ComputeTask
from rhapsody.api import Session
from rhapsody.backends import DaskExecutionBackend
from rhapsody.backends import ConcurrentExecutionBackend
from rhapsody.backends import DragonExecutionBackendV3Client

from concurrent.futures import ProcessPoolExecutor
#rhapsody.enable_logging(level=logging.DEBUG)


def func_task():
    print("Hello from function task")


async def main():
    # DragonExecutionBackendV3Client launches Dragon in a worker subprocess via ZMQ.
    # This process stays clean — no Dragon mp patches — so ProcessPoolExecutor coexists.
    drag_b = await DragonExecutionBackendV3Client()
    conc_b = await ConcurrentExecutionBackend(ProcessPoolExecutor())
    dask_b = await DaskExecutionBackend()
    session = Session(backends=[drag_b, conc_b, dask_b])

    print("--- Submitting Tasks ---")
    tasks = [ComputeTask(function=func_task, backend=drag_b.name) for i in range(512)]
    tasks.extend(ComputeTask(function=func_task, backend=conc_b.name) for i in range(512))
    tasks.extend(ComputeTask(function=func_task, backend=dask_b.name) for i in range(512))
    
    async with session:
        futures = await session.submit_tasks(tasks)

        print(f"Submitted {len(tasks)} tasks. Received {len(futures)} futures.")

        await asyncio.gather(*futures)

        for t in tasks:
            print(f"Task {t.uid}: {t.state} on backend {t.backend}")


if __name__ == "__main__":
    asyncio.run(main())
