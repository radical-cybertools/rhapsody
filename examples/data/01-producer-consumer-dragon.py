"""Same producer/consumer exchange as 01-producer-consumer-dragon.py, but consumed with the native
dragon.data.ddict.DDict client instead of RADEX's typed client -- proving DragonDataBackend's
endpoint is usable by ANY Dragon DDict client, not just RADEX's.

Run with:
    dragon -s -- python3 03-producer-consumer-dragon-native.py
"""

import asyncio
import logging

import rhapsody

from rhapsody.api import ComputeTask
from rhapsody.api import Session
from rhapsody.backends import DragonExecutionBackend
from rhapsody.backends.data import DragonDataBackend

rhapsody.enable_logging(level=logging.INFO)


# NOTE: task functions below are each fully self-contained -- see
# 01-producer-consumer-dragon.py for the full rationale. The only
# difference from that file is the client library: the native
# dragon.data.ddict.DDict.attach() here instead of
# radex.clients.core.DragonClient.


def func1(descriptor):
    import numpy as np
    from dragon.data.ddict import DDict

    client = DDict.attach(descriptor, timeout=10)
    try:
        samples = np.arange(10, dtype=np.float64) ** 2  # [0, 1, 4, 9, ..., 81]
        client["samples"] = samples
        client["sample-count"] = len(samples)
        return len(samples)
    finally:
        client.detach()


def func2(descriptor):
    from dragon.data.ddict import DDict

    client = DDict.attach(descriptor, timeout=10)
    try:
        # DragonDataBackend always constructs its DDict with
        # wait_for_keys=True, so __getitem__ already blocks until the key
        # exists (or the attach timeout elapses) -- no manual poll loop
        # needed here, unlike the redis-py case in
        # 02-producer-consumer-redis-native.py.
        samples = client["samples"]
        count = client["sample-count"]
        return {"count": int(count), "sum": float(samples.sum()), "mean": float(samples.mean())}
    finally:
        client.detach()


async def main():
    session = Session()
    exec_backend = await DragonExecutionBackend()

    # RHAPSODY owns launching the Dragon DDict; the consumer/producer below
    # never know that -- they only ever see the serialized descriptor.
    data_backend = await DragonDataBackend(managers_per_node=1, n_nodes=1)

    session.add_backend(exec_backend)
    session.add_backend(data_backend)

    descriptor = data_backend.endpoints[0].serialize()

    # Define tasks (UIDs auto-generated!)
    tasks = [
        ComputeTask(function=func1, args=(descriptor,)),
        ComputeTask(function=func2, args=(descriptor,)),
    ]

    # Submit tasks
    futures = await session.submit_tasks(tasks)

    # Wait for all tasks to complete (no manual callback needed!)
    results = await asyncio.gather(*futures)

    # Access task results - tasks are updated in-place
    for task in tasks:
        print(f"Task {task.uid} in {task.state} state.")
        print(f"Output: {task.return_value}")

    # Cleanup -- shuts down both backends
    await session.close()


if __name__ == "__main__":
    asyncio.run(main())
