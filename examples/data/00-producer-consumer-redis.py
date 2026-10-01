"""Same producer/consumer exchange as 00-producer-consumer-redis.py, but consumed with plain redis-
py instead of RADEX's typed client -- proving RedisDataBackend's endpoint is usable by ANY Redis
client, not just RADEX's.

Requires: pip install redis
"""

import asyncio
import logging
import os
from concurrent.futures import ProcessPoolExecutor

import rhapsody

from rhapsody.api import ComputeTask
from rhapsody.api import Session
from rhapsody.backends import ConcurrentExecutionBackend
from rhapsody.backends.data import RedisDataBackend

rhapsody.enable_logging(level=logging.INFO)


# NOTE: task functions below are each fully self-contained -- see
# 00-producer-consumer-redis.py for the full rationale. The only difference
# from that file is the client library: plain redis-py here instead of
# radex.clients.core.RedisClient.


def func1(descriptor):
    import pickle

    import numpy as np
    import redis

    host, port = descriptor.split(":")
    client = redis.Redis(host=host, port=int(port))

    samples = np.arange(10, dtype=np.float64) ** 2  # [0, 1, 4, 9, ..., 81]
    client.set("samples", pickle.dumps(samples))
    client.set("sample-count", len(samples))
    return len(samples)


def func2(descriptor):
    import pickle
    import time

    import redis

    host, port = descriptor.split(":")
    client = redis.Redis(host=host, port=int(port))

    # redis-py has no wait_for_*/blocking-get primitive (that's a RADEX
    # client feature, see 00-producer-consumer-redis.py) -- a plain client
    # has to poll for key existence itself.
    deadline = time.monotonic() + 10
    while not (client.exists("samples") and client.exists("sample-count")):
        if time.monotonic() > deadline:
            raise TimeoutError("timed out waiting for producer")
        time.sleep(0.05)

    samples = pickle.loads(client.get("samples"))
    count = int(client.get("sample-count"))
    return {"count": count, "sum": float(samples.sum()), "mean": float(samples.mean())}


async def main():
    session = Session()
    exec_backend = await ConcurrentExecutionBackend(ProcessPoolExecutor())

    # RHAPSODY owns launching the Redis infrastructure; the consumer/producer
    # below never know that -- they only ever see the host:port endpoint.
    data_backend = await RedisDataBackend(work_dir=os.path.join(session.work_dir, session.uid))

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
