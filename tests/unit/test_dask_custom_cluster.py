import asyncio

import pytest
from dask.distributed import Client
from dask.distributed import LocalCluster

from rhapsody.backends.execution.dask_parallel import DaskExecutionBackend


async def test_dask_preconfigured_cluster():
    print("Testing Dask with preconfigured LocalCluster...")
    async with LocalCluster(n_workers=1, threads_per_worker=1, asynchronous=True) as cluster:
        backend = await DaskExecutionBackend(cluster=cluster)
        assert backend._client is not None
        assert backend._client.cluster is cluster
        print("Successfully initialized DaskExecutionBackend with preconfigured cluster!")
        await backend.shutdown()


async def test_dask_preconfigured_client():
    print("\nTesting Dask with preconfigured Client...")
    async with LocalCluster(n_workers=1, threads_per_worker=1, asynchronous=True) as cluster:
        async with Client(cluster, asynchronous=True) as client:
            backend = await DaskExecutionBackend(client=client)
            assert backend._client is client
            print("Successfully initialized DaskExecutionBackend with preconfigured client!")
            await backend.shutdown()


if __name__ == "__main__":
    asyncio.run(test_dask_preconfigured_cluster())
    asyncio.run(test_dask_preconfigured_client())
