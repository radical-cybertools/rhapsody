import asyncio

import pytest
from dask.distributed import Client
from dask.distributed import LocalCluster

from rhapsody.api import ComputeTask
from rhapsody.api import Session
from rhapsody.backends.execution.dask_parallel import DaskExecutionBackend

# ---------------------------------------------------------------------------
# Cluster injection
# ---------------------------------------------------------------------------


async def test_dask_preconfigured_cluster():
    async with LocalCluster(n_workers=1, threads_per_worker=1, asynchronous=True) as cluster:
        backend = await DaskExecutionBackend(cluster=cluster)
        assert backend._client is not None
        assert backend._client.cluster is cluster
        await backend.shutdown()


async def test_dask_preconfigured_client():
    async with LocalCluster(n_workers=1, threads_per_worker=1, asynchronous=True) as cluster:
        async with Client(cluster, asynchronous=True) as client:
            backend = await DaskExecutionBackend(client=client)
            assert backend._client is client
            await backend.shutdown()


# ---------------------------------------------------------------------------
# Client/cluster ownership — shutdown() must not close resources it didn't create
# ---------------------------------------------------------------------------


async def test_dask_shutdown_does_not_close_external_client():
    """A caller-provided Client is left open after backend.shutdown()."""
    async with LocalCluster(n_workers=1, threads_per_worker=1, asynchronous=True) as cluster:
        async with Client(cluster, asynchronous=True) as client:
            backend = await DaskExecutionBackend(client=client)
            await backend.shutdown()

            assert client.status == "running"
            fut = client.submit(lambda: 1)
            assert await fut == 1


async def test_dask_shutdown_does_not_close_external_cluster():
    """A caller-provided Cluster stays usable after backend.shutdown() closes its own client."""
    async with LocalCluster(n_workers=1, threads_per_worker=1, asynchronous=True) as cluster:
        backend = await DaskExecutionBackend(cluster=cluster)
        await backend.shutdown()

        async with Client(cluster, asynchronous=True) as client:
            fut = client.submit(lambda: 2)
            assert await fut == 2


async def test_dask_shutdown_closes_owned_client():
    """A backend-created Client is closed by backend.shutdown()."""
    backend = await DaskExecutionBackend(resources={"n_workers": 1, "threads_per_worker": 1})
    owned_client = backend._client

    await backend.shutdown()

    assert backend._client is None
    assert owned_client.status != "running"


# ---------------------------------------------------------------------------
# End-to-end task execution
# ---------------------------------------------------------------------------


async def test_dask_sync_function_e2e():
    """Sync function tasks complete with correct return_value."""

    def square(n):
        return n * n

    async with DaskExecutionBackend(resources={"n_workers": 1, "threads_per_worker": 1}) as backend:
        session = Session(backends=[backend])
        tasks = [ComputeTask(function=square, args=(i,)) for i in range(5)]
        async with session:
            await session.submit_tasks(tasks)
            await session.wait_tasks(tasks)

    assert all(t.state == "DONE" for t in tasks)
    assert [t.return_value for t in tasks] == [i * i for i in range(5)]


async def test_dask_async_function_e2e():
    """Async function tasks complete with correct return_value."""

    async def double(n):
        await asyncio.sleep(0)
        return n * 2

    async with DaskExecutionBackend(resources={"n_workers": 1, "threads_per_worker": 1}) as backend:
        session = Session(backends=[backend])
        tasks = [ComputeTask(function=double, args=(i,)) for i in range(5)]
        async with session:
            await session.submit_tasks(tasks)
            await session.wait_tasks(tasks)

    assert all(t.state == "DONE" for t in tasks)
    assert [t.return_value for t in tasks] == [i * 2 for i in range(5)]


async def test_dask_executable_e2e():
    """Executable tasks complete with correct stdout and exit_code."""
    async with DaskExecutionBackend(resources={"n_workers": 1, "threads_per_worker": 1}) as backend:
        session = Session(backends=[backend])
        tasks = [ComputeTask(executable="/bin/echo", arguments=[f"hello {i}"]) for i in range(3)]
        async with session:
            await session.submit_tasks(tasks)
            await session.wait_tasks(tasks)

    assert all(t.state == "DONE" for t in tasks)
    assert all(t.exit_code == 0 for t in tasks)
    for i, t in enumerate(tasks):
        assert f"hello {i}" in t.stdout


# ---------------------------------------------------------------------------
# Resource scheduling — fail fast on unmet constraints
# ---------------------------------------------------------------------------


async def test_dask_unmet_resources_fails_immediately():
    """Tasks with unsatisfiable resource constraints must FAIL, not hang."""
    async with DaskExecutionBackend(resources={"n_workers": 1, "threads_per_worker": 1}) as backend:
        session = Session(backends=[backend])
        # LocalCluster workers have no GPU resources — should fail immediately
        tasks = [
            ComputeTask(
                function=lambda: None,
                task_backend_specific_kwargs={"resources": {"GPU": 1}},
            )
        ]
        async with session:
            await session.submit_tasks(tasks)
            await session.wait_tasks(tasks)

    assert tasks[0].state == "FAILED"
    assert tasks[0].exception is not None


async def test_dask_unmet_resources_executable_sets_stderr():
    """Executable tasks with unsatisfiable resources must set stderr and FAIL."""
    async with DaskExecutionBackend(resources={"n_workers": 1, "threads_per_worker": 1}) as backend:
        session = Session(backends=[backend])
        tasks = [
            ComputeTask(
                executable="/bin/echo",
                arguments=["hi"],
                task_backend_specific_kwargs={"resources": {"GPU": 1}},
            )
        ]
        async with session:
            await session.submit_tasks(tasks)
            await session.wait_tasks(tasks)

    assert tasks[0].state == "FAILED"
    assert tasks[0].stderr  # must contain the error message
    assert tasks[0].exit_code == 1


async def test_dask_satisfiable_resources_succeed():
    """Tasks with satisfiable resource constraints must succeed, not always fail.

    Regression test: the resource pre-check used to call `Client.scheduler_info()`,
    which for an asynchronous client always returns an empty `workers` mapping — so
    the check could never detect a *satisfiable* request, only ever "fail fast" on
    unsatisfiable ones. This exercises the case that used to be impossible to pass.
    """
    async with LocalCluster(
        n_workers=1, threads_per_worker=1, resources={"GPU": 1}, asynchronous=True
    ) as cluster:
        async with Client(cluster, asynchronous=True) as client:
            backend = await DaskExecutionBackend(client=client)
            session = Session(backends=[backend])
            tasks = [
                ComputeTask(
                    function=lambda: "ok",
                    task_backend_specific_kwargs={"resources": {"GPU": 1}},
                )
            ]
            async with session:
                await session.submit_tasks(tasks)
                await session.wait_tasks(tasks)

    assert tasks[0].state == "DONE"
    assert tasks[0].return_value == "ok"


async def test_dask_duplicate_function_args_do_not_share_a_future():
    """Two tasks calling the same function with the same args both complete correctly end-to-end.

    See `test_dask_duplicate_function_args_get_distinct_keys` in
    test_backend_execution_dask_parallel.py for the precise regression proof (that each
    task is submitted with its own distinct Dask key, rather than silently sharing one
    via `client.submit()`'s default `pure=True` tokenization).
    """

    def make_marker(n):
        return n

    async with DaskExecutionBackend(resources={"n_workers": 1, "threads_per_worker": 1}) as backend:
        session = Session(backends=[backend])
        tasks = [
            ComputeTask(function=make_marker, args=(1,)),
            ComputeTask(function=make_marker, args=(1,)),
        ]
        async with session:
            await session.submit_tasks(tasks)
            await session.wait_tasks(tasks)

    assert all(t.state == "DONE" for t in tasks)
    assert tasks[0].uid != tasks[1].uid
    assert [t.return_value for t in tasks] == [1, 1]


if __name__ == "__main__":
    asyncio.run(test_dask_preconfigured_cluster())
    asyncio.run(test_dask_preconfigured_client())
