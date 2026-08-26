"""Example: DaskExecutionBackend against a Slurm cluster via dask_jobqueue.

Requires `dask_jobqueue` installed and a Slurm scheduler reachable from this host
(salloc/sbatch on $PATH). Update queue/account/cores/memory/walltime for your allocation.

SLURMCluster must be constructed with asynchronous=True (and entered via `async with`)
so it shares this script's event loop instead of spinning up its own background-thread
loop. Skipping that makes the Client we build on top of it (cluster=...) inherit a
mismatched loop: every await on a Future or on shutdown then silently falls back to
blocking-sync mode and breaks with confusing TypeError/AttributeError failures.
"""

import asyncio
import logging

import rhapsody
from dask_jobqueue import SLURMCluster
from rhapsody.api import ComputeTask
from rhapsody.api import Session
from rhapsody.backends.execution.dask_parallel import DaskExecutionBackend

rhapsody.enable_logging(level=logging.DEBUG)

logger = logging.getLogger(__name__)


def compute_task(n: int) -> int:
    return n * n


async def main():
    # tested on purdue anvil
    async with SLURMCluster(
        queue="wholenode",
        account="dmrxxx", # user must provide this
        cores=16,  # cores per Slurm job (worker)
        memory="16GB",
        walltime="00:30:00",
        # job_extra_directives=["--gres=gpu:1"],  # e.g. for GPU-constrained jobs
        asynchronous=True,
    ) as cluster:
        await cluster.scale(jobs=1)  # submit 1 Slurm job hosting one worker

        # cluster= (not client=) -> backend creates+owns the Client, not the cluster.
        backend = await DaskExecutionBackend(cluster=cluster)
        session = Session(backends=[backend])

        tasks = [ComputeTask(function=compute_task, args=(i,)) for i in range(10)]

        async with session:
            await session.submit_tasks(tasks)
            await session.wait_tasks(tasks)

        for t in tasks:
            print(t.uid, t.state, t.return_value)
    # cluster is closed automatically on exit from `async with` — you created it,
    # so it's yours to close, but the async form does it via await for you.


if __name__ == "__main__":
    asyncio.run(main())
