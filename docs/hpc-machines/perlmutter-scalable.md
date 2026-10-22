# NERSC Perlmutter — Scalable Execution

[Perlmutter](https://docs.nersc.gov/systems/perlmutter/architecture/) is a Cray EX supercomputer at the National Energy Research Scientific Computing Center (NERSC), DOE. It features both GPU and CPU partitions, with GPU nodes equipped with 4x NVIDIA A100 40GB GPUs each. It runs a Cray PE software stack with Cray MPICH and Cray libfabric for high-speed interconnect.

This guide covers large-scale, multi-node task execution on Perlmutter via `DragonExecutionBackend`. For AI inference and fine-tuning workloads using `DragonVllmInferenceBackend`, see [AI-HPC](perlmutter-ai-hpc.md).

## Environment Setup

### 1. Allocate compute nodes

```bash
salloc -N 2 -t 30 -C cpu --account=<your_account>
```

### 2. Create a virtual environment using the system Python

Perlmutter provides a system Python through the Cray PE. Use it with `--system-site-packages` so that Cray-optimized libraries (libfabric, PMIx, `mpi4py`, etc.) are inherited inside the venv:

```bash
/opt/cray/pe/python/<version>/bin/python3 -m venv --system-site-packages ~/envs/cray_rhapsody
```

### 3. Load the required modules

```bash
module load cray-python/<version>
module load cray-mpich-abi/<version>
```

!!! note
    `cray-mpich-abi` provides the ABI-compatible MPI libraries required at runtime. Load it before activating the venv.

### 4. Activate the environment

```bash
source ~/envs/cray_rhapsody/bin/activate
```

### 5. Install RHAPSODY with Dragon backend

```bash
pip install "rhapsody-py[dragon]"
```

### 6. Configure Dragon for fast interconnect

This step is required for Dragon to use Cray's high-speed libfabric instead of the default transport:

```bash
dragon-config add --ofi-build-lib=/opt/cray/libfabric/<version>/lib64
dragon-config add --ofi-include=/opt/cray/libfabric/<version>/include
dragon-config add --ofi-runtime-lib=/opt/cray/libfabric/<version>/lib64
```

!!! warning
    This step is required on Perlmutter. Without it, Dragon will fail to initialize its transport layer across nodes.

### Run

```bash
dragon <script.py>
```

---

## Example

This example runs 32 MPI jobs concurrently using RHAPSODY's `Session` API with `DragonExecutionBackend`. Each job gets a random number of ranks (between 2 and 32), all scheduled across the 2 allocated nodes. Worker count is determined automatically by Dragon from the allocation.

```python title="mpi-rhapsody.py"
import asyncio
import logging
import random
import time

import rhapsody
from rhapsody.api import ComputeTask, Session
from rhapsody.backends import DragonExecutionBackend

rhapsody.enable_logging(level=logging.DEBUG)


def mpi_f(i, secs):
    import mpi4py
    mpi4py.rc.initialize = False

    from mpi4py import MPI

    MPI.Init()

    time.sleep(secs)

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    if rank == (size - 1):
        print(f"Job {i}: Rank {rank} of {size} says: Hello from MPI!", flush=True)


async def main():
    njobs = 32
    sleepsecs = 2
    maxranks = 32

    backend = await DragonExecutionBackend()
    session = Session(backends=[backend])

    print("--- Submitting Tasks ---")

    tasks = []
    for i in range(njobs):
        nranks = random.randint(2, maxranks)
        tasks.append(ComputeTask(
            function=mpi_f,
            args=[i, sleepsecs],
            task_backend_specific_kwargs={'process_templates': [(nranks, {})]}
        ))

    async with session:
        futures = await session.submit_tasks(tasks)
        print(f"Submitted {len(tasks)} tasks. Received {len(futures)} futures.")

        await asyncio.gather(*futures)

        for t in tasks:
            print(f"Task {t.uid}: {t.state} (output: {t.stdout.strip() if t.stdout else 'N/A'})")


if __name__ == "__main__":
    asyncio.run(main())
```

Run with:

```bash
dragon mpi-rhapsody.py
```

---

## Contributors

<div class="grid cards" markdown>

-   :fontawesome-solid-user-tie: **Aymen Alsaadi**

    ---

    RADICAL Lab, Rutgers University

    [:fontawesome-solid-arrow-up-right-from-square: Profile](https://radical.rutgers.edu/people/aymen-alsaadi)


</div>
