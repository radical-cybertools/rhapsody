# Princeton StellarAI — Scalable Execution

StellarAI is a Slurm cluster at Princeton University / PPPL with a `cpu` partition (96 cores and about 370 GB per node) and a `gpu` partition (96 cores, about 980 GB and 8x NVIDIA B200 per node). Nodes are connected over Ethernet and a bonded link, without a Cray PE or libfabric stack, so Dragon uses its TCP transport.

This guide covers large-scale, multi-node task execution on StellarAI via `DragonExecutionBackend`. For AI inference workloads using `DragonVllmInferenceBackend`, see [AI-HPC](stellarai-ai-hpc.md).

## Environment Setup

### 1. Allocate compute nodes

```bash
salloc -N 2 -p cpu -t 30 --exclusive --account=<your_account>
```

!!! note
    `--exclusive` gives Dragon the whole node. Without it, Dragon's `Batch` pool is sized from the CPUs the job may use; see [Partial-node allocations](index.md#backend-compatibility).

### 2. Create a virtual environment using the system Python

StellarAI has no Cray PE; use the system Python 3.12:

```bash
/usr/bin/python3.12 -m venv ~/envs/rhapsody
```

### 3. Activate the environment

```bash
source ~/envs/rhapsody/bin/activate
```

### 4. Install RHAPSODY with Dragon backend

```bash
pip install "rhapsody-py[dragon]"
```

### Run

Inside the allocation, launch with the TCP transport and a network prefix that matches StellarAI's interfaces:

```bash
dragon -t tcp --network-prefix '^(eno|bond)\w*$' <script.py>
```

!!! warning "`-t tcp` is required"
    Without `-t tcp`, a multi-node launch fails within seconds with `RuntimeError: Abnormal exit detected` from `dragon-backend`, before any of your code runs. Single-node runs work without it.

!!! note
    Dragon starts its backends through Slurm, so no `dragon-network-config` file or `-w ssh` launcher is needed. Dragon always skips the loopback interface, so `--network-prefix` only has to match `eno8303` and `bond0`.

---

## Example

This example spreads 192 Python-function tasks and 4 executable tasks across the 2 allocated nodes with `DragonExecutionBackend`, then reports how many tasks ran on each node. Worker count is determined automatically by Dragon from the allocation.

```python title="rhapsody-scalable.py"
import asyncio
import collections
import logging

import rhapsody
from rhapsody.api import ComputeTask, Session
from rhapsody.backends import DragonExecutionBackend

rhapsody.enable_logging(level=logging.INFO)


def work(i, n):
    import socket

    total = sum(k * k for k in range(n))  # a little CPU work
    return socket.gethostname(), i, total


async def main():
    ntasks = 192

    backend = await DragonExecutionBackend()
    session = Session(backends=[backend])

    tasks = [ComputeTask(function=work, args=[i, 200_000]) for i in range(ntasks)]
    tasks += [ComputeTask(executable="/usr/bin/hostname") for _ in range(4)]

    async with session:
        futures = await session.submit_tasks(tasks)
        print(f"Submitted {len(tasks)} tasks. Received {len(futures)} futures.", flush=True)

        await asyncio.gather(*futures)

        per_node = collections.Counter(t.return_value[0] for t in tasks[:ntasks])
        for node, count in sorted(per_node.items()):
            print(f"{node}: {count} function tasks", flush=True)

        for t in tasks[ntasks:]:
            print(f"Task {t.uid}: {t.state} (output: {t.stdout.strip() if t.stdout else 'N/A'})")


if __name__ == "__main__":
    asyncio.run(main())
```

Run with:

```bash
dragon -t tcp --network-prefix '^(eno|bond)\w*$' rhapsody-scalable.py
```

!!! success "Expected Output"
    ```text
    Submitted 196 tasks. Received 196 futures.
    sai-n01g1: 96 function tasks
    sai-n02g4: 96 function tasks
    Task task.000193: DONE (output: sai-n01g1)
    Task task.000194: DONE (output: sai-n02g4)
    ...
    ```
    Node names and the split between them depend on your allocation.

---

## Contributors

<div class="grid cards" markdown>

-   :fontawesome-solid-user-tie: **Aymen Alsaadi**

    ---

    RADICAL Lab, Rutgers University

    [:fontawesome-solid-arrow-up-right-from-square: Profile](https://radical.rutgers.edu/people/aymen-alsaadi)

</div>
