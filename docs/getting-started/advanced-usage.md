# Advanced Usage

RHAPSODY excels at orchestrating complex, multi-backend workloads. This guide covers explicit task routing and mixing AI inference with traditional compute.

## Multiple Execution Backends

You can register multiple backends in a single session and explicitly route tasks to them using the `backend` keyword.


!!! note "Running Command"
    Using Dragon backend requires starting the entire example with `dragon` instead of `python`


```python
import asyncio

import rhapsody
from rhapsody.api import Session
from rhapsody.api import ComputeTask
from rhapsody.backends import ConcurrentExecutionBackend, DragonExecutionBackendV3


async def compute_function():
    import platform
    print(f"Running on host: {platform.node()}")

async def multi_backend_demo():
    # Initialize two different backends
    be_local = ConcurrentExecutionBackend(name="local_cpu")
    be_remote = DragonExecutionBackendV3(name="hpc_gpu")
    await asyncio.gather(be_local, be_remote)


    async with Session(backends=[be_local, be_remote]) as session:
        # Route tasks explicitly by backend name
        task1 = ComputeTask(
            function=compute_function, backend=be_local.name)
        task2 = ComputeTask(
            function=compute_function, backend=be_remote.name)

        await session.submit_tasks([task1, task2])
        await asyncio.gather(task1, task2)

        print(f"Task 1 ran on: {task1.backend}")
        print(f"Task 2 ran on: {task2.backend}")


if __name__ == "__main__":
    asyncio.run(multi_backend_demo())
```


!!! success "Output"
    If everything is set up correctly, you should see:
    ```text
    Running on host: alienware
    Running on host: alienware
    Task 1 ran on: local_cpu
    Task 2 ran on: hpc_gpu
    +++ head proc exited, code 0

    real    0m5.729s
    user    0m9.789s
    sys     0m3.921s
    ```



!!! tip "Implicit Routing"
    If a task does not specify a `backend`, RHAPSODY will automatically route it to the first compatible backend registered in the session.



## HPC Workloads with Dragon

For large-scale HPC deployments, the Dragon backend provides native integration with the Dragon runtime, optimized for high-performance process management and communication.

```python
import asyncio
import rhapsody
from rhapsody.api import Session, ComputeTask
from rhapsody.backends import DragonExecutionBackendV3

async def dragon_demo():
    # Initialize the Dragon backend (optimized for HPC)
    # V3 uses native Dragon Batch for high-performance wait/callbacks
    dragon_be = await DragonExecutionBackendV3(
        num_workers=8,
        name="dragon_hpc"
    )

    async with Session(backends=[dragon_be]) as session:
        # Define tasks for the Dragon cluster
        tasks = [
            ComputeTask(
                executable="hostname", backend=dragon_be.name)
            for _ in range(16)
        ]

        await session.submit_tasks(tasks)
        results = await asyncio.gather(*tasks)

        for t in tasks:
            print(f"Task {t.uid} finished on Dragon")
if __name__ == "__main__":
    asyncio.run(dragon_demo())
```

!!! important "Dragon Environment"
    The Dragon backend requires a running Dragon runtime. Typically, you would start your script using the `dragon` launcher:
    ```bash
    dragon my_workflow.py
    ```

## Dragon Process Templates

When using `DragonExecutionBackendV3`, you can pass Dragon-native process configuration to `ComputeTask` via the `task_backend_specific_kwargs` parameter. This exposes the `process_template` and `process_templates` options, which map directly to Dragon's [ProcessTemplate](https://dragonhpc.github.io/dragon/doc/_build/html/ref/native/dragon.native.process.html#dragon.native.process.ProcessTemplate).

The `ProcessTemplate` accepts the following parameters:

| Parameter | Type | Description |
|-----------|------|-------------|
| `cwd` | `str`, optional | Current working directory, defaults to `"."` |
| `env` | `dict`, optional | Environment variables to pass to the process |
| `stdin` | `int`, optional | Standard input file handling (`PIPE`, `None`) |
| `stdout` | `int`, optional | Standard output file handling (`PIPE`, `STDOUT`, `None`) |
| `stderr` | `int`, optional | Standard error file handling (`PIPE`, `STDOUT`, `None`) |
| `policy` | `Policy`, optional | Determines the placement and resources of the process |
| `options` | `ProcessOptions`, optional | Process options, such as allowing the process to connect to the infrastructure |

!!! warning "Excluded Parameters"
    `DragonExecutionBackendV3` manages `target` (the binary or Python callable), `args`, and `kwargs` internally through the `ComputeTask` interface. These three parameters are **not** available in `process_template` / `process_templates` — use `ComputeTask.executable`, `ComputeTask.arguments`, and `ComputeTask.function` instead.

### Single-Process Template

Use `process_template` (singular) to apply a Dragon process configuration to a single task:

```python
# Single-process executable with a process template
ComputeTask(
    executable='/bin/bash',
    arguments=['-c', 'echo $HOSTNAME'],
    task_backend_specific_kwargs={
        "process_template": {}
    },
)

# Single-process function with a process template
ComputeTask(
    function=my_function,
    task_backend_specific_kwargs={
        "process_template": {}
    },
)
```

### Multi-Process (Parallel Job) Templates

Use `process_templates` (plural) to launch a task as a parallel job composed of multiple process groups. Each entry is a tuple of `(num_processes, template_dict)`:

```python
# Parallel job: 2 groups of 2 processes each (4 total)
ComputeTask(
    executable='/bin/bash',
    arguments=['-c', 'echo $HOSTNAME'],
    task_backend_specific_kwargs={
        "process_templates": [(2, {}), (2, {})]
    },
)

# Parallel function: 2 groups of 2 processes each
ComputeTask(
    function=parallel_function,
    task_backend_specific_kwargs={
        "process_templates": [(2, {}), (2, {})]
    },
)
```

### GPU Affinity with Policies

You can use Dragon's `Policy` to control process placement and GPU affinity. This is useful for pinning each worker to a specific GPU across multiple nodes in a round-robin fashion:

```python
import asyncio
from dragon.infrastructure.policy import Policy

import rhapsody
from rhapsody.api import ComputeTask, Session
from rhapsody.backends import DragonExecutionBackendV3


from dragon.native.machine import System, Node


def find_gpus():

    all_gpus = []
    # loop through all nodes Dragon is running on
    for huid in System().nodes:
        node = Node(huid)
        # loop through however many GPUs it may have
        for gpu_id in node.gpus:
            all_gpus.append((node.hostname, gpu_id))
    return all_gpus


def make_policies(all_gpus, nprocs=32):
    """Create per-process policies with round-robin GPU assignment."""
    policies = []
    i = 0
    for worker in range(nprocs):
        policies.append(
            Policy(
                placement=Policy.Placement.HOST_NAME,
                host_name=all_gpus[i][0],
                gpu_affinity=[all_gpus[i][1]],
            )
        )
        i += 1
        if i == len(all_gpus):
            i = 0
    return policies


async def main():
    backend = await DragonExecutionBackendV3()
    session = Session(backends=[backend])

    all_gpus = find_gpus()  # e.g. [("node-0", 0), ("node-0", 1), ("node-1", 0), ...]
    policies = make_policies(all_gpus, nprocs=4)

    async def gpu_work():
        import socket
        return socket.gethostname()

    # Single-process task pinned to a specific GPU
    task_single = ComputeTask(
        function=gpu_work,
        task_backend_specific_kwargs={
            "process_template": {"policy": policies[0]}
        },
    )

    # Parallel job: 2 processes each get their own GPU policy
    task_parallel = ComputeTask(
        function=gpu_work,
        task_backend_specific_kwargs={
            "process_templates": [
                (2, {"policy": policies[0]}),
                (2, {"policy": policies[1]}),
            ]
        },
    )

    async with session:
        await session.submit_tasks([task_single, task_parallel])
        await asyncio.gather(task_single, task_parallel)

        for t in [task_single, task_parallel]:
            print(f"Task {t.uid}: {t.state} (output: {t.return_value})")


if __name__ == "__main__":
    asyncio.run(main())
```

In the example above, `make_policies` assigns GPUs round-robin across all discovered nodes. Each policy is then passed into the `process_template` (or individual entries in `process_templates`) via the `policy` key, giving you fine-grained control over where each process runs and which GPU it uses.

### Full Heterogeneous Workload Example

The following example demonstrates mixing native functions, single-process tasks, and parallel jobs in a single session:

```python
import asyncio
import rhapsody
from rhapsody.api import ComputeTask, Session
from rhapsody.backends import DragonExecutionBackendV3


async def main():
    backend = await DragonExecutionBackendV3()
    session = Session(backends=[backend])

    async def single_function():
        import socket
        return socket.gethostname()

    async def parallel_function():
        import socket
        return socket.gethostname()

    async def native_function():
        import socket
        return socket.gethostname()

    tasks = [
        # Native function (no backend-specific config)
        ComputeTask(function=native_function),

        # Single-process executable
        ComputeTask(
            executable='/bin/bash',
            arguments=['-c', 'echo $HOSTNAME'],
            task_backend_specific_kwargs={
                "process_template": {}
            },
        ),

        # Parallel job executable (2+2 processes)
        ComputeTask(
            executable='/bin/bash',
            arguments=['-c', 'echo $HOSTNAME'],
            task_backend_specific_kwargs={
                "process_templates": [(2, {}), (2, {})]
            },
        ),

        # Single-process function
        ComputeTask(
            function=single_function,
            task_backend_specific_kwargs={
                "process_template": {}
            },
        ),

        # Parallel job function (2+2 processes)
        ComputeTask(
            function=parallel_function,
            task_backend_specific_kwargs={
                "process_templates": [(2, {}), (2, {})]
            },
        ),
    ]

    async with session:
        futures = await session.submit_tasks(tasks)
        await asyncio.gather(*futures)

        for t in tasks:
            print(
                f"Task {t.uid}: {t.state} "
                f"(output: {t.stdout.strip() if t.stdout else t.return_value})"
            )

if __name__ == "__main__":
    asyncio.run(main())
```

!!! note "Running Command"
    This example requires the Dragon runtime:
    ```bash
    dragon my_heterogeneous_workload.py
    ```

## Mixed HPC and AI Workloads

One of RHAPSODY's most powerful features is the ability to orchestrate traditional binaries alongside AI inference services (like vLLM).

```python
import asyncio
import rhapsody
from rhapsody.api import Session, ComputeTask, AITask
from rhapsody.backends import ConcurrentExecutionBackend
from rhapsody.backends.inference import DragonVllmInferenceBackend

async def mixed_workload():
    # 1. Setup Compute (HPC) and Inference (AI) backends
    hpc_backend = await ConcurrentExecutionBackend(name="hpc_cluster")
    ai_backend = await DragonVllmInferenceBackend(name="vllm_service", model="llama-3")

    async with Session(backends=[hpc_backend, ai_backend]) as session:

        # 2. Define a ComputeTask (e.g., simulation)
        sim_task = ComputeTask(executable="./simulate.sh", backend="hpc_cluster")

        # 3. Define an AITask (e.g., summarize results)
        summary_task = AITask(
            prompt="Summarize the simulation findings: ...",
            backend="vllm_service"
        )

        # 4. Submit both!
        await session.submit_tasks([sim_task, summary_task])

        # Wait for simulation to finish first
        await sim_task
        print(f"Simulation Done: {sim_task.return_value}")  # ComputeTask uses .return_value

        # Then summarize
        await summary_task
        print(f"AI Summary: {summary_task.response}")  # AITask uses .response

asyncio.run(mixed_workload())
```

## Using Dask Preconfigured clusters

As refactored recently, you can pass existing Dask clusters directly:

```python
from dask.distributed import LocalCluster
from rhapsody.backends.execution import DaskExecutionBackend

async def custom_cluster_demo():
    # Outside Rhapsody, you might have a specialized GPU cluster
    gpu_cluster = await LocalCluster(n_workers=4, threads_per_worker=1, asynchronous=True)

    # Pass it directly to the Dask backend
    backend = await DaskExecutionBackend(cluster=gpu_cluster)

    async with Session(backends=[backend]) as session:
        # Tasks will now run on your pre-allocated GPU workers
        ...
```
