# Advanced Usage

RHAPSODY excels at orchestrating complex, multi-backend workloads. This guide covers explicit task routing and mixing AI inference with traditional compute.

## Multiple Execution Backends

You can register multiple backends in a single session and explicitly route tasks to them using the `backend` keyword.

```python
import asyncio
from rhapsody import Session, ComputeTask
from rhapsody.backends import ConcurrentExecutionBackend

async def multi_backend_demo():
    # Initialize two different backends
    be_local = ConcurrentExecutionBackend(name="local_cpu")
    be_remote = ConcurrentExecutionBackend(name="remote_sim") # Mocking remote
    
    async with Session(backends=[be_local, be_remote]) as session:
        # Route tasks explicitly by backend name
        task1 = ComputeTask(executable="hostname", backend="local_cpu")
        task2 = ComputeTask(executable="hostname", backend="remote_sim")
        
        await session.submit_tasks([task1, task2])
        await asyncio.gather(task1, task2)
        
        print(f"Task 1 ran on: {task1.backend}")
        print(f"Task 2 ran on: {task2.backend}")

asyncio.run(multi_backend_demo())
```

!!! tip "Implicit Routing"
    If a task does not specify a `backend`, RHAPSODY will automatically route it to the first compatible backend registered in the session.

## HPC Workloads with Dragon

For large-scale HPC deployments, the Dragon backend provides native integration with the Dragon runtime, optimized for high-performance process management and communication.

```python
import asyncio
from rhapsody import Session, ComputeTask
from rhapsody.backends import DragonExecutionBackendV3

async def dragon_demo():
    # Initialize the Dragon backend (optimized for HPC)
    # V3 uses native Dragon Batch for high-performance wait/callbacks
    dragon_be = DragonExecutionBackendV3(
        num_workers=8,
        name="dragon_hpc"
    )
    
    async with Session(backends=[dragon_be]) as session:
        # Define tasks for the Dragon cluster
        tasks = [
            ComputeTask(executable="hostname", backend=dragon_be.name)
            for _ in range(16)
        ]
        
        await session.submit_tasks(tasks)
        results = await asyncio.gather(*tasks)
        
        for t in tasks:
            print(f"Task {t.uid} finished on Dragon")

asyncio.run(dragon_demo())
```

!!! important "Dragon Environment"
    The Dragon backend requires a running Dragon runtime. Typically, you would start your script using the `dragon` launcher:
    ```bash
    dragon my_workflow.py
    ```

## Mixed HPC and AI Workloads

One of RHAPSODY's most powerful features is the ability to orchestrate traditional binaries alongside AI inference services (like vLLM).

```python
import asyncio
from rhapsody import Session, ComputeTask, AITask
from rhapsody.backends import ConcurrentExecutionBackend
from rhapsody.backends.inference import DragonVllmInferenceBackend

async def mixed_workload():
    # 1. Setup Compute (HPC) and Inference (AI) backends
    hpc_backend = ConcurrentExecutionBackend(name="hpc_cluster")
    ai_backend = DragonVllmInferenceBackend(name="vllm_service", model="llama-3")
    
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
        print(f"Simulation Done: {sim_task.return_value}")
        
        # Then summarize
        result = await summary_task
        print(f"AI Summary: {result}")

asyncio.run(mixed_workload())
```

!!! note "Dynamic Scaling"
    By utilizing preconfigured Dask clusters or Dragon backends, these mixed workloads can scale dynamically from a single node to thousands of cores/GPUs.

## Using Preconfigured clusters

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
