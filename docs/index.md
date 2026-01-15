# RHAPSODY

**Runtime for Heterogeneous Applications, Service Orchestration and Dynamism**

RHAPSODY is a high-performance runtime system designed for orchestrating complex, heterogeneous workflows that combine traditional HPC tasks with modern AI/ML inference services.

!!! note "Core Objective"
    RHAPSODY aims to provide a unified, asynchronous interface for managing dynamic task graphs across diverse computing infrastructures, from local workstations to large-scale HPC machines.

## Key Features

- **Heterogeneous Task Support**: Seamlessly mix `ComputeTask` (HPC/Binary) and `AITask` (Inference) in a single workflow.
- **Explicit Task Routing**: Manually route tasks to specific backends or let the scheduler handle it automatically.
- **Asynchronous API**: Built on Python's `asyncio`, allowing for high-throughput task submission and non-blocking state monitoring.
- **Extensible Backends**: Support for multiple execution environments including Dragon, Dask, RADICAL-Pilot, and local concurrent workers.
- **Preconfigured Clusters**: Directly utilize existing Dask clusters (GPU, HPC) with zero-config overhead.

## Quick Example

```python
import asyncio
from rhapsody import Session, ComputeTask
from rhapsody.backends import ConcurrentExecutionBackend

async def main():
    # 1. Initialize session with a backend
    backend = ConcurrentExecutionBackend()
    async with Session(backends=[backend]) as session:
        
        # 2. Define a simple task
        task = ComputeTask(executable="/bin/echo", arguments=["Hello, Rhapsody!"])
        
        # 3. Submit and wait
        await session.submit_tasks([task])
        result = await task
        
        print(f"Task finished with state: {task.state}")
        print(f"Output: {task.return_value}")

if __name__ == "__main__":
    asyncio.run(main())
```

## Getting Started

Ready to dive in? Check out our [Installation Guide](getting-started/installation.md) or jump straight into the [Quick Start](getting-started/quick-start.md).
