# Quick Start

This guide will help you run your first heterogeneous workflow using RHAPSODY in just a few minutes.

## Core Concepts

Before we start, keep these three key objects in mind:

1.  **Backend**: Where your tasks actually run (e.g., your local CPU, a Dask cluster, or an HPC queue).
2.  **Task**: A unit of work. `ComputeTask` for binaries/scripts, or `AITask` for inference.
3.  **Session**: The manager that connects backends and tasks together.

## Basic Workflow

Here is a complete script demonstrating a simple compute workflow.

```python
import asyncio
from rhapsody import Session, ComputeTask
from rhapsody.backends import ConcurrentExecutionBackend

async def run_example():
    # 1. Choose an execution backend (Concurrent uses local threads/processes)
    backend = ConcurrentExecutionBackend(name="local")
    
    # 2. Open a session
    async with Session(backends=[backend]) as session:
        
        # 3. Define tasks
        tasks = [
            ComputeTask(executable="/bin/echo", arguments=[f"Task {i}"])
            for i in range(5)
        ]
        
        # 4. Submit tasks to the session
        await session.submit_tasks(tasks)
        
        # 5. Wait for all tasks to complete
        results = await asyncio.gather(*tasks)
        
        # 6. Inspect results
        for t in tasks:
            print(f"{t.uid}: {t.state} (Output: {t.return_value})")

if __name__ == "__main__":
    asyncio.run(run_example())
```

!!! success "Output"
    If everything is set up correctly, you should see:
    ```text
    task.0000: DONE (Output: Task 0)
    task.0001: DONE (Output: Task 1)
    ...
    ```

## What's Next?

- Explore **[Advanced Usage](advanced-usage.md)** for multi-backend and AI mixed workloads.
- Check the **[Configuration Guide](configuration.md)** for backend-specific tuning.
