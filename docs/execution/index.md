# Execution

RHAPSODY provides a seamless integration with high-performance workflow orchestration tools for execution.

## RADICAL AsyncFlow Integration

RHAPSODY integrates seamlessly with [RADICAL AsyncFlow](https://github.com/radical-cybertools/radical.asyncflow), a high-performance workflow engine for dynamic, asynchronous task graphs.

### Overview

RADICAL AsyncFlow provides:

- **Dynamic Task Graphs**: Create workflows with dependencies at runtime
- **Async/Await Syntax**: Natural Python async programming model
- **Decorator-Based API**: Simple function-to-task conversion
- **Backend Flexibility**: Use any RHAPSODY backend as execution engine

### Installation

Install RHAPSODY with AsyncFlow support:

```bash
pip install radical.asyncflow
pip install "rhapsody-py[dragon]"  # or your preferred backend
```

### Basic Usage

Here's a complete workflow example using AsyncFlow with RHAPSODY's Dragon backend:

```python
import asyncio
import multiprocessing as mp

from radical.asyncflow import WorkflowEngine
from rhapsody.backends import DragonExecutionBackend

async def main():
    # Set Dragon as the multiprocessing start method
    mp.set_start_method("dragon")

    # Initialize RHAPSODY backend
    backend = await DragonExecutionBackend()

    # Create AsyncFlow workflow engine with RHAPSODY backend
    flow = await WorkflowEngine.create(backend=backend)

    # Define tasks using decorators
    @flow.function_task
    async def task1(*args):
        """Data generation task"""
        print("Task 1: Generating data")
        data = list(range(1000))
        return sum(data)

    @flow.function_task
    async def task2(*args):
        """Data processing task"""
        input_data = args[0]
        print(f"Task 2: Processing data, input sum: {input_data}")
        return [x for x in range(1000) if x % 2 == 0]

    @flow.function_task
    async def task3(*args):
        """Data aggregation task"""
        sum_data, even_numbers = args
        print(f"Task 3: Aggregating results")
        return {
            "total_sum": sum_data,
            "even_count": len(even_numbers)
        }

    # Define workflow with dependencies
    async def run_workflow(wf_id):
        print(f"Starting workflow {wf_id}")

        # Create task graph: task3 depends on task1 and task2
        # task2 depends on task1
        t1 = task1()
        t2 = task2(t1)  # task2 waits for task1
        t3 = task3(t1, t2)  # task3 waits for both task1 and task2

        result = await t3  # Await final task
        print(f"Workflow {wf_id} completed, result: {result}")
        return result

    # Run multiple workflows concurrently
    results = await asyncio.gather(*[run_workflow(i) for i in range(10)])

    print(f"Completed {len(results)} workflows")

    # Shutdown the workflow engine
    await flow.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
```

!!! important "Running with Dragon"
    When using Dragon backend with AsyncFlow, launch with the `dragon` command:
    ```bash
    dragon -m workflow.py
    ```

### Key Features

#### 1. Automatic Dependency Management

AsyncFlow automatically tracks dependencies between tasks based on function arguments:

```python
@flow.function_task
async def step1():
    return "data"

@flow.function_task
async def step2(input_data):
    return f"processed_{input_data}"

# AsyncFlow automatically creates dependency: step2 waits for step1
result1 = step1()
result2 = step2(result1)
await result2
```

#### 2. Concurrent Workflow Execution

Run multiple independent workflows in parallel:

```python
# Each workflow has its own task graph
workflows = [run_workflow(i) for i in range(1000)]

# Execute all workflows concurrently
results = await asyncio.gather(*workflows)
```

#### 3. Backend Interoperability

AsyncFlow works with any RHAPSODY backend:

```python
# Local execution
from rhapsody.backends import ConcurrentExecutionBackend
backend = await ConcurrentExecutionBackend()

# Dask cluster
from rhapsody.backends import DaskExecutionBackend
backend = await DaskExecutionBackend()

# Dragon HPC
from rhapsody.backends import DragonExecutionBackend
backend = await DragonExecutionBackend()

# Create workflow with chosen backend
flow = await WorkflowEngine.create(backend=backend)
```

### Performance Considerations

!!! tip "Scaling Guidelines"
    - Use Dragon backend for HPC-scale workflows (1000+ concurrent tasks)
    - Use Dask backend for distributed cluster computing
    - Use Concurrent backend for local development and testing

!!! note "Task Granularity"
    - AsyncFlow excels at dynamic, fine-grained task graphs
    - For coarse-grained tasks, consider using RHAPSODY's Session API directly
    - All RHAPSODY API capabilities are exposed to AsyncFlow to launch workloads and workflows.


!!! warning "Dual API Usage"
    It is highly recommended not to combine RHAPSODY
    API with AsyncFlow API due to the possibility of
    `asyncio.loop` blocking.

For more information on specific backends, see the [Advanced Usage](../getting-started/advanced-usage.md) guide.
