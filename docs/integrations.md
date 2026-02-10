# Integrations

RHAPSODY provides seamless integrations with several high-performance computing and AI infrastructure tools. This guide covers the available integrations and how to use them effectively.

## Dragon-VLLM Inference Backend

The Dragon-VLLM integration provides a high-performance inference backend that combines Dragon's distributed computing capabilities with vLLM's efficient LLM serving.

### Overview

The `DragonVllmInferenceBackend` offers:

- **Request Batching**: Automatically accumulates individual requests into efficient batches
- **Server Mode**: Optional HTTP server with OpenAI-compatible API endpoints
- **Engine Mode**: Direct Python API for programmatic access
- **Async Operations**: Non-blocking inference with asyncio integration
- **Multi-Node Support**: Scale inference across multiple GPU nodes

### Installation

Install RHAPSODY with Dragon-VLLM support:

```bash
pip install "rhapsody[vllm-dragon]"
```

!!! warning "Prerequisites"
    The Dragon-VLLM backend requires:

    - Dragon runtime (`dragonhpc`)
    - Dragon-VLLM package (`dragon-vllm`)
    - Python >= 3.10
    - GPU nodes with CUDA support

### Basic Usage

Here's a complete example of using the Dragon-VLLM backend for AI inference:

```python
import asyncio
import multiprocessing as mp

from rhapsody import Session
from rhapsody.api import AITask, ComputeTask
from rhapsody.backends import DragonExecutionBackendV3, DragonVllmInferenceBackend

async def main():
    # Set Dragon as the multiprocessing start method
    mp.set_start_method("dragon")

    # Initialize execution backend for compute tasks
    execution_backend = await DragonExecutionBackendV3(num_workers=4)

    # Initialize inference backend for AI tasks
    inference_backend = DragonVllmInferenceBackend(
        config_file="config.yaml",
        model_name="Qwen2.5-0.5B-Instruct",
        num_nodes=1,
        num_gpus=1,
        tp_size=1,
        port=8001,
        offset=0,
    )

    # Initialize the inference service
    await inference_backend.initialize()

    # Create a session with both backends
    session = Session([execution_backend, inference_backend])

    # Define mixed workload: AI and compute tasks
    tasks = [
        AITask(
            prompt="What is the capital of France?",
            backend=inference_backend.name
        ),
        AITask(
            prompt=["Tell me a joke", "What is 2+2?"],
            backend=inference_backend.name
        ),
        ComputeTask(
            executable="/usr/bin/echo",
            arguments=["Hello from Dragon!"],
            backend=execution_backend.name,
        ),
    ]

    # Submit all tasks at once
    await session.submit_tasks(tasks)

    # Wait for results
    results = await asyncio.gather(*tasks)

    # Process results
    for i, task in enumerate(results):
        if "prompt" in task:
            # AITask: use .response for model output
            print(f"Task {i + 1} [AI]: {task.response}")
        else:
            # ComputeTask: use .return_value for function/executable output
            print(f"Task {i + 1} [Compute]: {task.return_value}")

    await session.close()

if __name__ == "__main__":
    asyncio.run(main())
```

!!! important "Running with Dragon"
    Scripts using Dragon backends must be launched with the `dragon` command:
    ```bash
    dragon -m my_script.py
    ```

### Configuration Options

The `DragonVllmInferenceBackend` supports the following parameters:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `config_file` | str | Required | Path to vLLM configuration YAML file |
| `model_name` | str | Required | HuggingFace model name or local path |
| `num_nodes` | int | 1 | Number of nodes for inference |
| `num_gpus` | int | 1 | Number of GPUs per node |
| `tp_size` | int | 1 | Tensor parallelism size |
| `port` | int | 8000 | HTTP server port (if use_service=True) |
| `offset` | int | 0 | Node offset for multi-service deployments |
| `use_service` | bool | True | Enable HTTP server with OpenAI-compatible API |
| `max_batch_size` | int | 1024 | Maximum requests per batch |
| `max_batch_wait_ms` | int | 500 | Maximum time to wait for batch accumulation |

### Server Mode vs Engine Mode

#### Server Mode (HTTP API)

When `use_service=True`, the backend starts an HTTP server with OpenAI-compatible endpoints:

```python
inference_backend = DragonVllmInferenceBackend(
    config_file="config.yaml",
    model_name="llama-3-8b",
    use_service=True,
    port=8000
)

await inference_backend.initialize()

# Access via HTTP
# GET  http://<hostname>:8000/health
# POST http://<hostname>:8000/generate
# POST http://<hostname>:8000/v1/chat/completions  (OpenAI-compatible)
# GET  http://<hostname>:8000/v1/models
```

#### Engine Mode (Direct API)

When `use_service=False`, use the Python API directly:

```python
inference_backend = DragonVllmInferenceBackend(
    config_file="config.yaml",
    model_name="llama-3-8b",
    use_service=False
)

await inference_backend.initialize()

# Direct inference
results = await inference_backend.generate(
    prompts=["What is AI?", "Explain machine learning"],
    timeout=300
)
```

### Batching Strategy

The backend automatically batches requests for optimal throughput:

1. Accumulates requests for up to `max_batch_wait_ms` milliseconds
2. Processes immediately when batch reaches `max_batch_size`
3. Submits combined batch to vLLM pipeline
4. Distributes responses back to individual requests

This is significantly more efficient than processing requests individually, especially for high-throughput workloads.

!!! tip "Performance Tuning"
    - Increase `max_batch_size` for higher throughput with more memory
    - Reduce `max_batch_wait_ms` for lower latency
    - Use `tp_size > 1` for large models that don't fit on a single GPU

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
pip install "rhapsody[dragon]"  # or your preferred backend
```

### Basic Usage

Here's a complete workflow example using AsyncFlow with RHAPSODY's Dragon backend:

```python
import asyncio
import multiprocessing as mp

from radical.asyncflow import WorkflowEngine
from rhapsody.backends import DragonExecutionBackendV3

async def main():
    # Set Dragon as the multiprocessing start method
    mp.set_start_method("dragon")

    # Initialize RHAPSODY backend
    backend = await DragonExecutionBackendV3()

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
from rhapsody.backends import DragonExecutionBackendV3
backend = await DragonExecutionBackendV3(num_workers=2048)

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

For more information on specific backends, see the [Advanced Usage](getting-started/advanced-usage.md) guide.
