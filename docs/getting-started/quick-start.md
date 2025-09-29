# Quick Start

This tutorial will get you up and running with RHAPSODY in just a few minutes. You'll create a simple workflow, execute it, and understand the basic concepts.

## Hello World Workflow

Let's start with a simple "Hello World" example that demonstrates the core RHAPSODY concepts.

### Step 1: Import Required Modules

```python
import asyncio
import rhapsody
from rhapsody.backends import Session
```

### Step 2: Create a Simple Workflow

```python
async def hello_world():
    # Create a session
    session = Session()

    # Get the Dask backend
    backend = rhapsody.get_backend("dask")

    # Set up a callback to track results
    results = []
    def task_callback(task, state):
        print(f"Task {task['uid']} changed to state: {state}")
        results.append((task['uid'], state))

    # Register the callback
    backend.register_callback(task_callback)

    # Define a simple task
    tasks = [
        {
            "uid": "hello_task",
            "executable": "echo",
            "arguments": ["Hello, RHAPSODY!"]
        }
    ]

    # Submit and execute the task
    await backend.submit_tasks(tasks)

    # Wait for completion
    await backend.wait()

    print(f"Workflow completed! Results: {results}")

# Run the workflow
if __name__ == "__main__":
    asyncio.run(hello_world())
```

### Step 3: Run the Workflow

Save the code above as `hello_rhapsody.py` and run:

```bash
python hello_rhapsody.py
```

Expected output:
```
Task hello_task changed to state: EXECUTING
Task hello_task changed to state: COMPLETED
Workflow completed! Results: [('hello_task', 'EXECUTING'), ('hello_task', 'COMPLETED')]
```

## Understanding the Code

Let's break down what happened in the Hello World example:

### Session
```python
session = Session()
```
A session manages the workflow execution context and resource lifecycle.

### Backend Selection
```python
backend = rhapsody.get_backend("dask")
```
We selected the Dask backend, which provides local and distributed computing capabilities.

### Callback Registration
```python
def task_callback(task, state):
    print(f"Task {task['uid']} changed to state: {state}")

backend.register_callback(task_callback)
```
Callbacks provide real-time monitoring of task state changes.

### Task Definition
```python
tasks = [
    {
        "uid": "hello_task",
        "executable": "echo",
        "arguments": ["Hello, RHAPSODY!"]
    }
]
```
Tasks are defined as dictionaries with:
- `uid`: Unique identifier
- `executable`: Command to run
- `arguments`: Command line arguments

## A More Complex Example

Let's create a workflow with dependencies and data flow:

```python
import asyncio
import tempfile
import os
import rhapsody
from rhapsody.backends import Session

async def data_processing_workflow():
    session = Session()
    backend = rhapsody.get_backend("dask")

    # Create a temporary directory for outputs
    temp_dir = tempfile.mkdtemp()

    # Define tasks with dependencies
    tasks = [
        {
            "uid": "generate_data",
            "executable": "python",
            "arguments": ["-c",
                f"import random; "
                f"data = [random.randint(1, 100) for _ in range(10)]; "
                f"with open('{temp_dir}/data.txt', 'w') as f: "
                f"f.write('\\n'.join(map(str, data)))"],
            "input_staging": [],
            "output_staging": [f"{temp_dir}/data.txt"]
        },
        {
            "uid": "process_data",
            "executable": "python",
            "arguments": ["-c",
                f"with open('{temp_dir}/data.txt', 'r') as f: "
                f"numbers = [int(line.strip()) for line in f]; "
                f"result = sum(numbers) / len(numbers); "
                f"print(f'Average: {{result}}'); "
                f"with open('{temp_dir}/result.txt', 'w') as f: "
                f"f.write(str(result))"],
            "input_staging": [f"{temp_dir}/data.txt"],
            "output_staging": [f"{temp_dir}/result.txt"],
            "dependencies": ["generate_data"]
        }
    ]

    # Execute workflow
    await backend.submit_tasks(tasks)
    await backend.wait()

    # Read the result
    with open(f"{temp_dir}/result.txt", "r") as f:
        result = f.read().strip()
        print(f"Final result: {result}")

    # Cleanup
    import shutil
    shutil.rmtree(temp_dir)

if __name__ == "__main__":
    asyncio.run(data_processing_workflow())
```

This example demonstrates:
- Task dependencies (`"dependencies": ["generate_data"]`)
- File staging for input/output data
- Data flow between tasks

## Key Takeaways

From these examples, you've learned:

1. **Session management**: Sessions orchestrate workflow execution
2. **Backend selection**: Choose between Dask (local/distributed) and RADICAL-Pilot (HPC)
3. **Task definition**: Tasks are defined with executables, arguments, and metadata
4. **Callbacks**: Real-time monitoring of task execution
5. **Dependencies**: Tasks can depend on other tasks
6. **Data staging**: File inputs and outputs can be managed automatically

## Backend Selection Guide

Choose the right backend for your use case:

- **Dask**: Use for development, testing, and moderate-scale workflows. Works on all platforms.
- **RADICAL-Pilot**: Use for large-scale HPC computing on clusters and supercomputers.

Example with RADICAL-Pilot:
```python
# For HPC environments
hpc_resources = {
    "resource": "your.hpc.system",
    "runtime": 30,  # minutes
    "cores": 16
}
backend = await rhapsody.get_backend("radical_pilot", hpc_resources)
```

## Next Steps

Now that you understand the basics, you can:

1. **Explore different backends**: Try the [Dask](../user-guide/backends.md#dask-backend) or [RADICAL-Pilot](../user-guide/backends.md#radical-pilot-backend) backends
2. **Build complex workflows**: Learn about [workflow orchestration](../user-guide/orchestration.md)
3. **See real examples**: Check out the [Examples](../examples/index.md) section
4. **Create your first workflow**: Follow the [First Workflow](first-workflow.md) tutorial

Ready to build something more substantial? Continue with the [First Workflow](first-workflow.md) tutorial.
