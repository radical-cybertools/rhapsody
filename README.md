# RHAPSODY

[![Build Status](https://github.com/radical-cybertools/rhapsody/actions/workflows/ci.yml/badge.svg)](https://github.com/radical-cybertools/rhapsody/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/radical-cybertools/rhapsody/branch/devel/graph/badge.svg)](https://codecov.io/gh/radical-cybertools/rhapsody)
[![Python Version](https://img.shields.io/pypi/pyversions/rhapsody.svg)](https://pypi.org/project/rhapsody/)
[![PyPI Version](https://img.shields.io/pypi/v/rhapsody.svg)](https://pypi.org/project/rhapsody/)
[![License](https://img.shields.io/pypi/l/rhapsody.svg)](https://github.com/radical-cybertools/rhapsody/blob/main/LICENSE.md)

**RHAPSODY** – **R**untime for **H**eterogeneous **AP**plications, **S**ervice **O**rchestration and **DY**namism

RHAPSODY is a high-performance runtime system designed for executing heterogeneous HPC-AI workflows with dynamic task graphs on high-performance computing infrastructures. It provides seamless integration between different computational paradigms, enabling efficient orchestration of complex scientific workloads.

## Key Features

- **Heterogeneous Execution**: Support for mixed CPU/GPU workloads and diverse computational frameworks
- **Dynamic Task Graphs**: Runtime adaptation of workflow structures based on execution results
- **Multiple Backend Support**: Pluggable execution backends including concurrent, Dask, and RADICAL-Pilot
- **HPC-Optimized**: Designed for large-scale scientific computing on supercomputing clusters
- **AsyncFlow Integration**: Full compatibility with AsyncFlow workflow management
- **Platform Abstraction**: Unified interface across different HPC platforms and resource managers
- **Fault Tolerance**: Robust error handling and recovery mechanisms
- **Real-time Monitoring**: Comprehensive logging and state tracking capabilities

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Core Concepts](#core-concepts)
- [Execution Backends](#execution-backends)
- [Platform Support](#platform-support)
- [Examples](#examples)
- [API Reference](#api-reference)
- [Development](#development)
- [Contributing](#contributing)
- [License](#license)

## Installation

### Basic Installation

```bash
pip install rhapsody
```

### Development Installation

```bash
git clone https://github.com/radical-cybertools/rhapsody.git
cd rhapsody
pip install -e .
```

### Backend-Specific Dependencies

For specific execution backends, install additional dependencies:

```bash
# For Dask backend
pip install "rhapsody[dask]"

# For RADICAL-Pilot backend
pip install "rhapsody[radical_pilot]"

# For development
pip install "rhapsody[dev]"
```

## Quick Start

### Basic Usage

```python
import asyncio
import rhapsody
from rhapsody.backends import Session

async def main():
    # Create a session
    session = Session()

    # Get a backend (concurrent backend by default)
    backend = rhapsody.get_backend("concurrent")

    # Set up a callback to track task results
    results = []
    def callback(task, state):
        results.append((task["uid"], state))

    backend.register_callback(callback)

    # Define tasks
    tasks = [
        {
            "uid": "task_1",
            "executable": "echo",
            "arguments": ["Hello, RHAPSODY!"]
        },
        {
            "uid": "task_2",
            "executable": "python",
            "arguments": ["-c", "print('Task 2 complete')"]
        }
    ]

    # Submit and execute tasks
    task_futures = await backend.submit_tasks(tasks)

    # Wait for all tasks to complete
    await asyncio.gather(*task_futures)

    # Check results
    for task_uid, state in results:
        print(f"Task {task_uid}: {state}")

    # Cleanup
    await backend.shutdown()
    session.close()

if __name__ == "__main__":
    asyncio.run(main())
```

### AsyncFlow Integration

```python
import asyncio
from rhapsody.backends.execution import ConcurrentExecutionBackend

# AsyncFlow workflow compatibility
async def run_asyncflow_workflow():
    backend = ConcurrentExecutionBackend()

    # Set up callback to track results
    results = []
    def callback(task, state):
        results.append((task["uid"], state))

    backend.register_callback(callback)

    # Define AsyncFlow-compatible tasks
    workflow_tasks = [
        {
            "uid": "data_prep",
            "executable": "python",
            "arguments": ["prep_data.py", "--input", "dataset.csv"]
        },
        {
            "uid": "analysis",
            "executable": "python",
            "arguments": ["analyze.py", "--input", "prepared_data.csv"],
            "dependencies": ["data_prep"]
        }
    ]

    # Execute workflow
    task_futures = await backend.submit_tasks(workflow_tasks)

    # Wait for all tasks to complete
    await asyncio.gather(*task_futures)

    # Check results
    for task_uid, state in results:
        print(f"Task {task_uid}: {state}")

    await backend.shutdown()

asyncio.run(run_asyncflow_workflow())
```

## Core Concepts

### Sessions
Sessions provide the execution context and resource management for RHAPSODY workflows:

```python
session = rhapsody.Session()
session.proxy_start()  # Start communication proxy
session.registry_start()  # Start service registry
```

### Tasks and Workloads
Tasks represent individual computational units with flexible execution models:

```python
task = {
    "uid": "unique_task_id",
    "executable": "/path/to/executable",
    "arguments": ["arg1", "arg2"],
    "environment": {"VAR": "value"},
    "working_directory": "/work/dir",
    "dependencies": ["parent_task_id"]
}
```

### State Management
RHAPSODY provides comprehensive task state tracking:

- `NEW`: Task created but not scheduled
- `SCHEDULING_PENDING`: Awaiting resource allocation
- `EXECUTING_PENDING`: Resources allocated, waiting to execute
- `EXECUTING`: Currently running
- `DONE`: Completed successfully
- `FAILED`: Terminated with error
- `CANCELLED`: Manually terminated

## Execution Backends

RHAPSODY supports multiple execution backends for different use cases:

### Concurrent Backend (Default)
High-performance local execution using Python's concurrent.futures:

```python
from rhapsody.backends.execution import ConcurrentExecutionBackend

backend = ConcurrentExecutionBackend()
```

**Features:**
- Multi-threaded task execution
- Process-based isolation
- Automatic resource management
- Subprocess cleanup and monitoring

### NoOp Backend
Lightweight backend for testing and development:

```python
backend = rhapsody.get_backend("noop")
```

### Dask Backend (Optional)
Distributed computing with Dask:

```python
# Requires: pip install "rhapsody[dask]"
backend = rhapsody.get_backend("dask")
```

### RADICAL-Pilot Backend (Optional)
HPC-optimized execution on supercomputing resources:

```python
# Requires: pip install "rhapsody[radical_pilot]"
backend = rhapsody.get_backend("radical_pilot")
```

## Platform Support

RHAPSODY provides platform abstraction for major HPC systems:

### Supported Platforms

#### TACC Frontera
```python
from rhapsody.platforms import frontera

# Platform automatically configured for Frontera
platform = frontera
print(f"Cores per node: {platform.cores_per_node}")  # 56
print(f"Resource manager: {platform.resource_manager}")  # SLURM
```

### Resource Managers

- **SLURM**: Full support with automatic resource detection
- **PBS**: Planned support
- **LSF**: Planned support

### Custom Platforms
```python
from rhapsody.platforms import PlatformDescription, ResourceManager

custom_platform = PlatformDescription(
    resource_manager=ResourceManager.SLURM,
    cores_per_node=64,
    gpus_per_node=4,
    partition="gpu",
    work_dir="$SCRATCH",
    env_setup=[
        "module load python/3.9",
        "module load cuda/11.7"
    ]
)
```

## Examples

### Data Processing Pipeline

```python
import asyncio
import rhapsody

async def data_pipeline():
    backend = rhapsody.get_backend("concurrent")

    # Stage 1: Data ingestion
    ingest_tasks = []
    for i in range(4):
        task = {
            "uid": f"ingest_{i}",
            "executable": "python",
            "arguments": ["ingest.py", f"--shard={i}"]
        }
        ingest_tasks.append(task)

    # Stage 2: Data processing (depends on ingestion)
    process_task = {
        "uid": "process_all",
        "executable": "python",
        "arguments": ["process.py", "--parallel=4"],
        "dependencies": [f"ingest_{i}" for i in range(4)]
    }

    # Stage 3: Analysis
    analyze_task = {
        "uid": "analyze",
        "executable": "python",
        "arguments": ["analyze.py", "--output=results.json"],
        "dependencies": ["process_all"]
    }

    # Execute pipeline
    all_tasks = ingest_tasks + [process_task, analyze_task]
    futures = await backend.submit_tasks(all_tasks)

    # Wait for completion
    for task_id, future in futures.items():
        result = await future
        print(f"Task {task_id}: {result['state']}")

    await backend.shutdown()

asyncio.run(data_pipeline())
```

### HPC Workflow with GPU Tasks

```python
import asyncio
from rhapsody.platforms import PlatformDescription, ResourceManager

async def gpu_workflow():
    # Configure platform
    platform = PlatformDescription(
        resource_manager=ResourceManager.SLURM,
        partition="gpu",
        cores_per_node=32,
        gpus_per_node=4,
        env_setup=["module load cuda/11.7", "module load python/3.9"]
    )

    # Set up backend
    backend = rhapsody.get_backend("concurrent")

    # GPU-accelerated tasks
    gpu_tasks = [
        {
            "uid": "training",
            "executable": "python",
            "arguments": ["train_model.py", "--gpu", "--epochs=100"],
            "environment": {"CUDA_VISIBLE_DEVICES": "0"}
        },
        {
            "uid": "inference",
            "executable": "python",
            "arguments": ["inference.py", "--model=trained_model.pth"],
            "dependencies": ["training"],
            "environment": {"CUDA_VISIBLE_DEVICES": "1"}
        }
    ]

    # Execute workflow
    futures = await backend.submit_tasks(gpu_tasks)

    # Monitor execution
    for task_id, future in futures.items():
        try:
            result = await future
            print(f"✅ Task {task_id}: {result['state']}")
        except Exception as e:
            print(f"❌ Task {task_id} failed: {e}")

    await backend.shutdown()

asyncio.run(gpu_workflow())
```

## API Reference

### Core Classes

#### `Session`
Main entry point for RHAPSODY workflows.

```python
class Session:
    def __init__(self)
```

#### `BaseExecutionBackend`
Abstract base class for execution backends.

```python
class BaseExecutionBackend:
    async def submit_tasks(self, tasks: List[Dict]) -> List
    async def shutdown(self) -> None
    def state(self) -> str
```

### Utility Functions

```python
# Get available backends
rhapsody.get_backend(name: str) -> BaseExecutionBackend
rhapsody.discover_backends() -> Dict[str, bool]
```

### Task State Management

```python
from rhapsody.backends.constants import TasksMainStates

# Available states
TasksMainStates.DONE
TasksMainStates.FAILED
TasksMainStates.CANCELED
TasksMainStates.RUNNING
```

### State Mapping

```python
from rhapsody.backends.constants import StateMapper

# Create a state mapper for a backend
mapper = StateMapper(backend='concurrent')
```

## 🔧 Development

### Setting Up Development Environment

```bash
# Clone repository
git clone https://github.com/radical-cybertools/rhapsody.git
cd rhapsody

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install in development mode
pip install -e ".[dev]"

# Install pre-commit hooks
pre-commit install
```

### Running Tests

```bash
# Run all tests
pytest

# Run specific test suites
pytest tests/test_asyncflow_integration.py
pytest tests/test_backend_functionality.py
pytest tests/test_realworld_integration.py

# Run with coverage
pytest --cov=rhapsody --cov-report=html
```

### Code Quality

```bash
# Format code
ruff format

# Lint code
ruff check --fix

# Type checking
mypy src/rhapsody

# Run pre-commit on all files
pre-commit run --all-files
```

### Performance Testing

```bash
# Backend performance tests
pytest tests/test_backend_performance.py -v

# Real-world integration tests
pytest tests/test_realworld_integration.py -v
```

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

### Development Workflow

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass (`pytest`)
6. Run code quality checks (`pre-commit run --all-files`)
7. Commit your changes (`git commit -m 'Add amazing feature'`)
8. Push to the branch (`git push origin feature/amazing-feature`)
9. Open a Pull Request

### Reporting Issues

Please use the [GitHub issue tracker](https://github.com/radical-cybertools/rhapsody/issues) to report bugs or request features.

## License

RHAPSODY is licensed under the [MIT License](LICENSE.md).

## Acknowledgments

RHAPSODY is developed by the [RADICAL Research Group](http://radical.rutgers.edu/) at Rutgers University.

### Related Projects

- [AsyncFlow](https://github.com/radical-cybertools/asyncflow): Asynchronous workflow management
- [RADICAL-Utils](https://github.com/radical-cybertools/radical.utils): Utility library
- [RADICAL-Pilot](https://github.com/radical-cybertools/radical.pilot): Pilot-based runtime system

## NSF-Funded Project

RHAPSODY is supported by the National Science Foundation (NSF) under Award ID [2103986](https://www.nsf.gov/awardsearch/showAward?AWD_ID=2103986). This collaborative project aims to advance the state-of-the-art in heterogeneous workflow execution for scientific computing.

[Learn More About the Project →](project/nsf-award.md){ .md-button }

### Citations

If you use RHAPSODY in your research, please cite:

```bibtex
@software{rhapsody2024,
  title={RHAPSODY: Runtime for Heterogeneous Applications, Service Orchestration and Dynamism},
  author={RADICAL Research Team},
  year={2024},
  url={https://github.com/radical-cybertools/rhapsody},
  version={0.1.0}
}
```

## Support

- **Documentation**: https://rhapsody.readthedocs.io/
- **Issues**: https://github.com/radical-cybertools/rhapsody/issues
- **Discussions**: https://github.com/radical-cybertools/rhapsody/discussions
