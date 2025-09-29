# API Reference

Welcome to the RHAPSODY API Reference. This section provides detailed documentation for all public APIs, classes, and functions in RHAPSODY.

## Core Modules

### :octicons-package-16: rhapsody.backends

Backend implementations for different execution environments.

[:octicons-arrow-right-24: Backends API](backends.md)

### :octicons-package-16: rhapsody.session

Session management for workflow orchestration.

[:octicons-arrow-right-24: Session API](session.md)

### :octicons-package-16: rhapsody.tasks

Task definition and specification utilities.

[:octicons-arrow-right-24: Tasks API](task.md)

### :octicons-package-16: rhapsody.config

Configuration management and utilities.

[:octicons-arrow-right-24: Configuration API](configuration.md)

### :octicons-package-16: rhapsody.utils

Utility functions and helpers.

[:octicons-arrow-right-24: Utilities API](utilities.md)

## Quick Reference

### Basic Usage

```python
import rhapsody
from rhapsody.backends import Session

# Create session and get backend
session = Session()
backend = rhapsody.get_backend("dask")

# Define tasks
tasks = [
    {
        "uid": "example_task",
        "executable": "echo",
        "arguments": ["Hello, World!"]
    }
]

# Submit and execute
backend = rhapsody.get_backend("dask")
await backend.submit_tasks(tasks)
```
```

### Common Patterns

#### Task Dependencies

```python
tasks = [
    {"uid": "task_1", "executable": "echo", "arguments": ["step 1"]},
    {"uid": "task_2", "executable": "echo", "arguments": ["step 2"],
     "dependencies": ["task_1"]}
]
```

#### Error Handling

```python
try:
    await backend.submit_tasks(tasks)
    await backend.wait()
except Exception as e:
    print(f"Workflow failed: {e}")
```

#### Callbacks

```python
def monitor_progress(task, state):
    print(f"Task {task['uid']}: {state}")

backend.register_callback(monitor_progress)
```

## API Index

### Functions

- [`get_backend()`](core/#rhapsody.get_backend) - Get a backend instance
- [`register_backend()`](core/#rhapsody.BackendRegistry.register_backend) - Register a custom backend
- [`list_backends()`](core/#rhapsody.list_backends) - List available backends

### Classes

- [`Session`](session/#rhapsody.session.Session) - Workflow session management
- [`BaseExecutionBackend`](backends/#rhapsody.backends.base.BaseExecutionBackend) - Base backend interface
- [`DaskExecutionBackend`](backends/#rhapsody.backends.execution.dask_parallel.DaskExecutionBackend) - Dask-based distributed execution
- [`RadicalExecutionBackend`](backends/#rhapsody.backends.execution.radical_pilot.RadicalExecutionBackend) - RADICAL-Pilot HPC execution

### Exceptions

- [`RhapsodyError`](utilities/#rhapsody.exceptions.RhapsodyError) - Base exception class
- [`BackendError`](utilities/#rhapsody.exceptions.BackendError) - Backend-specific errors
- [`TaskError`](utilities/#rhapsody.exceptions.TaskError) - Task execution errors
- [`ConfigurationError`](utilities/#rhapsody.exceptions.ConfigurationError) - Configuration errors

## Type Definitions

RHAPSODY uses type hints throughout. Key type definitions:

```python
from typing import Dict, List, Any, Optional, Callable

TaskDict = Dict[str, Any]
TaskList = List[TaskDict]
CallbackFunction = Callable[[TaskDict, str], None]
```

## Configuration Schema

Task definition schema:

```yaml
task:
  uid: string               # Required: Unique identifier
  executable: string        # Required: Command to execute
  arguments: list           # Optional: Command arguments
  dependencies: list        # Optional: Task UIDs this depends on
  input_staging: list       # Optional: Input files to stage
  output_staging: list      # Optional: Output files to collect
  environment: dict         # Optional: Environment variables
  working_directory: string # Optional: Working directory
  cpu_cores: integer        # Optional: CPU cores required
  gpu_cores: integer        # Optional: GPU cores required
  memory: string            # Optional: Memory requirement (e.g., "4GB")
  walltime: string          # Optional: Maximum runtime (e.g., "1:00:00")
```

## Version Information

:octicons-info-16: **Current Version**: 0.1.0
:octicons-calendar-16: **Release Date**: 2024
:octicons-mark-github-16: **Source Code**: [GitHub Repository](https://github.com/radical-cybertools/rhapsody)

## API Stability

- **Stable APIs**: Core functionality is stable and backward compatible
- **Experimental APIs**: Marked with `@experimental` decorator
- **Deprecated APIs**: Will include deprecation warnings

## Need Help?

- :octicons-book-16: [User Guide](../user-guide/index.md) - Comprehensive usage guide
- :octicons-code-16: [Examples](../examples/index.md) - Working code examples
- :octicons-issue-opened-16: [GitHub Issues](https://github.com/radical-cybertools/rhapsody/issues) - Bug reports and questions
- :octicons-comment-discussion-16: [Discussions](https://github.com/radical-cybertools/rhapsody/discussions) - Community forum
