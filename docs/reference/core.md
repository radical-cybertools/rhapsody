# Core API

This section documents the core RHAPSODY APIs and functions.

## Main Functions

::: rhapsody.get_backend

::: rhapsody.discover_backends

## Backend Registry

::: rhapsody.BackendRegistry
```

**Parameters:**

- `name` (str): Backend name identifier
- `backend_class` (type): Backend class implementing BaseBackend

**Example:**

```python
from rhapsody.backends.base import BaseBackend

class MyBackend(BaseBackend):
    # Implementation here
    pass

rhapsody.register_backend("my_backend", MyBackend)
backend = rhapsody.get_backend("my_backend")
```

### list_backends()

List all available backend implementations.

```python
def list_backends() -> List[str]
```

**Returns:**

- List of backend names

**Example:**

```python
backends = rhapsody.list_backends()
print(f"Available backends: {backends}")
# Output: ['concurrent', 'dask', 'radical_pilot']
```

## Exception Classes

### RhapsodyError {#RhapsodyError}

Base exception class for all RHAPSODY errors.

```python
class RhapsodyError(Exception):
    """Base exception for RHAPSODY errors."""

    def __init__(self, message: str, details: Optional[Dict] = None):
        self.message = message
        self.details = details or {}
        super().__init__(message)
```

**Attributes:**

- `message` (str): Error description
- `details` (dict): Additional error context

### BackendError {#BackendError}

Exception raised for backend-specific errors.

```python
class BackendError(RhapsodyError):
    """Exception for backend execution errors."""

    def __init__(self, message: str, backend_type: str, details: Optional[Dict] = None):
        self.backend_type = backend_type
        super().__init__(message, details)
```

**Attributes:**

- `backend_type` (str): Backend that caused the error
- Inherits from RhapsodyError

### TaskError {#TaskError}

Exception raised for task execution failures.

```python
class TaskError(RhapsodyError):
    """Exception for task execution errors."""

    def __init__(self, message: str, task_uid: str, exit_code: Optional[int] = None, details: Optional[Dict] = None):
        self.task_uid = task_uid
        self.exit_code = exit_code
        super().__init__(message, details)
```

**Attributes:**

- `task_uid` (str): Unique identifier of failed task
- `exit_code` (int, optional): Process exit code
- Inherits from RhapsodyError

### ConfigurationError {#ConfigurationError}

Exception raised for configuration-related errors.

```python
class ConfigurationError(RhapsodyError):
    """Exception for configuration errors."""

    def __init__(self, message: str, config_key: Optional[str] = None, details: Optional[Dict] = None):
        self.config_key = config_key
        super().__init__(message, details)
```

**Attributes:**

- `config_key` (str, optional): Configuration key that caused error
- Inherits from RhapsodyError

## Usage Examples

### Error Handling

```python
import asyncio
import rhapsody
from rhapsody.exceptions import TaskError, BackendError, ConfigurationError

async def robust_workflow():
    try:
        backend = rhapsody.get_backend("concurrent")

        tasks = [
            {
                "uid": "test_task",
                "executable": "false"  # Always fails
            }
        ]

        await backend.submit_tasks(tasks)
        await backend.wait()

    except TaskError as e:
        print(f"Task {e.task_uid} failed with exit code {e.exit_code}")
        print(f"Error details: {e.details}")

    except BackendError as e:
        print(f"Backend '{e.backend_type}' error: {e.message}")

    except ConfigurationError as e:
        print(f"Configuration error in '{e.config_key}': {e.message}")

    except RhapsodyError as e:
        print(f"RHAPSODY error: {e.message}")

    except Exception as e:
        print(f"Unexpected error: {e}")

asyncio.run(robust_workflow())
```

### Backend Discovery

```python
import rhapsody

# List available backends
print("Available backends:")
for backend_name in rhapsody.list_backends():
    try:
        backend = rhapsody.get_backend(backend_name)
        print(f"  {backend_name}: {backend.__class__.__name__}")
    except Exception as e:
        print(f"  {backend_name}: Not available ({e})")
```

### Custom Backend Registration

```python
from rhapsody.backends.base import BaseBackend
from typing import List, Dict, Any

class LoggingBackend(BaseBackend):
    """Backend that logs all operations."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.submitted_tasks = []

    async def submit_tasks(self, tasks: List[Dict[str, Any]]) -> None:
        print(f"LoggingBackend: Submitting {len(tasks)} tasks")
        self.submitted_tasks.extend(tasks)
        # Actual submission logic here

    async def wait(self, timeout: Optional[float] = None) -> None:
        print("LoggingBackend: Waiting for tasks to complete")
        # Actual wait logic here

    async def terminate(self) -> None:
        print("LoggingBackend: Terminating")
        # Cleanup logic here

# Register the custom backend
rhapsody.register_backend("logging", LoggingBackend)

# Use the custom backend
backend = rhapsody.get_backend("logging")
```

## Type Definitions

Common type aliases used throughout RHAPSODY:

```python
from typing import Dict, List, Any, Optional, Callable, Union

# Task definition type
TaskDict = Dict[str, Any]

# Collection of tasks
TaskList = List[TaskDict]

# Callback function signature
CallbackFunction = Callable[[TaskDict, str], None]

# Resource specification
ResourceDict = Dict[str, Union[int, str, float]]

# Configuration dictionary
ConfigDict = Dict[str, Any]
```

## Constants

Common constants used in RHAPSODY:

```python
# Task states
class TaskState:
    NEW = "NEW"
    WAITING = "WAITING"
    READY = "READY"
    EXECUTING = "EXECUTING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    RETRYING = "RETRYING"
    CANCELED = "CANCELED"

# Backend types
class BackendType:
    CONCURRENT = "concurrent"
    DASK = "dask"
    RADICAL_PILOT = "radical_pilot"

# Default values
class Defaults:
    BACKEND_TYPE = BackendType.CONCURRENT
    MAX_WORKERS = 4
    RETRY_ATTEMPTS = 0
    RETRY_DELAY = 1.0
    TIMEOUT = None
```

For more detailed API documentation, see the individual backend and component references.
