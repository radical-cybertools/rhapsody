# Task API Reference

API documentation for RHAPSODY task management and state handling.

## Task Execution Interface

Task execution is handled through backend interfaces. See the [Backends API](backends.md) for task submission methods.

## Task State Management

Task states and state mapping utilities are available in the [Utilities API](utilities.md#task-state-constants).

## Task Definition Schema

Tasks are defined as dictionaries with the following structure:

```python
task = {
    "uid": "unique_task_identifier",     # Required: String identifier
    "executable": "/path/to/executable",  # Required: Command to run
    "arguments": ["arg1", "arg2"],       # Optional: Command arguments
    "dependencies": ["task1", "task2"],   # Optional: Task dependencies
    "input_staging": [...],              # Optional: Files to stage in
    "output_staging": [...],             # Optional: Files to stage out
    "environment": {"VAR": "value"},     # Optional: Environment variables
    "working_directory": "/path",        # Optional: Working directory
    "cpu_cores": 4,                      # Optional: CPU requirement
    "gpu_cores": 1,                      # Optional: GPU requirement
    "memory": "8GB",                     # Optional: Memory requirement
    "walltime": "1:00:00"                # Optional: Runtime limit
}
```
