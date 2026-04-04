# Configuration

RHAPSODY uses a flexible configuration system for its session and backends.

## Session Configuration

The `Session` object is primarily configured through its constructor:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `backends` | `list` | `None` | List of initialized backend objects. |
| `uid` | `str` | `None` | Unique ID for the session (generated if None). |
| `work_dir` | `str` | `None` | Working directory for task files (defaults to CWD). |

## Backend Configuration

Each backend has its own set of parameters.

### Concurrent Backend
Local execution using Python's `concurrent.futures`.

```python
from concurrent.futures import ThreadPoolExecutor
from rhapsody.backends import ConcurrentExecutionBackend

# Default uses ThreadPoolExecutor
backend = ConcurrentExecutionBackend(name="local")

# Or provide a custom executor
executor = ThreadPoolExecutor(max_workers=8)
backend = ConcurrentExecutionBackend(name="local", executor=executor)
```

### Dask Backend
Distributed execution using any Dask-compatible cluster.

```python
DaskExecutionBackend(
    resources={"n_workers": 4, "memory_limit": "2GB"},  # passed to LocalCluster when no cluster/client given
    cluster=pre_existing_cluster_obj,  # SLURMCluster, KubeCluster, LocalCluster, …
    client=pre_existing_client_obj     # pre-existing dask.distributed.Client
)
```

Supports **sync functions**, **async functions**, and **executable tasks**. Pass Dask-specific scheduling hints via `task_backend_specific_kwargs`:

```python
ComputeTask(
    function=my_fn, args=(x,),
    task_backend_specific_kwargs={"resources": {"GPU": 1}},  # GPU worker only
)
ComputeTask(
    executable="/bin/sim", arguments=["--n", "4"],
    task_backend_specific_kwargs={"resources": {"CPU": 4}, "shell": True},
)
```

!!! tip "Preconfigured Clusters"
    If both `cluster` and `client` are omitted, Rhapsody creates a new `LocalCluster` using the provided `resources`. Pass `cluster=` to use SLURM, Kubernetes, or any other Dask cluster type.

### Dragon Backend
High-performance execution using the Dragon runtime.

```python
from rhapsody.backends import DragonExecutionBackendV3

backend = DragonExecutionBackendV3(
    name="dragon",
    num_nodes=4,            # Total nodes for the worker pool (optional)
    pool_nodes=2,           # Nodes per worker pool (optional)
    disable_telemetry=False,  # Enable/disable Dragon telemetry
)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str` | `"dragon"` | Backend identifier used for task routing |
| `num_nodes` | `int` | `None` | Total number of nodes; forwarded to `Batch(num_nodes=...)` |
| `pool_nodes` | `int` | `None` | Nodes per worker pool; forwarded to `Batch(pool_nodes=...)` |
| `disable_telemetry` | `bool` | `False` | Disable Dragon internal telemetry |

!!! note "Streaming pipeline"
    `DragonExecutionBackendV3` uses Dragon's streaming batch pipeline — tasks submitted via
    `session.submit_tasks()` are dispatched individually by a continuously running background
    thread. There is no compile or start step; tasks begin executing as soon as they are submitted.

!!! note "Dragon Versions"
    While `DragonExecutionBackendV3` is recommended for most users and will be always maintained, V1 and V2 are also available for legacy compatibility.

### RADICAL-Pilot Backend
Large-scale HPC execution using RADICAL-Pilot.

```python
RadicalExecutionBackend(
    resources={
        "resource": "local.localhost",
        "runtime": 30, # minutes
        "cores": 128
    }
)
```

## Environment Variables

RHAPSODY respects the following environment variables:

- `RHAPSODY_LOG_LEVEL`: Set to `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL`.
- `RHAPSODY_WORK_DIR`: Base directory for job artifacts.

!!! note "Logging"
    You can also enable logging programmatically:
    ```python
    from rhapsody import enable_logging
    enable_logging(level="DEBUG")
    ```
