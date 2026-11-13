# Resource Specification Guide

RHAPSODY uses a **per-backend** model for resource requirements.
Resource requirements are expressed through `task_backend_specific_kwargs`, which is forwarded directly to the backend that executes the task during the creation and submission of the task.

This design keeps the task API thin and makes resource semantics explicit per backend.

---

## Quick Reference

| Backend | Resource key | cwd | env | shell |
|---------|-------------|-----|-----|-------|
| `ConcurrentExecutionBackend` | not supported | `task_backend_specific_kwargs={"cwd": "..."}` | `task_backend_specific_kwargs={"env": {...}}` | `task_backend_specific_kwargs={"shell": True}` |
| `DaskExecutionBackend` | `task_backend_specific_kwargs={"resources": {"GPU": 1}}` | `task_backend_specific_kwargs={"cwd": "..."}` | `task_backend_specific_kwargs={"env": {...}}` | `task_backend_specific_kwargs={"shell": True}` |
| `DragonExecutionBackend` | `task_backend_specific_kwargs={"process_template": {"policy": ...}}` | `task_backend_specific_kwargs={"process_template": {"cwd": "..."}}` | `task_backend_specific_kwargs={"process_template": {"env": {...}}}` | not applicable |

---

## Concurrent Backend

`ConcurrentExecutionBackend` runs tasks in a local `ThreadPoolExecutor` or `ProcessPoolExecutor`.
There is no slot or resource scheduler — all processes share the same host.

**Supported `task_backend_specific_kwargs`:**

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `cwd` | `str` | `None` (inherits process cwd) | Working directory for the subprocess |
| `env` | `dict` | `None` (inherits parent environment) | Environment variables |
| `shell` | `bool` | `False` | Execute via shell (`create_subprocess_shell`) |

```python
from rhapsody.api import ComputeTask

# Working directory
task = ComputeTask(
    executable="/bin/pwd",
    task_backend_specific_kwargs={"cwd": "/data"},
)

# Custom environment
task = ComputeTask(
    executable="/bin/printenv",
    task_backend_specific_kwargs={
        "env": {"MY_VAR": "hello", "CUDA_VISIBLE_DEVICES": "0"},
    },
)

# Shell execution
task = ComputeTask(
    executable="echo $HOSTNAME && date",
    task_backend_specific_kwargs={"shell": True, "cwd": "/tmp"},
)
```

!!! note "CPU / GPU pinning"
    The Concurrent backend has no resource scheduler. To pin threads or GPUs, do it
    inside your task function — for example, call `torch.cuda.set_device(gpu_id)` at
    the start of your callable, or use `os.sched_setaffinity` for CPU pinning.

---

## Dask Backend

`DaskExecutionBackend` submits tasks to a Dask cluster. Resource scheduling uses Dask's
native [resource system](https://distributed.dask.org/en/stable/resources.html).

**Supported `task_backend_specific_kwargs`:**

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `resources` | `dict` | `{}` | Dask resource constraints forwarded to `client.submit(..., resources=...)` |
| `cwd` | `str` | `None` | Working directory for executable tasks |
| `env` | `dict` | `None` | Environment variables for executable tasks |
| `shell` | `bool` | `False` | Execute executable via shell |

`cwd` and `env` apply to executable tasks only (they are passed to `subprocess.run` inside the Dask worker). For function tasks, set the working directory or environment from within the function itself.

```python
from rhapsody.api import ComputeTask

# GPU resource constraint — worker must be started with --resources "GPU=1"
task = ComputeTask(
    function=my_gpu_fn,
    args=(data,),
    task_backend_specific_kwargs={
        "resources": {"GPU": 1},
    },
)

# Multi-resource constraint
task = ComputeTask(
    function=large_model_fn,
    args=(data,),
    task_backend_specific_kwargs={
        "resources": {"GPU": 2, "memory": 32e9},
    },
)

# Executable with cwd, env, and shell
task = ComputeTask(
    executable="./simulate.sh",
    arguments=["--config", "run.yaml"],
    task_backend_specific_kwargs={
        "cwd": "/scratch/run-001",
        "env": {"OMP_NUM_THREADS": "4"},
        "shell": True,
    },
)
```

Workers must be started advertising the resources you request:

```bash
# Start a worker with GPU and custom memory resources
dask worker tcp://scheduler:8786 --resources "GPU=1,memory=32e9"
```

!!! warning "Unsatisfiable resources"
    If no worker can satisfy the requested resources, the task fails immediately with
    a `TaskValidationError`-like message instead of hanging indefinitely. This is caught
    by `DaskExecutionBackend._check_resources_satisfiable()` at submit time.

### Cluster injection

The `resources=` init parameter configures `LocalCluster`. For any other cluster,
pass it via `cluster=` or `client=` — the task code is unchanged:

```python
from dask_jobqueue import SLURMCluster
from rhapsody.backends import DaskExecutionBackend

async with SLURMCluster(
    cores=4, memory="8GB", walltime="01:00:00", asynchronous=True
) as cluster:
    await cluster.scale(jobs=4)
    backend = await DaskExecutionBackend(cluster=cluster)
```

!!! warning "asynchronous=True is required"
    Construct `SLURMCluster` (and other non-`LocalCluster` cluster managers) with
    `asynchronous=True` and enter it via `async with`, as above — otherwise it runs
    its own background-thread event loop, the `Client` built around it inherits the
    mismatched loop, and task results / shutdown fail with confusing
    `TypeError`/`AttributeError` errors. See
    [Cluster injection](advanced-usage.md#cluster-injection) for details and
    `examples/07-dask-backend-slrum-cluster.py` for a full working example.

---

## Dragon Backend (V3)

`DragonExecutionBackend` submits tasks to the Dragon runtime. All per-task resource
and placement settings flow through Dragon's [ProcessTemplate](https://dragonhpc.github.io/dragon/doc/_build/html/ref/native/dragon.native.process.html#dragon.native.process.ProcessTemplate).

There is **no backend-level `working_directory`** in V3. Every process
configuration must be set per-task via `task_backend_specific_kwargs`.

### Backend-level configuration (`batch_kwargs`)

Pass a `batch_kwargs` dict when constructing the backend to tune the underlying
`dragon.workflows.batch.Batch` instance:

```python
backend = await DragonExecutionBackend(batch_kwargs={
    "num_nodes": 8,
    "results_ddict_mem": 4 * 1024**3,  # 4 GiB results DDict
    "scheduler_workers": 16,
})
```

All accepted keys and their defaults are documented in the
[Dragon Batch API](https://dragonhpc.github.io/dragon/doc/_build/html/ref/workflows/dragon.workflows.batch.Batch.html#dragon.workflows.batch.Batch).

!!! note "Dragon 0.14.0"
    `pool_nodes` is accepted for backwards compatibility but has no effect in
    Dragon 0.14.0 — it is silently forced to `1` internally.

**Supported `task_backend_specific_kwargs`:**

| Key | Type | Description |
|-----|------|-------------|
| `process_template` | `dict` | `ProcessTemplate` kwargs for a single-process task |
| `process_templates` | `list[tuple[int, dict]]` | List of `(n_procs, template_dict)` for multi-rank jobs |
| `pmi` | `str` or `PMIBackend` | PMI for a `process_templates` job: `"pmix"`, `"cray"` or `"none"`; overrides the backend's `pmi` (see [MPI jobs](#mpi-jobs-pmi-and-timeout)) |
| `timeout` | `float` | Seconds the task may run; past them it ends `FAILED` with a `TimeoutError` (default `1e9`, also for `None`; see [Timeouts and cancellation](#timeouts-and-cancellation)) |

**`ProcessTemplate` kwargs** (passed inside `process_template` or each entry of `process_templates`):

| Parameter | Type | Description |
|-----------|------|-------------|
| `cwd` | `str` | Working directory for the process (default: `"."`) |
| `env` | `dict` | Environment variables |
| `stdout` | `int` | stdout handling (`PIPE`, `STDOUT`, `None`) |
| `stderr` | `int` | stderr handling (`PIPE`, `STDOUT`, `None`) |
| `policy` | `Policy` | Placement and GPU/NUMA affinity |
| `options` | `ProcessOptions` | Dragon process options |

!!! warning "Managed parameters"
    `target`, `args`, and `kwargs` are managed by RHAPSODY internally via
    `ComputeTask.executable`, `ComputeTask.arguments`, and `ComputeTask.function`.
    Do not set them inside `process_template`.

    `stdout` defaults to `Popen.PIPE` for all process tasks (Dragon 0.14.0
    requires this to be set explicitly for output capture). You can override
    it by passing `stdout=None` in your template if you want to suppress
    capture, or `stdout=Popen.STDOUT` to merge with stderr.

### Single-process task

```python
from rhapsody.api import ComputeTask

# Executable with cwd and env
task = ComputeTask(
    executable="/bin/bash",
    arguments=["-c", "echo $HOSTNAME"],
    task_backend_specific_kwargs={
        "process_template": {
            "cwd": "/scratch/run-001",
            "env": {"OMP_NUM_THREADS": "8"},
        }
    },
)

# Function task with cwd
task = ComputeTask(
    function=my_simulation,
    args=(config,),
    task_backend_specific_kwargs={
        "process_template": {"cwd": "/scratch/run-001"},
    },
)
```

### Multi-rank (parallel) job

Use `process_templates` (plural) to launch a task as a parallel job. Each entry is
`(n_procs, template_dict)`:

```python
# 2 groups × 2 processes = 4 processes total
task = ComputeTask(
    executable="/bin/bash",
    arguments=["-c", "echo $HOSTNAME"],
    task_backend_specific_kwargs={
        "process_templates": [
            (2, {"cwd": "/data", "env": {"RANK": "0"}}),
            (2, {"cwd": "/data", "env": {"RANK": "2"}}),
        ]
    },
)
```

### MPI jobs: PMI and timeout

Dragon launches a `process_templates` job with a PMI, and Batch's default is
`PMIBackend.CRAY`. That is right for Cray MPICH. With Open MPI it is wrong, often
silently: each rank may run as a job of one rank, or every rank may fail. Choose the PMI
for every job on the backend, or per task (the task's choice wins):

```python
backend = await DragonExecutionBackend(pmi="pmix")  # Open MPI and other PMIx-based MPIs

task = ComputeTask(
    executable="./mpi_app",
    task_backend_specific_kwargs={
        "process_templates": [(16, {})],
        "pmi": "pmix",     # or "cray", or "none" for jobs of non-MPI processes
        "timeout": 600,    # seconds
    },
)
```

When neither sets one, Batch's default applies, and a warning is logged once for each
kind of job it can break: an executable with several ranks, and a Python function with
one rank or with several. Under Cray MPICH (`CRAY_MPICH_DIR` or `CRAY_MPICH_VERSION`
set), where that default is right, a debug line takes its place. That check reads only
the environment of the process running the backend, so an Open MPI binary run with
`cray-mpich` loaded gets no warning, and still needs `pmi="pmix"`.

A task's `"pmi": None` is the same as no `pmi` key: the task takes the backend's PMI.
Batch's default is `"cray"`, so a task on a backend with another PMI asks for it by that
name.

Tasks with `process_template`, or with no template, run without a PMI: a `pmi` has no
effect on them, but an unknown name still fails the task.

Give MPI jobs a `timeout` too. When a rank dies, Dragon does not end the job: the other
ranks can wait for it until the timeout, `1e9` s by default, and the job then ends
`FAILED` with a `TimeoutError`. The first multi-rank job without one, or with `None`,
logs a warning.

RHAPSODY can't tell whether a job's processes use MPI, so set `"pmi": "none"` for jobs
that don't, such as several copies of a script or a Python function. They then run
without a PMI, and log neither warning. Batch's default can break such jobs too: on a
non-Cray node, every rank of a 4-rank Python function job crashed with it ("double free
or corruption", exit code -6), and the same job ran with `"none"`. A one-rank function
job gets Batch's default too.

!!! note "PMIx needs `libpmix.so`"
    Dragon loads PMIx with `dlopen("libpmix.so")`, so a directory holding `libpmix.so`
    must be on `LD_LIBRARY_PATH`. Some distributions ship that name only in their PMIx
    development package (`libpmix-dev` on Ubuntu). When it can't be loaded, the whole
    Dragon runtime aborts with "Unable to initialize PMIx server".

### Timeouts and cancellation

A task that runs past its `timeout` ends `FAILED`, with a `TimeoutError` as its exception,
so `wait_tasks` and consumers such as AsyncFlow see an error they can retry or report. Only
a task cancelled with `backend.cancel_task(uid)` ends `CANCELED`.

- **Function tasks.** Dragon raises the `TimeoutError`. It stops the function only when the
  function next runs Python code: one blocked in a C call, such as `time.sleep`, runs until
  that call returns.
- **Process and job tasks.** Dragon stops the processes with SIGINT, then SIGTERM, then
  SIGKILL, and reports only their exit codes. The backend reports the task as timed out
  when, once the timeout has passed since the task was submitted, a process ended by one of
  those signals or exited with 128 plus its number (130, 143 or 137), as a process that
  catches it usually does. The task keeps its `exit_code`. Time spent waiting in Batch's
  queue counts toward the timeout here, so a process killed by one of those signals for
  another reason after that time, the out-of-memory killer for instance, is reported as
  timed out too. A process that catches SIGINT and exits 0 ends `DONE`.

`cancel_task` returns `True` when the task ends `CANCELED` because of the call; the
`CANCELED` report has been made by the time it returns. It returns `False` when nothing
happens: an unknown uid, a task that has already ended, or a cancel Dragon refused because
the task is finishing. It does not raise for an unknown uid.

### GPU affinity with `Policy`

Use Dragon's `Policy` to pin processes to specific GPUs across nodes:

```python
from dragon.infrastructure.policy import Policy
from dragon.native.machine import System, Node
from rhapsody.api import ComputeTask

def make_gpu_policies(nprocs: int):
    """Round-robin GPU assignment across all Dragon nodes."""
    all_gpus = [
        (node.hostname, gpu_id)
        for huid in System().nodes
        for node in [Node(huid)]
        for gpu_id in node.gpus
    ]
    return [
        Policy(
            placement=Policy.Placement.HOST_NAME,
            host_name=all_gpus[i % len(all_gpus)][0],
            gpu_affinity=[all_gpus[i % len(all_gpus)][1]],
        )
        for i in range(nprocs)
    ]

policies = make_gpu_policies(nprocs=4)

# Single process pinned to GPU 0
task = ComputeTask(
    function=gpu_work,
    task_backend_specific_kwargs={
        "process_template": {"policy": policies[0]},
    },
)

# Parallel job: 2 groups of 2 processes each, each group on its own GPU
task = ComputeTask(
    function=gpu_work,
    task_backend_specific_kwargs={
        "process_templates": [
            (2, {"policy": policies[0]}),
            (2, {"policy": policies[1]}),
        ]
    },
)
```
