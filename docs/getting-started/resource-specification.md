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

### What a task used (`observe_peaks`)

`ConcurrentExecutionBackend(executor, observe_peaks=True)` reports what each task used as
`task["observed"]` before its DONE or FAILED callback, with the same keys as the Dragon backend
(see [Peak memory](#peak-memory-observe_peaks)):

| Task | `host_mem` | `gpu_mem` | `started`, `ended`, `host` | `cpu_seconds` |
|------|------------|-----------|----------------------------|---------------|
| Executable | peak RSS from `wait4`, under the peak launcher | with `pynvml` | the command's | yes |
| Function, `ProcessPoolExecutor` | the worker's peak RSS during the call, its own size included; the growth during the call in `host_mem_growth`; `"approximate": True` (Linux only) | not measured | the call's, in the worker | the worker process's, during the call |
| Function, `ThreadPoolExecutor` | not measured (threads share one process) | not measured | the call's, in the worker | the calling thread's (not threads or processes the function starts); marked `"approximate": True` |

`started` and `ended` are wall-clock times taken where the task ran, so they show when it really
ran. The backend reports RUNNING when it hands the task to the executor or starts the launcher,
which can be earlier when the executor is busy. A FAILED task reports `observed` too: an
executable through its launcher, and a function that raised with what was measured until it
raised. Each executable starts one extra Python process, the launcher (`python -I`, so the
task's `PYTHON*` variables cannot break it). A command that is not found then exits with code 127
instead of raising.

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
| `timeout` | `float` | Seconds before Dragon cancels the task (default `1e9`, also for `None`) |

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
ranks can wait for it until the timeout, `1e9` s by default. The first multi-rank job
without one, or with `None`, logs a warning.

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

### Peak memory (`observe_peaks`)

With `observe_peaks=True`, the backend reports each task's peak memory as
`task["observed"] = {"host_mem": GiB, "gpu_mem": GiB per GPU}` when the task is DONE or
FAILED. AsyncFlow's `ResourcePolicy` learns the memory demand of a kind of task from it, and
AsyncFlow's policy telemetry carries it in `asyncflow.TaskReleased`. `DragonNativeExecutionBackend`
takes the same option and reports the same keys, always through the launcher for executables
(see its [guide](../execution/dragon-native.md#what-a-task-used-observe_peaks)).

When known, `observed` also has the task's execution window and CPU time:

- `started`, `ended`: wall-clock seconds, from where the task ran. The launcher stamps them
  around the command; a native function's wrapper, around the call. A job gives its earliest
  start and latest end. Dragon's `Task.rusage` gives them only when Dragon reports each
  process's `start` and `end`. RUNNING is reported when the task is handed to Batch, so these
  are the times to use for when a task really ran.
- `host` (or `hosts`, for a job on several nodes) and `cpu_seconds` (user plus system time,
  summed over a job's ranks; for a native function, the pool worker's during the call).
- A native function that raises still reports what was measured until it raised.

```python
backend = await DragonExecutionBackend(observe_peaks=True)  # peak_source="auto"
```

Executable tasks take their peaks from one of two sources, chosen by `peak_source`:

- `"dragon"`: a Dragon whose Batch tasks carry their processes' resource usage
  (`Task.rusage`; stock dragonhpc 0.14.1 does not) reports `max_rss` with each exit code. No
  extra process runs. It reports no GPU memory.
- `"launcher"`: each command runs under a small launcher that reaps it with `wait4` and samples
  NVML every 0.2 s. Works on stock Dragon, and also measures GPU memory.
- `"auto"` (the default) uses Dragon's rusage when the Dragon in use reports it, and the
  launcher otherwise. Pass `peak_source="launcher"` for GPU tasks.

| Task | How | `host_mem` | `gpu_mem` |
|------|-----|------------|-----------|
| Executable (process, `process_template`) | Dragon's `Task.rusage`, or the launcher | `ru_maxrss`: the largest single process of the command's tree | launcher only: largest per-GPU sum over the tree's processes; needs `pynvml` |
| Executable job (`process_templates`) | per rank, as above | sum over ranks (an upper bound); with the launcher, ranks that stayed at its floor count as one, the largest of them; `host_mem_max` is the largest rank, `ranks` their number | launcher only: per GPU, summed over the ranks that share it |
| Native function | the call is wrapped in the pool worker | the worker's peak RSS during the call, its own size included; the growth during the call in `host_mem_growth`; marked `"approximate": True` (Linux only) | not measured |
| Function with `process_template(s)` | Dragon's `Task.rusage` only | as for an executable | not measured |

Limits:

- `ru_maxrss` is a high-water mark kept across `exec`, so it includes the resident size of the
  process that forked the command: about 10 MiB with the launcher, and the size of Dragon's Local
  Services (about 140 MiB) with Dragon's rusage. A command that stays below it reports that size;
  larger peaks are exact. The launcher writes its size as the record's `floor`, so a job's ranks
  that stayed at or below it count as one, the largest of them, not one each. With Dragon's
  rusage, every rank counts its floor.
- `gpu_mem` is `None` without `pynvml`, without a GPU, or when NVML never listed the task's
  processes on a GPU. NVML reports host PIDs, so a task in its own PID namespace (some
  containers) is not seen.
- GPU memory is sampled every 0.2 s, so a GPU peak that lasts less than that can be missed
  or under-stated: a 0.3 s task that allocated 1 GiB reported 12 MiB.
- The launcher writes one small record per process to a directory the backend makes for each
  run, `<work_dir>/rhapsody-peaks.<tag>/<uid>.<random>/` (made absolute), which must be on a file
  system the nodes share. A job reports nothing unless exactly every rank wrote its record; a
  record written after the backend read the directory is dropped.
- The launcher runs the command with every signal at its default disposition and, outside a
  terminal's foreground, in a process group of its own. A signal to the launcher, or to its
  group, reaches the command's group once.
- NVML is set up in the launcher's sampler thread: a slow or broken NVML (or `pynvml`) neither
  delays the command nor fails the task; GPU memory NVML cannot tell (MIG) is unknown, not zero.
- A native function's `host_mem` is what its node held for it: the worker's own size (60 to
  100 MiB on Dragon) and the call's arguments included, so small calls are over-stated by about
  the worker's size. `host_mem_growth`, the growth during the call, misses memory the allocator
  kept from earlier calls in the same worker. Both count other threads of the worker, including
  threads an earlier call left running. The measurement travels to the worker by value, so the worker does not import
  rhapsody.
- A Dragon function that times out reports nothing: Dragon replaces its exception with a
  `TimeoutError` of its own.
- A task cancelled with `cancel_task()` reports nothing. A Dragon `timeout` ends as FAILED and
  still reports: the launcher passes the signal to the command and reaps it.
