# Execution backends: constructor and per-task options

This file summarises what an agent needs to configure each backend. Authoritative detail lives in
`$RHAPSODY_REPO/docs/getting-started/resource-specification.md` and `configuration.md`. If anything
here differs from `python scripts/rhapsody_env.py --api`, trust the live signature.

Rules that apply to every backend:
- Create a backend with `backend = await <Backend>(...)`.
- Each backend has a `name`, which defaults to its kind. Tasks select a backend with `backend=<name>`.
- Per-task options go in `ComputeTask(..., task_backend_specific_kwargs={...})`. Only the keys listed for the backend that runs the task are used.

---

## ConcurrentExecutionBackend: one host

```python
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from rhapsody.backends import ConcurrentExecutionBackend

backend = await ConcurrentExecutionBackend()                                     # default ThreadPoolExecutor
backend = await ConcurrentExecutionBackend(ThreadPoolExecutor(max_workers=8), name="local")
backend = await ConcurrentExecutionBackend(ProcessPoolExecutor(max_workers=8))   # needs cloudpickle
```

- **Constructor:** `executor` (any `concurrent.futures.Executor`) and `name` (default `"concurrent"`).
- **Concurrency:** bounded by the executor's `max_workers`.
- **Threads vs processes:** threads share the GIL, so use processes for CPU-bound Python functions. Executables run as subprocesses either way.

Per-task keys (executable tasks):

| Key | Meaning |
|---|---|
| `cwd` | Working directory |
| `env` | Environment dict; it **replaces** the inherited environment |
| `shell` | `True` joins executable and arguments and runs them through the shell |

The backend has no resource or GPU scheduling. To pin work, do it in the task:
- **GPU for an executable:** `env={**os.environ, "CUDA_VISIBLE_DEVICES": "1"}`. Merge with `os.environ`, because `env` replaces the whole environment.
- **CPU:** `os.sched_setaffinity` inside a function.

Install: included in `pip install rhapsody-py`. Example: `$RHAPSODY_REPO/examples/00-workload-native-api.py`.

---

## DaskExecutionBackend: Dask cluster

```python
from rhapsody.backends import DaskExecutionBackend

backend = await DaskExecutionBackend(resources={"n_workers": 4, "threads_per_worker": 1})  # new LocalCluster
backend = await DaskExecutionBackend(cluster=my_cluster)    # existing cluster (created asynchronous=True)
backend = await DaskExecutionBackend(client=my_client)      # existing Client(asynchronous=True)
```

**Constructor:**
- `resources`: forwarded to `dask.distributed.Client(**resources)`, which builds a `LocalCluster` when no cluster is given.
- `name`: default `"dask"`.
- `cluster`, `client`: an existing cluster or client to use instead.

**Ownership:** the backend never closes a `client` you pass in. If you pass only a `cluster`, the backend creates and closes its own `Client` but leaves the cluster alone.

Per-task keys:

| Key | Meaning |
|---|---|
| `resources` | Dask resource constraints, e.g. `{"GPU": 1}`. Workers must be started with `--resources "GPU=1"`. A task no worker can satisfy fails immediately. |
| `cwd`, `env`, `shell` | Executable tasks only; same meaning as Concurrent |
| other `client.submit` kwargs | e.g. `retries`, `priority`, `workers` |

Function tasks may be sync or `async def`.

**GPUs:** `{"resources": {"GPU": n}}` schedules the task onto a worker that advertises GPUs. It
does not choose a device; a worker with several GPUs must set the device inside the task.

Install: `pip install "rhapsody-py[dask]"`. A batch-scheduler cluster also needs `dask-jobqueue`.

Examples: `$RHAPSODY_REPO/examples/05-dask-backend.py` and `07-dask-backend-slrum-cluster.py`. The second shows a cluster entered with `async with` and `asynchronous=True`; that setting is required.

---

## DragonExecutionBackend: multi-node HPC (Dragon runtime)

```python
import multiprocessing as mp
from rhapsody.backends import DragonExecutionBackend

mp.set_start_method("dragon")                      # before creating the backend
backend = await DragonExecutionBackend(batch_kwargs={"num_nodes": 4}, pmi="pmix", name="hpc")
# or, with defaults (whole allocation): backend = await DragonExecutionBackend()
```

Create the Dragon backend once and shut it down with the session (`async with Session(...)`). A
backend that is never shut down keeps the Dragon runtime from exiting.

Launch the whole script with `dragon app.py`, not `python`. On multiple nodes the launcher also takes:
- `-w`: native scheduler launcher, or `ssh` plus `--network-config`;
- `-t`: `hsta` or `tcp`;
- `--network-prefix`: which interfaces to use.

See [multi-node.md](multi-node.md).

**Constructor:**
- `batch_kwargs`: forwarded to `dragon.workflows.batch.Batch(...)`. Common keys:
  - `num_nodes`, which defaults to the whole allocation;
  - `workers_per_node`: worker pool size on each node (see `size_pool_to_affinity`);
  - `scheduler_workers`;
  - `results_ddict_mem`;
  - `results_ddict_managers_per_pool`;
  - `disable_telem`;
  - `task_logs`: RHAPSODY sets `True`. `False` speeds up very large runs but leaves `stdout`/`stderr` empty.
- `pmi`: default PMI for MPI jobs (`"pmix"`, `"cray"`, `"none"`).
- `size_pool_to_affinity`: default `True`. When the job may use only part of a node (partial-node allocations), the worker pool on each node is sized to the cores the job may use, and a warning is logged. `False` keeps Dragon's own sizing, which is half the node's CPUs. A `workers_per_node` in `batch_kwargs` takes precedence.
  - Process and MPI jobs are still placed as if the job had whole nodes. Keep their ranks within the CPUs the job holds.
- `name`: default `"dragon"`.

**How a task runs:**

| Task | Mode | `return_value` |
|---|---|---|
| `function=f` (no template) | native function | Python return value |
| `function=f` + `process_template` | process | process exit code |
| `executable=...` | process | (use `stdout`/`exit_code`) |
| any + `process_templates=[(n, {...}), ...]` | parallel job (MPI-capable) | list of per-rank exit codes |

**Per-task keys:**

| Key | Meaning |
|---|---|
| `process_template` | dict of `ProcessTemplate` options for one process |
| `process_templates` | list of `(n_procs, dict)` groups forming one job |
| `pmi` | `"pmix"` (Open MPI and other PMIx MPIs), `"cray"` (Cray MPICH), `"none"` (non-MPI parallel processes). Overrides the backend's `pmi`. |
| `timeout` | seconds before Dragon cancels the task. Always set one for multi-rank jobs. |

**`ProcessTemplate` options:**
- `cwd`, `env`, `stdout`, `stderr`, `options`;
- `policy`: placement and GPU affinity through `dragon.infrastructure.policy.Policy`.

There is no backend-level working directory; set `cwd` per task.

**Affinity and placement.** A `Policy` in `process_template` (one process), or in each
`process_templates` group (a job), places processes. The fields:
- `placement=Policy.Placement.HOST_NAME` with `host_name=`: run on a given node.
- `cpu_affinity=[core ids]`: pin to cores.
- `gpu_affinity=[gpu ids]`: bind GPUs; Dragon sets `CUDA_VISIBLE_DEVICES`.

Take hostnames, core ids and GPU ids from `dragon.native.machine.System().nodes` and
`Node(huid)` (`.hostname`, `.cpus`, `.gpus`), and spread tasks round-robin over nodes, then
cores or GPUs. Pinned functions run as processes, so `return_value` is the exit code. Snippet:
"CPU and GPU affinity" in `SKILL.md`. Walkthrough:
`$RHAPSODY_REPO/tutorials/dragon-resource-placement.ipynb`.

Install: `pip install "rhapsody-py[dragon]"`.

Docs:
- `$RHAPSODY_REPO/docs/getting-started/resource-specification.md`, which covers MPI/PMI, `libpmix.so`, and GPU policies;
- `advanced-usage.md` ("Dragon Process Templates");
- `$RHAPSODY_REPO/docs/hpc-machines/` for machine setup.

Examples: `$RHAPSODY_REPO/examples/01-workload-async-gather.py` and `02-workload-heterogeneous.py`. A notebook is at `$RHAPSODY_REPO/tutorials/dragon-resource-placement.ipynb`.

---

## RadicalExecutionBackend: RADICAL-Pilot

```python
from rhapsody.backends import RadicalExecutionBackend

backend = await RadicalExecutionBackend(
    resources={"resource": "local.localhost", "runtime": 30, "cores": 8},  # pilot description
)
```

**Constructor:**
- `resources`: a RADICAL-Pilot pilot description, e.g. `resource`, `runtime` in minutes, `cores`, `gpus`, plus queue and project fields for HPC targets;
- `raptor_config`: masters and workers; this is required for **function** tasks;
- `name`: default `"radical_pilot"`.

**Per-task keys:** `task_backend_specific_kwargs` becomes a `radical.pilot.TaskDescription`, e.g. `ranks`, `cores_per_rank`, `gpus_per_rank`, `pre_exec`.

**Data staging:** tasks run in separate sandboxes, so this backend supports data linking between them.

Install: `pip install "rhapsody-py[radical_pilot]"`. Resource names come from RADICAL-Pilot's resource configuration for the target machine.

---

## OrbitExecutionBackend: remote execution through a broker

```python
from rhapsody.backends import OrbitExecutionBackend

backend = await OrbitExecutionBackend(broker_url="https://broker.example.org:8000",
                                      endpoint_name=None, backends=["dragon"])
```

The backend sends tasks to a remote ORBIT endpoint, which runs them with a local RHAPSODY backend.

**Configuration:**
- `broker_url` falls back to `RADICAL_ORBIT_BROKER_URL` or `~/.radical/orbit/broker.url`.
- The bearer token comes from `RADICAL_ORBIT_BROKER_TOKEN` or `~/.radical/orbit/broker.token`.
- `batch_window` and `batch_limit` batch submissions.
- `start_timeout` and `init_timeout` bound start-up.

**Function tasks** are shipped with cloudpickle, so client and endpoint must run the same Python `major.minor`.

Install: `pip install "rhapsody-py[orbit]"`. Docs: `$RHAPSODY_REPO/docs/getting-started/configuration.md` ("Orbit Backend").

---

## Several backends in one session

```python
local = await ConcurrentExecutionBackend(name="local")
hpc = await DaskExecutionBackend(resources={"n_workers": 4}, name="hpc")
async with Session(backends=[local, hpc]) as session:
    await session.submit_tasks([
        ComputeTask(function=prep, backend="local"),
        ComputeTask(function=heavy, backend="hpc"),
    ])
```

- A task without `backend` goes to the first backend in the list.
- An unknown name raises `ValueError` listing the available backends.
- Inference backends, such as the vLLM backend for `AITask`, join the same session. The RHAPSODY AI-HPC skill covers them, as does `$RHAPSODY_REPO/docs/ai/index.md`.
