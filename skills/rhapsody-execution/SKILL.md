---
name: rhapsody-execution
description: >-
  Build, configure, run and debug workloads (sets of independent tasks) with RHAPSODY, the
  Python workload manager for heterogeneous HPC and AI tasks. Covers choosing and initializing
  an execution backend (Concurrent for one machine, Dask for a Dask cluster, Dragon for
  multi-node HPC allocations), defining ComputeTasks (executables or Python functions),
  per-task resources and CPU/GPU affinity, submitting through a Session, monitoring and
  collecting results, and handling failures and retries. Use when the user wants to run,
  scale out, port or troubleshoot a workload with RHAPSODY, or mentions rhapsody,
  ComputeTask, Session.submit_tasks or a RHAPSODY backend. For tasks that depend on other
  tasks (workflows, DAGs) use AsyncFlow instead.
metadata:
  rhapsody-version: "0.7.0"
---

# RHAPSODY Execution

RHAPSODY is a **workload manager**. It runs and tracks many heterogeneous tasks, such as
executables, Python functions and AI inference requests, on one or more **backends** through
one asyncio **Session** API. Workflow logic (which task feeds which) sits above it, in
AsyncFlow. This skill takes an agent from "here is my workload" to a running, debuggable
RHAPSODY program.

## Sources of truth

- `$RHAPSODY_REPO` is the root of a RHAPSODY checkout. When this skill sits in
  `<repo>/skills/rhapsody-execution/`, it is two directories up. Without a checkout, read the
  same paths at `https://github.com/radical-cybertools/rhapsody/blob/v<version>/<path>`.
  Documentation is also published at `https://radical-cybertools.github.io/rhapsody/`.
- The installed code wins over any prose, including this file. Check signatures with
  `python scripts/rhapsody_env.py --api` before relying on a parameter.

## When to use / not use

Use this skill to:
- write or adapt a RHAPSODY program;
- pick a backend or pass per-task resources;
- run a workload and read its results;
- debug failed or hanging tasks.

Do not use it for:
- **Tasks that depend on other tasks' outputs:** pipelines, multi-stage runs and DAGs are workflows. Use AsyncFlow and its skills; AsyncFlow runs on these same backends.
- **AI inference configuration** (`AITask` with the vLLM backend), **telemetry analysis**, or **performance experiments**. Separate RHAPSODY skills cover these. This skill only notes where they plug in.

## Execution model

| Object | What it is |
|---|---|
| Backend | Where tasks run: `ConcurrentExecutionBackend`, `DaskExecutionBackend`, `DragonExecutionBackend`, … It must be **awaited** before use. |
| `ComputeTask` | Exactly one of `executable=` (with `arguments=[...]`) or `function=` (with `args=`, `kwargs=`). |
| `AITask` | An inference request (`prompt=`), routed to an inference backend. |
| `Session` | Owns backends and routes each task to `task.backend`, or to the first backend when unset. It tracks state. |

Tasks are dicts **and** awaitables:
- `task.uid` is the same value as `task["uid"]`.
- After a task finishes, its `state`, `stdout`, `stderr`, `exit_code`, `return_value` and `exception` are filled in place.
- Terminal states are `DONE`, `FAILED` and `CANCELED`.

## Steps

### 1. Check the environment

```bash
python scripts/rhapsody_env.py          # version, available backends, launchers, GPUs
python scripts/rhapsody_env.py --api    # live signatures of Session, tasks, backends
```

Install what is missing:
- `pip install rhapsody-py` (Python >= 3.11);
- add extras as needed: `[dask]`, `[dragon]`, `[radical_pilot]`, `[orbit]`, `[telemetry]`.

### 2. Choose a backend

| Situation | Backend | Launch with |
|---|---|---|
| Laptop, one node, debugging, CI | `ConcurrentExecutionBackend` (threads; processes for CPU-bound Python) | `python app.py` |
| An existing or new Dask cluster (local, batch scheduler, Kubernetes) | `DaskExecutionBackend` | `python app.py` |
| Multi-node HPC allocation, many tasks, MPI jobs, GPU placement | `DragonExecutionBackend` | `dragon app.py` |
| Pilot-based HPC execution | `RadicalExecutionBackend` | `python app.py` |
| Resources reachable only through a broker or endpoint | `OrbitExecutionBackend` | `python app.py` |

Constructor arguments and per-task options for each backend are in
[references/backends.md](references/backends.md).

### 3. Create backend(s) and a Session

```python
import asyncio
from concurrent.futures import ThreadPoolExecutor

from rhapsody.api import ComputeTask, Session
from rhapsody.backends import ConcurrentExecutionBackend


async def main():
    backend = await ConcurrentExecutionBackend(ThreadPoolExecutor(max_workers=4), name="local")
    async with Session(backends=[backend], work_dir="./run") as session:
        task = ComputeTask(executable="/bin/echo", arguments=["hello"], backend="local")
        await session.submit_tasks([task])
        await task
        print(task.state, task.stdout.strip())


asyncio.run(main())
```

How this fits together:
- **Several backends in one session:** pass a list, and set `backend=<name>` on each task.
- **Cleanup:** `async with` calls `session.close()` on exit, which shuts down every backend. A closed session cannot be reused.
- **`work_dir`:** defaults to the current directory. Per-session files go to `<work_dir>/<session.uid>/`.

### 4. Define tasks

```python
ComputeTask(executable="/bin/sh", arguments=["-c", "hostname"])     # executable
ComputeTask(function=simulate, args=(1,), kwargs={"steps": 100})    # Python function (sync or async)
ComputeTask(executable="./sim", capture_stdio=True)                 # stdout/stderr to files
ComputeTask(executable="./sim", task_backend_specific_kwargs={...}) # per-task resources (step 9)
```

- **Function placement:** define functions at module level. Process pools, Dask and Dragon pickle them, so lambdas and closures fail there.
- **Executables:** they run on the worker host, so paths and inputs must exist there.
- **`uid`:** generated automatically (`task.000001`, …). Pass `uid=` for your own IDs; they must be unique within a session.

### 5. Submit and wait

`await session.submit_tasks(tasks)` returns futures and does not wait. Then use one of three wait modes:

| Mode | On task failure | Use for |
|---|---|---|
| `await session.wait_tasks(tasks, timeout=s)` | Never raises; inspect `task.state`. Raises `asyncio.TimeoutError` on timeout. | Batches, fire-and-inspect |
| `await asyncio.gather(*tasks)` | Raises the first failure | Fail-fast |
| `await task` | Raises that task's failure | Waiting on one specific task |

Exceptions:
- An executable that exits non-zero raises `rhapsody.TaskExecutionError`, with `exit_code` set.
- A function task re-raises its own exception.

Details: `$RHAPSODY_REPO/docs/getting-started/advanced-usage.md` ("Wait Modes").

### 6. Independent tasks only

Every task in a submission is independent: it starts as soon as the backend has capacity,
and RHAPSODY does not order tasks. Submit the whole workload at once and let the backend
schedule it. If a task needs another task's output, that is a workflow. Use AsyncFlow,
which runs on these same backends.

### 7. Handle failures and retry

```python
def clone(t):
    """A new task with the same definition as t."""
    extra = {k: t[k] for k in ("backend", "task_backend_specific_kwargs") if t.get(k)}
    if t.function:
        return ComputeTask(function=t.function, args=t.args, kwargs=t.kwargs, **extra)
    return ComputeTask(executable=t.executable, arguments=t.arguments, **extra)

await session.wait_tasks(tasks)
failed = [t for t in tasks if t.state != "DONE"]
for t in failed:
    print(t.uid, t.state, t.exit_code, t.exception or t.stderr)
retry = [clone(t) for t in failed]
await session.submit_tasks(retry)
await session.wait_tasks(retry)
```

Rules for retries:
- **One attempt per object:** a task object represents one execution, so every retry is a new task object.
- **Cap the attempts:** for example, at most N retries.
- **Persistent failures:** report them with uid, `exit_code` and `stderr`, or with the exception.
- **Cancellation:** `await backend.cancel_task(task.uid)` returns `True` when the backend cancelled the task.

### 8. Read results

| Task kind | Result |
|---|---|
| Function | `task.return_value` |
| Executable | `task.stdout`, `task.stderr`, `task.exit_code` |
| `capture_stdio=True` | `task.stdout` and `task.stderr` are **file paths** under `<work_dir>/<session.uid>/` |
| Dragon `function=` + `process_template` / `process_templates` | `return_value` is the exit code (a list of per-rank codes for jobs). Print or write results from the function, or use a plain `function=` task to get the Python value. |

### 9. Pass per-task resources

Resource and placement settings are **per backend**. Set them in `task_backend_specific_kwargs`:

```python
# Concurrent / Dask executable: working dir, environment, shell
ComputeTask(executable="./sim.sh", task_backend_specific_kwargs={"cwd": "/data/run1", "env": {"OMP_NUM_THREADS": "4"}})
# Dask: only workers advertising the resource run it (start workers with --resources "GPU=1")
ComputeTask(function=train, task_backend_specific_kwargs={"resources": {"GPU": 1}})
# Dragon: 16-rank MPI job with PMI and timeout
ComputeTask(executable="./mpi_app", task_backend_specific_kwargs={"process_templates": [(16, {})], "pmi": "pmix", "timeout": 600})
```

Values such as node counts, GPU indices, paths, PMI flavour and environment come from the
target machine. Ask the user or read the machine's documentation; do not invent them. Full
tables: `$RHAPSODY_REPO/docs/getting-started/resource-specification.md`.

#### CPU and GPU affinity (Dragon)

Dragon places each process with a `Policy`, passed in `process_template`, or per rank group
in `process_templates`. Take hostnames, core ids and GPU ids from the allocation's real
topology:

```python
from dragon.infrastructure.policy import Policy
from dragon.native.machine import Node, System

nodes = [Node(h) for h in System().nodes]          # .hostname, .cpus (core ids), .gpus (GPU ids or None)
host, cores, gpus = nodes[0].hostname, nodes[0].cpus, nodes[0].gpus or []

# One process on a given node, pinned to two cores
pinned = Policy(placement=Policy.Placement.HOST_NAME, host_name=host, cpu_affinity=cores[:2])
ComputeTask(function=work, task_backend_specific_kwargs={"process_template": {"policy": pinned}})

# One process bound to one GPU of that node (Dragon sets CUDA_VISIBLE_DEVICES)
if gpus:
    on_gpu = Policy(placement=Policy.Placement.HOST_NAME, host_name=host, gpu_affinity=gpus[:1])
    ComputeTask(function=work, task_backend_specific_kwargs={"process_template": {"policy": on_gpu}})

# Parallel job: each rank group gets its own cores
ComputeTask(
    function=work,
    task_backend_specific_kwargs={
        "process_templates": [
            (2, {"policy": Policy(cpu_affinity=cores[0:2])}),
            (2, {"policy": Policy(cpu_affinity=cores[2:4])}),
        ],
        "pmi": "none",
        "timeout": 600,
    },
)
```

Things to know:
- **Results of pinned functions:** a function run with a policy runs as a process, so its `return_value` is the exit code. Print or write its results.
- **Spreading many tasks:** assign policies round-robin over nodes, then cores or GPUs. The template's `--pin-cpus` and `--gpus-per-task` do exactly this.
- **Other backends:**
  - Dask: `{"resources": {"GPU": n}}` only *schedules* onto GPU workers; it does not pick a device.
  - Concurrent: has no placement. For executables, set `env={**os.environ, "CUDA_VISIBLE_DEVICES": "0"}`; `env` replaces the whole environment.
  - Pin CPUs inside the task with `os.sched_setaffinity`.
- **More detail:** "GPU affinity with `Policy`" in `$RHAPSODY_REPO/docs/getting-started/resource-specification.md` and `$RHAPSODY_REPO/tutorials/dragon-resource-placement.ipynb`.

## Launching on a specific machine

- **Concurrent and Dask:** these run anywhere Python runs.
- **Dragon:** needs the `dragon` launcher (`dragon app.py`) and
  `multiprocessing.set_start_method("dragon")` before the backend is created.
- **Multi-node runs (Dragon):** the RHAPSODY code is the same; what changes is the setup and the
  launch.
  1. Identify the target machine: ask the user, or check the hostname and scheduler.
  2. If `$RHAPSODY_REPO/docs/hpc-machines/` has a guide for it, follow that guide.
  3. Otherwise follow [references/multi-node.md](references/multi-node.md):
     - allocation;
     - environment;
     - interconnect;
     - network interfaces;
     - launcher (`-w`) and transport (`-t`);
     - MPI/PMI.

     Ask the user only for the site values it lists.
  4. Verify that the run spans the nodes: run the template with `--pin-cpus` and check the
     report's host column.

## Template

[assets/workload_template.py](assets/workload_template.py) is a complete, tested program. It
submits one heterogeneous workload in a single call: executable tasks, Python-function tasks
and, on Dragon, a multi-rank job. Each task carries its own resources. It then waits without
raising, prints a per-task report (state, exit code, host, cores, GPUs), retries failures once
as new task objects, and writes results. Optional flags add CPU/GPU placement and telemetry.

```bash
python assets/workload_template.py                          # Concurrent, threads
python assets/workload_template.py --process -w 8           # Concurrent, processes
python assets/workload_template.py --backend dask -w 4
dragon assets/workload_template.py --backend dragon --pin-cpus     # one core per process
dragon assets/workload_template.py --backend dragon --gpus-per-task 1
python assets/workload_template.py --telemetry ./tel --out results.json
python assets/workload_template.py --fail-task 2            # exercise the failure path
```

The report shows whether the requested placement actually took effect (`[placement ok]`).

To adapt it, copy it next to the user's code and replace `run_case` and `exec_command` with
the real task code, and `task_options` with the real resources. Keep the structure: build all
tasks, submit once, wait, report, retry.

## Existing examples (read before writing new code)

| Need | Where |
|---|---|
| Smallest program | `$RHAPSODY_REPO/examples/00-workload-native-api.py` |
| Many tasks with `asyncio.gather` | `$RHAPSODY_REPO/examples/01-workload-async-gather.py` (Dragon) |
| Dragon modes: function, process, MPI-style job | `$RHAPSODY_REPO/examples/02-workload-heterogeneous.py` |
| Dask: sync, async and executable tasks | `$RHAPSODY_REPO/examples/05-dask-backend.py` |
| Dask on a batch-scheduler cluster | `$RHAPSODY_REPO/examples/07-dask-backend-slrum-cluster.py` |
| Compute and AI tasks in one session | `$RHAPSODY_REPO/examples/03-workload-ai-hpc.py` |
| Notebooks | `$RHAPSODY_REPO/tutorials/` |
| How to run each example | `$RHAPSODY_REPO/examples/README.md` |

## Limits

- **Concurrent:** single host with no resource scheduler. Threads share the GIL, so use `--process` (a `ProcessPoolExecutor`) for CPU-bound Python.
- **Dragon:** must run under the `dragon` launcher. Multi-rank jobs need `pmi` (`"pmix"`, `"cray"` or `"none"`) and a `timeout`. Its working directory is set per task.
- **Dask:** an injected cluster or client must be asynchronous (`asynchronous=True`).
- **Session:** provides no dependency graph, no automatic retries and no checkpoint/restart. The program, or AsyncFlow, owns those.

## Composes with

- **AsyncFlow skills:** workflow graphs on top of these backends.
- **Other RHAPSODY skills,** when installed:
  - AI-HPC: `AITask` and inference backends;
  - telemetry: `session.start_telemetry(...)` before the first submit;
  - performance: sweeps and scaling.

## Troubleshooting

See [references/troubleshooting.md](references/troubleshooting.md). Turn on logs first:

```python
import logging, rhapsody
rhapsody.enable_logging(logging.DEBUG)   # call before creating backends
```
