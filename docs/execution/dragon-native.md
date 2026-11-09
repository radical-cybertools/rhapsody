# Dragon Native Backend

`DragonNativeExecutionBackend` (backend name `dragon_native`) is a second Dragon backend. It sits
beside `DragonExecutionBackend`, which goes through `dragon.workflows.batch`, and does not replace
it. It drives Dragon's `Process`, `ProcessGroup`, `Policy` and `Queue` directly, on `dragonhpc`
0.15.0.

Both take the same task descriptions, so a workflow switches by changing only the backend. Some
results and behaviour differ, though: a function with a `process_template` returns its value, not
its exit code, and MPI on Cray MPICH needs `pmi="cray"` here, among others. See
[How it differs from `DragonExecutionBackend`](#how-it-differs-from-dragonexecutionbackend).

```python
from rhapsody.backends import DragonNativeExecutionBackend

backend = await DragonNativeExecutionBackend()                      # one worker per physical core
backend = await DragonNativeExecutionBackend(workers_per_node=32)   # or a set number per node

async with Session(backends=[backend]) as session:                  # through RHAPSODY's API
    ...

flow = await WorkflowEngine.create(backend=backend)                 # or through AsyncFlow
```

Install it with the `dragon` extra, and start your script with the Dragon launcher, as for
`DragonExecutionBackend`: `dragon my_workflow.py`.

## How each task runs

| Task | Runs as | `return_value` |
|---|---|---|
| Function, no templates | a pool worker | the function's value |
| Function with `placement` (`{"node", "host", "gpus"}`), or a `process_template` holding only a `policy` with a host and/or GPUs | a free pool worker on that host; for GPUs, a worker started for that GPU set | the function's value |
| Function with any other `process_template` (`{}`, `cwd`, `env`, a CPU affinity, ...) | one native `Process` | the function's value |
| Executable | one native `Process` | the exit code (also `exit_code`); a non-zero one also sets `exception` to the `TaskExecutionError` a `Session` raises |
| `process_templates` (function or executable) | one `ProcessGroup`, with the task's or the backend's `pmi` (none by default) | the list of exit codes; `exit_code` is the first non-zero one |

- **The pool.** At start the backend queries `System().nodes` and starts, on every node,
  `workers_per_node` workers, by default one per physical core of `Node.cpus`: the CPUs the job has
  there, never `os.cpu_count()`. See [Choosing the pool size](#choosing-the-pool-size). Each worker
  is one native `Process` pinned to its host and its share of the node's CPUs, with its own inbox
  `Queue` on its node. A share is whole physical cores while there are at least as many cores as
  workers, so no two workers share a core. All workers put results on one `Queue` on the client's
  node.
- **Pinning.** A worker runs only on its share. With the default pool that share is one core, so a
  function that starts its own threads, as NumPy and OpenMP do, gets that core's hardware threads.
  Set a lower `workers_per_node` for such functions, or give them a `process_template` of their own.
- **Dispatch.** The client sends a task only to an idle worker; a task with no idle worker for its
  host waits in the backend, first in, first out. Unplaced function tasks fill the client's node
  first, where a round trip is cheapest. Unplaced process tasks get no policy, so Dragon spreads
  them round robin over all nodes; place them to keep them local. A task that asks for GPUs goes to
  a worker started, on demand, with `gpu_affinity` set to exactly those GPUs (`gpu_workers_per_set`
  of them, default 1), so Dragon sets the vendor's visible-devices variable once, at worker start.
  Workers stay up for the backend's life. On a host, tasks placed there go before unplaced ones.
- **Results.** A worker pickles its message with cloudpickle and puts the bytes on the result
  queue. One client thread blocks on that queue, drains what is already there, applies it under one
  lock, and hands the batch to the asyncio loop with `loop.call_soon_threadsafe`. The backend calls
  the callback only on the loop. Results travel inline on the queue, so they use Dragon's memory
  pool until the client takes them: on a full pool a worker waits up to 60 s for room, and a result
  that does not fit then, or could never fit, fails its task with Dragon's error. Sending a task
  waits the same way: on a full pool the client holds the task and its worker, tries again with a
  backoff, and fails the task only after 60 s. A send that fails for any other reason replaces the
  worker, and the task goes to another; it fails only if that send fails too. The pool is 4 GiB
  per node by default (`DRAGON_DEFAULT_SEG_SZ`), so workers that finish together should return much
  less than that between them; write larger results to files.
- **Process tasks.** One thread per running process or group task starts it and joins it. At most
  `max_process_ranks` processes run at once (default: the runtime's CPUs); the rest wait, strictly
  first in, first out, so a large job at the head of the queue holds back smaller tasks behind it.
  A task that needs more than the limit runs alone. The limit counts only these tasks, over all
  nodes, not the pool: a node can hold its pool and as many ranks again.
- **Output.** With `capture_stdio`, output goes to `<work_dir>/<uid>.stdout` and `.stderr`, and
  those paths come back, as with `DragonExecutionBackend`. The files are written on the task's
  node, and the directory is made there if it is missing, so on a work directory the nodes don't
  share, the paths name files on that node. Without `capture_stdio`, the text comes back in
  `stdout` and `stderr`:
    - a pool worker points descriptors 1 and 2 at two unnamed files on its node for the task, and
      sends their text with the result;
    - a process task writes to files in `<work_dir>/.dragon_native.<id>/`, which the client reads
      and deletes after the task. This needs the work directory on a file system the nodes share;
    - `inline_stdio=False` sends that output to the console instead;
    - a `stdout` or `stderr` in a task's own template takes precedence, and `capture_stdio` is then
      skipped.

!!! note "Open files on nodes with many CPUs"
    Local Services holds about three file descriptors for each Python process on its node and one
    for each executable, and Dragon does not raise the soft limit. At a soft limit of 1,024, a node
    has room for about 320 workers, or fewer with process tasks running beside them. Past the limit,
    the pool fails to start, or a process task fails, with `Too many open files` at the end of a long
    error. Raise the limit with `ulimit -n` before starting Dragon; the backend logs a warning when
    a node may not fit.

!!! warning "Functions that wait on a Dragon process"
    With `dragonhpc` 0.15.0, a process killed while it waits in `Process.join()` aborts the whole
    Dragon runtime when the process it waits for ends. So cancelling a task, or giving it a
    `timeout` that it reaches, aborts the runtime if its function has started a Dragon `Process` and
    is waiting for it, in the pool or with a `process_template`. Submit that child as a task of its
    own instead, and let RHAPSODY wait for it.

## Choosing the pool size

The default is one worker per physical core of the job on each node, so that no two workers share a
core: 32 workers on a node with 32 cores and 64 hardware threads, 128 on a Perlmutter CPU node or a
Bridges-2 RM node. It is a default, not a limit: `workers_per_node` takes any number, above or below
it.

The best size depends on how long tasks run and what they do. Here are tasks per second on one node
with 32 cores and 64 hardware threads, for functions that do nothing or sleep. Each range covers the
waves of tasks in one run per setting. These runs carried timing code; plain runs of the first row
gave about 15,500 and about 4,100 tasks/s.

| Task | 32 workers | 64 workers |
|---|---|---|
| Nothing | 8,700–10,200 | 2,100–2,400 |
| Sleep 1 ms | 7,690–8,768 | 2,928–6,275 |
| Sleep 5 ms | 4,170–4,284 | 2,658–3,226 |
| Sleep 20 ms | 1,417–1,440 | 2,041–2,286 |

- **Many tasks under about 10 ms:** fewer workers running at once can do better, even fewer than
  the cores. The backend limits them itself, as the next section says. Bridges-2 RM nodes have no
  hardware threads, so the default fills them. On two of those nodes, before that limit, tasks
  that did nothing ran at about 13,100 tasks/s with 32 workers per node, and about 3,800 with 128.
- **Tasks that sleep or wait, about 20 ms or longer:** more workers than cores did better, as the
  table shows.
- **CPU-bound tasks:** not measured.

Short tasks suffer because each one allocates and frees its messages in the node's Dragon memory
pool, and every process on the node shares that pool's lock. On the node above, each of those calls
took microseconds with 32 workers and up to about 1 ms with 64. Contention on that lock is our
reading of these timings, not something we traced directly.

### Short tasks run on fewer workers

What collapses the rate is how many workers run at once, not how many there are: a worker waiting for
a task makes no calls on the pool. So while most of a node's recent tasks are short, the backend runs
at most `short_task_workers` of that node's workers at once, 32 by default. Its other tasks wait for
one of those workers, or go to another node with room. A task is short when its function ran for less
than 10 ms, and a node's tasks are short while more than half of its last 64 were. Once they run
longer, every worker runs again. So does a node that holds tasks back and finishes none for a second:
its running tasks are long after all, and tasks that wait on tasks held behind them would otherwise
wait for good.

On the node above, with a pool of 64:

| Workers running at once | Nothing (tasks/s) | Sleep 5 ms (tasks/s) |
|---|---|---|
| all 64 | 2,228–2,340 | 3,173–3,273 |
| at most 32 | 8,918–8,960 | 3,554–4,161 |
| at most 48 | 7,484–10,601 | 4,468–5,294 |
| a pool of 32, for comparison | 10,045–10,567 | 4,332–4,389 |

A node with 32 workers or fewer never reaches the default limit. The limit of 32 comes from this node
and from the 32 workers per node that did well on Bridges-2 RM and on a 16-node cluster. It hasn't
been measured on a node with more workers. `short_task_workers=None` turns the limit off.

## MPI jobs: PMI and timeout

A task with `process_templates` runs as one `ProcessGroup`. Unlike a Batch job, which gets Batch's
default `PMIBackend.CRAY`, a group has no PMI unless one is chosen. Without one, MPI ranks never
wire up: with Cray MPICH they hang in `MPI_Init`, and with Open MPI each may run as a job of one
rank. Choose the PMI for every group on the backend, or per task (the task's choice wins):

```python
backend = await DragonNativeExecutionBackend(pmi="pmix")  # Open MPI and other PMIx-based MPIs

task = ComputeTask(
    executable="./mpi_app",
    task_backend_specific_kwargs={
        "process_templates": [(16, {})],
        "pmi": "cray",     # or "pmix", or "none" for jobs of non-MPI processes
        "timeout": 600,    # seconds
    },
)
```

The values are `"pmix"`, `"cray"`, `"none"` and a Dragon `PMIBackend`. An unknown name raises
`ValueError` on the backend, before anything starts, and fails only its own task on a task. Tasks
with `process_template`, or with no template, run without a PMI, and ignore `pmi`.

When neither sets one, the first multi-rank job logs a warning. Under Cray MPICH
(`CRAY_MPICH_DIR` or `CRAY_MPICH_VERSION` set) a one-rank job logs it too. Give MPI jobs a `timeout`
as well: when a rank dies, Dragon does not end the group, and the other ranks wait for it. The
first multi-rank job without one logs a warning. RHAPSODY can't tell whether a job's processes use
MPI, so set `"pmi": "none"` for jobs that don't: it logs neither warning.

!!! note "PMIx needs `libpmix.so`"
    Dragon loads PMIx with `dlopen("libpmix.so")`, so a directory holding that name must be on
    `LD_LIBRARY_PATH`; some distributions ship it only in their PMIx development package. When it
    can't be loaded, the whole Dragon runtime aborts.

!!! warning "PMIx groups across nodes"
    With `dragonhpc` 0.15.0, PMIx groups that span nodes can fail or hang at startup, with or without
    RHAPSODY. Dragon's dictionary client keys each request on an index that nothing sets, so two
    requests can collide, and a PMIx fence then fails. Ranks log
    `Failed to receive UCX worker address`, or wait with no output. On Bridges-2 (Open MPI 5.0.8,
    HSTA), about 3% of two-node groups failed this way, and about 13% beside heavy queue traffic, more
    often when groups started within a fraction of a second of each other. Give these tasks a
    `timeout`, and resubmit the ones that fail.

    In one run of about 180 MPI tasks on Bridges-2, the whole runtime was lost at about the time a
    hung task's timeout ended. The cause of that isn't known, and it hasn't been tied to this bug.

!!! warning "Cray PMI and Python functions"
    With `dragonhpc` 0.15.0, a rank that runs a Python function under Cray PMI can abort at exit,
    after the function has done its work and whether or not it uses MPI. Run MPI from Python as an
    executable (`python script.py` with `process_templates`), and give function jobs that are not
    MPI `"pmi": "none"`.

## How it differs from `DragonExecutionBackend`

- **No Batch.** No Batch manager loop, no results DDict, no polling. A completion reaches the
  asyncio loop as soon as the result queue delivers it.
- **Placement is the caller's.** A task's `placement` is honoured: placed functions run in the pool,
  on a worker of that host. `DragonExecutionBackend` ignores it.
- **Sized from the job.** The pool and the process limit come from `Node.cpus`, so a partial-node
  job is not oversubscribed. The default pool has one pinned worker per physical core;
  `DragonExecutionBackend` defaults to half the CPUs, unpinned.
- **Functions with a `process_template` return their value**, through the result queue, and a
  failing one sets `exception` to its own exception. `DragonExecutionBackend` returns the exit code.
- **Groups have no PMI by default.** A `process_templates` group starts with the task's or the
  backend's `pmi`, and with none when neither sets one, where a Batch job has a default of its own.
  MPI on Cray MPICH needs `pmi="cray"`: see [MPI jobs](#mpi-jobs-pmi-and-timeout).
- **Executables start one process, not two.** A Batch process task builds a `ProcessGroup`, whose
  manager is a Python process that imports Dragon. Here an executable is one native `Process`,
  started through `sh -c 'exec "$@" >>out 2>>err'`.
- **Output stays out of Local Services' pipes.** With inline or captured output, process tasks get
  `stdout=DEVNULL` and redirect themselves to files, so a background child holding a task's output
  cannot stall Local Services for the whole node. `inline_stdio=False` and a template's own
  `stdout` do go through them.
- **Cancel and timeout stop the work.** A pool task's worker gets SIGKILL and is replaced, so a
  function stuck in a C call stops at once. Process tasks get SIGTERM, then SIGKILL after
  `kill_grace`, sent per puid: Dragon 0.15.0's `ProcessGroup.stop()` never sends its SIGKILL.
  Dragon signals only a task's own process, so every process a task starts carries
  `RHAPSODY_DRAGON_NATIVE_TASK=<token>` in its environment, and after the kill a short `sh` reaper
  on each of the task's hosts kills whatever still carries it.
- **A timeout is a `TimeoutError`**, for functions and processes alike, and the task is FAILED. Its
  clock starts when the task starts running, not when it is submitted. A `timeout` of zero or less,
  and a `process_templates` entry with no processes, fail the task before it reaches Dragon.
- **Cancel and shutdown.** `cancel_task` reports CANCELED at once, the kill follows in the
  background, and it returns False for an unknown or finished task, as the Concurrent and Dask
  backends do. `shutdown()` kills what still runs at once and reports it CANCELED, where
  `DragonExecutionBackend` gives tasks up to 10 s to end.
- **Bad placements fail only their task.** Hosts and GPU ids in a `placement` or a `policy` are
  checked against Dragon's nodes before anything is submitted: Dragon aborts the whole runtime on
  an unknown host and silently drops unknown GPU ids.
- **Dead workers are noticed.** A worker that dies on its own fails its task with the exit code,
  and is replaced (once: a replacement that dies before it is ready is not replaced again).
- **The garbage collector.** Dragon's `gs_request` turns Python's garbage collector off at the start
  of every Global Services request and back on at the end of any, for the whole process, so a
  thread blocked in `Process.join()` keeps it off until another request ends. The worker watcher
  therefore checks with a non-blocking `multi_join` once a second, which also turns the collector
  back on while process-task threads block in `join()`. With `workers_per_node=0` there is no
  watcher to do that.
- **Not here.** `wait()`, `create_ddict()` and `.batch` belong to `DragonExecutionBackend`. Before
  the backend starts, `topology()` returns an empty list.

## Telemetry

Task events, metrics and spans come from the `Session`, as for every backend. Node resources come
from the same `DragonTelemetryAdapter` as for `DragonExecutionBackend`: it starts its own process
group on the runtime's nodes and does not use Batch. See [Telemetry](../telemetry/index.md).

## What it does not do yet

- **A result store.** Results travel inline on the queue; there is no DDict for large values.
- **Per-task isolation in the pool.** As with Batch's workers, tasks in one worker share its
  interpreter: imported modules and global state persist between tasks. The backend restores the
  working directory and its token variable after each task, nothing else.
- **Data linking.** `link_implicit_data_deps` and `link_explicit_data_deps` are not implemented, as
  in `DragonExecutionBackend`.
- **Resource usage per task** (peak memory, CPU time).
- **Scheduling beyond "a free worker of this host".** No priorities, no per-node limit for process
  tasks (the limit is global), no worker recycling, no memory limits.
- **A CPU layout per node.** Physical cores are read from the client's node. A node numbered
  differently is grouped less well: it gets valid CPUs, but its workers may share cores, and its
  default pool may not have one worker per core.

## Configuration

| Argument | Default | Meaning |
|---|---|---|
| `workers_per_node` | one per physical core | pool workers per node, each pinned to its share of the node's CPUs |
| `short_task_workers` | 32 | pool workers per node that run at once while its tasks are short; `None` for all |
| `gpu_workers_per_set` | 1 | workers started on demand for each host and GPU set |
| `max_process_ranks` | the runtime's CPUs | processes of process and group tasks that may run at once |
| `inline_stdio` | `True` | return the output of tasks without `capture_stdio` as text |
| `kill_grace` | 2.0 s | SIGTERM to SIGKILL for process tasks |
| `worker_start_timeout` | 300 s | time allowed for the pool to start |
| `pmi` | `None` | PMI of every `process_templates` group whose task sets none |

`num_workers` is the pool's size over all nodes. `topology()` lists each node's CPUs and GPUs.

## Tests

`tests/unit/test_backend_execution_dragon_native.py` runs the backend against a fake Dragon
(threads for Python processes, real `/bin/sh` for executables), so it needs no Dragon runtime. Its
last test needs one:

```bash
dragon python -m pytest tests/unit/test_backend_execution_dragon_native.py
```
