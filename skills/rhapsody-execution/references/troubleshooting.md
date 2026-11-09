# Troubleshooting RHAPSODY execution

Before debugging anything else:

1. Run `python scripts/rhapsody_env.py`.
2. Enable logs **before** creating backends:
   ```python
   import logging, rhapsody
   rhapsody.enable_logging(logging.DEBUG)
   ```
3. Inspect the failed task:
   ```python
   print(t.uid, t.backend, t.state, t.exit_code, t.exception, t.stderr)
   ```

## Symptom → cause → fix

| Symptom | Likely cause | Fix |
|---|---|---|
| `ValueError: Backend '<x>' not registered` when creating `Session` | Backend object was created but not awaited | `backend = await SomeBackend(...)` before `Session(...)` |
| `RuntimeError: No task-executing backend configured in Session` | `Session()` without backends | `Session(backends=[backend])` or `session.add_backend(backend)` |
| `ValueError: Backend '<name>' requested by task ... not found` | `task.backend` names a backend not in the session | Use `backend.name` values from the session; check spelling |
| `TaskValidationError` at task creation | Both or neither of `executable`/`function`; `arguments` not a list; non-callable `function` | One of `executable=` or `function=`; `arguments=[...]` (list of str); `args=` tuple, `kwargs=` dict |
| `RuntimeError: Task ... has no bound future` on `await task` | Awaited before `submit_tasks` | Submit first, then await |
| Executable task `FAILED`, `exit_code` 127 / `FileNotFoundError` | Executable or input path does not exist on the worker host | Absolute paths valid on workers; set `cwd`; check the worker environment (`env` replaces it on Concurrent/Dask) |
| Executable picks up the wrong `python`, cannot import packages, or misses libraries/`HOME` only when `env` is set | `env` replaces the whole environment (Concurrent/Dask): `HOME`, the venv/conda/module `PATH` and `LD_LIBRARY_PATH` are dropped | `env={**os.environ, "VAR": "value"}` |
| Dragon: task ignores the requested cores/GPUs | `Policy` not passed through `process_template` / `process_templates`, or ids not from the node's topology | Put the policy in the template; take ids from `System().nodes` / `Node(...)` |
| `PicklingError` / `Can't pickle <lambda>` / `AttributeError: Can't get attribute` | Lambda, closure or `__main__`-only object sent to processes / Dask / Dragon | Define functions at module level in an importable module; guard the entry point with `if __name__ == "__main__":` |
| `ImportError: ProcessPoolExecutor requires 'cloudpickle'` | Missing dependency | `pip install cloudpickle` |
| CPU-bound function tasks don't speed up with more workers | Thread pool and the GIL | `ProcessPoolExecutor`, Dask workers, or Dragon |
| `asyncio.TimeoutError: Timeout after Ns: k/n tasks completed` | `wait_tasks(timeout=...)` expired | Raise the timeout; inspect states of unfinished tasks; check worker capacity |
| Program hangs, no task finishes | Backend has no capacity (zero workers, all blocked); Dask workers not connected; a Dragon MPI rank died | Check worker count / Dask dashboard (`client.dashboard_link`); give MPI jobs a `timeout` |
| Dask task fails at once: `No worker can satisfy resources {...}` | Requested `resources` not advertised by any worker | Start workers with `--resources "GPU=1"` (etc.) or drop the constraint |
| Dask: `TypeError`/`AttributeError` on results or shutdown with an injected cluster | Cluster/Client not asynchronous | Create it with `asynchronous=True` and enter it with `async with` (see example 07) |
| Dragon: script fails immediately / Dragon objects unavailable | Started with `python` | `dragon app.py`; call `mp.set_start_method("dragon")` first |
| Dragon: MPI job runs every rank as its own job, or all ranks crash | Wrong or missing PMI | `pmi="pmix"` (Open MPI/PMIx) or `"cray"` (Cray MPICH); `"none"` for non-MPI parallel processes |
| Dragon: `Unable to initialize PMIx server` | `libpmix.so` not on `LD_LIBRARY_PATH` | Add the directory containing `libpmix.so` |
| Dragon: `return_value` is `0` instead of the function's result | Function ran via `process_template(s)` | Use a plain `function=` task to get Python return values |
| Dragon: `stdout`/`stderr` empty | `batch_kwargs={"task_logs": False}` | Remove it, or use `capture_stdio=True` |
| `task.stdout` is a path, not text | `capture_stdio=True` | `open(task.stdout).read()` |
| `session.get_telemetry()` raises after the run | The session was closed | Keep the manager returned by `session.start_telemetry(...)` |

## Where to look next

- **Wait modes and exception types:** `$RHAPSODY_REPO/docs/getting-started/advanced-usage.md`.
- **Per-backend resources, MPI and GPU placement:** `$RHAPSODY_REPO/docs/getting-started/resource-specification.md`.
- **Machine-specific setup** (modules, network configuration, launch): `$RHAPSODY_REPO/docs/hpc-machines/`.
- **Behaviour in the installed version:** `$RHAPSODY_REPO/src/rhapsody/api/session.py` (submit/wait/close), `api/task.py` (task fields and validation), and `backends/execution/<backend>.py`.
- **Reporting a bug:** `https://github.com/radical-cybertools/rhapsody/issues`. Include the RHAPSODY version from `rhapsody_env.py`, the backend, a minimal script and the DEBUG log.
