# Dragon CI tests

Standalone pre-release contract tests for the Dragon runtime. The suite has
**no dependency on Rhapsody** — only `dragon`, `pytest`, `pytest-asyncio`,
`pytest-timeout`, `cloudpickle`, and the standard library. Its purpose is to
fail loudly when a Dragon release breaks any API surface that Rhapsody (or
similar downstream stacks built on the V3 `Batch` workflow) depends on.

## What's covered

Each test pins one observable contract: symbol name, callable signature,
attribute presence, or a short behavioral round-trip. Failures should point
the Dragon team at the specific contract that changed.

**Happy-path contract files** (expected to pass):

| File                            | Subsystem                                            |
|---------------------------------|------------------------------------------------------|
| `test_batch_lifecycle.py`       | `Batch` ctor kwargs, attrs, close/join/terminate    |
| `test_batch_function.py`        | `Batch.function()` + `results_ddict` 5-tuple shape  |
| `test_batch_process.py`         | `Batch.process()` + stdio-capture pitfalls          |
| `test_batch_job.py`             | `Batch.job()` (wire + opt-in real MPI launch)       |
| `test_process_template.py`      | `ProcessTemplate` ctor, attrs, `Popen` consts       |
| `test_policy.py`                | `Policy` ctor, `Distribution`/`Placement` enums     |
| `test_machine_system.py`        | `System().nnodes`, `hostname_policies`              |
| `test_process_group.py`         | native `ProcessGroup` lifecycle                     |
| `test_queue_event.py`           | `native.queue.Queue` + `native.event.Event`         |
| `test_ddict.py`                 | `DDict` core ops                                    |
| `test_telemetry_collector.py`   | `dragon.telemetry.collector` + `AccVendor`          |
| `test_async_in_batch.py`        | async-coroutine wrapping pattern (V3)               |
| `test_worker_failure_modes.py`  | caught-exception and `sys.exit` reporting paths     |

**Known-Dragon-bug pins** (expected to FAIL via pytest-timeout until Dragon fixes them):

| File                                       | Bug pinned                                                |
|--------------------------------------------|-----------------------------------------------------------|
| `test_ddict_unknown_key_blocks.py`         | `DDict[unknown_key]` blocks instead of raising `KeyError` when `wait_for_keys=True` |
| `test_sigkill_in_worker_hangs.py`          | SIGKILL'd worker is undetected; `task.get()` blocks forever; `timeout=` kwarg is ignored on the blocking DDict read |
| `test_unpickleable_function_hangs.py`      | Worker dies during cloudpickle unpickling without diagnostic — same silent-hang as SIGKILL |

These three live in their own files because `pytest-timeout`'s `method="thread"`
leaks the wedged test thread, which holds Dragon resources and breaks any
subsequent test in the same pytest invocation. One bug per file keeps things
clean.

## Running

The suite must run under the Dragon launcher because Dragon's multiprocessing
backend is required even for in-process tests.

```
dragon python -m pytest -c tests/dragon_ci/pytest.ini \
    --rootdir=tests/dragon_ci tests/dragon_ci/ -v
```

The `-c` and `--rootdir` flags keep pytest from picking up the parent
`tests/conftest.py` (which imports Rhapsody).

## Skip markers

Tests auto-skip what the environment can't support:

| Marker                          | Active when                                            |
|---------------------------------|--------------------------------------------------------|
| `requires_multi_node`           | `System().nnodes >= 2`                                 |
| `requires_gpu`                  | `identify_gpu()` reports at least one device          |
| `requires_mpi`                  | `DRAGON_CI_HAS_PMI=1` is set (opt-in)                  |
| `requires_telemetry_collector`  | `dragon.telemetry.collector` is importable             |

MPI launches are opt-in because a failing PMIx init aborts the entire Dragon
runtime — not just the calling test — and so cannot be probed safely.

## Adding tests

- One contract per test. Prefer parametrized signature/attribute checks for
  enumerations of kwargs or methods.
- For callables passed to `Batch.function()` / `ProcessTemplate(target=…)`,
  define the function in `_dragon_ci_helpers.py`, **not** in the test file.
  Top-level callables defined in `test_*.py` files cannot be re-imported by
  Dragon workers (pytest module names aren't on `PYTHONPATH`); the worker
  fails silently to unpickle and `task.get()` blocks forever. The conftest
  injects this directory into `PYTHONPATH` so the helpers module IS visible
  to workers.
- Use the `batch` session fixture when possible. Use `fresh_batch` only for
  tests that intentionally kill workers — those leave the Batch unrecoverable.
- New "documents a Dragon bug" tests should each live in their own file,
  use `@pytest.mark.timeout(N, method="thread")`, and contain only the one
  test (see the existing `*_hangs.py` / `*_blocks.py` files as templates).
- Do **not** import `rhapsody` from anywhere in this directory.
