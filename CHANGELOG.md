# Changelog

## [Unreleased]

### Added

- `DragonExecutionBackendV3`: migrated to the Dragon batch.py streaming pipeline. Tasks submitted via `session.submit_tasks()` are dispatched individually by a continuously running background thread — there is no compile or start step.
- `DragonExecutionBackendV3`: accepts two new constructor parameters: `num_nodes` (total nodes) and `pool_nodes` (nodes per worker pool), forwarded directly to `Batch()`.
- `DragonExecutionBackendV3.fence()`: new method that delegates to `batch.fence()`, allowing callers to wait for all in-flight tasks submitted by this client to complete.
- `DragonExecutionBackendV3.create_ddict()`: new helper that creates a Dragon `DDict` instance directly from the backend.
- `DragonExecutionBackendV3._deliver_result()` / `_deliver_failure()`: internal helpers that centralise result callback dispatch and cancellation checks, eliminating duplicated logic in the monitor loop. Both now expose `stdout`, `stderr`, and the full Dragon traceback string to the task description so callers can inspect execution output without polling side-effects.
- `DragonExecutionBackendV3` result monitoring reads directly from `Batch.results_ddict` (the same distributed dict Dragon workers write to) instead of going through the `Task` object.
- `DragonExecutionBackendV3._cancelled_tasks` converted from `list` to `set`: O(1) membership checks and automatic deduplication. Cancelled UIDs are now removed from the set once the monitor loop processes the task, preventing unbounded growth.
- `DragonExecutionBackendV3._task_registry` entries are now removed atomically (via `dict.pop`) on result or failure delivery, eliminating unbounded registry growth for long-running sessions.
- `ConcurrentExecutionBackend`: regular (synchronous) functions are now executed correctly in both `ThreadPoolExecutor` and `ProcessPoolExecutor`. Previously, all function tasks were dispatched via `asyncio.run(func(...))`, which raised `ValueError` for non-coroutine callables. The executor now detects `asyncio.iscoroutinefunction` and calls sync functions directly.
- `Session` / `TaskStateManager`: task futures now propagate exceptions on failure. Previously all futures were resolved with `set_result(task)` regardless of outcome. Failures now resolve as:
    - Function task raises → original exception propagated via `fut.set_exception(exc)`.
    - Executable task returns non-zero exit code → `fut.set_exception(TaskExecutionError(uid, stderr, exit_code))`.
    - Successful tasks → `fut.set_result(task)` (unchanged).
- `Session.wait_tasks`: replaced bare `except Exception` (which silently swallowed all errors) with `asyncio.gather(*futures, return_exceptions=True)`. Task failures no longer propagate through `wait_tasks`; callers inspect `task.state` / `task.exception` / `task.stderr` directly.
- API documentation: new **Wait Modes** reference in `docs/api/index.md` with a comparison table of `wait_tasks`, `asyncio.gather(*futures)`, and `await future / await task`, including exception types and when to use each.
- API documentation: new **Callable Task API** section documenting sync and async function support, executor behaviour, and result access fields.
- Getting-started guide: new **Wait Modes** and **Sync and Async Callable Tasks** sections added to `docs/getting-started/advanced-usage.md` with runnable code examples.
- `ConcurrentExecutionBackend`: executable tasks now honour per-task working directory via `task.cwd` (task-level) or `task_backend_specific_kwargs={"cwd": "..."}` (overrides task-level); passed as `cwd=` to `asyncio.create_subprocess_exec` / `asyncio.create_subprocess_shell`.
- `DaskExecutionBackend`: executable tasks now honour `cwd` from `task_backend_specific_kwargs` (overrides task-level `task.cwd`); forwarded to `_run_executable` which passes it as `cwd=` to `subprocess.run`.
- `DragonExecutionBackendV3`: documented that per-task working directory must be set via `task_backend_specific_kwargs={"process_template": {"cwd": "..."}}` (or `"process_templates"` for MPI jobs); this is the only supported mechanism in V3.
- `ConcurrentExecutionBackend`: executable tasks now honour `env` from `task_backend_specific_kwargs={"env": {...}}`; passed as `env=` to `asyncio.create_subprocess_exec` / `asyncio.create_subprocess_shell`. `None` (default) inherits the parent process environment.
- Unit tests for `cwd` and `env` behaviour across `ConcurrentExecutionBackend`, `DaskExecutionBackend`, and `DragonExecutionBackendV3`.
- `DaskExecutionBackend` now supports all three `ComputeTask` types: sync functions, async functions, and executable tasks (via subprocess inside a Dask worker).
- Cluster-agnostic design: pass `cluster=` or `client=` to target any Dask-compatible cluster (SLURM, Kubernetes, LocalCUDACluster, etc.) without changing task code.
- Resource scheduling via `task_backend_specific_kwargs={"resources": {"GPU": 1}}` routes tasks to workers advertising matching Dask resources.
- `shell=True` for executable tasks via `task_backend_specific_kwargs={"shell": True}`.
- `DaskExecutionBackend._check_resources_satisfiable()`: pre-submit check that immediately fails tasks with unsatisfiable resource constraints instead of hanging indefinitely. Function tasks set `task.exception`; executable tasks set `task.stderr` and `task.exit_code = 1`.
- Integration tests for Dask backend: end-to-end sync/async/executable task execution, cluster injection (cluster and client), and resource constraint failure behavior.

### Fixed

- `ConcurrentExecutionBackend._run_in_thread`: calling a regular (sync) function no longer raises `ValueError: a coroutine was expected`.
- `ConcurrentExecutionBackend._run_in_process`: same fix — sync functions are called directly after cloudpickle deserialization.
- `TaskStateManager._update_task_impl`: futures for failed tasks are now resolved with `set_exception` instead of `set_result`, so `await future` and `await asyncio.gather(*futures)` correctly raise on task failure.
- `TaskStateManager.get_wait_future`: same fix applied to the race-condition path (task already completed before `get_wait_future` was called).
- `Session.wait_tasks`: removed `except Exception: logger.error(...)` which was silently eating all non-timeout exceptions including programming errors.

### Changed

- `ComputeTask`: renamed `working_directory` parameter and dict key to `cwd` to distinguish task-level working directory from backend-level working directory (used by Dragon V1/V2 via `resources["working_dir"]`). Update call sites: `ComputeTask(cwd="/path")` and `task["cwd"]`.
- `DragonExecutionBackendV3`: removed the silently-ignored `working_directory` parameter from `__init__` and `create()` — it was never stored or used (unlike V1/V2 which honour `resources["working_dir"]`). Use `task_backend_specific_kwargs={"process_template": {"cwd": "..."}}` instead.
- `DaskExecutionBackend`: resource constraints are now specified exclusively via `task_backend_specific_kwargs={"resources": {...}}` (previously via `gpu=` / `cpu_threads=` on `ComputeTask`).
- `DaskExecutionBackend._build_dask_resources()`: reads from `task_backend_specific_kwargs["resources"]` instead of task-level `gpu` / `cpu_threads` fields.
- `DaskExecutionBackend._submit_executable()`: `shell=` now reads from `task_backend_specific_kwargs` for consistency.
- Optimized Dragon backend tests by using module-scoped fixtures, reusing a single backend instance per Dragon version (V1, V2, V3) across all tests instead of creating/destroying one per test.

### Breaking Changes

- **`DragonExecutionBackendV3`**: removed `num_workers`, `disable_background_batching`, and `disable_batch_submission` constructor parameters. These were incompatible with the new streaming pipeline — the Dragon batch always runs in streaming mode and worker counts are controlled via `num_nodes` / `pool_nodes`. Migration:
    - `DragonExecutionBackendV3(num_workers=16)` → `DragonExecutionBackendV3(num_nodes=4, pool_nodes=2)` (or simply omit both to let Dragon decide)
    - `DragonExecutionBackendV3(disable_batch_submission=True)` → remove (streaming is now always on)
    - `DragonExecutionBackendV3(disable_background_batching=True)` → remove
- **`ComputeTask` and `AITask`**: removed `ranks`, `memory`, `gpu`, `cpu_threads`, and `environment` parameters. These fields were never consumed by any execution backend — they were silently ignored, creating a misleading API. Resource requirements are now backend-specific and must be passed via `task_backend_specific_kwargs`. Migration:
  - `ComputeTask(executable=..., gpu=2)` → `ComputeTask(executable=..., task_backend_specific_kwargs={"resources": {"GPU": 2}})` (Dask) or `task_backend_specific_kwargs={"process_template": {"gpu": 2}}` (Dragon V3)
  - `ComputeTask(executable=..., environment={"K": "V"})` → `ComputeTask(executable=..., task_backend_specific_kwargs={"env": {"K": "V"}})` (Concurrent/Dask)
- **`ComputeTask`**: removed `cwd` and `shell` as named parameters. All execution context is now exclusively specified via `task_backend_specific_kwargs` for consistency. Migration:
  - `ComputeTask(executable=..., cwd="/path")` → `ComputeTask(executable=..., task_backend_specific_kwargs={"cwd": "/path"})`
  - `ComputeTask(executable=..., shell=True)` → `ComputeTask(executable=..., task_backend_specific_kwargs={"shell": True})`

### Fixed

- `_run_executable`: replaced `stdout=subprocess.PIPE, stderr=subprocess.PIPE` with `capture_output=True` (ruff UP022).

## [0.1.2] - 2026-02-16

### Fixed

- Fixed project URLs in `pyproject.toml` to point to the correct GitHub repository.
- Removed Python 3.8 classifier (package requires Python >= 3.9).

## [0.1.1] - 2026-02-16

### Fixed

- Fixed `ImportError` when installing `rhapsody-py` without optional backends (Dragon, Dask, RADICAL-Pilot). Optional backend imports in `backends/__init__.py` are now wrapped in `try/except`.

## [0.1.0] - 2025-02-16

### Added

- Initial PyPI release as `rhapsody-py` (import name: `rhapsody`).
- Execution backends: Concurrent (built-in), Dask, RADICAL-Pilot, Dragon (V1, V2, V3).
- Inference backend: Dragon-VLLM for LLM serving on HPC.
- Task API: `ComputeTask` for executables/functions, `AITask` for LLM inference.
- Session API for task submission, monitoring, and lifecycle management.
- Backend discovery and registry system.
- Removed ThreadPoolExecutor wait approach (`_wait_executor`), streamlining concurrency management.
- Added `disable_batch_submission` parameter to `DragonExecutionBackendV3` for configurable task submission strategy.
- Introduced polling-based batch monitoring with `_monitor_loop` and `_process_batch_results`.
