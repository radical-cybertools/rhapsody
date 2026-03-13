# Changelog

## [Unreleased]

### Added

- `DaskExecutionBackend` now supports all three `ComputeTask` types: sync functions, async functions, and executable tasks (via subprocess inside a Dask worker).
- Cluster-agnostic design: pass `cluster=` or `client=` to target any Dask-compatible cluster (SLURM, Kubernetes, LocalCUDACluster, etc.) without changing task code.
- Resource scheduling via `task_backend_specific_kwargs={"resources": {"GPU": 1}}` routes tasks to workers advertising matching Dask resources.
- `shell=True` for executable tasks via `task_backend_specific_kwargs={"shell": True}`.
- `DaskExecutionBackend._check_resources_satisfiable()`: pre-submit check that immediately fails tasks with unsatisfiable resource constraints instead of hanging indefinitely. Function tasks set `task.exception`; executable tasks set `task.stderr` and `task.exit_code = 1`.
- Integration tests for Dask backend: end-to-end sync/async/executable task execution, cluster injection (cluster and client), and resource constraint failure behavior.

### Changed

- `DaskExecutionBackend`: resource constraints are now specified exclusively via `task_backend_specific_kwargs={"resources": {...}}` (previously via `gpu=` / `cpu_threads=` on `ComputeTask`).
- `DaskExecutionBackend._build_dask_resources()`: reads from `task_backend_specific_kwargs["resources"]` instead of task-level `gpu` / `cpu_threads` fields.
- `DaskExecutionBackend._submit_executable()`: `shell=` now reads from `task_backend_specific_kwargs` for consistency.
- Optimized Dragon backend tests by using module-scoped fixtures, reusing a single backend instance per Dragon version (V1, V2, V3) across all tests instead of creating/destroying one per test.

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
