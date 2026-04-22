# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

RHAPSODY (Runtime for Heterogeneous Applications, Service Orchestration and Dynamism) — a Python runtime for executing heterogeneous HPC-AI workflows with dynamic task graphs. Developed by the RADICAL Research Group at Rutgers University.

## Common Commands

```bash
# Install for development
pip install -e ".[dev]"

# Lint (auto-fix) and format
make lint        # ruff check . --fix
make format      # ruff format .

# Lint/format check only (no changes)
make lint-check
make format-check

# Run all tests quickly (unit + integration, no Dragon)
make test-quick

# Run unit tests (excluding Dragon)
make test-unit-no-dragon
# equivalent to: pytest tests/unit/ --ignore=tests/unit/test_backend_execution_dragon.py -xvs

# Run a single test file / test function
pytest tests/unit/test_backend_base.py -xvs
pytest tests/unit/test_backend_base.py::test_function_name -xvs

# Run specific backend tests
make test-concurrent
make test-dask
make test-rp
make test-constants

# Run integration tests
make test-integration

# Full tox run (all Python versions)
tox
```

## Code Style

- **Ruff** is the sole linter and formatter. Line length: 100. Target: Python 3.9+.
- Pre-commit hooks are mandatory (`pre-commit install`).
- Async mode: `asyncio_mode = "auto"` in pytest — async tests run automatically.

## Architecture

The codebase lives under `src/rhapsody/` with two main subsystems:

### Execution Backends (`backends/`)
- **`base.py`** — `BaseExecutionBackend` ABC defining the interface: `submit_tasks()`, `shutdown()`, `wait_tasks()`, `build_task()`, state callbacks, data dependency linking.
- **`constants.py`** — Canonical state enums (`BackendMainStates`, `TasksMainStates`) and `StateMapper` for translating between backend-specific and canonical states.
- **`discovery.py`** — `BackendRegistry` with dynamic backend discovery and `get_backend()` factory. Backend names are auto-derived from CamelCase class names to snake_case.
- **`execution/`** — Concrete backends: `ConcurrentExecutionBackend` (built-in), `DaskParallelExecutionBackend`, `RadicalPilotExecutionBackend`, `DragonExecutionBackendV1/V2/V3`. Optional backends (Dask, RP, Dragon) gracefully handle missing imports.
- **`inference/vllm.py`** — vLLM inference backend.

### Resource Manager (`resource_manager/`)
- **`base.py`** — `ResourceManager` ABC, `RMConfig` dataclass, `RMInfo` (nodes/cores/GPUs/memory), `Node` info.
- Platform implementations: SLURM, PBS Pro, Cobalt, LSF, Torque, Fork.

### Public API (`__init__.py`)
Exports: `get_backend()`, `discover_backends()`, `BackendRegistry`, `enable_logging()`, `ResourceManager`, `RMConfig`, `RMInfo`.

## Testing

- Tests in `tests/unit/` and `tests/integration/`.
- Shared fixtures in `tests/conftest.py` (session, backend instances, sample tasks, mock callbacks, temp directories).
- Markers: `slow`, `radical_pilot`.
- Coverage configured via tox: `--cov=rhapsody`.
- Dragon tests are separated because they require the Dragon runtime; most dev work uses `test-unit-no-dragon` or `test-quick`.
