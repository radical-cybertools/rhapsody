# RHAPSODY HPC Integration Tests — Dragon Backend

End-to-end integration tests for **RHAPSODY + DragonHPC** on multi-node,
GPU-enabled HPC systems. These tests exercise the full stack: RHAPSODY's
session and backend layer, Dragon's Batch scheduler, DDict result transport,
process template placement, and MPI job orchestration.

This directory is part of a broader `test-hpc/` structure designed to support
multiple HPC execution backends side by side:

```
tests/integration/test-hpc/
├── conftest.py      ← CI gate: entire subtree skipped unless RHAPSODY_HPC=1
├── run_suite.py     ← runs the test groups, writes one JUnit file
├── benchmarks/      ← measurements with no pass/fail, not collected by pytest
├── dragon/          ← this directory (DragonHPC backend)
└── flux/            ← planned (Flux backend, same categories)
```

---

## Prerequisites

| Requirement | Details |
|-------------|---------|
| Python | 3.11 – 3.13 (Dragon constraint) |
| DragonHPC | 0.15.0 (the version pinned by the `dragon` extra) |
| RHAPSODY | installed from source (`pip install -e .`) |
| mpi4py | required only for MPI tests (`pip install mpi4py`) |
| Active allocation | Dragon global services must be running (`dragon` launcher) |

---

## CI behaviour

These tests are **never collected or run in GitHub Actions**. A parent-level
`conftest.py` at `tests/integration/test-hpc/conftest.py` uses the
`pytest_ignore_collect` hook to skip the entire subtree unless the environment
variable `RHAPSODY_HPC=1` is set. No Makefile or CI YAML changes are needed.

```
make test-ci        # RHAPSODY_HPC unset → test-hpc/ silently skipped
make test-regular   # same
```

---

## Running on an HPC cluster

Inside an allocation of at least 2 nodes, from the repository root:

```bash
source /path/to/your/venv/bin/activate

python3 tests/integration/test-hpc/run_suite.py --backend dragon --junit results/junit.xml \
    -- -w slurm -t tcp
```

Everything after `--` goes to the `dragon` launcher, so machine-specific settings stay in the
job script. `run_suite.py` sets `RHAPSODY_HPC=1` and `PYTHONPATH`.

Useful options:

```bash
--groups tasks,placement   # run only these groups
--budget mpi=600           # change a group's time budget, in seconds
```

To run one file by hand, without the runner:

```bash
export RHAPSODY_HPC=1
export PYTHONPATH=$PWD/tests/integration/test-hpc/dragon${PYTHONPATH:+:$PYTHONPATH}
dragon python3 -m pytest tests/integration/test-hpc/dragon/test_placement.py -v
dragon python3 -m pytest tests/integration/test-hpc/dragon/test_scale.py -m scale -v -s
```

---

## Test groups

Each group runs under its own `dragon` launch and time budget, cheapest first. If one crashes or
hangs, the later groups still run.

| Group | File | Tests | Budget | Covers |
|-------|------|-------|--------|--------|
| `tasks` | `test_tasks.py` | 4 | 3 min | Function return values, executables, concurrent submission |
| `placement` | `test_placement.py` | 5 | 3 min | Node pinning and exact GPU assignment |
| `scale` | `test_scale.py` | 4 | 4 min | 10 K functions, 1 K executables, mixed, waves |
| `mpi` | `test_mpi.py` | 6 | 4 min | gather, allreduce, broadcast, concurrent jobs; stops at the first failure |

### How failures appear

| What happens | In `junit.xml` | Later groups |
|--------------|----------------|--------------|
| A test fails | That test fails | Run |
| Launcher crashes | `<group>.launcher` fails, with the log tail; that group's other results are lost | Run |
| Group exceeds its budget | `<group>.launcher` fails, and is stopped and cleaned up | Run |

`dragon` can exit 0 even when the application fails or is killed, so the runner relies on the
results file and not on the exit code. Logs for each group are in a `groups/` directory next to
the JUnit file.

---

## File structure

```
test-hpc/
├── run_suite.py               # the groups, their budgets, and crash handling
├── benchmarks/
│   └── dragon_throughput.py   # task throughput measurement
└── dragon/
    ├── conftest.py            # topology detection, fixtures, capability checks
    ├── hpc_workers.py         # worker functions importable by Dragon remote processes
    ├── pytest.ini             # asyncio mode, markers, scale off by default
    ├── test_tasks.py
    ├── test_placement.py
    ├── test_scale.py
    └── test_mpi.py
```

---

## Missing capabilities

Topology is detected at runtime from `dragon.native.machine.System`. When a test's capability is
missing, it skips, unless the capability is listed in `RHAPSODY_HPC_REQUIRE`, which makes it fail:

```bash
export RHAPSODY_HPC_REQUIRE=gpu,multi_node,mpi
```

Set this on machines that should have those capabilities, so a lost dependency or a wrong
allocation shows up instead of skipping quietly.

| Mark | Missing when |
|------|-------------|
| `multi_node` | fewer than 2 nodes detected in the Dragon allocation |
| `gpu` | no GPU-equipped nodes detected |
| `mpi` | `mpi4py` not importable |

The `scale` mark only selects tests: scale tests are excluded by default, and `-m scale` selects them.

---

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `RHAPSODY_HPC` | unset | **Must be `1`** for pytest to collect these tests. `run_suite.py` sets it |
| `RHAPSODY_HPC_REQUIRE` | unset | Comma-separated capabilities that must be present: `gpu`, `multi_node`, `mpi` |
| `RHAPSODY_SCALE_FUNC_COUNT` | `10000` | Function task count for scale tests |
| `RHAPSODY_SCALE_EXEC_COUNT` | `1000` | Executable task count for scale tests |
| `RHAPSODY_DDICT_MEM_GB` | `2` (standard) / `4` (scale) | GiB per node for Dragon's results DDict |

---

## Design decisions

### `hpc_workers.py` — why a separate module

Dragon serialises Python functions by **module reference**
(`hpc_workers.identity`, etc.). When a remote worker on another node
deserialises the function, it imports `hpc_workers` by name. Functions defined
directly in test files (e.g. `test_scale.py`) would require `test_scale` to be
importable on every worker node, which fails on multi-node clusters where the
test directory is not on `sys.path`. The worker processes only see the
environment, so `PYTHONPATH` must include this directory (step 3); without it
every function task fails with `No module named 'hpc_workers'`.

### Pinning tests use executables, not Python functions

Dragon's `batch.process()` — used whenever `process_template` is specified —
stores the **process exit code** (integer `0`) in the DDict result, not the
Python function's return value. Node and GPU identity is therefore verified via
`stdout` (`/bin/hostname`, `echo $CUDA_VISIBLE_DEVICES`). Unpinned function
tasks use `batch.function()` which correctly captures the Python return value.

### `asyncio_default_test_loop_scope = module`

Module-scoped fixtures (`dragon_backend`, `rhapsody_session`) bind asyncio
futures to the module event loop. Without setting the test loop scope to
`module` as well, pytest-asyncio runs each test on its own function-scoped
loop, causing "future belongs to a different loop" errors.

---

## Known Gaps

| Gap | Description | Priority |
|-----|-------------|----------|
| **GPU + MPI combined** | No test pins MPI ranks to specific GPUs (e.g. 1 rank/GPU with `gpu_affinity`) | High |
| **Task failure propagation** | No test submits an intentionally failing task to verify `FAILED` state, exception capture, and non-blocking delivery of other tasks | High |
| **Task cancellation** | Cancellation under multi-node and GPU workloads not covered here; see `test_cancel.py` | Medium |
| **Return value from pinned processes** | `batch.process()` with `process_template` only returns the exit code; Python return values from pinned tasks require a separate communication channel | Medium |
| **DDict overflow / back-pressure** | No test fills the results DDict past capacity to verify graceful failure | Medium |
| **Heterogeneous node topologies** | All tests assume homogeneous GPU count per node | Low |
| **Flux backend parity** | `flux/` sibling directory does not yet exist; the same tests should be ported when the Flux backend matures | Low |
