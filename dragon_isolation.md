# Dragon isolation POC — handoff

Resume point for continuing development on a cluster where scaling can be tested.

## Context

**Goal.** Prove that the client/worker split in `dragon_test.py` can deliver Dragon-Batch throughput close to Dragon-native, while preserving the isolation property that allows:
- multiple Dragon instances in the same user process, and/or
- Dragon alongside another backend (Concurrent, Dask, …) in the same user process.

**Scope.** All edits happen in `/home/merzky/radical/rhapsody/dragon_test.py` only. The real module `src/rhapsody/backends/execution/dragon.py` is untouched until the POC validates.

**Observed problems (pre-POC).**
1. Overall system slower than Dragon native.
2. System stalls at 10k-task workloads.

**Full analysis** — see `/home/merzky/radical/rhapsody/dragon_test.md` §1–§6. It contains the architecture diagram, the slow-vs-native analysis (§3), the 10k-stall analysis (§4), the recommended fix list (§5), and the isolation-design-flaw analysis (§6). **Read it first** before resuming — this doc assumes that context.

## What was changed in round 1 (POC)

All three changes are in `dragon_test.py`. File is not git-tracked (untracked in the repo root).

### §6.1 — Lazy Dragon imports

**Before:** module-level `try: import dragon …` with 14 symbols pulled unconditionally. Any user process importing `DragonExecutionBackendV3Client` triggered `import dragon` in that process, potentially leaking Dragon's multiprocessing monkey-patches.

**After:**
- Module globals: `Batch = None` and `ProcessTemplate = None` only.
- New helper `_import_dragon_symbols()` — idempotent; imports `Batch` and `ProcessTemplate` into module globals on first call.
- `DragonExecutionBackendV3.__init__` calls `_import_dragon_symbols()` as its first line (before the `if not Batch` check and before `_bootstrap_dragon_runtime`).
- Dropped 11 dead top-level imports (`Process`, `Popen`, `Queue`, `System`, `Policy`, `Event`, `Telemetry`, `AccVendor`, `find_accelerators`, `ProcessGroup`, `DragonUserCodeError`) — grep confirmed none are referenced anywhere in `dragon_test.py`.
- `DDict` was already lazy-imported inline in `create_ddict()`; left alone.

**Client path** (`DragonExecutionBackendV3Client`) does not call `_import_dragon_symbols()` and references no Dragon symbols, so importing this module in a user process no longer pulls Dragon.

### §6.2 — Per-instance coordination dir

**Before:** `~/.rhapsody/sessions/<pid>` — two `Client(...)` instances in the same process collide on `endpoints.json`.

**After:** `~/.rhapsody/sessions/<pid>-<uuid4[:8]>`. Added `import uuid` at top.

### §5.2 — Register tasks inline with build loop

**Before:** `submit_tasks` built all tasks into a `batch_tasks_data` list, then did a second pass to populate `_monitored_batches`. With 10k tasks, the Batch background thread began dispatching tasks during build, completions piled into the DDict, and the monitor thread saw nothing because registration hadn't happened yet. When registration finally ran en masse, the monitor had thousands of pending entries simultaneously.

**After:** single loop — `build_task()` then `self._monitored_batches[batch_task.uid] = (batch_task, task["uid"])` right after, per task. The monitor begins draining completions as soon as the first task is built. Also dropped the `batch_tasks_data` list entirely (one less allocation per batch).

### Validation run so far

Only `python3 -m py_compile dragon_test.py` — passes. Pyright diagnostics present are all pre-existing (relative imports from `..base`/`..constants` don't resolve because the POC file lives at repo root; Dragon types aren't available without the library). None are regressions.

**No functional test has been run yet** — that's why we're moving to the cluster.

## What to do first on the cluster

### 0. Make dragon_test.py actually runnable

The file uses relative imports (`from ..base import BaseBackend`, `from ..constants import BackendMainStates`, `from ..constants import StateMapper`). These fail when the file is at repo root. Before any testing, decide one of:

- **Option A (recommended for POC):** drop the POC into `src/rhapsody/backends/execution/dragon_poc.py` so relative imports resolve. Run worker with `python -m rhapsody.backends.execution.dragon_poc --worker …`. Update `_async_init`'s launch command (currently hardcoded to `rhapsody.backends.execution.dragon`) accordingly — see `dragon_test.py:853`.
- **Option B:** convert the three relative imports to absolute (`from rhapsody.backends.base import BaseBackend`, etc.) and run as `python dragon_test.py`. Simpler but doesn't match how rhapsody is normally installed.

Option A is closer to how the real module is used and avoids divergence when we port the changes back.

### 1. Smoke test: isolation actually holds

Before any scale test, confirm §6.1 works end-to-end:

```python
# In a user process:
import sys
before = set(sys.modules)
from rhapsody.backends.execution.dragon_poc import DragonExecutionBackendV3Client
after = set(sys.modules)
assert not any(m.startswith("dragon") for m in (after - before)), \
    sorted(m for m in (after - before) if m.startswith("dragon"))
```

If this assertion passes, §6.1 is holding. If it fails, inspect which `dragon.*` module leaked in and move the corresponding import.

Also confirm a second test: instantiate a `ConcurrentExecutionBackend` (stdlib multiprocessing) alongside the client in the same process and submit jobs to both. If Concurrent's `ProcessPoolExecutor` still works, isolation is intact.

### 2. Smoke test: single client end-to-end

Submit ~10 trivial function tasks through a single `DragonExecutionBackendV3Client`. Confirm all callbacks fire with `"DONE"`. This validates that the lazy-import restructure and the inline-registration change didn't break the basic happy path.

### 3. Scale test: 10k tasks

The original stall reproducer. Submit 10_000 trivial function tasks. Expectation after round 1:

- **Likely still slow** (we haven't fixed §5.1 DDict polling or §5.4 per-task result send yet).
- **But: should no longer hard-stall** — or if it does, the stall should start later. The §5.2 change removes one cause; §5.1 is the other big one.

Worth instrumenting before declaring progress:
- monitor sweep duration (time around the `for tuid in batch_tuids` loop) — log at e.g. 1 Hz
- `len(self._monitored_batches)` — live queue depth
- `_zmq_result_queue.qsize()` in the worker
- callback rate on the client

If sweep time is >100 ms consistently, §5.1 is the binding constraint and must come next. If sweep time is fast but throughput is low, §5.4 (batched result path) is the binding constraint.

### 4. Scale test: multi-instance

Instantiate two `DragonExecutionBackendV3Client(...)` instances in the same user process, submit tasks to both concurrently. Expectation:

- Two distinct `endpoints.json` under distinct dirs (§6.2 validates).
- Both make progress.

**Known risk (§6.3):** the `dragon` launcher itself may not tolerate two concurrent runtimes on the same host (shm namespaces / port collisions / WLM allocation). If this test fails with "Dragon already running" / port-bind errors / shm errors rather than a rhapsody-level issue, §6.3 is the blocker and needs launcher-args plumbing (per-instance shm dir, etc.). Document the exact failure — we don't have enough info to pre-fix this blindly.

## Round 2 work queue (ordered by impact)

### §5.1 — Replace DDict polling (highest impact)

Current code (`dragon_test.py:249-281`) sweeps every in-flight task at ~200 Hz, doing a DDict `__getitem__` + KeyError catch per unfinished task. At 10k pending that's ~2M DDict RPCs/s, which saturates the GIL and DDict managers.

Two options, in order of preference:

**A. Event-driven via `Task.get(block=True)` and a thread pool.** If `dragon.workflows.batch.Task` exposes a blocking `get()` that sleeps on a Dragon channel event (not a poll), use a small pool (e.g. 16 threads) to `get()` tasks one at a time and hand results to the asyncio loop via `call_soon_threadsafe`. Needs Dragon API investigation — check the Batch docs / source on the cluster.

**B. Bounded-sweep polling (fallback).** If no event-driven primitive is available:
- Cap each sweep to a fixed window (e.g. 256 tuids); rotate the window between sweeps.
- Adaptive sleep: start at 1 ms, back off to 50 ms if nothing found over N sweeps, snap back to 1 ms on a hit.
- Avoid the `list(self._monitored_batches.keys())` copy every iteration — use an explicit `collections.deque` of pending tuids maintained by `submit_tasks`.

Option B guarantees progress and caps GIL contention. Option A is strictly better if available.

### §5.4 — Batch the result path

Currently each completion = one `queue.put` → one `cloudpickle.dumps` → one ZMQ `send` → one client `recv` → one `cloudpickle.loads` → one `call_soon_threadsafe`. For 10k results that's 10k of each.

Mirror the submit path:
- Worker: in `_zmq_result_loop`, drain `_zmq_result_queue` non-empty into a list (first `get()` blocks, then `get_nowait()` until empty), `cloudpickle.dumps(list)`, one `send`.
- Client: in `_result_loop`, `cloudpickle.loads` gives a list, then a single `call_soon_threadsafe(self._deliver_batch_client, results_list)` where the dispatcher walks the list.

Code site: `_zmq_result_loop` at `dragon_test.py:739-749`, client `_result_loop` at `dragon_test.py:952-979`.

### §6.5 — Handshake

After `_bind_zmq_early` and the worker's `_start_zmq_threads` complete and `Batch()` is up, have the worker send a `{"_rhapsody_cmd": "ready"}` down the result socket. The client's `_result_loop` sets a `threading.Event` on receipt; `_wait_for_endpoints` / `_async_init` on the client waits on that event before returning (with a timeout). This closes the "tasks sent before Batch() finishes initializing get dropped into a dead process" hole.

### §6.4 — Worker liveness

In the client's `_result_loop`, inside the poll timeout, also check `self._worker_proc.poll()`. If it returned non-None, raise via `call_soon_threadsafe` into the asyncio loop so user code gets an exception instead of a silent hang.

Optional second layer: periodic ping/pong over the existing socket pair (sentinel dict with `_rhapsody_cmd: ping`). Likely overkill for POC — `poll()` is enough.

### §6.7 — Exception pickle fallback

In the worker's `_wrapped` callback (`dragon_test.py:689-699`), before enqueueing:
```python
exc = task.get("exception")
if exc is not None:
    try:
        cloudpickle.dumps(exc)  # dry-run
    except Exception:
        exc = {"_rhapsody_exc_repr": repr(exc), "_rhapsody_exc_tb": task.get("stderr")}
```
(Or do the check later in `_zmq_result_loop` and substitute there.) Stops in-flight tasks from vanishing silently when a Dragon-native exception won't pickle.

### §6.3 — Multi-instance Dragon runtime isolation

Only address **after** §6.4 of round 2 validates multi-instance works at all. Needs on-cluster investigation of what Dragon actually collides on. Likely fix: surface a `launcher_args` default that sets per-instance shm dir / port range / WLM tag.

### §5.3, §5.5, §5.6, §5.7 — Lower priority

- §5.3 make `build_task` sync (small win, asyncio trampoline overhead per task).
- §5.5 chunk the submit blob into ~1k-task ZMQ messages (memory spike reduction, starts execution sooner).
- §5.6 surface and document `pool_nodes` sizing.
- §5.7 throughput/latency logging (queue depths, sweep time, DDict probes/s). Valuable regardless — do this first on the cluster, it makes every other diagnostic easier.

## Current state of `dragon_test.py` (key line numbers)

- Lazy import helper: `_import_dragon_symbols()` around line 30.
- `_import_dragon_symbols()` call site: first line of `DragonExecutionBackendV3.__init__`, around line 175.
- Per-instance coordination dir: in `DragonExecutionBackendV3Client.__init__`, around line 815.
- Inline-registration submit loop: `DragonExecutionBackendV3.submit_tasks`, around line 340.
- Monitor loop (untouched — next target): `_monitor_loop` around line 241.
- Worker result loop (untouched — §5.4 target): `_zmq_result_loop` around line 739.
- Client result loop (untouched — §5.4 + §6.4 target): `_result_loop` around line 952.

## Decisions still open

1. **Where to put the POC file** so relative imports resolve (see §0 above). Recommend moving to `src/rhapsody/backends/execution/dragon_poc.py`.
2. **§5.1 Option A vs B** — depends on whether `dragon.workflows.batch.Task` exposes a blocking event-driven `get()`. First cluster task: check the Dragon API.
3. **Instrumentation first?** — adding §5.7 logging before §5.1 means you'll have numbers to prove §5.1 fixed the problem. Recommend yes.

## How to run tests (rhapsody baseline)

From `CLAUDE.md`:
```bash
make test-quick            # unit + integration, no Dragon
make test-unit-no-dragon   # unit only, no Dragon
pytest tests/unit/test_backend_base.py -xvs
```

These do **not** exercise `dragon_test.py` (POC file). Once the POC is moved to `src/rhapsody/backends/execution/dragon_poc.py` and the launcher hardcode is updated, we can write a dedicated integration test and drop it under `tests/integration/`.

## Files to look at

- `dragon_test.py` — the POC under development
- `dragon_test.md` — the full analysis (architecture, slow/stall analysis, fix list, design flaws)
- `dragon_isolation.md` — this file
- `src/rhapsody/backends/execution/dragon.py` — the real module, untouched, target for eventual port-back
- `src/rhapsody/backends/base.py` — `BaseBackend` ABC the POC inherits from
- `src/rhapsody/backends/constants.py` — `BackendMainStates`, `StateMapper`
