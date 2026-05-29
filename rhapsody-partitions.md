# Rhapsody Partitions — Multi-Backend Execution Design & Plan

Status: **DRAFT / planning (revised, code-verified, KISS-scoped)**. Architecture for
running multiple execution backends, one per resource partition, within a single
SLURM allocation. This revision (a) replaces an earlier draft whose central
abstraction — engine-resolved DAG with driver-mediated "cut edges" — did **not**
match how Rhapsody works (see §4), and (b) scopes v1 to the smallest possible,
purely-additive change, with everything else explicitly deferred (§9).

## 1. Goal & context

An application (plus the Resource Manager, `rhapsody_rm`) runs inside a single
SLURM allocation. The RM carves the allocation's nodes into ≥1 *partitions*. Each
partition is driven by its own Rhapsody execution backend — and those backends may
be **different technologies** (e.g. Dragon on one partition, Flux on another).

A single application drives all partitions. Tasks are assigned to partitions, and
**data dependencies may cross partition boundaries**.

## 2. Why separate processes (the forcing constraint)

For heterogeneous partitions, separate OS processes are **mandatory, not a
preference**:

- A process is bootstrapped under the Dragon runtime (`dragon python …`) or it is
  not; Flux has its own broker/instance. Two runtimes cannot coexist in one
  interpreter.
- `os.environ` is process-global. Each backend needs its own partition environment
  (`SLURM_NODELIST`, etc.); that is only cleanly possible in separate processes.

Precedent: the **edge backend already runs a backend in a separate process under a
different interpreter** (Dragon V3 behind the bridge). We reuse the *concepts*
(serialized tasks, callback-based result propagation) but **not the transport** —
edge's HTTP+SSE+TLS is WAN-grade and too heavy for same-allocation local IPC.

## 3. Decisions locked

1. **Local only.** Partitions live in one allocation; driver + RM in that
   allocation. No edge/bridge machinery.
2. **Process-per-partition** for proxied partitions; **in-process for the single
   partition default** (§7).
3. **KISS / purely additive.** v1 adds new modules only — **zero edits to existing
   `Session`, task, or backend code** (justified in §5). When in doubt, choose the
   smaller, more localized change.
4. **Cross-partition data dependencies are required** — and turn out to be free
   (§5).
5. **Node pickup / pinning / env correctness for Dragon & Flux is OUT OF SCOPE** —
   the RM emits the partition `env`; honoring it is the backend/launcher's job.
6. **Swappability = localized, not abstracted.** Build exactly one implementation,
   kept localized so a later swap is contained. Do **not** introduce an abstraction
   layer/interface until a second implementation actually exists.
7. **Result-or-handle envelope is a HARD v1 requirement** — the *shape* only; handle
   resolution is deferred (§6.4, §9-D5).
8. **Partition assignment is explicit**, via the existing `task["backend"]`
   routing (§6.5) — no new assignment surface.
9. **Many small tasks** is a target workload, but **batching is deferred** (§9-D1);
   v1 is correct-but-unbatched and the message set stays batch-capable.
10. **RADICAL-Pilot is retiring** → Dragon + Flux are the primary backends; the
    `radical_pilot.py` partition hook is transitional.

## 4. Verified facts about Rhapsody (code-checked, not assumed)

File:line references are against the current `feature/edge` tree.

1. **No engine-level DAG or scheduler.** `Session.submit_tasks` groups tasks by the
   explicit `task["backend"]` field and forwards each group to that backend's
   `submit_tasks`; if unset it defaults to the first backend.
   (`api/session.py:177–248`)
2. **Dependencies are user-level `async`/`await`.** Each task binds an
   `asyncio.Future`; a backend callback → `_state_manager.update_task` →
   `_update_task_impl` resolves the future when `state` is terminal. The user
   awaits a task, reads its result, and submits dependents from driver code.
   (`api/session.py:40–78`, `api/task.py:183–206`)
3. **Backends report completion by mutating the task dict in place** — setting
   `return_value`/`stdout`/`stderr`/`exit_code`/`exception` — **then calling the
   single registered callback** `self._callback_func(task, state)`.
   (`backends/execution/concurrent.py:238–255`, `:143–168`)
4. **`register_callback` stores ONE callback.** `Session` registers
   `_state_manager.update_task`; the base docstring's "chaining" is not implemented.
   (`backends/base.py:92–102`, `concurrent.py:63`)
5. **`update_task` is thread-safe** (`loop.call_soon_threadsafe`), so callbacks may
   originate off the event loop — exactly what an IPC reader needs.
   (`api/session.py:40–52`)
6. **Terminal states come from the backend.** `add_backend` reads
   `backend.get_task_states_map().terminal_states` into the session's terminal-state
   set. `StateMapper` is a class-level registry; `terminal_states = (DONE, FAILED,
   CANCELED)`. (`api/session.py:166–169`, `backends/constants.py:424–442`)
7. **Backend lifecycle:** awaitable init (`await backend` → `_async_init`); `async
   submit_tasks(list[dict])`, `async shutdown()`, `async cancel_task(uid)->bool`,
   `async state()->str`. `build_task` and `task_state_cb` are **backend-internal /
   vestigial — never called by `Session`** (only RP/Dragon call their own
   `build_task`). (`concurrent.py:67–104,260–345`; grep of `.build_task(`)
8. **Partition flows in via the constructor `resources` dict.** Backends take
   `resources={...}`; `radical_pilot` pops `resources["partition"]` (`nodelist`,
   `env`); `concurrent` explicitly rejects partitions.
   (`concurrent.py:37–45`, `radical_pilot.py:_initialize`)
9. **Backends are constructed by name:** `get_backend(name, resources=...)` →
   `BackendRegistry.get_backend_class(name)(**kwargs)`, discovered from class names
   in `rhapsody.backends.execution`. (`backends/discovery.py:165–187`)
10. **Tasks pickle cleanly.** `BaseTask` is a `dict` subclass; `_future` is the only
    internal attr and is stripped in `__getstate__`/re-set to `None` in
    `__setstate__`. (`api/task.py:50–102,211–219`)
11. **Dragon `DataReference` is a zero-copy reference to results in a `DDict`**
    (`return_value` may be "Actual value **or** DataReference") — i.e. the real
    consumer of the envelope's `handle` variant. (`dragon.py:138–202`)

## 5. Consequence: the design is small and purely additive

Because deps are user async/await and **every completion already flows back to the
driver** (to resolve futures and populate `return_value`):

- **No cut-edge resolver, no DAG splitting, no intra/cross-partition distinction.**
  Cross-partition deps are covered *for free*: the user awaits a task on partition A
  (its result returns to the driver), then submits a dependent task to partition B
  with that result as an argument — ordinary Rhapsody usage.
- **Partition assignment = existing `task["backend"]` routing** — zero new `Session`
  code.
- **No driver-side `ResultStore`.** Results live on the task objects the user holds;
  the proxy merges them back onto those objects.

Because a `RemoteBackendProxy` *is* a `BaseBackend`, and `Session` already (a) holds
multiple named backends, (b) routes by `task["backend"]`, (c) registers a callback,
and (d) reads terminal states from `get_task_states_map()` — **nothing in existing
code changes.**

### v1 code footprint (target)
Purely additive, in a new `rhapsody/backends/multiproc/` package (NOT under
`execution/`, so `BackendRegistry` does not auto-discover the proxy):
- `proxy.py` — `RemoteBackendProxy(BaseBackend)`
- `host.py` (runnable via `python -m rhapsody.backends.multiproc.host`) — backend-host
- `spawn.py` — tiny `Listener` + subprocess spawn helper
- `examples/partitions.py` — manual wiring example (the v1 "orchestration", §9-D3)

Zero edits to `session.py`, `task.py`, `constants.py`, or any existing backend.

### Prior art — the dragon-isolation prototype (commit `660dc45`)
Branch `feature/dragon-isolation` (on `origin`, commit
`660dc457632b9758127806cb8e3b056a3d0073cf`) is a **working Dragon-specific
prototype of this exact architecture**, built against v0.2.0 and superseded in the
tree by the v0.3.0 dragon refactor. It is the reference implementation for several
pieces here — mine it when implementing Phase 1/3:
- `DragonExecutionBackendV3Client(BaseBackend)` ≈ `RemoteBackendProxy` (§6.1):
  driver-side backend; `_result_loop` fires the callback on incoming results.
- `DragonExecutionBackendV3Worker` + `_worker_main`/`main()` (argparse) ≈ the
  backend-host (§6.2) and its `python -m` entry point.
- ZeroMQ transport: `_bind_zmq_early`/`_start_zmq_threads`/`_zmq_task_loop`/
  `_zmq_result_loop`, with `_wait_for_endpoints`/`_connect_zmq` as the
  spawn/handshake (informs §6.3 transport and §9-D2).
- `_bootstrap_dragon_runtime(...)` ≈ launching the child under the Dragon runtime
  (informs Phase 3 spawn and §9-D4/D5).

Differences from this plan: it is Dragon-specific (client/worker are Dragon
subclasses) and uses **ZeroMQ**; this plan generalizes the proxy to any
`BaseBackend` and defaults to `multiprocessing.connection` (§6.3, decision 6).
Retrieve with: `git show 660dc45:src/rhapsody/backends/execution/dragon.py`.

## 6. Components

```
 ┌──────────────────── driver process ────────────────────┐
 │  Session (UNCHANGED): routes by task["backend"]         │
 │     ├─ "p0"  → in-process BaseBackend     (default)     │
 │     ├─ "p1"  → RemoteBackendProxy ──┐                   │
 │     └─ "p2"  → RemoteBackendProxy ──┤ control plane     │
 │                                     │ (local, unbatched │
 │  user code: await task; submit      │  in v1)           │
 │  dependents (any backend)           │                   │
 └─────────────────────────────────────┼──────────────────┘
                                        │
        ┌───────────────────────────────┴──────────────┐
        ▼                                               ▼
 ┌─ child: backend-host (own runtime + partition env) ─┐  …p2…
 │  control-plane server (SUBMIT / CANCEL / SHUTDOWN)  │
 │  real BaseBackend via get_backend(name, resources)  │
 │  callback → STATE / COMPLETE(envelope) back         │
 └─────────────────────────────────────────────────────┘
```

### 6.1 `RemoteBackendProxy(BaseBackend)` *(driver side)* — the crux
Implements the verified `BaseBackend` surface by forwarding over the control plane:
- `__init__(name=<partition-backend-name>)`; awaitable init connects to the child
  and waits for `READY`. **v1 registers default task states** (DONE/FAILED/CANCELED)
  in the `StateMapper` registry; the child's custom state map is deferred (§9-D4).
- `async submit_tasks(tasks)`: hold references to the submitted task objects by
  `uid` (these are the user's real objects, per fact 1/3), serialize copies, send
  `SUBMIT`. (One message per call in v1; batching deferred §9-D1.)
- `async cancel_task(uid)`, `async shutdown()`, `async state()`.
- On `STATE`/`COMPLETE` from the child: **merge the result fields
  (`state/stdout/stderr/exit_code/return_value/response/exception`) back into the
  driver-side task object held by `uid`**, then call `self._callback_func(task,
  state)` (= `Session.update_task`, thread-safe per fact 5). This is the direct
  cross-process analogue of fact 3.
- `build_task`/`task_state_cb` exist only to satisfy the ABC; never invoked (fact 7).

### 6.2 Backend-host *(child side)*
Thin process — `python -m rhapsody.backends.multiproc.host` (later `dragon python -m
…` / `flux …`) — launched with the partition `env`. It:
- constructs the real backend via `get_backend(name, resources={"partition": …})`
  and `await`s its init (facts 8, 9, 7);
- registers a callback that sends `STATE`/`COMPLETE` (with result envelope) back
  over the control plane;
- serves `SUBMIT`/`CANCEL`/`SHUTDOWN`.

### 6.3 Control plane (driver ↔ child)
- **Transport (one impl, used directly — no abstraction, per decision 6):**
  `multiprocessing.connection` Listener/Client (stdlib, authenticated full-duplex
  pickle channel). Seam extraction deferred (§9-D2).
- **Spawn/handshake (in scope):** driver creates a `Listener`, spawns the child via
  subprocess with the connection address + authkey + backend name + partition env;
  child connects and sends `READY`. (Node pinning correctness is out of scope §3.5.)
- **Serialization:** pickle for task dicts (fact 10); **cloudpickle for task
  functions/results**, consistent with the edge backend — same Python-version-skew
  hazard edge already guards against.
- **v1 is unbatched** (one message per submit/event); batching deferred (§9-D1).
- **Message set (stable contract; list-shaped so batching is a later non-breaking
  fill):**
  - driver→child: `SUBMIT(batch)`, `CANCEL(uid)`, `SHUTDOWN`
  - child→driver: `READY`, `STATE(uid,state)…`, `COMPLETE(uid,state,envelope)…`,
    `ERROR`
  - reserved for deferred work: `GET_RESULT(uid)` (handle resolution §9-D5),
    `READY(state_map)` payload (custom states §9-D4)

### 6.4 Result-or-handle envelope **(HARD v1 requirement — shape only)**
Every `COMPLETE` carries a result *envelope*, never a bare result:
```
envelope = {"kind": "inline", "payload": <serialized result fields>}
         | {"kind": "handle", "ref": <descriptor: e.g. Dragon DataReference>}
```
**v1 only emits and consumes `kind="inline"`.** The `handle` branch is reserved in
the wire format (so adding it later is non-breaking) but not implemented — see
§9-D5. The future `handle` consumer is a Dragon `DataReference` (fact 11).

### 6.5 Partition assignment — existing routing, no new code
Each partition is registered in the `Session` as a backend with a **unique name**
(e.g. `dragon_v3.p0`, `flux.p1`). The user routes with `task["backend"] =
"<partition-backend-name>"` (fact 1). Single-partition users set nothing and hit the
default backend.

### 6.6 RM integration — v1 is user/example code (no orchestration component)
The wiring — `partition_spec()` → spawn host → build proxy → `Session(backends=…)`
— lives in `examples/partitions.py` for v1, not a framework component (§9-D3). The
driver uses `rhapsody_rm.partition_spec(rm, part_id, n)` →
`{"nodelist": [Node…], "env": {…}}` (existing contract, `rhapsody_rm/CONTRACT.md`);
the `env` becomes the child's process environment; the spec is passed to the child's
backend as `resources={"partition": …}` (fact 8). No RM contract change needed.

## 7. Single-partition fast path (the default)

The single-partition case binds the partition to a **real in-process backend**
(existing behavior): zero new code, zero serialization, no subprocess, no
regression. It is the proxy-less binding of the locality choice.

Bounded cost when proxied: the proxy returns deserialized *copies* of results while
in-process returns live objects — acceptable because Rhapsody already assumes
serializable results (cloudpickle/edge). Mitigation: **CI can force the proxy
binding with 1 partition** so the IPC path is exercised without taxing the default.

## 8. Cost analysis (many small tasks)

There is no backend-internal dep resolution (fact 2), so a proxied partition ships
every completion (and its result) back to the driver. In v1 (unbatched) that is one
message per task per direction; the per-task **serialization** cost is inherent, and
round-trip overhead is the thing batching (§9-D1) later amortizes. The common case
is unaffected because the single-partition default stays in-process (§7); only
genuine multi-partition runs pay it.

## 9. Implementation phases & deferred work

### Phases
- **Phase 0 — Skeletons & protocol.** `proxy.py`, `host.py`, `spawn.py`; the
  message set; the **result-or-handle envelope shape**. Direct `multiprocessing.connection`.
- **Phase 1 — Proxy round-trip (1 partition).** Drive `concurrent`/`noop` through
  proxy + host; assert observable parity with the in-process path (states, ordering,
  `return_value`, exceptions incl. `TaskExecutionError`, cancellation). Add the CI
  knob to force proxy@1-partition. (Default states; unbatched; inline only.)
- **Phase 2 — Multiple proxied partitions.** Two `concurrent` children under
  distinct backend names; a workflow whose task on `p1` consumes (via user await) a
  result from `p0`. No new driver machinery — two proxies + user async/await + the
  `examples/partitions.py` wiring.
- **Phase 3 — Heterogeneous + spawn.** Real Dragon + Flux children launched under
  their runtimes with RM-supplied env; **pulls in D4 (state-map handshake)**;
  `resources["partition"]` passed through. (Pinning issues → upstream.)
- **Phase 4 — Hardening.** Child-death detection, ordered drain/teardown,
  backpressure, cross-process observability/telemetry correlation.

### Deferred work (detailed — pick up in a later session)

Each entry: *what*, *why deferred*, *where it plugs in*, *trigger*, *gotchas*.

**D1 — Control-plane batching.**
- *What:* collect multiple `SUBMIT` tasks (driver→child) and multiple
  `STATE`/`COMPLETE` events (child→driver) over a time window or up to a size cap,
  and send as one message instead of one-per-task/event.
- *Why deferred:* adds windowing/flush/buffering machinery; correctness doesn't need
  it. Decision 9.
- *Where:* localized to two spots — `RemoteBackendProxy.submit_tasks` (outbound
  batch) and the backend-host completion callback (inbound batch). The message set is
  already list-shaped, so v1 sends batches of size 1; batching just fills larger
  ones. No protocol change.
- *Trigger:* measured per-task overhead under a many-small-tasks workload on ≥2
  proxied partitions.
- *Reference:* edge backend's `batch_window`/`batch_limit`/`notify_batch_window`/
  `notify_batch_size` (in `edge.py`) — reuse the *design* (time window + size cap +
  explicit flush), not the transport.
- *Gotchas:* preserve per-task ordering; **flush on shutdown/drain** so terminal
  states are never lost or delayed past teardown.

**D2 — Transport seam extraction.**
- *What:* a thin `Transport` interface (`send(msg)`/`recv()->msg`/`close()`) so the
  control plane can swap `multiprocessing.connection` for pipes / ZeroMQ / a
  cross-node socket.
- *Why deferred:* only one impl exists; abstracting now risks the wrong seam
  (rule-of-three). Decision 6.
- *Where:* the send/recv calls live in exactly two places (proxy client, host
  server). Extraction = move those behind `Transport`; localized.
- *Trigger:* a second transport is actually needed — e.g. partition count/throughput
  outgrows `multiprocessing.connection`, or a PUB/SUB fan-out for state becomes
  worthwhile.
- *Gotchas:* `multiprocessing.connection` gives framing + authkey for free; raw
  sockets/ZeroMQ need explicit message framing and an auth story to match.

**D3 — Multi-partition orchestration helper.**
- *What:* a convenience (e.g. `build_partitioned_session(rm, layout)` or a
  `PartitionedSession`) that allocates partitions, spawns hosts, builds proxies, and
  registers them in a `Session`, with lifecycle (ordered spawn/teardown).
- *Why deferred:* v1 keeps wiring as `examples/partitions.py`; the app already builds
  a `Session` with backends, so no framework is needed to prove the design. Avoid
  premature API.
- *Where:* a new module under `rhapsody/backends/multiproc/` wrapping
  `partition_spec()` → `spawn` → `RemoteBackendProxy` → `Session(...)`. Purely
  additive.
- *Trigger:* the manual wiring pattern stabilizes/repeats, or lifecycle complexity
  (spawn/teardown ordering, partial-failure cleanup) warrants encapsulation.
- *v1 substitute:* the example script is the reference implementation.

**D4 — State-map handshake for non-default backends.**
- *What:* child sends its `StateMapper` mapping in `READY(state_map)`; proxy
  registers it in the driver's class-level `StateMapper` registry so
  `get_task_states_map().terminal_states` matches the child backend (fact 6).
- *Why deferred:* Phase 1/2 use `concurrent`/`noop` (default states), so the proxy
  registering defaults is sufficient. Needed first at Phase 3.
- *Where:* `RemoteBackendProxy` awaitable init (consume `READY(state_map)`),
  `host.py` startup (emit `backend.get_task_states_map()` contents), and
  `StateMapper.register_backend_tasks_states(...)` driver-side.
- *Trigger:* first non-default-state backend (Dragon/Flux) proxied.
- *Gotchas:* key the registration by the **unique partition-backend-name** to avoid
  collisions when two partitions run the same backend type; transmit the mapping in a
  picklable form (string states).

**D5 — `handle` envelope + `GET_RESULT` (large-payload data plane).**
- *What:* `COMPLETE` may carry `kind="handle"` (e.g. a Dragon `DataReference`); the
  driver resolves it lazily via `GET_RESULT(uid)` when the user accesses
  `return_value`.
- *Why deferred:* v1 ships inline; large cross-partition payloads are the trigger.
  The envelope *shape* is already in v1 (hard req), so this is non-breaking.
- *Where:* `host.py` emits `handle` envelopes for large/opted results; `proxy.py`
  stores the handle on the task and resolves on access; new `GET_RESULT` round-trip.
- *Trigger:* measured copy cost for large cut-partition results. This is also the
  **only** place a Dragon/Flux feature request is likely justified (§11).
- *Gotchas:* a Dragon `DataReference` resolving *from the driver process* may require
  the driver to attach to the child's `DDict` — crossing runtime boundaries. If that
  is not possible, resolve via a **host-mediated fetch** (`GET_RESULT` → host reads
  its DDict → returns bytes). Confirm before relying on direct reference resolution.

**D6 — Result lifecycle / eviction.**
- *What:* the proxy retains task objects (and any results) by `uid`; decide when they
  may be dropped.
- *Why deferred:* not needed for correctness at small scale.
- *Where:* `RemoteBackendProxy`'s uid→task map.
- *Trigger:* long-running sessions where retained results grow unbounded.

## 10. Testing strategy

- **Proxy parity:** identical observable behavior in-process vs proxied for
  `concurrent`/`noop` (states, ordering, `return_value`, exception propagation incl.
  `TaskExecutionError` on non-zero exit, cancellation).
- **Force proxy@1-partition in CI** to cover the IPC path without making it default.
- **Cross-partition test** with two `concurrent` children — no cluster needed.
- **Mock the transport** in unit tests (as `edge.py` mocks `BridgeClient`).
- Dragon tests continue to require the Dragon runtime (`dragon python -m pytest`).

## 11. Feature-request policy (Dragon/Flux)

Cross-partition deps need **zero** Dragon/Flux changes: the backend contract already
lets the host *get* a result (fact 3) and *supply* an input (task args). Hold the
feature-request card for one specific, measured case: avoiding a copy for **large**
cut-partition payloads — consumed by the deferred `handle` variant / Dragon
`DataReference` (§9-D5). Do not request features for v1.

## 12. Out of scope

- Node pickup, pinning, affinity, and their correctness/efficiency (Dragon/Flux).
- Remote/cross-allocation execution (edge backend's domain).
- Dynamic/load-aware placement (assignment is explicit, §6.5).
- Replacing the transitional `radical_pilot.py` partition hook (handle when RP
  retires).

## 13. Open items to confirm while building

- **Result/exception picklability:** `return_value` and `exception` must serialize
  across the boundary; custom exception/result classes must be importable on the
  driver. Same class of constraint as edge's cloudpickle skew — surface clear errors.
- **Telemetry across processes:** driver-side submit/state telemetry works via the
  proxy callback; child-emitted spans (e.g. Dragon) need correlation — Phase 4.
