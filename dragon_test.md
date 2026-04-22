# dragon_test.py — Architecture & Performance Analysis

## 1. Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  USER PROCESS  (python script.py)                                               │
│  DragonExecutionBackendV3Client                                                 │
│                                                                                 │
│   ┌──────────────────────────┐           ┌───────────────────────────────┐      │
│   │ Main thread (asyncio)    │           │ Thread: dragon_client_results │      │
│   │                          │           │   (_result_loop)              │      │
│   │  submit_tasks() ──┐      │           │                               │      │
│   │  shutdown()  ──┐  │      │           │   zmq.Poller on:              │      │
│   │                │  │      │           │     • _result_socket (PULL)   │      │
│   │                │  │      │           │     • ctrl PAIR (inproc)      │      │
│   │                │  │      │           │                               │      │
│   │                │  │      │  cross-   │   loop.call_soon_threadsafe ──┼──┐   │
│   │                │  │      │  thread   │     → _callback_func(task,s)  │  │   │
│   │                │  │      │ wake-ups  │                               │  │   │
│   │                │  └──────┼───────────┼── ctrl PAIR "\x00" (shutdown) │  │   │
│   │  _callback ◄───┼─────────┼───────────┼──────────────┐                │  │   │
│   └──────┬─────────┘  │      │           └───────┬───────┼────────────────┘  │   │
│          │ PUSH       │      │                   │ PULL  │ inproc PAIR       │   │
│          ▼ _task_socket      │                   ▼       │  rhapsody-client  │   │
│       ┌──┴────────────┴──────────────────────────┴───────┴──┐                │   │
│       │          ZMQ context   (HWM=0, cloudpickle)         │                │   │
│       └──┬───────────────────────────────────────────┬──────┘                │   │
│          │                                           │                       │   │
│          │  subprocess.Popen(["dragon",…,"-m rhapsody…--worker"])            │   │
│          │   stdout/stderr → worker.{stdout,stderr}  │                       │   │
│          │                                           │                       │   │
└──────────┼───────────────────────────────────────────┼───────────────────────┼───┘
           │                                           │                       │
           │  ipc:// (single-node)  OR  tcp:// (multi-node, SLURM/PBS)         │
           │  endpoints.json discovered on filesystem (polled)                 │
           │                                           │                       │
           ▼ PUSH tasks                                │ PUSH results          │
┌──────────┴───────────────────────────────────────────┴───────────────────────┼───┐
│ DRAGON HEAD PROCESS  (spawned by dragon launcher: dragon_single | dragon_multi_fe)│
│ DragonExecutionBackendV3Worker  (DRAGON_MY_PUID set)                         │   │
│                                                                              │   │
│   _task_socket PULL           _result_socket PUSH     _zmq_ctrl_pub PAIR     │   │
│        │                            ▲                       │                │   │
│        │  zmq.Poller                │                       │  "\x00"        │   │
│        ▼                            │                       ▼                │   │
│   ┌──────────────────┐        ┌─────┴────────────┐   (inproc:                │   │
│   │ Thread:          │        │ Thread:          │    rhapsody-worker-ctrl)  │   │
│   │ dragon_zmq_tasks │        │ dragon_zmq_results│                          │   │
│   │ (_zmq_task_loop) │        │ (_zmq_result_loop)│                          │   │
│   │                  │        │                  │                           │   │
│   │ drain NOBLOCK →  │        │ _zmq_result_queue│                           │   │
│   │ cloudpickle.loads│        │ .get() (blocking)│                           │   │
│   │                  │        │                  │                           │   │
│   │ asyncio.run_     │        └─────▲────────────┘                           │   │
│   │  coroutine_      │              │ queue.Queue (thread-safe)              │   │
│   │  threadsafe ──┐  │              │  put() from _wrapped callback          │   │
│   └───────────────┼──┘              │                                        │   │
│                   ▼                 │                                        │   │
│   ┌──────────────────────────────────────────────────────────────────────┐   │   │
│   │ Main thread (asyncio event loop, _loop)                              │   │   │
│   │                                                                      │   │   │
│   │   super().submit_tasks(tasks)                                        │   │   │
│   │     └─▶ build_task() ─▶ batch.function|process|job(...)              │   │   │
│   │         registers in _monitored_batches[batch_task.uid]              │   │   │
│   │                                                                      │   │   │
│   │   _deliver_batch(completions)   ◄── call_soon_threadsafe ─┐          │   │   │
│   │     └─▶ _callback_func (wrapped) ─▶ _zmq_result_queue.put│          │   │   │
│   └──────────────────────────────────────────────────────────┼──────────┘   │   │
│                                                              │              │   │
│   ┌──────────────────────────────────────────────────────────┼──────────┐   │   │
│   │ Thread: dragon_monitor_loop   (_monitor_loop, daemon)    │          │   │   │
│   │                                                          │          │   │   │
│   │   while not _shutdown_event or _monitored_batches:       │          │   │   │
│   │       for tuid in _monitored_batches:                    │          │   │   │
│   │           batch.results_ddict[tuid]  ─── KeyError? skip  │          │   │   │
│   │       if completed: ─── call_soon_threadsafe ────────────┘          │   │   │
│   │       sleep(0.005)                                                  │   │   │
│   └──────────────┬──────────────────────────────────────────────────────┘   │   │
│                  │ reads                                                    │   │
│                  ▼                                                          │   │
│   ┌─────────────────────────────────────────────────────────────────────┐   │   │
│   │  dragon.workflows.batch.Batch                                       │   │   │
│   │   ├─ internal auto-dispatch background thread                       │   │   │
│   │   ├─ results_ddict  :  dragon DDict  (managers/pool across nodes)   │   │   │
│   │   └─ ProcessGroup / ProcessTemplate / Popen — schedules:            │   │   │
│   │        function() | process() | job(MPI)                            │   │   │
│   └──────────────────────────────┬──────────────────────────────────────┘   │   │
└──────────────────────────────────┼──────────────────────────────────────────┼───┘
                                   │ Dragon channels / HSTA transport        │
                                   │ (TCP/SSH between nodes, shm on-node)    │
                                   ▼                                         │
       ┌───────────────────────────────────────────────────────────┐         │
       │  DRAGON RUNTIME  —  Global Services + Local Services      │         │
       │                                                           │         │
       │   Node 0          Node 1         ...        Node N-1      │         │
       │   ┌──────┐       ┌──────┐                  ┌──────┐       │         │
       │   │ LS   │       │ LS   │                  │ LS   │       │         │
       │   │ ┌──┐ │       │ ┌──┐ │                  │ ┌──┐ │       │         │
       │   │ │Wk│ │       │ │Wk│ │                  │ │Wk│ │       │         │
       │   │ └──┘ │       │ └──┘ │                  │ └──┘ │       │         │
       │   │DDict │       │DDict │                  │DDict │       │         │
       │   │mgr   │       │mgr   │                  │mgr   │       │         │
       │   └──────┘       └──────┘                  └──────┘       │         │
       └───────────────────────────────────────────────────────────┘         │
```

### Communication paths

- **User ↔ Worker (across processes):** ZMQ PUSH/PULL. `ipc://` single-node, `tcp://` multi-node. Transport chosen by `SLURM_JOB_ID`/`PBS_JOBID` env. Endpoints exchanged via `endpoints.json` in `_coordination_dir`. Payloads = `cloudpickle`. Shutdown = in-band sentinel `{"_rhapsody_cmd":"shutdown"}`.
- **Inside each process (thread wake-ups):** inproc `PAIR` sockets (`rhapsody-client-ctrl`, `rhapsody-worker-ctrl`) + `threading.Event` (`_shutdown_event`) + `queue.Queue` (`_zmq_result_queue`).
- **Thread → asyncio loop:** `loop.call_soon_threadsafe` (monitor/result delivery) and `asyncio.run_coroutine_threadsafe` (ZMQ-received batch → `submit_tasks`).
- **Monitor → Batch:** polls `batch.results_ddict[tuid]` (Dragon DDict, uses Dragon channels underneath).
- **Process spawn:** `subprocess.Popen(["dragon", …])` — dragon launcher (`dragon_single` or `dragon_multi_fe`) starts Global/Local Services, then re-execs the script as the head process (detected via `DRAGON_MY_PUID`).

---

## 2. Why a dedicated `dragon_client_results` thread (not inline in asyncio)?

1. **`zmq.Socket.recv()` is blocking and not asyncio-native.** This code uses plain `pyzmq`, not `zmq.asyncio`. Calling `recv()` inline on the event loop would stall every other coroutine (including `submit_tasks`) until a message arrives. Integrating raw `zmq` with `asyncio` via `loop.add_reader` on the socket's edge-triggered fd is possible but tricky (you have to drain-until-EAGAIN on every wake-up); a thread sidesteps that entirely.
2. **Drain-side backpressure.** The thread continuously pulls from the PULL socket regardless of what user code is doing. If a user callback or coroutine is slow, results still get lifted out of ZMQ / kernel buffers promptly on the worker side — the pipe keeps flowing. Inline recv would couple receive throughput to whatever the loop happens to be busy with.
3. **Clean separation: transport vs. user semantics.** The thread handles decode (`cloudpickle.loads`) and fd reads; it then hands the task to the loop via `call_soon_threadsafe`, so user callbacks still execute on the asyncio thread with normal async semantics. User code never has to think about thread safety.
4. **Symmetry with the control path.** The same thread polls `_result_socket` *and* an inproc `PAIR` (`rhapsody-client-ctrl`) for shutdown. One `zmq.Poller` cleanly handles both — inline in asyncio you'd need a separate mechanism to interrupt a blocked recv.
5. **GIL-friendly for a blocking-IO thread.** `recv()` releases the GIL while waiting, so the thread costs ~nothing when idle and doesn't compete with the event loop.

Tradeoff: one extra thread + one `call_soon_threadsafe` hop per result, negligible compared to a ZMQ round trip.

---

## 3. Why is the system slower than Dragon native?

Tracing one task end-to-end exposes several layers of overhead. Ordered by likely impact:

### 3.1 The monitor loop polls DDict instead of waiting on Dragon events

`_monitor_loop` (dragon_test.py:241) sleeps 5 ms then, for **every** in-flight task, does `self.batch.results_ddict[tuid]` and catches `KeyError` if not ready.

- Each probe is a DDict RPC (local channel at best, cross-node in multi-node). For `N` in-flight tasks running at ~200 Hz that's `200·N` cross-thread/RPC probes/second, plus the cost of Python exception construction every miss.
- Minimum detection latency ≈ 5 ms (plus another 10 ms when the queue drains momentarily, dragon_test.py:256).
- Dragon native uses `Task.get(block=True)` which sleeps on a Dragon channel event — O(µs) wakeup, no busy-work.

This is almost certainly the dominant cost. Everything else below is additive.

### 3.2 Result path is per-task; submit path is per-batch

Submission batches fine (one `cloudpickle.dumps(tasks)` + one `send`, dragon_test.py:947). Results don't: `_zmq_result_queue.put` happens per-completion in `_deliver_batch` (dragon_test.py:292), and `_zmq_result_loop` (dragon_test.py:739) does one `dumps` + one `send` per item. For M completions you pay M pickles, M ZMQ syscalls, M client-side `recv`+`loads`+`call_soon_threadsafe` hops.

`_deliver_batch` already receives a list — the "batch" is thrown away the moment it enters the queue.

### 3.3 Hop count per task

- **Submit:** user asyncio → ZMQ PUSH → worker `_zmq_task_loop` (decode) → `run_coroutine_threadsafe` → worker asyncio → `batch.function()`.
- **Complete:** DDict ← monitor (poll+5 ms sleep) → `call_soon_threadsafe` → worker asyncio `_deliver_batch` → `queue.put` → `_zmq_result_loop` → ZMQ PUSH → client `_result_loop` (decode) → `call_soon_threadsafe` → user callback.

That's ~10 thread/process/loop transitions per task round trip where native Dragon has ~0.

### 3.4 Serialization

Every task + every result goes through `cloudpickle` twice (once each direction), with function closures walked on submit. Native Dragon doesn't need this at all — the callable stays in-process.

### 3.5 `await build_task` is sequential per task inside a batched `submit_tasks`

dragon_test.py:341-348 loops `await self.build_task(task)` one task at a time on the worker's event loop before any of them is handed to Batch. `build_task` is CPU-only (no real await points), so each task pays the asyncio trampoline cost for nothing, and the first task can't start being translated into a Batch call until the preceding one's `ProcessTemplate`/`batch.function()` has returned.

### 3.6 Bookkeeping

Two registries per task (`_pending_tasks` on client, `_task_registry` + `_monitored_batches` on worker), each touched on submit and completion from different threads.

---

## 4. Why does it stall at 10k tasks?

A stall (vs. slow throughput) suggests the design has an O(N) hot path that catches up with the arrival rate. Several mechanisms converge at that scale:

### 4.1 Monitor sweep is O(N) in pending tasks, run at ~200 Hz

`_monitor_loop` (dragon_test.py:249-281):
```python
batch_tuids = list(self._monitored_batches.keys())
for tuid in batch_tuids:
    try:
        result, tb, raised, stdout, stderr = self.batch.results_ddict[tuid]
    except KeyError:
        continue
...
time.sleep(0.005)
```

With 10k in-flight tasks, **every sweep** does 10k DDict `__getitem__` RPCs plus 10k KeyError constructions for the unfinished ones. A DDict probe is at best tens of µs on-node, hundreds of µs to ms on multi-node. One sweep ≈ 1–10 s. The `sleep(0.005)` is rounding error.

Consequences:
- Result-detection latency grows linearly with in-flight count.
- The monitor thread becomes GIL-dominant, starving the asyncio loop and the `_zmq_result_loop` thread. From outside it looks frozen.
- Meanwhile the Batch auto-dispatch thread keeps piling completed entries into DDict that nothing drains → DDict memory grows, Dragon's channels can back up.

This is by far the most likely primary cause.

### 4.2 `await build_task` serializes 10k tasks on the worker event loop before anything is monitored

`submit_tasks` (dragon_test.py:339-358) builds all tasks first, then registers:
```python
for task in tasks:
    batch_task = await self.build_task(task)
    batch_tasks_data.append((task["uid"], batch_task))
...
for uid, batch_task in batch_tasks_data:
    self._monitored_batches[batch_task.uid] = (batch_task, uid)
```

`build_task` itself calls `batch.function()/process()/job()` which can be ~ms each. 10k × 1 ms ≈ 10 s where:
- The asyncio loop is blocked, so `_deliver_batch` cannot run.
- Early tasks start executing as Batch auto-dispatches them, pile results into DDict, but the monitor has an empty `_monitored_batches` because registration happens only after the whole loop finishes.
- When registration finally lands, the monitor suddenly has 10k entries and immediately enters the death spiral in §4.1.

User-visible symptom: long period of no progress, then sluggish trickle.

### 4.3 The 10k-task submit is a single cloudpickle blob

Client side (dragon_test.py:947):
```python
self._task_socket.send(cloudpickle.dumps(tasks), flags=1)
```

10k task dicts — each carrying a pickled callable + closures — can be hundreds of MB. HWM is 0 (unlimited), so no drop, but the peer must allocate, recv, and `cloudpickle.loads` it in one shot. Large GC churn, a big latency spike on both ends before any task starts executing.

### 4.4 Per-task result path (asymmetry with submit)

Submit is batched (one ZMQ message for 10k tasks). Results are not:
- `_deliver_batch` → per-completion `_zmq_result_queue.put` (dragon_test.py:288-312)
- `_zmq_result_loop` → per-item `cloudpickle.dumps` + `send` (dragon_test.py:739-749)
- Client `_result_loop` → per-item `recv` + `loads` + `call_soon_threadsafe` (dragon_test.py:952-979)

So you pay 10k serialize/syscall/wakeup cycles on the return path while the monitor is already fighting for GIL. On top of GIL contention from §4.1, this is what turns "slow" into "stalled."

### 4.5 DDict sizing

`Batch(pool_nodes=…)` controls how much capacity backs `results_ddict`. If `pool_nodes` is default/small, 10k entries of `(result, tb, raised, stdout, stderr)` can saturate the DDict pool, back-pressuring the Batch dispatcher. Worth checking: does stall correlate with a `pool_nodes` setting, and does raising it help?

### 4.6 GIL pileup

Threads in the worker process: asyncio loop, `_zmq_task_loop`, `_zmq_result_loop`, `dragon_monitor_loop`, plus Dragon's own internal Batch dispatch thread. A tight Python-level loop over 10k dict keys + DDict lookups in the monitor is exactly the kind of workload that monopolizes the GIL. When the asyncio loop can't get CPU, submit acks, deliveries, and shutdown all appear to hang.

---

## 5. Recommended changes (ordered by expected impact)

1. **Stop polling DDict.** Use `Task.get(block=True)` (event-based) via a small thread pool, or a Dragon `wait_any`-type primitive if exposed. If polling must stay, **bound sweep size** (probe K keys per pass, rotate) and use an adaptive sleep. Just doing this probably unblocks the 10k case.
2. **Register `_monitored_batches[uid]` inside the build loop**, not after. Results can be delivered while the remaining tasks are still being translated.
3. **Make `build_task` sync** and/or yield control periodically (`await asyncio.sleep(0)` every N) so the loop can service `call_soon_threadsafe` during the build phase.
4. **Batch the result path.** Drain `_zmq_result_queue` non-empty into a list, send one pickled list per ZMQ message; on the client, dispatch completions in a single `call_soon_threadsafe`. Mirrors what submit already does.
5. **Chunk the submit blob.** Break 10k into ~1k-task messages so the worker can start building/dispatching earlier and memory spikes are bounded.
6. **Surface `pool_nodes`** and document/recommend a sizing rule (≈ expected concurrent in-flight results).
7. Add a throughput/latency log (queue depths, sweep time, DDict probes/s) so the failure mode is visible rather than appearing as a hang.

(1) and (2) together likely turn the stall into "just slow"; (3)–(5) close the remaining gap with Dragon native.

---

## 6. Design flaws in the isolation architecture

The client/worker split exists so the user's process stays free of Dragon's `multiprocessing` patches — enabling multiple Dragon instances, or Dragon alongside a non-Dragon backend (Concurrent, Dask, etc.) in the same user process. Accepting that requirement as given, the current implementation still has genuine design flaws. Ordered by severity.

### 6.1 Client and Worker live in the same module that imports `dragon` at top level

dragon_test.py:26-62 does `import dragon` unconditionally at module scope, *before* either class is defined. The user's "clean" process imports `DragonExecutionBackendV3Client` from this module → triggers `import dragon` in that process. If `import dragon` has any module-level side effects (env manipulation, shm handle creation, thread spawn, implicit multiprocessing patch on some platforms/versions), isolation is already leaky before the subprocess even starts. The explicit `_patch_multiprocessing()` call in `_bootstrap_dragon_runtime` suggests patching isn't automatic on import, but you're one Dragon release away from that changing silently.

**Fix:** split `DragonExecutionBackendV3Client` into its own module that imports *only* `subprocess`, `zmq`, and stdlib. The worker module (with `import dragon`) is only ever loaded inside the subprocess via `python -m`.

### 6.2 Coordination dir is PID-keyed → multiple instances in the same process collide

```python
self._coordination_dir = coordination_dir or os.path.join(
    os.path.expanduser("~"), ".rhapsody", "sessions", str(os.getpid())
)
```
(dragon_test.py:813-815)

Two `DragonExecutionBackendV3Client(...)` instances in the same user process write and poll the **same** `endpoints.json`. Whichever worker writes last wins; the other client connects to the wrong endpoints. The stated goal "spawn multiple dragon instances" doesn't work as-is. Add a uuid/counter suffix.

### 6.3 Dragon runtime-level isolation between instances isn't configured

Even once §6.2 is fixed, two `dragon` CLI launches on the same host share shm namespaces, port ranges, and (on WLM) allocation accounting. The client passes `launcher_args` through unchanged but doesn't set a per-instance `DRAGON_DEFAULT_SEG_SZ` / shm dir / WLM allocation tag. Two instances may appear to come up and then stomp on each other's channels. Worth verifying what Dragon actually needs to isolate multiple runtimes on one host and plumbing it through — or documenting that instances must be on disjoint node sets.

### 6.4 No liveness/heartbeat for the worker subprocess after startup

`_wait_for_endpoints` checks `self._worker_proc.poll()` during init (dragon_test.py:896-901) and then nothing ever does. If the head process dies mid-run (Dragon bug, node fail, OOM, DDict exhausted), the client:
- Keeps pushing tasks into a PUSH socket with HWM=0 → they queue unbounded in the client.
- Waits for results that never come.
- Looks like a hang with no error.

For an isolation architecture whose whole selling point is "the subprocess can fail independently," failure is invisible. Needs at least a periodic `poll()` in the result thread and a heartbeat ping/pong so user code gets a real exception.

### 6.5 No handshake; tasks can queue into nothing

`_bind_zmq_early` intentionally binds *before* `Batch()` so the client can connect early (dragon_test.py:623). The comment (dragon_test.py:605-608) celebrates this: "Tasks sent in that window queue up in ZMQ (HWM=0) and are drained once the task thread starts." But combined with §6.4: if `Batch()` raises during init, `endpoints.json` is already written, the client connects successfully, and sends tasks that vanish into ZMQ buffers attached to a dead process. No error on the client side. A simple "READY" message on a control channel after `_start_zmq_threads` would close this.

### 6.6 Shutdown races with coordination-dir cleanup

`shutdown()` detaches after 5 s if the worker is still alive ("Dragon launcher cleaning up nodes; detaching", dragon_test.py:1030), then immediately:
```python
shutil.rmtree(self._coordination_dir, ignore_errors=True)
```
(dragon_test.py:1049) — while the detached Dragon launcher may still be writing to `worker.stdout`/`worker.stderr` in that directory and reading config. The cleanup should be deferred (register on detached-proc exit) or skipped when detaching.

Also: `_deliver_batch` → `_zmq_result_queue.put` can fire on the asyncio loop *after* `_zmq_result_loop` has already exited on its `None` sentinel (ordering in `shutdown`, dragon_test.py:753-759, is: send sentinel, join threads, then close). In-flight results at shutdown can land in a queue no one reads → silently dropped.

### 6.7 Exception payload is cloudpickled blindly

`_wrapped` callback (dragon_test.py:689-699) stuffs `task.get("exception")` into the result dict, which `_zmq_result_loop` then `cloudpickle.dumps`. If the exception isn't pickleable (some Dragon-native exception types wrapping channel handles), the send raises inside `_zmq_result_loop`, logs a warning, and the task disappears from the client's world — no DONE, no FAILED, no timeout. `_pending_tasks[uid]` leaks forever. Needs a fallback: pickle the `repr(exc)` + traceback string when the real exception won't serialize.

### What *isn't* a design flaw given this context

The ZMQ transport, the cloudpickle payload, the dedicated `dragon_client_results` thread, and the subprocess launch are all the right shape for the isolation goal. Those are the costs of the requirement, not design mistakes.

### Priority summary

- **Must fix if multi-instance is a real goal:** §6.1, §6.2, §6.3
- **Must fix for production robustness:** §6.4, §6.5, §6.7
- **Cosmetic until it burns someone:** §6.6

