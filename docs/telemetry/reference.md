# Events, Metrics & Spans Reference

This page is the authoritative reference for every piece of data that RHAPSODY telemetry emits: lifecycle events, resource updates, OTel metrics instruments, and span structure.

---

## Event model

All telemetry originates as **frozen Python dataclasses** (`BaseEvent` subclasses). They are:

- **Immutable** — safe for zero-copy fan-out to multiple subscribers.
- **Timestamped** — every event carries two wall-clock times: `event_time` (when it occurred in the task's history) and `emit_time` (when it entered the event bus). Their difference is the queue latency.
- **Identified** — every event carries a unique `event_id` (UUID4 hex), a `session_id`, and a `backend` name.
- **OTel-enriched** in the JSONL file — the serializer adds `trace_id` and `span_id` fields that link the event to the correct OTel span.

---

## Base fields (all events)

| Field | Type | Description |
|---|---|---|
| `event_id` | `str` | UUID4 hex, unique per event |
| `event_type` | `str` | Discriminator string (matches the class name) |
| `event_time` | `float` | Wall-clock time the event occurred (`time.time()`) |
| `emit_time` | `float` | Wall-clock time of queue insertion (`time.time()`) |
| `session_id` | `str` | Owning session identifier |
| `backend` | `str` | Backend name (e.g. `"dragon_v3"`, `"dask"`, `"concurrent"`) |
| `task_id` | `str \| None` | Task UID — `None` for session-level events |
| `node_id` | `str \| None` | Hostname or worker address — `None` when not applicable |
| `attributes` | `dict` | Optional metadata (see per-event notes below) |

---

## Session lifecycle events

### `SessionStarted`

Emitted once when `await telemetry.start()` activates the dispatch loop.

```
event_type = "SessionStarted"
task_id    = None
```

In OTel terms this event coincides with the **root session span** being opened. Its `span_id` in the JSONL file is the session span's ID — the parent of every task span.

---

### `SessionEnded`

Emitted once when `await telemetry.stop()` is called.

| Extra field | Type | Description |
|---|---|---|
| `duration_seconds` | `float` | Total wall-clock duration of the session |

The `span_id` in the JSONL file matches the session span that is closed at this point.

---

## Task lifecycle events

The canonical lifecycle is:

```
TaskSubmitted → TaskQueued → TaskStarted → TaskCompleted
                                        ↘ TaskFailed
```

`TaskQueued` is optional (not all backends emit it).

### `TaskSubmitted`

Emitted by the session immediately after routing assigns a backend to the task.

```
attributes = {
    "executable": str,   # binary path, function name, or model name
    "task_type":  str,   # "compute", "function", "ai", …
}
```

!!! note "OTel correlation"
    At submission time no task span exists yet.  The JSONL `trace_id` is the session trace ID (so the event is queryable within the right trace), but `span_id` is `null`.

---

### `TaskQueued`

Emitted just before the task is handed to the backend's internal queue. Marks the boundary between session-level routing latency and backend-level scheduling latency.

```
attributes = { "executable": str, "task_type": str }
```

Same OTel correlation as `TaskSubmitted` — trace-linked but no task span yet.

---

### `TaskStarted`

Emitted when the task transitions to `RUNNING` state. This is the authoritative "execution began" timestamp.

```
attributes = { "executable": str, "task_type": str }
```

!!! note "OTel correlation"
    Opening this event **opens the task OTel span** (named `"task"`) as a child of the session span. The JSONL `trace_id` and `span_id` are those of the freshly opened task span.

---

### `TaskCompleted`

Emitted when the task reaches `DONE` state.

| Extra field | Type | Description |
|---|---|---|
| `duration_seconds` | `float` | Wall-clock time from `RUNNING` → `DONE`. `0.0` if `RUNNING` was never recorded (`incomplete_lifecycle=True` in attributes). |

```
attributes = {
    "executable": str,
    "task_type":  str,
    # optionally:
    "incomplete_lifecycle": True,   # STARTED was not recorded
}
```

The task span is **closed** at this point. `span_id` in the JSONL is the closed span's ID.

---

### `TaskFailed`

Emitted when the task reaches `FAILED` state.

| Extra field | Type | Description |
|---|---|---|
| `duration_seconds` | `float` | Same semantics as `TaskCompleted`. |
| `error_type` | `str` | Python exception class name, or `"unknown"`. |

```
attributes = {
    "executable":  str,
    "task_type":   str,
    "error_type":  str,
    # optionally:
    "incomplete_lifecycle": True,
}
```

---

## Resource update events

### `ResourceUpdate`

Emitted periodically by backend adapters (default every 5 s). Every `ResourceUpdate` carries a `resource_scope` discriminator that tells consumers exactly what the event represents and which fields are populated.

| `resource_scope` | `gpu_id` | Populated fields |
|---|---|---|
| `"per_node"` | `null` | `cpu_percent`, `memory_percent`, `gpu_percent` (aggregate), `disk_*`, `net_*` |
| `"per_gpu"` | `0..N` | `gpu_percent`, `gpu_id` only — all others are `null` |

`resource_scope` is computed automatically from `gpu_id` and is always present. Consumers should use it as the primary discriminator instead of checking `gpu_id is None`.

#### Full field reference

| Field | Type | `per_node` | `per_gpu` | Description |
|---|---|:---:|:---:|---|
| `resource_scope` | `str` | `"per_node"` | `"per_gpu"` | Scope discriminator, always set |
| `gpu_id` | `int \| None` | `null` | `0..N` | Device index; `null` on node-level events |
| `cpu_percent` | `float \| None` | ✅ float 0–100 | `null` | Node CPU utilization |
| `memory_percent` | `float \| None` | ✅ float 0–100 | `null` | Node memory utilization |
| `gpu_percent` | `float \| None` | ✅ or `null` | ✅ float 0–100 | GPU utilization; `null` on `per_node` when backend has no GPU |
| `disk_read_bytes` | `float \| None` | ✅ or `null` | `null` | Bytes read since previous poll (delta). `null` on first poll |
| `disk_write_bytes` | `float \| None` | ✅ or `null` | `null` | Bytes written since previous poll |
| `net_sent_bytes` | `float \| None` | ✅ or `null` | `null` | Bytes sent since previous poll |
| `net_recv_bytes` | `float \| None` | ✅ or `null` | `null` | Bytes received since previous poll |

!!! info "OTel correlation"
    `ResourceUpdate` events are **anchored to the session span**, not to a task span. Their `trace_id` and `span_id` in the JSONL file match the root session span, making all resource data queryable within the session's trace in any OTel-compatible tool.

### Schema examples

**`per_node` event (gpu_id=null, no GPU on this node):**
```json
{
  "resource_scope": "per_node",
  "gpu_id": null,
  "cpu_percent": 41.2,
  "memory_percent": 28.7,
  "gpu_percent": null,
  "disk_read_bytes": 4096.0,
  "disk_write_bytes": 0.0,
  "net_sent_bytes": 512.0,
  "net_recv_bytes": 1024.0
}
```

**`per_node` event (gpu_id=null, aggregate GPU across 4 devices):**
```json
{
  "resource_scope": "per_node",
  "gpu_id": null,
  "cpu_percent": 78.3,
  "memory_percent": 62.1,
  "gpu_percent": 87.0,
  "disk_read_bytes": 8192.0,
  "disk_write_bytes": 2048.0,
  "net_sent_bytes": 1024.0,
  "net_recv_bytes": 4096.0
}
```

**`per_gpu` event (one per device):**
```json
{
  "resource_scope": "per_gpu",
  "gpu_id": 2,
  "cpu_percent": null,
  "memory_percent": null,
  "gpu_percent": 75.2,
  "disk_read_bytes": null,
  "disk_write_bytes": null,
  "net_sent_bytes": null,
  "net_recv_bytes": null
}
```

### Node-level vs per-device semantics

```
One poll cycle on a 4-GPU node emits 5 ResourceUpdate events:

  resource_scope="per_node", gpu_id=null  → cpu, mem, disk, net, max-GPU%
  resource_scope="per_gpu",  gpu_id=0     → gpu_percent only
  resource_scope="per_gpu",  gpu_id=1     → gpu_percent only
  resource_scope="per_gpu",  gpu_id=2     → gpu_percent only
  resource_scope="per_gpu",  gpu_id=3     → gpu_percent only
```

To get CPU/memory for a specific GPU node, join `per_gpu` events to the `per_node` event on `(session_id, node_id)` within the same poll window.

### Adapter coverage matrix

| Metric | `resource_scope` | Concurrent | Dask | Dragon |
|---|---|:---:|:---:|:---:|
| `cpu_percent` | `per_node` only | ✅ | ✅ | ✅ |
| `memory_percent` | `per_node` only | ✅ | ✅ | ✅ |
| `gpu_percent` (aggregate) | `per_node` | ✅ if NVIDIA present | ❌ | ✅ |
| `gpu_percent` + `gpu_id` | `per_gpu` | ✅ if NVIDIA present | ❌ | ✅ |
| `disk_read/write_bytes` | `per_node` only | ✅ (delta) | ✅ (delta, first poll = null) | ✅ (delta) |
| `net_sent/recv_bytes` | `per_node` only | ✅ (delta) | ❌ | ✅ (delta) |

---

## OTel metrics instruments

The `TelemetryManager` registers the following instruments on a `rhapsody` meter:

| Instrument name | Type | Unit | Description |
|---|---|---|---|
| `tasks_submitted` | Counter | tasks | Total tasks submitted to the session |
| `tasks_started` | Counter | tasks | Total tasks that transitioned to RUNNING |
| `tasks_completed` | Counter | tasks | Total tasks that reached DONE |
| `tasks_failed` | Counter | tasks | Total tasks that reached FAILED |
| `tasks_running` | UpDownCounter | tasks | Tasks currently in RUNNING state |
| `task_duration_seconds` | Histogram | seconds | Per-task execution duration (RUNNING → DONE/FAILED) |
| `node_cpu_utilization` | UpDownCounter | % | Current CPU % per node (last-seen delta tracking) |
| `node_memory_utilization` | UpDownCounter | % | Current memory % per node |
| `node_gpu_utilization` | UpDownCounter | % | Current GPU % per node |

All instruments carry `session_id` and `backend` as label dimensions. Task instruments additionally carry `task_id`, `executable`, and `task_type`.

---

## OTel span structure

RHAPSODY emits two span types, both in the `"rhapsody"` tracer scope:

### Session span (`name="session"`)

```
session span
├── attributes:
│     session_id = "session.0001"
│     backend    = "rhapsody"
└── parent: none  (this is the trace root)
```

All task spans and resource events are linked to this trace via the same `trace_id`.

### Task span (`name="task"`)

```
task span
├── parent:  session span
├── attributes:
│     session_id = "session.0001"
│     backend    = "dragon_v3"
│     task_id    = "task.0042"
│     executable = "/usr/bin/python"
│     task_type  = "compute"
│     status     = "completed" | "failed"
│     error_type = "RuntimeError"   (failed tasks only)
└── duration: TaskStarted.event_time → TaskCompleted.event_time
```

The span duration is derived from the backend's own task history timestamps — not from event queue latency — so it reflects true execution time.

### Trace–span correlation in the JSONL file

Every line in the checkpoint file carries two extra keys added at serialization time:

| JSONL key | Source |
|---|---|
| `trace_id` | Hex string of the OTel trace ID |
| `span_id` | Hex string of the correlated span ID (`null` for TaskSubmitted/TaskQueued) |
| `name` | Alias for `event_type` (OTel-compatible) |

This makes the JSONL file directly importable into any OTel-aware tool as a timeline.

---

## JSONL checkpoint format

Each line in the `.telemetry.jsonl` file is a JSON object with a `"section"` discriminator:

```json
{"section": "event",  "event_type": "SessionStarted", "trace_id": "0x…", "span_id": "0x…", …}
{"section": "event",  "event_type": "TaskStarted",    "task_id": "…", "trace_id": "0x…", "span_id": "0x…", …}
{"section": "event", "event_type": "ResourceUpdate", "resource_scope": "per_node", "gpu_id": null, "cpu_percent": 42.3, "memory_percent": 28.7, "gpu_percent": null, "disk_read_bytes": 4096.0, …}
{"section": "event", "event_type": "ResourceUpdate", "resource_scope": "per_gpu",  "gpu_id": 0,    "cpu_percent": null, "memory_percent": null, "gpu_percent": 75.2, "disk_read_bytes": null, …}
{"section": "metric", "tasks_submitted": 100, "tasks_completed": 98, "tasks_running": 2, …}
{"section": "span",   "name": "task", "span_id": "0x…", "trace_id": "0x…", "parent_span_id": "0x…", "duration_ms": 1234.5, …}
```

The file is written line-buffered, so it is readable during a live run.
