# Events, Metrics & Spans Reference

This page is the authoritative reference for every piece of data that RHAPSODY telemetry emits: lifecycle events, resource updates, OTel metrics instruments, and span structure.

---

## Event model

All telemetry originates as **frozen Python dataclasses** (`BaseEvent` subclasses). They are:

- **Immutable** — safe for zero-copy fan-out to multiple subscribers.
- **Timestamped** — every event carries two wall-clock times: `event_time` (when it occurred in the task's history) and `emit_time` (when it entered the event bus). Their difference is the queue latency.
- **Identified** — every event carries a unique `event_id` (UUID4 hex), a `session_id`, and a `backend` name.
- **OTel-enriched** in the JSONL file — the serializer adds `trace_id` and `span_id` fields that link the event to the correct OTel span.

Backends can emit events too, through their emitter (see [Backends, emitters and instrumentations](#backends-emitters-and-instrumentations)): their `event_type` is a namespaced name such as `rhapsody.inference.BatchEnded`, not a class name.

---

## Base fields (all events)

| Field | Type | Description |
|---|---|---|
| `event_id` | `str` | UUID4 hex, unique per event |
| `event_type` | `str` | Discriminator string (the class name; for events emitted by a backend, its namespaced name, e.g. `rhapsody.inference.BatchEnded`) |
| `event_time` | `float` | Wall-clock time the event occurred (`time.time()`) |
| `emit_time` | `float` | Wall-clock time of queue insertion (`time.time()`) |
| `session_id` | `str` | Owning session identifier |
| `backend` | `str` | Backend name (e.g. `"dragon"`, `"dask"`, `"concurrent"`) |
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
(TaskCreated) → TaskSubmitted → (TaskQueued) → TaskStarted → TaskCompleted
                                                           ↘ TaskFailed
                                                           ↘ TaskCanceled
```

### `TaskCreated`

Emitted by `Session.submit_tasks()` when the future is bound and the task is registered, before backend routing and submission.

```
attributes = {
    "executable": str,   # function name or binary path
    "task_type":  str,   # "compute", "function", etc.
}
```

Because `TaskCreated` fires before routing, its `backend` field is empty (`""`). The assigned backend appears on the subsequent `TaskSubmitted` event.

---

### `TaskSubmitted`

Emitted by the session immediately after routing assigns a backend to the task.

```
attributes = {
    "executable": str,   # binary path, function name, or model name
    "task_type":  str,   # "compute", "function", "ai", …
}
```

!!! note "OTel correlation"
    The task span is already open at this point (opened at `TaskCreated`). The JSONL `trace_id` and `span_id` are those of the task span, making this event queryable as a child of the session span in any OTel-compatible tool.

---

### `TaskQueued`

Emitted just before the task is handed to the backend's internal queue. Marks the boundary between session-level routing latency and backend-level scheduling latency.

```
attributes = { "executable": str, "task_type": str }
```

Same OTel correlation as `TaskSubmitted` — the task span is already open; `trace_id` and `span_id` are both set.

---

### `TaskStarted`

Emitted when the task transitions to `RUNNING` state. This is the authoritative "execution began" timestamp.

```
attributes = { "executable": str, "task_type": str }
```

!!! note "OTel correlation"
    The task OTel span (named `"task"`) was opened at `TaskCreated`. This event is recorded as an annotation on that already-open span. The JSONL `trace_id` and `span_id` are those of the task span.

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

### `TaskCanceled`

Emitted when a task reaches `CANCELED` state (user-initiated cancellation or backend-level abort). Distinct from `TaskFailed` — cancellation is intentional, not an error.

| Extra field | Type | Description |
|---|---|---|
| `duration_seconds` | `float` | Wall-clock time from `RUNNING` → `CANCELED`. `0.0` if RUNNING was never recorded. |

```
attributes = {
    "executable": str,
    "task_type":  str,
    # optionally:
    "incomplete_lifecycle": True,   # task canceled before RUNNING was seen
}
```

No `error_type` field. To distinguish cancellations from failures in a subscriber, filter on `event.event_type == "TaskCanceled"`.

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

## Custom events — `define_event()`

Application code and orchestration layers can define their own event types and emit them through the same telemetry bus — without modifying RHAPSODY source.

```python
from rhapsody.telemetry import define_event
from rhapsody.telemetry.events import make_event

# Define a typed event class at module level
LineTimer = define_event(
    "myapp.LineTimer",
    label=str,
    duration_ms=float,
)
```

`define_event(name, **fields)` creates a frozen `BaseEvent` subclass at runtime via `dataclasses.make_dataclass`. It enforces two rules:

1. **Namespace required** — `name` must contain a `.` (e.g. `"myapp.LineTimer"`). This prevents collisions with RHAPSODY's own event names.
2. **Base fields protected** — fields declared on `BaseEvent` (`event_id`, `event_time`, `session_id`, etc.) cannot be shadowed. Attempting to do so raises `ValueError` at definition time.

### Emitting a custom event

```python
import time
from rhapsody.telemetry.events import make_event

t0 = time.time()
# ... your code ...
duration_ms = (time.time() - t0) * 1000

telemetry.emit(
    make_event(
        LineTimer,
        session_id=telemetry.session_id,
        backend="app",
        label="my_block",
        duration_ms=duration_ms,
    )
)
```

`make_event(cls, *, session_id, backend, **fields)` stamps the event with a fresh `event_id`, `event_time`, and `emit_time`. Pass `event_time=t0` explicitly if you want the event timestamped to when the block *started* rather than when it was emitted.

`telemetry.emit()` is **synchronous** — no `await` needed.

### Receiving custom events in a subscriber

```python
def on_event(event):
    if event.event_type == "myapp.LineTimer":
        print(f"{event.label}: {event.duration_ms:.1f} ms")

telemetry.subscribe(on_event)
```

Custom events flow through the same dispatch loop as system events and appear in the JSONL checkpoint file with their full field set.

### Field type defaults

When `define_event` is called with a bare type (not a `(type, default)` tuple), defaults are assigned automatically:

| Field type | Default |
|---|---|
| `float` | `0.0` |
| `int` | `0` |
| `str` | `""` |
| `bool` | `False` |
| Any other type | `None` |

To specify a custom default, pass a `(type, default)` tuple:

```python
Checkpoint = define_event(
    "myapp.Checkpoint",
    step=(int, -1),           # default -1
    loss=(float, float("inf")),
    label=str,                # default ""
)
```

---

## OTel metrics instruments

The task instrumentation (`rhapsody.telemetry.instrumentation.task`) records the following instruments on the `rhapsody` meter scope, from the task and resource events of every backend:

| Instrument name | Type | Unit | Description |
|---|---|---|---|
| `tasks_submitted` | Counter | tasks | Total tasks submitted to the session |
| `tasks_started` | Counter | tasks | Total tasks that transitioned to RUNNING |
| `tasks_completed` | Counter | tasks | Total tasks that reached DONE |
| `tasks_failed` | Counter | tasks | Total tasks that reached FAILED |
| `tasks_canceled` | Counter | tasks | Total tasks that reached CANCELED |
| `tasks_running` | UpDownCounter | tasks | Tasks currently in RUNNING state |
| `task_duration_seconds` | Histogram | seconds | Per-task execution duration (RUNNING → DONE/FAILED) |
| `node_cpu_utilization` | UpDownCounter | % | Current CPU % per node (last-seen delta tracking) |
| `node_memory_utilization` | UpDownCounter | % | Current memory % per node |
| `node_gpu_utilization` | UpDownCounter | % | Current GPU % per node |

The *Unit* column describes the quantity; these instruments do not set an OpenTelemetry `unit` (a known limitation, kept for compatibility).

All instruments carry `session_id` and `backend` as label dimensions. Task instruments additionally carry `executable` and `task_type`. They do not carry `task_id`: one series per task would grow without bound. Per-task identity is on the task spans.

---

## Inference telemetry (experimental)

!!! warning "Experimental"
    The inference telemetry and its attribute names follow the OpenTelemetry GenAI semantic conventions, which are still in development (limitation L8). Names and attributes may change.

When a `DragonVllmInferenceBackend` is part of a session or workflow engine with telemetry started, RHAPSODY records, for every batch the backend sends to Dragon:

- one **batch span** (`chat {model}`) with the batch's real start and end times, linked to the tasks of the batch;
- **per-task attributes** on the task spans (batch id, prompt count, queue wait, timeouts);
- **histograms and counters** for queue wait, batch duration, Dragon's model time, requests, prompts, timed-out prompts and batches.

Nothing needs to be enabled besides telemetry itself (`await session.start_telemetry()` or `await flow.start_telemetry()`) and `pip install 'rhapsody-py[telemetry]'` (`opentelemetry-sdk>=1.30`, the first release with histogram bucket advice).

```
DragonVllmInferenceBackend ──emit──▶ rhapsody.inference.BatchStarted / BatchEnded   (plain data)
                                          │ routed by backend name
                                          ▼
                       InferenceInstrumentation (OpenTelemetry API only)
                                          │
                       ┌──────────────────┴──────────────────┐
                 `chat {model}` span + task attributes      histograms + counters
```

### Events

The backend reports two events per batch through its emitter. They are ordinary RHAPSODY events: they reach `subscribe()` callbacks and the JSONL file like any other. Their own `task_id` is `None`, so their `trace_id`/`span_id` in the JSONL file resolve to the session span.

`rhapsody.inference.BatchStarted` is emitted right before the batch is sent to Dragon (`event_time` is the dispatch time):

| Field | Type | Description |
|---|---|---|
| `batch_id` | `int` | Counts up from 1 for this backend |
| `model` | `str` | The configured model name (may be a path, L14) |
| `batch_prompts` | `int` | Prompts in the batch |
| `batch_requests` | `int` | Requests in the batch (an AITask or one `generate()` call is one request) |
| `batch_trigger` | `str` | `"size"` when the batch reached `max_batch_size` requests, else `"deadline"` |
| `task_ids` | `list[str]` | UIDs of the AITasks in the batch |

`rhapsody.inference.BatchEnded` is emitted when the responses are in (`event_time` is the real end), before the tasks are reported DONE or FAILED:

| Field | Type | Description |
|---|---|---|
| `batch_id`, `model`, `batch_trigger`, `batch_prompts`, `batch_requests` | | As above, repeated so the event stands alone |
| `batch_duration_s` | `float` | Time from sending the batch until its last response |
| `timed_out_prompts` | `int` | Prompts that got no response within the batch deadline |
| `dragon_model_duration_s` | `float` (optional) | Dragon's model inference time of the batch, from the first response (Dragon rounds it to 0.01 s) |
| `dragon_worker_host` | `str` (optional) | Host name of the Dragon worker, from the first response |
| `requests` | `list[dict]` | One row per request: `task_id` (`None` for `generate()` and HTTP requests), `prompt_count`, `queue_duration_s`, `timed_out_prompts` |

A request's **source** is derived: `task` when its row has a `task_id`, `api` otherwise. `BatchEnded` carries one row per request, so it is part of the JSONL file (about 90 bytes per request) and of the in-memory event log (about 260 bytes per request), measured on 1024-request batches.

### Metrics

Recorded on the `rhapsody.instrumentation.inference` meter scope. No instrument carries a task or batch id. A counter or histogram with no events has no data point.

| Instrument | Type | Unit | Attributes | Description |
|---|---|---|---|---|
| `rhapsody.inference.request.queue.duration` | Histogram | `s` | base + `rhapsody.inference.request.source` | Time a request waited in the batcher queue before its batch was sent |
| `rhapsody.inference.batch.duration` | Histogram | `s` | base | Time from sending a batch until its last response |
| `rhapsody.inference.model.duration` | Histogram | `s` | base | Dragon's model time per batch; recorded only when Dragon reports a value above 0 |
| `rhapsody.inference.requests` | Counter | `{request}` | base + source | Requests batched |
| `rhapsody.inference.prompts` | Counter | `{prompt}` | base + source | Prompts batched |
| `rhapsody.inference.timed_out.prompts` | Counter | `{prompt}` | base + source | Prompts that got no response within the batch deadline |
| `rhapsody.inference.batches` | Counter | `{batch}` | base + `rhapsody.inference.batch.trigger` | Batches sent to Dragon |

*base* is `session_id`, `backend` and `gen_ai.request.model`. The three histograms use the advisory bucket bounds 0.01, 0.02, 0.04 ... 327.68 seconds (10 ms doubling, 16 bounds).

### Reading it

```python
telemetry = await session.start_telemetry(checkpoint_path="./telemetry/")
# ... run AITasks ...
await session.close()  # data stays readable after the session closed

for rm in telemetry.read_metrics().resource_metrics:
    for sm in rm.scope_metrics:
        if sm.scope.name == "rhapsody.instrumentation.inference":
            for metric in sm.metrics:
                print(metric.name, [dp.attributes for dp in metric.data.data_points])

batches = [s for s in telemetry.read_traces() if s.name.startswith("chat")]
```

To forward the data to an OTel backend, pass a `MetricReader` or `SpanProcessor` to `start_telemetry()` (see [Integrations](integrations.md)).

### Privacy

Events, spans and metrics carry numbers, ids, the model name (which can be a filesystem path, L14) and the Dragon worker's host name. They never carry prompt or response text; a test checks every payload the backend emits.

### What is not available

- Aggregated inference metrics produced by Dragon Telemetry (owned by another group).
- A live queue depth and running totals between batches (limitation L16): only what the batch events carry is recorded.
- Token counts: Dragon computes them but does not forward them.
- Trace context into Dragon and vLLM (limitation L2): vLLM's own spans, if Dragon enables them, are separate traces.

---

## OTel span structure

RHAPSODY emits three span types. The session and task spans are in the `"rhapsody"` tracer scope; the inference batch span is in `"rhapsody.instrumentation.inference"`:

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
│     backend    = "dragon"
│     task_id    = "task.0042"
│     executable = "/usr/bin/python"
│     task_type  = "compute"
│     status     = "completed" | "failed"
│     error_type = "RuntimeError"   (failed tasks only)
└── duration: TaskCreated (span open) → TaskCompleted/Failed/Canceled (span close)
```

The span covers the full task lifecycle from registration to terminal state. To get pure execution time, use the `task_duration_seconds` OTel histogram metric, which is recorded from `TaskStarted` → terminal state.

### Inference batch span (`name="chat {model}"`)

One span per batch sent by a `DragonVllmInferenceBackend` (experimental). The name is `chat` when the model is unknown.

```
chat span   (kind CLIENT)
├── parent:  session span
├── links:   at most 128 task spans that were open when the batch was sent (L7)
├── attributes:
│     session_id, backend
│     gen_ai.operation.name        = "chat"
│     gen_ai.request.model
│     rhapsody.inference.batch.id
│     rhapsody.inference.batch.size       prompts in the batch
│     rhapsody.inference.batch.requests   requests in the batch (the real count, whatever the link cap)
│     rhapsody.inference.batch.trigger    "size" | "deadline"
│     rhapsody.inference.dragon.model.duration   (when Dragon reports it)
│     rhapsody.inference.dragon.worker.host      (when Dragon reports it)
│     error.type = "TimeoutError", rhapsody.inference.timed_out_prompts   (when prompts timed out)
├── status:  ERROR when prompts timed out, or "batch did not finish" when the batch never ended
└── time:    the real dispatch time → the real time of the last response
```

The batch span is not parented to a task, because a batch can serve tasks of several workflows; the span enrichers (such as AsyncFlow's `workflow_id`) are not applied to it. When the batch ends, each AITask span in it gets these attributes: `gen_ai.request.model`, `rhapsody.inference.batch.id`, `rhapsody.inference.prompt_count`, `rhapsody.inference.request.queue.duration` and, if its prompts timed out, `error.type` and `rhapsody.inference.timed_out_prompts`.

### Trace–span correlation in the JSONL file

Every line in the checkpoint file carries two extra keys added at serialization time:

| JSONL key | Source |
|---|---|
| `trace_id` | Hex string of the OTel trace ID |
| `span_id` | Hex string of the correlated span ID — always non-null for all task lifecycle and session events |
| `name` | Alias for `event_type` (OTel-compatible) |

This makes the JSONL file directly importable into any OTel-aware tool as a timeline.

---

## Span annotation and enrichment API

These methods let application code interact with the active OTel span without importing OTel directly.

### `span_scope(name, attributes=None)`

Context manager that opens a child span under the currently active span (or under the session span when called outside a task context).

```python
with telemetry.span_scope("preprocessing", {"dataset": "train_2024"}):
    # Any spans opened here will be children of "preprocessing"
    process_data()
```

Useful for grouping a sequence of operations into a named sub-trace. If telemetry is not started, `span_scope()` is a no-op.

---

### `annotate_current_span(event)`

Records a `BaseEvent` as an **annotation** (OTel span event) on the currently active span. Does nothing if there is no active span.

```python
def on_event(event):
    if event.event_type == "TaskCompleted":
        telemetry.annotate_current_span(event)

telemetry.subscribe(on_event)
```

Attributes are serialized to strings. Useful for linking RHAPSODY lifecycle events to custom spans opened with `span_scope()`.

---

### `register_span_enricher(func)`

Registers a callback that is called for every `TaskCreated` event and returns extra attributes to add to that task's OTel span.

```python
telemetry.register_span_enricher(
    lambda event: {"my.workflow_id": event.attributes.get("my.workflow_id", "")}
)
```

`func` receives a `BaseEvent` and must return a `dict[str, str | int | float | bool]`. Multiple enrichers are applied in registration order. Enrichers are the correct place to stamp custom grouping keys (workflow IDs, pipeline names, etc.) onto task spans without modifying RHAPSODY source.

---

### `export_as_otlp(path=None)`

Serializes all finished spans to a standards-compliant OTLP JSON payload (the `POST /v1/traces` wire format). Call after `stop()` when all spans are finalized.

```python
await session.close()

# Write to file
telemetry.export_as_otlp("./traces.json")

# Or get the JSON string directly
payload = telemetry.export_as_otlp()
```

The returned string is valid OTLP JSON and can be:
- Uploaded to any visualizer that accepts OTLP format (Jaeger, Grafana Tempo, Zipkin with conversion)
- POSTed directly to an OTLP HTTP endpoint
- Stored for offline analysis

---

## Backends, emitters and instrumentations

This section is for people who write a backend or add telemetry for a new domain. It is experimental.

### Why backends do not call OTel

RHAPSODY does not own the execution events of the runtimes behind its backends (Dragon, Dask, RADICAL-Pilot, ...). A backend is a translation layer: it turns what its runtime reports into RHAPSODY's normalized task lifecycle and into **plain data**. Recording that data in one place gives every runtime the same names, lifecycle and JSONL output, and keeps OpenTelemetry out of every backend. OTel *at the source* belongs to the owner of the source (for example Dragon enabling vLLM tracing). Where RHAPSODY owns the source, as in the inference batcher, a later step of the telemetry refactor (Tier 1) lets such code use the OTel API through the session's providers; the data and the names stay the same.

So a backend never imports OpenTelemetry and never sees the `TelemetryManager`. It reports through an *emitter*, and an *instrumentation* turns the data into spans and metrics.

```
backend (plain data)  ──emitter──▶  TelemetryManager (generic host)  ──routes by backend──▶  instrumentation (OTel API)
```

### Emitting from a backend

Every backend, execution, AI or data, has `self.telemetry`:

- It is a no-op emitter (`enabled` is `False`) until the `TelemetryManager` attaches the backend, which happens when a session or workflow engine with telemetry attaches it. The manager then calls the backend's `register_emitter(emitter)` hook, the counterpart of `register_callback`. The default stores the emitter in `self.telemetry`; override the hook to react and call `super().register_emitter(emitter)`. A backend therefore never checks for `None`.
- Today only the vLLM backend emits. The Concurrent, Dask and Dragon execution backends have the emitter but do not use it; their resource telemetry still comes from the adapters.

```python
if self.telemetry.enabled:  # skip building the attributes when telemetry is off
    self.telemetry.emit(
        "rhapsody.myservice.RequestServed",  # namespaced name: it must contain a "."
        {"count": 3},  # plain numbers and strings, never prompt or response text
        event_time=time.time(),  # optional: when it really happened (default: now)
        task_id=None,  # optional: the task the event belongs to
    )
```

Rules:

- The name must contain a `.` (otherwise `ValueError`); the convention is `rhapsody.<domain>.<Event>`.
- Events emitted while the manager is not running are dropped.
- Hand the attributes over and do not change them afterwards.
- Call `emit()` from the event-loop thread (limitation L17).
- Never put prompt or response text in the attributes.
- A pickled backend comes back with a no-op emitter.

`Emitter`, `NoopEmitter` and `BackendEmitter` live in `rhapsody.telemetry.emitter` (not re-exported from `rhapsody.telemetry`).

### Writing an instrumentation

An instrumentation is the only place that knows a telemetry domain: metric names, units, span attributes. It uses the OpenTelemetry **API** only, through the meter and tracer the host hands it. A backend names its domain with the class attribute `telemetry_domain`; a registry maps the name to the class.

```python
from rhapsody.telemetry.instrumentation import Instrumentation
from rhapsody.telemetry.instrumentation import register_instrumentation


class MyServiceInstrumentation(Instrumentation):
    scope_name = "rhapsody.instrumentation.myservice"

    def instrument(self, ctx):
        # Called at start (and again after a restart): create ALL instruments here
        # and reset ALL per-session state.
        self._served = ctx.meter.create_counter(
            "myservice.requests.served", unit="{request}", description="Requests served"
        )

    def on_event(self, event):
        # The host routes by backend name, so you also receive the backend's other
        # events (task events, for example): ignore the ones you do not own.
        if event.event_type != "rhapsody.myservice.RequestServed":
            return
        self._served.add(event.attributes.get("count", 1), {"backend": event.backend})


register_instrumentation("myservice", MyServiceInstrumentation)
```

On the backend side, set `telemetry_domain = "myservice"` and emit as above.

- `register_instrumentation(domain, cls)` must run **before** the backend is attached. `cls` can also be a `"module:ClassName"` string, which is imported on first use.
- The `InstrumentationContext` that `instrument()` receives has `meter` and `tracer` (scoped to `scope_name`), `session_id`, `backend_name`, `session_span_context()`, `task_span(task_id)` (the task's open span, or `None`) and `span_context_for(task_id)`.
- `uninstrument()` (optional) is called before the providers shut down, for example to end spans that are still open.
- Instrumentations are keyed by backend name (limitation L15). An unknown domain, or the built-in `task` domain, logs a warning and is ignored.
- The task and resource metrics are recorded by a built-in default instrumentation that sees every event, whatever its backend.
- A failing `instrument()` is logged with its traceback and the instrumentation gets no events; the session carries on. A failing `on_event` warns once per backend and instrumentation, then logs at debug level.

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

## Known limitations

Each limitation has a `TELEMETRY-LIMITATION(<id>)` comment at the place in the code where it applies: in the RHAPSODY source, except L9, which is in the radical.asyncflow source. References such as B2, T3, T9 or A1 are items of the maintainers' issue report for this work, not GitHub issues, and Tier 1, 2 and 3 are the steps of the planned telemetry refactor: Tier 1 injects the OpenTelemetry providers so that RHAPSODY-owned code can use the API directly, Tier 2 moves the resource metrics and the JSONL file onto OpenTelemetry mechanisms, Tier 3 removes the event bus.

| Id | Limitation | Planned |
|---|---|---|
| L2 | No trace context is passed to Dragon or vLLM. If Dragon enables vLLM's native tracing, its spans form separate traces that can only be matched to the batch span by time, model and host. | Tier 1 + a Dragon feature request (accept and forward `traceparent`, enable vLLM tracing) |
| L3 | The JSONL metric snapshot sums data points across all attributes (T9), so series split by attribute are added together in the file. `read_metrics()` keeps the dimensions. | Tier 2 |
| L4 | Task spans start and end at the time the dispatch loop processes the event, not at the event's own time (T3), so they can be offset from spans an instrumentation records with real times. | Tier 3 (Tier 2 partially) |
| L5 | `subscribe()` callbacks receive events only. OTel metrics recorded by an instrumentation are available through `read_metrics()` or a metric reader, not as events. | Tier 3 |
| L7 | The batch span links to at most 128 of the batch's task spans (the SDK keeps 128 links per span by default, `OTEL_SPAN_LINK_COUNT_LIMIT`); the real request count is on the span as `rhapsody.inference.batch.requests`. | Configuration |
| L8 | The inference attributes follow the OpenTelemetry GenAI semantic conventions, which are still in development and may change; `gen_ai.provider.name` is not set (vLLM has no registered value). | Follow the conventions when they stabilize |
| L9 | AsyncFlow always emits `TaskFailed` with `error_type="unknown"`, even when the task carries an exception (issue A1; AsyncFlow-owned). RHAPSODY's `Session` uses the exception's type. An inference timeout is still marked by `error.type` on the task span. | AsyncFlow: use the exception's type |
| L11 | After a timeout, the late responses of that batch stay in the shared response queue (issue B2), so the next batch may take its Dragon fields (model time, worker host) from the wrong batch. | B2 fix |
| L12 | A batch that fails on the batch processor's exception path emits no `BatchEnded` (issue B11): its batch span ends at the next batch or at the end of the session, and the queue times of its tasks are missing. | B11 fix |
| L14 | `gen_ai.request.model` is the configured model name and may be a local filesystem path. | Documented only |
| L15 | Instrumentations are keyed by backend name: two backends of one session with the same name share one slot, and the later one is ignored. `Session` and `WorkflowEngine` key their backends by name, so a same-name backend replaces the earlier one there; instrumentations follow the same rule. | None needed |
| L16 | Only what the batch events carry is recorded: there is no live queue depth and no cumulative totals between batches (the vLLM batcher is one sequential loop, one batch in flight at a time). | Tier 1 (the emitter handle gains `.meter`) |
| L17 | `Emitter.emit()` (the handle every backend gets as `backend.telemetry`) must be called on the event-loop thread, because the event queue is not thread-safe. A backend whose callbacks run in worker threads would have to hop onto the loop first. The vLLM batcher runs on the loop. | Tier 1 of the telemetry refactor, or a `call_soon_threadsafe` hop in the emitter |
| L18 | The OTLP export (`export_as_otlp`) writes OTel `SpanKind` values that are off by one from OTLP (CLIENT is 2 in OTel, 3 in OTLP), hardcodes the scope `rhapsody` and drops span links, so exported inference batch spans are mislabeled and lose their links (issue T19). | Tier 2 |
