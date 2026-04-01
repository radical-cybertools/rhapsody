# Quick Start

Telemetry is enabled with a single method call on the session. No external collector, database, or configuration file is needed.

## Installation

```bash
pip install 'rhapsody-py[telemetry]'
```

This pulls in `opentelemetry-sdk`. All other telemetry dependencies (`psutil`, `pynvml`) are optional and enabled when present.

---

## Minimal example

```python
import asyncio
from rhapsody import Session, ComputeTask
from rhapsody.backends import ConcurrentExecutionBackend

async def main():
    backend = ConcurrentExecutionBackend(num_workers=4)
    session = Session(backends=[backend])

    # 1. Enable telemetry — returns a TelemetryManager
    telemetry = session.enable_telemetry(
        resource_poll_interval=2.0,      # collect node metrics every 2 s
        checkpoint_path="./telemetry/",  # write a JSONL file here
    )
    await telemetry.start()

    # 2. Submit your workload
    tasks = [ComputeTask(executable="/bin/sleep", arguments=["0.1"]) for _ in range(20)]
    async with session:
        await session.submit_tasks(tasks)
        await session.wait_tasks(tasks)

    await telemetry.stop()

    # 3. Inspect results — no OTel knowledge required
    print(telemetry.summary())

asyncio.run(main())
```

### Sample output

```python
{
    "session_id": "session.0000",
    "duration_seconds": 3.42,
    "tasks": {
        "submitted": 20,
        "started":   20,
        "completed": 20,
        "failed":     0,
        "running":    0,
    },
    "duration": {
        "total_seconds": 2.18,
        "mean_seconds":  0.109,
        "min_seconds":   0.101,
        "max_seconds":   0.134,
    },
    "resources": {
        "concurrent/myhost": {
            "cpu_percent":       41.2,
            "memory_percent":    28.7,
            "gpu_percent":       None,
            "disk_read_bytes":   4096.0,
            "disk_write_bytes":  0.0,
            "net_sent_bytes":    512.0,
            "net_recv_bytes":   1024.0,
        }
    }
}
```

---

## Reading metrics and spans

```python
# Read raw OTel MetricsData (after telemetry.stop())
metrics = telemetry.read_metrics()
for rm in metrics.resource_metrics:
    for sm in rm.scope_metrics:
        for metric in sm.metrics:
            for dp in metric.data.data_points:
                print(metric.name, dp.value)

# Read per-task spans as plain dicts
for span in telemetry.task_spans():
    print(
        span["task_id"],
        span["status"],
        span["duration_seconds"],
        span["trace_id"],
        span["span_id"],
    )
```

---

## Subscribing to live events

```python
from rhapsody.telemetry import TaskCompleted, ResourceUpdate

def on_event(event):
    if isinstance(event, TaskCompleted):
        print(f"Task {event.task_id} done in {event.duration_seconds:.3f}s")
    elif isinstance(event, ResourceUpdate) and event.gpu_id is not None:
        print(f"GPU {event.gpu_id} on {event.node_id}: {event.gpu_percent:.1f}%")

telemetry.subscribe(on_event)
```

Async callbacks are also supported:

```python
async def async_handler(event):
    await my_metrics_queue.put(event)

telemetry.subscribe(async_handler)
```

Exceptions inside a subscriber are caught and logged — they never crash the dispatch loop.

---

## Using `TelemetrySubscriber` and `TelemetryReader`

For larger applications RHAPSODY provides typed wrapper classes:

```python
from rhapsody.telemetry import TelemetrySubscriber, TelemetryReader

sub    = TelemetrySubscriber(telemetry)
reader = TelemetryReader(telemetry)

# Push API
sub.subscribe(lambda e: print(e.event_type))

# Pull API — filter by session or task
task_spans = reader.read_traces(task_id="task.0042")
```

---

## Checkpoint file

When `checkpoint_path` is set, RHAPSODY writes a JSONL file:

```
telemetry/
└── rhapsody.session.session.0000.1775068460.telemetry.jsonl
```

The file is written **line-buffered** during the session — you can `tail -f` it in another terminal to watch events stream in real time.

### Plotting the checkpoint

```bash
python examples/plot.py telemetry/rhapsody.session.session.0000.1775068460.telemetry.jsonl
```

This produces a multi-panel PNG (shown in [Integrations — Real Run Visualization](integrations.md#real-run-visualization)) with CPU, memory, GPU (per-device), disk I/O, network I/O, task throughput, and task lifetime timeline.

---

## `enable_telemetry` parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `resource_poll_interval` | `float` | `5.0` | Seconds between resource metric polls |
| `checkpoint_interval` | `float \| None` | `None` | Seconds between periodic metric+span flushes. `None` = flush only at `stop()` |
| `checkpoint_path` | `str \| None` | `None` | Directory for the JSONL file. `None` = no file output |

---

## What gets collected automatically

Once `enable_telemetry()` is called, RHAPSODY hooks into the task state manager and registers the appropriate backend adapter. You do not need to instrument individual tasks.

| Source | Automatic? | Notes |
|---|---|---|
| Task lifecycle events | ✅ | All state transitions are captured via the session's observer slot |
| Session start / end | ✅ | Emitted in `start()` / `stop()` |
| Node CPU & memory | ✅ | Polled by the backend adapter on the `resource_poll_interval` |
| Disk & network I/O | ✅ on Concurrent + Dragon | Not exposed by Dask's `scheduler_info()` for network |
| Per-GPU utilization | ✅ if `pynvml` installed (Concurrent) or Dragon runtime active | Per-device events with `gpu_id=N` |
