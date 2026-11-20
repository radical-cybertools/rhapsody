# RHAPSODY Telemetry Examples

Scripts for collecting, visualising, and understanding RHAPSODY telemetry output.

---

## Files

### `00-telemetry.py`
Basic example of enabling telemetry on a session.
Starts a `Session` with `start_telemetry()`, submits a batch of `ComputeTask`s,
waits for completion, then prints the session summary via `telemetry.summary()`.
**Use it to:** verify your telemetry setup works end-to-end and see what the summary output looks like.


You must compile the `gpu-cuda-stress.cu` as follows to be usable by the `00-telemetry.py` example:

```
nvcc gpu-cuda-stress.cu -o gpu-cuda-stress
```
---

### `01-inference-telemetry.py` — Inference telemetry (AI)

Runs a set of scientific prompts as `AITask`s on the vLLM backend (`DragonVllmInferenceBackend`) with telemetry enabled, then prints what telemetry recorded about the inference backend: the batches it sent to the model (one `chat` span each: requests, prompts, trigger, duration, Dragon model time, worker host), the per-task attributes (batch id, prompt count, queue wait) and the inference metrics. The telemetry is also saved as JSONL in `--out`.
**Use it to:** see inference telemetry on a real GPU allocation. The fields are described in the [telemetry reference](../../docs/telemetry/reference.md) ("Inference telemetry"). The feature is experimental.

It needs a GPU allocation and the Dragon launcher. Get the allocation and choose the launcher options (workload manager, network transport) the way your machine requires; the launch below is the same for any machine apart from those options (see [AI inference on Perlmutter](../../docs/hpc-machines/perlmutter-ai-hpc.md) for one machine's environment). For 2 nodes with one GPU each, inside the allocation:

```
dragon <launcher options for your machine> examples/telemetry/01-inference-telemetry.py \
    --model /path/to/Qwen2.5-0.5B-Instruct \
    --nodes 2 --gpus-per-node 1 \
    --tasks 64 --out ./inference-telemetry
```

`--model` can also come from the `MODEL_PATH` environment variable; `HF_TOKEN` is read from the environment if the model must be downloaded. The backend here sends batches of at most 16 requests, so `--tasks 64` gives about four batches plus a last partial one (more `--tasks`, more batches).

### `01-inference-telemetry-plot.py` — Inference telemetry plots

Draws the saved JSONL of an inference run, from simple to sophisticated: batch size and trigger, queue wait, batch and model time, a timeline with the reconstructed queue depth, throughput and idle gaps, the latency of each task split into queue wait and batch time, worker hosts, CPU/memory/GPU against batch activity, and the data volume of the telemetry files. Needs only `matplotlib` and `numpy`.

```
python examples/telemetry/01-inference-telemetry-plot.py ./inference-telemetry            # plots go to ./inference-telemetry/plots
python examples/telemetry/01-inference-telemetry-plot.py ./inference-telemetry --out ./plots
```

Plots whose data is missing are skipped with a note (for example the timeouts plot when nothing timed out).

---

### `plot_dashboard.py` — Telemetry Dashboard

A polished, presentation-ready overview of a completed run.
Reads one JSONL telemetry file and produces a 10-panel figure (8 small + 2 full-width rows).

```
python plot_dashboard.py run.jsonl
```

| Panel | What it shows | Why it matters |
|---|---|---|
| **Task Duration** | Histogram of per-task execution time (valid vs incomplete-lifecycle) | Spot outliers and understand the time distribution of your workload |
| **Concurrency** | Step curve of tasks running simultaneously over time | See how well the backend saturates available workers |
| **Cumulative Completed** | Running count of finished tasks vs wall-clock time | Measure overall throughput ramp-up and detect stalls |
| **Completion Throughput** | Tasks completed per second in 30 time bins | Identify burst periods or throughput degradation |
| **CPU Utilization** | Per-node CPU % over time | Detect CPU-bound nodes or idle workers |
| **Memory Utilization** | Per-node memory % over time | Watch for memory pressure or leaks across the run |
| **GPU Utilization (per device)** | Per-node, per-device GPU % over time | Verify GPU saturation; catch devices that never fire |
| **Event Type Distribution** | Horizontal bar count of every event type emitted | Quick sanity check — confirms all lifecycle events fired and none are missing |
| **Aggregated Resource + Concurrency** *(full-width row)* | Mean CPU %, mean Mem %, mean GPU % across **all nodes** on the right axis; concurrency step on the left axis | Single-glance view of whether the whole cluster was busy while tasks ran |
| **Per-Node / Per-GPU + Concurrency** *(full-width row)* | Individual CPU/Mem lines per node (solid/dashed) and per-GPU lines (dotted) on the right axis; concurrency on the left | Pinpoint which specific node or GPU was the bottleneck, correlated with task activity |

---

### `plot_session_snapshot.py` — Session Diagnostics

A deep-diagnostic dump for debugging performance issues.
Produces up to 15 panels from the same JSONL file.

```
python plot_session_snapshot.py run.jsonl
```

| Panel | What it shows | Why it matters |
|---|---|---|
| **Task Duration** | Same histogram as the dashboard | Baseline reference |
| **Duration CDF** | Cumulative distribution with p50 / p90 / p95 / p99 lines | Quantify tail latency — p99 often reveals serialisation bottlenecks |
| **Concurrency** | Step curve of running tasks | Same as dashboard |
| **Cumulative Completed** | Running completion count | Same as dashboard |
| **Queue Wait** (TaskQueued → TaskStarted) | Histogram of time tasks waited in the backend queue | Long waits mean the backend is saturated or misconfigured |
| **Routing Latency** (TaskSubmitted → TaskQueued) | Histogram of session-to-backend hand-off time | Detects overhead in the RHAPSODY session routing layer |
| **End-to-End Latency** (TaskSubmitted → TaskCompleted) | Histogram of total wall time per task from submission | The number users care about — includes all overheads |
| **Emit Latency** (emit_time − event_time) | Histogram of event bus latency in microseconds | Verifies the telemetry bus itself is not a bottleneck |
| **CPU Utilization** | Per-node CPU % | Same as dashboard |
| **Memory Utilization** | Per-node memory % | Same as dashboard |
| **GPU Utilization** | Per-node, per-GPU % | Same as dashboard |
| **Event Type Distribution** | Count per event type | Same as dashboard |
| **Throughput** | Tasks/s over time | Same as dashboard |
| **OTel Metric Snapshot** | Final value of OTel counters (submitted / started / completed / failed / running) | Cross-checks event counts against the OTel instrument totals |
| **Concurrency + Node Resources** | Concurrency + per-node CPU/Mem/GPU combined on one panel | Compact combined view for embedding in reports |

---

## Choosing between them

| | `plot_dashboard.py` | `plot_session_snapshot.py` |
|---|---|---|
| **Audience** | Papers, presentations, stakeholders | Developers, performance debugging |
| **Latency breakdown** | No | Yes — queue wait, routing, e2e, emit |
| **CDF / percentiles** | No | Yes |
| **OTel counter cross-check** | No | Yes |
| **Full-width resource rows** | Yes — aggregated + per-node | One combined panel |

---

## Input format

Both plot scripts accept any RHAPSODY JSONL telemetry checkpoint file produced by
`TelemetryManager` with `checkpoint_path` set.
Each line is a JSON object with a `"section"` field: `"event"`, `"metric"`, or `"span"`.
