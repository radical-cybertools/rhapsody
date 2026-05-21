# rhapsody live viz

Browser viewer for RHAPSODY task placements. Reads the telemetry event
stream and animates each task as it lands on a node's GPU/CPU slots.

Two delivery paths, sharing the same renderer:

## 1. Live — in-process, no files

Wire `LiveVizBridge` as a telemetry subscriber inside your session
script. Events flow `manager.emit() → dispatch loop → bridge.emit() →
per-client Queue → SSE`. No JSONL file is opened or polled.

```python
from examples.telemetry.viz.bridge import LiveVizBridge

bridge = LiveVizBridge(port=8765)
bridge.start()

telemetry = await session.start_telemetry()   # no checkpoint_path
telemetry.subscribe(bridge.emit)

# ... session runs ...

bridge.stop()
```

Open `http://127.0.0.1:8765/?live` while it runs.

A working end-to-end demo:

```bash
python examples/telemetry/viz/run_live_demo.py --open
```

## 2. Replay — post-mortem, from a JSONL file

For sessions that wrote a `*.telemetry.jsonl` checkpoint, replay them
through the bridge:

```bash
# generate a sample JSONL first (the repo's .gitignore excludes *.jsonl)
python examples/telemetry/viz/fake_dragon_stream.py \
    --tasks 200 --out /tmp/sample.jsonl

python examples/telemetry/viz/bridge.py --replay /tmp/sample.jsonl --open
```

`speed=` in the URL controls pacing (default 1.0; `inf` dumps the file
in one shot).

You can also skip the bridge entirely and drop a `*.telemetry.jsonl`
file directly onto the canvas — `index.html` reads it locally with
no server needed.

## Synthetic stream (no Dragon needed)

For development and regression testing without a real workload:

```bash
# Burst out a finished JSONL for replay:
python examples/telemetry/viz/fake_dragon_stream.py \
    --tasks 300 --out /tmp/fake.jsonl
```

## Visual encoding

- **Color** = task size, weighted as `cores + 8 * gpus`:
  - green = tiny (≤ 2)
  - cyan  = small (≤ 12)
  - amber = medium (≤ 30)
  - magenta = large (> 30)
- **Slot grid** = topology from the `ResourceLayout` event (re-flows
  automatically for any node count / cores / gpus).
- **Flight** = a tile from the queue strip into the assigned slot on
  TaskStarted. One tile per occupied slot for large tasks.
- **Halo** = on-landing glow (decays ~0.5s).
- **Fall + fade** = task terminal (Completed / Failed / Canceled); the
  tile drops past the bottom edge of the canvas.

Animations run on a wall-clock anim clock, **not** speed-scaled. You can
fast-forward the event timeline without losing the visual transitions.

## When placement isn't available

Some backends — including `DragonExecutionBackendV3` — don't report
per-task placement on `TaskStarted` yet. The viewer falls back to a
stable per-task hash so the visualization stays coherent and
reproducible across reloads. Once placement is exposed (Dragon V2
already does; V3 lands later), the viewer automatically uses the real
coordinates.
