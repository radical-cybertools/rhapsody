#!/usr/bin/env python3
"""Generate a realistic telemetry JSONL stream as if it came from Dragon.

Used by the viz developers (and by sample-generation) to exercise the
replay/live paths end-to-end without needing actual Dragon hardware.

Output format matches what rhapsody.telemetry.manager writes to
``<session>.<ts>.telemetry.jsonl``: one JSON event per line, with
section="event", plus a ResourceLayout event up front so the viewer
knows the topology.

Usage::

    # Full session → file (for replay):
    python fake_dragon_stream.py --out sample.jsonl --tasks 200

    # Live append (for the SSE bridge's /live mode):
    python fake_dragon_stream.py --out live.jsonl --tasks 200 --live --rate 8

    # To stdout (for piping):
    python fake_dragon_stream.py --tasks 50 --rate 4

The generator deliberately produces a mix of task sizes (1-core, 4-core,
16-core, 32-core; 0/1/2/4 GPUs) so the viewer's size-coloring is visible.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
import uuid
from typing import Any

# Task-shape catalogue.  Kept short and obviously distinct so the four
# size-buckets in the viewer all light up regularly. Weights bias toward
# small tasks (typical real workload mix).
SHAPES = [
    {"name": "tiny",   "cores": 1,  "gpus": 0, "duration": (2.0, 5.0),  "weight": 6, "exec": "/bin/echo"},
    {"name": "small",  "cores": 4,  "gpus": 1, "duration": (3.0, 8.0),  "weight": 4, "exec": "./infer"},
    {"name": "medium", "cores": 16, "gpus": 2, "duration": (5.0, 12.0), "weight": 2, "exec": "./train"},
    {"name": "large",  "cores": 32, "gpus": 4, "duration": (8.0, 20.0), "weight": 1, "exec": "./gkeyll"},
]


def _new_id() -> str:
    return uuid.uuid4().hex


def _weighted_choice(rng: random.Random) -> dict:
    pool = []
    for s in SHAPES:
        pool.extend([s] * s["weight"])
    return rng.choice(pool)


def _emit(sink, line: str, live: bool, dt: float) -> None:
    """Write one event line.  In live mode, sleep dt seconds first."""
    if live and dt > 0:
        time.sleep(dt)
    sink.write(line + "\n")
    sink.flush()


def make_event(
    event_type: str,
    *,
    session_id: str,
    backend: str,
    task_id: str | None = None,
    node_id: str | None = None,
    event_time: float,
    attributes: dict[str, Any] | None = None,
    **extras: Any,
) -> dict[str, Any]:
    e: dict[str, Any] = {
        "event_id": _new_id(),
        "event_type": event_type,
        "event_time": event_time,
        "emit_time": event_time,
        "session_id": session_id,
        "backend": backend,
        "task_id": task_id,
        "node_id": node_id,
        "attributes": attributes or {},
        "name": event_type,
        "section": "event",
    }
    e.update(extras)
    return e


def generate(
    *,
    sink,
    nodes: int,
    cores_per_node: int,
    gpus_per_node: int,
    tasks: int,
    rate: float,
    seed: int,
    live: bool,
) -> None:
    """Generate a full session of N tasks at ``rate`` tasks/second."""
    rng = random.Random(seed)
    session_id = f"session.{uuid.uuid4().hex[:8]}"
    backend = "dragon"
    t0 = time.time()
    now = t0

    # SessionStarted + ResourceLayout up front.  Both events have
    # task_id=None and node_id=None.
    _emit(sink, json.dumps(make_event(
        "SessionStarted",
        session_id=session_id, backend="rhapsody",
        event_time=now,
    )), live=False, dt=0)

    layout_nodes = [
        {"id": f"node-{i:04d}", "cores": cores_per_node, "gpus": gpus_per_node}
        for i in range(nodes)
    ]
    _emit(sink, json.dumps(make_event(
        "ResourceLayout",
        session_id=session_id, backend=backend,
        event_time=now,
        attributes={"nodes": layout_nodes},
    )), live=False, dt=0)

    # Per-node free-resource bookkeeping so we don't oversubscribe.
    free_cores = [cores_per_node] * nodes
    free_gpus  = [list(range(gpus_per_node)) for _ in range(nodes)]
    next_core  = [0] * nodes  # next core id to hand out per node

    # In-flight tasks waiting to be completed (heap-of-end-times).
    import heapq
    in_flight: list[tuple[float, dict]] = []

    def _drain_until(t_target: float) -> None:
        """Emit TaskCompleted events for tasks finishing before t_target."""
        while in_flight and in_flight[0][0] <= t_target:
            end_t, task = heapq.heappop(in_flight)
            n = task["_node"]
            free_cores[n] += task["_cores"]
            for g in task["_gpus"]:
                free_gpus[n].append(g)
            _emit(sink, json.dumps(make_event(
                "TaskCompleted",
                session_id=session_id, backend=backend,
                task_id=task["uid"], node_id=task["_node_name"],
                event_time=end_t,
                duration_seconds=end_t - task["_start_t"],
                attributes={
                    "executable": task["exec"],
                    "task_type": "ComputeTask",
                },
            )), live=live, dt=max(0.0, end_t - now) if live else 0.0)

    next_task_t = now
    for i in range(tasks):
        # Pacing: rate tasks per second wall-clock (or simulated wall-clock).
        next_task_t += 1.0 / rate if rate > 0 else 0.0
        # Drain any tasks that should have completed before this submission.
        _drain_until(next_task_t)

        shape = _weighted_choice(rng)
        uid = f"task.{i:06d}"
        exec_name = shape["exec"]

        # Pick a node with enough free resources.  Round-robin starting at
        # random offset so the visual grid doesn't always fill left-to-right.
        offset = rng.randrange(nodes)
        node_idx = None
        for k in range(nodes):
            ni = (offset + k) % nodes
            if free_cores[ni] >= shape["cores"] and len(free_gpus[ni]) >= shape["gpus"]:
                node_idx = ni
                break
        if node_idx is None:
            # Cluster saturated — drain until something frees up, then retry.
            if not in_flight:
                # Pathological: even the empty cluster can't host this shape.
                continue
            _drain_until(in_flight[0][0] + 0.001)
            for k in range(nodes):
                ni = (offset + k) % nodes
                if free_cores[ni] >= shape["cores"] and len(free_gpus[ni]) >= shape["gpus"]:
                    node_idx = ni
                    break
            if node_idx is None:
                continue

        # Allocate cores + gpus.
        core_ids = []
        for _ in range(shape["cores"]):
            core_ids.append(next_core[node_idx] % cores_per_node)
            next_core[node_idx] += 1
        free_cores[node_idx] -= shape["cores"]
        gpu_ids = [free_gpus[node_idx].pop(0) for _ in range(shape["gpus"])]

        node_name = layout_nodes[node_idx]["id"]
        placement = {
            "node_id": node_name,
            "cores": shape["cores"],
            "core_ids": core_ids,
            "gpu_ids": gpu_ids,
        }

        # TaskCreated → TaskSubmitted → TaskStarted, tight on the wall clock.
        t_create = next_task_t
        t_submit = t_create + 0.001
        t_start  = t_submit + rng.uniform(0.01, 0.08)
        for et, name in ((t_create, "TaskCreated"),
                         (t_submit, "TaskSubmitted"),
                         (t_start,  "TaskStarted")):
            _emit(sink, json.dumps(make_event(
                name,
                session_id=session_id, backend=backend,
                task_id=uid,
                node_id=node_name if name == "TaskStarted" else None,
                event_time=et,
                attributes=(
                    {"executable": exec_name, "task_type": "ComputeTask", "placement": placement}
                    if name == "TaskStarted"
                    else {"executable": exec_name, "task_type": "ComputeTask"}
                ),
            )), live=live, dt=max(0.0, et - now) if live else 0.0)
            if live:
                now = et

        if not live:
            now = t_start

        duration = rng.uniform(*shape["duration"])
        heapq.heappush(in_flight, (t_start + duration, {
            "uid": uid, "exec": exec_name,
            "_node": node_idx, "_node_name": node_name,
            "_cores": shape["cores"], "_gpus": gpu_ids,
            "_start_t": t_start,
        }))

    # Drain remaining tasks.
    while in_flight:
        end_t, task = heapq.heappop(in_flight)
        n = task["_node"]
        free_cores[n] += task["_cores"]
        for g in task["_gpus"]:
            free_gpus[n].append(g)
        _emit(sink, json.dumps(make_event(
            "TaskCompleted",
            session_id=session_id, backend=backend,
            task_id=task["uid"], node_id=task["_node_name"],
            event_time=end_t,
            duration_seconds=end_t - task["_start_t"],
            attributes={"executable": task["exec"], "task_type": "ComputeTask"},
        )), live=live, dt=max(0.0, end_t - now) if live else 0.0)
        if live:
            now = end_t

    # SessionEnded.
    _emit(sink, json.dumps(make_event(
        "SessionEnded",
        session_id=session_id, backend="rhapsody",
        event_time=now,
        duration_seconds=now - t0,
    )), live=False, dt=0)


def main() -> int:
    p = argparse.ArgumentParser(
        description=(__doc__ or "fake dragon telemetry stream").split("\n", 1)[0]
    )
    p.add_argument("--out",    default="-", help="output file (default: stdout)")
    p.add_argument("--tasks",  type=int,   default=120,  help="number of tasks to emit")
    p.add_argument("--nodes",  type=int,   default=8,    help="cluster size (default 8)")
    p.add_argument("--cores",  type=int,   default=32,   help="cores per node")
    p.add_argument("--gpus",   type=int,   default=4,    help="gpus per node")
    p.add_argument("--rate",   type=float, default=4.0,  help="tasks submitted per second of (sim or wall) time")
    p.add_argument("--seed",   type=int,   default=42)
    p.add_argument("--live",   action="store_true",
                   help="sleep between events using real wall-clock — for live-tail testing")
    args = p.parse_args()

    sink = sys.stdout if args.out == "-" else open(args.out, "w", buffering=1)
    try:
        generate(
            sink=sink,
            nodes=args.nodes,
            cores_per_node=args.cores,
            gpus_per_node=args.gpus,
            tasks=args.tasks,
            rate=args.rate,
            seed=args.seed,
            live=args.live,
        )
    finally:
        if sink is not sys.stdout:
            sink.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
