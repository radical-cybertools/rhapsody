#!/usr/bin/env python3
"""RHAPSODY workflow template: prepare -> fan-out -> fan-in, on any execution backend.

Copy this file and replace the three stage functions with your workload. It shows the patterns
an agent needs most often, all against the real RHAPSODY API:

* choosing and initializing a backend (each backend must be awaited before use);
* executable tasks and Python-function tasks in one session;
* stage dependencies expressed by awaiting one stage before building the next
  (for workflow graphs use AsyncFlow);
* failure handling with ``wait_tasks`` (never raises on task failure) and a retry that
  builds a new task object per attempt (one task object = one execution);
* optional telemetry written to a JSONL checkpoint.

Run::

    python workflow_template.py                                   # Concurrent, threads
    python workflow_template.py --backend concurrent --process -w 4
    python workflow_template.py --backend dask -w 4
    dragon workflow_template.py --backend dragon                  # needs the Dragon runtime
    python workflow_template.py --telemetry ./telemetry --out results.json
    python workflow_template.py --fail-member 2                   # exercise the failure path
"""

from __future__ import annotations

import argparse
import asyncio
import glob
import json
import os
import random
import socket
import sys
import time

from rhapsody.api import ComputeTask
from rhapsody.api import Session

# --------------------------------------------------------------------------------------------
# Stage functions. Keep them at module level: process pools, Dask and Dragon pickle them.
# --------------------------------------------------------------------------------------------


def simulate(member: int, seed: int, steps: int, fail: bool = False) -> dict:
    """Stand-in for one simulation ensemble member."""
    if fail:
        raise RuntimeError(f"member {member}: injected failure")
    t_start = time.time()
    rng = random.Random(seed)
    energy = 0.0
    for _ in range(steps):
        energy += rng.gauss(0.0, 1.0)
    return {
        "member": member,
        "energy": energy / steps,
        "host": socket.gethostname(),
        "t_start": t_start,
        "t_end": time.time(),
    }


def analyze(results: list[dict]) -> dict:
    """Stand-in for the fan-in analysis of all members."""
    energies = [r["energy"] for r in results]
    return {
        "members": len(results),
        "mean_energy": sum(energies) / len(energies),
        "hosts": sorted({r["host"] for r in results}),
    }


# --------------------------------------------------------------------------------------------
# Backend selection
# --------------------------------------------------------------------------------------------


async def make_backend(kind: str, workers: int, use_processes: bool):
    """Create and initialize (await) one execution backend."""
    if kind == "concurrent":
        from concurrent.futures import ProcessPoolExecutor
        from concurrent.futures import ThreadPoolExecutor

        from rhapsody.backends import ConcurrentExecutionBackend

        pool = ProcessPoolExecutor if use_processes else ThreadPoolExecutor
        return await ConcurrentExecutionBackend(pool(max_workers=workers), name="local")
    if kind == "dask":
        from rhapsody.backends import DaskExecutionBackend

        # resources= is forwarded to dask.distributed.Client(...) -> LocalCluster. Pass
        # cluster=/client= instead to target an existing cluster (asynchronous=True required).
        return await DaskExecutionBackend(
            resources={"n_workers": workers, "threads_per_worker": 1}, name="dask"
        )
    if kind == "dragon":
        import multiprocessing as mp

        mp.set_start_method("dragon", force=True)
        from rhapsody.backends import DragonExecutionBackend

        # batch_kwargs is forwarded to dragon.workflows.batch.Batch (e.g. {"num_nodes": 2}).
        return await DragonExecutionBackend(name="dragon")
    raise ValueError(f"unknown backend {kind!r}")


# --------------------------------------------------------------------------------------------
# Workflow
# --------------------------------------------------------------------------------------------


async def run_stage(session: Session, tasks: list[ComputeTask], timeout: float | None):
    """Submit a stage and wait for all of it; return (done, failed) without raising."""
    await session.submit_tasks(tasks)
    await session.wait_tasks(tasks, timeout=timeout)  # raises only asyncio.TimeoutError
    done = [t for t in tasks if t.state == "DONE"]
    failed = [t for t in tasks if t.state != "DONE"]
    for t in failed:
        print(f"  {t.uid} {t.state}: {t.exception or t.stderr}", file=sys.stderr)
    return done, failed


async def main(args: argparse.Namespace) -> int:
    backend = await make_backend(args.backend, args.workers, args.process)
    session = Session(backends=[backend], work_dir=args.work_dir)

    telemetry = None
    if args.telemetry:
        # Must be started before the first submit_tasks().
        telemetry = await session.start_telemetry(
            resource_poll_interval=1.0, checkpoint_path=args.telemetry
        )

    async with session:  # close() shuts down telemetry and every backend
        # Stage 1: an executable task (runs through the backend like any other task).
        prep = ComputeTask(
            executable="/bin/sh",
            arguments=["-c", "echo inputs ready on $(hostname)"],
            backend=backend.name,
        )
        _, failed = await run_stage(session, [prep], args.timeout)
        if failed:
            return 1
        print(f"prepare: {prep.stdout.strip()}")

        # Stage 2: fan-out. Depends on stage 1 because we awaited it above.
        def member_task(m: int) -> ComputeTask:
            return ComputeTask(
                function=simulate,
                args=(m, args.seed + m, args.steps),
                kwargs={"fail": m == args.fail_member},
                backend=backend.name,
            )

        sims = [member_task(m) for m in range(args.members)]
        done, failed = await run_stage(session, sims, args.timeout)
        if failed and args.retries:
            # Retry with NEW task objects. Injected failures stay injected, so --fail-member
            # demonstrates a permanent failure surviving the retry.
            retry = [member_task(t.args[0]) for t in failed]
            more_done, failed = await run_stage(session, retry, args.timeout)
            done += more_done
        print(f"simulate: {len(done)} done, {len(failed)} failed")
        if not done:
            return 1

        # Stage 3: fan-in. Pass upstream results by value.
        agg = ComputeTask(
            function=analyze, args=([t.return_value for t in done],), backend=backend.name
        )
        _, failed_agg = await run_stage(session, [agg], args.timeout)
        if failed_agg:
            return 1
        print(f"analyze: {agg.return_value}")

    if telemetry is not None:  # still readable after session.close()
        print("telemetry summary:", json.dumps(telemetry.summary()["tasks"]))
        files = glob.glob(os.path.join(args.telemetry, f"{session.uid}.*.telemetry.jsonl"))
        print("telemetry file:", files[0] if files else "<none>")

    if args.out:
        with open(args.out, "w") as fh:
            json.dump(
                {
                    "session": session.uid,
                    "backend": args.backend,
                    "summary": agg.return_value,
                    "failed_members": [t.args[0] for t in failed],
                },
                fh,
                indent=2,
            )
    return 0 if not failed else 2


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--backend", choices=["concurrent", "dask", "dragon"], default="concurrent")
    ap.add_argument("-w", "--workers", type=int, default=os.cpu_count() or 4)
    ap.add_argument("--process", action="store_true", help="Concurrent: use processes")
    ap.add_argument("--members", type=int, default=8)
    ap.add_argument("--steps", type=int, default=200_000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--retries", type=int, default=1, choices=[0, 1])
    ap.add_argument("--fail-member", type=int, default=-1, help="inject a failure (testing)")
    ap.add_argument("--timeout", type=float, default=None, help="per-stage timeout (s)")
    ap.add_argument("--work-dir", default=None, help="session work dir (default: cwd)")
    ap.add_argument("--telemetry", default=None, help="directory for the telemetry JSONL")
    ap.add_argument("--out", default=None, help="write a JSON result file")
    return ap.parse_args()


if __name__ == "__main__":
    sys.exit(asyncio.run(main(parse_args())))
