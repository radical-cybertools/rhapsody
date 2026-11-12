#!/usr/bin/env python3
"""RHAPSODY workload template: run one heterogeneous set of independent tasks on any backend.

RHAPSODY is a workload manager. This program shows the full loop an agent needs:

* choose and initialize a backend (Concurrent, Dask or Dragon; each must be awaited);
* build a heterogeneous workload: executable tasks, Python-function tasks and (Dragon) a
  multi-rank job, each with its own resources in ``task_backend_specific_kwargs``;
* place processes: CPU and GPU affinity with Dragon ``Policy`` objects, or the closest
  equivalent on the other backends;
* submit everything at once, wait without raising on failures, report every task, retry
  failures once with new task objects, and write the results.

All tasks are independent. For tasks that consume other tasks' outputs (workflows), use
AsyncFlow. Keep task functions at module level: process pools, Dask and Dragon pickle them.

Run::

    python workload_template.py                                   # Concurrent, threads
    python workload_template.py --process -w 8                    # Concurrent, processes
    python workload_template.py --backend dask -w 4
    dragon workload_template.py --backend dragon --pin-cpus       # pinned to cores
    dragon workload_template.py --backend dragon --gpus-per-task 1   # needs GPU nodes
    python workload_template.py --telemetry ./telemetry --out results.json
    python workload_template.py --fail-task 2                     # exercise the failure path
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
# Task code. Replace with your own. A function run by Dragon through a process template
# returns its exit code, not its value, so it also prints its record (emit=True).
# --------------------------------------------------------------------------------------------


def run_case(case: int, size: int, fail: bool = False, emit: bool = False) -> dict:
    """CPU-bound stand-in for one parameter case."""
    if fail:
        raise RuntimeError(f"case {case}: injected failure")
    t0 = time.time()
    rng = random.Random(case)
    value = sum(rng.random() for _ in range(size)) / size
    record = {
        "case": case,
        "value": value,
        "host": socket.gethostname(),
        "pid": os.getpid(),
        "cpus": sorted(os.sched_getaffinity(0)),
        "gpus": os.environ.get("CUDA_VISIBLE_DEVICES"),
        "seconds": round(time.time() - t0, 4),
    }
    if emit:
        print(json.dumps(record), flush=True)
    return record


def exec_command(case: int) -> list[str]:
    """Executable task: reports where it ran and what it was given."""
    script = f"echo case={case} host=$(hostname) cpus=$(nproc) gpus=${{CUDA_VISIBLE_DEVICES:-none}}"
    return ["-c", script]


# --------------------------------------------------------------------------------------------
# Backends and placement
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

        # resources= goes to dask.distributed.Client(...) -> LocalCluster. Pass cluster=/client=
        # instead to use an existing cluster (created with asynchronous=True).
        return await DaskExecutionBackend(
            resources={"n_workers": workers, "threads_per_worker": 1}, name="dask"
        )
    if kind == "dragon":
        import multiprocessing as mp

        mp.set_start_method("dragon", force=True)
        from rhapsody.backends import DragonExecutionBackend

        # batch_kwargs goes to dragon.workflows.batch.Batch, e.g. {"num_nodes": 2}.
        return await DragonExecutionBackend(name="dragon")
    raise ValueError(f"unknown backend {kind!r}")


def dragon_slots() -> list[dict]:
    """Nodes of the Dragon allocation with their CPU cores and GPU ids (the real topology)."""
    try:
        from dragon.native.machine import Node
        from dragon.native.machine import System

        nodes = [Node(huid) for huid in System().nodes]
        return [
            {"host": n.hostname, "cpus": list(n.cpus or []), "gpus": list(n.gpus or [])}
            for n in nodes
        ]
    except Exception as exc:  # noqa: BLE001 - fall back to the local host
        print(f"topology probe failed ({exc}); using the local host", file=sys.stderr)
        return [
            {"host": socket.gethostname(), "cpus": list(range(os.cpu_count() or 1)), "gpus": []}
        ]


def dragon_policy(i: int, slots: list[dict], pin_cpus: bool, gpus_per_task: int):
    """Policy for the i-th placed process: round-robin over nodes, then cores / GPUs."""
    from dragon.infrastructure.policy import Policy

    slot = slots[i % len(slots)]
    k = i // len(slots)
    kwargs: dict = {"placement": Policy.Placement.HOST_NAME, "host_name": slot["host"]}
    requested: dict = {"host": slot["host"]}
    if pin_cpus and slot["cpus"]:
        core = slot["cpus"][k % len(slot["cpus"])]
        kwargs["cpu_affinity"] = requested["cpus"] = [core]
    if gpus_per_task:
        if len(slot["gpus"]) < gpus_per_task:
            raise SystemExit(
                f"node {slot['host']} has {len(slot['gpus'])} GPU(s); "
                f"--gpus-per-task {gpus_per_task} cannot be placed"
            )
        start = (k * gpus_per_task) % len(slot["gpus"])
        ids = [slot["gpus"][(start + j) % len(slot["gpus"])] for j in range(gpus_per_task)]
        kwargs["gpu_affinity"] = requested["gpus"] = ids
    return Policy(**kwargs), requested


def task_options(kind: str, i: int, args: argparse.Namespace, slots: list[dict]):
    """Per-task ``task_backend_specific_kwargs`` (and what was requested) for one backend."""
    cwd = os.path.abspath(args.work_dir or os.getcwd())
    if args.backend == "dragon":
        template: dict = {"cwd": cwd}
        requested: dict = {}
        if args.pin_cpus or args.gpus_per_task:
            template["policy"], requested = dragon_policy(
                i, slots, args.pin_cpus, args.gpus_per_task
            )
        return {"process_template": template}, requested
    if args.backend == "dask":
        opts: dict = {"cwd": cwd} if kind == "exec" else {}
        if args.gpus_per_task:  # scheduling constraint: workers must advertise --resources GPU=n
            opts["resources"] = {"GPU": args.gpus_per_task}
        return opts, {}
    # Concurrent: no scheduler. GPUs only for executables, through the environment.
    opts = {"cwd": cwd} if kind == "exec" else {}
    if args.gpus_per_task and kind == "exec":
        ids = args.gpu_ids[i * args.gpus_per_task % len(args.gpu_ids) :][: args.gpus_per_task]
        # env REPLACES the inherited environment: merge with os.environ.
        opts["env"] = {**os.environ, "CUDA_VISIBLE_DEVICES": ",".join(map(str, ids))}
        return opts, {"gpus": ids}
    return opts, {}


# --------------------------------------------------------------------------------------------
# Workload
# --------------------------------------------------------------------------------------------


def build_workload(
    args: argparse.Namespace, backend_name: str, slots: list[dict]
) -> list[ComputeTask]:
    tasks: list[ComputeTask] = []
    placed = 0
    for i in range(args.executables):
        opts, req = task_options("exec", placed, args, slots)
        placed += 1
        tasks.append(
            ComputeTask(
                executable="/bin/sh",
                arguments=exec_command(i),
                backend=backend_name,
                task_backend_specific_kwargs=opts,
                kind="exec",
                requested=req,
            )
        )
    for i in range(args.functions):
        opts, req = task_options("func", placed, args, slots)
        placed += 1
        tasks.append(
            ComputeTask(
                function=run_case,
                args=(i, args.size),
                kwargs={"fail": i == args.fail_task, "emit": args.backend == "dragon"},
                backend=backend_name,
                task_backend_specific_kwargs=opts,
                kind="func",
                requested=req,
            )
        )
    if args.backend == "dragon" and args.ranks > 1:
        # One parallel job: rank groups of up to 2, each with its own placement policy.
        groups, requested = [], []
        for start in range(0, args.ranks, 2):
            template: dict = {}
            if args.pin_cpus or args.gpus_per_task:
                template["policy"], req = dragon_policy(
                    placed, slots, args.pin_cpus, args.gpus_per_task
                )
                requested.append(req)
                placed += 1
            groups.append((min(2, args.ranks - start), template))
        tasks.append(
            ComputeTask(
                function=run_case,
                args=(1000, args.size),
                kwargs={"emit": True},
                backend=backend_name,
                # pmi "none": these ranks are not MPI. Use "pmix" or "cray" for MPI programs.
                task_backend_specific_kwargs={
                    "process_templates": groups,
                    "pmi": "none",
                    "timeout": 600,
                },
                kind="job",
                requested={"groups": requested},
            )
        )
    return tasks


def clone(t: ComputeTask) -> ComputeTask:
    """A new task with the same definition as t (one task object = one execution)."""
    keep = ("backend", "task_backend_specific_kwargs", "kind", "requested")
    extra = {k: t[k] for k in keep if t.get(k)}
    if t.function:
        return ComputeTask(function=t.function, args=t.args, kwargs=t.kwargs, **extra)
    return ComputeTask(executable=t.executable, arguments=t.arguments, **extra)


def records(t: ComputeTask) -> list[dict]:
    """JSON records printed by function tasks (native: return_value; process/job: stdout)."""
    if isinstance(t.return_value, dict):
        return [t.return_value]
    out = []
    for line in (t.stdout or "").splitlines():
        if line.startswith("{"):
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return out


def placement_ok(t: ComputeTask) -> str:
    want = t.get("requested") or {}
    if not want or t.state != "DONE" or t.get("kind") == "exec":
        return ""
    recs = records(t)
    wants = want.get("groups") or [want]
    got_cpus = {tuple(r["cpus"]) for r in recs}
    exp_cpus = {tuple(w["cpus"]) for w in wants if "cpus" in w}
    if exp_cpus and got_cpus != exp_cpus:
        return f"cpu MISMATCH want {sorted(exp_cpus)} got {sorted(got_cpus)}"
    return "placement ok" if exp_cpus or any("gpus" in w for w in wants) else ""


def report(tasks: list[ComputeTask]) -> None:
    print(f"{'uid':<13}{'kind':<6}{'state':<9}{'exit':<6}result")
    for t in tasks:
        if t.state != "DONE":  # last line of the error: the exception message
            lines = f"{t.exception or t.stderr}".strip().splitlines() or [""]
            result = lines[-1][:70]
        elif t.get("kind") == "exec":
            result = (t.stdout or "").strip()[:70]
        else:
            recs = records(t)
            result = "; ".join(
                f"case {r['case']} on {r['host']} cpus={_short(r['cpus'])} gpus={r['gpus']}"
                for r in recs
            )[:90]
        check = placement_ok(t)
        print(
            f"{t.uid:<13}{t.get('kind', ''):<6}{t.state!s:<9}{t.exit_code!s:<6}{result}"
            + (f"  [{check}]" if check else "")
        )


def _short(cpus: list[int]) -> str:
    return str(cpus) if len(cpus) <= 4 else f"[{cpus[0]}..{cpus[-1]}] ({len(cpus)})"


async def main(args: argparse.Namespace) -> int:
    if args.backend != "dragon" and args.pin_cpus:
        print(
            "--pin-cpus needs Dragon policies; on this backend pin inside the task "
            "(os.sched_setaffinity). Ignoring.",
            file=sys.stderr,
        )
    if args.backend == "concurrent" and args.gpus_per_task and not args.gpu_ids:
        raise SystemExit("Concurrent has no GPU discovery: pass --gpu-ids 0,1,... for this node")

    backend = await make_backend(args.backend, args.workers, args.process)
    session = Session(backends=[backend], work_dir=args.work_dir)
    telemetry = None
    if args.telemetry:  # must be started before the first submit_tasks()
        telemetry = await session.start_telemetry(
            resource_poll_interval=1.0, checkpoint_path=args.telemetry
        )

    async with session:  # close() stops telemetry and shuts the backend down
        slots = dragon_slots() if args.backend == "dragon" else []
        tasks = build_workload(args, backend.name, slots)
        await session.submit_tasks(tasks)  # one submission: all tasks run concurrently
        await session.wait_tasks(tasks, timeout=args.timeout)  # never raises on task failure

        failed = [t for t in tasks if t.state != "DONE"]
        final = tasks
        if failed and args.retries:
            retry = [clone(t) for t in failed]
            await session.submit_tasks(retry)
            await session.wait_tasks(retry, timeout=args.timeout)
            final = [t for t in tasks if t.state == "DONE"] + retry
            print(f"retried {len(retry)} failed task(s) as {[t.uid for t in retry]}")
        report(final)

    counts: dict[str, int] = {}
    for t in final:
        counts[str(t.state)] = counts.get(str(t.state), 0) + 1
    print("totals:", counts)
    if telemetry is not None:  # still readable after session.close()
        files = glob.glob(os.path.join(args.telemetry, f"{session.uid}.*.telemetry.jsonl"))
        print("telemetry:", json.dumps(telemetry.summary()["tasks"]), files[0] if files else "")
    if args.out:
        with open(args.out, "w") as fh:
            json.dump(
                [
                    {
                        "uid": t.uid,
                        "kind": t.get("kind"),
                        "state": t.state,
                        "exit_code": t.exit_code,
                        "records": records(t),
                        "stdout": t.stdout if t.get("kind") == "exec" else None,
                        "error": None if t.state == "DONE" else str(t.exception or t.stderr),
                    }
                    for t in final
                ],
                fh,
                indent=2,
            )
    done = counts.get("DONE", 0)
    return 0 if done == len(final) else (2 if done else 1)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--backend", choices=["concurrent", "dask", "dragon"], default="concurrent")
    ap.add_argument("-w", "--workers", type=int, default=os.cpu_count() or 4)
    ap.add_argument("--process", action="store_true", help="Concurrent: use processes")
    ap.add_argument("--executables", type=int, default=4, help="number of executable tasks")
    ap.add_argument("--functions", type=int, default=8, help="number of function tasks")
    ap.add_argument(
        "--ranks", type=int, default=4, help="Dragon: ranks of the parallel job (0: none)"
    )
    ap.add_argument("--size", type=int, default=200_000, help="work per function task")
    ap.add_argument("--pin-cpus", action="store_true", help="Dragon: one core per process")
    ap.add_argument("--gpus-per-task", type=int, default=0)
    ap.add_argument(
        "--gpu-ids",
        type=lambda s: [int(x) for x in s.split(",")],
        default=None,
        help="Concurrent: GPU ids of this node, e.g. 0,1,2,3",
    )
    ap.add_argument("--retries", type=int, default=1, choices=[0, 1])
    ap.add_argument("--fail-task", type=int, default=-1, help="make function task K fail (testing)")
    ap.add_argument("--timeout", type=float, default=None, help="wait timeout in seconds")
    ap.add_argument("--work-dir", default=None, help="session work dir and task cwd (default: cwd)")
    ap.add_argument("--telemetry", default=None, help="directory for the telemetry JSONL")
    ap.add_argument("--out", default=None, help="write a JSON result file")
    return ap.parse_args()


if __name__ == "__main__":
    sys.exit(asyncio.run(main(parse_args())))
