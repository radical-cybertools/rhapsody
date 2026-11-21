"""Task throughput benchmark for the Ensemble Launcher (EL) execution backend.

Sweeps the EL tuning knobs and measures no-op task throughput through a
Rhapsody ``Session``:

  * task/result buffer size     (``buffer_size``)
  * task/result flush interval  (``flush_interval``)
  * number of levels      (``nlevels``)
  * number of leaves      (``nleafs``)

A fresh launcher is started for every configuration, so the numbers are
independent of each other. The ``test`` MPI flavour is used so the benchmark
runs on a laptop as well as on a cluster (plain ``mpirun -np``, no host or
binding flags).

Two sweep modes:

  * ``oat`` (default): one-at-a-time. Every knob is varied over its list
    while the others stay at the baseline (the first value of each list).
  * ``grid``: full cartesian product of all lists. This grows quickly.

Examples:
    python tests/integration/test-hpc/el/test_throughput.py
    python tests/integration/test-hpc/el/test_throughput.py \\
        --nlevels 0 1 2 --nleaves 1 2 4 --max_tasks 4096 --csv el_throughput.csv
    python tests/integration/test-hpc/el/test_throughput.py --mode grid \\
        --buffer_sizes 0 1000 --flush_intervals 0.01 0.1
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import itertools
import os
import shutil
import tempfile
import time
from dataclasses import asdict
from dataclasses import dataclass

from rhapsody.api import ComputeTask
from rhapsody.api import Session
from rhapsody.backends import EnsembleExecutionBackend


def no_op():
    pass


@dataclass(frozen=True)
class Config:
    buffer_size: int
    flush_interval: float
    nlevels: int
    nleaves: int

    def label(self) -> str:
        return (
            f"buffer_size={self.buffer_size} flush_interval={self.flush_interval} "
            f"nlevels={self.nlevels} nleaves={self.nleaves}"
        )


SWEEP_KEYS = {
    "buffer_size": "buffer_sizes",
    "flush_interval": "flush_intervals",
    "nlevels": "nlevels",
    "nleaves": "nleaves",
}


def build_configs(args: argparse.Namespace) -> list[tuple[str, Config]]:
    """Return (swept_parameter, config) pairs for the requested sweep mode."""
    values = {field: getattr(args, arg) for field, arg in SWEEP_KEYS.items()}

    if args.mode == "grid":
        fields = list(values)
        return [
            ("grid", Config(**dict(zip(fields, combo))))
            for combo in itertools.product(*(values[f] for f in fields))
        ]

    baseline = {field: vals[0] for field, vals in values.items()}
    configs: list[tuple[str, Config]] = [("baseline", Config(**baseline))]
    seen = {configs[0][1]}
    for field, vals in values.items():
        for v in vals[1:]:
            cfg = Config(**{**baseline, field: v})
            if cfg not in seen:
                seen.add(cfg)
                configs.append((field, cfg))
    return configs


def task_counts(min_tasks: int, max_tasks: int) -> list[int]:
    counts = []
    n = min_tasks
    while n <= max_tasks:
        counts.append(n)
        n *= 2
    return counts


async def run_tasks(session: Session, num_tasks: int, timeout: float) -> tuple[float, int]:
    """Submit ``num_tasks`` no-op tasks and wait for all of them.

    Returns (elapsed seconds, number of failed tasks).
    """
    tasks = [ComputeTask(function=no_op) for _ in range(num_tasks)]
    start = time.perf_counter()
    await session.submit_tasks(tasks)
    await session.wait_tasks(tasks, timeout=timeout)
    elapsed = time.perf_counter() - start
    failed = sum(1 for t in tasks if t.get("state") != "DONE")
    return elapsed, failed


async def bench_config(
    cfg: Config, counts: list[int], args: argparse.Namespace, ckpt_root: str
) -> list[dict]:
    ckpt_dir = tempfile.mkdtemp(prefix="ckpt_", dir=ckpt_root)
    backend = EnsembleExecutionBackend(
        mpi_flavour="test",
        nlevels=cfg.nlevels,
        nleafs=cfg.nleaves,
        buffer_size=cfg.buffer_size,
        flush_interval=cfg.flush_interval,
        checkpoint_dir=ckpt_dir,
        worker_logs=args.logs,
        master_logs=args.logs,
        return_stdout=False,
        cpus=list(range(args.ncpus)) if args.ncpus else None,
    )

    rows = []
    try:
        backend = await backend
        async with Session(backends=[backend]) as session:
            for _ in range(args.warmup):
                await run_tasks(session, counts[0], args.timeout)

            for n in counts:
                samples = []
                failed = 0
                for _ in range(args.repeats):
                    elapsed, nfail = await run_tasks(session, n, args.timeout)
                    samples.append(elapsed)
                    failed += nfail
                best = min(samples)
                mean = sum(samples) / len(samples)
                row = {
                    **asdict(cfg),
                    "num_tasks": n,
                    "best_s": best,
                    "mean_s": mean,
                    "throughput_best": n / best,
                    "throughput_mean": n / mean,
                    "failed": failed,
                }
                rows.append(row)
                print(
                    f"  {n:>8} {row['throughput_best']:>14.1f} {row['throughput_mean']:>14.1f}"
                    f" {mean:>10.3f} {failed:>6}",
                    flush=True,
                )
    except Exception as e:  # keep going with the next configuration
        print(f"  FAILED: {type(e).__name__}: {e}", flush=True)
        rows.append({**asdict(cfg), "num_tasks": None, "error": f"{type(e).__name__}: {e}"})
    finally:
        await backend.shutdown()
        if not args.keep_ckpt:
            shutil.rmtree(ckpt_dir, ignore_errors=True)
    return rows


async def main(args: argparse.Namespace) -> None:
    configs = build_configs(args)
    counts = task_counts(args.min_tasks, args.max_tasks)
    ckpt_root = os.path.abspath(args.ckpt_root)
    os.makedirs(ckpt_root, exist_ok=True)

    print("EL backend task throughput benchmark (mpi flavour: test)", flush=True)
    print(f"mode={args.mode} configs={len(configs)} task counts={counts}", flush=True)
    print("", flush=True)

    all_rows: list[dict] = []
    for i, (swept, cfg) in enumerate(configs, 1):
        print(f"[{i}/{len(configs)}] sweep={swept} | {cfg.label()}", flush=True)
        print(
            f"  {'num tasks':>8} {'best [t/s]':>14} {'mean [t/s]':>14} {'mean [s]':>10} {'fail':>6}",
            flush=True,
        )
        rows = await bench_config(cfg, counts, args, ckpt_root)
        for r in rows:
            r["sweep"] = swept
        all_rows.extend(rows)
        print("", flush=True)

    if args.csv:
        fields = [
            "sweep",
            *Config.__dataclass_fields__,
            "num_tasks",
            "best_s",
            "mean_s",
            "throughput_best",
            "throughput_mean",
            "failed",
            "error",
        ]
        with open(args.csv, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            writer.writerows(all_rows)
        print(f"results written to {args.csv}", flush=True)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="EL backend task throughput benchmark")
    p.add_argument(
        "--mode",
        choices=["oat", "grid"],
        default="oat",
        help="oat: vary one knob at a time around the baseline (first value of "
        "each list); grid: full cartesian product",
    )
    p.add_argument(
        "--buffer_sizes",
        type=int,
        nargs="+",
        default=[1000, 0, 100, 10000],
        help="task and result buffer sizes",
    )
    p.add_argument(
        "--flush_intervals",
        type=float,
        nargs="+",
        default=[0.05, 0.001, 0.01, 0.5],
        help="task and result flush intervals [s]",
    )
    p.add_argument("--nlevels", type=int, nargs="+", default=[1, 0, 2])
    p.add_argument("--nleaves", type=int, nargs="+", default=[1, 2, 4])
    p.add_argument("--min_tasks", type=int, default=64)
    p.add_argument("--max_tasks", type=int, default=4096)
    p.add_argument("--repeats", type=int, default=3, help="runs per task count")
    p.add_argument("--warmup", type=int, default=1, help="warmup runs per config")
    p.add_argument("--timeout", type=float, default=600.0, help="per-run wait timeout [s]")
    p.add_argument(
        "--ncpus",
        type=int,
        default=None,
        help="number of CPUs given to the launcher (default: all)",
    )
    p.add_argument("--csv", type=str, default=None, help="write results to this CSV file")
    p.add_argument(
        "--ckpt_root",
        type=str,
        default="el_bench_ckpt",
        help="directory for per-config checkpoint dirs",
    )
    p.add_argument("--keep_ckpt", action="store_true", help="keep checkpoint dirs and logs")
    p.add_argument("--logs", action="store_true", help="enable master/worker logs")
    args = p.parse_args()
    args.max_tasks = max(args.max_tasks, args.min_tasks)
    return args


if __name__ == "__main__":
    asyncio.run(main(parse_args()))
