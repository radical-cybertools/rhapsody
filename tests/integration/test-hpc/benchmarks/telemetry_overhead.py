"""Telemetry overhead: the wall time of one inference workload with telemetry off and on.

A measurement, not a test (nothing passes or fails). The vLLM backend is started and warmed once;
then the same workload runs in alternating rounds, with telemetry off and with telemetry on, each
arm in a fresh `Session` on the same backend, so the arms differ in nothing but telemetry.
The order alternates between rounds to cancel a drift in the machine's speed.

Run on a GPU allocation with the Dragon launcher:

    dragon <launcher options for your machine> tests/integration/test-hpc/benchmarks/telemetry_overhead.py \
        --model /path/to/model --nodes 2 --gpus-per-node 1 --out ./overhead

The wall time covers the workload only. Stopping and draining telemetry after an "on" run costs
extra, which is printed next to it and saved as ``stop_s``. Every ``Session`` creates a
``rhapsody.session.*`` work directory in the current directory.

Results: a table on stdout and ``<out>/4-Overhead/{off,on}-<round>/`` folders, each with an
``inference_telemetry_report.json`` (wall time) and, for the "on" arm, the telemetry files.
"""

import argparse
import asyncio
import gc
import json
import logging
import multiprocessing as mp
import os
import statistics
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dragon"))

from telemetry_checks import provenance  # noqa: E402
from telemetry_checks import run_inference_telemetry  # noqa: E402
from telemetry_checks import run_workload  # noqa: E402
from telemetry_checks import warm_up  # noqa: E402

MAX_BATCH_SIZE = 32


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--model", required=True, help="model name or local snapshot path")
    parser.add_argument("--nodes", type=int, default=1)
    parser.add_argument("--gpus-per-node", type=int, default=1)
    parser.add_argument("--tasks", type=int, default=4000, help="AITasks per run")
    parser.add_argument(
        "--rounds",
        type=int,
        default=6,
        help="off/on pairs (even: each arm goes first equally often)",
    )
    parser.add_argument("--warmup", type=int, default=8, help="requests before the first round")
    parser.add_argument("--resource-poll-interval", type=float, default=2.0)
    parser.add_argument("--out", default="overhead", help="directory for the results")
    return parser.parse_args(argv)


def _summary(label, times):
    mean = statistics.mean(times)
    spread = statistics.stdev(times) if len(times) > 1 else 0.0
    return f"{label:<4} n={len(times)}  mean {mean:7.2f} s  sd {spread:5.2f} s  runs {times}"


async def run_off(backend, out: Path, tasks: int) -> float:
    """The workload without telemetry, in a fresh session; saves the wall time."""
    from rhapsody.api import Session

    session = Session([backend])
    started = time.perf_counter()
    await run_workload(session, backend, tasks, 2, 4)
    wall = time.perf_counter() - started
    out.mkdir(parents=True, exist_ok=True)
    (out / "inference_telemetry_report.json").write_text(
        json.dumps({"wall_time_s": wall, "args": {"tasks": tasks, "telemetry": False}})
    )
    return wall


async def settle_after_telemetry(backend, timeout_s: float) -> None:
    """Return the backend to its no-telemetry state and wait for the node workers to end.

    The manager is stopped, but the Dragon resource adapter thread ends later (it checks its flag
    once per poll interval), and the stopped manager's event log stays reachable through the
    backend's emitter until another is attached. Neither may leak into the next arm.
    """
    from rhapsody.telemetry.emitter import NoopEmitter

    backend.register_emitter(NoopEmitter())
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline and any(
        t.name == "telemetry-dragon-adapter" and t.is_alive() for t in threading.enumerate()
    ):
        await asyncio.sleep(0.2)
    gc.collect()


async def warm_workload(backend, tasks: int) -> None:
    """One untimed workload of full size: batches of MAX_BATCH_SIZE warm every worker."""
    from rhapsody.api import Session

    await run_workload(Session([backend]), backend, tasks, 2, 4)


async def main(argv=None):
    from rhapsody.backends import DragonVllmInferenceBackend
    from rhapsody.backends.ai.config import HardwareConfig
    from rhapsody.backends.ai.config import ModelConfig

    args = parse_args(argv)
    import rhapsody

    rhapsody.enable_logging(level=logging.WARNING)
    mp.set_start_method("dragon", force=True)
    base = Path(args.out) / "4-Overhead"

    backend = await DragonVllmInferenceBackend(
        model=ModelConfig(
            model_name=args.model,
            tp_size=1,
            gpu_memory_utilization=0.85,
            hf_token=os.environ.get("HF_TOKEN", ""),
        ),
        hardware=HardwareConfig(num_nodes=args.nodes, num_gpus=args.gpus_per_node, node_offset=0),
        max_batch_size=MAX_BATCH_SIZE,
        max_batch_wait_ms=500,
        use_service=False,
    )
    times = {"off": [], "on": []}
    stops: list = []  # seconds telemetry took to stop and drain after each "on" run
    starts: list = []  # seconds start_telemetry() took before each "on" run (not in the wall time)
    last_arm = "off" if args.rounds % 2 == 0 else "on"
    try:
        await warm_up(backend, args.warmup)
        # Sequential single-prompt calls do not exercise full batches: run one untimed workload.
        await warm_workload(backend, min(args.tasks, 512))
        # The adapter thread ends up to one poll interval after stop(): allow for that.
        settle = max(2.0, 2 * args.resource_poll_interval) + 5.0
        for round_no in range(1, args.rounds + 1):
            for arm in ("off", "on") if round_no % 2 else ("on", "off"):
                out = base / f"{arm}-{round_no}"
                if arm == "off":
                    wall = await run_off(backend, out, args.tasks)
                else:
                    report, facts = await run_inference_telemetry(
                        backend,
                        out,
                        args.tasks,
                        max_batch_size=MAX_BATCH_SIZE,
                        resource_poll_interval=args.resource_poll_interval,
                        hardware={"nodes": args.nodes, "gpus_per_node": args.gpus_per_node},
                    )
                    wall = facts["wall_time_s"]
                    stops.append(facts["stop_s"])
                    starts.append(facts["start_s"])
                    if report.failures():
                        print(f"note: the telemetry checks failed in {out}:", report.failures()[:3])
                times[arm].append(round(wall, 2))
                extra = (
                    f" (+{starts[-1]:.2f} s to start and {stops[-1]:.2f} s to stop telemetry)"
                    if arm == "on"
                    else ""
                )
                print(f"round {round_no} {arm:<3} {wall:7.2f} s{extra}", flush=True)
                if arm == "on":
                    await settle_after_telemetry(backend, settle)
                if not (round_no == args.rounds and arm == last_arm):
                    await asyncio.sleep(2.0)
                gc.collect()
    finally:
        await backend.shutdown()

    print()
    print(_summary("off", times["off"]))
    print(_summary("on", times["on"]))
    if times["off"] and times["on"]:
        diff = statistics.mean(times["on"]) - statistics.mean(times["off"])
        print(
            f"telemetry on vs off: {diff:+.2f} s ({100 * diff / statistics.mean(times['off']):+.1f}%)"
        )
        # Rounds are pairs run back to back, so the paired difference cancels slow drift.
        paired = [on - off for on, off in zip(times["on"], times["off"])]
        spread = statistics.stdev(paired) if len(paired) > 1 else 0.0
        print(
            f"paired per-round difference: mean {statistics.mean(paired):+.2f} s, sd {spread:.2f} s"
        )
    (Path(args.out) / "overhead_summary.json").write_text(
        json.dumps(
            {
                "times_s": times,
                "start_s": starts,
                "stop_s": stops,
                "args": vars(args),
                **provenance(),
            },
            indent=2,
            default=str,
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
