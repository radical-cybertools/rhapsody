#!/usr/bin/env python3
"""Measure how fast DragonNativeExecutionBackend starts tasks while its client process is busy.

Standalone benchmark (not collected by pytest); run it under Dragon::

    dragon python tests/performance/dragon_native_busy_client.py --busy thread
    dragon python tests/performance/dragon_native_busy_client.py --busy none --tasks 100000

It submits ``--tasks`` no-op functions through a ``Session`` in one ``submit_tasks`` call and waits
for them all. With ``--busy thread``, one Python thread spins in the client for the whole run, as
a busy agent or workflow process does. The backend's own threads share that process's GIL, so
this measures how much of it they need per task. ``--switch-interval`` sets
``sys.setswitchinterval`` (Python's default is 5 ms).

Output is one JSON line. Numbers are machine- and load-dependent: compare before/after on the same
machine.
"""

import argparse
import asyncio
import json
import sys
import tempfile
import threading
import time

from rhapsody.api import ComputeTask
from rhapsody.api import Session
from rhapsody.backends import DragonNativeExecutionBackend


def noop():
    return time.time()


def spin(stop):
    """Run bytecode until ``stop``: a thread that always wants the GIL."""
    x = 0
    while not stop.is_set():
        for _ in range(1000):
            x += 1


async def main(args):
    if args.switch_interval is not None:
        sys.setswitchinterval(args.switch_interval)
    backend = await DragonNativeExecutionBackend(workers_per_node=args.workers_per_node)
    with tempfile.TemporaryDirectory() as work_dir:
        session = Session(backends=[backend], work_dir=work_dir)
        tasks = [ComputeTask(function=noop, uid=f"busy.{i:07d}") for i in range(args.tasks)]
        stop = threading.Event()
        if args.busy == "thread":
            threading.Thread(target=spin, args=(stop,), daemon=True).start()
        t0 = time.time()
        try:
            futures = await session.submit_tasks(tasks)
            outcomes = await asyncio.gather(*futures, return_exceptions=True)
            wall = time.time() - t0
        finally:
            stop.set()
            await session.close()
    starts = sorted(t["return_value"] for t in tasks if t.get("state") == "DONE")
    span = starts[-1] - starts[0] if len(starts) > 1 else 0.0
    print(
        json.dumps(
            {
                "busy": args.busy,
                "tasks": args.tasks,
                "workers": backend.num_workers,
                "failed": sum(isinstance(o, BaseException) for o in outcomes),
                "wall_s": round(wall, 3),
                "tasks_per_s": round(args.tasks / wall, 1),
                "started_per_s_while_starting": round(len(starts) / span, 1) if span else None,
                "switch_interval_ms": sys.getswitchinterval() * 1e3,
            }
        )
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--tasks", type=int, default=10000)
    parser.add_argument("--workers-per-node", type=int, default=None)
    parser.add_argument("--busy", choices=("none", "thread"), default="thread")
    parser.add_argument("--switch-interval", type=float, default=None)
    asyncio.run(main(parser.parse_args()))
