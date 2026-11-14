#!/usr/bin/env python3
"""Run many executables at once through DragonNativeExecutionBackend, and report the rate.

Standalone benchmark (not collected by pytest); run it under Dragon::

    dragon python tests/performance/dragon_native_many_processes.py --ranks 512 --tasks 2000

It submits ``--tasks`` executables that each sleep ``--seconds`` (``/bin/sleep``, or a Python
interpreter with ``--python``), with ``max_process_ranks=--ranks``, through a ``Session``, and
prints how many finished every 10 s. Each running process task used to have a thread blocked in
``Process.join``, a Global Services request: at 512 at once none of 2,000 finished, the stall
this reproduces. ``--wait`` bounds the run: a run that does not finish by then prints where the
client's threads are and exits with 1 without shutting down, since that would hang too.

Output is one JSON line. Numbers are machine- and load-dependent: compare before/after on the same
machine.
"""

import argparse
import asyncio
import collections
import json
import os
import sys
import tempfile
import threading
import time
import traceback
from pathlib import Path

from rhapsody.api import ComputeTask
from rhapsody.api import Session
from rhapsody.backends import DragonNativeExecutionBackend


def stacks() -> list:
    """Where this process's threads are: the innermost three frames of each, counted."""
    where = collections.Counter()
    for frame in sys._current_frames().values():
        tb = traceback.extract_stack(frame)[-3:]
        where[" < ".join(f"{Path(f.filename).name}:{f.lineno} {f.name}" for f in reversed(tb))] += 1
    return [f"{count} threads at {line}" for line, count in where.most_common(8)]


async def main(args) -> int:
    backend = await DragonNativeExecutionBackend(
        workers_per_node=args.workers_per_node, max_process_ranks=args.ranks
    )
    if args.python:
        command = [sys.executable, "-c", "import sys, time; time.sleep(float(sys.argv[1]))"]
    else:
        command = ["/bin/sleep"]
    work_dir = args.work_dir or tempfile.mkdtemp(prefix="rh-many-")
    session = Session(backends=[backend], work_dir=work_dir)
    tasks = [
        ComputeTask(
            executable=command[0], arguments=[*command[1:], repr(args.seconds)], uid=f"exe.{i:06d}"
        )
        for i in range(args.tasks)
    ]
    peak_threads = threading.active_count()
    done_at: list = []
    t0 = time.time()
    futures = await session.submit_tasks(tasks)
    for future in futures:
        future.add_done_callback(lambda _f: done_at.append(time.time()))
    deadline = t0 + args.wait
    last_report = t0
    while len(done_at) < args.tasks and time.time() < deadline:
        await asyncio.sleep(0.5)
        peak_threads = max(peak_threads, threading.active_count())
        if time.time() - last_report >= 10:
            last_report = time.time()
            print(
                f"[{last_report - t0:5.0f} s] finished {len(done_at)}/{args.tasks}, "
                f"running ranks {backend._running_ranks}, queued {len(backend._proc_queue)}, "
                f"threads {threading.active_count()}",
                flush=True,
            )
    finished = len(done_at)
    failed = sum(1 for f in futures if f.done() and not f.cancelled() and f.exception())
    span = max(done_at) - t0 if done_at else 0.0
    result = {
        "ranks": backend._max_process_ranks,
        "tasks": args.tasks,
        "seconds": args.seconds,
        "python": args.python,
        "finished": finished,
        "failed": failed,
        "span_s": round(span, 2),
        "per_s": round(finished / span, 1) if span else 0.0,
        "ideal_per_s": round(min(args.tasks, backend._max_process_ranks) / args.seconds, 1),
        "peak_threads": peak_threads,
        "stalled": finished < args.tasks,
    }
    if finished < args.tasks:
        result["stacks"] = stacks()
        print(json.dumps(result), flush=True)
        os._exit(1)
    t1 = time.time()
    await session.close()
    result["close_s"] = round(time.time() - t1, 2)
    print(json.dumps(result), flush=True)
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--ranks", type=int, default=None, help="max_process_ranks")
    parser.add_argument("--tasks", type=int, default=2000)
    parser.add_argument("--seconds", type=float, default=0.5)
    parser.add_argument("--python", action="store_true", help="sleep in a Python interpreter")
    parser.add_argument("--workers-per-node", type=int, default=1)
    parser.add_argument("--wait", type=float, default=240.0)
    parser.add_argument("--work-dir", default=None)
    sys.exit(asyncio.run(main(parser.parse_args())))
