#!/usr/bin/env python3
"""Time how long DragonNativeExecutionBackend takes to shut down, and where the time goes.

Standalone benchmark (not collected by pytest); run it under Dragon::

    dragon python tests/performance/dragon_native_shutdown.py --workers 768
    dragon python tests/performance/dragon_native_shutdown.py --workers 768 --busy 256 --exes 256

It starts a pool of ``--workers`` workers on each node, runs one no-op task per worker, then
starts ``--busy`` pool tasks and ``--exes`` executables that sleep for good, and closes the
``Session`` once they all run. While it closes, a sampler records every 50 ms where each of the
client's threads is, by the innermost frame in the backend and the innermost frame overall. A close
that takes longer than ``--close-timeout`` prints the samples and exits with 1.

After the close it asks Global Services whether the workers and the executables have exited.

Output is one JSON line. Numbers are machine- and load-dependent: compare before/after on the same
machine.
"""

import argparse
import asyncio
import collections
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

from rhapsody.api import ComputeTask
from rhapsody.api import Session
from rhapsody.backends import DragonNativeExecutionBackend

INTERVAL = 0.05


def noop():
    return None


def sleep_forever():
    time.sleep(3600)


class Sampler:
    """Count, every ``INTERVAL`` seconds, where each thread of this process is."""

    def __init__(self):
        self.where = collections.Counter()
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)

    def run(self):
        me = threading.get_ident()
        names = {}
        while not self.stop.wait(INTERVAL):
            for t in threading.enumerate():
                names[t.ident] = t.name
            for ident, frame in sys._current_frames().items():
                if ident == me:
                    continue
                backend, inner = None, None
                f = frame
                while f is not None:
                    code = f.f_code
                    if inner is None:
                        inner = f"{Path(code.co_filename).name}:{code.co_name}"
                    if backend is None and code.co_filename.endswith("dragon_native.py"):
                        backend = f"{code.co_name}:{f.f_lineno}"
                    f = f.f_back
                if backend is None:
                    continue
                name = names.get(ident, "?").rstrip("0123456789_")
                self.where[(name, backend, inner)] += 1

    def top(self, n=12):
        return [
            {"s": round(count * INTERVAL, 2), "thread": t, "backend": b, "inner": i}
            for (t, b, i), count in self.where.most_common(n)
        ]


async def main(args) -> int:
    t0 = time.time()
    backend = await DragonNativeExecutionBackend(
        workers_per_node=args.workers, max_process_ranks=max(args.exes, 1)
    )
    start_s = time.time() - t0
    work_dir = args.work_dir or tempfile.mkdtemp(prefix="rh-shutdown-")
    session = Session(backends=[backend], work_dir=work_dir)
    workers = backend.num_workers
    t1 = time.time()
    warm = [ComputeTask(function=noop, uid=f"warm.{i:06d}") for i in range(workers * args.warm)]
    if warm:
        await asyncio.gather(*await session.submit_tasks(warm))
    warm_s = time.time() - t1
    busy = [ComputeTask(function=sleep_forever, uid=f"busy.{i:06d}") for i in range(args.busy)]
    exes = [
        ComputeTask(executable="/bin/sleep", arguments=["3600"], uid=f"exe.{i:06d}")
        for i in range(args.exes)
    ]
    if busy or exes:
        await session.submit_tasks(busy + exes)

        def running():
            with backend._lock:
                recs = list(backend._tasks.values())
            return sum(1 for r in recs if r.state == "running" and (r.kind == "pool" or r.puids))

        deadline = time.time() + 120
        while running() < len(busy) + len(exes) and time.time() < deadline:
            await asyncio.sleep(0.2)
    with backend._lock:
        worker_puids = [w.proc.puid for w in backend._workers.values() if w.proc is not None]
        exe_puids = [p for r in backend._tasks.values() if r.kind != "pool" for p in r.puids]
    sampler = Sampler()
    sampler.thread.start()
    t2 = time.time()
    try:
        await asyncio.wait_for(session.close(), args.close_timeout)
        closed = True
    except asyncio.TimeoutError:
        closed = False
    close_s = time.time() - t2
    sampler.stop.set()
    result = {
        "workers": workers,
        "busy": args.busy,
        "exes": args.exes,
        "start_s": round(start_s, 2),
        "warm_s": round(warm_s, 2),
        "close_s": round(close_s, 2),
        "closed": closed,
        "phases": getattr(backend, "_stop_phases", None),
        "where": sampler.top(),
    }
    if not closed:
        # This user's busiest processes on this node, as the kernel sees them.
        ps = subprocess.run(
            [
                "ps",
                "-u",
                str(os.getuid()),
                "-o",
                "pid,stat,etime,pcpu,rss,wchan:24,args",
                "--sort=-pcpu",
            ],
            capture_output=True,
            text=True,
            check=False,
        ).stdout.splitlines()
        result["processes"] = len(ps) - 1
        result["ps"] = [line[:160] for line in ps[:10]]
        print(json.dumps(result), flush=True)
        os._exit(1)
    from dragon.globalservices.process import multi_join

    for name, puids in (("workers_left", worker_puids), ("exes_left", exe_puids)):
        if puids:
            try:
                exited, _ = multi_join(puids, timeout=0, join_all=False)
                result[name] = len(puids) - len(exited or ())
            except Exception as exc:  # Global Services knows none of them
                result[name] = repr(exc)[:200]
    print(json.dumps(result), flush=True)
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--workers", type=int, default=768, help="workers_per_node")
    parser.add_argument("--warm", type=int, default=1, help="no-op tasks per worker first")
    parser.add_argument("--busy", type=int, default=0, help="pool tasks running at the close")
    parser.add_argument("--exes", type=int, default=0, help="executables running at the close")
    parser.add_argument("--close-timeout", type=float, default=300.0)
    parser.add_argument("--work-dir", default=None)
    sys.exit(asyncio.run(main(parser.parse_args())))
