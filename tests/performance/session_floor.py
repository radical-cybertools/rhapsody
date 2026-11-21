#!/usr/bin/env python3
"""Measure the RHAPSODY Session path floor with backends that do no work.

Standalone benchmark (not collected by pytest). It times the Session path —
task construction, ``submit_tasks``, state updates, wait — with backends that
complete every task immediately, so backend cost is excluded:

- ``batch``: ``BatchNoopBackend`` below, which completes a whole submitted
  batch from one loop callback. Isolates the Session path itself.
- ``noop``: the stock ``NoopExecutionBackend``, which creates one asyncio task
  per completion.

Usage::

    python tests/performance/session_floor.py --backend batch --n 100000 --telemetry
    python tests/performance/session_floor.py --backend batch --n 3000 --wait chain --telemetry

Modes (``--wait``):

- ``wait_tasks`` (default) / ``gather``: submit all tasks (in ``--chunk`` sized
  calls), then wait. Prints per-task costs; ``e2e_us`` includes
  ``session.close()``, i.e. the telemetry queue drain, and ``task_events``
  counts the task events telemetry actually processed (4 per task when none
  were dropped).
- ``chain``: dependent steps (submit one task, await it, submit the next).
  Prints ``edge_us``, the cost per step.

Output is one JSON line. Numbers are machine- and load-dependent: compare
before/after on the same machine.
"""

import argparse
import asyncio
import gc
import json
import time

from rhapsody.api import ComputeTask
from rhapsody.api import Session
from rhapsody.backends.execution.noop import NoopExecutionBackend


class BatchNoopBackend(NoopExecutionBackend):
    """Completes a whole submitted batch from one loop callback (no asyncio task per task)."""

    async def submit_tasks(self, tasks):
        for task in tasks:
            task.update({"return_value": True, "stdout": "", "stderr": "", "exit_code": 0})
        asyncio.get_running_loop().call_soon(self._complete_all, tasks)

    def _complete_all(self, tasks):
        cb = self._callback_func
        for task in tasks:
            task["state"] = "DONE"
            cb(task, "DONE")


def noop():
    return None


def _task_events(telemetry):
    if telemetry is None:
        return None
    return sum(1 for e in telemetry._event_log if getattr(e, "task_id", None))


async def run(kind, n, chunk, wait_mode, telemetry):
    backend = await (BatchNoopBackend(name="batch") if kind == "batch" else NoopExecutionBackend())
    session = Session([backend])
    if telemetry:
        await session.start_telemetry(resource_poll_interval=3600)
    tel = session._telemetry

    if wait_mode == "chain":
        t0 = time.perf_counter()
        for _ in range(n):
            task = ComputeTask(function=noop)
            await session.submit_tasks([task])
            await task
        await session.close()
        dt = time.perf_counter() - t0
        return {
            "backend": kind,
            "n": n,
            "wait": "chain",
            "telemetry": telemetry,
            "edge_us": round(dt / n * 1e6, 1),
            "chain_rate": round(n / dt),
            "task_events": _task_events(tel),
        }

    gc.collect()
    t0 = time.perf_counter()
    tasks = [ComputeTask(function=noop) for _ in range(n)]
    t1 = time.perf_counter()
    for i in range(0, n, chunk):
        await session.submit_tasks(tasks[i : i + chunk])
    t2 = time.perf_counter()
    if wait_mode == "wait_tasks":
        await session.wait_tasks(tasks, timeout=3600)
    else:
        await asyncio.gather(*tasks)
    t3 = time.perf_counter()
    done = sum(1 for t in tasks if t["state"] == "DONE")
    await session.close()  # includes the telemetry queue drain
    t4 = time.perf_counter()
    return {
        "backend": kind,
        "n": n,
        "chunk": chunk,
        "wait": wait_mode,
        "telemetry": telemetry,
        "done": done,
        "build_us": round((t1 - t0) / n * 1e6, 1),
        "session_us": round((t3 - t1) / n * 1e6, 1),
        "e2e_us": round((t4 - t1) / n * 1e6, 1),
        "submit_s": round(t2 - t1, 3),
        "wait_s": round(t3 - t2, 3),
        "close_s": round(t4 - t3, 3),
        "task_events": _task_events(tel),
    }


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=(__doc__ or "").split("\n", 1)[0])
    ap.add_argument("--backend", default="batch", choices=["batch", "noop"])
    ap.add_argument("--n", type=int, default=10000)
    ap.add_argument("--chunk", type=int, default=0, help="submit chunk size (0 = all at once)")
    ap.add_argument("--wait", default="wait_tasks", choices=["wait_tasks", "gather", "chain"])
    ap.add_argument("--telemetry", action="store_true")
    a = ap.parse_args()
    print(json.dumps(asyncio.run(run(a.backend, a.n, a.chunk or a.n, a.wait, a.telemetry))))
