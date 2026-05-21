#!/usr/bin/env python3
"""End-to-end smoke: drive a real RHAPSODY session and write a viz-able JSONL.

Uses ConcurrentExecutionBackend (works without Dragon) so this also runs in
CI / a developer laptop. The output JSONL is consumable by the viz exactly
the same way Dragon V2/V3 JSONLs would be — the only difference is whether
TaskStarted carries a non-null ``placement`` (V2 yes; V3 / Concurrent no,
viewer synthesizes).
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from pathlib import Path

logging.basicConfig(level=logging.WARNING)

from rhapsody.api.session import Session
from rhapsody.api.task import ComputeTask
from rhapsody.backends import ConcurrentExecutionBackend


async def main(outdir: Path, n_tasks: int) -> Path:
    outdir.mkdir(parents=True, exist_ok=True)
    backend = await ConcurrentExecutionBackend()
    session = Session(backends=[backend])

    telemetry = await session.start_telemetry(
        resource_poll_interval=0.2,
        checkpoint_path=str(outdir),
    )

    # Mix of "executables" — really /bin/sleep — with varied durations to
    # show different size buckets in the viewer.
    tasks = []
    for i in range(n_tasks):
        # Three coarse "sizes" by sleep duration. cores/gpus aren't meaningful
        # on the Concurrent backend so the viewer's size-bucket coloring will
        # all collapse to tiny here — that's expected.
        dur = [0.1, 0.4, 1.2][i % 3]
        tasks.append(ComputeTask(executable="/bin/sleep", arguments=[str(dur)]))

    await session.submit_tasks(tasks)
    await asyncio.gather(*tasks)
    await session.close()

    # Find the freshly-written JSONL.
    files = sorted(outdir.glob("*.telemetry.jsonl"), key=lambda p: p.stat().st_mtime)
    assert files, "no telemetry JSONL produced"
    print(f"telemetry JSONL: {files[-1]}", flush=True)
    summary = telemetry.summary()
    print(f"session summary: {summary}", flush=True)
    return files[-1]


if __name__ == "__main__":
    outdir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("telemetry-output")
    n = int(sys.argv[2]) if len(sys.argv) > 2 else 12
    asyncio.run(main(outdir, n))
