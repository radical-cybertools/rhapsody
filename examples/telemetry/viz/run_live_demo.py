#!/usr/bin/env python3
"""In-process live viz demo.

Runs a small ConcurrentExecutionBackend workload and streams the
telemetry events straight to the browser via :class:`LiveVizBridge` —
no JSONL file, no checkpoint, no polling.

Run it::

    python examples/telemetry/viz/run_live_demo.py --open

then watch tasks fly into the slot grid in real time.  The browser
URL is ``http://127.0.0.1:8765/?live``.

The interesting line is the ``telemetry.subscribe(bridge.emit)`` call:
that single hook turns the bridge into a sink on the in-memory event
bus, with no checkpoint file involved.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
import webbrowser
from pathlib import Path

logging.basicConfig(level=logging.WARNING)

# Allow `python examples/telemetry/viz/run_live_demo.py` to find bridge.py
# as a sibling module without requiring a package install.
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from bridge import LiveVizBridge  # noqa: E402

from rhapsody.api.session import Session  # noqa: E402
from rhapsody.api.task import ComputeTask  # noqa: E402
from rhapsody.backends import ConcurrentExecutionBackend  # noqa: E402


async def main(n_tasks: int, port: int, open_browser: bool) -> None:
    bridge = LiveVizBridge(port=port)
    bridge.start()
    url = f"http://127.0.0.1:{port}/?live"
    print(f"[demo] viewer:  {url}")

    if open_browser:
        try:
            webbrowser.open(url)
        except Exception:
            pass

    backend = await ConcurrentExecutionBackend()
    session = Session(backends=[backend])

    # No checkpoint_path → no file ever touches disk.
    telemetry = await session.start_telemetry(resource_poll_interval=0.5)
    telemetry.subscribe(bridge.emit)

    # let the viewer connect and settle before we start submitting tasks
    time.sleep(2)

    # Mix sleep durations so the size buckets in the viewer light up.
    # On the Concurrent backend cores/gpus per task aren't meaningful
    # (everything is "tiny"); the durations still drive the timing.
    tasks = []
    for I in range(3):
        for i in range(n_tasks):
            dur = [1.0, 1.5, 2.0, 2.5][i % 4]
            tasks.append(ComputeTask(executable="/bin/sleep", arguments=[str(dur)]))

        print(f"[demo] submitting {n_tasks} tasks…")
        await session.submit_tasks(tasks)
        await asyncio.gather(*tasks)
    await session.close()

    # Give the SSE clients a moment to drain their queues before we tear
    # the bridge down — otherwise the last few TaskCompleted frames can
    # be lost in transit.
    await asyncio.sleep(2)

    # Tell the bridge to shut down cleanly.  This broadcasts an
    # ``event: shutdown`` SSE frame to every connected /live client so
    # the viewer drops its state back to the "pending" placeholder
    # instead of leaving a stale slot grid on screen.
    bridge.stop()
    print("[demo] session done.")


if __name__ == "__main__":
    p = argparse.ArgumentParser(
        description=(__doc__ or "in-process live viz demo").split("\n", 1)[0]
    )
    p.add_argument("--tasks", type=int, default=24)
    p.add_argument("--port",  type=int, default=8765)
    p.add_argument("--open",  dest="open_browser", action="store_true",
                   help="open the viewer in the default browser at startup")
    args = p.parse_args()
    try:
        asyncio.run(main(args.tasks, args.port, args.open_browser))
    except KeyboardInterrupt:
        pass
