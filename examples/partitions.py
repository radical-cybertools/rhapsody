#!/usr/bin/env python3
"""Multi-partition example — manual wiring (the v1 "orchestration" per §9-D3).

Demonstrates two proxied partitions in one Session, with a cross-partition data
dependency expressed as ordinary user async/await — no new driver machinery is
needed (see ``rhapsody-partitions.md``).

Uses two ``concurrent`` backends so the example runs without a cluster.  In a
real allocation, you'd:
  - build partitions with ``rhapsody_rm.partition_spec(rm, "p0", n)``
  - pass the spec as ``resources={"partition": spec}``
  - pass the partition's env dict via ``extra_env=spec["env"]``
  - launch under the appropriate runtime with e.g. ``launch_prefix=["dragon"]``
"""

from __future__ import annotations

import asyncio

from rhapsody import ComputeTask
from rhapsody import Session
from rhapsody.backends.multiproc import RemoteBackendProxy


async def main() -> None:
    # Two partitioned backends, each in its own child process.
    p0 = await RemoteBackendProxy(name="p0", backend="concurrent")
    p1 = await RemoteBackendProxy(name="p1", backend="concurrent")

    try:
        async with Session(backends=[p0, p1]) as session:
            # Task 1 runs on p0
            t0 = ComputeTask(
                function=(lambda: sum(range(1000))),
                backend="p0",
            )
            await session.submit_tasks([t0])
            await session.wait_tasks([t0])
            print(f"p0 → {t0['return_value']}")

            # Task 2 runs on p1 and consumes t0's result (cross-partition dep,
            # resolved purely by user-level await; no driver-side machinery).
            t1 = ComputeTask(
                function=(lambda x: x * 2),
                args=(t0["return_value"],),
                backend="p1",
            )
            await session.submit_tasks([t1])
            await session.wait_tasks([t1])
            print(f"p1 → {t1['return_value']}")
    finally:
        await p0.shutdown()
        await p1.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
