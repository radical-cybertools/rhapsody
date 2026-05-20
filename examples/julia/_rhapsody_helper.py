"""Sync facade around Rhapsody for the Julia driver demo (b).

Rhapsody is async; juliacall cannot await Python coroutines directly, so the
demo wraps a small coroutine in `asyncio.run()` and exposes a synchronous
entry point. A real integration would expose a richer, non-blocking API.
"""

import asyncio

from rhapsody.api import ComputeTask, Session
from rhapsody.backends import ConcurrentExecutionBackend


def run_echo(message: str) -> str:
    async def _go():
        backend = await ConcurrentExecutionBackend()
        session = Session([backend])
        task = ComputeTask(
            executable="/bin/echo",
            arguments=[message],
        )
        await session.submit_tasks([task])
        await session.wait_tasks([task])
        out = (task.stdout or "").strip()
        await backend.shutdown()
        return out

    return asyncio.run(_go())
