"""One event loop per worker thread, for async function tasks on a thread pool.

``asyncio.run`` creates an event loop for every call and closes it at the end. Its
clean-up cancels the tasks the call left running, and to find them it calls
``asyncio.all_tasks()``. In Python 3.12 that walks every asyncio Task of the process,
whichever loop it belongs to. On a thread pool that shares its process with the
engine, a task therefore costs more the more Tasks are alive: about 0.1 ms with none,
about 1 ms with 10,000, about 8 ms with 100,000.

``ThreadLoops`` keeps one loop per worker thread instead, and after each call does the
part of that clean-up that matters between tasks, without the walk:

- it cancels the tasks the call created and left running, found through a task factory
  that records them, waits for them, and reports their errors to the loop's exception
  handler, as ``asyncio.run`` does;
- it unsets the thread's current loop, so that between tasks the thread has none.

What differs from ``asyncio.run``:

- async generators a call left open are closed when they are garbage-collected, during
  a later call on that thread, not at the end of the call;
- the loop's default executor (``asyncio.to_thread``) lives until the backend shuts
  down, instead of being created and joined per call;
- objects bound to a loop that a task keeps beyond its call (an async HTTP client, a
  connection pool) stay usable by later tasks on the same thread, not on others.

A loop a task stopped or closed is replaced by a new one. ``close`` closes every loop,
after the worker threads have finished.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Coroutine
from typing import Any

__all__ = ["ThreadLoops"]

logger = logging.getLogger(__name__)


class ThreadLoops:
    """The event loops of a thread pool's workers; see the module's docstring."""

    def __init__(self) -> None:
        self._local = threading.local()
        self._lock = threading.Lock()
        self._loops: list[asyncio.AbstractEventLoop] = []

    def loops(self) -> list[asyncio.AbstractEventLoop]:
        """Every loop made so far, closed ones included."""
        with self._lock:
            return list(self._loops)

    def _loop(self) -> asyncio.AbstractEventLoop:
        loop = getattr(self._local, "loop", None)
        if loop is None or loop.is_closed():
            loop = asyncio.new_event_loop()
            local = self._local

            def record(loop, coro, **kwargs):
                task = asyncio.Task(coro, loop=loop, **kwargs)
                created = getattr(local, "created", None)
                if created is not None:
                    created.append(task)
                return task

            loop.set_task_factory(record)
            self._local.loop = loop
            with self._lock:
                self._loops.append(loop)
        return loop

    def run(self, coro: Coroutine) -> Any:
        """Run ``coro`` to its end on this thread's loop, as ``asyncio.run`` would."""
        loop = self._loop()
        created: list[asyncio.Task] = []
        self._local.created = created
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            self._local.created = None
            try:
                self._cancel_left(loop, created)
            finally:
                asyncio.set_event_loop(None)
                if not loop.is_closed() and loop.is_running():
                    loop.stop()

    @staticmethod
    def _cancel_left(loop: asyncio.AbstractEventLoop, created: list) -> None:
        left = [t for t in created if not t.done()]
        if not left or loop.is_closed():
            return
        for task in left:
            task.cancel()
        loop.run_until_complete(asyncio.gather(*left, return_exceptions=True))
        for task in left:
            if task.cancelled():
                continue
            if task.exception() is not None:
                loop.call_exception_handler(
                    {
                        "message": "unhandled exception of a task a function task left running",
                        "exception": task.exception(),
                        "task": task,
                    }
                )

    def close(self) -> None:
        """Close every loop: call it once the worker threads have finished."""
        with self._lock:
            loops, self._loops = self._loops, []
        for loop in loops:
            if loop.is_closed() or loop.is_running():
                continue
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
                loop.run_until_complete(loop.shutdown_default_executor())
            except Exception as exc:  # noqa: BLE001 - closing must go on for the others
                logger.debug("closing a worker thread's event loop: %r", exc)
            finally:
                loop.close()
        with self._lock:
            self._loops.extend(loops)  # kept, closed, so callers can see they closed
