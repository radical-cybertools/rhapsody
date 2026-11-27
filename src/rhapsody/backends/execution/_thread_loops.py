"""One event loop per worker thread, for async function tasks on a thread pool.

``asyncio.run`` creates an event loop for every call and closes it at the end. Its
clean-up cancels the tasks the call left running, and to find them it calls
``asyncio.all_tasks()``. In Python 3.11 to 3.14 that walks every asyncio Task of the
process, whichever loop it belongs to. On a thread pool that shares its process with
the engine, a task therefore costs more the more Tasks are alive: about 0.1 ms with
none, about 1 ms with 10,000, about 8 ms with 100,000.

``ThreadLoops`` keeps one loop per worker thread instead, and after each call does the
part of that clean-up that matters between tasks, without the walk:

- it cancels the tasks the call created and left running, waits for them, and reports
  their errors to the loop's exception handler, as ``asyncio.run`` does. It finds them
  through a task factory that records them weakly, as ``asyncio`` does, around the
  factory the loop was created with (an eager-task policy's, say);
- it puts back the loop's task factory, exception handler and debug flag, should the
  call have changed them, so that every call starts as on a new loop;
- it unsets the thread's current loop, so that between tasks the thread has none.

What differs from ``asyncio.run``:

- async generators a call left open are closed when they are garbage-collected, during
  a later call on that thread, not at the end of the call;
- callbacks a call scheduled with ``call_later`` or ``call_at`` and left pending stay
  on the loop: they run during a later call on that thread if their time comes, where
  ``asyncio.run`` drops them with its loop;
- the loop's default executor (``asyncio.to_thread``) lives until the backend shuts
  down, instead of being created and joined per call;
- objects bound to a loop that a task keeps beyond its call (an async HTTP client, a
  connection pool) stay usable by later tasks on the same thread, not on others.

A task that stops its loop fails, and the next task runs on that loop again; a loop
found closed is replaced by a new one. ``close`` closes every loop, after the worker
threads have finished. Neither ``run`` nor ``close`` may be called on a running event
loop.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import weakref
from collections.abc import Coroutine
from typing import Any

__all__ = ["ThreadLoops"]

logger = logging.getLogger(__name__)


def _running_loop() -> asyncio.AbstractEventLoop | None:
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        return None


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
        local = self._local
        loop = getattr(local, "loop", None)
        if loop is None or loop.is_closed():
            loop = asyncio.new_event_loop()
            make_task = loop.get_task_factory()

            def record(loop, coro, **kwargs):
                if make_task is None:
                    task = asyncio.Task(coro, loop=loop, **kwargs)
                else:
                    task = make_task(loop, coro, **kwargs)
                created = getattr(local, "created", None)
                if created is not None:
                    created.add(task)
                return task

            # what a new loop starts with, put back after every call
            local.settings = (record, loop.get_exception_handler(), loop.get_debug())
            loop.set_task_factory(record)
            local.loop = loop
            with self._lock:
                self._loops.append(loop)
        return loop

    def run(self, coro: Coroutine) -> Any:
        """Run ``coro`` to its end on this thread's loop, as ``asyncio.run`` would."""
        if _running_loop() is not None:
            raise RuntimeError("ThreadLoops.run() cannot be called from an event loop")
        loop = self._loop()
        local = self._local
        created: weakref.WeakSet[asyncio.Task] = weakref.WeakSet()
        local.created = created
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            local.created = None
            try:
                self._cancel_left(loop, created)
            finally:
                asyncio.set_event_loop(None)
                self._reset(loop, local.settings)

    @staticmethod
    def _cancel_left(loop: asyncio.AbstractEventLoop, created) -> None:
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

    @staticmethod
    def _reset(loop: asyncio.AbstractEventLoop, settings: tuple) -> None:
        record, handler, debug = settings
        if loop.is_closed():
            return
        if loop.get_task_factory() is not record:
            loop.set_task_factory(record)
        if loop.get_exception_handler() is not handler:
            loop.set_exception_handler(handler)
        if loop.get_debug() != debug:
            loop.set_debug(debug)

    def close(self) -> None:
        """Close every loop: call it once the worker threads have finished, off any
        event loop (``await asyncio.to_thread(loops.close)``)."""
        if _running_loop() is not None:
            raise RuntimeError(
                "ThreadLoops.close() cannot be called from an event loop: it would "
                "block it, and could not run the loops' clean-up"
            )
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
