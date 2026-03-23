"""High-level Flux job submission and event tracking helper.

:class:`FluxHelper` connects to an existing Flux instance and manages
batch job submission and event delivery through three dedicated daemon
threads — one for watching the journal, one for draining the event queue,
and one for submitting jobs.

Note:
    Each thread creates its own ``flux.Flux(uri)`` handle.  The Flux Python
    bindings are not thread-safe; sharing a handle between threads is
    explicitly avoided.
"""

from __future__ import annotations

import logging
import queue
import threading
import uuid
from collections import defaultdict
from typing import Any
from typing import Callable

from .flux_module import FluxModule

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------------------
#
def _as_list(value: str | list[str]) -> list[str]:
    """Return *value* wrapped in a list if it is not already a list."""
    return value if isinstance(value, list) else [value]


# ------------------------------------------------------------------------------
#
class FluxHelper:
    """High-level interface for submitting and tracking Flux jobs.

    Args:
        uri: ``FLUX_URI`` of an already-running Flux instance.
        log: Optional :class:`logging.Logger`.  Defaults to the module logger.

    The typical lifecycle is::

        fh = FluxHelper(uri=fs.uri)
        fh.register_cb(my_callback)
        fh.start()
        tids = fh.submit(specs)
        fh.wait(tids)
        fh.stop()
    """

    # --------------------------------------------------------------------------
    #
    def __init__(
        self,
        uri: str,
        log: logging.Logger | None = None,
    ) -> None:
        self._uri: str = uri
        self._log: logging.Logger = log or logger
        self._uid: str = str(uuid.uuid4())

        self._fm = FluxModule()
        self._handle = self._fm.core.Flux(self._uri)
        self._api_lock = threading.Lock()

        # Journal-watcher thread
        self._jthread: threading.Thread | None = None
        self._jterm: threading.Event = threading.Event()

        # Event-drain thread
        self._ethread: threading.Thread | None = None
        self._equeue: queue.Queue = queue.Queue()

        # Submit thread
        self._sthread: threading.Thread | None = None
        self._squeue: queue.Queue = queue.Queue()
        self._sevent: threading.Event = threading.Event()

        # Shared state (protected by _idlock / _elock)
        self._idlock: threading.Lock = threading.Lock()
        self._elock: threading.Lock = threading.Lock()
        self._task_ids: dict = {}  # flux_id  -> task_uid
        self._flux_ids: dict = {}  # task_uid -> flux_id
        self._events: defaultdict = defaultdict(list)  # flux_id -> [events]
        self._cbacks: list[Callable] = []

        self._fm.verify()

    # --------------------------------------------------------------------------
    # Properties
    # --------------------------------------------------------------------------

    @property
    def uid(self) -> str:
        """Unique identifier for this helper instance."""
        return self._uid

    @property
    def uri(self) -> str | None:
        """The Flux URI this helper is connected to."""
        return self._uri

    # --------------------------------------------------------------------------
    # Thread workers
    # --------------------------------------------------------------------------

    def _jwatcher(self) -> None:
        """Journal-watcher thread: polls the Flux job journal."""
        # Each thread needs its own Flux handle (not thread-safe)
        fh = self._fm.core.Flux(self._uri)
        journal = self._fm.job.JournalConsumer(fh)
        journal.start()

        while not self._jterm.is_set():
            try:
                event = journal.poll(timeout=1.0)
                if event:
                    self._handle_events(fh, event.jobid, event)
            except TimeoutError:
                pass

    # --------------------------------------------------------------------------
    #
    def _swatcher(self) -> None:
        """Submit-watcher thread: dequeues spec batches and submits them."""
        self._log.debug("swatcher started")
        fh = self._fm.core.Flux(self._uri)

        while True:
            try:
                specs = self._squeue.get(block=True, timeout=1.0)
                self._log.debug("got %d specs", len(specs))
            except queue.Empty:
                continue
            except Exception:
                self._log.exception("exception in swatcher get")
                raise

            with self._idlock:
                try:
                    futs = []
                    for spec in specs:
                        tid = spec.attributes["user"]["uid"]
                        pri = spec.attributes["user"].get("priority")
                        kwargs: dict = {"waitable": True}
                        if pri is not None:
                            kwargs["urgency"] = pri
                        fut = self._fm.job.submit_async(fh, spec, **kwargs)
                        futs.append((fut, tid))

                    for fut, tid in futs:
                        fid = fut.get_id()
                        self._task_ids[fid] = tid
                        self._flux_ids[tid] = fid
                        self._equeue.put(fid)

                except Exception:
                    self._log.exception("exception in swatcher submit")
                    raise

                finally:
                    self._log.debug("submit done")
                    self._sevent.set()

    # --------------------------------------------------------------------------
    #
    def _ewatcher(self) -> None:
        """Event-drain thread: flushes buffered events for newly-known job IDs."""
        fh = self._fm.core.Flux(self._uri)
        while True:
            try:
                fid = self._equeue.get(timeout=1.0)
                self._handle_events(fh, fid)
            except queue.Empty:
                continue

    # --------------------------------------------------------------------------
    #
    def _handle_events(
        self,
        fh: Any,
        fid: Any,
        event: Any = None,
    ) -> None:
        """Route a job event to all registered callbacks.

        Events that arrive before the job ID is known (race between the journal
        thread and the submit thread) are buffered and flushed once the ID
        mapping is established.
        """
        with self._elock:
            # If triggered by the submit thread, check if there is anything to do
            if not event:
                if fid not in self._events:
                    return

            # No callbacks yet — buffer the event
            if not self._cbacks:
                if event:
                    self._events[fid].append(event)
                return

            # Job not yet in our maps — buffer the event
            if fid not in self._task_ids:
                if event:
                    self._events[fid].append(event)
                return

            tid = self._task_ids[fid]

            # Flush previously buffered events
            for ev in self._events[fid]:
                for cb in self._cbacks:
                    try:
                        cb(tid, ev)
                    except Exception:  # noqa: BLE001
                        self._log.exception("callback raised an exception")
            self._events[fid] = []

            # Dispatch the current event
            if event:
                for cb in self._cbacks:
                    try:
                        cb(tid, event)
                    except Exception:  # noqa: BLE001
                        self._log.exception("callback raised an exception")

    # --------------------------------------------------------------------------
    # Public API
    # --------------------------------------------------------------------------

    def start(self) -> None:
        """Launch the three daemon threads.

        This method is idempotent; calling it more than once is a no-op.
        """
        with self._api_lock:
            if self._jthread is not None:
                return

            self._jthread = threading.Thread(target=self._jwatcher, daemon=True)
            self._jthread.start()

            self._ethread = threading.Thread(target=self._ewatcher, daemon=True)
            self._ethread.start()

            self._sthread = threading.Thread(target=self._swatcher, daemon=True)
            self._sthread.start()

    # --------------------------------------------------------------------------
    #
    def stop(self) -> None:
        """Signal the journal thread to stop and wait for it."""
        with self._api_lock:
            if self._jthread is None:
                return
            self._jterm.set()
            self._jthread.join()
            self._jthread = None
            self._uri = None

    # --------------------------------------------------------------------------
    #
    def register_cb(self, cb: Callable) -> None:
        """Register a callback invoked on every job state event.

        Args:
            cb: Callable with signature ``cb(task_uid: str, event) -> None``.
        """
        with self._api_lock, self._elock:
            self._log.debug("register callback: %s", cb)
            self._cbacks.append(cb)

    # --------------------------------------------------------------------------
    #
    def unregister_cb(self, cb: Callable) -> None:
        """Remove a previously registered callback.

        Args:
            cb: The callable to remove.
        """
        with self._api_lock, self._elock:
            self._cbacks.remove(cb)

    # --------------------------------------------------------------------------
    #
    def submit(self, specs: list[Any]) -> list[str]:
        """Submit a batch of jobspecs and return the task UIDs.

        Args:
            specs: List of ``flux.job.JobspecV1`` objects.  Each spec must
                have ``spec.attributes['user']['uid']`` set.

        Returns:
            Ordered list of task UIDs (same order as *specs*).

        Raises:
            RuntimeError: If submission does not complete within 60 seconds.
        """
        with self._api_lock:
            if not self._handle:
                raise RuntimeError("FluxHelper has been stopped")

            self._log.debug("submit: %d specs", len(specs))
            tids = [spec.attributes["user"]["uid"] for spec in specs]

            self._sevent.clear()
            self._squeue.put(specs)
            self._sevent.wait(timeout=60.0)

            if not self._sevent.is_set():
                raise RuntimeError("flux submit timed out after 60 s")

            self._log.debug("submit done: %d specs", len(specs))
            return tids

    # --------------------------------------------------------------------------
    #
    def cancel(self, tids: str | list[str]) -> None:
        """Cancel one or more tasks.

        Args:
            tids: A single task UID or a list of task UIDs.
        """
        with self._api_lock:
            if not self._handle:
                raise RuntimeError("FluxHelper has been stopped")

            with self._idlock:
                for tid in _as_list(tids):
                    fid = self._flux_ids[tid]
                    self._fm.job.cancel_async(self._handle, fid, reason="user cancel")

    # --------------------------------------------------------------------------
    #
    def wait(self, tids: str | list[str]) -> None:
        """Block until all listed tasks have finished.

        Args:
            tids: A single task UID or a list of task UIDs.
        """
        with self._api_lock:
            if not self._handle:
                raise RuntimeError("FluxHelper has been stopped")

            with self._idlock:
                fids = [self._flux_ids[tid] for tid in _as_list(tids)]

            for fid in fids:
                self._fm.job.wait(self._handle, fid)
