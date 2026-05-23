"""Contract: ``dragon.native.queue.Queue`` and ``dragon.native.event.Event``.

The telemetry adapter uses these as the cross-process result channel and
shutdown signal: ``Queue(maxsize=)`` with ``put(timeout=)``/``get(timeout=)``,
``Event`` with ``wait(timeout=)``/``set()``.
"""

from __future__ import annotations

import inspect
import time

import pytest

from dragon.native.event import Event
from dragon.native.process import ProcessTemplate
from dragon.native.process_group import ProcessGroup
from dragon.native.queue import Queue

from _dragon_ci_helpers import pg_worker  # noqa: E402


def test_queue_ctor_accepts_maxsize():
    assert "maxsize" in inspect.signature(Queue.__init__).parameters


@pytest.mark.parametrize("method", ["put", "get", "close"])
def test_queue_method_present(method):
    assert callable(getattr(Queue, method, None)), f"Queue.{method}() removed"


@pytest.mark.parametrize("method", ["wait", "set", "clear", "is_set"])
def test_event_method_present(method):
    assert callable(getattr(Event, method, None)), f"Event.{method}() removed"


def test_queue_put_get_roundtrip_in_process():
    q = Queue(maxsize=4)
    q.put({"k": "v"}, timeout=5.0)
    assert q.get(timeout=5.0) == {"k": "v"}


def test_event_wait_returns_truthy_when_set():
    e = Event()
    assert not e.is_set()
    e.set()
    assert e.is_set()
    assert e.wait(timeout=1.0)


def test_event_wait_returns_falsy_on_timeout():
    e = Event()
    t0 = time.time()
    rv = e.wait(timeout=0.2)
    assert not rv, f"Event.wait should be falsy on timeout, got {rv!r}"
    assert time.time() - t0 >= 0.15, "Event.wait returned before its timeout"


def test_queue_event_cross_process_roundtrip():
    """A ProcessGroup worker pushes to a Queue and observes an Event."""
    q = Queue(maxsize=8)
    ev = Event()

    grp = ProcessGroup(restart=False, pmi=None)
    grp.add_process(nproc=1, template=ProcessTemplate(target=pg_worker, args=(q, ev)))
    grp.init()
    grp.start()
    try:
        msg = q.get(timeout=30.0)
        assert msg.get("pid") and msg.get("host")
    finally:
        ev.set()
        grp.join(timeout=30.0)
        grp.close()
