"""Contract: native ``ProcessGroup`` lifecycle.

The telemetry adapter spawns one worker per node via::

    grp = ProcessGroup(restart=False, pmi=None)
    grp.add_process(nproc=1, template=ProcessTemplate(target=worker_fn, ...))
    grp.init(); grp.start(); ...; grp.join(timeout=...); grp.close()

and treats ``DragonUserCodeError`` as an expected exception class on
graceful shutdown.
"""

from __future__ import annotations

import inspect

import pytest

from dragon.native.event import Event
from dragon.native.process import ProcessTemplate
from dragon.native.process_group import DragonUserCodeError, ProcessGroup
from dragon.native.queue import Queue

from _dragon_ci_helpers import pg_worker  # noqa: E402


@pytest.mark.parametrize("kwarg", ["restart", "pmi", "policy"])
def test_process_group_ctor_kwarg(kwarg):
    assert kwarg in inspect.signature(ProcessGroup.__init__).parameters, (
        f"ProcessGroup.__init__ no longer accepts {kwarg!r}"
    )


@pytest.mark.parametrize("method", ["add_process", "init", "start", "join", "stop", "close"])
def test_process_group_method_present(method):
    assert callable(getattr(ProcessGroup, method, None)), (
        f"ProcessGroup.{method}() removed"
    )


@pytest.mark.parametrize("kwarg", ["nproc", "template"])
def test_process_group_add_process_kwarg(kwarg):
    assert kwarg in inspect.signature(ProcessGroup.add_process).parameters, (
        f"ProcessGroup.add_process no longer accepts {kwarg!r}"
    )


def test_dragon_user_code_error_importable():
    """Rhapsody catches ``DragonUserCodeError`` during graceful shutdown."""
    assert isinstance(DragonUserCodeError, type)
    assert issubclass(DragonUserCodeError, BaseException)


def test_process_group_lifecycle_smoke():
    """Build, init, start, join, close a 1-worker group end-to-end."""
    q = Queue(maxsize=10)
    shutdown = Event()

    grp = ProcessGroup(restart=False, pmi=None)
    grp.add_process(nproc=1, template=ProcessTemplate(target=pg_worker, args=(q, shutdown)))
    grp.init()
    grp.start()

    msg = q.get(timeout=30.0)
    assert msg.get("pid") and msg.get("host")

    shutdown.set()
    grp.join(timeout=30.0)
    grp.close()
