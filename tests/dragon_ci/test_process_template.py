"""Contract: ``dragon.native.process.ProcessTemplate`` constructor and attributes.

Rhapsody V3 builds ProcessTemplate objects with these kwargs::

    ProcessTemplate(target, args=(...), kwargs={...}, cwd=..., env=...,
                    policy=Policy(...), stdin=Popen.DEVNULL,
                    stdout=Popen.PIPE, stderr=Popen.PIPE)

and later introspects ``pt.cwd``, ``pt.policy``, ``pt.env`` and unpacks
``pt.argdata`` via ``cloudpickle.loads`` to verify ``(target, args, kwargs)``
were preserved.
"""

from __future__ import annotations

import inspect

import cloudpickle
import pytest

from dragon.infrastructure.policy import Policy
from dragon.native.process import Popen, ProcessTemplate


_TEMPLATE_KWARGS = (
    "target", "args", "kwargs", "cwd", "env", "stdin", "stdout", "stderr", "policy",
)


@pytest.mark.parametrize("kwarg", _TEMPLATE_KWARGS)
def test_process_template_ctor_kwarg(kwarg):
    assert kwarg in inspect.signature(ProcessTemplate.__init__).parameters, (
        f"ProcessTemplate.__init__ no longer accepts {kwarg!r}"
    )


@pytest.mark.parametrize("name", ["PIPE", "DEVNULL"])
def test_popen_stdio_sentinel(name):
    """Rhapsody uses ``Popen.PIPE`` and ``Popen.DEVNULL`` as stream sentinels."""
    assert hasattr(Popen, name), f"Popen.{name} constant removed"


def test_template_cwd_attribute():
    pt = ProcessTemplate("/bin/echo", args=("x",), cwd="/tmp")
    assert pt.cwd == "/tmp"


def test_template_policy_identity_preserved():
    """``pt.policy is policy`` — Rhapsody's own tests rely on the same identity."""
    policy = Policy(gpu_affinity=[0, 1, 2, 3])
    pt = ProcessTemplate("/bin/echo", args=("x",), policy=policy)
    assert pt.policy is policy
    assert pt.policy.gpu_affinity == [0, 1, 2, 3]


def test_template_env_dict_preserved():
    pt = ProcessTemplate("/usr/bin/env", args=(), env={"FOO": "BAR"})
    assert pt.env.get("FOO") == "BAR"


def test_template_stdio_sentinels_accepted():
    """PIPE/DEVNULL must be acceptable to the stdin/stdout/stderr kwargs."""
    ProcessTemplate(
        "/bin/echo", args=("x",),
        stdin=Popen.DEVNULL, stdout=Popen.PIPE, stderr=Popen.PIPE,
    )


def test_template_argdata_round_trips_via_cloudpickle():
    """Rhapsody round-trips ``(target, args, kwargs)`` out of ``pt.argdata``."""
    def _target(x, y):
        return x + y

    pt = ProcessTemplate(_target, args=(1, 2), kwargs={"z": 3})
    target, stored_args, stored_kwargs = cloudpickle.loads(pt.argdata)
    # ``target`` is reconstituted across cloudpickle, so identity won't hold —
    # compare behaviour instead.
    assert target(1, 2) == _target(1, 2)
    assert tuple(stored_args) == (1, 2)
    assert stored_kwargs == {"z": 3}
