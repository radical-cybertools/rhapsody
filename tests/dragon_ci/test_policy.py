"""Contract: ``dragon.infrastructure.policy.Policy`` shape.

Rhapsody V3 builds Policy instances with combinations of ``host_id``,
``distribution``, ``placement``, ``host_name``, and ``gpu_affinity``.
"""

from __future__ import annotations

import inspect

import pytest
from dragon.infrastructure.policy import Policy

_POLICY_KWARGS = (
    "placement", "host_name", "host_id", "distribution",
    "cpu_affinity", "gpu_affinity", "wait_mode",
)


@pytest.mark.parametrize("kwarg", _POLICY_KWARGS)
def test_policy_ctor_kwarg(kwarg):
    assert kwarg in inspect.signature(Policy.__init__).parameters, (
        f"Policy.__init__ no longer accepts {kwarg!r}"
    )


@pytest.mark.parametrize("attr", _POLICY_KWARGS)
def test_policy_attribute_present(attr):
    assert hasattr(Policy(), attr), f"Policy().{attr} attribute removed"


@pytest.mark.parametrize(
    "enum, required",
    [
        (Policy.Distribution, ("BLOCK", "ROUNDROBIN", "DEFAULT")),
        (Policy.Placement, ("DEFAULT", "HOST_NAME", "HOST_ID", "ANYWHERE", "LOCAL")),
    ],
)
def test_policy_enum_members(enum, required):
    members = {m.name for m in enum}
    missing = set(required) - members
    assert not missing, f"{enum.__name__} missing members: {missing}; got {members}"


def test_policy_with_host_id_and_block_distribution():
    p = Policy(host_id=0, distribution=Policy.Distribution.BLOCK)
    assert p.host_id == 0
    assert p.distribution == Policy.Distribution.BLOCK


def test_policy_with_placement_host_name_and_gpu_affinity():
    p = Policy(placement=Policy.Placement.HOST_NAME, host_name="x", gpu_affinity=[0, 1])
    assert p.placement == Policy.Placement.HOST_NAME
    assert p.host_name == "x"
    assert p.gpu_affinity == [0, 1]
