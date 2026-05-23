"""Contract: ``dragon.native.machine.System`` shape and node enumeration.

Rhapsody's telemetry adapter calls ``System().hostname_policies()`` to obtain
one ``Policy`` per node, then pins one worker per Policy.
"""

from __future__ import annotations

import pytest

from dragon.infrastructure.policy import Policy
from dragon.native.machine import System


@pytest.mark.parametrize("attr", ["nnodes", "hostname_policies"])
def test_system_attribute_present(attr):
    """Rhapsody reads ``System().nnodes`` (V1 DDict sizing) and calls
    ``hostname_policies()`` (telemetry adapter, one worker per node)."""
    assert hasattr(System(), attr), f"System.{attr} removed"


def test_system_nnodes_is_int_ge_1():
    assert isinstance(System().nnodes, int) and System().nnodes >= 1


def test_hostname_policies_returns_one_policy_per_node():
    s = System()
    policies = s.hostname_policies()
    assert isinstance(policies, list)
    assert len(policies) == s.nnodes
    assert all(isinstance(p, Policy) for p in policies)


def test_hostname_policies_identify_each_node():
    """Each per-node Policy must carry an identifier (host_name or host_id)."""
    for p in System().hostname_policies():
        assert p.host_name or p.host_id >= 0 or p.placement == Policy.Placement.HOST_NAME, (
            f"hostname_policies returned a Policy with no node identifier: {p}"
        )


@pytest.mark.requires_multi_node
def test_hostname_policies_are_distinct():
    policies = System().hostname_policies()
    keys = [(p.host_name, p.host_id) for p in policies]
    assert len(set(keys)) == len(policies), f"duplicate identifiers: {keys}"
