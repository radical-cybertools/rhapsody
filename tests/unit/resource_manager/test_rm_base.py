#!/usr/bin/env python3

import pytest
import rhapsody


def test_rm_base():
    cfg = rhapsody.RMConfig(requested_nodes=3, fake_resources=True)
    with pytest.raises(NotImplementedError):
        rhapsody.ResourceManager(cfg=cfg)


# -----------------------------------------------------------------------------
# compactify_hostlist / expand_hostlist tests
# -----------------------------------------------------------------------------

COMPACTIFY_TEST_CASES = [
    # (input_hosts, expected_output)
    # Main example from the prompt
    (
        ["host001", "host002", "host003", "host007", "host012", "host013"],
        ["host00[1-3,7]", "host01[2,3]"],
    ),
    # Cross digit boundary - consecutive across 08-11
    (
        ["host008", "host009", "host010", "host011"],
        ["host0[08-11]"],
    ),
    # Single hostname
    (
        ["server01"],
        ["server0[1]"],
    ),
    # Non-consecutive numbers
    (
        ["web1", "web3", "web5", "web7"],
        ["web[1,3,5,7]"],
    ),
    # Mixed prefixes
    (
        ["db01", "db02", "web01", "web02"],
        ["db0[1,2]", "web0[1,2]"],
    ),
    # Larger consecutive range
    (
        ["node001", "node002", "node003", "node004", "node005"],
        ["node00[1-5]"],
    ),
    # Empty list
    ([], []),
    # Different widths with same prefix
    (
        ["app1", "app2", "app01", "app02"],
        ["app[1,2]", "app0[1,2]"],
    ),
    # Duplicates should be removed
    (
        ["srv01", "srv01", "srv02"],
        ["srv0[1,2]"],
    ),
    # Larger cross-boundary range
    (
        ["node097", "node098", "node099", "node100", "node101", "node102"],
        ["node[097-102]"],
    ),
]


@pytest.mark.parametrize("input_hosts,expected", COMPACTIFY_TEST_CASES)
def test_compactify_hostlist(input_hosts, expected):
    result = rhapsody.ResourceManager.compactify_hostlist(input_hosts)
    assert result == expected


@pytest.mark.parametrize("input_hosts,expected", COMPACTIFY_TEST_CASES)
def test_compactify_expand_roundtrip(input_hosts, expected):
    """Test that expand_hostlist reverses compactify_hostlist."""
    if not input_hosts:
        return  # Skip empty list case

    compacted = rhapsody.ResourceManager.compactify_hostlist(input_hosts)
    expanded = rhapsody.ResourceManager.expand_hostlist(compacted)
    assert expanded == sorted(set(input_hosts))


def test_expand_hostlist_no_brackets():
    """Test that hostnames without brackets pass through unchanged."""
    hosts = ["plain-host", "another-host"]
    result = rhapsody.ResourceManager.expand_hostlist(hosts)
    assert result == sorted(hosts)


def test_expand_hostlist_unsorted():
    """Test that sort=False preserves input order."""
    hosts = ["z-host[1,2]", "a-host[1,2]"]
    result = rhapsody.ResourceManager.expand_hostlist(hosts, sort=False)
    assert result == ["z-host1", "z-host2", "a-host1", "a-host2"]


# -----------------------------------------------------------------------------
# get_hostlist tests
# -----------------------------------------------------------------------------

GET_HOSTLIST_TEST_CASES = [
    # (hoststring, expected_output)
    (
        "node-b1-[1-3,5],node-c1-4,node-d3-3,node-k[10-12,15]",
        [
            "node-b1-1", "node-b1-2", "node-b1-3", "node-b1-5",
            "node-c1-4", "node-d3-3",
            "node-k10", "node-k11", "node-k12", "node-k15",
        ],
    ),
    # Simple range
    ("host[1-3]", ["host1", "host2", "host3"]),
    # No brackets
    ("host1,host2,host3", ["host1", "host2", "host3"]),
    # Mixed
    ("web[01-02],db01", ["web01", "web02", "db01"]),
    # Single host
    ("single-host", ["single-host"]),
    # Empty string
    ("", []),
]


@pytest.mark.parametrize("hoststring,expected", GET_HOSTLIST_TEST_CASES)
def test_get_hostlist(hoststring, expected):
    """Test get_hostlist parses hoststrings correctly."""
    # Need an RM instance to call non-static method
    cfg = rhapsody.RMConfig(requested_nodes=1, fake_resources=True)
    rm = rhapsody.ResourceManager.get_instance(name="FORK", cfg=cfg)
    result = rm.get_hostlist(hoststring)
    assert result == expected


# -----------------------------------------------------------------------------
# get_hostlist_by_range tests
# -----------------------------------------------------------------------------

GET_HOSTLIST_BY_RANGE_TEST_CASES = [
    # (hoststring, prefix, width, expected)
    # Cobalt-style use case
    ("1-3,5", "nid", 5, ["nid00001", "nid00002", "nid00003", "nid00005"]),
    # Width inferred from input
    ("01-03,05", "node", 0, ["node01", "node02", "node03", "node05"]),
    # Single number
    ("42", "host", 3, ["host042"]),
    # No prefix
    ("1-3", "", 2, ["01", "02", "03"]),
    # Larger range
    ("10-15", "n", 2, ["n10", "n11", "n12", "n13", "n14", "n15"]),
]


@pytest.mark.parametrize("hoststring,prefix,width,expected", GET_HOSTLIST_BY_RANGE_TEST_CASES)
def test_get_hostlist_by_range(hoststring, prefix, width, expected):
    """Test get_hostlist_by_range converts numeric ranges to hostnames."""
    cfg = rhapsody.RMConfig(requested_nodes=1, fake_resources=True)
    rm = rhapsody.ResourceManager.get_instance(name="FORK", cfg=cfg)
    result = rm.get_hostlist_by_range(hoststring, prefix, width)
    assert result == expected


def test_get_hostlist_by_range_invalid_input():
    """Test get_hostlist_by_range rejects non-numeric input."""
    cfg = rhapsody.RMConfig(requested_nodes=1, fake_resources=True)
    rm = rhapsody.ResourceManager.get_instance(name="FORK", cfg=cfg)
    with pytest.raises(ValueError, match="non numeric"):
        rm.get_hostlist_by_range("abc", "host", 3)


if __name__ == "__main__":
    test_rm_base()
