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


# -----------------------------------------------------------------------------
# get_partition / get_partition_env tests
# -----------------------------------------------------------------------------

def test_get_partition_returns_tuple():
    """Test get_partition returns (node_list, env_changes) tuple."""
    cfg = rhapsody.RMConfig(requested_nodes=3, fake_resources=True)
    rm = rhapsody.ResourceManager.get_instance(name="FORK", cfg=cfg)

    node_list, env_changes = rm.get_partition("part1", 2)

    assert len(node_list) == 2
    assert isinstance(env_changes, dict)
    # Fork uses base class get_partition_env which returns {}
    assert env_changes == {}


def test_get_partition_with_custom_env():
    """Test get_partition accepts custom env dict."""
    cfg = rhapsody.RMConfig(requested_nodes=3, fake_resources=True)
    rm = rhapsody.ResourceManager.get_instance(name="FORK", cfg=cfg)

    custom_env = {"CUSTOM_VAR": "value"}
    node_list, env_changes = rm.get_partition("part1", 2, env=custom_env)

    assert len(node_list) == 2
    assert env_changes == {}


def test_get_partition_zero_nodes():
    """Test get_partition with zero nodes returns empty tuple elements."""
    cfg = rhapsody.RMConfig(requested_nodes=3, fake_resources=True)
    rm = rhapsody.ResourceManager.get_instance(name="FORK", cfg=cfg)

    node_list, env_changes = rm.get_partition("part1", 0)

    assert node_list == []
    assert env_changes == {}


def test_get_partition_env_base_returns_empty():
    """Test base class get_partition_env returns empty dict."""
    cfg = rhapsody.RMConfig(requested_nodes=3, fake_resources=True)
    rm = rhapsody.ResourceManager.get_instance(name="FORK", cfg=cfg)

    # Get some nodes first
    node_list, _ = rm.get_partition("part1", 2)

    # Call get_partition_env directly (with part_id)
    env_changes = rm.get_partition_env(node_list, {"SOME_VAR": "value"}, part_id="part1")
    assert env_changes == {}


def test_release_partition_env_base_noop():
    """Test base class release_partition_env is a no-op."""
    cfg = rhapsody.RMConfig(requested_nodes=3, fake_resources=True)
    rm = rhapsody.ResourceManager.get_instance(name="FORK", cfg=cfg)

    # Should not raise any errors
    rm.release_partition_env("nonexistent_partition")


def test_release_partition_cleans_up():
    """Test release_partition releases nodes."""
    cfg = rhapsody.RMConfig(requested_nodes=3, fake_resources=True)
    rm = rhapsody.ResourceManager.get_instance(name="FORK", cfg=cfg)

    # Get a partition
    node_list, _ = rm.get_partition("part1", 2)
    assert len(node_list) == 2

    # Verify nodes are marked with partition_id
    for node in node_list:
        assert node.partition_id == "part1"

    # Release the partition
    rm.release_partition("part1")

    # Verify nodes are no longer in the partition
    for node in node_list:
        assert node.partition_id is None


# -----------------------------------------------------------------------------
# nodefile helper tests
# -----------------------------------------------------------------------------

def test_get_nodefile_path():
    """Test _get_nodefile_path returns expected path."""
    import os
    cfg = rhapsody.RMConfig(requested_nodes=3, fake_resources=True)
    rm = rhapsody.ResourceManager.get_instance(name="FORK", cfg=cfg)

    path = rm._get_nodefile_path("test_part")
    assert path == os.path.abspath("partition_test_part.nodes")


def test_write_and_remove_nodefile():
    """Test _write_nodefile and _remove_nodefile work correctly."""
    import os
    cfg = rhapsody.RMConfig(requested_nodes=3, fake_resources=True)
    rm = rhapsody.ResourceManager.get_instance(name="FORK", cfg=cfg)

    # Get some nodes
    node_list, _ = rm.get_partition("test_nodefile", 2)

    # Write nodefile
    path = rm._write_nodefile("test_nodefile", node_list)
    assert os.path.exists(path)

    # Read and verify contents
    with open(path) as f:
        lines = f.read().strip().split("\n")
    assert len(lines) == 2
    assert lines[0] == node_list[0].name
    assert lines[1] == node_list[1].name

    # Remove nodefile
    rm._remove_nodefile("test_nodefile")
    assert not os.path.exists(path)

    # Clean up partition
    rm.release_partition("test_nodefile")


def test_remove_nodefile_nonexistent():
    """Test _remove_nodefile handles nonexistent files gracefully."""
    cfg = rhapsody.RMConfig(requested_nodes=3, fake_resources=True)
    rm = rhapsody.ResourceManager.get_instance(name="FORK", cfg=cfg)

    # Should not raise any errors
    rm._remove_nodefile("nonexistent_partition")


# -----------------------------------------------------------------------------
# Node dataclass tests
# -----------------------------------------------------------------------------

def test_node_requires_cores_without_rm_info():
    """Test Node raises ValueError when cores not provided and no rm_info."""
    from rhapsody.resource_manager.base import Node

    with pytest.raises(ValueError, match="cores must be provided"):
        Node(name="test", index=0, gpus=1)


def test_node_requires_gpus_without_rm_info():
    """Test Node raises ValueError when gpus not provided and no rm_info."""
    from rhapsody.resource_manager.base import Node

    with pytest.raises(ValueError, match="gpus must be provided"):
        Node(name="test", index=0, cores=4)


def test_node_partition_id_double_assignment():
    """Test Node raises ValueError when assigning partition_id twice."""
    cfg = rhapsody.RMConfig(requested_nodes=1, fake_resources=True)
    rm = rhapsody.ResourceManager.get_instance(name="FORK", cfg=cfg)

    # Get a real node from the RM
    node = rm.node_list[0]
    node.partition_id = "part1"

    with pytest.raises(ValueError, match="already in partition"):
        node.partition_id = "part2"

    # Clean up
    node._partition_id = None


def test_node_partition_id_can_be_cleared():
    """Test Node partition_id can be set to None."""
    cfg = rhapsody.RMConfig(requested_nodes=1, fake_resources=True)
    rm = rhapsody.ResourceManager.get_instance(name="FORK", cfg=cfg)

    node = rm.node_list[0]
    node.partition_id = "part1"
    node.partition_id = None  # Should not raise
    assert node.partition_id is None


# -----------------------------------------------------------------------------
# get_partition error tests
# -----------------------------------------------------------------------------

def test_get_partition_insufficient_nodes():
    """Test get_partition raises RuntimeError when not enough nodes."""
    cfg = rhapsody.RMConfig(requested_nodes=2, fake_resources=True)
    rm = rhapsody.ResourceManager.get_instance(name="FORK", cfg=cfg)

    with pytest.raises(RuntimeError, match="not enough free nodes"):
        rm.get_partition("part1", 5)  # Request more than available


# -----------------------------------------------------------------------------
# get_instance tests
# -----------------------------------------------------------------------------

def test_get_instance_with_explicit_name():
    """Test get_instance with explicit RM name."""
    cfg = rhapsody.RMConfig(requested_nodes=1, fake_resources=True)
    rm = rhapsody.ResourceManager.get_instance(name="FORK", cfg=cfg)

    assert rm is not None
    assert len(rm.node_list) == 1


def test_get_instance_unknown_rm_raises():
    """Test get_instance raises RuntimeError for unknown RM name."""
    cfg = rhapsody.RMConfig(requested_nodes=1, fake_resources=True)

    with pytest.raises(RuntimeError, match="no such ResourceManager"):
        rhapsody.ResourceManager.get_instance(name="NONEXISTENT_RM", cfg=cfg)


# -----------------------------------------------------------------------------
# _parse_nodefile tests
# -----------------------------------------------------------------------------

def test_parse_nodefile_basic(tmp_path):
    """Test _parse_nodefile parses a simple nodefile."""
    cfg = rhapsody.RMConfig(requested_nodes=1, fake_resources=True)
    rm = rhapsody.ResourceManager.get_instance(name="FORK", cfg=cfg)

    # Create a test nodefile
    nodefile = tmp_path / "nodefile"
    nodefile.write_text("node1\nnode2\nnode3\n")

    nodes = rm._parse_nodefile(str(nodefile))
    assert nodes == ["node1", "node2", "node3"]


def test_parse_nodefile_with_duplicates(tmp_path):
    """Test _parse_nodefile handles duplicate entries."""
    cfg = rhapsody.RMConfig(requested_nodes=1, fake_resources=True)
    rm = rhapsody.ResourceManager.get_instance(name="FORK", cfg=cfg)

    # Create nodefile with duplicates (PBS-style, one entry per slot)
    nodefile = tmp_path / "nodefile"
    nodefile.write_text("node1\nnode1\nnode2\nnode2\nnode2\n")

    nodes = rm._parse_nodefile(str(nodefile))
    assert set(nodes) == {"node1", "node2"}


def test_parse_nodefile_with_cpn(tmp_path):
    """Test _parse_nodefile with explicit cores_per_node."""
    cfg = rhapsody.RMConfig(requested_nodes=1, fake_resources=True)
    rm = rhapsody.ResourceManager.get_instance(name="FORK", cfg=cfg)

    nodefile = tmp_path / "nodefile"
    nodefile.write_text("node1\nnode2\n")

    # cpn parameter overrides detected slot count
    nodes = rm._parse_nodefile(str(nodefile), cpn=8)
    assert nodes == ["node1", "node2"]


def test_parse_nodefile_nonexistent():
    """Test _parse_nodefile returns empty list for nonexistent file."""
    cfg = rhapsody.RMConfig(requested_nodes=1, fake_resources=True)
    rm = rhapsody.ResourceManager.get_instance(name="FORK", cfg=cfg)

    nodes = rm._parse_nodefile("/nonexistent/nodefile")
    assert nodes == []


def test_parse_nodefile_empty(tmp_path):
    """Test _parse_nodefile handles empty file."""
    cfg = rhapsody.RMConfig(requested_nodes=1, fake_resources=True)
    rm = rhapsody.ResourceManager.get_instance(name="FORK", cfg=cfg)

    nodefile = tmp_path / "nodefile"
    nodefile.write_text("")

    nodes = rm._parse_nodefile(str(nodefile))
    assert nodes == []


# -----------------------------------------------------------------------------
# _get_cores_per_node tests
# -----------------------------------------------------------------------------

def test_get_cores_per_node_uniform():
    """Test _get_cores_per_node with uniform node list."""
    cfg = rhapsody.RMConfig(requested_nodes=1, fake_resources=True)
    rm = rhapsody.ResourceManager.get_instance(name="FORK", cfg=cfg)

    nodes = [("node1", 4), ("node2", 4), ("node3", 4)]
    cpn = rm._get_cores_per_node(nodes)
    assert cpn == 4


def test_get_cores_per_node_non_uniform():
    """Test _get_cores_per_node raises ValueError for non-uniform list."""
    cfg = rhapsody.RMConfig(requested_nodes=1, fake_resources=True)
    rm = rhapsody.ResourceManager.get_instance(name="FORK", cfg=cfg)

    nodes = [("node1", 4), ("node2", 8), ("node3", 4)]

    with pytest.raises(ValueError, match="non-uniform node list"):
        rm._get_cores_per_node(nodes)


# -----------------------------------------------------------------------------
# _filter_nodes tests (blocked cores/gpus)
# -----------------------------------------------------------------------------
# NOTE: _filter_nodes with blocked_cores/blocked_gpus uses node["cores"] syntax
# but Fork RM creates Node dataclass objects. This would need code changes to test.
# Skipping these tests for now.


if __name__ == "__main__":
    test_rm_base()
