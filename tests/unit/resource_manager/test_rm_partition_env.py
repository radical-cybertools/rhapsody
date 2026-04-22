#!/usr/bin/env python3

"""Tests for partition environment variable methods across file-based RMs.

These tests directly test the get_partition_env and release_partition_env
methods using mock nodes, without requiring actual RM environments.
"""

import os
from unittest.mock import MagicMock

import pytest

from rhapsody.resource_manager.cobalt import Cobalt
from rhapsody.resource_manager.lsf import LSF
from rhapsody.resource_manager.pbspro import PBSPro
from rhapsody.resource_manager.torque import Torque


def _make_mock_nodes(names: list[str]) -> list[MagicMock]:
    """Create mock Node objects with given names."""
    nodes = []
    for name in names:
        mock = MagicMock()
        mock.name = name
        nodes.append(mock)
    return nodes


class TestPBSProPartitionEnv:
    """Tests for PBSPro get_partition_env and release_partition_env."""

    def test_get_partition_env_writes_nodefile(self, tmp_path, monkeypatch):
        """Test PBSPro get_partition_env writes a nodefile."""
        monkeypatch.chdir(tmp_path)

        nodes = _make_mock_nodes(["node001", "node002"])
        env = {"PBS_NODEFILE": "/original/path", "PBS_NUM_NODES": "5"}

        # Create a mock PBSPro instance
        rm = object.__new__(PBSPro)
        rm._rm_info = MagicMock()

        changes = rm.get_partition_env(nodes, env, part_id="test1")

        # Check nodefile was written
        nodefile_path = tmp_path / "partition_test1.nodes"
        assert nodefile_path.exists()
        assert nodefile_path.read_text() == "node001\nnode002\n"

        # Check env changes
        assert changes["PBS_NODEFILE"] == str(nodefile_path)
        assert changes["PBS_NUM_NODES"] == "2"

    def test_get_partition_env_no_change_same_count(self, tmp_path, monkeypatch):
        """Test PBSPro get_partition_env omits PBS_NUM_NODES if unchanged."""
        monkeypatch.chdir(tmp_path)

        nodes = _make_mock_nodes(["node001", "node002"])
        env = {"PBS_NODEFILE": "/original/path", "PBS_NUM_NODES": "2"}

        rm = object.__new__(PBSPro)
        rm._rm_info = MagicMock()

        changes = rm.get_partition_env(nodes, env, part_id="test2")

        # PBS_NUM_NODES should NOT be in changes since it's already "2"
        assert "PBS_NODEFILE" in changes
        assert "PBS_NUM_NODES" not in changes

    def test_get_partition_env_only_existing_vars(self, tmp_path, monkeypatch):
        """Test PBSPro get_partition_env only returns vars that exist in env."""
        monkeypatch.chdir(tmp_path)

        nodes = _make_mock_nodes(["node001"])
        # Only PBS_NUM_NODES in env, not PBS_NODEFILE
        env = {"PBS_NUM_NODES": "5"}

        rm = object.__new__(PBSPro)
        rm._rm_info = MagicMock()

        changes = rm.get_partition_env(nodes, env, part_id="test3")

        # Should only return PBS_NUM_NODES
        assert "PBS_NODEFILE" not in changes
        assert "PBS_NUM_NODES" in changes
        assert changes["PBS_NUM_NODES"] == "1"

    def test_get_partition_env_empty_node_list(self, tmp_path, monkeypatch):
        """Test PBSPro get_partition_env returns empty for empty node list."""
        monkeypatch.chdir(tmp_path)

        rm = object.__new__(PBSPro)
        rm._rm_info = MagicMock()

        changes = rm.get_partition_env([], {"PBS_NODEFILE": "/path"}, part_id="test4")
        assert changes == {}

    def test_get_partition_env_requires_part_id(self, tmp_path, monkeypatch):
        """Test PBSPro get_partition_env raises error without part_id."""
        monkeypatch.chdir(tmp_path)

        nodes = _make_mock_nodes(["node001"])
        env = {"PBS_NODEFILE": "/path"}

        rm = object.__new__(PBSPro)
        rm._rm_info = MagicMock()

        with pytest.raises(ValueError, match="part_id is required"):
            rm.get_partition_env(nodes, env)

    def test_release_partition_env_removes_nodefile(self, tmp_path, monkeypatch):
        """Test PBSPro release_partition_env removes the nodefile."""
        monkeypatch.chdir(tmp_path)

        nodes = _make_mock_nodes(["node001"])
        env = {"PBS_NODEFILE": "/path"}

        rm = object.__new__(PBSPro)
        rm._rm_info = MagicMock()

        # Create nodefile
        rm.get_partition_env(nodes, env, part_id="test5")
        nodefile_path = tmp_path / "partition_test5.nodes"
        assert nodefile_path.exists()

        # Release
        rm.release_partition_env("test5")
        assert not nodefile_path.exists()


class TestTorquePartitionEnv:
    """Tests for Torque get_partition_env and release_partition_env."""

    def test_get_partition_env_writes_nodefile(self, tmp_path, monkeypatch):
        """Test Torque get_partition_env writes a nodefile."""
        monkeypatch.chdir(tmp_path)

        nodes = _make_mock_nodes(["torque01", "torque02", "torque03"])
        env = {"PBS_NODEFILE": "/original/path", "PBS_NUM_NODES": "10"}

        rm = object.__new__(Torque)
        rm._rm_info = MagicMock()

        changes = rm.get_partition_env(nodes, env, part_id="torque_part")

        nodefile_path = tmp_path / "partition_torque_part.nodes"
        assert nodefile_path.exists()
        assert nodefile_path.read_text() == "torque01\ntorque02\ntorque03\n"

        assert changes["PBS_NODEFILE"] == str(nodefile_path)
        assert changes["PBS_NUM_NODES"] == "3"

    def test_release_partition_env_removes_nodefile(self, tmp_path, monkeypatch):
        """Test Torque release_partition_env removes the nodefile."""
        monkeypatch.chdir(tmp_path)

        nodes = _make_mock_nodes(["torque01"])
        env = {"PBS_NODEFILE": "/path"}

        rm = object.__new__(Torque)
        rm._rm_info = MagicMock()

        rm.get_partition_env(nodes, env, part_id="torque_cleanup")
        nodefile_path = tmp_path / "partition_torque_cleanup.nodes"
        assert nodefile_path.exists()

        rm.release_partition_env("torque_cleanup")
        assert not nodefile_path.exists()


class TestCobaltPartitionEnv:
    """Tests for Cobalt get_partition_env and release_partition_env."""

    def test_get_partition_env_writes_nodefile(self, tmp_path, monkeypatch):
        """Test Cobalt get_partition_env writes a nodefile."""
        monkeypatch.chdir(tmp_path)

        nodes = _make_mock_nodes(["nid00001", "nid00002"])
        env = {"COBALT_NODEFILE": "/original/path", "COBALT_PARTSIZE": "10"}

        rm = object.__new__(Cobalt)
        rm._rm_info = MagicMock()

        changes = rm.get_partition_env(nodes, env, part_id="cobalt1")

        nodefile_path = tmp_path / "partition_cobalt1.nodes"
        assert nodefile_path.exists()
        assert nodefile_path.read_text() == "nid00001\nnid00002\n"

        assert changes["COBALT_NODEFILE"] == str(nodefile_path)
        assert changes["COBALT_PARTSIZE"] == "2"

    def test_get_partition_env_no_change_same_count(self, tmp_path, monkeypatch):
        """Test Cobalt get_partition_env omits COBALT_PARTSIZE if unchanged."""
        monkeypatch.chdir(tmp_path)

        nodes = _make_mock_nodes(["nid00001", "nid00002"])
        env = {"COBALT_NODEFILE": "/path", "COBALT_PARTSIZE": "2"}

        rm = object.__new__(Cobalt)
        rm._rm_info = MagicMock()

        changes = rm.get_partition_env(nodes, env, part_id="cobalt2")

        assert "COBALT_NODEFILE" in changes
        assert "COBALT_PARTSIZE" not in changes

    def test_release_partition_env_removes_nodefile(self, tmp_path, monkeypatch):
        """Test Cobalt release_partition_env removes the nodefile."""
        monkeypatch.chdir(tmp_path)

        nodes = _make_mock_nodes(["nid00001"])
        env = {"COBALT_NODEFILE": "/path"}

        rm = object.__new__(Cobalt)
        rm._rm_info = MagicMock()

        rm.get_partition_env(nodes, env, part_id="cobalt_cleanup")
        nodefile_path = tmp_path / "partition_cobalt_cleanup.nodes"
        assert nodefile_path.exists()

        rm.release_partition_env("cobalt_cleanup")
        assert not nodefile_path.exists()


class TestLSFPartitionEnv:
    """Tests for LSF get_partition_env and release_partition_env."""

    def test_get_partition_env_writes_hostfile(self, tmp_path, monkeypatch):
        """Test LSF get_partition_env writes a hostfile."""
        monkeypatch.chdir(tmp_path)

        nodes = _make_mock_nodes(["lsf01", "lsf02", "lsf03"])
        env = {"LSB_DJOB_HOSTFILE": "/original/path"}

        rm = object.__new__(LSF)
        rm._rm_info = MagicMock()

        changes = rm.get_partition_env(nodes, env, part_id="lsf1")

        hostfile_path = tmp_path / "partition_lsf1.nodes"
        assert hostfile_path.exists()
        assert hostfile_path.read_text() == "lsf01\nlsf02\nlsf03\n"

        assert changes["LSB_DJOB_HOSTFILE"] == str(hostfile_path)

    def test_get_partition_env_only_existing_vars(self, tmp_path, monkeypatch):
        """Test LSF get_partition_env only returns vars that exist in env."""
        monkeypatch.chdir(tmp_path)

        nodes = _make_mock_nodes(["lsf01"])
        # No LSB_DJOB_HOSTFILE in env
        env = {"OTHER_VAR": "value"}

        rm = object.__new__(LSF)
        rm._rm_info = MagicMock()

        changes = rm.get_partition_env(nodes, env, part_id="lsf2")

        # Should return empty since LSB_DJOB_HOSTFILE not in env
        assert changes == {}

    def test_get_partition_env_empty_node_list(self, tmp_path, monkeypatch):
        """Test LSF get_partition_env returns empty for empty node list."""
        monkeypatch.chdir(tmp_path)

        rm = object.__new__(LSF)
        rm._rm_info = MagicMock()

        changes = rm.get_partition_env([], {"LSB_DJOB_HOSTFILE": "/path"}, part_id="lsf3")
        assert changes == {}

    def test_release_partition_env_removes_hostfile(self, tmp_path, monkeypatch):
        """Test LSF release_partition_env removes the hostfile."""
        monkeypatch.chdir(tmp_path)

        nodes = _make_mock_nodes(["lsf01"])
        env = {"LSB_DJOB_HOSTFILE": "/path"}

        rm = object.__new__(LSF)
        rm._rm_info = MagicMock()

        rm.get_partition_env(nodes, env, part_id="lsf_cleanup")
        hostfile_path = tmp_path / "partition_lsf_cleanup.nodes"
        assert hostfile_path.exists()

        rm.release_partition_env("lsf_cleanup")
        assert not hostfile_path.exists()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
