#!/usr/bin/env python3

import glob
import os
from unittest.mock import MagicMock

import pytest
import rhapsody


def test_rm_slurm():
    pwd = os.path.dirname(os.path.abspath(__file__))

    # make a copy of os.environ to restore later
    old_env = os.environ.copy()

    # for each slurm test case in test_cases/*_slurm_*.env:
    #   - set the env
    #   - run the test
    #   - restore the env
    for env_file in glob.glob(f"{pwd}/test_cases/*_slurm_*.env"):
        try:
            # read the env file and set os.environ
            with open(env_file) as f:
                for line in f:
                    key, value = line.strip().split("=", 1)
                    os.environ[key] = value

            rm = rhapsody.ResourceManager.get_instance(name="SLURM")

            assert len(rm.node_list) == int(os.environ["SLURM_NNODES"])

        finally:
            # restore the env
            os.environ.clear()
            os.environ.update(old_env)


# -----------------------------------------------------------------------------
# get_partition_env tests for Slurm
# -----------------------------------------------------------------------------

def test_slurm_get_partition_env_returns_changed_vars():
    """Test Slurm get_partition_env returns only changed vars with correct values."""
    pwd = os.path.dirname(os.path.abspath(__file__))
    old_env = os.environ.copy()

    try:
        # Load a Slurm environment
        env_file = f"{pwd}/test_cases/frontier_slurm_2.env"
        with open(env_file) as f:
            for line in f:
                key, value = line.strip().split("=", 1)
                os.environ[key] = value

        rm = rhapsody.ResourceManager.get_instance(name="SLURM")

        # Get a partition with 1 node (out of 2)
        node_list, env_changes = rm.get_partition("part1", 1)

        assert len(node_list) == 1
        node_name = node_list[0].name

        # Should have changes since we're using a subset of nodes
        # Verify all keys and values
        assert env_changes["SLURM_NNODES"] == "1"
        assert env_changes["SLURM_JOB_NUM_NODES"] == "1"

        # Nodelist should be compactified version of the single node
        expected_nodelist = ",".join(rm.compactify_hostlist([node_name]))
        assert env_changes["SLURM_NODELIST"] == expected_nodelist
        assert env_changes["SLURM_JOB_NODELIST"] == expected_nodelist

    finally:
        os.environ.clear()
        os.environ.update(old_env)


def test_slurm_get_partition_env_no_change_when_same():
    """Test Slurm get_partition_env returns empty when values match."""
    pwd = os.path.dirname(os.path.abspath(__file__))
    old_env = os.environ.copy()

    try:
        # Load a Slurm environment
        env_file = f"{pwd}/test_cases/frontier_slurm_2.env"
        with open(env_file) as f:
            for line in f:
                key, value = line.strip().split("=", 1)
                os.environ[key] = value

        rm = rhapsody.ResourceManager.get_instance(name="SLURM")

        # Get a partition with ALL nodes (2 nodes)
        node_list, env_changes = rm.get_partition("part1", 2)

        assert len(node_list) == 2

        # SLURM_NNODES should NOT be in changes since it's already "2"
        assert "SLURM_NNODES" not in env_changes
        assert "SLURM_JOB_NUM_NODES" not in env_changes

    finally:
        os.environ.clear()
        os.environ.update(old_env)


def test_slurm_get_partition_env_only_existing_vars():
    """Test Slurm get_partition_env only returns vars that exist in env."""
    # Create mock nodes
    mock_node = MagicMock()
    mock_node.name = "node001"
    node_list = [mock_node]

    # Create a Slurm RM instance for testing (we'll call get_partition_env directly)
    pwd = os.path.dirname(os.path.abspath(__file__))
    old_env = os.environ.copy()

    try:
        env_file = f"{pwd}/test_cases/frontier_slurm_2.env"
        with open(env_file) as f:
            for line in f:
                key, value = line.strip().split("=", 1)
                os.environ[key] = value

        rm = rhapsody.ResourceManager.get_instance(name="SLURM")

        # Call with env that only has SLURM_NNODES (not the nodelist vars)
        partial_env = {"SLURM_NNODES": "5", "SLURM_JOB_NUM_NODES": "5"}
        # part_id is optional for Slurm (not used)
        env_changes = rm.get_partition_env(node_list, partial_env, part_id="test")

        # Should only have the vars that were in partial_env
        assert "SLURM_NNODES" in env_changes
        assert "SLURM_JOB_NUM_NODES" in env_changes
        assert "SLURM_NODELIST" not in env_changes
        assert "SLURM_JOB_NODELIST" not in env_changes

        # Values should reflect single node
        assert env_changes["SLURM_NNODES"] == "1"
        assert env_changes["SLURM_JOB_NUM_NODES"] == "1"

    finally:
        os.environ.clear()
        os.environ.update(old_env)


def test_slurm_get_partition_env_empty_node_list():
    """Test Slurm get_partition_env returns empty for empty node list."""
    pwd = os.path.dirname(os.path.abspath(__file__))
    old_env = os.environ.copy()

    try:
        env_file = f"{pwd}/test_cases/frontier_slurm_2.env"
        with open(env_file) as f:
            for line in f:
                key, value = line.strip().split("=", 1)
                os.environ[key] = value

        rm = rhapsody.ResourceManager.get_instance(name="SLURM")

        env_changes = rm.get_partition_env([], os.environ, part_id="test")
        assert env_changes == {}

    finally:
        os.environ.clear()
        os.environ.update(old_env)


def test_slurm_release_partition_env_noop():
    """Test Slurm release_partition_env is a no-op (no files created)."""
    pwd = os.path.dirname(os.path.abspath(__file__))
    old_env = os.environ.copy()

    try:
        env_file = f"{pwd}/test_cases/frontier_slurm_2.env"
        with open(env_file) as f:
            for line in f:
                key, value = line.strip().split("=", 1)
                os.environ[key] = value

        rm = rhapsody.ResourceManager.get_instance(name="SLURM")

        # Should not raise any errors
        rm.release_partition_env("nonexistent_partition")

    finally:
        os.environ.clear()
        os.environ.update(old_env)


if __name__ == "__main__":
    test_rm_slurm()
