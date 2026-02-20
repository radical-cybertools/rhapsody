"""
Torque resource manager implementation.

Torque is an open-source batch system based on the original PBS code.
"""

import os

from .base import ResourceManager


class Torque(ResourceManager):
    """
    Torque resource manager implementation.

    Discovers resources from Torque batch system environment variables,
    specifically the PBS_NODEFILE which contains allocated nodes.

    Environment variables:
        - PBS_JOBID: Job identifier
        - PBS_NODEFILE: Path to nodefile containing allocated nodes
        - PBS_NUM_NODES: Number of nodes in allocation
    """

    def _initialize(self) -> None:
        """
        Initialize Torque resource manager.

        Parses PBS_NODEFILE to discover allocated nodes and determines
        cores_per_node if not already configured.

        Raises:
            RuntimeError: If PBS_NODEFILE is not set.
        """
        rm_info = self._rm_info

        nodefile = os.environ.get("PBS_NODEFILE")
        if not nodefile:
            raise RuntimeError("$PBS_NODEFILE not set")

        nodes = self._parse_nodefile_and_cpn(nodefile)

        if not rm_info.cores_per_node:
            rm_info.cores_per_node = self._get_cores_per_node(nodes)

        node_names = [n[0] for n in nodes]
        rm_info.node_list = self._get_node_list(node_names, rm_info)

    def get_partition_env(
        self, node_list: list, env: dict, part_id: str | None = None
    ) -> dict:
        """Return PBS/Torque environment variable changes for a partition."""
        return self._get_partition_env_with_nodefile(
            node_list, env, part_id,
            nodefile_var="PBS_NODEFILE",
            node_count_var="PBS_NUM_NODES"
        )

    def release_partition_env(self, part_id: str) -> None:
        """
        Remove the nodefile created for the given partition.

        Args:
            part_id: Identifier of the partition being released.
        """
        self._remove_nodefile(part_id)

