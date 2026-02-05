
import os

from .base import ResourceManager


class Torque(ResourceManager):

    @staticmethod
    def batch_started():
        return bool(os.getenv("PBS_JOBID"))

    def _initialize(self) -> None:
        rm_info = self._rm_info

        nodefile = os.environ.get("PBS_NODEFILE")
        if not nodefile:
            raise RuntimeError("$PBS_NODEFILE not set")

        nodes = self._parse_nodefile(nodefile)

        if not rm_info.cores_per_node:
            rm_info.cores_per_node = self._get_cores_per_node(nodes)

        rm_info.node_list = self._get_node_list(nodes, rm_info)

    def get_partition_env(
        self, node_list: list, env: dict, part_id: str | None = None
    ) -> dict:
        """
        Return PBS/Torque environment variable changes for a partition.

        Writes a nodefile containing the partition's hostnames and returns
        environment variable changes for PBS_NODEFILE and PBS_NUM_NODES.

        Args:
            node_list: List of Node objects in the partition.
            env: Current environment dict (for reference).
            part_id: Partition identifier for nodefile naming.

        Returns:
            Dict with PBS_NODEFILE path and PBS_NUM_NODES (if it exists in env
            and differs from the partition size).
        """
        if not node_list:
            return {}

        if part_id is None:
            raise ValueError("part_id is required for Torque get_partition_env")

        nodefile_path = self._write_nodefile(part_id, node_list)
        n_nodes_str = str(len(node_list))

        changes = {}

        if "PBS_NODEFILE" in env:
            changes["PBS_NODEFILE"] = nodefile_path

        if "PBS_NUM_NODES" in env and env["PBS_NUM_NODES"] != n_nodes_str:
            changes["PBS_NUM_NODES"] = n_nodes_str

        return changes

    def release_partition_env(self, part_id: str) -> None:
        """
        Remove the nodefile created for the given partition.

        Args:
            part_id: Identifier of the partition being released.
        """
        self._remove_nodefile(part_id)

