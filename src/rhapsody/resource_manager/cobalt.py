
import os

from .base import ResourceManager


class Cobalt(ResourceManager):
    @staticmethod
    def batch_started():
        return bool(os.getenv("COBALT_JOBID"))

    def _initialize(self) -> None:
        rm_info = self._rm_info

        if not rm_info.cores_per_node:
            raise RuntimeError("cores_per_node undetermined")

        if "COBALT_NODEFILE" in os.environ:
            # this env variable is used for GPU nodes
            nodefile = os.environ["COBALT_NODEFILE"]
            nodes = self._parse_nodefile(nodefile, rm_info.cores_per_node)

        elif "COBALT_PARTNAME" in os.environ:
            node_range = os.environ["COBALT_PARTNAME"]
            nodes = [
                (node, rm_info.cores_per_node)
                for node in self.get_hostlist_by_range(node_range, "nid", 5)
            ]

            # Another option is to run `aprun` with the rank of nodes
            # we *think* we have, and with `-N 1` to place one rank per node,
            # and run `hostname` - that gives the list of hostnames.
            # (The number of nodes we receive from `$COBALT_PARTSIZE`.)
            #   out = ru.sh_callout("aprun -q -n %d -N 1 hostname" % n_nodes)[0]
            #   nodes = out.split()

        else:
            raise RuntimeError("no $COBALT_NODEFILE nor $COBALT_PARTNAME set")

        rm_info.node_list = self._get_node_list(nodes, rm_info)

    def get_partition_env(
        self, node_list: list, env: dict, part_id: str | None = None
    ) -> dict:
        """
        Return Cobalt environment variable changes for a partition.

        Writes a nodefile containing the partition's hostnames and returns
        environment variable changes for COBALT_NODEFILE and COBALT_PARTSIZE.

        Args:
            node_list: List of Node objects in the partition.
            env: Current environment dict (for reference).
            part_id: Partition identifier for nodefile naming.

        Returns:
            Dict with COBALT_NODEFILE path and COBALT_PARTSIZE (if they exist
            in env and differ from the partition values).
        """
        if not node_list:
            return {}

        if part_id is None:
            raise ValueError("part_id is required for Cobalt get_partition_env")

        # Write nodefile
        nodefile_path = self._write_nodefile(part_id, node_list)
        n_nodes_str = str(len(node_list))

        changes = {}

        # Always set nodefile path if COBALT_NODEFILE exists in env
        if "COBALT_NODEFILE" in env:
            changes["COBALT_NODEFILE"] = nodefile_path

        # Only include COBALT_PARTSIZE if it exists and differs
        if "COBALT_PARTSIZE" in env and env["COBALT_PARTSIZE"] != n_nodes_str:
            changes["COBALT_PARTSIZE"] = n_nodes_str

        return changes

    def release_partition_env(self, part_id: str) -> None:
        """
        Remove the nodefile created for the given partition.

        Args:
            part_id: Identifier of the partition being released.
        """
        self._remove_nodefile(part_id)

