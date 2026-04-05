"""
Cobalt resource manager implementation.

Cobalt is a job scheduler used on IBM Blue Gene systems and other HPC platforms.
"""

import os

from .base import ResourceManager


class Cobalt(ResourceManager):
    """
    Cobalt resource manager implementation.

    Supports resource discovery from Cobalt batch system environment variables.
    Handles both GPU nodes (via COBALT_NODEFILE) and compute nodes (via COBALT_PARTNAME).

    Environment variables:
        - COBALT_JOBID: Job identifier
        - COBALT_NODEFILE: Path to nodefile (for GPU nodes)
        - COBALT_PARTNAME: Partition name/node range (for compute nodes)
        - COBALT_PARTSIZE: Number of nodes in partition
    """

    def _initialize(self) -> None:
        """
        Initialize Cobalt resource manager.

        Discovers nodes from COBALT_NODEFILE or COBALT_PARTNAME environment
        variables. If cores_per_node is not set, it is determined by counting
        the cores on the current host (localhost), assuming that the agent runs
        on a compute node and that all nodes in the allocation are identical.

        Raises:
            RuntimeError: If neither COBALT_NODEFILE nor COBALT_PARTNAME is available.
        """
        rm_info = self._rm_info

        if not rm_info.cores_per_node:
            rm_info.cores_per_node = os.cpu_count() or 1

        if "COBALT_NODEFILE" in os.environ:
            # this env variable is used for GPU nodes
            nodefile = os.environ["COBALT_NODEFILE"]
            nodes = self._parse_nodefile_and_cpn(nodefile, rm_info.cores_per_node)

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

        node_names = [n[0] for n in nodes]
        rm_info.node_list = self._get_node_list(node_names, rm_info)

    def get_partition_env(
        self, node_list: list, env: dict, part_id: str | None = None
    ) -> dict:
        """Return Cobalt environment variable changes for a partition."""
        return self._get_partition_env_with_nodefile(
            node_list, env, part_id,
            nodefile_var="COBALT_NODEFILE",
            node_count_var="COBALT_PARTSIZE"
        )

    def release_partition_env(self, part_id: str) -> None:
        """
        Remove the nodefile created for the given partition.

        Args:
            part_id: Identifier of the partition being released.
        """
        self._remove_nodefile(part_id)

