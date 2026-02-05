"""
LSF (Load Sharing Facility) resource manager implementation.

LSF is IBM's workload management platform for distributed HPC environments.
"""

import logging
import os

from .base import ResourceManager

logger = logging.getLogger(__name__)


class LSF(ResourceManager):
    """
    LSF (Load Sharing Facility) resource manager implementation.

    Discovers resources from LSF batch system environment variables,
    particularly the LSB_DJOB_HOSTFILE which contains node allocations.

    Environment variables:
        - LSB_JOBID: Job identifier
        - LSB_DJOB_HOSTFILE: Path to hostfile containing allocated nodes
    """

    def _initialize(self) -> None:
        """
        Initialize LSF resource manager.

        Parses LSB_DJOB_HOSTFILE to discover allocated nodes, filtering out
        login and batch nodes. Validates cores_per_node configuration.

        Raises:
            RuntimeError: If LSB_DJOB_HOSTFILE is not set.
            AssertionError: If configured cores_per_node doesn't match discovered value.
        """
        rm_info = self._rm_info

        # LSF hostfile format:
        #
        #     node_1
        #     node_1
        #     ...
        #     node_2
        #     node_2
        #     ...
        #
        # There are in total "-n" entries (number of tasks of the job)
        # and "-R" entries per node (tasks per host).
        #
        hostfile = os.environ.get("LSB_DJOB_HOSTFILE")
        if not hostfile:
            raise RuntimeError("$LSB_DJOB_HOSTFILE not set")

        smt = rm_info.threads_per_core
        nodes = self._parse_nodefile_and_cpn(hostfile, smt=smt)

        # LSF adds login and batch nodes to the hostfile (with 1 core) which
        # needs filtering out.
        #
        # It is possible that login/batch nodes were not marked at hostfile
        # and were not filtered out, thus we assume that there is only one
        # such node with 1 physical core, i.e., equals to SMT threads
        # (otherwise assertion error will be raised later)
        # *) affected machine(s): Lassen@LLNL
        filtered = []
        for node in nodes:
            if "login" not in node[0] and "batch" not in node[0] and smt != node[1]:
                filtered.append(node)
        nodes = filtered

        lsf_cores_per_node = self._get_cores_per_node(nodes)
        if rm_info.cores_per_node:
            assert rm_info.cores_per_node == lsf_cores_per_node
        else:
            rm_info.cores_per_node = lsf_cores_per_node

        logger.debug("found %d nodes with %d cores", len(nodes), rm_info.cores_per_node)

        # While LSF node names are unique and could serve as node uids, we
        # need an integer index later on for resource set specifications.
        # (LSF starts node indexes at 1, not 0)
        node_names = [n[0] for n in nodes]
        rm_info.node_list = self._get_node_list(node_names, rm_info)

        # NOTE: blocked cores should be in sync with SMT level,
        #       as well with CPU indexing type ("logical" vs "physical").
        #       Example of CPU indexing on Summit:
        #          https://github.com/olcf-tutorials/ERF-CPU-Indexing
        #
        # The current approach uses "logical" CPU indexing
        # FIXME: set cpu_indexing as a parameter in resource config

    def get_partition_env(
        self, node_list: list, env: dict, part_id: str | None = None
    ) -> dict:
        """Return LSF environment variable changes for a partition."""
        return self._get_partition_env_with_nodefile(
            node_list, env, part_id,
            nodefile_var="LSB_DJOB_HOSTFILE"
        )

    def release_partition_env(self, part_id: str) -> None:
        """
        Remove the hostfile created for the given partition.

        Args:
            part_id: Identifier of the partition being released.
        """
        self._remove_nodefile(part_id)

