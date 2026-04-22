"""
PBS Professional resource manager implementation.

PBS Pro is a workload management system for HPC environments.
"""

import logging
import os
import subprocess

from .base import ResourceManager

logger = logging.getLogger(__name__)


class PBSPro(ResourceManager):
    """
    PBS Professional resource manager implementation.

    Discovers resources from PBS Pro batch system environment variables.
    Attempts to parse exec_vnode information via qstat, falling back to
    PBS_NODEFILE if unavailable.

    Environment variables:
        - PBS_JOBID: Job identifier
        - PBS_NODEFILE: Path to nodefile containing allocated nodes
        - PBS_NUM_NODES: Number of nodes in allocation
    """

    def _initialize(self) -> None:
        """
        Initialize PBS Pro resource manager.

        First attempts to parse vnodes using qstat exec_vnode output.
        Falls back to PBS_NODEFILE parsing if qstat is unavailable.

        Raises:
            RuntimeError: If neither exec_vnode nor PBS_NODEFILE is available,
                or if cores_per_node is not configured when using PBS_NODEFILE.
        """
        rm_info = self._rm_info
        nodes = None

        try:
            vnodes, rm_info.cores_per_node = self._parse_pbspro_vnodes()
            nodes = [(node, rm_info.cores_per_node) for node in vnodes]

        except (IndexError, ValueError):
            logger.debug("exec_vnodes not detected")

        except RuntimeError as e:
            err_message = str(e)
            if not err_message.startswith("qstat failed"):
                raise
            logger.debug("%s", err_message)

        if not nodes:
            if "PBS_NODEFILE" not in os.environ:
                raise RuntimeError(
                    "resource configuration unknown: $PBS_NODEFILE not set"
                )

            nodes = self._parse_nodefile_and_cpn(
                os.environ["PBS_NODEFILE"], cpn=rm_info.cores_per_node, smt=rm_info.threads_per_core
            )

            if not rm_info.cores_per_node:
                rm_info.cores_per_node = self._get_cores_per_node(nodes)

        node_names = [n[0] for n in nodes]
        rm_info.node_list = self._get_node_list(node_names, rm_info)

    def get_partition_env(
        self, node_list: list, env: dict, part_id: str | None = None
    ) -> dict:
        """Return PBS environment variable changes for a partition."""
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

    def _parse_pbspro_vnodes(self) -> tuple[list[str], int]:
        # PBS Job ID
        jobid = os.environ.get("PBS_JOBID")
        if not jobid:
            raise RuntimeError("$PBS_JOBID not set")

        # Get the output of qstat -f for this job
        proc = subprocess.run(["qstat", "-f", jobid], capture_output=True, text=True)
        if proc.returncode:
            raise RuntimeError(f"qstat failed: {proc.stderr}")

        # Get the (multiline) "exec_vnode" entry
        vnodes_str = ""
        for line in proc.stdout.splitlines():
            # Detect start of entry
            if "exec_vnode = " in line:
                vnodes_str += line.strip()
            elif vnodes_str:
                # Find continuing lines
                if " = " in line:
                    break
                vnodes_str += line.strip()

        # Get the RHS of the entry
        rhs = vnodes_str.split("=", 1)[1].strip()
        logger.debug("exec_vnodes: %s", rhs)

        nodes_list = []
        # Break up the individual node partitions into vnode slices
        while True:
            idx = rhs.find(")+(")
            node_str = rhs[1:idx]
            nodes_list.append(node_str)
            rhs = rhs[idx + 2 :]
            if idx < 0:
                break

        vnodes_set = set()
        ncpus_set = set()
        # Split out the slices into vnode name and cpu count
        for node_str in nodes_list:
            slices = node_str.split("+")
            for _slice in slices:
                vnode, ncpus = _slice.split(":")
                vnodes_set.add(vnode)
                ncpus_set.add(int(ncpus.split("=")[1]))

        logger.debug("vnodes: %s", vnodes_set)
        logger.debug("ncpus: %s", ncpus_set)

        if len(ncpus_set) > 1:
            raise RuntimeError("detected vnodes of different sizes")

        return sorted(vnodes_set), ncpus_set.pop()

