
import logging
import os

from .base import ResourceManager

logger = logging.getLogger(__name__)


class Slurm(ResourceManager):

    def _initialize(self) -> None:
        # ensure we run in a SLURM environment
        if "SLURM_JOB_ID" not in os.environ:
            raise RuntimeError("not running in a SLURM job")

        rm_info = self._rm_info

        node_list = os.environ.get("SLURM_NODELIST") or os.environ.get("SLURM_JOB_NODELIST")
        if node_list is None:
            raise RuntimeError("$SLURM_*NODELIST not set")

        # Parse SLURM nodefile environment variable
        node_names = self.get_hostlist(node_list)
        logger.debug("found node list %s. Expanded: %s", node_list, node_names)

        if not rm_info.cores_per_node:
            # $SLURM_CPUS_ON_NODE = Number of physical cores per node
            cpn_str = os.environ.get("SLURM_CPUS_ON_NODE")
            if cpn_str is None:
                raise RuntimeError("$SLURM_CPUS_ON_NODE not set")
            rm_info.cores_per_node = int(cpn_str)

        if not rm_info.gpus_per_node:
            if os.environ.get("SLURM_GPUS_ON_NODE"):
                rm_info.gpus_per_node = int(os.environ["SLURM_GPUS_ON_NODE"])
            else:
                # GPU IDs per node
                # - global context: SLURM_JOB_GPUS and SLURM_STEP_GPUS
                # - cgroup context: GPU_DEVICE_ORDINAL
                gpu_ids = (
                    os.environ.get("SLURM_JOB_GPUS")
                    or os.environ.get("SLURM_STEP_GPUS")
                    or os.environ.get("GPU_DEVICE_ORDINAL")
                )
                if gpu_ids:
                    rm_info.gpus_per_node = len(gpu_ids.split(","))

        rm_info.node_list = self._get_node_list(node_names, rm_info)

    def get_partition_env(
        self, node_list: list, env: dict, part_id: str | None = None
    ) -> dict:
        """
        Return Slurm environment variable changes for a partition.

        Only returns changes for variables that exist in env and have
        different values than the partition would require.

        Args:
            node_list: List of Node objects in the partition.
            env: Current environment dict (for reference).
            part_id: Partition identifier (unused for Slurm, which uses
                     environment variables rather than files).

        Returns:
            Dict with changed SLURM_NODELIST, SLURM_JOB_NODELIST, SLURM_NNODES,
            and/or SLURM_JOB_NUM_NODES to reflect the partition.
        """
        if not node_list:
            return {}

        hostnames = [node.name for node in node_list]
        compacted = self.compactify_hostlist(hostnames)
        nodelist_str = ",".join(compacted)
        n_nodes_str = str(len(node_list))

        # Map of env var names to their partition values
        partition_env = {
            "SLURM_NODELIST": nodelist_str,
            "SLURM_JOB_NODELIST": nodelist_str,
            "SLURM_NNODES": n_nodes_str,
            "SLURM_JOB_NUM_NODES": n_nodes_str,
        }

        # Only return changes for vars that exist in env and differ
        return {
            key: val
            for key, val in partition_env.items()
            if key in env and env[key] != val
        }

