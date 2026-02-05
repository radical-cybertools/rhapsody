
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

