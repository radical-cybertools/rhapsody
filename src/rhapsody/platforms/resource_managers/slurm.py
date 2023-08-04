
import os

import radical.utils as ru

from ._base import LocalResourceManager


class SLURM(LocalResourceManager):

    @staticmethod
    def set_resources(pd):

        nodelist = os.environ.get('SLURM_NODELIST') or \
                   os.environ.get('SLURM_JOB_NODELIST')

        if not nodelist:
            raise RuntimeError('No allocated resources')

        # list of allocated nodes
        pd.resources.noodelist = ru.get_hostlist(nodelist)
        # cores per node from the list of allocated nodes
        pd.cores_per_node = int(os.environ.get('SLURM_CPUS_ON_NODE'))

        # gpus per node from the list of allocated nodes
        gpus_per_node = 0
        if os.environ.get('SLURM_GPUS_ON_NODE'):
            gpus_per_node = int(os.environ['SLURM_GPUS_ON_NODE'])
        else:
            gpu_ids = os.environ.get('SLURM_JOB_GPUS') or \
                      os.environ.get('SLURM_STEP_GPUS') or \
                      os.environ.get('GPU_DEVICE_ORDINAL')
            if gpu_ids:
                # list of GPU IDs (global or cgroup context)
                gpus_per_node = len(gpu_ids.split(','))
        pd.gpus_per_node = gpus_per_node

