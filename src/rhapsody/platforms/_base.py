
import os

from typing import Optional

import radical.utils as ru

from .utils import EnumTypes

ResourceManager = EnumTypes(
    ('SLURM', 'slurm')
)


class PlatformDescription(ru.TypedDict):

    _schema = {
        'resource_manager': str,
        'partition': str,
        'env_setup': [str],
        'work_dir': str,
        'cores_per_node': int,
        'gpus_per_node': int,
        'mem_per_node': int,
        'hwthreads_per_core': int,
        'fqdn': str,
        'description': str,
        'docs_url': str,
        'resources': {str: None}
    }

    _defaults = {
        'env_setup': [],
        'work_dir': os.path.expanduser('~'),
        'cores_per_node': 1,
        'gpus_per_node': 0,
        'hwthreads_per_core': 1,
    }

    def __init__(self, **kwargs):
        super().__init__(from_dict=kwargs)
        self._cpus_per_node = 0

    @property
    def cpus_per_node(self) -> Optional[int]:
        if not self._cpus_per_node and self.cores_per_node:
            self._cpus_per_node = self.cores_per_node * self.hwthreads_per_core
        return self._cpus_per_node

    def set_resources(self):
        if self.resource_manager == ResourceManager.SLURM:
            from .resource_managers import SLURM as rm
        else:
            raise RuntimeError('ResourceManager unknown or not set')

        rm.set_resources(pd=self)

