
import os

from .base import ResourceManager
from .base import RMInfo


class Torque(ResourceManager):

    @staticmethod
    def batch_started():
        return bool(os.getenv("PBS_JOBID"))

    def init_from_scratch(self, rm_info: RMInfo) -> RMInfo:
        nodefile = os.environ.get("PBS_NODEFILE")
        if not nodefile:
            raise RuntimeError("$PBS_NODEFILE not set")

        nodes = self._parse_nodefile(nodefile)

        if not rm_info.cores_per_node:
            rm_info.cores_per_node = self._get_cores_per_node(nodes)

        rm_info.node_list = self._get_node_list(nodes, rm_info)

        return rm_info

