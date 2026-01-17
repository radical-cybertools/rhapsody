
import multiprocessing

from .base import ResourceManager
from .base import RMInfo


class Fork(ResourceManager):

    def _initialize(self) -> RMInfo:
        rm_info = self._rm_info
        rm_cfg = rm_info.cfg

        rm_info.cores_per_node = multiprocessing.cpu_count()

        # FIXME: GPU, mem, lfs detection
        n_nodes = rm_info.cfg.requested_nodes + rm_info.cfg.backup_nodes
        fake_resources = rm_cfg.fake_resources

        if n_nodes > 1 and not fake_resources:
            raise ValueError("1 out of {n_nodes} nodes found (fake disabled)")

        nodes = ["localhost" for _ in range(n_nodes)]

        rm_info.node_list = self._get_node_list(nodes, rm_info)

