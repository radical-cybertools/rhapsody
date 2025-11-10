
__copyright__ = 'Copyright 2016-2022, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import math
import multiprocessing

from .base import RMInfo, ResourceManager


# ------------------------------------------------------------------------------
#
class Fork(ResourceManager):

    # --------------------------------------------------------------------------
    #
    @staticmethod
    def check() -> bool:
        # this RM is always available
        return True


    # --------------------------------------------------------------------------
    #
    def init_from_scratch(self) -> RMInfo:

        rm_info = self._rm_info
        rm_cfg  = rm_info.cfg

        rm_info.cores_per_node = multiprocessing.cpu_count()

        # FIXME: GPU, mem, lfs detection

        n_nodes        = rm_info.cfg.requested_nodes + rm_info.cfg.backup_nodes
        fake_resources = rm_cfg.fake_resources

        if n_nodes > 1 and not fake_resources:
            raise ValueError('%d nodes requested, 1 found' % n_nodes)

        nodes = [('localhost', rm_info.cores_per_node)
                   for _ in range(n_nodes)]

        rm_info.node_list = self._get_node_list(nodes, rm_info)

        import pprint
        pprint.pp(rm_info.as_dict())
        pprint.pp(rm_info.node_list)


# ------------------------------------------------------------------------------

