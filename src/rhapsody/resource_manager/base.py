
__copyright__ = 'Copyright 2016-2023, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import math
import os

from rc.process import Process
from typing     import Optional, List, Tuple, Dict, Any

T_NODE_LIST = List[Dict[str, Any]]


import logging
logger = logging.getLogger(__name__)

import radical.utils as ru

# FIXME: proper enums
# 'enum' for resource manager types
RM_NAME_FORK        = 'FORK'
RM_NAME_CCM         = 'CCM'
RM_NAME_LSF         = 'LSF'
RM_NAME_PBSPRO      = 'PBSPRO'
RM_NAME_SLURM       = 'SLURM'
RM_NAME_TORQUE      = 'TORQUE'
RM_NAME_COBALT      = 'COBALT'
RM_NAME_YARN        = 'YARN'
RM_NAME_DEBUG       = 'DEBUG'

# 'enum' for resource manager states
RM_STATUS_FREE      =  0.0
RM_STATUS_BUSY      =  1.0
RM_STATUS_DOWN      = None


# ------------------------------------------------------------------------------
#
class RMConfig(ru.TypedDict):

    _schema = {
            'backup_nodes'    : int,           # number of backup nodes
            'requested_nodes' : int,           # number of requested nodes
            'oversubscribe'   : bool,          # allow oversubscription
            'fake_resources'  : bool,          # use fake resources
            'exact'           : bool,          # require exclusive node access
            'network'         : str,           # network interface to use
            'blocked_cores'   : list,          # list of blocked core indices
            'blocked_gpus'    : list,          # list of blocked gpu indices
    }

    _defaults = {
            'backup_nodes'    : 0,
            'requested_nodes' : 0,
            'oversubscribe'   : False,
            'fake_resources'  : False,
            'exact'           : False,
            'network'         : None,
            'blocked_cores'   : list(),
            'blocked_gpus'    : list(),
    }





# ------------------------------------------------------------------------------
#
class RMInfo(ru.TypedDict):
    '''
    Each resource manager instance must gather provide the information defined
    in this class.
    '''

    _schema = {
            'node_list'            : [None],        # tuples of node uids and names
            'backup_list'          : [None],        # list of backup nodes

            'cores_per_node'       : int,           # number of cores per node
            'threads_per_core'     : int,           # number of threads per core

            'gpus_per_node'        : int,           # number of gpus per node
            'threads_per_gpu'      : int,           # number of threads per gpu
            'mem_per_gpu'          : int,           # memory per gpu (MB)

            'lfs_per_node'         : int,           # node local FS size (MB)
            'lfs_path'             : str,           # node local FS path
            'mem_per_node'         : int,           # memory per node (MB)

            'cfg'                  : RMConfig,      # resource manager config

            'numa_domain_map'      : {int: None},   # resources per numa domain
    }

    _defaults = {
            'cfg'                  : RMConfig(),

            'node_list'            : list(),
            'backup_list'          : list(),

            'cores_per_node'       : 0,
            'threads_per_core'     : 0,

            'gpus_per_node'        : 0,
            'threads_per_gpu'      : 1,
            'mem_per_gpu'          : 0,

            'lfs_per_node'         : 0,
            'lfs_path'             : '/tmp/',
            'mem_per_node'         : 0,

            'numa_domain_map'      : dict(),
    }


    # --------------------------------------------------------------------------
    #
    def _verify(self):

        assert self['node_list'        ]
        assert self['cores_per_node'   ]
        assert self['gpus_per_node'    ] is not None
        assert self['threads_per_core' ] is not None


# ------------------------------------------------------------------------------
#
# Base class for ResourceManager implementations.
#
class ResourceManager(object):
    '''
    The Resource Manager provides fundamental resource information via
    `self.info` (see `RMInfo` class definition).

      ResourceManager.node_list      : list of nodes names and uids
      ResourceManager.cores_per_node : number of cores each node has available
      ResourceManager.gpus_per_node  : number of gpus  each node has available

    Schedulers can rely on these information to be available.  Specific
    ResourceManager incarnation may have additional information available -- but
    schedulers relying on those are invariably bound to the specific
    ResourceManager.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg: Optional[RMConfig] = None) -> None:

        self.name = type(self).__name__
        logger.debug('configuring RM %s', self.name)

        if cfg is None:
            cfg = RMConfig()

        logger.debug('RM init from scratch: %s', cfg)

        self._init_info(cfg)
        self._rm_info.verify()

        #  FIXME RHAPSODY: where to put this?
      # # immediately set the network interface if it was configured
      # # NOTE: setting this here implies that no ZMQ connectio was set up
      # #       before the ResourceManager got created!
      # if rm_info.details.get('network'):
      #     rc_cfg = ru.config.DefaultConfig()
      #     rc_cfg.iface = rm_info.details['network']


    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate sub-class for the
    # ResourceManager.
    #
    @classmethod
    def get_instance(cls, name=None, cfg: Optional[RMConfig] = None):

        cfg = RMConfig(cfg)

        # Make sure that we are the base-class!
        if cls != ResourceManager:
            raise TypeError('ResourceManager Factory only available to base class!')

        from .slurm   import Slurm
        from .pbspro  import PBSPro
        from .torque  import Torque
        from .cobalt  import Cobalt
        from .lsf     import LSF
        from .fork    import Fork

        # ordered list of RMs.  SLURM is most likely, FORK is fallback if no
        # other RM is detected.
        rms = [
            [RM_NAME_SLURM , Slurm],
            [RM_NAME_PBSPRO, PBSPro],
            [RM_NAME_TORQUE, Torque],
            [RM_NAME_COBALT, Cobalt],
            [RM_NAME_LSF   , LSF],
            [RM_NAME_FORK  , Fork],
        ]

        if name:
            try:
                for rm_name, rm_impl in rms:
                    if rm_name == name:
                        logger.debug('create RM %s', rm_name)
                        return rm_impl(cfg)

                raise RuntimeError('no such ResourceManager: %s' % name)

            except Exception as e:
                raise RuntimeError('RM %s creation failed' % name) from e

        else:
            rm = None
            for rm_name, rm_impl in rms:

                try:
                    logger.debug('try RM %s', rm_name)
                    rm = rm_impl(cfg)

                except Exception as e:
                    logger.exception('RM %s failed: %s', rm_name, e)

            if not rm:
                raise RuntimeError('no ResourceManager detected')

            return rm


    # --------------------------------------------------------------------------
    #
    @property
    def info(self):

        return self._rm_info


    @property
    def node_list(self):

        return self._rm_info.node_list


    # --------------------------------------------------------------------------
    #
    def _initialize(self) -> None:
        '''
        This method MUST be overloaded by any RM implementation.  It will be
        called during `_initialize` and is expected to check and correct
        or complete node information, such as `cores_per_node`, `gpus_per_node`
        etc., and to provide `rm_info.node_list` of the following form:

            node_list = [
                {
                    'name' : str                        # node name
                    'index': int                        # node index
                    'cores': [RM_STATUS_FREE, RM_STATUS_FREE, ...]  # cores per node
                    'gpus' : [RM_STATUS_FREE, RM_STATUS_FREE, ...]  # gpus per node
                    'lfs'  : int                        # lfs per node (MB)
                    'mem'  : int                        # mem per node (MB)
                },
                ...
            ]

        The node entries can be augmented with additional information which may
        be interpreted by the specific scheduler instance.
        '''

        raise NotImplementedError('_initialize is not implemented')


    # --------------------------------------------------------------------------
    #
    def _init_info(self, cfg):

        rm_info = RMInfo()
        rm_info.cfg = ru.Config(cfg)

        self._rm_info = rm_info

        # let the RM implementation gather resource information
        self._initialize()

        # we expect to have a valid node list now
        logger.info('node list: %s', rm_info.node_list)

        self._filter_nodes()


    # --------------------------------------------------------------------------
    #
    def _filter_nodes(self, check_nodes: bool = False) -> None:

        rm_info = self._rm_info

        # ensure that blocked resources are marked as down
        blocked_cores = rm_info.cfg.get('blocked_cores', [])
        blocked_gpus  = rm_info.cfg.get('blocked_gpus' , [])

        if blocked_cores or blocked_gpus:

            rm_info.cores_per_node -= len(blocked_cores)
            rm_info.gpus_per_node  -= len(blocked_gpus)

            for node in rm_info.node_list:

                for idx in blocked_cores:
                    assert len(node['cores']) > idx
                    node['cores'][idx] = RM_STATUS_DOWN

                for idx in blocked_gpus:
                    assert len(node['gpus']) > idx
                    node['gpus'][idx] = RM_STATUS_DOWN

        assert rm_info.cfg.requested_nodes <= len(rm_info.node_list)


        # if requested, check all nodes for accessibility via ssh
        # FIXME: add configurable to limit number of concurrent ssh procs
        if check_nodes:
            procs = list()
            for node in rm_info.node_list:
                name = node['name']
                cmd  = 'ssh -oBatchMode=yes %s hostname' % name
                logger.debug('check node: %s [%s]', name, cmd)
                proc = Process(cmd)
                proc.start()
                procs.append([name, proc, node])

            ok = list()
            for name, proc, node in procs:
                proc.wait(timeout=15)
                logger.debug('check node: %s [%s]', name,
                                [proc.stdout, proc.stderr, proc.retcode])
                if proc.retcode is not None:
                    if not proc.retcode:
                        ok.append(node)
                else:
                    logger.warning('check node: %s [%s] timed out',
                                      name, [proc.stdout, proc.stderr])
                    proc.cancel()
                    proc.wait(timeout=15)
                    if proc.retcode is None:
                        logger.warning('check node: %s [%s] timed out again',
                                           name, [proc.stdout, proc.stderr])

            logger.warning('using %d nodes out of %d', len(ok), len(procs))

            if not ok:
                raise RuntimeError('no accessible nodes found')

            # limit the node list to the requested number of nodes
            rm_info.node_list = ok

        # reduce the node list to the requested size
        rm_info.backup_list = list()
        if rm_info.cfg.requested_nodes and \
           len(rm_info.node_list) > rm_info.cfg.requested_nodes:

            logger.debug('reduce %d nodes to %d', len(rm_info.node_list),
                                                      rm_info.cfg.requested_nodes)
            rm_info.node_list   = rm_info.node_list[:rm_info.cfg.requested_nodes]
            rm_info.backup_list = rm_info.node_list[rm_info.cfg.requested_nodes:]

        # check if we can do any work
        if not rm_info.node_list:
            raise RuntimeError('ResourceManager has no nodes left to run tasks')


    # --------------------------------------------------------------------------
    #
    def _parse_nodefile(self, fname: str,
                              cpn  : Optional[int] = 0,
                              smt  : Optional[int] = 1) -> list:
        '''
        parse the given nodefile and return a list of tuples of the form

            [['node_1', 42 * 4],
             ['node_2', 42 * 4],
             ...
            ]

        where the first tuple entry is the name of the node found, and the
        second entry is the number of entries found for this node.  The latter
        number usually corresponds to the number of process slots available on
        that node.

        Some nodefile formats though have one entry per node, not per slot.  In
        those cases we'll use the passed cores per node (`cpn`) to fill the slot
        count for the returned node list (`cpn` will supercede the detected slot
        count).

        An invalid or un-parsable file will result in an empty list being
        returned.
        '''

        if not smt:
            smt = 1

        logger.info('using nodefile: %s', fname)
        try:
            nodes = dict()
            with ru.ru_open(fname, 'r') as fin:
                for line in fin.readlines():
                    node = line.strip()
                    assert ' ' not in node
                    if node in nodes: nodes[node] += 1
                    else            : nodes[node]  = 1

            if cpn:
                for node in list(nodes.keys()):
                    nodes[node] = cpn

            # convert node dict into tuple list
            return list(nodes.keys())

        except Exception:
            return []


    # --------------------------------------------------------------------------
    #
    def _get_cores_per_node(self, nodes: list[str]) -> Optional[int]:
        '''
        From a node dict as returned by `self._parse_nodefile()`, determine the
        number of cores per node.  To do so, we check if all nodes have the same
        number of cores.  If that is the case we return that number.  If the
        node list is heterogeneous we will raise an `ValueError`.
        '''

        cores_per_node = set([node[1] for node in nodes])

        if len(cores_per_node) == 1:
            cores_per_node = cores_per_node.pop()
            logger.debug('found %d [%d cores]', len(nodes), cores_per_node)
            return cores_per_node

        else:
            raise ValueError('non-uniform node list, cores_per_node invalid')


    # --------------------------------------------------------------------------
    #
    def _get_node_list(self, nodes  : list[str],
                             rm_info: RMInfo) -> T_NODE_LIST:
        '''
        From a node dict as returned by `self._parse_nodefile()`, and from
        additonal per-node information stored in rm_info, create a node list
        as required for rm_info.
        '''

        # FIXME: use proper data structures for nodes and resources
        # keep nodes to be indexed (node_index)
        node_list = [{'name'  : node,
                      'index' : idx,
                      'cores' : [RM_STATUS_FREE] * rm_info.cores_per_node,
                      'gpus'  : [RM_STATUS_FREE] * rm_info.gpus_per_node,
                      'lfs'   : rm_info.lfs_per_node,
                      'mem'   : rm_info.mem_per_node}
                     for idx, node in enumerate(nodes)]

        return node_list


# ------------------------------------------------------------------------------

