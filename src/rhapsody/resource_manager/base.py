
__copyright__ = "Copyright 2016-2023, The RADICAL-Cybertools Team"
__license__   = "MIT"


import logging
from dataclasses import dataclass
from dataclasses import field
from enum import Enum
from typing import Any
from typing import Optional

from rc.process import Process

T_NODE_LIST = list[dict[str, Any]]

logger = logging.getLogger(__name__)


# enum for resource manager types
class RMType(Enum):
    FORK    = "FORK"
    CCM     = "CCM"
    LSF     = "LSF"
    PBSPRO  = "PBSPRO"
    SLURM   = "SLURM"
    TORQUE  = "TORQUE"
    COBALT  = "COBALT"
    YARN    = "YARN"
    DEBUG   = "DEBUG"


# enum for node states
class RMStatus(Enum):
    FREE    = 0.0
    BUSY    = 1.0
    DOWN    = None

@dataclass
class RMConfig:
    '''
    Resource Manager configuration class.

    backup_nodes:     number of backup nodes (0)
    requested_nodes:  number of requested nodes (0)
    oversubscribe:    allow oversubscription (False)
    fake_resources:   use fake resources (False)
    exact:            require exclusive node access (False)
    network:          network interface to use (None)
    blocked_cores:    list of blocked core indices ([])
    blocked_gpus:     list of blocked gpu indices ([])
    '''

    backup_nodes: int = 0
    requested_nodes: int = 0
    oversubscribe: bool = False
    fake_resources: bool = False
    exact: bool = False
    network: Optional[str] = None
    blocked_cores: list[int] = field(default_factory=list)
    blocked_gpus: list[int] = field(default_factory=list)


@dataclass
class RMInfo:
    """
    Resource-manager runtime information.

    Instances carry the discovered/derived resource topology and capacities for
    a specific RM instance (nodes, CPU/GPU layout, memory, local filesystem),
    plus the RM configuration used to interpret or constrain resources.

    Construction applies sensible defaults and validates required fields via
    `__post_init__()`.

    node_list : list
        Tuples of node uids and names.
    backup_list : list
        List of backup nodes.

    cores_per_node : int
        Number of cores per node.
    threads_per_core : int | None
        Number of threads per core.

    gpus_per_node : int | None
        Number of GPUs per node.
    threads_per_gpu : int
        Number of threads per GPU.
    mem_per_gpu : int
        Memory per GPU (MB).

    lfs_per_node : int
        Node local filesystem size (MB).
    lfs_path : str
        Node local filesystem path.
    mem_per_node : int
        Memory per node (MB).

    cfg : RMConfig
        Resource manager config.

    numa_domain_map : dict[int, Any]
        Resources per NUMA domain.
    """

    # tuples of node uids and names (kept flexible; tighten if you know exact types)
    node_list: list[Any] = field(default_factory=list)
    backup_list: list[Any] = field(default_factory=list)

    cores_per_node: int = 0
    threads_per_core: Optional[int] = 0

    gpus_per_node: Optional[int] = 0
    threads_per_gpu: int = 1
    mem_per_gpu: int = 0

    lfs_per_node: int = 0
    lfs_path: str = "/tmp/"
    mem_per_node: int = 0

    cfg: "RMConfig" = field(default_factory=lambda: RMConfig())

    numa_domain_map: dict[int, "RMInfo"] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self._verify()

    def _verify(self) -> None:
        assert self.node_list, "node_list must be non-empty"
        assert self.cores_per_node, "cores_per_node must be non-zero"
        assert self.gpus_per_node is not None, "gpus_per_node must not be None"
        assert self.threads_per_core is not None, "threads_per_core must not be None"


# Base class for ResourceManager implementations.
class ResourceManager:
    """
    The Resource Manager provides fundamental resource information via
    `self.info` (see `RMInfo` class definition).

      ResourceManager.node_list      : list of nodes names and uids
      ResourceManager.cores_per_node : number of cores each node has available
      ResourceManager.gpus_per_node  : number of gpus  each node has available

    Schedulers can rely on these information to be available.  Specific
    ResourceManager incarnation may have additional information available -- but
    schedulers relying on those are invariably bound to the specific
    ResourceManager.
    """

    def __init__(self, cfg: Optional[RMConfig] = None) -> None:

        self.name = type(self).__name__
        logger.debug("configuring RM %s", self.name)

        if cfg is None:
            cfg = RMConfig()

        logger.debug("RM init from scratch: %s", cfg)

        self._init_info(cfg)
        self._rm_info.verify()

        #  FIXME RHAPSODY: where to put this?
      # # immediately set the network interface if it was configured
      # # NOTE: setting this here implies that no ZMQ connectio was set up
      # #       before the ResourceManager got created!
      # if rm_info.details.get("network"):
      #     rc_cfg = ru.config.DefaultConfig()
      #     rc_cfg.iface = rm_info.details["network"]


    @classmethod
    def get_instance(cls, name=None, cfg: Optional[RMConfig] = None):
        """
        Factory method to create ResourceManager instances.
        """

        cfg = RMConfig(cfg)

        # Make sure that we are the base-class!
        if cls != ResourceManager:
            raise TypeError("ResourceManager Factory only available to base class!")

        from .cobalt import Cobalt
        from .fork import Fork
        from .lsf import LSF
        from .pbspro import PBSPro
        from .slurm import Slurm
        from .torque import Torque

        # ordered list of RMs.  SLURM is most likely, FORK is fallback if no
        # other RM is detected.
        rms = [
            [RMType.SLURM , Slurm],
            [RMType.PBSPRO, PBSPro],
            [RMType.TORQUE, Torque],
            [RMType.COBALT, Cobalt],
            [RMType.LSF   , LSF],
            [RMType.FORK  , Fork],
        ]

        if name:
            try:
                for rm_name, rm_impl in rms:
                    if rm_name == name:
                        logger.debug("create RM %s", rm_name)
                        return rm_impl(cfg)

                raise RuntimeError("no such ResourceManager: %s" % name)

            except Exception as e:
                raise RuntimeError("RM %s creation failed" % name) from e

        else:
            rm = None
            for rm_name, rm_impl in rms:

                try:
                    logger.debug("try RM %s", rm_name)
                    rm = rm_impl(cfg)

                except Exception as e:
                    logger.exception("RM %s failed: %s", rm_name, e)

            if not rm:
                raise RuntimeError("no ResourceManager detected")

            return rm

    @property
    def info(self):
        return self._rm_info

    @property
    def node_list(self):
        return self._rm_info.node_list

    def _initialize(self) -> None:
        """
        This method MUST be overloaded by any RM implementation.  It will be
        called during `_initialize` and is expected to check and correct
        or complete node information, such as `cores_per_node`, `gpus_per_node`
        etc., and to provide `rm_info.node_list` of the following form:

            node_list = [
                {
                    "name" : str                        # node name
                    "index": int                        # node index
                    "cores": [RMStatus.FREE, RMStatus.FREE, ...]  # cores per node
                    "gpus" : [RMStatus.FREE, RMStatus.FREE, ...]  # gpus per node
                    "lfs"  : int                        # lfs per node (MB)
                    "mem"  : int                        # mem per node (MB)
                },
                ...
            ]

        The node entries can be augmented with additional information which may
        be interpreted by the specific scheduler instance.
        """

        raise NotImplementedError("_initialize is not implemented")

    def _init_info(self, cfg):
        """
        Initialize RMInfo structure with configuration and default values.
        """

        rm_info = RMInfo()
        rm_info.cfg = ru.Config(cfg)

        self._rm_info = rm_info

        # let the RM implementation gather resource information
        self._initialize()

        # we expect to have a valid node list now
        logger.info("node list: %s", rm_info.node_list)

        self._filter_nodes()


    def _filter_nodes(self, check_nodes: bool = False) -> None:
        """
        Apply filtering to the node list as per RM configuration, and, if
        requested, check if nodes are accessible via ssh.
        """

        rm_info = self._rm_info

        # ensure that blocked resources are marked as down
        blocked_cores = rm_info.cfg.get("blocked_cores", [])
        blocked_gpus  = rm_info.cfg.get("blocked_gpus" , [])

        if blocked_cores or blocked_gpus:

            rm_info.cores_per_node -= len(blocked_cores)
            rm_info.gpus_per_node  -= len(blocked_gpus)

            for node in rm_info.node_list:

                for idx in blocked_cores:
                    assert len(node["cores"]) > idx
                    node["cores"][idx] = RMStatus.DOWN

                for idx in blocked_gpus:
                    assert len(node["gpus"]) > idx
                    node["gpus"][idx] = RMStatus.DOWN

        assert rm_info.cfg.requested_nodes <= len(rm_info.node_list)


        # if requested, check all nodes for accessibility via ssh
        # FIXME: add configurable to limit number of concurrent ssh procs
        if check_nodes:
            procs = list()
            for node in rm_info.node_list:
                name = node["name"]
                cmd  = "ssh -oBatchMode=yes %s hostname" % name
                logger.debug("check node: %s [%s]", name, cmd)
                proc = Process(cmd)
                proc.start()
                procs.append([name, proc, node])

            ok = list()
            for name, proc, node in procs:
                proc.wait(timeout=15)
                logger.debug("check node: %s [%s]", name,
                                [proc.stdout, proc.stderr, proc.retcode])
                if proc.retcode is not None:
                    if not proc.retcode:
                        ok.append(node)
                else:
                    logger.warning("check node: %s [%s] timed out",
                                      name, [proc.stdout, proc.stderr])
                    proc.cancel()
                    proc.wait(timeout=15)
                    if proc.retcode is None:
                        logger.warning("check node: %s [%s] timed out again",
                                           name, [proc.stdout, proc.stderr])

            logger.warning("using %d nodes out of %d", len(ok), len(procs))

            if not ok:
                raise RuntimeError("no accessible nodes found")

            # limit the node list to the requested number of nodes
            rm_info.node_list = ok

        # reduce the node list to the requested size
        rm_info.backup_list = list()
        if rm_info.cfg.requested_nodes and \
           len(rm_info.node_list) > rm_info.cfg.requested_nodes:

            logger.debug("reduce %d nodes to %d", len(rm_info.node_list),
                                                      rm_info.cfg.requested_nodes)
            rm_info.node_list   = rm_info.node_list[:rm_info.cfg.requested_nodes]
            rm_info.backup_list = rm_info.node_list[rm_info.cfg.requested_nodes:]

        # check if we can do any work
        if not rm_info.node_list:
            raise RuntimeError("ResourceManager has no nodes left to run tasks")

    def _parse_nodefile(self, fname: str,
                              cpn  : Optional[int] = 0,
                              smt  : Optional[int] = 1) -> list:
        """
        parse the given nodefile and return a list of tuples of the form

            [["node_1", 42 * 4],
             ["node_2", 42 * 4],
             ...
            ]

        where the first tuple entry is the name of the node found, and the
        second entry is the number of entries found for this node.  The latter
        number usually corresponds to the number of process slots available on
        that node.

        Some nodefile formats though have one entry per node, not per slot.  In
        those cases we"ll use the passed cores per node (`cpn`) to fill the slot
        count for the returned node list (`cpn` will supercede the detected slot
        count).

        An invalid or un-parsable file will result in an empty list being
        returned.
        """

        if not smt:
            smt = 1

        logger.info("using nodefile: %s", fname)
        try:
            nodes = dict()
            with ru.ru_open(fname, "r") as fin:
                for line in fin.readlines():
                    node = line.strip()
                    assert " " not in node
                    if node in nodes: nodes[node] += 1
                    else            : nodes[node]  = 1

            if cpn:
                for node in list(nodes.keys()):
                    nodes[node] = cpn

            # convert node dict into tuple list
            return list(nodes.keys())

        except Exception:
            return []

    def _get_cores_per_node(self, nodes: list[str]) -> Optional[int]:
        """
        From a node dict as returned by `self._parse_nodefile()`, determine the
        number of cores per node.  To do so, we check if all nodes have the same
        number of cores.  If that is the case we return that number.  If the
        node list is heterogeneous we will raise an `ValueError`.
        """

        cores_per_node = set([node[1] for node in nodes])

        if len(cores_per_node) == 1:
            cores_per_node = cores_per_node.pop()
            logger.debug("found %d [%d cores]", len(nodes), cores_per_node)
            return cores_per_node

        else:
            raise ValueError("non-uniform node list, cores_per_node invalid")

    def _get_node_list(self, nodes  : list[str],
                             rm_info: RMInfo) -> T_NODE_LIST:
        """
        From a node dict as returned by `self._parse_nodefile()`, and from
        additonal per-node information stored in rm_info, create a node list
        as required for rm_info.
        """

        # FIXME: use proper data structures for nodes and resources
        # keep nodes to be indexed (node_index)
        node_list = [{"name"  : node,
                      "index" : idx,
                      "cores" : [RMStatus.FREE] * rm_info.cores_per_node,
                      "gpus"  : [RMStatus.FREE] * rm_info.gpus_per_node,
                      "lfs"   : rm_info.lfs_per_node,
                      "mem"   : rm_info.mem_per_node}
                     for idx, node in enumerate(nodes)]

        return node_list

