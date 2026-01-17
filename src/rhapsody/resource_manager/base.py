
import logging
import tempfile
from collections import defaultdict
from dataclasses import dataclass
from dataclasses import field, InitVar
from typing import Any
from typing import Optional

from rc.process import Process

T_NODE_LIST = list[dict[str, Any]]

logger = logging.getLogger(__name__)


@dataclass
class RMConfig:
    """
    Resource Manager configuration class.

    backup_nodes:     number of backup nodes (0)
    requested_nodes:  number of requested nodes (0)
    oversubscribe:    allow oversubscription (False)
    fake_resources:   use fake resources (False)
    exact:            require exclusive node access (False)
    network:          network interface to use (None)
    blocked_cores:    list of blocked core indices ([])
    blocked_gpus:     list of blocked gpu indices ([])
    """

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
    """

    # tuples of node uids and names (kept flexible; tighten if you know exact types)
    node_list: list[Any] = field(default_factory=list)
    backup_list: list[Any] = field(default_factory=list)

    cores_per_node: int = 0
    threads_per_core: Optional[int] = 1

    gpus_per_node: Optional[int] = 0
    threads_per_gpu: int = 1
    mem_per_gpu: int = 0

    lfs_per_node: int = 0
    lfs_path: str = tempfile.gettempdir()
    mem_per_node: int = 0

    cfg: "RMConfig" = field(default_factory=lambda: RMConfig())

    def verify(self) -> None:
        assert self.node_list, "node_list must be non-empty"
        assert self.cores_per_node, "cores_per_node must be non-zero"
        assert self.gpus_per_node is not None, "gpus_per_node must not be None"
        assert self.threads_per_core is not None, "threads_per_core must not be None"


@dataclass
class Node:
    """
    name : hostname of the node
    index: index of the node in the node list
    cores: number of cores
    gpus : number of gpus
    lfs  : local filesystem size (MB)
    mem  : memory size (MB)

    partition_id    : partition id of the node
    blocked_cores   : list of cores reserved/blocked by the system
    threads_per_core: number of threads per core
    threads_per_gpu : number of threads per gpu
    mem_per_gpu     : memory per gpu (MB)
    lfs_path        : path to local filesystem
    """

    name: str
    index: int
    cores: int | None = None
    gpus: int | None = None
    lfs: int | None = None
    mem: int | None = None

    blocked_cores: list[int] | None = None
    threads_per_core: int | None = None
    threads_per_gpu: int | None = None
    mem_per_gpu: int | None = None
    lfs_path: str | None = None

    rm_info: InitVar[Optional[RMInfo]]= None

    _partition_id: Optional[int] = field(default=None, init=False, repr=False)

    def __post_init__(self, rm_info: RMInfo = None) -> None:

        if not rm_info:
            # ensure that cores and gpus are set
            if not self.cores:
                raise ValueError("cores must be provided if no rm_info is given")
            if not self.gpus:
                raise ValueError("gpus must be provided if no rm_info is given")

        # otherwise rm_info will provide missing resource info
        if self.cores is None:
            self.cores = rm_info.cores_per_node
        if self.gpus is None:
            self.gpus = rm_info.gpus_per_node
        if self.lfs is None:
            self.lfs = rm_info.lfs_per_node
        if self.mem is None:
            self.mem = rm_info.mem_per_node
        if self.blocked_cores is None:
            self.blocked_cores = rm_info.cfg.blocked_cores
        if self.threads_per_core is None:
            self.threads_per_core = rm_info.threads_per_core
        if self.threads_per_gpu is None:
            self.threads_per_gpu = rm_info.threads_per_gpu
        if self.mem_per_gpu is None:
            self.mem_per_gpu = rm_info.mem_per_gpu
        if self.lfs_path is None:
            self.lfs_path = rm_info.lfs_path


    @property
    def partition_id(self) -> Optional[str]:
        return self._partition_id

    @partition_id.setter
    def partition_id(self, v: Optional[str]) -> None:

        if v is None:
            self._partition_id = None
            return

        cur = self._partition_id
        if cur is not None:
            raise ValueError(f"node {self.name} is already in partition {cur}")

        self._partition_id = v

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

    # defines for resource manager types
    FORK = "FORK"
    CCM = "CCM"
    LSF = "LSF"
    PBSPRO = "PBSPRO"
    SLURM = "SLURM"
    TORQUE = "TORQUE"
    COBALT = "COBALT"
    YARN = "YARN"
    DEBUG = "DEBUG"

    # defines for node states
    FREE = 0.0
    BUSY = 1.0
    DOWN = None

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
            [cls.SLURM, Slurm],
            [cls.PBSPRO, PBSPro],
            [cls.TORQUE, Torque],
            [cls.COBALT, Cobalt],
            [cls.LSF, LSF],
            [cls.FORK, Fork],
        ]

        if name:
            try:
                for rm_name, rm_impl in rms:
                    if rm_name == name:
                        logger.debug("create RM %s", rm_name)
                        return rm_impl(cfg)

                raise RuntimeError(f"no such ResourceManager: {name}")

            except Exception as e:
                raise RuntimeError(f"RM {name} creation failed") from e

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
                    "name" : str                          # node name
                    "index": int                          # node index
                    "cores": [self.FREE, self.FREE, ...]  # cores status
                    "gpus" : [self.FREE, self.FREE, ...]  # gpus status
                    "lfs"  : int                          # lfs per node (MB)
                    "mem"  : int                          # mem per node (MB)
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
        rm_info.cfg = cfg

        self._rm_info = rm_info

        # let the RM implementation gather resource information
        print("initialize")
        self._initialize()

        # we expect to have a valid node list now
        logger.info(f"found {len(rm_info.node_list)} nodes")

        self._filter_nodes()

    def _filter_nodes(self, check_nodes: bool = False) -> None:
        """
        Apply filtering to the node list as per RM configuration, and, if
        requested, check if nodes are accessible via ssh.
        """

        rm_info = self._rm_info

        # ensure that blocked resources are marked as down
        blocked_cores = rm_info.cfg.blocked_cores
        blocked_gpus = rm_info.cfg.blocked_gpus

        if blocked_cores or blocked_gpus:
            rm_info.cores_per_node -= len(blocked_cores)
            rm_info.gpus_per_node -= len(blocked_gpus)

            for node in rm_info.node_list:
                for idx in blocked_cores:
                    assert len(node["cores"]) > idx
                    node["cores"][idx] = self.DOWN

                for idx in blocked_gpus:
                    assert len(node["gpus"]) > idx
                    node["gpus"][idx] = self.DOWN

        assert rm_info.cfg.requested_nodes <= len(rm_info.node_list)

        # if requested, check all nodes for accessibility via ssh
        # FIXME: add configurable to limit number of concurrent ssh procs
        if check_nodes:
            procs = []
            for node in rm_info.node_list:
                name = node["name"]
                cmd = f"ssh -oBatchMode=yes {name} hostname"
                logger.debug("check node: %s [%s]", name, cmd)
                proc = Process(cmd)
                proc.start()
                procs.append([name, proc, node])

            ok = []
            for name, proc, node in procs:
                proc.wait(timeout=15)
                logger.debug("check node: %s [%s]", name, [proc.stdout, proc.stderr, proc.retcode])
                if proc.retcode is not None:
                    if not proc.retcode:
                        ok.append(node)
                else:
                    logger.warning(
                        "check node: %s [%s] timed out", name, [proc.stdout, proc.stderr]
                    )
                    proc.cancel()
                    proc.wait(timeout=15)
                    if proc.retcode is None:
                        logger.warning(
                            "check node: %s [%s] timed out again", name, [proc.stdout, proc.stderr]
                        )

            logger.warning("using %d nodes out of %d", len(ok), len(procs))

            if not ok:
                raise RuntimeError("no accessible nodes found")

            # limit the node list to the requested number of nodes
            rm_info.node_list = ok

        # reduce the node list to the requested size
        rm_info.backup_list = []
        if rm_info.cfg.requested_nodes and len(rm_info.node_list) > rm_info.cfg.requested_nodes:
            logger.debug(
                "reduce %d nodes to %d", len(rm_info.node_list), rm_info.cfg.requested_nodes
            )
            rm_info.node_list = rm_info.node_list[: rm_info.cfg.requested_nodes]
            rm_info.backup_list = rm_info.node_list[rm_info.cfg.requested_nodes :]

        # check if we can do any work
        if not rm_info.node_list:
            raise RuntimeError("ResourceManager has no nodes left to run tasks")

    def _parse_nodefile(self, fname: str, cpn: Optional[int] = 0, smt: Optional[int] = 1) -> list:
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
            nodes = defaultdict(int)
            with open(fname, encoding="utf-8") as fin:
                for line in fin.readlines():
                    node = line.strip()
                    assert " " not in node
                    nodes[node] += 1

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

        cores_per_node = (node[1] for node in nodes)

        if len(cores_per_node) == 1:
            cores_per_node = cores_per_node.pop()
            logger.debug("found %d [%d cores]", len(nodes), cores_per_node)
            return cores_per_node

        else:
            raise ValueError("non-uniform node list, cores_per_node invalid")

    def _get_node_list(self, nodes: list[str], rm_info: RMInfo) -> T_NODE_LIST:
        """
        From a node dict as returned by `self._parse_nodefile()`, and from
        additonal per-node information stored in rm_info, create a node list
        as required for rm_info.
        """

        node_list = [Node(name=node, index=idx, rm_info=rm_info)
                     for idx, node in enumerate(nodes)]

        return node_list

    def get_hostlist_by_range(self, hoststring, prefix="", width=0):
        """Convert string with host IDs into list of hosts.

        Example: Cobalt RM would have host template as "nid%05d"
                    get_hostlist_by_range("1-3,5", prefix="nid", width=5) =>
                    ["nid00001", "nid00002", "nid00003", "nid00005"]
        """

        if not hoststring.replace("-", "").replace(",", "").isnumeric():
            raise ValueError(f"non numeric set of ranges ({hoststring})")

        host_ids = []
        id_width = 0

        for num in hoststring.split(","):
            num_range = num.split("-")

            if len(num_range) > 1:
                num_lo, num_hi = num_range
                if not num_lo or not num_hi:
                    raise ValueError(f"incorrect range format ({num})")
                host_ids.extend(list(range(int(num_lo), int(num_hi) + 1)))

            else:
                host_ids.append(int(num_range[0]))

            id_width = max(id_width, *[len(n) for n in num_range])

        width = width or id_width
        return [f"{prefix}{hid:0{width}d}" for hid in host_ids]

    def get_hostlist(self, hoststring):
        """Convert string with hosts (IDs within brackets) into list of hosts.

        Example: "node-b1-[1-3,5],node-c1-4,node-d3-3,node-k[10-12,15]" =>
                 ["node-b1-1", "node-b1-2", "node-b1-3", "node-b1-5",
                  "node-c1-4", "node-d3-3",
                  "node-k10", "node-k11", "node-k12", "node-k15"]
        """

        output = []
        hoststring += ","
        host_group = []

        idx, idx_stop = 0, len(hoststring)
        while idx != idx_stop:
            comma_idx = hoststring.find(",", idx)
            bracket_idx = hoststring.find("[", idx)

            if comma_idx >= 0 and (bracket_idx == -1 or comma_idx < bracket_idx):
                if host_group:
                    prefix = hoststring[idx:comma_idx]
                    if prefix:
                        for h_idx in range(len(host_group)):
                            host_group[h_idx] += prefix
                    output.extend(host_group)
                    del host_group[:]

                else:
                    output.append(hoststring[idx:comma_idx])

                idx = comma_idx + 1

            elif bracket_idx >= 0 and (comma_idx == -1 or bracket_idx < comma_idx):
                prefix = hoststring[idx:bracket_idx]
                if not host_group:
                    host_group.append(prefix)
                else:
                    for h_idx in range(len(host_group)):
                        host_group[h_idx] += prefix

                closed_bracket_idx = hoststring.find("]", bracket_idx)
                range_set = hoststring[(bracket_idx + 1) : closed_bracket_idx]

                host_group_ = []
                for prefix in host_group:
                    host_group_.extend(self.get_hostlist_by_range(range_set, prefix))
                host_group = host_group_

                idx = closed_bracket_idx + 1

        return output


    def get_partition(self, part_id: str, n_nodes: int) -> list:
        """
        Find 'n_nodes' which don't yet belong to a partition and return them
        """

        if n_nodes <= 0:
            return []

        node_list = []
        for node in self._rm_info.node_list:
            if node.partition_id is None:
                node.partition_id = part_id
                node_list.append(node)
                if len(node_list) == n_nodes:
                    break

        if len(node_list) < n_nodes:
            raise RuntimeError(
                f"not enough free nodes to allocate partition {part_id} ({n_nodes})"
            )

        return node_list

    def release_partition(self, part_id: str) -> None:
        """
        Release all nodes belonging to partition 'part_id'
        """

        for node in self._rm_info.node_list:
            if node.partition_id == part_id:
                node.partition_id = None

