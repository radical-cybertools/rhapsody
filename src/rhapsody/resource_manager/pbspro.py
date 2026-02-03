
import logging
import os
import subprocess

from .base import ResourceManager

logger = logging.getLogger(__name__)


class PBSPro(ResourceManager):

    @staticmethod
    def batch_started():
        return bool(os.getenv("PBS_JOBID"))

    def _initialize(self) -> None:
        rm_info = self._rm_info
        nodes = None

        try:
            vnodes, rm_info.cores_per_node = self._parse_pbspro_vnodes()
            nodes = [(node, rm_info.cores_per_node) for node in vnodes]

        except (IndexError, ValueError):
            logger.debug("exec_vnodes not detected")

        except RuntimeError as e:
            err_message = str(e)
            if not err_message.startswith("qstat failed"):
                raise
            logger.debug("%s", err_message)

        if not nodes:
            if not rm_info.cores_per_node or "PBS_NODEFILE" not in os.environ:
                raise RuntimeError(
                    "resource configuration unknown, either cores_per_node or $PBS_NODEFILE not set"
                )

            nodes = self._parse_nodefile(
                os.environ["PBS_NODEFILE"], cpn=rm_info.cores_per_node, smt=rm_info.threads_per_core
            )

        rm_info.node_list = self._get_node_list(nodes, rm_info)

    def _parse_pbspro_vnodes(self) -> tuple[list[str], int]:
        # PBS Job ID
        jobid = os.environ.get("PBS_JOBID")
        if not jobid:
            raise RuntimeError("$PBS_JOBID not set")

        # Get the output of qstat -f for this job
        proc = subprocess.run(["qstat", "-f", jobid], capture_output=True, text=True)
        if proc.returncode:
            raise RuntimeError(f"qstat failed: {proc.stderr}")

        # Get the (multiline) "exec_vnode" entry
        vnodes_str = ""
        for line in proc.stdout.splitlines():
            # Detect start of entry
            if "exec_vnode = " in line:
                vnodes_str += line.strip()
            elif vnodes_str:
                # Find continuing lines
                if " = " in line:
                    break
                vnodes_str += line.strip()

        # Get the RHS of the entry
        rhs = vnodes_str.split("=", 1)[1].strip()
        logger.debug("exec_vnodes: %s", rhs)

        nodes_list = []
        # Break up the individual node partitions into vnode slices
        while True:
            idx = rhs.find(")+(")
            node_str = rhs[1:idx]
            nodes_list.append(node_str)
            rhs = rhs[idx + 2 :]
            if idx < 0:
                break

        vnodes_set = set()
        ncpus_set = set()
        # Split out the slices into vnode name and cpu count
        for node_str in nodes_list:
            slices = node_str.split("+")
            for _slice in slices:
                vnode, ncpus = _slice.split(":")
                vnodes_set.add(vnode)
                ncpus_set.add(int(ncpus.split("=")[1]))

        logger.debug("vnodes: %s", vnodes_set)
        logger.debug("ncpus: %s", ncpus_set)

        if len(ncpus_set) > 1:
            raise RuntimeError("detected vnodes of different sizes")

        return sorted(vnodes_set), ncpus_set.pop()

