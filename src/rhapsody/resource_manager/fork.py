
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

    def get_partition_env(
        self, node_list: list, env: dict, part_id: str | None = None
    ) -> dict:
        """
        Return environment variable changes for a partition.

        Fork does not manage environment variables for partitions,
        so this always returns an empty dict.

        Args:
            node_list: List of Node objects in the partition.
            env: Current environment dict (for reference).
            part_id: Partition identifier (unused for Fork).

        Returns:
            Empty dict (no environment changes needed).
        """
        return {}

    def release_partition_env(self, part_id: str) -> None:
        """
        Clean up environment artifacts for a released partition.

        Fork does not create any files or resources for partitions,
        so this is a no-op.

        Args:
            part_id: Identifier of the partition being released.
        """
        pass

