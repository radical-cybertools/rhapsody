"""DragonExecutionBackend sizes Batch's worker pools to the CPUs Dragon may use.

Batch starts ``Node.num_cpus // 2`` workers per node, where ``Node.num_cpus`` is the node's
``os.cpu_count()``. On a Slurm job that holds part of a node, Dragon may run only on
``Node.cpus``, so the backend passes ``workers_per_node`` instead. These tests swap Batch, Node
and System for fakes that behave like dragonhpc 0.15.0, so they also run where Dragon is absent.
"""

import logging
from unittest.mock import patch

import pytest

from rhapsody.backends.execution import dragon as dragon_module
from rhapsody.backends.execution.dragon import DragonExecutionBackend
from rhapsody.backends.execution.dragon import _physical_cores

LOGGER = "rhapsody.backends.execution.dragon"


class FakeNode:
    """Stands in for dragon.native.machine.Node: CPU count and affinity per host."""

    hosts: dict = {}
    created: list = []

    def __init__(self, h_uid):
        self.hostname = h_uid
        self.num_cpus, self.cpus = self.hosts[h_uid]
        self.created.append(h_uid)


class FakeSystem:
    @property
    def nodes(self):
        return list(FakeNode.hosts)


class FakeBatch:
    """Sizes its pools and placement capacity the way dragonhpc 0.15.0's Batch does."""

    def __init__(self, num_nodes=None, task_logs=False, *, workers_per_node=None, **kwargs):
        self.kwargs = {"num_nodes": num_nodes, "workers_per_node": workers_per_node, **kwargs}
        hosts = list(FakeNode.hosts)[: num_nodes or len(FakeNode.hosts)]
        cpus_per_node = max(1, FakeNode.hosts[hosts[0]][0] // 2)
        if workers_per_node is None:
            workers_per_node = cpus_per_node
        elif not 1 <= workers_per_node <= cpus_per_node:
            raise ValueError(f"workers_per_node must be between 1 and {cpus_per_node}")
        self.num_workers = workers_per_node
        self.physical_cores_per_node = cpus_per_node  # placement capacity: still the whole node
        self.num_managers = len(hosts) + 1


class IgnoringBatch(FakeBatch):
    """A Batch that drops workers_per_node, as a future Dragon change might."""

    def __init__(self, **kwargs):
        kwargs.pop("workers_per_node", None)
        super().__init__(**kwargs)


def smt_pairs(n):
    """CPU ids of n cores with two hardware threads each, numbered like Linux (i, i + 256)."""
    return [c for core in range(n) for c in (core, core + 256)]


@pytest.fixture
def fake_dragon():
    """Install the fakes; count two hardware threads per core, as on rust."""
    FakeNode.created = []
    with (
        patch.multiple(dragon_module, Batch=FakeBatch, Node=FakeNode, System=FakeSystem),
        patch.object(dragon_module, "_physical_cores", lambda cpus: max(1, len(cpus) // 2)),
    ):
        yield FakeNode
    FakeNode.hosts = {}


def test_partial_node_gets_one_worker_per_allowed_core(fake_dragon, caplog):
    """4 CPUs (2 cores) of 512: 2 workers, not 256, and placement capacity unchanged."""
    fake_dragon.hosts = {"julia": (512, smt_pairs(2))}

    with caplog.at_level(logging.WARNING, logger=LOGGER):
        backend = DragonExecutionBackend()

    assert backend.batch.kwargs["workers_per_node"] == 2
    assert backend.batch.num_workers == 2
    assert backend.batch.physical_cores_per_node == 256
    assert "julia 2" in caplog.text
    assert "starting 2 workers per node instead of 256" in caplog.text
    assert "as if the job had whole nodes" in caplog.text


def test_whole_node_is_left_alone(fake_dragon, caplog):
    fake_dragon.hosts = {"julia": (512, list(range(512)))}

    with caplog.at_level(logging.WARNING, logger=LOGGER):
        backend = DragonExecutionBackend()

    assert backend.batch.kwargs["workers_per_node"] is None
    assert backend.batch.num_workers == 256
    assert "part of some nodes" not in caplog.text


def test_user_workers_per_node_wins(fake_dragon, caplog):
    fake_dragon.hosts = {"julia": (512, smt_pairs(2))}

    with caplog.at_level(logging.WARNING, logger=LOGGER):
        backend = DragonExecutionBackend(batch_kwargs={"workers_per_node": 8})

    assert backend.batch.num_workers == 8
    assert "part of some nodes" not in caplog.text


def test_opt_out_keeps_batch_sizing_and_warns(fake_dragon, caplog):
    fake_dragon.hosts = {"julia": (512, smt_pairs(2))}

    with caplog.at_level(logging.WARNING, logger=LOGGER):
        backend = DragonExecutionBackend(size_pool_to_affinity=False)

    assert backend.batch.kwargs["workers_per_node"] is None
    assert backend.batch.num_workers == 256
    assert "will start 256 workers per node, oversubscribing them" in caplog.text


def test_smallest_node_sets_the_size_for_all(fake_dragon, caplog):
    """Batch uses one worker count for every node, so the smallest allowance wins."""
    fake_dragon.hosts = {
        "haskell": (512, smt_pairs(8)),
        "julia": (512, list(range(512))),
        "fortran": (512, smt_pairs(2)),
    }

    with caplog.at_level(logging.WARNING, logger=LOGGER):
        backend = DragonExecutionBackend()

    assert backend.batch.num_workers == 2
    assert "haskell 8, fortran 2" in caplog.text
    assert "julia" not in caplog.text


def test_probe_reads_only_the_nodes_batch_uses(fake_dragon):
    fake_dragon.hosts = {
        "haskell": (512, smt_pairs(8)),
        "julia": (512, smt_pairs(4)),
        "fortran": (512, smt_pairs(1)),
    }

    backend = DragonExecutionBackend(batch_kwargs={"num_nodes": 2})

    assert backend.batch.num_workers == 4
    assert "fortran" not in fake_dragon.created


def test_failed_probe_falls_back_to_batch_sizing(fake_dragon, caplog):
    class BrokenSystem:
        @property
        def nodes(self):
            raise RuntimeError("global services unreachable")

    fake_dragon.hosts = {"julia": (512, smt_pairs(2))}
    with (
        patch.object(dragon_module, "System", BrokenSystem),
        caplog.at_level(logging.WARNING, logger=LOGGER),
    ):
        backend = DragonExecutionBackend()

    assert backend.batch.kwargs["workers_per_node"] is None
    assert backend.batch.num_workers == 256
    assert caplog.text == ""


def test_warns_when_batch_ignores_the_size(fake_dragon, caplog):
    fake_dragon.hosts = {"julia": (512, smt_pairs(2))}

    with (
        patch.object(dragon_module, "Batch", IgnoringBatch),
        caplog.at_level(logging.WARNING, logger=LOGGER),
    ):
        DragonExecutionBackend()

    assert "Asked Batch for 2 workers per node but it reports 256" in caplog.text


def test_one_thread_per_core_is_not_halved(fake_dragon):
    """Frontier-style: 56 cores with one thread each of 128 CPUs gives 56 workers, not 28."""
    fake_dragon.hosts = {"frontier": (128, list(range(1, 57)))}

    with patch.object(dragon_module, "_physical_cores", len):
        backend = DragonExecutionBackend()

    assert backend.batch.num_workers == 56


# ============================================================================
# _physical_cores against a fake sysfs
# ============================================================================


def _sysfs(tmp_path, cores):
    """Write topology files: ``cores`` maps a CPU id to its (package, core) pair."""
    for cpu, (package, core) in cores.items():
        topology = tmp_path / f"cpu{cpu}" / "topology"
        topology.mkdir(parents=True)
        (topology / "physical_package_id").write_text(f"{package}\n")
        (topology / "core_id").write_text(f"{core}\n")
    return str(tmp_path)


def test_physical_cores_counts_sibling_threads_once(tmp_path):
    root = _sysfs(tmp_path, {0: (0, 0), 256: (0, 0), 1: (0, 1), 257: (0, 1)})
    with patch.object(dragon_module, "_SYSFS_CPU", root):
        assert _physical_cores([0, 256, 1, 257]) == 2


def test_physical_cores_counts_one_thread_per_core(tmp_path):
    root = _sysfs(tmp_path, {1: (0, 1), 2: (0, 2), 3: (0, 3)})
    with patch.object(dragon_module, "_SYSFS_CPU", root):
        assert _physical_cores([1, 2, 3]) == 3


def test_physical_cores_tells_packages_apart(tmp_path):
    root = _sysfs(tmp_path, {0: (0, 0), 64: (1, 0)})
    with patch.object(dragon_module, "_SYSFS_CPU", root):
        assert _physical_cores([0, 64]) == 2


def test_physical_cores_without_sysfs_assumes_two_threads(tmp_path):
    with patch.object(dragon_module, "_SYSFS_CPU", str(tmp_path / "missing")):
        assert _physical_cores([0, 1, 2, 3]) == 2
        assert _physical_cores([0]) == 1
