"""DragonExecutionBackend sizes Batch's worker pools from the CPUs Dragon may use.

Batch sizes each pool as ``Node.num_cpus // 2``, where ``Node.num_cpus`` is the node's
``os.cpu_count()``. On a Slurm job that holds part of a node, Dragon may run only on
``Node.cpus``, so the default pool oversubscribes those CPUs. These tests swap Batch, Node and
System for fakes that reproduce that sizing, so they also run where Dragon is absent (the main
Dragon test module skips there).
"""

import logging
from unittest.mock import patch

import pytest

from rhapsody.backends.execution import dragon as dragon_module
from rhapsody.backends.execution.dragon import DragonExecutionBackend

LOGGER = "rhapsody.backends.execution.dragon"


class FakeNode:
    """Stands in for dragon.native.machine.Node: CPU count and affinity per host."""

    hosts: dict = {}

    def __init__(self, h_uid):
        self.hostname = h_uid
        self._num_cpus, self._cpus = self.hosts[h_uid]

    @property
    def num_cpus(self):
        return self._num_cpus

    @property
    def cpus(self):
        return self._cpus


class FakeSystem:
    @property
    def nodes(self):
        return list(FakeNode.hosts)


class FakeBatch:
    """Sizes its pool the way dragon.workflows.batch.Batch 0.14.1 does."""

    def __init__(self, **kwargs):
        first = FakeNode(next(iter(FakeNode.hosts)))
        self.num_workers = max(1, first.num_cpus // 2)
        self.num_managers = 2


@pytest.fixture
def fake_dragon():
    """Install the fakes; restore the real Node property afterwards."""
    original = FakeNode.__dict__["num_cpus"]
    with patch.multiple(dragon_module, Batch=FakeBatch, Node=FakeNode, System=FakeSystem):
        yield FakeNode
    FakeNode.num_cpus = original
    FakeNode.hosts = {}


def test_partial_node_pool_sized_from_allowed_cpus(fake_dragon, caplog):
    """4 allowed CPUs of 512: 2 workers, not 256, and a warning naming the node."""
    fake_dragon.hosts = {"haskell": (512, [0, 128, 256, 384])}

    with caplog.at_level(logging.WARNING, logger=LOGGER):
        backend = DragonExecutionBackend()

    assert backend.batch.num_workers == 2
    assert "haskell 4/512" in caplog.text
    assert "sizing the Batch worker pools from the allowed CPUs" in caplog.text


def test_node_property_restored_after_batch(fake_dragon):
    """The cap applies only while Batch() is built."""
    fake_dragon.hosts = {"haskell": (512, list(range(4)))}
    DragonExecutionBackend()

    assert FakeNode("haskell").num_cpus == 512


def test_node_property_restored_when_batch_fails(fake_dragon):
    fake_dragon.hosts = {"haskell": (512, list(range(4)))}

    class FailingBatch:
        def __init__(self, **kwargs):
            raise RuntimeError("batch failed")

    with patch.object(dragon_module, "Batch", FailingBatch):
        with pytest.raises(RuntimeError, match="batch failed"):
            DragonExecutionBackend()

    assert FakeNode("haskell").num_cpus == 512


def test_whole_node_unchanged_and_silent(fake_dragon, caplog):
    """When Dragon may use every CPU, Batch's own sizing stands and nothing is logged."""
    fake_dragon.hosts = {"haskell": (512, list(range(512)))}

    with caplog.at_level(logging.WARNING, logger=LOGGER):
        backend = DragonExecutionBackend()

    assert backend.batch.num_workers == 256
    assert caplog.text == ""


def test_opt_out_keeps_batch_sizing_and_warns(fake_dragon, caplog):
    fake_dragon.hosts = {"haskell": (512, list(range(4)))}

    with caplog.at_level(logging.WARNING, logger=LOGGER):
        backend = DragonExecutionBackend(size_pool_to_affinity=False)

    assert backend.batch.num_workers == 256
    assert "will oversubscribe the allowed CPUs" in caplog.text
    assert "size_pool_to_affinity=True" in caplog.text


def test_only_restricted_nodes_reported(fake_dragon, caplog):
    fake_dragon.hosts = {"haskell": (512, list(range(128))), "julia": (512, list(range(512)))}

    with caplog.at_level(logging.WARNING, logger=LOGGER):
        backend = DragonExecutionBackend()

    assert backend.batch.num_workers == 64
    assert "haskell 128/512" in caplog.text
    assert "julia" not in caplog.text


def test_failed_probe_does_not_block_batch(fake_dragon, caplog):
    class BrokenSystem:
        @property
        def nodes(self):
            raise RuntimeError("no runtime")

    fake_dragon.hosts = {"haskell": (512, list(range(4)))}
    with patch.object(dragon_module, "System", BrokenSystem):
        with caplog.at_level(logging.WARNING, logger=LOGGER):
            backend = DragonExecutionBackend()

    assert backend.batch.num_workers == 256
    assert caplog.text == ""


def test_unknown_affinity_keeps_batch_sizing(fake_dragon, caplog):
    """An empty affinity list means Dragon could not tell; do not shrink the pool to nothing."""
    fake_dragon.hosts = {"haskell": (512, [])}

    with caplog.at_level(logging.WARNING, logger=LOGGER):
        backend = DragonExecutionBackend()

    assert backend.batch.num_workers == 256
    assert caplog.text == ""
