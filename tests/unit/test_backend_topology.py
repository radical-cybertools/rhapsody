"""Unit tests for backend topology() reporting (ResourceLayout source)."""

import logging
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

from rhapsody.backends.execution.concurrent import ConcurrentExecutionBackend
from rhapsody.backends.execution.dragon import DragonExecutionBackend


def test_dragon_topology_from_batch_topology():
    """One node entry per pool hostname, cores = the pool's workers per node."""
    batch_topology = SimpleNamespace(
        pool_hostnames=[["nid001", "nid002"], ["nid003"]],
        workers_per_pool=[64, 16],
    )
    backend = SimpleNamespace(
        batch=SimpleNamespace(topology=lambda: batch_topology),
        logger=logging.getLogger("test"),
        _probe_local_resources=lambda: (99, 4),
    )

    nodes = DragonExecutionBackend.topology(backend)

    assert nodes == [
        {"id": "nid001", "cores": 32, "gpus": 4},
        {"id": "nid002", "cores": 32, "gpus": 4},
        {"id": "nid003", "cores": 16, "gpus": 4},
    ]


def test_dragon_topology_unknown_when_batch_topology_fails():
    def _fail():
        raise RuntimeError("no topology")

    backend = SimpleNamespace(
        batch=SimpleNamespace(topology=_fail),
        logger=logging.getLogger("test"),
        _probe_local_resources=lambda: (8, 0),
    )

    assert DragonExecutionBackend.topology(backend) == []


def test_concurrent_topology_capped_at_executor_workers():
    with ThreadPoolExecutor(max_workers=2) as executor:
        backend = SimpleNamespace(
            executor=executor,
            _probe_local_resources=lambda: (128, 0),
        )
        (node,) = ConcurrentExecutionBackend.topology(backend)

    assert node["cores"] == 2
    assert node["gpus"] == 0
