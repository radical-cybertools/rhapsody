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


class _FakeNVMLError(Exception):
    pass


class _LibraryNotFoundError(_FakeNVMLError):
    pass


def _fake_pynvml(init_error=None, count=2):
    def init():
        if init_error is not None:
            raise init_error

    return SimpleNamespace(
        NVMLError=_FakeNVMLError,
        NVMLError_LibraryNotFound=_LibraryNotFoundError,
        NVMLError_DriverNotLoaded=type("_DriverNotLoaded", (_FakeNVMLError,), {}),
        nvmlInit=init,
        nvmlDeviceGetCount=lambda: count,
        nvmlShutdown=lambda: None,
    )


def test_probe_counts_gpus(monkeypatch):
    import sys

    from rhapsody.backends.base import BaseBackend

    monkeypatch.setitem(sys.modules, "pynvml", _fake_pynvml(count=4))
    assert BaseBackend._probe_local_resources(SimpleNamespace())[1] == 4


def test_probe_no_nvidia_library_means_no_gpus(monkeypatch):
    import sys

    from rhapsody.backends.base import BaseBackend

    monkeypatch.setitem(sys.modules, "pynvml", _fake_pynvml(init_error=_LibraryNotFoundError()))
    assert BaseBackend._probe_local_resources(SimpleNamespace())[1] == 0


def test_probe_other_nvml_failure_raises(monkeypatch):
    import sys

    import pytest

    from rhapsody.backends.base import BaseBackend

    monkeypatch.setitem(sys.modules, "pynvml", _fake_pynvml(init_error=_FakeNVMLError("boom")))
    with pytest.raises(_FakeNVMLError):
        BaseBackend._probe_local_resources(SimpleNamespace())


def test_dask_topology_groups_workers_by_host():
    from rhapsody.backends.execution.dask_parallel import DaskExecutionBackend

    workers = {
        "tcp://a:1": {"host": "nodeA", "nthreads": 4, "resources": {"GPU": 1}},
        "tcp://a:2": {"host": "nodeA", "nthreads": 4, "resources": {"GPU": 1}},
        "tcp://b:1": {"host": "nodeB", "nthreads": 8},
    }
    client = SimpleNamespace(scheduler_info=lambda n_workers=-1: {"workers": workers})
    backend = SimpleNamespace(_client=client)

    assert DaskExecutionBackend.topology(backend) == [
        {"id": "nodeA", "cores": 8, "gpus": 2},
        {"id": "nodeB", "cores": 8, "gpus": 0},
    ]
