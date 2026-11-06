"""Shared fixtures and markers for the RHAPSODY + Dragon HPC test suite.

Run with:
    python3 ../run_suite.py --backend dragon --junit junit.xml

All topology detection happens here.  Tests never hardcode node counts,
GPU counts, or hostnames — they skip automatically when the required
resources are not present in the current allocation.
"""

import os

import pytest

# Capabilities this machine must have, e.g. RHAPSODY_HPC_REQUIRE=gpu,multi_node,mpi.
# A missing capability listed here fails its tests; any other missing capability skips them.
REQUIRED = {name.strip() for name in os.environ.get("RHAPSODY_HPC_REQUIRE", "").split(",")} - {""}


def _unavailable(capability, message):
    if capability in REQUIRED:
        pytest.fail(f"required by RHAPSODY_HPC_REQUIRE: {message}", pytrace=False)
    pytest.skip(message)


# ---------------------------------------------------------------------------
# Topology detection  (session-scoped — runs once per pytest session)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def topology():
    """Detect nodes and GPUs from the live Dragon allocation.

    Returns a list of dicts, one per node:
        [{"huid": int, "hostname": str, "gpus": [int, ...], "num_gpus": int}, ...]

    Raises RuntimeError if Dragon is not initialised.
    """
    from dragon.native.machine import Node
    from dragon.native.machine import System

    nodes = []
    for huid in System().nodes:
        node = Node(huid)
        gpu_ids = list(node.gpus)
        nodes.append(
            {
                "huid": huid,
                "hostname": node.hostname,
                "gpus": gpu_ids,
                "num_gpus": len(gpu_ids),
            }
        )
    assert nodes, "Dragon allocation is empty — no nodes detected"
    return nodes


@pytest.fixture(scope="session")
def num_nodes(topology):
    return len(topology)


@pytest.fixture(scope="session")
def gpu_nodes(topology):
    """Subset of topology entries that have at least one GPU."""
    return [n for n in topology if n["num_gpus"] > 0]


@pytest.fixture(scope="session")
def total_gpus(gpu_nodes):
    return sum(n["num_gpus"] for n in gpu_nodes)


# ---------------------------------------------------------------------------
# Automatic skip logic — applied via autouse fixture
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _check_capabilities(request, topology, gpu_nodes):
    """Skip or fail tests whose resource marks the allocation cannot satisfy."""
    if request.node.get_closest_marker("multi_node") and len(topology) < 2:
        _unavailable("multi_node", f"test needs >= 2 nodes; allocation has {len(topology)}")

    if request.node.get_closest_marker("gpu") and not gpu_nodes:
        _unavailable("gpu", "test needs at least one GPU-equipped node; none detected")

    if request.node.get_closest_marker("mpi"):
        try:
            import mpi4py  # noqa: F401
        except ImportError:
            _unavailable("mpi", "test needs mpi4py; package not available")


# ---------------------------------------------------------------------------
# Backend and session fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def start_backend():
    """Return a coroutine function that starts a DragonExecutionBackend.

    RHAPSODY_DDICT_MEM_GB overrides the per-node results DDict size for every test file.
    """

    async def start(ddict_gb=2, **batch_kwargs):
        from dragon.native.machine import System

        from rhapsody.backends import DragonExecutionBackend

        ddict_gb = int(os.environ.get("RHAPSODY_DDICT_MEM_GB", ddict_gb))
        ddict_mem = ddict_gb * len(list(System().nodes)) * (1024**3)
        return await DragonExecutionBackend(
            batch_kwargs={"results_ddict_mem": ddict_mem, **batch_kwargs}
        )

    return start


@pytest.fixture(scope="module")
async def dragon_backend(start_backend):
    """DragonExecutionBackend shared by the tests of one module."""
    backend = await start_backend()
    yield backend
    await backend.shutdown()


@pytest.fixture(scope="module")
async def rhapsody_session(dragon_backend):
    """RHAPSODY Session wrapping the module-scoped Dragon backend."""
    from rhapsody.api import Session

    session = Session(backends=[dragon_backend])
    yield session
    # Session is closed by the backend shutdown in dragon_backend fixture
