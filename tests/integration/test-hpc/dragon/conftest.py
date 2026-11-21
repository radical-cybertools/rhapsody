"""Shared fixtures and markers for the RHAPSODY + Dragon HPC test suite.

Run with:
    python3 ../run_suite.py --backend dragon --junit junit.xml

All topology detection happens here.  Tests never hardcode node counts,
GPU counts, or hostnames — they skip automatically when the required
resources are not present in the current allocation.
"""

import os
from pathlib import Path

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
        gpu_ids = list(node.gpus or [])  # None on nodes without GPUs
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


# ---------------------------------------------------------------------------
# AI telemetry fixtures (the vLLM backend needs a GPU and a model)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def ai_model():
    """Model name or local snapshot path from RHAPSODY_HPC_MODEL; skips (or fails if required)."""
    model = os.environ.get("RHAPSODY_HPC_MODEL")
    if not model:
        _unavailable("model", "set RHAPSODY_HPC_MODEL to a model name or a local snapshot path")
    return model


@pytest.fixture(scope="module")
def artifact_dir(request):
    """Folder for this module's telemetry files and report (RHAPSODY_HPC_ARTIFACTS, default ./hpc-
    artifacts)."""
    base = Path(os.environ.get("RHAPSODY_HPC_ARTIFACTS", "hpc-artifacts")).resolve()
    out = base / request.module.__name__
    out.mkdir(parents=True, exist_ok=True)
    return out


@pytest.fixture(scope="module")
def log_capture():
    """Collect the WARNING and ERROR records of the run, so a failing hook or instrumentation shows.

    Attached to the "rhapsody" logger before the backend is created, because Dragon may reset the
    root handlers.
    """
    import logging

    from telemetry_checks import LogCapture

    handler = LogCapture()
    logging.getLogger("rhapsody").addHandler(handler)
    yield handler
    logging.getLogger("rhapsody").removeHandler(handler)


@pytest.fixture(scope="module")
async def inference_backend(ai_model, topology, gpu_nodes, log_capture):
    """A warm DragonVllmInferenceBackend (engine mode, no HTTP server) on all GPU nodes."""
    import multiprocessing as mp

    from telemetry_checks import AI_BATCH_SIZE
    from telemetry_checks import AI_WARMUP_REQUESTS
    from telemetry_checks import warm_up

    if not gpu_nodes:
        _unavailable("gpu", "the inference backend needs at least one GPU-equipped node")

    from rhapsody.backends import DragonVllmInferenceBackend
    from rhapsody.backends.ai.config import HardwareConfig
    from rhapsody.backends.ai.config import ModelConfig

    if mp.get_start_method(allow_none=True) != "dragon":
        mp.set_start_method("dragon", force=True)

    backend = await DragonVllmInferenceBackend(
        model=ModelConfig(
            model_name=ai_model,
            tp_size=1,
            gpu_memory_utilization=0.85,
            hf_token=os.environ.get("HF_TOKEN", ""),
        ),
        hardware=HardwareConfig(
            num_nodes=len(gpu_nodes),
            num_gpus=min(n["num_gpus"] for n in gpu_nodes),  # per node
            node_offset=0,
        ),
        max_batch_size=AI_BATCH_SIZE,
        max_batch_wait_ms=500,
        use_service=False,  # no HTTP server, so the request counts stay exact
    )
    try:
        # Before telemetry: the backend drops events until a manager is attached, and the first
        # call to each worker is slow.
        await warm_up(backend, AI_WARMUP_REQUESTS)
        yield backend
    finally:
        await backend.shutdown()
