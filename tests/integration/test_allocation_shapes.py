"""Backend behaviour across allocation shapes: whole node, part of a node, one CPU.

RHAPSODY runs on whole HPC nodes and on slices of shared nodes, where the job may use fewer
CPUs than ``os.cpu_count()`` reports. CI runners always hold the whole machine, so code that
reads the node size instead of the job's CPUs passes CI and fails on a cluster (#95, and the
placement hang in #106). These tests check three properties that should hold for any shape:

1. Liveness: every kind of task finishes within a timeout. That covers function tasks, single
   executables, single processes, and multi-rank jobs as large as the job's CPUs and Dragon's
   placement capacity allow.
2. Bounded resources: the processes and threads the backend adds grow with the CPUs the job
   may use, not with the node size.
3. No leaked state: starting and closing a backend leaves process-wide state as it was, apart
   from changes listed in ``KNOWN_DRAGON_CHANGES``.

Run them under Dragon, once per shape. ``taskset`` gives the job part of the machine, and
``dragon -s`` inherits that CPU set::

    RHAPSODY_TEST_MODE=dragon dragon python -m pytest tests/integration/test_allocation_shapes.py
    RHAPSODY_TEST_MODE=dragon taskset -c 0-1 dragon python -m pytest tests/integration/test_allocation_shapes.py
    RHAPSODY_TEST_MODE=dragon taskset -c 0 dragon python -m pytest tests/integration/test_allocation_shapes.py

``make test-allocation-shapes`` runs all three. Inside a Slurm job, plain ``sbatch -c N`` does
the same as ``taskset``.

Environment:
    RHAPSODY_TEST_MODE: must be ``dragon`` or ``all``; otherwise the module is skipped.
    RHAPSODY_SHAPES_TIMEOUT: seconds each task may take (default 120).
    RHAPSODY_SHAPES_PROC_BASE, RHAPSODY_SHAPES_PROC_PER_CPU: the process bound,
        ``base + per_cpu * allowed CPUs`` (defaults 40 and 1).
    RHAPSODY_SHAPES_THREAD_BASE, RHAPSODY_SHAPES_THREAD_PER_CPU: the thread bound
        (defaults 100 and 2).
    RHAPSODY_SHAPES_STRICT=1: also run a job with one rank per allowed CPU. Dragon's
        placement capacity is ``os.cpu_count() // 2`` per node, so this case hangs on any
        machine where the job holds more than half the node.
"""

import asyncio
import logging
import multiprocessing as mp
import os
import signal
import sys
import threading
import time

import pytest
import pytest_asyncio

if os.environ.get("RHAPSODY_TEST_MODE", "regular").lower() not in ("dragon", "all"):
    pytest.skip(
        "set RHAPSODY_TEST_MODE=dragon and run under the dragon launcher", allow_module_level=True
    )

pytest.importorskip("dragon", reason="Dragon is required for the allocation-shape tests")
psutil = pytest.importorskip("psutil", reason="psutil counts the backend's processes")

from rhapsody import ComputeTask  # noqa: E402
from rhapsody.api import Session  # noqa: E402
from rhapsody.backends.discovery import get_backend  # noqa: E402

TIMEOUT = float(os.environ.get("RHAPSODY_SHAPES_TIMEOUT", "120"))
PROC_BASE = int(os.environ.get("RHAPSODY_SHAPES_PROC_BASE", "40"))
PROC_PER_CPU = float(os.environ.get("RHAPSODY_SHAPES_PROC_PER_CPU", "1"))
THREAD_BASE = int(os.environ.get("RHAPSODY_SHAPES_THREAD_BASE", "100"))
THREAD_PER_CPU = float(os.environ.get("RHAPSODY_SHAPES_THREAD_PER_CPU", "2"))
STRICT = os.environ.get("RHAPSODY_SHAPES_STRICT") == "1"

# Tasks left running after a timeout. Closing a session with one of them can hang in Dragon's
# shutdown (cancel_task doesn't release a job that waits for nodes), so the module then skips the
# close and lets pytest print its report.
HUNG = []

# Process-wide changes that Dragon makes and RHAPSODY can't undo. Each one is reported, not failed.
KNOWN_DRAGON_CHANGES = {
    "root logger handlers": "Dragon replaces the client's root handlers when Batch starts",
}


# ============================================================================
# Helpers
# ============================================================================


def _shape():
    """The CPUs the job may use and the node's CPU count, per Dragon, over all nodes."""
    from dragon.native.machine import Node
    from dragon.native.machine import System

    nodes = [Node(huid) for huid in System().nodes]
    return {
        "nodes": len(nodes),
        "allowed": min(len(node.cpus) for node in nodes),
        "num_cpus": min(node.num_cpus for node in nodes),
    }


def _job_processes():
    """Processes in this process's cgroup: the Slurm job or step, or the CI runner.

    Falls back to every process of this user where cgroups aren't readable.
    """
    try:
        with open("/proc/self/cgroup") as f:
            mine = f.read()
    except OSError:
        mine = None
    uid = os.getuid()
    procs = []
    for proc in psutil.process_iter(["uids"]):
        try:
            if proc.info["uids"] is None or proc.info["uids"].real != uid:
                continue
            if mine is not None:
                with open(f"/proc/{proc.pid}/cgroup") as f:
                    if f.read() != mine:
                        continue
            procs.append(proc)
        except (OSError, psutil.Error):
            continue
    return procs


def _count():
    """Processes and threads in the job right now."""
    procs = threads = 0
    for proc in _job_processes():
        try:
            threads += proc.num_threads()
            procs += 1
        except psutil.Error:
            continue
    return procs, threads


def _snapshot():
    """Process-wide state a backend should leave as it found it."""
    from dragon.native.machine import Node

    root = logging.getLogger()
    rlog = logging.getLogger("rhapsody")
    return {
        "Node.num_cpus": Node.__dict__.get("num_cpus"),
        "root logger handlers": list(root.handlers),
        "root logger level": root.level,
        "rhapsody logger": (list(rlog.handlers), rlog.level, rlog.propagate),
        "multiprocessing start method": mp.get_start_method(allow_none=True),
        "environment": dict(os.environ),
        "cwd": os.getcwd(),
        "sys.path": list(sys.path),
        "SIGINT handler": signal.getsignal(signal.SIGINT),
        "SIGTERM handler": signal.getsignal(signal.SIGTERM),
        "threads": sorted(t.name for t in threading.enumerate()),
    }


def _diff(before, after):
    changes = {}
    for key in before:
        if key == "environment":
            keys = set(before[key]) | set(after[key])
            changed = sorted(k for k in keys if before[key].get(k) != after[key].get(k))
            if changed:
                changes[key] = changed
        elif before[key] != after[key]:
            changes[key] = (before[key], after[key])
    return changes


async def _run(session, tasks, timeout=TIMEOUT):
    """Submit and wait.

    Cancel what's left on timeout, so the session can still close.
    """
    start = time.monotonic()
    await session.submit_tasks(tasks)
    try:
        await session.wait_tasks(tasks, timeout=timeout)
    except asyncio.TimeoutError:
        states = sorted({str(t.get("state")) for t in tasks})
        for backend in session.backends.values():
            for t in tasks:
                if t.get("state") not in ("DONE", "FAILED", "CANCELED"):
                    HUNG.append(t["uid"])
                    await backend.cancel_task(t["uid"])
        pytest.fail(f"{len(tasks)} task(s) not finished after {timeout:.0f} s; states {states}")
    print(f"{len(tasks)} task(s) finished in {time.monotonic() - start:.1f} s")
    return tasks


def _assert_done(tasks):
    failed = [t for t in tasks if t.get("state") != "DONE"]
    for t in failed[:3]:
        print(f"{t['uid']}: {t.get('state')}: {str(t.get('stderr') or t.get('exception'))[-800:]}")
    assert not failed, f"{len(failed)} of {len(tasks)} task(s) not DONE"


def _square_task(x):
    # Defined inside a function so it's pickled by value: Dragon's workers can't import this module.
    def square(x):
        return x * x

    return ComputeTask(function=square, args=(x,))


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture(scope="module")
def shape():
    shape = _shape()
    print(f"\nallocation shape: {shape}")
    return shape


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def session(shape):
    """One Dragon session for the liveness and resource tests, with its startup cost."""
    procs, threads = _count()
    backend = await get_backend("dragon")
    session = Session(backends=[backend])
    session.baseline = (procs, threads)
    yield session
    if HUNG:
        print(f"\nnot closing the session: {len(HUNG)} task(s) timed out; the run may need a kill")
        return
    await asyncio.wait_for(session.close(), timeout=TIMEOUT)


# ============================================================================
# 3. No leaked state (first, so no other backend has started in this process)
# ============================================================================


@pytest.mark.asyncio(loop_scope="module")
async def test_backend_leaves_process_state(shape):
    before = _snapshot()
    backend = await get_backend("dragon")
    session = Session(backends=[backend])
    task = _square_task(3)
    await _run(session, [task])
    await asyncio.wait_for(session.close(), timeout=TIMEOUT)
    after = _snapshot()

    changes = _diff(before, after)
    known = {k: v for k, v in changes.items() if k in KNOWN_DRAGON_CHANGES}
    unknown = {k: v for k, v in changes.items() if k not in KNOWN_DRAGON_CHANGES}
    for key in known:
        print(f"known Dragon change: {key}: {KNOWN_DRAGON_CHANGES[key]}")
    for key, change in unknown.items():
        print(f"changed: {key}: {change}")
    _assert_done([task])
    assert not unknown, f"backend changed process-wide state: {unknown}"


# ============================================================================
# 1. Liveness
# ============================================================================


@pytest.mark.asyncio(loop_scope="module")
async def test_function_tasks_finish(session, shape):
    """Twice as many function tasks as allowed CPUs, so the pool has to queue them."""
    tasks = [_square_task(i) for i in range(2 * shape["allowed"])]
    await _run(session, tasks)
    _assert_done(tasks)
    assert [t["return_value"] for t in tasks] == [i * i for i in range(len(tasks))]


@pytest.mark.asyncio(loop_scope="module")
async def test_executable_finishes(session, shape):
    task = ComputeTask(executable="/bin/sh", arguments=["-c", "echo shape-ok"])
    await _run(session, [task])
    _assert_done([task])


@pytest.mark.asyncio(loop_scope="module")
async def test_single_process_finishes(session, shape):
    task = ComputeTask(
        executable="/bin/sh",
        arguments=["-c", "true"],
        task_backend_specific_kwargs={"process_template": {}},
    )
    await _run(session, [task])
    _assert_done([task])


def _job(ranks):
    return ComputeTask(
        executable="/bin/sh",
        arguments=["-c", "true"],
        task_backend_specific_kwargs={"process_templates": [(ranks, {})]},
    )


@pytest.mark.asyncio(loop_scope="module")
async def test_one_rank_job_finishes(session, shape):
    task = _job(1)
    await _run(session, [task])
    _assert_done([task])


@pytest.mark.asyncio(loop_scope="module")
async def test_largest_placeable_job_finishes(session, shape):
    """A job with as many ranks as both the job's CPUs and Dragon's placement capacity allow.

    Dragon places up to ``os.cpu_count() // 2`` ranks per node. Code that lowers that capacity
    below the job's CPUs (as the ``num_cpus`` override in #106 did) makes this job wait forever
    for nodes the allocation doesn't have.
    """
    ranks = max(1, min(shape["allowed"], shape["num_cpus"] // 2))
    task = _job(ranks)
    await _run(session, [task])
    assert task["state"] == "DONE", f"{ranks}-rank job: {task['state']}"


@pytest.mark.skipif(
    not STRICT, reason="set RHAPSODY_SHAPES_STRICT=1; hangs where the job holds > half the node"
)
@pytest.mark.asyncio(loop_scope="module")
async def test_job_with_one_rank_per_cpu_finishes(session, shape):
    task = _job(shape["allowed"])
    await _run(session, [task], timeout=min(TIMEOUT, 60))
    _assert_done([task])


# ============================================================================
# 2. Bounded resources (after the liveness tests, so the pools have started)
# ============================================================================


@pytest.mark.asyncio(loop_scope="module")
async def test_backend_resources_follow_allowed_cpus(session, shape):
    # Run a round of tasks so every pool is up, then let process startup settle.
    await _run(session, [_square_task(i) for i in range(shape["allowed"])])
    await asyncio.sleep(2)
    procs, threads = _count()
    base_procs, base_threads = session.baseline
    added_procs, added_threads = procs - base_procs, threads - base_threads
    proc_bound = PROC_BASE + PROC_PER_CPU * shape["allowed"]
    thread_bound = THREAD_BASE + THREAD_PER_CPU * shape["allowed"]
    print(
        f"backend added {added_procs} processes (bound {proc_bound:.0f}) and {added_threads} "
        f"threads (bound {thread_bound:.0f}); {shape['allowed']} of {shape['num_cpus']} CPUs allowed"
    )
    assert added_procs <= proc_bound, (
        f"{added_procs} processes for {shape['allowed']} allowed CPUs: is something sized "
        f"from the node ({shape['num_cpus']} CPUs)?"
    )
    assert added_threads <= thread_bound, (
        f"{added_threads} threads for {shape['allowed']} allowed CPUs (node: {shape['num_cpus']})"
    )
