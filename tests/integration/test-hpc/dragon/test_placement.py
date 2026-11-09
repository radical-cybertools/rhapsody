"""Group "placement": node and GPU pinning are enforced.

These tests use executables, not Python functions, because Dragon's batch.process() returns
the process exit code instead of a Python return value. Placement is verified through stdout.

Run:
    dragon python3 -m pytest dragon/test_placement.py -v
"""

import pytest


def _pinned(executable, hostname, arguments=None, gpus=None):
    from dragon.infrastructure.policy import Policy

    from rhapsody.api import ComputeTask

    policy = {"placement": Policy.Placement.HOST_NAME, "host_name": hostname}
    if gpus is not None:
        policy["gpu_affinity"] = gpus
    return ComputeTask(
        executable=executable,
        arguments=arguments or [],
        task_backend_specific_kwargs={"process_template": {"policy": Policy(**policy)}},
    )


def _echo_gpus(hostname, gpus):
    return _pinned("/bin/bash", hostname, ["-c", "echo $CUDA_VISIBLE_DEVICES"], gpus)


@pytest.mark.asyncio
async def test_one_task_per_node(rhapsody_session, topology):
    """Each node receives one pinned executable; stdout contains that node's hostname."""
    tasks = [_pinned("/bin/hostname", node["hostname"]) for node in topology]

    await rhapsody_session.submit_tasks(tasks)
    results = await rhapsody_session.wait_tasks(tasks, timeout=120.0)

    assert len(results) == len(topology)
    for task, node in zip(results, topology):
        assert task["state"] == "DONE", f"Task on {node['hostname']} did not reach DONE"
        stdout = (task.get("stdout") or "").strip()
        assert node["hostname"] in stdout, (
            f"Expected hostname {node['hostname']!r} in stdout, got {stdout!r}"
        )


@pytest.mark.multi_node
@pytest.mark.asyncio
async def test_exclusive_node_pinning(rhapsody_session, topology):
    """Tasks pinned to node[0] and node[1] must land on different nodes."""
    node_a, node_b = topology[0], topology[1]
    task_a = _pinned("/bin/hostname", node_a["hostname"])
    task_b = _pinned("/bin/hostname", node_b["hostname"])

    await rhapsody_session.submit_tasks([task_a, task_b])
    await rhapsody_session.wait_tasks([task_a, task_b], timeout=60.0)

    stdout_a = (task_a.get("stdout") or "").strip()
    stdout_b = (task_b.get("stdout") or "").strip()

    assert node_a["hostname"] in stdout_a, f"task_a landed on wrong node: {stdout_a!r}"
    assert node_b["hostname"] in stdout_b, f"task_b landed on wrong node: {stdout_b!r}"
    assert stdout_a != stdout_b, "Both tasks reported the same hostname — pinning had no effect"


@pytest.mark.gpu
@pytest.mark.asyncio
async def test_pin_to_specific_gpu(rhapsody_session, gpu_nodes):
    """One task per GPU on the first GPU node; each sees exactly the GPU it was pinned to."""
    node = gpu_nodes[0]
    tasks = [_echo_gpus(node["hostname"], [gpu_id]) for gpu_id in node["gpus"]]

    await rhapsody_session.submit_tasks(tasks)
    await rhapsody_session.wait_tasks(tasks, timeout=120.0)

    for task, gpu_id in zip(tasks, node["gpus"]):
        assert task["state"] == "DONE"
        stdout = (task.get("stdout") or "").strip()
        assert stdout == str(gpu_id), f"Pinned to GPU {gpu_id}, task saw {stdout!r}"


@pytest.mark.multi_node
@pytest.mark.gpu
@pytest.mark.asyncio
async def test_one_task_per_gpu_node(rhapsody_session, gpu_nodes):
    """One task per GPU node with all of its GPUs; each sees that node's full GPU list."""
    tasks = [_echo_gpus(node["hostname"], node["gpus"]) for node in gpu_nodes]

    await rhapsody_session.submit_tasks(tasks)
    results = await rhapsody_session.wait_tasks(tasks, timeout=120.0)

    assert len(results) == len(gpu_nodes)
    for task, node in zip(results, gpu_nodes):
        assert task["state"] == "DONE"
        stdout = (task.get("stdout") or "").strip()
        assert stdout == ",".join(str(gpu) for gpu in node["gpus"]), (
            f"Node {node['hostname']}: expected GPUs {node['gpus']}, task saw {stdout!r}"
        )


@pytest.mark.multi_node
@pytest.mark.gpu
@pytest.mark.asyncio
async def test_round_robin_gpu_across_nodes(rhapsody_session, gpu_nodes):
    """One task per (node, GPU) pair; each reports the expected host and GPU."""
    expected = [(node["hostname"], gpu_id) for node in gpu_nodes for gpu_id in node["gpus"]]
    tasks = [
        _pinned("/bin/bash", host, ["-c", "echo $(hostname):$CUDA_VISIBLE_DEVICES"], [gpu_id])
        for host, gpu_id in expected
    ]

    await rhapsody_session.submit_tasks(tasks)
    await rhapsody_session.wait_tasks(tasks, timeout=180.0)

    for task, (host, gpu_id) in zip(tasks, expected):
        assert task["state"] == "DONE"
        stdout = (task.get("stdout") or "").strip()
        assert ":" in stdout, f"Unexpected stdout format: {stdout!r}"
        seen_host, seen_gpu = stdout.split(":", 1)
        assert host in seen_host, f"Hostname mismatch: expected {host!r}, got {seen_host!r}"
        assert seen_gpu == str(gpu_id), f"Pinned to GPU {gpu_id} on {host}, task saw {seen_gpu!r}"
