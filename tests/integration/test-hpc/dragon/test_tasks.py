"""Group "tasks": functions and executables run and return correct results.

Function tasks without a process_template use batch.function(), which captures the Python
return value. Executables are checked through stdout.

Run:
    dragon python3 -m pytest dragon/test_tasks.py -v
"""

import pytest
from hpc_workers import add as _add

TASKS_PER_NODE = 8


@pytest.mark.asyncio
async def test_function_return_value(rhapsody_session):
    """batch.function() correctly captures the Python return value."""
    from rhapsody.api import ComputeTask

    task = ComputeTask(function=_add, args=(21, 21))

    await rhapsody_session.submit_tasks([task])
    results = await rhapsody_session.wait_tasks([task], timeout=60.0)

    assert results[0]["state"] == "DONE"
    assert results[0]["return_value"] == 42


@pytest.mark.asyncio
async def test_multiple_function_tasks_return_correct_values(rhapsody_session):
    """Multiple concurrent function tasks all return correct independent values."""
    from rhapsody.api import ComputeTask

    pairs = [(i, i * 2) for i in range(10)]
    tasks = [ComputeTask(function=_add, args=(a, b)) for a, b in pairs]

    await rhapsody_session.submit_tasks(tasks)
    results = await rhapsody_session.wait_tasks(tasks, timeout=60.0)

    for task, (a, b) in zip(results, pairs):
        assert task["state"] == "DONE"
        assert task["return_value"] == a + b, (
            f"_add({a}, {b}) returned {task['return_value']!r}, expected {a + b}"
        )


@pytest.mark.asyncio
async def test_executable_echo_on_first_node(rhapsody_session, topology):
    """/bin/echo with arguments runs on the first node and stdout is captured."""
    from dragon.infrastructure.policy import Policy

    from rhapsody.api import ComputeTask

    node = topology[0]
    task = ComputeTask(
        executable="/bin/echo",
        arguments=["hello-rhapsody"],
        task_backend_specific_kwargs={
            "process_template": {
                "policy": Policy(
                    placement=Policy.Placement.HOST_NAME,
                    host_name=node["hostname"],
                )
            }
        },
    )

    await rhapsody_session.submit_tasks([task])
    results = await rhapsody_session.wait_tasks([task], timeout=60.0)

    assert results[0]["state"] == "DONE"
    assert "hello-rhapsody" in (results[0].get("stdout") or "")


@pytest.mark.asyncio
async def test_concurrent_submission_all_nodes(rhapsody_session, topology):
    """Several tasks per node in one call; all complete and report the right hostname."""
    from dragon.infrastructure.policy import Policy

    from rhapsody.api import ComputeTask

    expected = [node["hostname"] for node in topology for _ in range(TASKS_PER_NODE)]
    tasks = [
        ComputeTask(
            executable="/bin/hostname",
            task_backend_specific_kwargs={
                "process_template": {
                    "policy": Policy(
                        placement=Policy.Placement.HOST_NAME,
                        host_name=hostname,
                    )
                }
            },
        )
        for hostname in expected
    ]

    await rhapsody_session.submit_tasks(tasks)
    await rhapsody_session.wait_tasks(tasks, timeout=120.0)

    for task, hostname in zip(tasks, expected):
        assert task["state"] == "DONE"
        stdout = (task.get("stdout") or "").strip()
        assert hostname in stdout, f"Expected {hostname!r} in stdout, got {stdout!r}"
