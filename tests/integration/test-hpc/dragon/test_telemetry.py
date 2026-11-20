"""Group "telemetry": the task telemetry of a real Dragon allocation.

One workload runs on the Dragon execution backend with telemetry on: function tasks, an executable
pinned to each node, and one failing executable. Telemetry is stopped once, then the tests check the
events, the task spans, the task counters, the per-node resource updates and the JSONL file. No model
or GPU is needed.

Run:
    RHAPSODY_HPC=1 dragon python3 -m pytest dragon/test_telemetry.py -v
"""

import asyncio
import glob
import json
import os
import time

import pytest
from hpc_workers import add as _add

N_FUNCTIONS = 8
TASKS_PER_NODE = 2
TASK_EVENTS = ("TaskSubmitted", "TaskStarted", "TaskCompleted", "TaskFailed")
RESOURCE_WAIT_S = 60.0


def _short(host):
    return str(host).split(".")[0]


@pytest.fixture(scope="module")
async def run(rhapsody_session, topology, artifact_dir, log_capture):
    """Run the workload with telemetry on and stop telemetry once.

    Returns what the tests read.
    """
    from dragon.infrastructure.policy import Policy

    from rhapsody.api import ComputeTask

    events = []
    telemetry = await rhapsody_session.start_telemetry(
        checkpoint_path=str(artifact_dir), resource_poll_interval=1.0
    )
    telemetry.subscribe(events.append)
    before = telemetry.task_count()  # counters are cumulative: the tests compare deltas

    functions = [ComputeTask(function=_add, args=(i, i)) for i in range(N_FUNCTIONS)]
    pinned = [
        (
            node["hostname"],
            ComputeTask(
                executable="/bin/hostname",
                task_backend_specific_kwargs={
                    "process_template": {
                        "policy": Policy(
                            placement=Policy.Placement.HOST_NAME, host_name=node["hostname"]
                        )
                    }
                },
            ),
        )
        for node in topology
        for _ in range(TASKS_PER_NODE)
    ]
    failing = ComputeTask(executable="/bin/false")
    tasks = [*functions, *(t for _, t in pinned), failing]

    try:
        await rhapsody_session.submit_tasks(tasks)
        await rhapsody_session.wait_tasks(tasks, timeout=120.0)

        # Wait for a resource sample of every node. Never count samples: the adapter can drop
        # later ones from remote nodes, but the first one of each node always arrives.
        wanted = {_short(n["hostname"]) for n in topology}
        deadline = time.monotonic() + RESOURCE_WAIT_S
        while time.monotonic() < deadline:
            seen = {_short(e.node_id) for e in events if e.event_type == "ResourceUpdate"}
            if wanted <= seen:
                break
            await asyncio.sleep(0.5)
    finally:
        await telemetry.stop()  # also when the workload failed: stop the adapter's polling
    files = glob.glob(os.path.join(str(artifact_dir), "*.telemetry.jsonl"))
    return {
        "telemetry": telemetry,
        "events": events,
        "before": before,
        "tasks": tasks,
        "failing": failing,
        "pinned": pinned,
        "jsonl": max(files, key=os.path.getmtime) if files else None,
        "log_records": log_capture.records,
    }


def test_task_lifecycle_events_and_spans(run):
    """Every task has a submitted event and a terminal event, and one task span."""
    events = run["events"]
    spans = {s["task_id"]: s for s in run["telemetry"].task_spans()}
    failing_id = run["failing"]["uid"]

    for task in run["tasks"]:
        uid = task["uid"]
        kinds = {e.event_type for e in events if e.task_id == uid}
        assert "TaskSubmitted" in kinds, f"{uid}: no TaskSubmitted event ({sorted(kinds)})"
        assert "TaskStarted" in kinds, f"{uid}: no TaskStarted event ({sorted(kinds)})"
        expected_end = "TaskFailed" if uid == failing_id else "TaskCompleted"
        assert expected_end in kinds, f"{uid}: no {expected_end} event ({sorted(kinds)})"
        assert uid in spans, f"{uid}: no task span"
        assert spans[uid]["status"] == ("failed" if uid == failing_id else "completed")
        assert not spans[uid].get("incomplete_lifecycle"), f"{uid}: span has no running phase"
    assert spans[failing_id].get("error_type"), "the failed task's span has no error_type"
    assert len(spans) == len(run["tasks"])

    for hostname, task in run["pinned"]:  # the pinned executables ran where they were placed
        assert hostname in (task.get("stdout") or "")


def test_task_metrics_match_the_workload(run):
    """The task counters grew by exactly the tasks of the workload, by state."""
    before, after = run["before"], run["telemetry"].task_count()
    delta = {k: after[k] - before[k] for k in after}
    assert delta["submitted"] == len(run["tasks"])
    assert delta["completed"] == len(run["tasks"]) - 1
    assert delta["failed"] == 1
    assert delta["running"] == 0


def test_resource_updates_cover_every_node(run, topology):
    """A resource sample arrived from every node; GPU rows only come from GPU nodes."""
    updates = [e for e in run["events"] if e.event_type == "ResourceUpdate"]
    seen = {_short(e.node_id) for e in updates}
    wanted = {_short(n["hostname"]) for n in topology}
    problems = [
        r.getMessage() for r in run["log_records"] if r.name.startswith("rhapsody.telemetry")
    ]
    assert wanted <= seen, (
        f"no resource sample from {sorted(wanted - seen)}; seen {sorted(seen)}; "
        f"telemetry log: {problems[:3]}"
    )

    gpus = {_short(n["hostname"]): n["num_gpus"] for n in topology}
    for node, count in gpus.items():
        ids = {e.gpu_id for e in updates if _short(e.node_id) == node and e.gpu_id is not None}
        assert len(ids) <= count, f"{node}: GPU rows for {sorted(ids)} but {count} GPUs"


def test_jsonl_checkpoint_matches_memory(run):
    """The JSONL file holds the same task events as the in-memory log."""
    assert run["jsonl"], "no telemetry checkpoint file was written"
    in_file = dict.fromkeys(TASK_EVENTS, 0)
    with open(run["jsonl"]) as handle:
        for line in handle:
            try:
                kind = json.loads(line).get("event_type")
            except ValueError:
                continue
            if kind in in_file:
                in_file[kind] += 1
    in_memory = {k: sum(e.event_type == k for e in run["events"]) for k in TASK_EVENTS}
    assert in_file == in_memory, f"file {in_file}, memory {in_memory}"
