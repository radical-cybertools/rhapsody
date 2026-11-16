"""Group "ai-telemetry": the inference telemetry of the vLLM backend on a real GPU allocation.

One workload runs on `DragonVllmInferenceBackend` with telemetry on: AITasks with list and plain
string prompts, plus direct `generate()` calls. Telemetry is stopped once and checked once
(`telemetry_checks.verify`); the first three tests report the outcome by topic.

The last test sets the batch deadline far below what the model needs, so prompts time out, and
checks that FAILED tasks, ERROR batch spans, the timed-out counter and the batch events agree. It
runs last on purpose: late responses after a timeout stay in the backend's shared response queue
(issue B2) and can contaminate later batches, so nothing may run after it on this backend. If it
hangs, the group's budget ends the run and the results of the whole file are lost: run it alone
with `-k timeouts` when investigating.

Needs GPUs and a model.

Run:
    RHAPSODY_HPC_MODEL=/path/to/model RHAPSODY_HPC=1 dragon python3 -m pytest dragon/test_ai_telemetry.py -v

Environment: RHAPSODY_HPC_AI_TASKS (AITasks in the workload, default 256) and
RHAPSODY_HPC_AI_TIMEOUT_S (batch deadline of the last test in seconds, default 0.05).
"""

import os

import pytest
from telemetry_checks import AI_BATCH_SIZE
from telemetry_checks import AI_TASKS_DEFAULT
from telemetry_checks import run_inference_telemetry

TASKS = int(os.environ.get("RHAPSODY_HPC_AI_TASKS", AI_TASKS_DEFAULT))
TIMEOUT_S = float(os.environ.get("RHAPSODY_HPC_AI_TIMEOUT_S", "0.05"))
TIMEOUT_TASKS = 64

pytestmark = pytest.mark.gpu


def _crashed(report):
    """The failure line when verify() itself raised (every other group is then missing)."""
    return [line for line in report.failures("events") if "the checks ran" in line]


@pytest.fixture(scope="module")
async def report(inference_backend, topology, gpu_nodes, artifact_dir, log_capture):
    result, _facts = await run_inference_telemetry(
        inference_backend,
        artifact_dir,
        TASKS,
        max_batch_size=AI_BATCH_SIZE,
        known_hosts={n["hostname"] for n in topology},
        log_capture=log_capture,
        hardware={
            "nodes": len(gpu_nodes),
            "gpus_per_node": min(n["num_gpus"] for n in gpu_nodes),
        },
    )
    print("\n" + result.table())  # shown with -s and by pytest on failure; also saved as a file
    return result


def test_inference_telemetry_is_consistent(report):
    """Events, spans, task attributes and metrics agree, and Dragon's fields are plausible."""
    assert not _crashed(report), "\n".join(_crashed(report))
    failed = report.failures(exclude=("privacy", "workload"))
    assert not failed, "\n".join(failed)
    groups = {c["group"] for c in report.checks}
    assert {"events", "spans", "task spans", "metrics", "dragon"} <= groups, sorted(groups)
    assert report.stats["batches"] > 0


def test_no_prompt_or_response_text_in_telemetry_files(report):
    """No prompt or response text reached the JSONL or the OTLP file."""
    assert not _crashed(report), "\n".join(_crashed(report))
    assert report.group("privacy"), "the privacy check did not run"
    assert not report.failures("privacy"), "\n".join(report.failures("privacy"))


def test_every_request_was_seen(report):
    """No request raised, and telemetry saw exactly the tasks and generate() calls submitted."""
    assert not _crashed(report), "\n".join(_crashed(report))
    assert len(report.group("workload")) >= 2, "the workload checks did not run"
    assert not report.failures("workload"), "\n".join(report.failures("workload"))


@pytest.fixture(scope="module")
async def timeout_report(inference_backend, topology, gpu_nodes, artifact_dir, log_capture):
    """A second, short run with a tiny batch deadline.

    Created when the last test asks for it,
    after the main tests are done with the backend; its files go to ``timeout/``.
    """
    import rhapsody.backends.ai.vllm as vllm_module

    # The backend fixture has warmed up already, or the deadline would time the warm-up out. The
    # backend reads the value for every batch, so it applies to this run.
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(vllm_module, "_BATCH_RESPONSE_TIMEOUT_S", TIMEOUT_S)
        result, _facts = await run_inference_telemetry(
            inference_backend,
            artifact_dir / "timeout",
            TIMEOUT_TASKS,
            max_batch_size=AI_BATCH_SIZE,
            timeouts=True,
            known_hosts={n["hostname"] for n in topology},
            log_capture=log_capture,
            hardware={
                "nodes": len(gpu_nodes),
                "gpus_per_node": min(n["num_gpus"] for n in gpu_nodes),
            },
        )
    print("\n" + result.table())
    return result


def test_timeouts_are_reported_consistently(timeout_report):
    """Timed-out prompts show up as FAILED tasks, ERROR spans, a counter and batch events.

    Keep this test last in the file (see the module docstring).
    """
    occurred = [c for c in timeout_report.checks if c["name"] == "timeouts: a timeout occurred"]
    assert not _crashed(timeout_report), "\n".join(_crashed(timeout_report))
    assert occurred and occurred[0]["status"] == "PASS", (
        f"no prompt timed out with a deadline of {TIMEOUT_S} s; lower RHAPSODY_HPC_AI_TIMEOUT_S"
    )
    failed = timeout_report.failures()
    assert not failed, "\n".join(failed)
