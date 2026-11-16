"""Group "ai-response-queues": the response queues of the vLLM backend on a real GPU allocation.

`DragonVllmInferenceBackend` reads ordinary batches from one Dragon response queue; a batch that
timed out gets a new queue for the next batch, and the old one is drained and closed in the
background. So the late answers of a timed-out batch cannot be read by the next batch and cannot fill
a queue until a worker blocks (which could hang `shutdown()`). These tests run that on real Dragon
workers and a real model.

Tests (they share one backend and run in this order; the last one shuts it down):
1. `test_normal_batches_use_one_queue`: a scale run of ordinary batches; one queue serves them all.
2. `test_late_answers_do_not_reach_the_next_batch`: a batch times out, the next one must get only
   its own answers.
3. `test_timeouts_at_scale_leave_no_late_queue_open`: rounds of timed-out batches, far more late
   answers than one 100-slot Dragon queue holds, each round drained and checked; no late queue stays open.
4. `test_shutdown_after_timeouts_returns`: the case that hung before: timed-out batches whose late
   answers are still unread, then `shutdown()`. If it hangs, the group's budget ends the run and the
   results of the whole file are lost; run it alone with `-k shutdown` when investigating.

Needs GPUs and a model. Every prompt asks for one marker word, so an answer that belongs to another
batch is recognised by its word.

Run:
    RHAPSODY_HPC_MODEL=/path/to/model RHAPSODY_HPC=1 dragon python3 -m pytest dragon/test_ai_response_queues.py -v -s

Environment: RHAPSODY_HPC_AI_SCALE_PROMPTS (prompts of the throughput run, default 4096),
RHAPSODY_HPC_AI_TIMEOUT_S (batch deadline of the timeout runs in seconds, default 0.05),
RHAPSODY_HPC_AI_ROUNDS (rounds of the timeout scale test, default 5).
"""

import asyncio
import json
import os
import resource
import time

import pytest

SCALE_PROMPTS = int(os.environ.get("RHAPSODY_HPC_AI_SCALE_PROMPTS", "4096"))
TIMEOUT_S = float(os.environ.get("RHAPSODY_HPC_AI_TIMEOUT_S", "0.05"))
ROUNDS = int(os.environ.get("RHAPSODY_HPC_AI_ROUNDS", "5"))

BURST_BATCHES = 6  # timed-out batches per round
BATCH_PROMPTS = 56  # prompts per timed-out batch (one request: Dragon gives it to one worker)
DRAIN_WAIT_S = 240  # time allowed for the workers to finish the late batches
SHUTDOWN_LIMIT_S = 90  # shutdown() must return well inside the backend's own 120 s bound

LATE = "ALPHA"  # the word asked for by prompts that time out
NORMAL = "OMEGA"  # the word asked for by prompts whose answers we read

pytestmark = pytest.mark.gpu

REPORT: dict = {}


def _prompts(word, count, tag=""):
    """Prompts asking for one word; numbered, so no two are equal."""
    return [f"Reply with only the word {word}. ({tag}{i})" for i in range(count)]


def _is_error(answer):
    return not isinstance(answer, str) or answer.startswith("ERROR")


def _queues(backend):
    return backend._response_queues


def _rss_mb():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  # KB on Linux


async def _timed_out_batch(backend, count=BATCH_PROMPTS, tag=""):
    """One batch of ``count`` LATE prompts that times out; returns its answers (all ERROR)."""
    import rhapsody.backends.ai.vllm as vllm_module

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(vllm_module, "_BATCH_RESPONSE_TIMEOUT_S", TIMEOUT_S)
        return await backend.generate(_prompts(LATE, count, tag), 600)


async def _until_drained(backend):
    """Wait until every late queue got its late answers (the workers finished)."""
    began = time.monotonic()
    while _queues(backend).late_queue_count:
        if time.monotonic() - began > DRAIN_WAIT_S:
            pytest.fail(
                f"{_queues(backend).late_queue_count} response queues still wait for late answers "
                f"after {DRAIN_WAIT_S} s"
            )
        await asyncio.sleep(0.5)
    return time.monotonic() - began


async def _normal_batch(backend, count, tag=""):
    """One batch of NORMAL prompts with the normal deadline; returns its answers."""
    return await backend.generate(_prompts(NORMAL, count, tag), 600)


def _assert_only_own_answers(answers, label):
    """No answer carries the word of the timed-out prompts, and the model really did follow the
    instruction (otherwise this check could not notice a mix-up)."""
    errors = [a for a in answers if _is_error(a)]
    assert not errors, f"{label}: {len(errors)} answers are errors, e.g. {errors[0]!r}"
    foreign = [a for a in answers if LATE.lower() in a.lower()]
    assert not foreign, (
        f"{label}: {len(foreign)} of {len(answers)} answers belong to a timed-out batch "
        f"(late answers were read by the next batch), e.g. {foreign[0]!r}"
    )
    own = sum(NORMAL.lower() in a.lower() for a in answers)
    assert own >= len(answers) // 2, (
        f"{label}: only {own} of {len(answers)} answers contain {NORMAL!r}; the model does not "
        "follow the one-word instruction, so this test cannot detect a mix-up (use another model)"
    )


# ---------------------------------------------------------------------------------------------
# 1. Scale: ordinary batches
# ---------------------------------------------------------------------------------------------


async def test_normal_batches_use_one_queue(inference_backend):
    """Many batches without a timeout: one response queue serves all of them."""
    backend = inference_backend
    requests = SCALE_PROMPTS // 4
    began = time.perf_counter()
    results = await asyncio.gather(
        *(backend.generate(_prompts(NORMAL, 4, f"r{r}-"), 600) for r in range(requests))
    )
    elapsed = time.perf_counter() - began
    answers = [a for result in results for a in result]

    errors = [a for a in answers if _is_error(a)]
    assert len(answers) == requests * 4
    assert not errors, f"{len(errors)} of {len(answers)} answers are errors, e.g. {errors[0]!r}"
    queues = _queues(backend)
    assert queues.created_queue_count == 1, f"{queues.created_queue_count} response queues for ordinary batches"
    assert queues.late_queue_count == 0
    REPORT["throughput"] = {
        "prompts": len(answers),
        "requests": requests,
        "seconds": round(elapsed, 2),
        "prompts_per_s": round(len(answers) / elapsed, 1),
        "response_queues": queues.created_queue_count,
    }
    print(f"\nthroughput: {REPORT['throughput']}")


# ---------------------------------------------------------------------------------------------
# 2. Correctness: the next batch gets only its own answers
# ---------------------------------------------------------------------------------------------


async def test_late_answers_do_not_reach_the_next_batch(inference_backend):
    """A batch times out; the very next batch (normal deadline) must read only its own answers."""
    backend = inference_backend
    late = await _timed_out_batch(backend, tag="solo-")
    assert all(_is_error(a) for a in late), (
        f"the batch did not time out with a deadline of {TIMEOUT_S} s; lower RHAPSODY_HPC_AI_TIMEOUT_S"
    )
    assert _queues(backend).late_queue_count >= 1  # its answers are still missing

    answers = await _normal_batch(backend, 32, tag="after-")  # sent while the late answers come in

    _assert_only_own_answers(answers, "the batch after a timeout")
    await _until_drained(backend)


# ---------------------------------------------------------------------------------------------
# 3. Scale: many timed-out batches
# ---------------------------------------------------------------------------------------------


async def test_timeouts_at_scale_leave_no_late_queue_open(inference_backend):
    """ROUNDS rounds of BURST_BATCHES timed-out batches (each round holds more late answers than a
    100-slot Dragon queue); every round drains, the next batch is still right, all late queues are closed."""
    backend = inference_backend
    queues = _queues(backend)
    rss_start = _rss_mb()
    rounds = []
    for n in range(ROUNDS):
        for b in range(BURST_BATCHES):
            late = await _timed_out_batch(backend, tag=f"{n}.{b}-")
            assert all(_is_error(a) for a in late), f"round {n}: batch {b} did not time out"
        peak = queues.late_queue_count  # queues that wait for late answers
        late_answers = BURST_BATCHES * BATCH_PROMPTS
        drained_in = await _until_drained(backend)

        answers = await _normal_batch(backend, 32, tag=f"round{n}-")
        _assert_only_own_answers(answers, f"round {n}")
        rounds.append(
            {
                "round": n,
                "late_answers": late_answers,
                "late_queues_peak": peak,
                "drained_in_s": round(drained_in, 1),
                "response_queues_total": queues.created_queue_count,
            }
        )
        print(f"\nround {n}: {rounds[-1]}")

    assert queues.late_queue_count == 0, "late response queues are left open after every drain"
    REPORT["timeouts_at_scale"] = {
        "timed_out_batches": ROUNDS * BURST_BATCHES,
        "late_answers": ROUNDS * BURST_BATCHES * BATCH_PROMPTS,
        "response_queues": queues.created_queue_count,
        "rss_mb_before": round(rss_start),
        "rss_mb_after": round(_rss_mb()),
        "rounds": rounds,
    }


# ---------------------------------------------------------------------------------------------
# 4. The shutdown hang
# ---------------------------------------------------------------------------------------------


async def test_shutdown_after_timeouts_returns(inference_backend, artifact_dir, log_capture):
    """Timed-out batches with their late answers unread, then shutdown(): it must return.

    With one shared 100-slot response queue the late answers could fill it, block a worker, and
    leave Inference.destroy() waiting for that worker (seen once, cause not proven).
    """
    backend = inference_backend
    for b in range(4):  # 4 x 56 = 224 late answers, more than one queue holds
        late = await _timed_out_batch(backend, tag=f"end{b}-")
        assert all(_is_error(a) for a in late), f"batch {b} did not time out"
    assert _queues(backend).late_queue_count >= 1  # the workers are still busy with them

    began = time.perf_counter()
    await backend.shutdown()
    elapsed = time.perf_counter() - began

    stuck = [r.getMessage() for r in log_capture.records if "did not return within" in r.getMessage()]
    REPORT["shutdown"] = {"seconds": round(elapsed, 1), "destroy_timed_out": bool(stuck)}
    (artifact_dir / "response_queues_report.json").write_text(json.dumps(REPORT, indent=2))
    print(f"\nreport: {json.dumps(REPORT, indent=2)}")

    assert not stuck, f"destroy() hit its time limit: {stuck[0]}"
    assert elapsed < SHUTDOWN_LIMIT_S, f"shutdown took {elapsed:.0f} s"
    assert not backend.is_initialized
    assert _queues(backend).late_queue_count == 0
