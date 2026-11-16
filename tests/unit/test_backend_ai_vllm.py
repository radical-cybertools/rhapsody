"""Unit tests for DragonVllmInferenceBackend (rhapsody.backends.ai.vllm).

These tests cover the config-building/translation layer and the response
unwrapping logic added by the migration to dragon.ai.inference. They do not
require a running Dragon allocation or GPU — Inference/DragonQueue internals
are never invoked; only the constructor and a couple of pure helper functions
are exercised.

Run with:
    dragon python -m pytest tests/unit/test_backend_ai_vllm.py -v
"""

import asyncio
import contextlib
import threading
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

# Skip the entire module when dragon.ai.inference (dragonhpc[ai]) is not installed.
# Bare `dragon` can import fine while `dragon.ai.inference` still fails — its
# dependency closure (vllm, torch, etc.) is only pulled in by the [ai] extra,
# which isn't available on every Python version (e.g. 3.9/3.10 in some envs).
pytest.importorskip(
    "dragon.ai.inference",
    reason="dragon.ai.inference (dragonhpc[ai]) is required for DragonVllmInferenceBackend tests",
)

from fake_dragon import FakePipeline as _FakePipeline
from fake_dragon import model_config as _model_config
from fake_dragon import running_backend as _running_backend

from rhapsody.backends.ai import vllm as vllm_mod
from rhapsody.backends.ai.config import BatchingConfig
from rhapsody.backends.ai.config import DynamicWorkerConfig
from rhapsody.backends.ai.config import GuardrailsConfig
from rhapsody.backends.ai.config import HardwareConfig
from rhapsody.backends.ai.vllm import DragonVllmInferenceBackend
from rhapsody.backends.ai.vllm import ResponseQueues
from rhapsody.backends.ai.vllm import _unwrap_assistant_response

# ============================================================================
# Config translation
# ============================================================================


def test_build_inference_config_passes_objects_through_verbatim():
    """_build_inference_config() forwards the stored config objects unchanged."""
    model = _model_config()
    hardware = HardwareConfig(num_nodes=2, num_gpus=4)
    batching = BatchingConfig(enabled=True, batch_type="pre-batch")
    guardrails = GuardrailsConfig(enabled=True)
    dynamic_worker = DynamicWorkerConfig(enabled=True)

    backend = DragonVllmInferenceBackend(
        model=model,
        hardware=hardware,
        batching=batching,
        guardrails=guardrails,
        dynamic_worker=dynamic_worker,
        use_service=False,
    )

    config = backend._build_inference_config()

    assert config.model is model
    assert config.hardware is hardware
    assert config.batching is batching
    assert config.guardrails is guardrails
    assert config.dynamic_worker is dynamic_worker


def test_defaults_match_current_rhapsody_behavior():
    """Omitting hardware/batching/guardrails/dynamic_worker preserves today's forced defaults:

    pre-batch batching enabled, guardrails off, dynamic workers off.
    """
    backend = DragonVllmInferenceBackend(model=_model_config(), use_service=False)

    assert backend.batching.enabled is True
    assert backend.batching.batch_type == "pre-batch"
    assert backend.guardrails.enabled is False
    assert backend.dynamic_worker.enabled is False
    assert isinstance(backend.hardware, HardwareConfig)


def test_model_name_convenience_field():
    """self.model_name mirrors model.model_name for the HTTP handlers."""
    backend = DragonVllmInferenceBackend(
        model=_model_config(model_name="my-model"), use_service=False
    )

    assert backend.model_name == "my-model"


def test_dynamic_batch_type_rejected():
    """batch_type='dynamic' is out of scope for this backend and must fail fast."""
    with pytest.raises(NotImplementedError):
        DragonVllmInferenceBackend(
            model=_model_config(),
            batching=BatchingConfig(enabled=True, batch_type="dynamic"),
            use_service=False,
        )


@pytest.mark.asyncio
async def test_await_backend_delegates_to_initialize():
    """Await backend (and thus await DragonVllmInferenceBackend(...)) delegates to initialize() and
    returns the backend itself, matching the other execution backends' __await__ contract."""
    backend = DragonVllmInferenceBackend(model=_model_config(), use_service=False)
    backend.initialize = AsyncMock(return_value=backend)

    result = await backend

    backend.initialize.assert_called_once()
    assert result is backend


# ============================================================================
# Response unwrapping
# ============================================================================


def test_unwrap_assistant_response_extracts_assistant_key():
    """Inference.query() dict responses are unwrapped to their 'assistant' text."""
    response = {
        "assistant": "hello world",
        "model_inference_latency": 0.42,
        "requests_per_second": 3.1,
    }

    assert _unwrap_assistant_response(response) == "hello world"


def test_unwrap_assistant_response_dict_without_assistant_key_passes_through():
    """A dict missing the 'assistant' key is returned as-is (defensive fallback)."""
    response = {"unexpected": "shape"}

    assert _unwrap_assistant_response(response) == response


def test_unwrap_assistant_response_string_passthrough():
    """Plain strings (e.g. the batch processor's own timeout/error sentinels) pass through."""
    assert _unwrap_assistant_response("ERROR: timeout") == "ERROR: timeout"
    assert _unwrap_assistant_response("plain text") == "plain text"


# ============================================================================
# HTTP handler: /v1/chat/completions response normalization
# ============================================================================


@pytest.mark.asyncio
async def test_chat_completions_normalizes_assistant_dict_response():
    """_handle_chat_completions extracts 'assistant' from a dict response.

    Regression test for the response-shape change: Inference.query() now returns a
    metrics dict instead of a bare string, and the OpenAI-compatible endpoint must
    still surface the generated text, not the whole dict.
    """
    backend = DragonVllmInferenceBackend(model=_model_config(), use_service=True)
    backend.is_initialized = True
    backend.generate = AsyncMock(
        return_value=[{"assistant": "generated text", "model_inference_latency": 0.1}]
    )

    request = MagicMock()
    request.json = AsyncMock(
        return_value={"messages": [{"role": "user", "content": "hi"}], "model": "test-model"}
    )

    response = await backend._handle_chat_completions(request)

    import json

    body = json.loads(response.body)
    assert body["choices"][0]["message"]["content"] == "generated text"


# ============================================================================
# Batch processor: response timeout (B1)
# ============================================================================


async def _wait_terminal(states, count, limit=5.0):
    async with asyncio.timeout(limit):
        while len([s for s in states if s[1] in ("DONE", "FAILED")]) < count:
            await asyncio.sleep(0.005)


@pytest.mark.asyncio
async def test_answered_task_is_done(monkeypatch):
    async with _running_backend(monkeypatch) as (backend, states):
        task = {"uid": "t1", "prompt": ["a", "b"]}
        await backend.submit_tasks([task])
        await _wait_terminal(states, 1)
        assert states[-1] == ("t1", "DONE")
        assert task["response"] == ["re: a", "re: b"]
        assert "exception" not in task


@pytest.mark.asyncio
async def test_task_with_timed_out_prompts_is_failed_with_partial_response(monkeypatch):
    async with _running_backend(monkeypatch, answer=1) as (backend, states):
        task = {"uid": "t1", "prompt": ["a", "b", "c"]}
        await backend.submit_tasks([task])
        await _wait_terminal(states, 1)
        assert states[-1] == ("t1", "FAILED")
        assert isinstance(task["exception"], TimeoutError)
        assert "2 of 3" in str(task["exception"])
        assert task["response"][0] == "re: a"
        assert all(r.startswith("ERROR:") for r in task["response"][1:])


@pytest.mark.asyncio
async def test_only_the_task_with_unanswered_prompts_fails(monkeypatch):
    async with _running_backend(monkeypatch, answer=1) as (backend, states):
        answered = {"uid": "t1", "prompt": "a"}
        starved = {"uid": "t2", "prompt": ["b", "c"]}
        await backend.submit_tasks([answered, starved])
        await _wait_terminal(states, 2)
        final = {uid: state for uid, state in states if state in ("DONE", "FAILED")}
        assert final == {"t1": "DONE", "t2": "FAILED"}
        assert "exception" not in answered
        assert isinstance(starved["exception"], TimeoutError)


@pytest.mark.asyncio
async def test_generate_keeps_error_strings_on_timeout(monkeypatch):
    async with _running_backend(monkeypatch, answer=0) as (backend, _states):
        results = await backend.generate(["a", "b"], timeout=5)
        assert len(results) == 2
        assert all(r.startswith("ERROR:") for r in results)


# ============================================================================
# Telemetry: plain data through the emitter
# ============================================================================


class _RecordingEmitter:
    """An emitter that records every call in a list it shares with the task callbacks."""

    enabled = True

    def __init__(self, log, raises=False):
        self.log = log
        self.raises = raises

    def emit(self, name, attributes=None, *, event_time=None, task_id=None):
        if self.raises:
            raise RuntimeError("emitter broke")
        self.log.append(("emit", name, attributes, event_time))


def _record(backend, raises=False):
    """Make the backend emit into a shared log together with its task callbacks."""
    log = []
    backend.telemetry = _RecordingEmitter(log, raises=raises)
    backend.register_callback(lambda task, state: log.append(("cb", task["uid"], state)))
    return log


async def _wait_callbacks(log, count, limit=5.0):
    async with asyncio.timeout(limit):
        while len([e for e in log if e[0] == "cb" and e[2] in ("DONE", "FAILED")]) < count:
            await asyncio.sleep(0.005)


def _events(log, name=None):
    return [e for e in log if e[0] == "emit" and (name is None or e[1].endswith(name))]


STARTED = "BatchStarted"
ENDED = "BatchEnded"


@pytest.mark.parametrize(
    ("first_timeout", "size", "start", "total", "expected"),
    [
        (None, 3, 0, 5, 0),  # no timeout at all
        (0, 3, 0, 5, 3),  # everything unanswered
        (2, 3, 0, 5, 1),  # the timeout falls inside the request
        (2, 2, 3, 5, 2),  # the request lies after it
        (4, 2, 0, 5, 0),  # the request lies before it
        (1, 9, 3, 5, 2),  # the request runs past the total
    ],
)
def test_timed_out_in_range(first_timeout, size, start, total, expected):
    from rhapsody.backends.ai.vllm import _timed_out_in_range

    assert _timed_out_in_range(start, size, first_timeout, total) == expected


def test_dragon_fields_take_only_the_finite_numbers_and_strings():
    from rhapsody.backends.ai.vllm import _dragon_fields

    ok = {"model_inference_latency": 0.5, "hostname": "n1", "assistant": "secret text"}
    assert _dragon_fields(ok) == {"dragon_model_duration_s": 0.5, "dragon_worker_host": "n1"}
    assert _dragon_fields({"model_inference_latency": True, "hostname": ""}) == {}
    assert _dragon_fields({"model_inference_latency": float("nan")}) == {}
    assert _dragon_fields({"model_inference_latency": float("inf")}) == {}
    assert _dragon_fields("ERROR: text") == {}
    assert _dragon_fields(None) == {}


@pytest.mark.asyncio
async def test_one_pair_of_events_per_batch_whatever_its_size(monkeypatch):
    async with _running_backend(monkeypatch) as (backend, _states):
        log = _record(backend)
        tasks = [{"uid": f"t{i}", "prompt": [f"p{i}a", f"p{i}b"]} for i in range(3)]
        await backend.submit_tasks(tasks)
        await _wait_callbacks(log, 3)

    names = [e[1] for e in _events(log)]
    assert names == ["rhapsody.inference.BatchStarted", "rhapsody.inference.BatchEnded"]
    started, ended = _events(log, STARTED)[0][2], _events(log, ENDED)[0][2]
    assert started["batch_id"] == ended["batch_id"] == 1
    assert started["task_ids"] == ["t0", "t1", "t2"]
    assert (started["batch_prompts"], started["batch_requests"]) == (6, 3)
    assert started["batch_trigger"] == ended["batch_trigger"] == "deadline"
    assert started["model"] == ended["model"] == "test-model"
    assert ended["batch_duration_s"] >= 0
    assert (ended["batch_prompts"], ended["batch_requests"]) == (6, 3)
    assert ended["timed_out_prompts"] == 0
    assert ended["dragon_model_duration_s"] == 0.1
    assert ended["dragon_worker_host"] == "node7"
    assert [r["task_id"] for r in ended["requests"]] == ["t0", "t1", "t2"]
    assert [r["prompt_count"] for r in ended["requests"]] == [2, 2, 2]


@pytest.mark.asyncio
async def test_the_batch_ids_count_up_and_the_trigger_is_judged_from_the_batch(monkeypatch):
    async with _running_backend(monkeypatch, max_batch_size=2) as (backend, _states):
        log = _record(backend)
        await backend.submit_tasks([{"uid": f"t{i}", "prompt": "p"} for i in range(3)])
        await _wait_callbacks(log, 3)

    starts = [e[2] for e in _events(log, STARTED)]
    assert [s["batch_id"] for s in starts] == [1, 2]
    assert [s["batch_trigger"] for s in starts] == ["size", "deadline"]  # 2 = max, then 1 left
    assert [len(s["task_ids"]) for s in starts] == [2, 1]


@pytest.mark.asyncio
async def test_the_end_event_is_emitted_before_any_task_is_reported(monkeypatch):
    async with _running_backend(monkeypatch) as (backend, _states):
        log = _record(backend)
        await backend.submit_tasks([{"uid": "t1", "prompt": "a"}, {"uid": "t2", "prompt": "b"}])
        await _wait_callbacks(log, 2)

    order = [e[1].split(".")[-1] if e[0] == "emit" else e[2] for e in log]
    # RUNNING callbacks come first (submit), then the two events, then DONE for every task.
    assert order == ["RUNNING", "RUNNING", STARTED, ENDED, "DONE", "DONE"]


@pytest.mark.asyncio
async def test_the_queue_wait_is_measured_from_the_request_being_created(monkeypatch):
    async with _running_backend(monkeypatch, max_batch_wait_ms=150) as (backend, _states):
        log = _record(backend)
        await backend.submit_tasks([{"uid": "t1", "prompt": "a"}])
        await _wait_callbacks(log, 1)

    (row,) = _events(log, ENDED)[0][2]["requests"]
    assert 0.1 <= row["queue_duration_s"] < 5.0  # waited out most of the accumulation window


@pytest.mark.asyncio
async def test_timed_out_prompts_are_counted_per_request_and_sum_to_the_batch(monkeypatch):
    # A (task, 1 prompt), B (task, 2 prompts), api (generate, 2 prompts): 5 prompts, 2 answered.
    async with _running_backend(monkeypatch, answer=2) as (backend, _states):
        log = _record(backend)
        await backend.submit_tasks([{"uid": "A", "prompt": "a"}])
        await backend.submit_tasks([{"uid": "B", "prompt": ["b1", "b2"]}])
        generate = asyncio.create_task(backend.generate(["g1", "g2"], timeout=5))
        await asyncio.sleep(0)
        await _wait_callbacks(log, 2)
        await generate

    ended = _events(log, ENDED)[0][2]
    rows = {r["task_id"]: r for r in ended["requests"]}
    assert [r["task_id"] for r in ended["requests"]] == ["A", "B", None]
    assert rows["A"]["timed_out_prompts"] == 0
    assert rows["B"]["timed_out_prompts"] == 1
    assert rows[None]["timed_out_prompts"] == 2
    assert ended["timed_out_prompts"] == 3 == sum(r["timed_out_prompts"] for r in ended["requests"])
    # B1 unchanged: only B is reported FAILED.
    final = {e[1]: e[2] for e in log if e[0] == "cb" and e[2] in ("DONE", "FAILED")}
    assert final == {"A": "DONE", "B": "FAILED"}


@pytest.mark.asyncio
async def test_generate_only_batches_emit_rows_without_a_task_id(monkeypatch):
    async with _running_backend(monkeypatch) as (backend, _states):
        log = _record(backend)
        await backend.generate(["a", "b"], timeout=5)

    started, ended = _events(log, STARTED)[0][2], _events(log, ENDED)[0][2]
    assert started["task_ids"] == []
    assert [r["task_id"] for r in ended["requests"]] == [None]
    assert ended["requests"][0]["prompt_count"] == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("respond", "expected"),
    [
        (lambda p: {"assistant": p, "hostname": "n9"}, {"dragon_worker_host": "n9"}),  # no latency
        (
            lambda p: {"assistant": p, "model_inference_latency": 0.7},
            {"dragon_model_duration_s": 0.7},
        ),
        (lambda p: f"plain {p}", {}),  # not a dict at all
    ],
)
async def test_dragon_fields_are_optional(monkeypatch, respond, expected):
    async with _running_backend(monkeypatch, pipeline=_FakePipeline(respond=respond)) as (b, _s):
        log = _record(b)
        await b.submit_tasks([{"uid": "t1", "prompt": "a"}])
        await _wait_callbacks(log, 1)

    ended = _events(log, ENDED)[0][2]
    got = {k: v for k, v in ended.items() if k.startswith("dragon_")}
    assert got == expected


@pytest.mark.asyncio
async def test_no_prompt_or_response_text_in_any_payload(monkeypatch):
    marker_prompt, marker_answer = "PROMPT-MARKER-123", "ANSWER-MARKER-456"
    pipeline = _FakePipeline(
        respond=lambda p: {"assistant": marker_answer, "user": p, "hostname": "n1"}
    )
    async with _running_backend(monkeypatch, pipeline=pipeline) as (backend, _states):
        log = _record(backend)
        await backend.submit_tasks([{"uid": "t1", "prompt": marker_prompt}])
        await _wait_callbacks(log, 1)

    import json

    dumped = json.dumps([e[2] for e in _events(log)], default=str)
    assert marker_prompt not in dumped
    assert marker_answer not in dumped


@pytest.mark.asyncio
async def test_an_empty_batch_emits_nothing(monkeypatch):
    async with _running_backend(monkeypatch) as (backend, _states):
        log = _record(backend)
        await backend.submit_tasks([{"uid": "t1", "prompt": []}])
        await asyncio.sleep(0.15)  # several accumulation windows
    assert _events(log) == []


@pytest.mark.asyncio
async def test_a_backend_without_telemetry_never_builds_or_emits_anything(monkeypatch):
    async with _running_backend(monkeypatch) as (backend, states):

        def boom(*args, **kwargs):
            raise AssertionError("telemetry was not disabled")

        backend._emit_telemetry = boom  # the default emitter is the no-op one: enabled is False
        await backend.submit_tasks([{"uid": "t1", "prompt": "a"}])
        await _wait_terminal(states, 1)
    assert states[-1] == ("t1", "DONE")


@pytest.mark.asyncio
async def test_an_emitter_that_raises_does_not_break_the_batch(monkeypatch):
    async with _running_backend(monkeypatch) as (backend, _states):
        log = _record(backend, raises=True)
        await backend.submit_tasks([{"uid": "t1", "prompt": "a"}])
        await _wait_callbacks(log, 1)
    assert [e for e in log if e[0] == "cb"][-1] == ("cb", "t1", "DONE")


@pytest.mark.asyncio
async def test_a_failing_pipeline_emits_a_start_but_no_end(monkeypatch):
    """Existing issue B11, pinned so a fix is a conscious change (see TELEMETRY-LIMITATION L12)."""
    async with _running_backend(monkeypatch, pipeline=_FakePipeline(fail=True)) as (b, _s):
        # The backend fails the request futures nobody awaits; keep asyncio from logging that.
        asyncio.get_running_loop().set_exception_handler(lambda loop, context: None)
        log = _record(b)
        await b.submit_tasks([{"uid": "t1", "prompt": "a"}])
        await _wait_callbacks(log, 1)
    assert len(_events(log, STARTED)) == 1
    assert _events(log, ENDED) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("attach_after", [False, True])
async def test_a_session_records_the_batch_of_the_real_backend(monkeypatch, tmp_path, attach_after):
    pytest.importorskip("opentelemetry", reason="opentelemetry-sdk not installed")
    from rhapsody.api import AITask
    from rhapsody.api import Session

    async with _running_backend(monkeypatch) as (backend, _states):
        session = Session(work_dir=str(tmp_path))
        if attach_after:
            telemetry = await session.start_telemetry(resource_poll_interval=3600)
            session.add_backend(backend)  # the backend is attached after telemetry started
        else:
            session.add_backend(backend)
            telemetry = await session.start_telemetry(resource_poll_interval=3600)
        task = AITask(prompt="hello", model="test-model")
        await session.submit_tasks([task])
        await session.wait_tasks([task], timeout=10)
        await telemetry.stop()

    spans = list(telemetry.read_traces())
    (task_span,) = [s for s in spans if s.name == "task"]
    (batch,) = [s for s in spans if s.name == "chat test-model"]
    assert [link.context.span_id for link in batch.links] == [task_span.context.span_id]
    attrs = dict(task_span.attributes)
    assert attrs["gen_ai.request.model"] == "test-model"
    assert attrs["rhapsody.inference.prompt_count"] == 1
    scope = "rhapsody.instrumentation.inference"
    metrics = {
        m.name: m
        for rm in telemetry.read_metrics().resource_metrics
        for sm in rm.scope_metrics
        if sm.scope.name == scope
        for m in sm.metrics
    }
    assert sum(dp.value for dp in metrics["rhapsody.inference.batches"].data.data_points) == 1
    assert sum(dp.value for dp in metrics["rhapsody.inference.requests"].data.data_points) == 1


@pytest.mark.asyncio
async def test_a_failed_batch_leaves_a_span_that_ends_at_stop(monkeypatch, tmp_path):
    pytest.importorskip("opentelemetry", reason="opentelemetry-sdk not installed")
    from rhapsody.api import AITask
    from rhapsody.api import Session

    async with _running_backend(monkeypatch, pipeline=_FakePipeline(fail=True)) as (backend, _s):
        asyncio.get_running_loop().set_exception_handler(lambda loop, context: None)
        session = Session(work_dir=str(tmp_path))
        session.add_backend(backend)
        telemetry = await session.start_telemetry(resource_poll_interval=3600)
        task = AITask(prompt="hello", model="test-model")
        await session.submit_tasks([task])
        await session.wait_tasks([task], timeout=10)
        await telemetry.stop()

    (batch,) = [s for s in telemetry.read_traces() if s.name == "chat test-model"]
    assert batch.status.status_code.name == "ERROR"
    assert batch.status.description == "batch did not finish"


@pytest.mark.asyncio
async def test_the_batch_duration_is_the_time_the_model_took(monkeypatch):
    pipeline = _FakePipeline(delay=0.12)  # every answer arrives 120 ms after the query
    async with _running_backend(monkeypatch, pipeline=pipeline, timeout_s=5) as (backend, _s):
        log = _record(backend)
        await backend.submit_tasks([{"uid": "t1", "prompt": "a"}])
        await _wait_callbacks(log, 1)

    ended = _events(log, ENDED)[0][2]
    assert 0.12 <= ended["batch_duration_s"] < 1.0  # not 0, not scaled to milliseconds


@pytest.mark.asyncio
async def test_the_trigger_counts_requests_not_prompts(monkeypatch):
    # One request holding two prompts in a batch limited to two requests: not a full batch.
    async with _running_backend(monkeypatch, max_batch_size=2) as (backend, _states):
        log = _record(backend)
        await backend.submit_tasks([{"uid": "t1", "prompt": ["p1", "p2"]}])
        await _wait_callbacks(log, 1)

    started = _events(log, STARTED)[0][2]
    assert (started["batch_requests"], started["batch_prompts"]) == (1, 2)
    assert started["batch_trigger"] == "deadline"


@pytest.mark.asyncio
async def test_a_timeout_at_the_first_prompt_reports_every_prompt_as_timed_out(monkeypatch):
    async with _running_backend(monkeypatch, answer=0) as (backend, _states):
        log = _record(backend)
        await backend.submit_tasks([{"uid": "t1", "prompt": ["a", "b", "c"]}])
        await _wait_callbacks(log, 1)

    ended = _events(log, ENDED)[0][2]
    assert ended["timed_out_prompts"] == 3
    assert [r["timed_out_prompts"] for r in ended["requests"]] == [3]


@pytest.mark.asyncio
async def test_a_late_response_is_not_taken_for_the_next_batchs_answer(monkeypatch):
    """B2: after a timeout the late answer must not reach the next batch.

    Fails with a single shared response queue (B would get A's answer). Passes because the
    timed-out batch's queue is a late queue the next batch never reads.
    """
    late = threading.Event()
    pipeline = _FakePipeline(hold={"a": late})  # a's answer is held back until released by hand
    async with _running_backend(monkeypatch, pipeline=pipeline, timeout_s=0.3) as (backend, states):
        first = {"uid": "A", "prompt": "a"}
        await backend.submit_tasks([first])
        await _wait_terminal(states, 1)  # A timed out: FAILED

        second = {"uid": "B", "prompt": "b"}
        await backend.submit_tasks([second])
        await _wait_terminal(states, 2)
        assert backend._response_queues.late_queue_count == 1
        late.set()  # now A's answer arrives, after B was answered
        async with asyncio.timeout(3):
            while backend._response_queues.late_queue_count:
                await asyncio.sleep(0.01)

    assert ("A", "FAILED") in states
    assert second["response"] == ["re: b"]  # B got its own answer, not A's
    assert len(backend.fake_queues) == 2
    assert backend.fake_queues[0].closed  # all answers of the late queue arrived: it is closed


@pytest.mark.asyncio
async def test_late_answers_beyond_the_queue_capacity_do_not_hang_the_shutdown(monkeypatch):
    """The cluster hang: unread late answers filled the 100-slot queue, a worker blocked in put(),
    and destroy() waited for it. Here a queue holds 5 and 8 late answers arrive.

    Fails (``hung``) with the old single queue that nobody reads after the timeout.
    """
    late = threading.Event()
    pipeline = _FakePipeline(hold=late)
    async with _running_backend(
        monkeypatch, pipeline=pipeline, timeout_s=0.1, queue_capacity=5
    ) as (backend, states):
        task = {"uid": "t1", "prompt": [f"p{i}" for i in range(8)]}
        await backend.submit_tasks([task])
        await _wait_terminal(states, 1)  # timed out with nothing read: 8 answers are missing
        late.set()
        await asyncio.wait_for(backend.shutdown(), timeout=10)

    assert pipeline.hung is False  # destroy() returned: no worker stayed blocked
    assert backend.fake_queues[0].max_backlog <= 5


@pytest.mark.asyncio
async def test_without_draining_the_same_shutdown_hangs(monkeypatch):
    """Proof that the test above reproduces the hang: take away the draining and it hangs."""
    monkeypatch.setattr(ResponseQueues, "give_up", lambda self, queue, missing: None)
    late = threading.Event()
    pipeline = _FakePipeline(hold=late)
    async with _running_backend(
        monkeypatch, pipeline=pipeline, timeout_s=0.1, queue_capacity=5
    ) as (backend, states):
        await backend.submit_tasks([{"uid": "t1", "prompt": [f"p{i}" for i in range(8)]}])
        await _wait_terminal(states, 1)
        late.set()
        with pytest.raises(RuntimeError, match="destroy"):
            await asyncio.wait_for(backend.shutdown(), timeout=10)
    assert pipeline.hung is True


@pytest.mark.asyncio
async def test_many_answered_batches_use_a_single_response_queue(monkeypatch):
    """Normal operation allocates nothing per batch.

    (Does not catch the shared-queue mutation: the B2 test does.)
    """
    async with _running_backend(monkeypatch, max_batch_size=2) as (backend, states):
        await backend.submit_tasks([{"uid": f"t{i}", "prompt": "p"} for i in range(9)])
        await _wait_terminal(states, 9)
    assert len(backend.fake_queues) == 1
    assert backend._response_queues.late_queue_count == 0


@pytest.mark.asyncio
async def test_a_failing_query_keeps_the_queue_current(monkeypatch):
    async with _running_backend(monkeypatch, pipeline=_FakePipeline(fail=True)) as (backend, _s):
        with pytest.raises(Exception):
            await backend.generate(["a"], timeout=2)
        assert backend._response_queues.late_queue_count == 0  # nothing was sent: none missing
        assert len(backend.fake_queues) == 1


@pytest.mark.asyncio
async def test_a_partly_answered_batch_leaves_exactly_the_missing_answers(monkeypatch):
    async with _running_backend(monkeypatch, pipeline=_FakePipeline(answer=1), timeout_s=0.1) as (
        backend,
        states,
    ):
        await backend.submit_tasks([{"uid": "t1", "prompt": ["a", "b", "c"]}])
        await _wait_terminal(states, 1)  # 1 of 3 answered, then the deadline
        assert list(backend._response_queues.late_queues.values()) == [2]


@pytest.mark.asyncio
async def test_a_batch_without_prompts_takes_no_response_queue(monkeypatch):
    async with _running_backend(monkeypatch) as (backend, _states):
        await backend.submit_tasks([{"uid": "t1", "prompt": []}])
        await asyncio.sleep(0.15)
    assert backend.fake_queues == []  # nothing was sent, so no queue was made (B13 is separate)


def test_a_destroy_that_ends_after_the_loop_closed_does_not_fail(monkeypatch):
    release = threading.Event()
    errors = []
    monkeypatch.setattr(threading, "excepthook", lambda args: errors.append(args))
    monkeypatch.setattr(vllm_mod, "_DESTROY_TIMEOUT_S", 0.05)

    class Pipeline(_FakePipeline):
        def destroy(self):
            release.wait(10)

    async def scenario():
        async with _running_backend(monkeypatch, pipeline=Pipeline()) as (backend, _states):
            await backend.shutdown()  # gives up after 0.05 s, the thread keeps running
            return backend

    backend = asyncio.run(scenario())  # the loop is closed when this returns
    release.set()
    for thread in threading.enumerate():
        if thread.name == "rhapsody-destroy":
            thread.join(5)
    assert errors == []
    assert not backend.is_initialized


@pytest.mark.asyncio
async def test_cancelling_the_processor_mid_batch_keeps_the_unread_answers_missing(monkeypatch):
    held = threading.Event()
    async with _running_backend(monkeypatch, pipeline=_FakePipeline(hold=held), timeout_s=30) as (
        backend,
        _states,
    ):
        await backend.submit_tasks([{"uid": "t1", "prompt": ["a", "b"]}])
        async with asyncio.timeout(3):
            while not backend.inference_pipeline.threads:  # the batch was sent
                await asyncio.sleep(0.005)
        backend.processor.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await backend.processor
        assert backend._response_queues.late_queues == {backend.fake_queues[0]: 2}
        held.set()
        async with asyncio.timeout(3):
            while backend._response_queues.late_queue_count:
                await asyncio.sleep(0.01)
        assert backend.fake_queues[0].closed


@pytest.mark.asyncio
async def test_shutdown_closes_a_queue_that_never_got_its_answers(monkeypatch):
    async with _running_backend(monkeypatch, pipeline=_FakePipeline(answer=0), timeout_s=0.05) as (
        backend,
        states,
    ):
        await backend.submit_tasks([{"uid": "t1", "prompt": ["a", "b"]}])
        await _wait_terminal(states, 1)
        assert backend._response_queues.late_queue_count == 1  # 2 answers that never come
        await backend.shutdown()
    assert all(queue.closed for queue in backend.fake_queues)
    assert backend._response_queues.late_queue_count == 0
    assert backend._instance_id not in vllm_mod._QUEUE_REGISTRY
    assert not backend.is_initialized


@pytest.mark.asyncio
async def test_shutdown_runs_destroy_off_the_event_loop(monkeypatch):
    seen = {}

    class Pipeline(_FakePipeline):
        def destroy(self):
            seen["thread"] = threading.current_thread()

    async with _running_backend(monkeypatch, pipeline=Pipeline()) as (backend, _states):
        await backend.shutdown()
    assert seen["thread"] is not threading.main_thread()
    assert seen["thread"].daemon


@pytest.mark.asyncio
async def test_shutdown_raises_what_destroy_raised_and_still_closes_the_queues(monkeypatch):
    class Pipeline(_FakePipeline):
        def destroy(self):
            raise ValueError("destroy failed")

    async with _running_backend(monkeypatch, pipeline=Pipeline(answer=0), timeout_s=0.05) as (
        backend,
        states,
    ):
        await backend.submit_tasks([{"uid": "t1", "prompt": "a"}])
        await _wait_terminal(states, 1)
        with pytest.raises(ValueError, match="destroy failed"):
            await backend.shutdown()
    assert all(queue.closed for queue in backend.fake_queues)
    assert backend._instance_id not in vllm_mod._QUEUE_REGISTRY


@pytest.mark.asyncio
async def test_shutdown_goes_on_when_destroy_does_not_return_and_leaves_queues_open(monkeypatch):
    monkeypatch.setattr(vllm_mod, "_DESTROY_TIMEOUT_S", 0.1)
    release = threading.Event()

    class Pipeline(_FakePipeline):
        def destroy(self):
            release.wait(10)  # a worker that never ends

    async with _running_backend(monkeypatch, pipeline=Pipeline(answer=0), timeout_s=0.05) as (
        backend,
        states,
    ):
        await backend.submit_tasks([{"uid": "t1", "prompt": "a"}])
        await _wait_terminal(states, 1)
        await asyncio.wait_for(backend.shutdown(), timeout=5)  # returned although destroy() did not
        assert not any(queue.closed for queue in backend.fake_queues)  # a worker may still use them
        assert not backend.is_initialized
    release.set()  # the daemon thread finishes later and must not fail the process


@pytest.mark.asyncio
async def test_a_cancelled_shutdown_still_clears_the_state(monkeypatch):
    release = threading.Event()

    class Pipeline(_FakePipeline):
        def destroy(self):
            release.wait(10)

    async with _running_backend(monkeypatch, pipeline=Pipeline()) as (backend, _states):
        shutdown = asyncio.create_task(backend.shutdown())
        await asyncio.sleep(0.05)
        shutdown.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await shutdown
        assert not backend.is_initialized
        assert backend._instance_id not in vllm_mod._QUEUE_REGISTRY
    release.set()


def test_a_pickled_backend_gets_fresh_response_queues():
    import pickle

    backend = DragonVllmInferenceBackend(model=_model_config(), use_service=False)
    restored = pickle.loads(pickle.dumps(backend))
    assert restored._response_queues is not backend._response_queues
    assert restored._response_queues.created_queue_count == 0


@pytest.mark.xfail(
    reason="B13: a request without prompts is never answered and its task never finishes "
    "(reported, not fixed)",
    strict=False,
)
@pytest.mark.asyncio
async def test_a_task_without_prompts_finishes(monkeypatch):
    async with _running_backend(monkeypatch) as (backend, states):
        await backend.submit_tasks([{"uid": "t1", "prompt": []}])
        await asyncio.sleep(0.15)
    assert [s for s in states if s[1] in ("DONE", "FAILED")], "the task was never reported"
