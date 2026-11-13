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

from rhapsody.backends.ai.config import BatchingConfig
from rhapsody.backends.ai.config import DynamicWorkerConfig
from rhapsody.backends.ai.config import GuardrailsConfig
from rhapsody.backends.ai.config import HardwareConfig
from rhapsody.backends.ai.config import ModelConfig
from rhapsody.backends.ai.vllm import DragonVllmInferenceBackend
from rhapsody.backends.ai.vllm import _unwrap_assistant_response


def _model_config(**overrides):
    kwargs = {"model_name": "test-model", "hf_token": "hf-test-token", "tp_size": 1}
    kwargs.update(overrides)
    return ModelConfig(**kwargs)


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


class _FakeQueue:
    """Stand-in for the Dragon response queue (empty / get_nowait / put / close)."""

    def __init__(self):
        self._items = []

    def empty(self):
        return not self._items

    def get_nowait(self):
        return self._items.pop(0)

    def put(self, item):
        self._items.append(item)

    def close(self):
        pass


class _FakePipeline:
    """Answers the first ``answer`` prompts of every query, in order, like Inference.query()."""

    def __init__(self, answer=None):
        self.answer = answer  # None = answer everything

    def query(self, args):
        prompts, response_queue = args
        count = len(prompts) if self.answer is None else min(self.answer, len(prompts))
        for prompt in prompts[:count]:
            response_queue.put({"assistant": f"re: {prompt}", "model_inference_latency": 0.1})


async def _running_backend(monkeypatch, answer=None, timeout_s=0.05):
    from rhapsody.backends.ai import vllm as vllm_mod

    monkeypatch.setattr(vllm_mod, "_BATCH_RESPONSE_TIMEOUT_S", timeout_s)
    backend = DragonVllmInferenceBackend(
        model=_model_config(), use_service=False, max_batch_wait_ms=10
    )
    queue = _FakeQueue()
    monkeypatch.setitem(vllm_mod._QUEUE_REGISTRY, backend._instance_id, {"response": queue})
    backend.inference_pipeline = _FakePipeline(answer)
    backend.is_initialized = True
    states = []
    backend.register_callback(lambda task, state: states.append((task["uid"], state)))
    processor = asyncio.create_task(backend._batch_processor())
    return backend, states, processor


async def _wait_terminal(states, count, limit=5.0):
    async with asyncio.timeout(limit):
        while len([s for s in states if s[1] in ("DONE", "FAILED")]) < count:
            await asyncio.sleep(0.005)


@pytest.mark.asyncio
async def test_answered_task_is_done(monkeypatch):
    backend, states, processor = await _running_backend(monkeypatch)
    try:
        task = {"uid": "t1", "prompt": ["a", "b"]}
        await backend.submit_tasks([task])
        await _wait_terminal(states, 1)
        assert states[-1] == ("t1", "DONE")
        assert task["response"] == ["re: a", "re: b"]
        assert "exception" not in task
    finally:
        processor.cancel()


@pytest.mark.asyncio
async def test_task_with_timed_out_prompts_is_failed_with_partial_response(monkeypatch):
    backend, states, processor = await _running_backend(monkeypatch, answer=1)
    try:
        task = {"uid": "t1", "prompt": ["a", "b", "c"]}
        await backend.submit_tasks([task])
        await _wait_terminal(states, 1)
        assert states[-1] == ("t1", "FAILED")
        assert isinstance(task["exception"], TimeoutError)
        assert "2 of 3" in str(task["exception"])
        assert task["response"][0] == "re: a"
        assert all(r.startswith("ERROR:") for r in task["response"][1:])
    finally:
        processor.cancel()


@pytest.mark.asyncio
async def test_only_the_task_with_unanswered_prompts_fails(monkeypatch):
    backend, states, processor = await _running_backend(monkeypatch, answer=1)
    try:
        answered = {"uid": "t1", "prompt": "a"}
        starved = {"uid": "t2", "prompt": ["b", "c"]}
        await backend.submit_tasks([answered, starved])
        await _wait_terminal(states, 2)
        final = {uid: state for uid, state in states if state in ("DONE", "FAILED")}
        assert final == {"t1": "DONE", "t2": "FAILED"}
        assert "exception" not in answered
        assert isinstance(starved["exception"], TimeoutError)
    finally:
        processor.cancel()


@pytest.mark.asyncio
async def test_generate_keeps_error_strings_on_timeout(monkeypatch):
    backend, _states, processor = await _running_backend(monkeypatch, answer=0)
    try:
        results = await backend.generate(["a", "b"], timeout=5)
        assert len(results) == 2
        assert all(r.startswith("ERROR:") for r in results)
    finally:
        processor.cancel()
