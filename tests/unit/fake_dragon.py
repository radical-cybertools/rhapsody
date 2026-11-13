"""Fake Dragon pieces shared by the vLLM backend tests and the inference telemetry tests.

``FakeQueue`` and ``FakePipeline`` stand in for Dragon's response queue and ``Inference.query()``;
``running_backend`` gives a ``DragonVllmInferenceBackend`` with its batch processor running over
them. Nothing here imports Dragon at module level: the test files that use it skip on their own.
"""

import asyncio
import contextlib


def model_config(**overrides):
    from rhapsody.backends.ai.config import ModelConfig

    kwargs = {"model_name": "test-model", "hf_token": "hf-test-token", "tp_size": 1}
    kwargs.update(overrides)
    return ModelConfig(**kwargs)


class FakeQueue:
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


class FakePipeline:
    """Answers the first ``answer`` prompts of every query, in order, like Inference.query()."""

    def __init__(self, answer=None, respond=None, fail=False):
        self.answer = answer  # None = answer everything
        self.respond = respond  # prompt -> response, to shape what Dragon sends back
        self.fail = fail

    def query(self, args):
        if self.fail:
            raise RuntimeError("pipeline down")
        prompts, response_queue = args
        count = len(prompts) if self.answer is None else min(self.answer, len(prompts))
        for prompt in prompts[:count]:
            if self.respond is not None:
                response_queue.put(self.respond(prompt))
            else:
                response_queue.put(
                    {
                        "assistant": f"re: {prompt}",
                        "model_inference_latency": 0.1,
                        "hostname": "node7",
                        "batch_size": len(prompts),
                    }
                )


@contextlib.asynccontextmanager
async def running_backend(
    monkeypatch, answer=None, timeout_s=0.05, pipeline=None, **backend_kwargs
):
    """A backend with its batch processor running; the processor is stopped on exit."""
    from rhapsody.backends.ai import vllm as vllm_mod
    from rhapsody.backends.ai.vllm import DragonVllmInferenceBackend

    monkeypatch.setattr(vllm_mod, "_BATCH_RESPONSE_TIMEOUT_S", timeout_s)
    backend_kwargs.setdefault("max_batch_wait_ms", 10)
    backend = DragonVllmInferenceBackend(model=model_config(), use_service=False, **backend_kwargs)
    queue = FakeQueue()
    monkeypatch.setitem(vllm_mod._QUEUE_REGISTRY, backend._instance_id, {"response": queue})
    backend.inference_pipeline = pipeline or FakePipeline(answer)
    backend.is_initialized = True
    states = []
    backend.register_callback(lambda task, state: states.append((task["uid"], state)))
    processor = asyncio.create_task(backend._batch_processor())
    try:
        yield backend, states
    finally:
        processor.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await processor
