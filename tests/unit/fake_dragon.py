"""Fake Dragon pieces shared by the vLLM backend tests and the inference telemetry tests.

``FakeQueue`` and ``FakePipeline`` stand in for Dragon's response queue and ``Inference.query()``;
``running_backend`` gives a ``DragonVllmInferenceBackend`` with its batch processor running over
them. Nothing here imports Dragon at module level: the test files that use it skip on their own.

Like Dragon, a queue can have a capacity: a ``put`` on a full queue waits until someone reads, and
``FakePipeline.destroy()`` waits for such workers, so a test can reproduce the shutdown hang. Workers
that wait or sleep run in real threads, never on the event loop (a parked ``put`` on the loop would
freeze everything, including the drain task under test).
"""

import asyncio
import collections
import contextlib
import queue as stdlib_queue
import threading
import time


def model_config(**overrides):
    from rhapsody.backends.ai.config import ModelConfig

    kwargs = {"model_name": "test-model", "hf_token": "hf-test-token", "tp_size": 1}
    kwargs.update(overrides)
    return ModelConfig(**kwargs)


class FakeQueue:
    """Stand-in for the Dragon response queue (empty / get_nowait / put / close).

    ``capacity`` (None = unlimited) makes ``put`` wait while the queue is full, as Dragon's does;
    ``parked`` counts the puts waiting right now and ``max_backlog`` the most items it ever held.
    """

    def __init__(self, capacity=None):
        self.capacity = capacity
        self._items = collections.deque()
        self._cond = threading.Condition()
        self.parked = 0
        self.max_backlog = 0
        self.closed = False

    def empty(self):
        with self._cond:
            return not self._items

    def get_nowait(self):
        with self._cond:
            if not self._items:
                raise stdlib_queue.Empty
            item = self._items.popleft()
            self._cond.notify_all()
            return item

    def put(self, item):
        with self._cond:
            if self.capacity is not None and len(self._items) >= self.capacity:
                self.parked += 1
                try:
                    self._cond.wait_for(
                        lambda: len(self._items) < self.capacity or self.closed, timeout=30
                    )
                finally:
                    self.parked -= 1
            self._items.append(item)
            self.max_backlog = max(self.max_backlog, len(self._items))

    def __len__(self):
        with self._cond:
            return len(self._items)

    def close(self):
        with self._cond:
            self.closed = True
            self._cond.notify_all()


class FakePipeline:
    """Answers the first ``answer`` prompts of every query, in order, like Inference.query().

    ``delay`` (seconds, or a function of the prompt) makes each answer arrive that long after the
    query, from a worker thread, so batches take time and answers can arrive late. A queue with a
    capacity also needs a worker thread, because its puts may wait. ``hold`` (a ``threading.Event``,
    or a dict prompt -> Event) keeps the answer back until the event is set, for tests that release
    a late answer by hand.
    """

    def __init__(self, answer=None, respond=None, fail=False, delay=None, hold=None):
        self.answer = answer  # None = answer everything
        self.respond = respond  # prompt -> response, to shape what Dragon sends back
        self.fail = fail
        self.delay = delay  # None, a number, or prompt -> seconds
        self.hold = hold
        self.threads = []
        self.hung = False  # destroy() found a worker still blocked

    def _item(self, prompt, count):
        if self.respond is not None:
            return self.respond(prompt)
        return {
            "assistant": f"re: {prompt}",
            "model_inference_latency": 0.1,
            "hostname": "node7",
            "batch_size": count,
        }

    def _work(self, prompts, response_queue):
        for prompt in prompts:
            wait = self.delay(prompt) if callable(self.delay) else self.delay
            if wait:
                time.sleep(wait)
            held = self.hold.get(prompt) if isinstance(self.hold, dict) else self.hold
            if held is not None:
                held.wait(timeout=30)
            response_queue.put(self._item(prompt, len(prompts)))

    def query(self, args):
        if self.fail:
            raise RuntimeError("pipeline down")
        prompts, response_queue = args
        count = len(prompts) if self.answer is None else min(self.answer, len(prompts))
        prompts = prompts[:count]
        needs_thread = bool(self.delay) or self.hold is not None
        needs_thread = needs_thread or getattr(response_queue, "capacity", None) is not None
        if not needs_thread:
            for prompt in prompts:
                response_queue.put(self._item(prompt, count))
            return
        worker = threading.Thread(target=self._work, args=(prompts, response_queue), daemon=True)
        self.threads.append(worker)
        worker.start()

    def destroy(self, timeout_s=2.0):
        """Wait for the workers like Dragon's ``destroy()``; a blocked one is a hang."""
        deadline = time.monotonic() + timeout_s
        for worker in self.threads:
            worker.join(max(deadline - time.monotonic(), 0))
        if any(worker.is_alive() for worker in self.threads):
            self.hung = True
            raise RuntimeError("destroy() hung: a worker is blocked on a response queue")


@contextlib.asynccontextmanager
async def running_backend(
    monkeypatch, answer=None, timeout_s=0.05, pipeline=None, queue_capacity=None, **backend_kwargs
):
    """A backend with its batch processor running; the processor is stopped on exit.

    The backend's response queues make ``FakeQueue(queue_capacity)``; the queues it made are in
    ``backend.fake_queues``.
    """
    from rhapsody.backends.ai import vllm as vllm_mod
    from rhapsody.backends.ai.vllm import DragonVllmInferenceBackend
    from rhapsody.backends.ai.vllm import ResponseQueues

    monkeypatch.setattr(vllm_mod, "_BATCH_RESPONSE_TIMEOUT_S", timeout_s)
    backend_kwargs.setdefault("max_batch_wait_ms", 10)
    backend = DragonVllmInferenceBackend(model=model_config(), use_service=False, **backend_kwargs)
    backend.fake_queues = []

    def make_queue():
        queue = FakeQueue(queue_capacity)
        backend.fake_queues.append(queue)
        return queue

    backend._response_queues = ResponseQueues(make_queue)
    backend.inference_pipeline = pipeline or FakePipeline(answer)
    backend.is_initialized = True
    states = []
    backend.register_callback(lambda task, state: states.append((task["uid"], state)))
    processor = asyncio.create_task(backend._batch_processor())
    backend.processor = processor  # for tests that cancel it mid-batch
    try:
        yield backend, states
    finally:
        processor.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await processor
        await backend._response_queues.close(workers_gone=True)
