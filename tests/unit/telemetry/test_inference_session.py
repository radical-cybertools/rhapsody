"""Inference telemetry through a real Session, without Dragon.

A stand-in backend does what the vLLM batcher does around a batch: it emits ``BatchStarted`` and
``BatchEnded`` through its emitter, then reports its tasks DONE. This pins the ordering the
instrumentation relies on: the end event is processed before the task's terminal event, so the task
span is still open when its per-task attributes are set.
"""

import asyncio
import time

import pytest

pytest.importorskip("opentelemetry", reason="opentelemetry-sdk not installed")

from rhapsody.api import ComputeTask
from rhapsody.api import Session
from rhapsody.backends.execution.noop import NoopExecutionBackend

PREFIX = "rhapsody.inference."


def noop():
    return None


class FakeInference(NoopExecutionBackend):
    """Reports each submitted batch the way the vLLM batcher does."""

    telemetry_domain = "inference"

    async def submit_tasks(self, tasks):
        for task in tasks:
            self._callback_func(task, "RUNNING")
        self._futures[tasks[0]["uid"]] = asyncio.create_task(self._run_batch(list(tasks)))

    async def _run_batch(self, tasks):
        await asyncio.sleep(0.02)  # the batcher's accumulation window
        uids = [t["uid"] for t in tasks]
        self.telemetry.emit(
            PREFIX + "BatchStarted",
            {
                "batch_id": 1,
                "model": "m",
                "batch_prompts": len(uids),
                "batch_requests": len(uids),
                "batch_trigger": "deadline",
                "task_ids": uids,
            },
            event_time=time.time(),
        )
        await asyncio.sleep(0.02)
        self.telemetry.emit(
            PREFIX + "BatchEnded",
            {
                "batch_id": 1,
                "model": "m",
                "batch_trigger": "deadline",
                "batch_prompts": len(uids),
                "batch_requests": len(uids),
                "batch_duration_s": 0.02,
                "timed_out_prompts": 0,
                "requests": [
                    {
                        "task_id": uid,
                        "prompt_count": 1,
                        "queue_duration_s": 0.02,
                        "timed_out_prompts": 0,
                    }
                    for uid in uids
                ],
            },
            event_time=time.time(),
        )
        for task in tasks:
            task.update({"return_value": True, "stdout": "", "stderr": "", "exit_code": 0})
            task["state"] = "DONE"
            self._callback_func(task, "DONE")


@pytest.mark.parametrize("attach_after", [False, True])
async def test_the_end_event_is_processed_before_the_tasks_end(tmp_path, attach_after):
    backend = await FakeInference(name="fakeinf")
    session = Session(work_dir=str(tmp_path))
    if attach_after:
        telemetry = await session.start_telemetry(resource_poll_interval=3600)
        session.add_backend(backend)
    else:
        session.add_backend(backend)
        telemetry = await session.start_telemetry(resource_poll_interval=3600)

    tasks = [ComputeTask(function=noop) for _ in range(2)]
    await session.submit_tasks(tasks)
    await session.wait_tasks(tasks, timeout=10)
    await telemetry.stop()

    # Event order: the batch events come before the tasks' terminal events.
    types = [e.event_type for e in telemetry._event_log]
    end_at = types.index(PREFIX + "BatchEnded")
    completed_at = [i for i, t in enumerate(types) if t == "TaskCompleted"]
    assert len(completed_at) == 2
    assert types.index(PREFIX + "BatchStarted") < end_at < min(completed_at)

    # Because of that order, the task spans carry the per-task attributes...
    spans = list(telemetry.read_traces())
    task_spans = [s for s in spans if s.name == "task"]
    assert len(task_spans) == 2
    for span in task_spans:
        attrs = dict(span.attributes)
        assert attrs["gen_ai.request.model"] == "m"
        assert attrs["rhapsody.inference.batch.id"] == 1
        assert attrs["rhapsody.inference.prompt_count"] == 1
        assert attrs["rhapsody.inference.request.queue.duration"] == 0.02

    # ...and the batch span links to both of them.
    (batch,) = [s for s in spans if s.name == "chat m"]
    assert {link.context.span_id for link in batch.links} == {s.context.span_id for s in task_spans}
    names = {
        m.name
        for rm in telemetry.read_metrics().resource_metrics
        for sm in rm.scope_metrics
        if sm.scope.name == "rhapsody.instrumentation.inference"
        for m in sm.metrics
    }
    assert "rhapsody.inference.batches" in names
