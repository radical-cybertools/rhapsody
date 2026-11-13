"""The inference instrumentation: batch events in, spans and metrics out. No Dragon needed."""

import logging

import pytest

pytest.importorskip("opentelemetry", reason="opentelemetry-sdk not installed")

from opentelemetry.trace import SpanKind
from opentelemetry.trace import StatusCode

from rhapsody.telemetry.events import TaskCompleted
from rhapsody.telemetry.events import TaskCreated
from rhapsody.telemetry.events import make_event
from rhapsody.telemetry.instrumentation import inference
from rhapsody.telemetry.manager import TelemetryManager

SCOPE = "rhapsody.instrumentation.inference"
STARTED = "rhapsody.inference.BatchStarted"
ENDED = "rhapsody.inference.BatchEnded"
# Advisory bucket bounds of the duration histograms: 10 ms doubling up to 327.68 s.
BUCKETS = [0.01, 0.02, 0.04, 0.08, 0.16, 0.32, 0.64, 1.28]
BUCKETS += [2.56, 5.12, 10.24, 20.48, 40.96, 81.92, 163.84, 327.68]


class Backend:
    """Stand-in for the vLLM backend: it only names its domain and emits plain data."""

    telemetry_domain = "inference"

    def submit_tasks(self, tasks):
        pass


async def _manager(**kwargs):
    manager = TelemetryManager(session_id="s", **kwargs)
    backend = Backend()
    manager.attach_backend(backend, session_id="s", backend_name="vllm", interval=60)
    await manager.start()
    return manager, backend


async def _create_task(manager, task_id):
    manager.emit(make_event(TaskCreated, session_id="s", backend="vllm", task_id=task_id))
    await manager._queue.join()


def _started(backend, batch_id=1, task_ids=(), t=100.0, **kw):
    attrs = {
        "batch_id": batch_id,
        "model": "m",
        "batch_prompts": 3,
        "batch_requests": 2,
        "batch_trigger": "deadline",
        "task_ids": list(task_ids),
        **kw,
    }
    backend.telemetry.emit(STARTED, attrs, event_time=t)


def _ended(backend, batch_id=1, rows=(), t=102.5, **kw):
    attrs = {
        "batch_id": batch_id,
        "model": "m",
        "batch_trigger": "deadline",
        "batch_prompts": 3,
        "batch_requests": 2,
        "batch_duration_s": 2.5,
        "timed_out_prompts": 0,
        "requests": list(rows),
        **kw,
    }
    backend.telemetry.emit(ENDED, attrs, event_time=t)


def _row(task_id, prompts=1, queue=0.5, timed_out=0):
    return {
        "task_id": task_id,
        "prompt_count": prompts,
        "queue_duration_s": queue,
        "timed_out_prompts": timed_out,
    }


def _spans(manager):
    return list(manager.read_traces())


def _batch_spans(manager):
    return [s for s in _spans(manager) if s.name.split(" ")[0] == "chat"]


def _metrics(manager):
    data = manager.read_metrics()
    out = {}
    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            if sm.scope.name == SCOPE:
                for metric in sm.metrics:
                    out[metric.name] = metric
    return out


# ------------------------------------------------------------------- batch span


async def test_the_batch_span_has_real_times_parent_kind_links_and_attributes():
    manager, backend = await _manager()
    await _create_task(manager, "t1")
    await _create_task(manager, "t2")
    _started(backend, task_ids=["t1", "t2", "no-such-task"])
    _ended(backend, dragon_model_duration_s=2.0, dragon_worker_host="node7")
    await manager._queue.join()
    for task_id in ("t1", "t2"):  # finished spans only appear once their task ended
        manager.emit(make_event(TaskCompleted, session_id="s", backend="vllm", task_id=task_id))
    await manager.stop()

    (span,) = _batch_spans(manager)
    session = next(s for s in _spans(manager) if s.name == "session")
    tasks = {dict(s.attributes)["task_id"]: s for s in _spans(manager) if s.name == "task"}
    assert span.name == "chat m"
    assert span.kind == SpanKind.CLIENT
    assert span.start_time == 100_000_000_000
    assert span.end_time == 102_500_000_000
    assert span.parent.span_id == session.context.span_id
    assert span.status.status_code != StatusCode.ERROR
    assert {link.context.span_id for link in span.links} == {
        tasks["t1"].context.span_id,
        tasks["t2"].context.span_id,
    }  # the unknown task id is skipped
    assert dict(span.attributes) == {
        "session_id": "s",
        "backend": "vllm",
        "gen_ai.operation.name": "chat",
        "gen_ai.request.model": "m",
        "rhapsody.inference.batch.id": 1,
        "rhapsody.inference.batch.size": 3,
        "rhapsody.inference.batch.requests": 2,
        "rhapsody.inference.batch.trigger": "deadline",
        "rhapsody.inference.dragon.model.duration": 2.0,
        "rhapsody.inference.dragon.worker.host": "node7",
    }


async def test_the_number_of_links_is_capped(monkeypatch):
    # The SDK's own default limit is also 128, so the cap is lowered to see our own cut.
    monkeypatch.setattr(inference, "_MAX_LINKS", 3)
    assert inference._MAX_LINKS != 128
    manager, backend = await _manager()
    ids = [f"t{i}" for i in range(5)]
    for task_id in ids:
        manager.emit(make_event(TaskCreated, session_id="s", backend="vllm", task_id=task_id))
    await manager._queue.join()
    _started(backend, task_ids=ids, batch_requests=5)
    _ended(backend, batch_requests=5)
    await manager._queue.join()
    await manager.stop()

    (span,) = _batch_spans(manager)
    assert len(span.links) == 3
    assert dict(span.attributes)["rhapsody.inference.batch.requests"] == 5  # the real count


def test_the_link_cap_matches_the_sdk_default():
    assert inference._MAX_LINKS == 128


async def test_timed_out_prompts_make_the_span_an_error():
    manager, backend = await _manager()
    _started(backend)
    _ended(backend, timed_out_prompts=2)
    await manager._queue.join()
    await manager.stop()

    (span,) = _batch_spans(manager)
    attrs = dict(span.attributes)
    assert span.status.status_code == StatusCode.ERROR
    assert attrs["error.type"] == "TimeoutError"
    assert attrs["rhapsody.inference.timed_out_prompts"] == 2


async def test_a_batch_that_never_ends_is_closed_by_the_next_batch_and_by_stop():
    manager, backend = await _manager()
    _started(backend, batch_id=1, t=10.0)
    _started(backend, batch_id=2, t=20.0)  # batch 1 never ended
    _ended(backend, batch_id=2, t=21.0)
    _started(backend, batch_id=3, t=30.0)  # batch 3 never ends either
    await manager._queue.join()
    await manager.stop()

    by_id = {dict(s.attributes)["rhapsody.inference.batch.id"]: s for s in _batch_spans(manager)}
    assert set(by_id) == {1, 2, 3}
    assert by_id[1].status.status_code == StatusCode.ERROR
    assert by_id[1].status.description == "batch did not finish"
    assert by_id[2].status.status_code != StatusCode.ERROR
    assert by_id[3].status.status_code == StatusCode.ERROR


async def test_an_end_before_the_start_never_gives_a_negative_span():
    manager, backend = await _manager()
    _started(backend, t=100.0)
    _ended(backend, t=99.0)  # clock step backwards
    await manager._queue.join()
    await manager.stop()
    (span,) = _batch_spans(manager)
    assert span.end_time >= span.start_time


# ------------------------------------------------------------------- task spans


async def test_per_task_facts_land_on_the_open_task_span():
    manager, backend = await _manager()
    await _create_task(manager, "t1")
    await _create_task(manager, "t2")
    _started(backend, task_ids=["t1", "t2"])
    _ended(
        backend,
        rows=[_row("t1", prompts=2, queue=0.25), _row("t2", prompts=1, queue=0.5, timed_out=1)],
        timed_out_prompts=1,
    )
    await manager._queue.join()
    for task_id in ("t1", "t2"):
        manager.emit(make_event(TaskCompleted, session_id="s", backend="vllm", task_id=task_id))
    await manager.stop()

    tasks = {
        dict(s.attributes)["task_id"]: dict(s.attributes)
        for s in _spans(manager)
        if s.name == "task"
    }
    assert tasks["t1"]["gen_ai.request.model"] == "m"
    assert tasks["t1"]["rhapsody.inference.batch.id"] == 1
    assert tasks["t1"]["rhapsody.inference.prompt_count"] == 2
    assert tasks["t1"]["rhapsody.inference.request.queue.duration"] == 0.25
    assert "error.type" not in tasks["t1"]
    assert tasks["t2"]["error.type"] == "TimeoutError"
    assert tasks["t2"]["rhapsody.inference.timed_out_prompts"] == 1


async def test_missing_values_are_skipped_not_set_to_none():
    manager, backend = await _manager()
    await _create_task(manager, "t1")
    _started(backend, task_ids=["t1"], model=None)
    _ended(backend, model=None, rows=[_row("t1")])
    await manager._queue.join()
    manager.emit(make_event(TaskCompleted, session_id="s", backend="vllm", task_id="t1"))
    await manager.stop()
    task = next(dict(s.attributes) for s in _spans(manager) if s.name == "task")
    (batch,) = _batch_spans(manager)
    assert batch.name == "chat"  # no model, no "chat None"
    assert "gen_ai.request.model" not in task
    assert task["rhapsody.inference.prompt_count"] == 1


# ---------------------------------------------------------------------- metrics


async def test_metric_names_units_buckets_and_attributes():
    manager, backend = await _manager()
    _started(backend)
    _ended(
        backend,
        rows=[_row("t1", prompts=2, queue=0.5), _row(None, prompts=1, queue=1.5)],
        dragon_model_duration_s=2.0,
    )
    await manager._queue.join()
    await manager.stop()

    metrics = _metrics(manager)
    assert {m.name: m.unit for m in metrics.values()} == {
        "rhapsody.inference.request.queue.duration": "s",
        "rhapsody.inference.batch.duration": "s",
        "rhapsody.inference.model.duration": "s",
        "rhapsody.inference.requests": "{request}",
        "rhapsody.inference.prompts": "{prompt}",
        "rhapsody.inference.batches": "{batch}",
    }  # no timed-out counter without timeouts
    queue = metrics["rhapsody.inference.request.queue.duration"].data.data_points
    assert {dp.attributes["rhapsody.inference.request.source"]: dp.sum for dp in queue} == {
        "task": 0.5,
        "api": 1.5,
    }
    for dp in queue:
        assert list(dp.explicit_bounds) == pytest.approx(BUCKETS)
        assert set(dp.attributes) == {
            "session_id",
            "backend",
            "gen_ai.request.model",
            "rhapsody.inference.request.source",
        }
    assert len(BUCKETS) == 16

    def by_source(name):
        points = metrics[name].data.data_points
        return {dp.attributes["rhapsody.inference.request.source"]: dp.value for dp in points}

    assert by_source("rhapsody.inference.requests") == {"task": 1, "api": 1}
    assert by_source("rhapsody.inference.prompts") == {"task": 2, "api": 1}
    (batches,) = metrics["rhapsody.inference.batches"].data.data_points
    assert batches.attributes["rhapsody.inference.batch.trigger"] == "deadline"
    (model,) = metrics["rhapsody.inference.model.duration"].data.data_points
    assert model.sum == 2.0


async def test_timed_out_prompts_are_counted_per_source():
    manager, backend = await _manager()
    _started(backend)
    _ended(backend, rows=[_row("t1", timed_out=1), _row(None, prompts=2, timed_out=2)])
    await manager._queue.join()
    await manager.stop()
    points = _metrics(manager)["rhapsody.inference.timed_out.prompts"].data.data_points
    assert {dp.attributes["rhapsody.inference.request.source"]: dp.value for dp in points} == {
        "task": 1,
        "api": 2,
    }


@pytest.mark.parametrize("model_time", [0, 0.0, None])
async def test_the_model_duration_is_not_recorded_when_it_is_zero_or_absent(model_time):
    manager, backend = await _manager()
    _started(backend)
    extra = {} if model_time is None else {"dragon_model_duration_s": model_time}
    _ended(backend, rows=[_row("t1")], **extra)
    await manager._queue.join()
    await manager.stop()
    assert "rhapsody.inference.model.duration" not in _metrics(manager)


async def test_an_end_without_a_start_records_metrics_but_no_span():
    manager, backend = await _manager()
    _ended(backend, rows=[_row("t1")])
    await manager._queue.join()
    await manager.stop()
    assert _batch_spans(manager) == []
    assert "rhapsody.inference.batches" in _metrics(manager)


@pytest.mark.parametrize(
    "bad",
    [
        {"task_id": "t1"},  # fields missing
        "not a row",
        {"task_id": None, "prompt_count": "x", "queue_duration_s": 1.0},  # not a number
        {"task_id": None, "prompt_count": 1, "queue_duration_s": None},
        {"task_id": None, "prompt_count": 1, "queue_duration_s": 1.0, "timed_out_prompts": "y"},
        {"task_id": None, "prompt_count": True, "queue_duration_s": 1.0},  # a bool is no number
    ],
)
async def test_a_malformed_row_is_skipped_as_a_whole(bad):
    manager, backend = await _manager()
    _started(backend)
    _ended(backend, rows=[bad, _row("t2", prompts=3, queue=0.5)])
    await manager._queue.join()
    await manager.stop()

    metrics = _metrics(manager)

    def total(name):
        return sum(dp.value for dp in metrics[name].data.data_points)

    # Only the good row counts, in every instrument: nothing is half recorded.
    assert total("rhapsody.inference.requests") == 1
    assert total("rhapsody.inference.prompts") == 3
    queue = metrics["rhapsody.inference.request.queue.duration"].data.data_points
    assert sum(dp.count for dp in queue) == 1
    assert sum(dp.sum for dp in queue) == 0.5


# ---------------------------------------------------------------- host contract


async def test_other_events_of_the_backend_are_ignored(caplog):
    manager, backend = await _manager()
    with caplog.at_level(logging.WARNING):
        await _create_task(manager, "t1")
        backend.telemetry.emit("rhapsody.other.Thing", {"x": 1})
        backend.telemetry.emit("rhapsody.inference.Unknown", {"x": 1})
        await manager._queue.join()
    await manager.stop()
    assert "failed on" not in caplog.text
    assert _batch_spans(manager) == []


async def test_a_restart_creates_fresh_instruments_and_state():
    manager, backend = await _manager()
    _started(backend, batch_id=1)
    await manager._queue.join()
    await manager.stop()  # the open span is ended here

    await manager.start()
    _started(backend, batch_id=1, t=200.0)
    _ended(backend, batch_id=1, t=201.0, rows=[_row("t1")])
    await manager._queue.join()
    assert "rhapsody.inference.batches" in _metrics(manager)  # the new meter records
    await manager.stop()
    ended = [s for s in _batch_spans(manager) if s.end_time >= 201_000_000_000]
    assert ended and ended[-1].status.status_code != StatusCode.ERROR
