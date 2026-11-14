"""The cluster verification script, tested offline against the fake Dragon pipeline.

The script ``examples/telemetry/01-inference-telemetry.py`` checks a finished inference run. Here the
same workload and the same checks run against the fake pipeline, on healthy data and on deliberately
broken data, so that most mistakes in the checks show up before the script meets a GPU. The test does
not call ``main()`` (which needs Dragon's launcher) and has no ResourceUpdate data from a real node.
"""

import glob
import importlib.util
from pathlib import Path

import pytest

pytest.importorskip("opentelemetry", reason="opentelemetry-sdk not installed")
pytest.importorskip("dragon.ai.inference", reason="the fake-Dragon backend needs Dragon")

from fake_dragon import FakePipeline
from fake_dragon import running_backend

SCRIPT = (
    Path(__file__).resolve().parents[3] / "examples" / "telemetry" / "01-inference-telemetry.py"
)

pytestmark = pytest.mark.skipif(not SCRIPT.exists(), reason="the examples are not installed")


def _load_kit():
    spec = importlib.util.spec_from_file_location("inference_kit", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _respond(prompt):
    # Latency far below the batch time, like a real fast model, so "model <= batch" holds.
    return {
        "assistant": f"a long enough answer for the privacy scan to see, to: {prompt[:10]}",
        "model_inference_latency": 0.001,
        "hostname": "node7",
        "batch_size": 1,
    }


async def _healthy_run(monkeypatch, tmp_path):
    from rhapsody.api import Session

    kit = _load_kit()
    async with running_backend(
        monkeypatch, pipeline=FakePipeline(respond=_respond), max_batch_size=4
    ) as (backend, _states):
        session = Session(work_dir=str(tmp_path / "work"))
        session.add_backend(backend)
        telemetry = await session.start_telemetry(
            checkpoint_path=str(tmp_path), resource_poll_interval=3600
        )
        events = []
        telemetry.subscribe(events.append)
        workload = await kit.run_workload(session, backend, 7, 2, 3)
        await telemetry.stop()
    otlp = tmp_path / "inference_telemetry.otlp.json"
    telemetry.export_as_otlp(str(otlp))
    jsonl = sorted(glob.glob(str(tmp_path / "*.telemetry.jsonl")))[-1]
    expected = {**workload, "max_batch_size": 4, "timeouts": False}
    return kit, telemetry, events, expected, jsonl, str(otlp)


def _statuses(report):
    return {c["name"]: c["status"] for c in report.checks}


async def test_a_healthy_run_passes_every_check(monkeypatch, tmp_path):
    kit, telemetry, events, expected, jsonl, otlp = await _healthy_run(monkeypatch, tmp_path)
    assert expected["workload_errors"] == []
    report = kit.verify(telemetry, events, expected, jsonl_path=jsonl, otlp_path=otlp)
    print(report.table())  # shown by pytest on failure, and with -s
    failed = [c for c in report.checks if c["status"] == "FAIL"]
    assert failed == [], failed
    assert report.stats["batches"] >= 3
    assert report.stats["requests"] == 7 + 3
    assert report.stats["worker_hosts"] == ["node7"]
    # No ResourceUpdate in the fake run: the host membership is reported, not failed.
    assert any(c["name"].startswith("hosts:") for c in report.checks)


async def test_a_dropped_batch_span_fails(monkeypatch, tmp_path):
    kit, telemetry, events, expected, jsonl, otlp = await _healthy_run(monkeypatch, tmp_path)

    class Dropped:
        def __getattr__(self, name):
            return getattr(telemetry, name)

        def read_traces(self):
            spans = list(telemetry.read_traces())
            first = next(s for s in spans if s.name.startswith("chat"))
            return [s for s in spans if s is not first]

    report = kit.verify(Dropped(), events, expected, jsonl_path=jsonl, otlp_path=otlp)
    assert _statuses(report)["spans: one chat span per batch"] == "FAIL"


async def test_a_missing_end_event_fails(monkeypatch, tmp_path):
    kit, telemetry, events, expected, jsonl, otlp = await _healthy_run(monkeypatch, tmp_path)
    last_end = max(i for i, e in enumerate(events) if e.event_type == kit.ENDED)
    broken = events[:last_end] + events[last_end + 1 :]
    report = kit.verify(telemetry, broken, expected, jsonl_path=jsonl, otlp_path=otlp)
    assert _statuses(report)["events: BatchStarted and BatchEnded pair up"] == "FAIL"


async def test_prompt_text_in_the_files_fails(monkeypatch, tmp_path):
    kit, telemetry, events, expected, jsonl, otlp = await _healthy_run(monkeypatch, tmp_path)
    with open(jsonl, "a") as handle:
        handle.write('{"leak": "' + expected["canaries"][0] + '"}\n')
    report = kit.verify(telemetry, events, expected, jsonl_path=jsonl, otlp_path=otlp)
    assert _statuses(report)["privacy: no prompt or response text in the telemetry files"] == "FAIL"


async def test_a_wrong_counter_fails(monkeypatch, tmp_path):
    kit, telemetry, events, expected, jsonl, otlp = await _healthy_run(monkeypatch, tmp_path)
    real = kit._inference_metrics

    def without_batches(t):
        data = real(t)
        data.pop("rhapsody.inference.batches", None)
        return data

    monkeypatch.setattr(kit, "_inference_metrics", without_batches)
    report = kit.verify(telemetry, events, expected, jsonl_path=jsonl, otlp_path=otlp)
    assert _statuses(report)["metrics: batches == batch events"] == "FAIL"


async def test_a_timeout_run_checks_that_events_tasks_and_spans_agree(monkeypatch, tmp_path):
    from rhapsody.api import Session

    kit = _load_kit()
    async with running_backend(
        monkeypatch, timeout_s=0.1, pipeline=FakePipeline(answer=3, respond=_respond)
    ) as (backend, _states):
        session = Session(work_dir=str(tmp_path / "work"))
        session.add_backend(backend)
        telemetry = await session.start_telemetry(
            checkpoint_path=str(tmp_path), resource_poll_interval=3600
        )
        events = []
        telemetry.subscribe(events.append)
        workload = await kit.run_workload(session, backend, 4, 1, 0)
        await telemetry.stop()
    otlp = tmp_path / "inference_telemetry.otlp.json"
    telemetry.export_as_otlp(str(otlp))
    jsonl = sorted(glob.glob(str(tmp_path / "*.telemetry.jsonl")))[-1]
    expected = {**workload, "max_batch_size": 1024, "timeouts": True}
    report = kit.verify(telemetry, events, expected, jsonl_path=jsonl, otlp_path=str(otlp))
    failed = [c for c in report.checks if c["status"] == "FAIL"]
    assert failed == [], failed
    statuses = _statuses(report)
    assert statuses["timeouts: a timeout occurred"] == "PASS"
    assert statuses["timeouts: FAILED tasks == tasks with timed-out prompts"] == "PASS"
    assert report.stats["timed_out_prompts"] == 1  # 4 prompts, 3 answered


async def test_the_host_check_passes_for_an_allocation_node_and_fails_for_a_stranger(
    monkeypatch, tmp_path
):
    kit, telemetry, events, expected, jsonl, otlp = await _healthy_run(monkeypatch, tmp_path)
    name = "hosts: every Dragon worker host is a node of the allocation"
    ok = kit.verify(telemetry, events, expected, jsonl, otlp, known_hosts={"node7.cluster"})
    assert _statuses(ok)[name] == "PASS"  # the short name matches the fully qualified one
    bad = kit.verify(telemetry, events, expected, jsonl, otlp, known_hosts={"othernode"})
    assert _statuses(bad)[name] == "FAIL"


async def test_a_model_time_above_the_batch_time_fails(monkeypatch, tmp_path):
    kit, telemetry, events, expected, jsonl, otlp = await _healthy_run(monkeypatch, tmp_path)
    slow = []
    for event in events:
        if event.event_type == kit.ENDED:
            attrs = {**event.attributes, "dragon_model_duration_s": 60.0}
            event = type(event)(**{**event.__dict__, "attributes": attrs})
        slow.append(event)
    report = kit.verify(telemetry, slow, expected, jsonl_path=jsonl, otlp_path=otlp)
    name = "dragon: model time <= batch time (+0.015 s rounding), when reported"
    assert _statuses(report)[name] == "FAIL"


async def test_a_model_time_of_zero_counts_as_not_reported(monkeypatch, tmp_path):
    """Dragon's error response sends 0: nothing is recorded for it, and nothing fails."""
    from rhapsody.api import Session

    kit = _load_kit()

    def zero_latency(prompt):
        return {**_respond(prompt), "model_inference_latency": 0}

    async with running_backend(monkeypatch, pipeline=FakePipeline(respond=zero_latency)) as (
        backend,
        _states,
    ):
        session = Session(work_dir=str(tmp_path / "work"))
        session.add_backend(backend)
        telemetry = await session.start_telemetry(
            checkpoint_path=str(tmp_path), resource_poll_interval=3600
        )
        events = []
        telemetry.subscribe(events.append)
        workload = await kit.run_workload(session, backend, 3, 1, 1)
        await telemetry.stop()
    jsonl = sorted(glob.glob(str(tmp_path / "*.telemetry.jsonl")))[-1]
    expected = {**workload, "max_batch_size": 1024, "timeouts": False}
    report = kit.verify(telemetry, events, expected, jsonl_path=jsonl)
    assert [c for c in report.checks if c["status"] == "FAIL"] == []


def test_the_newest_checkpoint_file_is_the_one_used(tmp_path):
    import os

    kit = _load_kit()
    assert kit._latest_jsonl(tmp_path) is None
    older = tmp_path / "z.0.telemetry.jsonl"  # sorts later by name, but is older
    newer = tmp_path / "a.1.telemetry.jsonl"
    older.write_text("x")
    newer.write_text("y")
    os.utime(older, (1, 1))
    assert Path(kit._latest_jsonl(tmp_path)).name == "a.1.telemetry.jsonl"
