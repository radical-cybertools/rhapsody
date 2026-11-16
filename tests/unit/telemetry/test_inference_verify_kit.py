"""The cluster test checks, tested offline against the fake Dragon pipeline.

``tests/integration/test-hpc/dragon/telemetry_checks.py`` holds the checks the cluster tests
(``test_ai_telemetry.py``) run on a finished inference run. Here the
same workload and the same checks run against the fake pipeline, on healthy data and on deliberately
broken data, so that most mistakes in the checks show up before they meet a GPU. The fixtures that
create the real backend need Dragon's launcher and are not covered, and there is no ResourceUpdate
data from a real node.
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
    Path(__file__).resolve().parents[3]
    / "tests"
    / "integration"
    / "test-hpc"
    / "dragon"
    / "telemetry_checks.py"
)


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
    assert kit.latest_jsonl(tmp_path) is None
    older = tmp_path / "z.0.telemetry.jsonl"  # sorts later by name, but is older
    newer = tmp_path / "a.1.telemetry.jsonl"
    older.write_text("x")
    newer.write_text("y")
    os.utime(older, (1, 1))
    assert Path(kit.latest_jsonl(tmp_path)).name == "a.1.telemetry.jsonl"


def _record(name, message="boom", level=None):
    import logging

    return logging.LogRecord(name, level or logging.WARNING, __file__, 1, message, None, None)


async def test_the_log_checks_pass_clean_fail_on_a_telemetry_warning_and_warn_on_others(
    monkeypatch, tmp_path
):
    kit, telemetry, events, expected, jsonl, otlp = await _healthy_run(monkeypatch, tmp_path)
    ours = "logs: no warning or error from rhapsody.telemetry"
    others = "logs: no other rhapsody warning or error"

    clean = _statuses(kit.verify(telemetry, events, expected, jsonl, otlp, log_records=[]))
    assert (clean[ours], clean[others]) == ("PASS", "PASS")

    failing = [_record("rhapsody.telemetry.instrumentation.inference", "on_event failed")]
    bad = _statuses(kit.verify(telemetry, events, expected, jsonl, otlp, log_records=failing))
    assert bad[ours] == "FAIL"

    backend = [_record("rhapsody.backends.ai.vllm", "Batch processor error")]
    warn = _statuses(kit.verify(telemetry, events, expected, jsonl, otlp, log_records=backend))
    assert (warn[ours], warn[others]) == ("PASS", "WARN")

    # Not captured (the offline tests of the other checks): no log line at all.
    assert not any(
        c["name"].startswith("logs:") for c in kit.verify(telemetry, events, expected).checks
    )


async def test_string_and_list_prompts_are_both_in_the_workload(monkeypatch, tmp_path):
    kit, telemetry, events, expected, jsonl, otlp = await _healthy_run(monkeypatch, tmp_path)
    ends = [e for e in events if e.event_type == kit.ENDED]
    counts = [r["prompt_count"] for e in ends for r in e.attributes["requests"] if r.get("task_id")]
    assert 1 in counts and 2 in counts  # task 3 is a plain string; the others hold two prompts


async def test_http_requests_count_as_api_requests(monkeypatch, tmp_path):
    import socket

    from rhapsody.api import Session

    kit = _load_kit()
    async with running_backend(
        monkeypatch, pipeline=FakePipeline(respond=_respond), max_batch_size=4
    ) as (backend, _states):
        with socket.socket() as sock:  # a free port
            sock.bind(("127.0.0.1", 0))
            backend.port = sock.getsockname()[1]
        backend.hostname = "127.0.0.1"
        backend.use_service = True
        await backend._start_http_server()
        try:
            session = Session(work_dir=str(tmp_path / "work"))
            session.add_backend(backend)
            telemetry = await session.start_telemetry(
                checkpoint_path=str(tmp_path), resource_poll_interval=3600
            )
            events = []
            telemetry.subscribe(events.append)
            workload = await kit.run_workload(session, backend, 4, 2, 1, http_requests=4)
            await telemetry.stop()
        finally:
            await backend.runner.cleanup()
    assert workload["workload_errors"] == []
    assert workload["api_requests"] == 5
    jsonl = sorted(glob.glob(str(tmp_path / "*.telemetry.jsonl")))[-1]
    expected = {**workload, "max_batch_size": 4, "timeouts": False}
    report = kit.verify(telemetry, events, expected, jsonl_path=jsonl)
    assert [c for c in report.checks if c["status"] == "FAIL"] == []
    assert report.stats["requests"] == 4 + 5


async def test_every_check_has_a_group(monkeypatch, tmp_path):
    kit, telemetry, events, expected, jsonl, otlp = await _healthy_run(monkeypatch, tmp_path)
    report = kit.verify(telemetry, events, expected, jsonl_path=jsonl, otlp_path=otlp)
    assert all(c["group"] == c["name"].split(":")[0] for c in report.checks)
    assert {"events", "spans", "task spans", "metrics", "dragon", "privacy", "workload"} <= {
        c["group"] for c in report.checks
    }
    assert report.failures() == []
    assert report.failures("privacy") == []


async def test_a_prompt_canary_and_a_sampled_response_both_fail_the_privacy_check(
    monkeypatch, tmp_path
):
    kit, telemetry, events, expected, jsonl, otlp = await _healthy_run(monkeypatch, tmp_path)
    name = "privacy: no prompt or response text in the telemetry files"
    response = next(c for c in expected["canaries"] if not c.startswith("CANARY-"))
    for leak in (next(c for c in expected["canaries"] if c.startswith("CANARY-")), response):
        copy = tmp_path / "leaky.jsonl"
        copy.write_text(Path(jsonl).read_text() + '{"leak": "' + leak + '"}\n')
        report = kit.verify(telemetry, events, expected, jsonl_path=str(copy), otlp_path=otlp)
        assert _statuses(report)[name] == "FAIL"
        assert report.failures("privacy")


async def test_request_errors_fail_the_workload_group_but_not_a_timeout_run(monkeypatch, tmp_path):
    kit, telemetry, events, expected, jsonl, otlp = await _healthy_run(monkeypatch, tmp_path)
    name = "workload: no request raised an error"
    broken = {**expected, "workload_errors": ["RuntimeError('boom')"]}
    report = kit.verify(telemetry, events, broken, jsonl_path=jsonl, otlp_path=otlp)
    assert _statuses(report)[name] == "FAIL"
    assert report.failures("workload")

    report = kit.verify(
        telemetry, events, {**broken, "timeouts": True}, jsonl_path=jsonl, otlp_path=otlp
    )
    assert name not in _statuses(report)  # errors are expected when prompts time out


def test_provenance_reports_each_key_on_its_own():
    kit = _load_kit()
    info = kit.provenance()
    assert set(info) == {
        "git_head",
        "git_status",
        "packages",
        "rhapsody_module",
        "rhapsody_install",
    }
    assert info["rhapsody_module"].endswith("__init__.py")


async def test_warm_up_counts_calls_and_survives_an_error(capsys):
    kit = _load_kit()

    class Backend:
        calls = 0

        async def generate(self, prompts, timeout):
            self.calls += 1
            if self.calls == 2:
                raise RuntimeError("boom")
            return ["fine"]

    backend = Backend()
    await kit.warm_up(backend, 3)
    shown = capsys.readouterr().out
    assert backend.calls == 3
    assert "warm-up call failed" in shown and "3 requests" in shown and "(1 failed)" in shown
    await kit.warm_up(backend, 0)
    assert backend.calls == 3


async def test_run_inference_telemetry_runs_checks_and_saves_the_report(monkeypatch, tmp_path):
    """The whole helper the cluster tests call: workload, one stop, checks, saved report."""
    import json

    kit = _load_kit()
    monkeypatch.chdir(tmp_path)
    out = tmp_path / "artifacts" / "module"
    async with running_backend(
        monkeypatch, pipeline=FakePipeline(respond=_respond), max_batch_size=4
    ) as (backend, _states):
        report, facts = await kit.run_inference_telemetry(
            backend,
            out,
            7,
            generate_calls=3,
            max_batch_size=4,
            known_hosts={"node7.cluster"},
            hardware={"nodes": 1, "gpus_per_node": 1},
        )
    assert report.failures() == []
    assert facts["wall_time_s"] > 0 and facts["workload"]["workload_errors"] == []
    saved = json.loads((out / "inference_telemetry_report.json").read_text())
    assert saved["hardware"] == {"nodes": 1, "gpus_per_node": 1}
    assert saved["wall_time_s"] == facts["wall_time_s"]
    assert {"git_head", "packages", "checks", "stats"} <= set(saved)
    assert list(out.glob("*.telemetry.jsonl")) and (out / "inference_telemetry.otlp.json").exists()


async def test_one_backend_serves_two_telemetry_sessions_in_a_row(monkeypatch, tmp_path):
    """The overhead benchmark alternates telemetry arms on one backend; each arm must be exact."""
    kit = _load_kit()
    monkeypatch.chdir(tmp_path)
    async with running_backend(
        monkeypatch, pipeline=FakePipeline(respond=_respond), max_batch_size=4
    ) as (backend, _states):
        first, _ = await kit.run_inference_telemetry(
            backend, tmp_path / "a", 7, generate_calls=2, max_batch_size=4
        )
        second, _ = await kit.run_inference_telemetry(
            backend, tmp_path / "b", 7, generate_calls=2, max_batch_size=4
        )
    for report in (first, second):
        assert report.failures() == []
        assert report.stats["batches"] == 3  # nothing of the first run leaks into the second
        assert report.stats["requests"] == 7 + 2


async def test_the_overhead_benchmark_arms_write_what_the_plot_script_reads(monkeypatch, tmp_path):
    import json

    path = SCRIPT.parents[1] / "benchmarks" / "telemetry_overhead.py"
    spec = importlib.util.spec_from_file_location("overhead", path)
    bench = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(bench)
    monkeypatch.chdir(tmp_path)
    async with running_backend(
        monkeypatch, pipeline=FakePipeline(respond=_respond), max_batch_size=4
    ) as (backend, _states):
        wall = await bench.run_off(backend, tmp_path / "off-1", 6)
    saved = json.loads((tmp_path / "off-1" / "inference_telemetry_report.json").read_text())
    assert saved["wall_time_s"] == wall > 0
    assert "mean" in bench._summary("off", [1.0, 3.0]) and "2.00" in bench._summary(
        "off", [1.0, 3.0]
    )


class _Span:
    """A finished span with some fields replaced; everything else reads through."""

    def __init__(self, span, **replaced):
        self._span = span
        self._replaced = replaced

    def __getattr__(self, name):
        if name in self._replaced:
            return self._replaced[name]
        return getattr(self._span, name)


def _is_chat(span):
    return span.name.startswith("chat")


def _with_spans(telemetry, change):
    class Telemetry:
        def __getattr__(self, name):
            return getattr(telemetry, name)

        def read_traces(self):
            return change(list(telemetry.read_traces()))

    return Telemetry()


def _replace_first(match, **replaced):
    def change(spans):
        first = next(s for s in spans if match(s))
        return [_Span(s, **replaced) if s is first else s for s in spans]

    return change


def _task_span_without_batch_id(spans):
    first = next(s for s in spans if s.name == "task" and s.attributes)
    attributes = {k: v for k, v in first.attributes.items() if k != "rhapsody.inference.batch.id"}
    return [_Span(s, attributes=attributes) if s is first else s for s in spans]


_SPAN_CASES = {
    "a duplicated batch span": (
        lambda spans: [*spans, next(s for s in spans if _is_chat(s))],
        "spans: one chat span per batch",
    ),
    "a batch span of the wrong kind": (
        _replace_first(_is_chat, kind=type("Kind", (), {"name": "SERVER"})()),
        "spans: kind CLIENT, ordered times, session parent, links = task requests (max 128)",
    ),
    "a batch span without a parent": (
        _replace_first(_is_chat, parent=None),
        "spans: kind CLIENT, ordered times, session parent, links = task requests (max 128)",
    ),
    "a batch span with a link too few": (
        _replace_first(lambda s: _is_chat(s) and len(s.links) > 1, links=()),
        "spans: kind CLIENT, ordered times, session parent, links = task requests (max 128)",
    ),
    "a task span without its batch id": (
        _task_span_without_batch_id,
        "task spans: batch id, prompt count, queue wait, error.type",
    ),
}


@pytest.mark.parametrize("case", sorted(_SPAN_CASES))
async def test_broken_spans_fail_the_matching_check(case, monkeypatch, tmp_path):
    kit, telemetry, events, expected, jsonl, otlp = await _healthy_run(monkeypatch, tmp_path)
    change, failing_check = _SPAN_CASES[case]
    report = kit.verify(
        _with_spans(telemetry, change), events, expected, jsonl_path=jsonl, otlp_path=otlp
    )
    assert _statuses(report)[failing_check] == "FAIL", case


_EXPECTED_CASES = {
    "an uncounted API request": (
        lambda e: {**e, "api_requests": e["api_requests"] + 1},
        "workload: the run saw every request it submitted",
    ),
    "a task that failed in a normal run": (
        lambda e: {**e, "failed_task_ids": ["task.000001"]},
        "workload: no task failed",
    ),
    "a request that raised": (
        lambda e: {**e, "workload_errors": ["RuntimeError('x')"]},
        "workload: no request raised an error",
    ),
}


@pytest.mark.parametrize("case", sorted(_EXPECTED_CASES))
async def test_a_workload_that_does_not_match_the_telemetry_fails(case, monkeypatch, tmp_path):
    kit, telemetry, events, expected, jsonl, otlp = await _healthy_run(monkeypatch, tmp_path)
    change, failing_check = _EXPECTED_CASES[case]
    report = kit.verify(telemetry, events, change(expected), jsonl_path=jsonl, otlp_path=otlp)
    assert _statuses(report)[failing_check] == "FAIL", case


async def test_prompt_text_in_the_otlp_file_fails_too(monkeypatch, tmp_path):
    kit, telemetry, events, expected, jsonl, otlp = await _healthy_run(monkeypatch, tmp_path)
    leaky = tmp_path / "leaky.otlp.json"
    leaky.write_text(Path(otlp).read_text() + expected["canaries"][0])
    report = kit.verify(telemetry, events, expected, jsonl_path=jsonl, otlp_path=str(leaky))
    assert _statuses(report)["privacy: no prompt or response text in the telemetry files"] == "FAIL"


async def test_a_missing_checkpoint_file_is_a_failure_of_the_run_helper(monkeypatch, tmp_path):
    kit = _load_kit()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(kit, "latest_jsonl", lambda out: None)
    async with running_backend(
        monkeypatch, pipeline=FakePipeline(respond=_respond), max_batch_size=4
    ) as (backend, _states):
        report, _ = await kit.run_inference_telemetry(
            backend, tmp_path / "out", 4, generate_calls=1, max_batch_size=4
        )
    assert _statuses(report)["events: a JSONL checkpoint was written"] == "FAIL"
    assert (
        (tmp_path / "out" / "inference_telemetry_table.txt")
        .read_text()
        .startswith(("PASS", "WARN", "FAIL"))
    )


async def test_a_crashing_check_still_saves_the_report(monkeypatch, tmp_path):
    kit = _load_kit()
    monkeypatch.chdir(tmp_path)

    def boom(*args, **kwargs):
        raise KeyError("batch_id")

    monkeypatch.setattr(kit, "verify", boom)
    async with running_backend(
        monkeypatch, pipeline=FakePipeline(respond=_respond), max_batch_size=4
    ) as (backend, _states):
        report, _ = await kit.run_inference_telemetry(
            backend, tmp_path / "out", 4, generate_calls=1, max_batch_size=4
        )
    assert any("the checks ran" in line for line in report.failures("events"))
    assert (tmp_path / "out" / "inference_telemetry_report.json").exists()


async def test_telemetry_is_stopped_when_the_workload_raises(monkeypatch, tmp_path):
    kit = _load_kit()
    monkeypatch.chdir(tmp_path)

    async def broken(*args, **kwargs):
        raise RuntimeError("workload failed")

    monkeypatch.setattr(kit, "run_workload", broken)
    async with running_backend(
        monkeypatch, pipeline=FakePipeline(respond=_respond), max_batch_size=4
    ) as (backend, _states):
        with pytest.raises(RuntimeError, match="workload failed"):
            await kit.run_inference_telemetry(backend, tmp_path / "out", 4, max_batch_size=4)
        assert backend.telemetry.enabled is False  # the manager was stopped, not left polling
