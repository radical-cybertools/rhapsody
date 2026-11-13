"""The telemetry reference and the code must not drift apart.

What these tests cover: the limitation ids (code comments vs the table), the inference instrument
names, types, units and bucket bounds, the inference event names and payload fields, and that the
extension-guide examples compile and the instrumentation skeleton runs.

What they do not cover: the prose, the attribute lists of the spans, the per-instrument metric
attributes beyond what the metric test reads, and the task-metric table.
"""

import re
from pathlib import Path

import pytest

pytest.importorskip("opentelemetry", reason="opentelemetry-sdk not installed")

from rhapsody.telemetry.instrumentation import registry
from rhapsody.telemetry.manager import TelemetryManager

ROOT = Path(__file__).resolve().parents[3]
DOCS = ROOT / "docs" / "telemetry" / "reference.md"
SRC = ROOT / "src" / "rhapsody"

# Limitation ids whose code comment lives in another repository (radical.asyncflow).
ASYNCFLOW_OWNED = {"L9"}

pytestmark = pytest.mark.skipif(not DOCS.exists(), reason="the docs are not installed")


def _section(text: str, start: str, end: str | None = None) -> str:
    """Text from the heading ``start`` up to the next heading that begins ``end``."""
    begin = text.index(start)
    stop = text.index(end, begin + len(start)) if end else len(text)
    return text[begin:stop]


def _docs() -> str:
    return DOCS.read_text()


def _first_python_block(section: str) -> str:
    match = re.search(r"```python\n(.*?)```", section, re.DOTALL)
    assert match, "no python block in the section"
    return match.group(1)


# ------------------------------------------------------------------ limitation ids


def test_the_limitation_ids_in_the_code_match_the_table():
    code_ids = set()
    for path in SRC.rglob("*.py"):
        code_ids.update(re.findall(r"TELEMETRY-LIMITATION\((L\d+)\)", path.read_text()))
    table_ids = set(re.findall(r"^\| (L\d+) \|", _docs(), re.MULTILINE))
    assert ASYNCFLOW_OWNED <= table_ids
    assert code_ids == table_ids - ASYNCFLOW_OWNED


# --------------------------------------------------------------- inference metrics


class _Backend:
    telemetry_domain = "inference"

    def submit_tasks(self, tasks):
        pass


async def _record_inference_metrics():
    manager = TelemetryManager(session_id="s")
    backend = _Backend()
    manager.attach_backend(backend, session_id="s", backend_name="vllm", interval=60)
    await manager.start()
    backend.telemetry.emit(
        "rhapsody.inference.BatchStarted",
        {"batch_id": 1, "model": "m", "batch_prompts": 2, "batch_requests": 2,
         "batch_trigger": "size", "task_ids": []},
    )  # fmt: skip
    backend.telemetry.emit(
        "rhapsody.inference.BatchEnded",
        {
            "batch_id": 1, "model": "m", "batch_trigger": "size", "batch_prompts": 2,
            "batch_requests": 2, "batch_duration_s": 1.0, "timed_out_prompts": 1,
            "dragon_model_duration_s": 0.5,
            "requests": [
                {"task_id": "t", "prompt_count": 1, "queue_duration_s": 0.1,
                 "timed_out_prompts": 1},
                {"task_id": None, "prompt_count": 1, "queue_duration_s": 0.2,
                 "timed_out_prompts": 0},
            ],
        },
    )  # fmt: skip
    await manager._queue.join()
    await manager.stop()
    found = {}
    for rm in manager.read_metrics().resource_metrics:
        for sm in rm.scope_metrics:
            if sm.scope.name == "rhapsody.instrumentation.inference":
                for metric in sm.metrics:
                    found[metric.name] = metric
    return found


async def test_the_inference_instruments_match_the_docs():
    found = await _record_inference_metrics()
    section = _section(_docs(), "## Inference telemetry", "## OTel span structure")
    rows = re.findall(
        r"^\| `(rhapsody\.inference\.[\w.]+)` \| (\w+) \| `([^`]+)` \|", section, re.MULTILINE
    )
    documented = {name: (kind, unit) for name, kind, unit in rows}
    actual = {}
    for name, metric in found.items():
        kind = "Histogram" if hasattr(metric.data.data_points[0], "bucket_counts") else "Counter"
        actual[name] = (kind, metric.unit)
    assert documented == actual

    bounds = [
        dp.explicit_bounds for dp in found["rhapsody.inference.batch.duration"].data.data_points
    ]
    assert list(bounds[0]) == pytest.approx([0.01 * 2**k for k in range(16)])
    assert "0.01, 0.02, 0.04 ... 327.68" in section
    assert "16 bounds" in section


# ------------------------------------------------------------------- event payloads


def _documented_fields(section: str, event: str) -> set[str]:
    """Backticked field names in the first column of the table that follows ``event``."""
    start = section.index(f"`rhapsody.inference.{event}`")
    table = section[start:].split("\n\n")[1]  # the table after the sentence
    fields = set()
    for line in table.splitlines():
        if line.startswith("|") and not line.startswith("|---") and "Field" not in line:
            first_cell = line.split("|")[1]
            fields.update(re.findall(r"`([a-z_]+)`", first_cell))
    return fields


async def test_the_inference_events_match_the_docs(monkeypatch):
    pytest.importorskip("dragon.ai.inference", reason="the fake-Dragon backend needs Dragon")
    import asyncio

    from fake_dragon import running_backend

    emitted = []

    class Recorder:
        enabled = True

        def emit(self, name, attributes=None, *, event_time=None, task_id=None):
            emitted.append((name, attributes))

    async with running_backend(monkeypatch) as (backend, states):
        backend.telemetry = Recorder()
        await backend.submit_tasks([{"uid": "t1", "prompt": "a"}])
        async with asyncio.timeout(5):
            while not any(state[1] == "DONE" for state in states):
                await asyncio.sleep(0.005)

    section = _section(_docs(), "### Events", "### Metrics")
    names = [name for name, _ in emitted]
    assert names == ["rhapsody.inference.BatchStarted", "rhapsody.inference.BatchEnded"]
    for name, payload in emitted:
        event = name.rsplit(".", 1)[1]
        assert set(payload) == _documented_fields(section, event), event
    ended = emitted[1][1]
    requests_row = next(line for line in section.splitlines() if line.startswith("| `requests`"))
    for key in ended["requests"][0]:
        assert f"`{key}`" in requests_row


# ----------------------------------------------------------------- extension guide


def test_the_emit_example_compiles():
    section = _section(_docs(), "### Emitting from a backend", "### Writing an instrumentation")
    compile(_first_python_block(section), "<reference.md emit example>", "exec")


async def test_the_instrumentation_skeleton_runs():
    section = _section(_docs(), "### Writing an instrumentation", "\n---\n")
    code = _first_python_block(section)
    saved = dict(registry._REGISTRY)
    try:
        exec(compile(code, "<reference.md skeleton>", "exec"), {})
        manager = TelemetryManager(session_id="s")

        class Backend:
            telemetry_domain = "myservice"

            def submit_tasks(self, tasks):
                pass

        backend = Backend()
        manager.attach_backend(backend, session_id="s", backend_name="svc", interval=60)
        await manager.start()
        backend.telemetry.emit("rhapsody.myservice.RequestServed", {"count": 3})
        backend.telemetry.emit("rhapsody.other.Thing", {"count": 99})  # not owned: ignored
        await manager._queue.join()
        await manager.stop()
        metrics = {
            m.name: m
            for rm in manager.read_metrics().resource_metrics
            for sm in rm.scope_metrics
            if sm.scope.name == "rhapsody.instrumentation.myservice"
            for m in sm.metrics
        }
        served = metrics["myservice.requests.served"]
        assert served.unit == "{request}"
        assert sum(dp.value for dp in served.data.data_points) == 3
    finally:
        registry._REGISTRY.clear()
        registry._REGISTRY.update(saved)
