"""The inference telemetry example and its plot script, run offline against the fake Dragon
pipeline.

``examples/telemetry/01-inference-telemetry.py`` prints what telemetry recorded for a vLLM run, and
``01-inference-telemetry-plot.py`` draws the saved JSONL. The launch part of the example needs
Dragon's launcher and a GPU, but ``summarize()`` and the plot script only read telemetry, so they run
here on a fake pipeline and fail if the fields they read are renamed.
"""

import glob
import importlib.util
import re
import shutil
from pathlib import Path

import pytest

pytest.importorskip("opentelemetry", reason="opentelemetry-sdk not installed")
pytest.importorskip("dragon.ai.inference", reason="the fake-Dragon backend needs Dragon")

from fake_dragon import FakePipeline
from fake_dragon import running_backend

EXAMPLES = Path(__file__).resolve().parents[3] / "examples" / "telemetry"
EXAMPLE = EXAMPLES / "01-inference-telemetry.py"
PLOT = EXAMPLES / "01-inference-telemetry-plot.py"


PROMPT_MARKER = "MARKER-PROMPT-TEXT"


def _load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _respond(prompt):
    return {
        "assistant": "an answer",
        "model_inference_latency": 0.001,
        "hostname": "node7",
        "batch_size": 1,
    }


async def _run(monkeypatch, tmp_path, n_tasks=9):
    """Run n_tasks AITasks with telemetry on; return (telemetry, events, jsonl path)."""
    from rhapsody.api import AITask
    from rhapsody.api import Session

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
        tasks = [
            AITask(prompt=[f"{PROMPT_MARKER} {i}", f"{PROMPT_MARKER} again"], backend=backend.name)
            for i in range(n_tasks)
        ]
        await session.submit_tasks(tasks)
        await session.wait_tasks(tasks, timeout=60)
        await telemetry.stop()
    return telemetry, events, sorted(glob.glob(str(tmp_path / "*.telemetry.jsonl")))[-1]


async def test_summarize_describes_every_batch_task_and_metric(monkeypatch, tmp_path):
    example = _load(EXAMPLE, "inference_example")
    telemetry, events, _ = await _run(monkeypatch, tmp_path)
    ended = [e for e in events if e.event_type == example.PREFIX + "BatchDone"]

    lines = example.summarize(telemetry, events)
    text = "\n".join(lines)

    assert lines[0] == f"{len(ended)} batches (one span each):"
    batch_lines = [ln for ln in lines if ln.startswith("  ") and "trigger=" in ln]
    assert len(batch_lines) == len(ended) >= 3
    # Every field of a batch line: id, requests, prompts, trigger, duration, model time, host.
    batch_line = re.compile(
        r"  \d+: \d+ requests, \d+ prompts, trigger=(size|deadline), \d+ ms, "
        r"model \d+\.\d\d s, host node7"
    )
    assert all(batch_line.fullmatch(ln) for ln in batch_lines), batch_lines
    assert sum("trigger=size" in ln for ln in batch_lines) >= 1
    assert "9 task spans with inference attributes" in text
    task_line = re.compile(r"  task\.\d+: batch \d+, 2 prompts, queue wait \d+\.\d{3} s")
    assert len([ln for ln in lines if ln.startswith("  task.")]) == 3
    assert all(task_line.fullmatch(ln) for ln in lines if ln.startswith("  task."))
    assert f"rhapsody.inference.batches: {len(ended)}" in text
    assert "rhapsody.inference.request.queue.duration: 9 samples" in text
    assert f"events seen: {len(ended)} BatchStarted, {len(ended)} BatchDone, 0 BatchFailed" in text


async def test_summarize_never_prints_prompt_text(monkeypatch, tmp_path):
    example = _load(EXAMPLE, "inference_example")
    telemetry, events, _ = await _run(monkeypatch, tmp_path)
    assert PROMPT_MARKER not in "\n".join(example.summarize(telemetry, events))


async def test_the_plot_script_draws_the_plots_from_a_saved_run(monkeypatch, tmp_path, capsys):
    """The plot script reads a run directory holding only the JSONL the example saves."""
    pytest.importorskip("matplotlib")
    _, _, jsonl = await _run(monkeypatch, tmp_path)
    run = tmp_path / "runs" / "example-run"
    run.mkdir(parents=True)
    shutil.copy(jsonl, run)
    plot = _load(PLOT, "inference_plot")

    assert plot.main([str(tmp_path / "runs"), "--out", str(tmp_path / "plots")]) == 0
    drawn = {p.name[:2] for p in (tmp_path / "plots" / "example-run").glob("*.png")}
    # Not drawn here: 13 needs resource samples and 16 needs timeouts.
    assert drawn == {f"{n:02d}" for n in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 15)}
    shown = capsys.readouterr().out
    assert "FAILED" not in shown and "Traceback" not in shown  # a plot that raised is reported
