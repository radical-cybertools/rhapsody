"""TaskFailed carries the failure details: message, exit code, traceback or stderr.

They must reach the JSONL checkpoint (event and span records) and the task span, so a failure
can be debugged from telemetry after the run.
"""

import asyncio
import json
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor

import pytest

pytest.importorskip("opentelemetry", reason="opentelemetry-sdk not installed")

from rhapsody.api.session import Session
from rhapsody.api.task import ComputeTask
from rhapsody.backends.execution.concurrent import ConcurrentExecutionBackend
from rhapsody.telemetry import manager as manager_mod
from rhapsody.telemetry.manager import TelemetryManager

MARKER = manager_mod._TRUNCATION_MARKER


def _raise_bad_input(x):
    raise ValueError(f"bad input {x}")


def _caught(func, *args) -> BaseException:
    """Call ``func`` and return the exception it raised, with its traceback."""
    try:
        func(*args)
    except BaseException as e:  # noqa: BLE001
        return e
    raise AssertionError("expected an exception")


def _read_checkpoint(tmpdir: str) -> list[dict]:
    path = os.path.join(tmpdir, os.listdir(tmpdir)[0])
    with open(path) as f:
        return [json.loads(line) for line in f]


def _failed_event(rows: list[dict], task_id: str) -> dict:
    return next(
        r
        for r in rows
        if r["section"] == "event" and r["event_type"] == "TaskFailed" and r["task_id"] == task_id
    )


def _task_span(rows: list[dict], task_id: str) -> dict:
    return next(
        r
        for r in rows
        if r["section"] == "span" and r["name"] == "task" and r["attributes"]["task_id"] == task_id
    )


async def _fail(task: dict) -> tuple[dict, dict, TelemetryManager]:
    """Run ``task`` through RUNNING and FAILED; return its event and span records."""
    with tempfile.TemporaryDirectory() as tmpdir:
        m = TelemetryManager(session_id="fail-details", checkpoint_path=tmpdir)
        await m.start()
        m._on_task_state_change(task, "RUNNING")
        m._on_task_state_change(task, "FAILED")
        await asyncio.sleep(0.1)
        await m.stop()
        rows = _read_checkpoint(tmpdir)
    return _failed_event(rows, task["uid"]), _task_span(rows, task["uid"]), m


class TestTaskFailedDetails:
    async def test_function_task_that_raises(self):
        exc = _caught(_raise_bad_input, 3)
        # As the concurrent backend leaves a function task that raised.
        task = {
            "uid": "t-func",
            "backend": "concurrent",
            "function": _raise_bad_input,
            "exception": exc,
            "exit_code": 1,
            "stderr": str(exc),
        }
        event, span, _ = await _fail(task)

        attrs = event["attributes"]
        assert attrs["error_type"] == "ValueError"
        assert attrs["error_message"] == "bad input 3"
        assert attrs["exit_code"] == 1
        assert "in _raise_bad_input" in attrs["traceback"]
        assert attrs["traceback"].endswith("ValueError: bad input 3\n")
        assert "stderr" not in attrs  # it only repeats the message

        for key in ("error_message", "exit_code", "traceback"):
            assert span["attributes"][key] == attrs[key]
        assert span["status_code"] == "ERROR"
        assert span["status_description"] == "ValueError"

    async def test_executable_with_exit_code_and_stderr(self):
        task = {
            "uid": "t-exec",
            "backend": "concurrent",
            "executable": "/bin/sh",
            "exception": None,
            "exit_code": 3,
            "stderr": "oops: no such file\n",
        }
        event, span, _ = await _fail(task)

        attrs = event["attributes"]
        assert attrs["error_type"] == "unknown"
        assert attrs["exit_code"] == 3
        assert attrs["stderr"] == "oops: no such file\n"
        assert "error_message" not in attrs
        assert "traceback" not in attrs
        assert span["attributes"]["exit_code"] == 3
        assert span["attributes"]["stderr"] == "oops: no such file\n"
        assert span["status_description"] == "unknown"

    async def test_long_texts_keep_their_tail_within_the_caps(self):
        exc = _caught(_raise_bad_input, "x" * 10_000 + " END-OF-MESSAGE")
        task = {
            "uid": "t-long",
            "backend": "concurrent",
            "executable": "/bin/sh",
            "exception": exc,
            "exit_code": 2,
            "stderr": "e" * 10_000 + " END-OF-STDERR\n",
        }
        event, span, _ = await _fail(task)

        attrs = event["attributes"]
        caps = {
            "error_message": manager_mod._ERROR_MESSAGE_MAX_CHARS,
            "traceback": manager_mod._ERROR_OUTPUT_MAX_CHARS,
            "stderr": manager_mod._ERROR_OUTPUT_MAX_CHARS,
        }
        for key, cap in caps.items():
            assert len(attrs[key]) == cap, key
            assert attrs[key].startswith(MARKER), key
            assert span["attributes"][key] == attrs[key]
        assert attrs["error_message"].endswith(" END-OF-MESSAGE")
        assert attrs["traceback"].endswith(" END-OF-MESSAGE\n")
        assert attrs["stderr"].endswith(" END-OF-STDERR\n")
        # The span status stays short.
        assert span["status_description"] == "ValueError"

    async def test_string_exception(self):
        """Some backends store the exception as a plain string."""
        task = {"uid": "t-str", "backend": "vllm", "exception": "connection reset by peer"}
        event, span, _ = await _fail(task)

        attrs = event["attributes"]
        assert attrs["error_type"] == "str"
        assert attrs["error_message"] == "connection reset by peer"
        assert "traceback" not in attrs
        assert "exit_code" not in attrs
        assert span["attributes"]["error_message"] == "connection reset by peer"
        assert span["status_code"] == "ERROR"

    async def test_traceback_text_stored_by_the_backend(self):
        """An exception rebuilt from a remote failure has no traceback; the backend's text is
        used."""
        remote_tb = (
            'Traceback (most recent call last):\n  File "w.py", line 1\nRuntimeError: remote\n'
        )
        task = {
            "uid": "t-remote",
            "backend": "orbit",
            "exception": RuntimeError("remote"),
            "traceback": remote_tb,
        }
        event, _, _ = await _fail(task)
        assert event["attributes"]["traceback"] == remote_tb

    async def test_unprintable_exception(self):
        class UnprintableError(Exception):
            def __str__(self):
                raise RuntimeError("no str")

        task = {"uid": "t-unprintable", "backend": "test", "exception": UnprintableError()}
        event, _, _ = await _fail(task)
        assert event["attributes"]["error_message"] == "<unprintable UnprintableError>"

    async def test_otlp_export_carries_the_details(self):
        exc = _caught(_raise_bad_input, 7)
        task = {"uid": "t-otlp", "backend": "concurrent", "exception": exc, "exit_code": 1}
        _, _, m = await _fail(task)

        data = json.loads(m.export_as_otlp())
        spans = data["resourceSpans"][0]["scopeSpans"][0]["spans"]
        span = next(s for s in spans if s["name"] == "task")
        attrs = {a["key"]: a["value"] for a in span["attributes"]}
        assert attrs["error_message"] == {"stringValue": "bad input 7"}
        assert attrs["exit_code"] == {"intValue": "1"}
        assert attrs["traceback"]["stringValue"].endswith("ValueError: bad input 7\n")
        assert span["status"]["message"] == "ValueError"


class TestTaskFailedDetailsThroughSession:
    async def test_function_and_executable_failures_reach_the_checkpoint(self):
        backend = await ConcurrentExecutionBackend(executor=ThreadPoolExecutor(max_workers=2))
        with tempfile.TemporaryDirectory() as tmpdir:
            session = Session(backends=[backend])
            telemetry = await session.start_telemetry(checkpoint_path=tmpdir)
            func_task = ComputeTask(function=_raise_bad_input, args=[5])
            exec_task = ComputeTask(
                executable="/bin/sh", arguments=["-c", "echo 'disk full' >&2; exit 3"]
            )
            try:
                await session.submit_tasks([func_task, exec_task])
                await session.wait_tasks([func_task, exec_task], timeout=10.0)
                assert func_task["state"] == exec_task["state"] == "FAILED"
            finally:
                await session.close()
                await backend.shutdown()

            rows = _read_checkpoint(tmpdir)
            assert telemetry.task_count()["failed"] == 2

        func_attrs = _failed_event(rows, func_task["uid"])["attributes"]
        assert func_attrs["error_type"] == "ValueError"
        assert func_attrs["error_message"] == "bad input 5"
        assert func_attrs["traceback"].endswith("ValueError: bad input 5\n")
        assert "in _raise_bad_input" in func_attrs["traceback"]

        exec_attrs = _failed_event(rows, exec_task["uid"])["attributes"]
        assert exec_attrs["exit_code"] == 3
        assert exec_attrs["stderr"] == "disk full\n"
