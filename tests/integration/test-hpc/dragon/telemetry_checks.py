"""Checks and helpers for the inference telemetry cluster tests.

The tests in ``test_ai_telemetry.py`` run the vLLM backend with
telemetry on a GPU allocation and call :func:`verify`, which checks that the events, the batch spans,
the per-task attributes and the metrics agree with each other, that the Dragon fields are plausible,
and that no prompt text reached the telemetry files. :func:`verify` takes plain data, so it is
exercised offline against a fake Dragon pipeline, on healthy and on broken data
(tests/unit/telemetry/test_inference_verify_kit.py), before it meets a GPU.

The workload mixes plain-string and list prompts, and can also send requests through the backend's
HTTP endpoints (``/generate`` and ``/v1/chat/completions``) like a service-mode run.
"""

from __future__ import annotations

import asyncio
import functools
import glob
import json
import logging
import math
import os
import re
import sys
import time
import uuid
from importlib import metadata
from pathlib import Path

PREFIX = "rhapsody.inference."
STARTED = PREFIX + "BatchStarted"
DONE = PREFIX + "BatchDone"
FAILED = PREFIX + "BatchFailed"
ENDED = (DONE, FAILED)  # a batch ends with one of them
SCOPE = "rhapsody.instrumentation.inference"
# Dragon reports latencies rounded to 0.01 s, so the model time can exceed the measured batch
# time by up to that much.
ROUNDING_S = 0.015
LINK_CAP = 128
# Default number of AITasks in the cluster test workload.
AI_TASKS_DEFAULT = 256
# Largest batch the cluster tests' backend sends (the checks compare 'size' batches with it).
AI_BATCH_SIZE = 32
AI_WARMUP_REQUESTS = 8
# A soft check allows this much slack between the sum of its parts and a measured total.
SOFT_TOLERANCE_S = 1.0
# Privacy check: any prompt canary in a file is a leak; this many response texts are also searched.
_CANARY_RE = re.compile(r"CANARY-[0-9a-f]{8}-\w+")
_RESPONSE_SAMPLE = 200


# ---------------------------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------------------------


class Report:
    """The result table: a list of (name, status, detail) with PASS, WARN or FAIL."""

    def __init__(self) -> None:
        self.checks: list[dict] = []
        self.stats: dict = {}

    def add(self, name: str, ok: bool, detail: str = "", soft: bool = False) -> None:
        status = "PASS" if ok else ("WARN" if soft else "FAIL")
        self.checks.append(
            {"name": name, "group": name.split(":", 1)[0], "status": status, "detail": detail}
        )

    def note(self, name: str, detail: str) -> None:
        self.checks.append(
            {"name": name, "group": name.split(":", 1)[0], "status": "WARN", "detail": detail}
        )

    def group(self, *groups: str) -> list[dict]:
        """The checks of the given groups (the word before the colon in a check's name)."""
        return [c for c in self.checks if c["group"] in groups]

    def failures(self, *groups: str, exclude: tuple[str, ...] = ()) -> list[str]:
        """FAIL lines (name and detail) of the given groups, or of every group but ``exclude``."""
        return [
            f"{c['name']}: {c['detail']}"
            for c in self.checks
            if c["status"] == "FAIL"
            and (c["group"] in groups if groups else c["group"] not in exclude)
        ]

    @property
    def failed(self) -> bool:
        return any(c["status"] == "FAIL" for c in self.checks)

    def table(self) -> str:
        width = max((len(c["name"]) for c in self.checks), default=10)
        lines = [f"{c['status']:<5} {c['name']:<{width}}  {c['detail']}" for c in self.checks]
        return "\n".join(lines)


def _percentiles(values: list[float]) -> dict:
    if not values:
        return {}
    ordered = sorted(values)

    def pick(q: float) -> float:
        return ordered[min(len(ordered) - 1, math.ceil(q * len(ordered)) - 1)]

    return {
        "n": len(ordered),
        "min": ordered[0],
        "p50": pick(0.5),
        "p90": pick(0.9),
        "max": ordered[-1],
    }


def _inference_metrics(telemetry) -> dict:
    """Name -> list of data points of the inference scope (empty dict without any)."""
    out: dict = {}
    data = telemetry.read_metrics()
    if data is None:
        return out
    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            if sm.scope.name == SCOPE:
                for metric in sm.metrics:
                    out[metric.name] = metric.data.data_points
    return out


def _sum_points(points, source: str | None = None) -> float:
    total = 0.0
    for dp in points or ():
        if source is None or dp.attributes.get("rhapsody.inference.request.source") == source:
            total += dp.value
    return total


def verify(
    telemetry,
    events: list,
    expected: dict,
    jsonl_path: str | None = None,
    otlp_path: str | None = None,
    known_hosts: set[str] | None = None,
    log_records: list | None = None,
) -> Report:
    """Check the telemetry of a finished inference run. Returns the report.

    Args:
        telemetry: The stopped ``TelemetryManager``.
        events: Every event the run produced (collected with ``telemetry.subscribe``).
        expected: ``task_requests`` (AITasks), ``api_requests`` (``generate()`` calls),
            ``max_batch_size``, ``canaries`` (strings that must not appear in the files),
            ``timeouts`` (True for a run that is meant to time out), ``failed_task_ids``.
        jsonl_path, otlp_path: The telemetry files to scan for the privacy check and the rows.
        known_hosts: Node names of the allocation (for example from ``SLURM_JOB_NODELIST``).
        log_records: The WARNING and ERROR records the run logged (``None`` = not captured).
    """
    report = Report()
    spans = list(telemetry.read_traces())
    metrics = _inference_metrics(telemetry)
    starts = [e for e in events if e.event_type == STARTED]
    ends = [e for e in events if e.event_type in ENDED]
    rows = [
        (e.attributes["batch_id"], row) for e in ends for row in e.attributes.get("requests", [])
    ]
    task_rows = [(b, r) for b, r in rows if r.get("task_id")]
    api_rows = [(b, r) for b, r in rows if not r.get("task_id")]
    timeouts_expected = bool(expected.get("timeouts"))
    total_timed_out = sum(e.attributes.get("timed_out_prompts", 0) for e in ends)
    n_failed = sum(e.event_type == FAILED for e in ends)

    # 1. The events pair up, in memory and in the JSONL file.
    start_ids = sorted(e.attributes["batch_id"] for e in starts)
    end_ids = sorted(e.attributes["batch_id"] for e in ends)
    report.add(
        "events: every BatchStarted has a BatchDone or BatchFailed",
        bool(ends) and start_ids == end_ids,
        f"{len(starts)} started, {len(ends)} ended",
    )
    if jsonl_path:
        counts = {STARTED: 0, DONE: 0, FAILED: 0}
        for line in Path(jsonl_path).read_text().splitlines():
            try:
                kind = json.loads(line).get("event_type")
            except ValueError:
                continue
            if kind in counts:
                counts[kind] += 1
        report.add(
            "events: the JSONL file has the same events",
            counts
            == {
                STARTED: len(starts),
                DONE: sum(e.event_type == DONE for e in ends),
                FAILED: sum(e.event_type == FAILED for e in ends),
            },
            f"file {counts[STARTED]}/{counts[DONE]}/{counts[FAILED]} "
            f"(started/done/failed), memory {len(starts)}/{len(ends) - n_failed}/{n_failed}",
        )

    # 2. One batch span per batch, with the right shape.
    batch_spans = {
        s.attributes.get("rhapsody.inference.batch.id"): s
        for s in spans
        if s.name == "chat" or s.name.startswith("chat ")
    }
    session_span = next((s for s in spans if s.name == "session"), None)
    n_chat_spans = sum(1 for s in spans if s.name == "chat" or s.name.startswith("chat "))
    report.add(
        "spans: one chat span per batch",
        sorted(batch_spans) == end_ids
        and len(batch_spans) == len(ends)
        and n_chat_spans == len(batch_spans),  # a duplicate span would hide in the dict
        f"{n_chat_spans} spans, {len(ends)} batches",
    )
    bad_shape = []
    for batch_id, span in batch_spans.items():
        batch_rows = [r for b, r in rows if b == batch_id and r.get("task_id")]
        problems = []
        if span.kind.name != "CLIENT":
            problems.append("kind")
        if span.end_time is None or span.end_time < span.start_time:
            problems.append("times")
        if (
            session_span is None
            or span.parent is None
            or (span.parent.span_id != session_span.context.span_id)
        ):
            problems.append("parent")
        if len(span.links) != min(len(batch_rows), LINK_CAP):
            problems.append(f"links {len(span.links)} != {min(len(batch_rows), LINK_CAP)}")
        if problems:
            bad_shape.append(f"batch {batch_id}: {', '.join(problems)}")
    report.add(
        "spans: kind CLIENT, ordered times, session parent, links = task requests (max 128)",
        not bad_shape,
        "; ".join(bad_shape[:3]) or "ok",
    )
    errored = sorted(b for b, s in batch_spans.items() if s.status.status_code.name == "ERROR")
    failed_batches = sorted(e.attributes["batch_id"] for e in ends if e.event_type == FAILED)
    report.add(
        "spans: ERROR exactly for the batches that failed",
        errored == failed_batches,
        f"error spans {errored}, failed batches {failed_batches}",
    )

    # 3. The per-task attributes.
    task_spans = {
        s.attributes.get("task_id"): s for s in spans if s.name == "task" and s.attributes
    }
    bad_tasks = []
    for batch_id, row in task_rows:
        span = task_spans.get(row["task_id"])
        if span is None:
            bad_tasks.append(f"{row['task_id']}: no task span")
            continue
        attrs = span.attributes
        problems = []
        if attrs.get("rhapsody.inference.batch.id") != batch_id:
            problems.append("batch id")
        if attrs.get("rhapsody.inference.prompt_count") != row["prompt_count"]:
            problems.append("prompt count")
        queue = attrs.get("rhapsody.inference.request.queue.duration")
        if queue is None or queue < 0:
            problems.append("queue wait")
        if ("error.type" in attrs) != bool(row.get("timed_out_prompts")):
            problems.append("error.type")
        if problems:
            bad_tasks.append(f"{row['task_id']}: {', '.join(problems)}")
    report.add(
        "task spans: batch id, prompt count, queue wait, error.type",
        not bad_tasks,
        "; ".join(bad_tasks[:3]) or f"{len(task_rows)} tasks ok",
    )

    # 4. The metrics agree with the events (a missing data point means 0).
    n_batches = len(ends)
    report.add(
        "metrics: batches == batch events",
        _sum_points(metrics.get("rhapsody.inference.batches")) == n_batches,
        f"{_sum_points(metrics.get('rhapsody.inference.batches')):.0f} vs {n_batches}",
    )
    requests = metrics.get("rhapsody.inference.requests")
    report.add(
        "metrics: requests by source == AITasks and generate() calls",
        _sum_points(requests, "task") == len(task_rows)
        and _sum_points(requests, "api") == len(api_rows),
        f"task {_sum_points(requests, 'task'):.0f}/{len(task_rows)}, "
        f"api {_sum_points(requests, 'api'):.0f}/{len(api_rows)}",
    )
    prompts = sum(r["prompt_count"] for _, r in rows)
    report.add(
        "metrics: prompts == the sum of the request rows",
        _sum_points(metrics.get("rhapsody.inference.prompts")) == prompts,
        f"{_sum_points(metrics.get('rhapsody.inference.prompts')):.0f} vs {prompts}",
    )
    queue_points = metrics.get("rhapsody.inference.request.queue.duration") or []
    report.add(
        "metrics: queue histogram count == requests",
        sum(dp.count for dp in queue_points) == len(rows),
        f"{sum(dp.count for dp in queue_points)} vs {len(rows)}",
    )
    report.add(
        "metrics: timed-out prompts == the batch events",
        _sum_points(metrics.get("rhapsody.inference.timed_out.prompts")) == total_timed_out,
        f"{_sum_points(metrics.get('rhapsody.inference.timed_out.prompts')):.0f} vs "
        f"{total_timed_out}",
    )
    if expected.get("task_requests") is not None:
        report.add(
            "workload: the run saw every request it submitted",
            len(task_rows) == expected["task_requests"]
            and len(api_rows) == expected.get("api_requests", len(api_rows)),
            f"task {len(task_rows)}/{expected['task_requests']}, "
            f"api {len(api_rows)}/{expected.get('api_requests')}",
        )

    if "workload_errors" in expected and not timeouts_expected:  # errors are expected then
        report.add(
            "workload: no request raised an error",
            not expected["workload_errors"],
            "; ".join(expected["workload_errors"][:3]) or "none",
        )
    if "failed_task_ids" in expected and not timeouts_expected:
        report.add(
            "workload: no task failed",
            not expected["failed_task_ids"],
            f"{len(expected['failed_task_ids'])} failed",
        )

    # 5. Dragon's model time against the measured batch time.
    bad_model = []
    model_batches = 0
    for e in ends:
        model = e.attributes.get("dragon_model_duration_s")
        batch = e.attributes.get("batch_duration_s")
        if model is None or model == 0:
            continue  # not reported (Dragon's error response sends 0): nothing is recorded either
        model_batches += 1
        if model < 0 or (not timeouts_expected and model > batch + ROUNDING_S):
            bad_model.append(f"batch {e.attributes['batch_id']}: model {model} batch {batch:.3f}")
    report.add(
        "dragon: model time <= batch time (+0.015 s rounding), when reported",
        not bad_model,
        "; ".join(bad_model[:3]) or f"{model_batches} batches carry it",
        soft=timeouts_expected,
    )
    model_points = metrics.get("rhapsody.inference.model.duration") or []
    report.add(
        "metrics: model histogram has one point per batch that reported a value",
        sum(dp.count for dp in model_points) == model_batches,
        f"{sum(dp.count for dp in model_points)} vs {model_batches}",
    )

    # 6. Batch triggers.
    max_size = expected.get("max_batch_size")
    deadline_batches = [e for e in ends if e.attributes.get("batch_trigger") == "deadline"]
    if max_size:
        report.add(
            "triggers: a 'deadline' batch has fewer than max_batch_size requests",
            all(e.attributes["batch_requests"] < max_size for e in deadline_batches),
            f"{len(deadline_batches)} deadline batches",
        )
    kinds = {e.attributes.get("batch_trigger") for e in ends}
    report.add(
        "triggers: both 'size' and 'deadline' batches occurred",
        {"size", "deadline"} <= kinds,
        f"seen {sorted(k for k in kinds if k)}",
        soft=True,
    )

    # 7. Hosts.
    resource_nodes = {e.node_id for e in events if e.event_type == "ResourceUpdate" and e.node_id}
    hosts = {
        e.attributes["dragon_worker_host"] for e in ends if e.attributes.get("dragon_worker_host")
    }
    nodes = resource_nodes | set(known_hosts or ())
    if hosts and nodes:
        short = {n.split(".")[0] for n in nodes}
        unknown = {h for h in hosts if h not in nodes and h.split(".")[0] not in short}
        report.add(
            "hosts: every Dragon worker host is a node of the allocation",
            not unknown,
            f"hosts {sorted(hosts)}, unknown {sorted(unknown)}",
        )
    else:
        report.note(
            "hosts: worker host membership not checked",
            f"hosts {sorted(hosts)}, nodes known {len(nodes)} (no ResourceUpdate or node list)",
        )
    if known_hosts:
        report.add(
            "resources: ResourceUpdate covers every node of the allocation",
            {n.split(".")[0] for n in known_hosts} <= {n.split(".")[0] for n in resource_nodes},
            f"seen {sorted(resource_nodes)}, allocation {sorted(known_hosts)}",
            soft=True,
        )

    # 8. Soft timing checks.
    batch_by_id = {e.attributes["batch_id"]: e.attributes["batch_duration_s"] for e in ends}
    slow = []
    for batch_id, row in task_rows:
        span = task_spans.get(row["task_id"])
        if span is None or span.end_time is None:
            continue
        total = (span.end_time - span.start_time) / 1e9
        if row["queue_duration_s"] + batch_by_id[batch_id] > total + SOFT_TOLERANCE_S:
            slow.append(row["task_id"])
    report.add(
        "timing: queue wait + batch time <= the task's own duration",
        not slow,
        f"{len(slow)} tasks above the tolerance" if slow else "ok",
        soft=True,
    )

    # 9. Timeouts.
    if timeouts_expected:
        report.add(
            "timeouts: a timeout occurred",
            total_timed_out > 0,
            f"{total_timed_out} prompts timed out",
            soft=True,
        )
        failed = set(expected.get("failed_task_ids", ()))
        timed_tasks = {r["task_id"] for _, r in task_rows if r.get("timed_out_prompts")}
        report.add(
            "timeouts: FAILED tasks == tasks with timed-out prompts",
            failed == timed_tasks,
            f"failed {len(failed)}, timed out {len(timed_tasks)}",
        )
    else:
        report.add(
            "timeouts: no prompt timed out", total_timed_out == 0, f"{total_timed_out} timed out"
        )

    # 10. Privacy: no canary text in the files, raw or JSON-escaped.  Prompt canaries share one
    # prefix, so one regex pass finds any of them; response texts are arbitrary, so a sample of
    # them is searched one by one (searching thousands of strings in a 14 MB file takes minutes).
    canaries = list(expected.get("canaries", ()))
    prompt_canaries = [c for c in canaries if c.startswith("CANARY-")]
    responses = [c for c in canaries if not c.startswith("CANARY-")]
    step = max(1, -(-len(responses) // _RESPONSE_SAMPLE))  # ceil: at most the sample size
    sampled = responses[::step]
    leaked = []
    for path in (jsonl_path, otlp_path):
        if not path:
            continue
        text = Path(path).read_text()
        leaked += [f"{Path(path).name}: {m}" for m in set(_CANARY_RE.findall(text))][:3]
        for response in sampled:
            if response in text or json.dumps(response)[1:-1] in text:
                leaked.append(f"{Path(path).name}: {response[:30]}")
    report.add(
        "privacy: no prompt or response text in the telemetry files",
        not leaked,
        "; ".join(leaked[:3])
        or f"{len(prompt_canaries)} prompt canaries and {len(sampled)} of "
        f"{len(responses)} responses absent",
    )

    # 11. Logs: a telemetry warning means an instrumentation or hook failed (it warns once).
    if log_records is not None:
        telemetry_logs = [r for r in log_records if r.name.startswith("rhapsody.telemetry")]
        other_logs = [r for r in log_records if not r.name.startswith("rhapsody.telemetry")]
        report.add(
            "logs: no warning or error from rhapsody.telemetry",
            not telemetry_logs,
            "; ".join(r.getMessage()[:80] for r in telemetry_logs[:3]) or "none logged",
        )
        report.add(
            "logs: no other rhapsody warning or error",
            not other_logs,
            "; ".join(f"{r.name}: {r.getMessage()[:80]}" for r in other_logs[:3]) or "none logged",
            soft=True,
        )

    # Numbers for the report.
    report.stats = {
        "batches": n_batches,
        "requests": len(rows),
        "prompts": prompts,
        "timed_out_prompts": total_timed_out,
        "queue_wait_s": _percentiles([r["queue_duration_s"] for _, r in rows]),
        "batch_duration_s": _percentiles([e.attributes["batch_duration_s"] for e in ends]),
        "dragon_model_duration_s": _percentiles(
            [
                e.attributes["dragon_model_duration_s"]
                for e in ends
                if e.attributes.get("dragon_model_duration_s") is not None
            ]
        ),
        "worker_hosts": sorted(hosts),
        "resource_nodes": sorted(resource_nodes),
        "triggers": {
            k: sum(1 for e in ends if e.attributes.get("batch_trigger") == k) for k in kinds if k
        },
    }
    return report


# ---------------------------------------------------------------------------------------------
# Workload
# ---------------------------------------------------------------------------------------------


async def _http_requests(backend, count: int, prompt) -> list:
    """Send ``count`` requests through the backend's HTTP endpoints (alternating the two)."""
    if count <= 0:
        return []  # no HTTP run: get_endpoint() would warn that the backend has no server
    import aiohttp

    base = backend.get_endpoint()
    errors: list = []
    async with aiohttp.ClientSession() as http:
        for i in range(count):
            if i % 2 == 0:
                url, body = f"{base}/generate", {"prompts": [prompt(f"h{i}")], "timeout": 3600}
            else:
                url = f"{base}/v1/chat/completions"
                body = {"messages": [{"role": "user", "content": prompt(f"h{i}")}]}
            try:
                async with http.post(url, json=body) as response:
                    if response.status != 200:
                        errors.append(f"{url}: HTTP {response.status}")
            except Exception as exc:
                errors.append(f"{url}: {exc!r}")
    return errors


async def run_workload(
    session,
    backend,
    n_tasks: int,
    prompts_per_task: int,
    generate_calls: int,
    http_requests: int = 0,
):
    """Submit AITasks, direct generate() calls and HTTP requests together.

    Every fourth AITask gets a plain string prompt instead of a list (one prompt), because users
    write both. Returns what the checks need.
    """
    from rhapsody.api import AITask

    run_id = uuid.uuid4().hex[:8]
    canaries: list[str] = []

    def prompt(label: str) -> str:
        text = f"CANARY-{run_id}-{label} Answer in one short sentence: what is 2 + 2?"
        canaries.append(f"CANARY-{run_id}-{label}")
        return text

    tasks = [
        AITask(
            prompt=(
                prompt(f"t{i}")
                if i % 4 == 3
                else [prompt(f"t{i}p{j}") for j in range(prompts_per_task)]
            ),
            backend=backend.name,
            model=getattr(backend, "model_name", None),
        )
        for i in range(n_tasks)
    ]
    generate_prompts = [[prompt(f"g{i}")] for i in range(generate_calls)]
    await session.submit_tasks(tasks)
    results = await asyncio.gather(
        session.wait_tasks(tasks, timeout=3600),
        *[backend.generate(p, timeout=3600) for p in generate_prompts],
        _http_requests(backend, http_requests, prompt),
        return_exceptions=True,
    )
    failed = [t["uid"] for t in tasks if t.get("state") == "FAILED"]
    responses = []
    for t in tasks:
        for r in t.get("response") or []:
            if isinstance(r, str) and len(r) >= 20 and not r.startswith("ERROR:"):
                responses.append(r)
    errors = [r for r in results if isinstance(r, Exception)]
    errors += results[-1] if isinstance(results[-1], list) else []
    return {
        "task_requests": n_tasks,
        "api_requests": generate_calls + http_requests,
        "canaries": canaries + responses,
        "failed_task_ids": failed,
        "workload_errors": [repr(e) for e in errors],
    }


# ---------------------------------------------------------------------------------------------
# Support: log capture, provenance, warm-up
# ---------------------------------------------------------------------------------------------


class LogCapture(logging.Handler):
    """Keeps the WARNING and ERROR records of the run, so a failing hook or instrumentation
    shows."""

    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.records: list = []

    def emit(self, record) -> None:
        self.records.append(record)


def versions() -> dict:
    out = {}
    for name in ("rhapsody-py", "radical.asyncflow", "opentelemetry-sdk", "dragonhpc", "vllm"):
        try:
            out[name] = metadata.version(name)
        except metadata.PackageNotFoundError:
            out[name] = None
    return out


def latest_jsonl(out: Path) -> str | None:
    """The newest telemetry checkpoint file in ``out`` (a re-run may leave older ones there)."""
    files = glob.glob(str(out / "*.telemetry.jsonl"))
    return max(files, key=os.path.getmtime) if files else None


def _run(cmd: list[str], cwd: Path) -> str | None:
    """Output of a command, or None when it cannot run or fails."""
    import subprocess

    try:
        done = subprocess.run(cmd, capture_output=True, text=True, timeout=30, cwd=cwd)
    except Exception:
        return None
    return done.stdout.strip() if done.returncode == 0 else None


@functools.cache
def provenance() -> dict:
    """Which code produced the run. Each key is collected on its own and is None if unreadable.

    ``git_head`` and ``git_status`` describe the checkout this script is in, which is not
    necessarily the code that ran: a regular (non-editable) install only picks up source edits after
    a reinstall. ``rhapsody_module`` and ``rhapsody_install`` say where the imported code came from.
    """
    here = Path(__file__).resolve().parent
    out: dict = {
        "git_head": _run(["git", "rev-parse", "HEAD"], here),
        "git_status": _run(["git", "status", "--short"], here),
    }
    try:
        listing = _run([sys.executable, "-m", "pip", "list", "--format=freeze"], here)
        keep = ("rhapsody", "opentelemetry", "dragon", "vllm", "asyncflow")
        out["packages"] = (
            None
            if listing is None
            else [ln for ln in listing.splitlines() if ln.lower().startswith(keep)]
        )
    except Exception:
        out["packages"] = None
    try:
        import rhapsody

        out["rhapsody_module"] = str(Path(rhapsody.__file__).resolve())
    except Exception:
        out["rhapsody_module"] = None
    try:
        # pip records how a package was installed here (a path for a local install; editable or not)
        out["rhapsody_install"] = metadata.distribution("rhapsody-py").read_text("direct_url.json")
    except Exception:
        out["rhapsody_install"] = None
    return out


async def warm_up(backend, count: int) -> None:
    """Send ``count`` generate() calls one at a time; an error is printed and does not stop the run.

    One at a time, because calls sent together would likely form one batch on one worker. Even so,
    nothing here guarantees that every worker is warmed.
    """
    if count <= 0:
        return
    began = time.perf_counter()
    errors = 0
    for _ in range(count):
        try:
            answers = await backend.generate(["Answer in one short sentence: what is 2 + 2?"], 600)
            if any(isinstance(a, str) and a.startswith("ERROR:") for a in answers):
                errors += 1
        except Exception as exc:
            errors += 1
            print(f"warm-up call failed: {exc!r}")
    note = f" ({errors} failed)" if errors else ""
    print(f"warm-up: {count} requests in {time.perf_counter() - began:.2f} s{note}")


# ---------------------------------------------------------------------------------------------
# One run: workload, stop, check, save
# ---------------------------------------------------------------------------------------------


async def run_inference_telemetry(
    backend,
    out: Path,
    tasks: int,
    *,
    prompts_per_task: int = 2,
    generate_calls: int = 4,
    http_requests: int = 0,
    max_batch_size: int = 32,
    known_hosts: set[str] | None = None,
    log_capture: LogCapture | None = None,
    timeouts: bool = False,
    resource_poll_interval: float = 1.0,
    hardware: dict | None = None,
) -> tuple[Report, dict]:
    """Run a workload on a started backend with telemetry on, and check the telemetry.

    The backend must already be warm: it drops events until a manager is attached, and a cold
    first batch would distort the numbers. Telemetry is stopped once here, not the session, so the
    backend stays up for the caller to shut down. Writes the OTLP export and
    ``inference_telemetry_report.json`` next to the JSONL checkpoint in ``out``.
    Returns the report and the run's facts (wall time, workload).
    """
    from rhapsody.api import Session

    provenance_info = provenance()  # before the run's files exist, so git status shows the checkout
    out.mkdir(parents=True, exist_ok=True)
    # The capture spans the whole module (backend start, warm-up, earlier runs): keep this run's.
    first_record = len(log_capture.records) if log_capture else 0
    session = Session([backend])
    events: list = []
    began = time.perf_counter()
    telemetry = await session.start_telemetry(
        checkpoint_path=str(out), resource_poll_interval=resource_poll_interval
    )
    start_s = time.perf_counter() - began
    telemetry.subscribe(events.append)
    try:
        started = time.perf_counter()
        workload = await run_workload(
            session, backend, tasks, prompts_per_task, generate_calls, http_requests
        )
        wall = time.perf_counter() - started
    finally:
        # Also when the workload raised: a running telemetry manager keeps the resource adapter
        # polling while the caller shuts the backend down.
        stopped = time.perf_counter()
        await telemetry.stop()
        stop_s = time.perf_counter() - stopped

    otlp_path = out / "inference_telemetry.otlp.json"
    telemetry.export_as_otlp(str(otlp_path))
    expected = {**workload, "max_batch_size": max_batch_size, "timeouts": timeouts}
    jsonl = latest_jsonl(out)
    records = log_capture.records[first_record:] if log_capture else None
    try:
        report = verify(
            telemetry,
            events,
            expected,
            jsonl_path=jsonl,
            otlp_path=str(otlp_path),
            known_hosts=known_hosts,
            log_records=records,
        )
    except Exception as exc:  # a malformed payload must not lose the run's report file
        report = Report()
        report.add("events: the checks ran", False, f"verify() raised {exc!r}")
    # Without the file, the JSONL and privacy checks would skip it silently.
    report.add(
        "events: a JSONL checkpoint was written",
        jsonl is not None,
        jsonl or f"no *.telemetry.jsonl in {out}",
    )
    facts = {"wall_time_s": wall, "start_s": start_s, "stop_s": stop_s, "workload": workload}
    write_run_report(out, report, wall, workload, provenance_info, records, hardware, tasks)
    return report, facts


def write_run_report(
    out: Path,
    report: Report,
    wall_time_s: float,
    workload: dict,
    provenance_info: dict,
    log_records: list | None,
    hardware: dict | None,
    tasks: int,
) -> None:
    """Save the run's report: checks, numbers, wall time and where the code came from."""
    (out / "inference_telemetry_report.json").write_text(
        json.dumps(
            {
                "checks": report.checks,
                "stats": report.stats,
                "wall_time_s": wall_time_s,
                "args": {"tasks": tasks},
                "versions": versions(),
                **provenance_info,
                "hardware": hardware,
                "workload_errors": workload["workload_errors"],
                "logged_warnings": [
                    f"{r.levelname} {r.name}: {r.getMessage()}"
                    for r in (log_records[:50] if log_records else [])
                ],
            },
            indent=2,
            default=str,
        )
    )
    # The soft rows (timing, triggers, resources, logs) are easy to miss in the JSON.
    (out / "inference_telemetry_table.txt").write_text(report.table() + "\n")
