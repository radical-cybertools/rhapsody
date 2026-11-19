"""Golden test: the task and resource metrics must not change when their code moves.

A deterministic scenario goes through a real TelemetryManager and the resulting OTel data points,
``task_count()`` and the JSONL metric snapshot are compared with ``golden_task_metrics.json``. The
fixture was generated from the code before the task metrics moved into their own instrumentation,
so this test is the proof that the move changed nothing observable. Meter scope names are not part
of it. The default histogram buckets come from the OpenTelemetry SDK, so an SDK upgrade can
legitimately change the fixture.

Regenerate (only when the metrics are meant to change)::

    python tests/unit/telemetry/test_task_metrics_golden.py --write
"""

import asyncio
import json
import pathlib
import sys
import tempfile

GOLDEN = pathlib.Path(__file__).with_name("golden_task_metrics.json")

NAMES = {
    "tasks_submitted",
    "tasks_started",
    "tasks_completed",
    "tasks_failed",
    "tasks_canceled",
    "tasks_running",
    "task_duration_seconds",
    "node_cpu_utilization",
    "node_memory_utilization",
    "node_gpu_utilization",
}


def _normalize(metrics_data):
    """Reduce MetricsData to plain JSON: the 10 metrics, their points, no timestamps."""
    out = {}
    for resource_metrics in metrics_data.resource_metrics:
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                if metric.name not in NAMES:
                    continue
                data = metric.data
                points = []
                for dp in data.data_points:
                    point = {"attributes": sorted([k, v] for k, v in dp.attributes.items())}
                    if hasattr(dp, "bucket_counts"):  # histogram
                        point.update(
                            count=dp.count,
                            sum=dp.sum,
                            sum_type=type(dp.sum).__name__,
                            min=dp.min,
                            max=dp.max,
                            bucket_counts=list(dp.bucket_counts),
                            explicit_bounds=list(dp.explicit_bounds),
                        )
                    else:
                        point.update(value=dp.value, value_type=type(dp.value).__name__)
                    points.append(point)
                points.sort(key=lambda p: json.dumps(p["attributes"]))
                out[metric.name] = {
                    "type": type(data).__name__,
                    "description": metric.description,
                    "unit": metric.unit,
                    "temporality": data.aggregation_temporality.name,
                    "monotonic": getattr(data, "is_monotonic", None),
                    "points": points,
                }
    return dict(sorted(out.items()))


async def run_scenario(checkpoint_dir):
    """Run the scenario and return everything the fixture pins."""
    from rhapsody.telemetry.events import ResourceUpdate
    from rhapsody.telemetry.events import TaskCanceled
    from rhapsody.telemetry.events import TaskCompleted
    from rhapsody.telemetry.events import TaskFailed
    from rhapsody.telemetry.events import TaskStarted
    from rhapsody.telemetry.events import TaskSubmitted
    from rhapsody.telemetry.events import make_event
    from rhapsody.telemetry.manager import TelemetryManager

    manager = TelemetryManager(session_id="golden", checkpoint_path=str(checkpoint_dir))
    await manager.start()

    def task_event(cls, backend, task_id, exe="sleep", kind="executable", **kw):
        attrs = {"executable": exe, "task_type": kind, **kw.pop("attributes", {})}
        return make_event(
            cls,
            session_id="golden",
            backend=backend,
            task_id=task_id,
            event_time=1000.0,
            attributes=attrs,
            **kw,
        )

    def resource(backend, node, **kw):
        return make_event(ResourceUpdate, session_id="golden", backend=backend, node_id=node, **kw)

    events = [
        # A normal lifecycle, a failure and a cancellation on one backend.
        task_event(TaskSubmitted, "alpha", "t1"),
        task_event(TaskStarted, "alpha", "t1"),
        task_event(TaskCompleted, "alpha", "t1", duration_seconds=1.5),
        task_event(TaskSubmitted, "alpha", "t2", exe="work", kind="function"),
        task_event(TaskStarted, "alpha", "t2", exe="work", kind="function"),
        task_event(
            TaskFailed,
            "alpha",
            "t2",
            exe="work",
            kind="function",
            duration_seconds=0.25,
            error_type="ValueError",
            attributes={"error_type": "ValueError"},
        ),
        task_event(TaskSubmitted, "alpha", "t3"),
        task_event(TaskStarted, "alpha", "t3"),
        task_event(TaskCanceled, "alpha", "t3", duration_seconds=0.0),
        # A terminal event with no TaskStarted must not decrement tasks_running.
        task_event(TaskCompleted, "beta", "t4", attributes={"incomplete_lifecycle": True}),
        # A duplicate TaskStarted is counted once.
        task_event(TaskStarted, "beta", "t5"),
        task_event(TaskStarted, "beta", "t5"),
        task_event(TaskCompleted, "beta", "t5", duration_seconds=2.0),
        # The terminal event differs from the start in executable and task type: the -1 must
        # land on the series of the +1.
        task_event(TaskStarted, "beta", "t6", exe="a", kind="x"),
        task_event(TaskCompleted, "beta", "t6", exe="b", kind="y", duration_seconds=3.0),
        # A duplicate terminal event counts again but does not decrement a second time.
        task_event(TaskStarted, "alpha", "t7"),
        task_event(TaskCompleted, "alpha", "t7", duration_seconds=0.5),
        task_event(TaskCompleted, "alpha", "t7", duration_seconds=0.5),
        # A task that is still running at the end.
        task_event(TaskStarted, "alpha", "t9", exe="long", kind="executable"),
        # Resource updates: repeated updates (deltas), None vs 0.0, per-GPU events, and the same
        # node_id reported by two backends (the delta state is keyed by node_id only).
        resource("alpha", "n1", cpu_percent=10.0, memory_percent=20.0),
        resource("alpha", "n1", cpu_percent=30.0, memory_percent=20.0, gpu_percent=5.0),
        resource("beta", "n1", cpu_percent=50.0),
        resource("alpha", "n2", cpu_percent=0.0),
        resource("alpha", "n2", gpu_percent=75.0, gpu_id=0),
        resource("alpha", "n2", cpu_percent=12.5, memory_percent=0.0),
    ]

    # Task events of a backend that names a telemetry domain count like any other.
    class _Tagged:
        telemetry_domain = "golden-domain"

        def submit_tasks(self, tasks):
            pass

    from rhapsody.telemetry.instrumentation import Instrumentation
    from rhapsody.telemetry.instrumentation import register_instrumentation
    from rhapsody.telemetry.instrumentation import registry

    class _Quiet(Instrumentation):
        scope_name = "rhapsody.instrumentation.golden"

        def instrument(self, ctx):
            pass

        def on_event(self, event):
            pass

    saved_registry = dict(registry._REGISTRY)  # the test domain must not leak into other tests
    register_instrumentation("golden-domain", _Quiet)
    try:
        manager.attach_backend(_Tagged(), session_id="golden", backend_name="tagged", interval=60)
        events += [
            task_event(TaskSubmitted, "tagged", "t8"),
            task_event(TaskStarted, "tagged", "t8"),
            task_event(TaskCompleted, "tagged", "t8", duration_seconds=4.0),
        ]

        for event in events:
            manager.emit(event)
        await manager.stop()
    finally:
        registry._REGISTRY.clear()
        registry._REGISTRY.update(saved_registry)

    (path,) = pathlib.Path(checkpoint_dir).glob("*.jsonl")
    metric_lines = [
        json.loads(line)
        for line in path.read_text().splitlines()
        if json.loads(line).get("section") == "metric"
    ]
    snapshot = {
        k: v
        for k, v in metric_lines[-1].items()
        if any(k == name or k.startswith(name + "_") for name in NAMES)
    }
    return {
        "metrics": _normalize(manager.read_metrics()),
        "task_count": manager.task_count(),
        "jsonl_metric": snapshot,
    }


def test_task_and_resource_metrics_are_unchanged(tmp_path):
    import pytest

    pytest.importorskip("opentelemetry", reason="opentelemetry-sdk not installed")
    assert GOLDEN.exists(), "run this file with --write on the unmodified code to create it"
    result = asyncio.run(run_scenario(tmp_path))
    expected = json.loads(GOLDEN.read_text())
    assert json.loads(json.dumps(result)) == expected  # round trip: same JSON types as the file


if __name__ == "__main__":
    if "--write" not in sys.argv:
        raise SystemExit(__doc__)
    with tempfile.TemporaryDirectory() as tmp:
        result = asyncio.run(run_scenario(tmp))
    GOLDEN.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(f"wrote {GOLDEN}")
