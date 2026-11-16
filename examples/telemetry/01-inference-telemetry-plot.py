#!/usr/bin/env python3
"""Plot the inference telemetry of a run, from simple to sophisticated.

Reads the telemetry JSONL that ``01-inference-telemetry.py`` saves (``--out``), or the artifacts of the
cluster tests, and draws what the telemetry recorded about the inference backend.

Usage::

    python examples/telemetry/01-inference-telemetry-plot.py inference-telemetry/    # one run
    python examples/telemetry/01-inference-telemetry-plot.py runs/                   # every run inside it
    python examples/telemetry/01-inference-telemetry-plot.py a/ b/ --out ./plots     # several runs

A *run* is a directory with a ``*.telemetry.jsonl`` checkpoint (directly in it or in an
``inference-telemetry/`` folder inside it). If ``inference_telemetry_report.json`` (checks, wall time)
or ``inference_telemetry.otlp.json`` are also there they are used too, and a ``<run>.log`` next to the
directory gives the wall time of a run without telemetry.

Plots per run (numbers match the file names; each is skipped with a note when its data is missing):

  Overview
    01 checks and key numbers          02 batches: size, trigger, count
  Distributions
    03 queue wait                      04 batch time, model time, the difference
    05 prompts and requests per task
  Relationships
    06 batch/model time against batch size (fit: seconds per prompt)
    07 every request: when it queued, how long it waited
  Time
    08 timeline: batches, queue depth, tasks in flight, cumulative prompts
    09 throughput and idle gaps between batches
  Per task
    10 latency decomposition: queue wait + batch time + the rest
    11 task lifecycle phases (created -> submitted -> queued -> started -> completed)
  Hardware
    12 worker hosts                    13 CPU, memory and GPU against batch activity
  Trust
    14 events against metrics, batch span against batch event
    15 data volume of the telemetry files
    16 timeouts (only when something timed out)

Across runs (two or more): 90 telemetry overhead (wall times with and without telemetry),
91 run comparison (batches, latencies, throughput, data volume).

Derived, not recorded: a request's enqueue time is ``BatchStarted`` time minus its queue wait, so the
queue depth in plot 08 is a reconstruction. Only matplotlib and numpy are needed (like the other plot
scripts in examples/telemetry).
"""

from __future__ import annotations

import argparse
import collections
import glob
import json
import os
import re
import sys
from pathlib import Path

try:
    import matplotlib
    import numpy as np

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError as exc:  # pragma: no cover
    sys.exit(f"matplotlib and numpy are required: {exc}")

PREFIX = "rhapsody.inference."
STARTED = PREFIX + "BatchStarted"
ENDED = (PREFIX + "BatchDone", PREFIX + "BatchFailed")  # a batch ends with one of them
COLORS = {
    "size": "#2a78c2",
    "deadline": "#e0892b",
    "task": "#2a78c2",
    "api": "#8a4fbf",
    "model": "#2f9e6f",
    "batch": "#2a78c2",
    "queue": "#e0892b",
    "bad": "#c9302c",
    "grey": "#8c8c8c",
}
plt.rcParams.update(
    {
        "figure.dpi": 110,
        "axes.grid": True,
        "grid.alpha": 0.25,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.titlesize": 11,
        "axes.labelsize": 9,
        "font.size": 9,
    }
)


# ---------------------------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------------------------


class Run:
    """One run directory, loaded and reduced to the tables the plots need."""

    def __init__(self, path: Path, name: str | None = None):
        self.path = path
        self.name = name or path.name
        self.events: list[dict] = []
        self.spans: list[dict] = []
        self.metrics: list[dict] = []
        self.report: dict = {}
        self.jsonl_bytes = 0
        self.otlp_bytes = 0
        self.bytes_by_type: collections.Counter = collections.Counter()
        self.wall_s: float | None = None

        # The telemetry files are in the run directory or in its inference-telemetry/ folder (the
        # latter when a wrapper script gave every run its own directory and log).
        data = path / "inference-telemetry" if (path / "inference-telemetry").is_dir() else path
        files = glob.glob(str(data / "*.telemetry.jsonl"))
        self.jsonl = max(files, key=os.path.getmtime) if files else None
        if self.jsonl:
            self._read_jsonl()
        rep = data / "inference_telemetry_report.json"
        if rep.exists():
            self.report = json.loads(rep.read_text())
            self.wall_s = self.report.get("wall_time_s")
        otlp = data / "inference_telemetry.otlp.json"
        if otlp.exists():
            self.otlp_bytes = otlp.stat().st_size
        if self.wall_s is None:
            self.wall_s = _wall_from_log(path)
        self._derive()

    def _read_jsonl(self) -> None:
        self.jsonl_bytes = os.path.getsize(self.jsonl)
        with open(self.jsonl) as handle:
            for line in handle:
                try:
                    rec = json.loads(line)
                except ValueError:
                    continue
                section = rec.get("section")
                kind = rec.get("event_type") if section == "event" else section
                self.bytes_by_type[kind or "?"] += len(line.encode())
                if section == "event":
                    self.events.append(rec)
                elif section == "span":
                    self.spans.append(rec)
                elif section == "metric":
                    self.metrics.append(rec)

    @property
    def has_telemetry(self) -> bool:
        return bool(self.events)

    def _derive(self) -> None:
        ev = collections.defaultdict(list)
        for e in self.events:
            ev[e["event_type"]].append(e)
        self.by_type = ev
        times = [e["event_time"] for e in self.events]
        started = ev.get("SessionStarted")
        self.t0 = started[0]["event_time"] if started else (min(times) if times else 0.0)
        self.t_end = max(times) if times else 0.0

        starts = {e["attributes"]["batch_id"]: e for e in ev.get(STARTED, [])}
        self.batches: list[dict] = []
        self.requests: list[dict] = []
        ends = [e for kind in ENDED for e in ev.get(kind, [])]
        for e in sorted(ends, key=lambda x: x["attributes"]["batch_id"]):
            a = e["attributes"]
            s = starts.get(a["batch_id"])
            if s is None:
                continue
            t_start = s["event_time"]
            self.batches.append(
                {
                    "id": a["batch_id"],
                    "t_start": t_start - self.t0,
                    "duration": a["batch_duration_s"],
                    "requests": a["batch_requests"],
                    "prompts": a["batch_prompts"],
                    "trigger": s["attributes"].get("batch_trigger"),
                    "model_s": a.get("dragon_model_duration_s"),
                    "host": a.get("dragon_worker_host"),
                    "timed_out": a.get("timed_out_prompts", 0),
                }
            )
            for row in a.get("requests", []):
                self.requests.append(
                    {
                        "batch": a["batch_id"],
                        "task_id": row.get("task_id"),
                        "source": "task" if row.get("task_id") else "api",
                        "prompts": row["prompt_count"],
                        "queue": row["queue_duration_s"],
                        "timed_out": row.get("timed_out_prompts", 0),
                        "t_dispatch": t_start - self.t0,
                        "t_enqueue": t_start - self.t0 - row["queue_duration_s"],
                    }
                )
        self.batch_spans = [
            s for s in self.spans if s["name"] == "chat" or s["name"].startswith("chat ")
        ]
        self.task_spans = {
            s["attributes"].get("task_id"): s for s in self.spans if s["name"] == "task"
        }

    def col(self, rows: list[dict], key: str, where=None) -> np.ndarray:
        vals = [r[key] for r in rows if (where is None or where(r)) and r.get(key) is not None]
        return np.asarray(vals, dtype=float)


def _wall_from_log(path: Path) -> float | None:
    """Wall time printed by the script: from <run>/logs.log or from the <run>.log next to it."""
    for log in (path / "logs.log", path.parent / (path.name + ".log")):
        if log.exists():
            match = re.findall(r"workload wall time: ([0-9.]+) s", log.read_text(errors="replace"))
            if match:
                return float(match[-1])
    return None


# ---------------------------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------------------------


def _pcts(x: np.ndarray) -> str:
    if not len(x):
        return "n=0"
    p50, p90, p99 = np.percentile(x, [50, 90, 99])
    return f"n={len(x)}  p50={p50:.3g}  p90={p90:.3g}  p99={p99:.3g}  max={x.max():.3g}"


def _ecdf(ax, x, label, color=None):
    x = np.sort(np.asarray(x, dtype=float))
    if len(x):
        ax.step(x, np.arange(1, len(x) + 1) / len(x), where="post", label=label, color=color)


def _logbins(x: np.ndarray, n: int = 40) -> np.ndarray:
    x = x[x > 0]
    if not len(x) or x.min() == x.max():
        return np.linspace(0, max(float(x.max()) if len(x) else 1.0, 1e-3) * 1.1, 10)
    return np.logspace(np.log10(x.min()), np.log10(x.max()), n)


def _box(ax, data, labels) -> None:
    """Box plot with tick labels, whatever the matplotlib version calls the argument."""
    try:
        ax.boxplot(data, tick_labels=labels, showfliers=False)  # matplotlib >= 3.9
    except TypeError:
        ax.boxplot(data, labels=labels, showfliers=False)


def _note(ax, text: str) -> None:
    ax.text(0.5, 0.5, text, ha="center", va="center", transform=ax.transAxes, color=COLORS["grey"])
    ax.set_axis_off()


def _save(fig, out: Path, name: str, saved: list) -> None:
    fig.tight_layout()
    fig.savefig(out / name)
    plt.close(fig)
    saved.append(name)


def _shade_batches(ax, run: Run, alpha: float = 0.12) -> None:
    for b in run.batches:
        ax.axvspan(
            b["t_start"], b["t_start"] + b["duration"], color=COLORS["batch"], alpha=alpha, lw=0
        )


# ---------------------------------------------------------------------------------------------
# Per-run plots
# ---------------------------------------------------------------------------------------------


def plot_01_overview(run: Run, out: Path, saved: list) -> None:
    checks = run.report.get("checks", [])
    fig, (a, b) = plt.subplots(1, 2, figsize=(13, max(4, 0.32 * len(checks) + 1.5)))
    if checks:
        colors = {"PASS": "#2f9e6f", "WARN": "#e0892b", "FAIL": COLORS["bad"]}
        a.set_axis_off()
        for i, c in enumerate(checks):
            y = 1 - (i + 0.5) / len(checks)
            a.text(
                0.0,
                y,
                c["status"],
                color=colors[c["status"]],
                weight="bold",
                va="center",
                transform=a.transAxes,
            )
            a.text(0.08, y, c["name"][:78], va="center", transform=a.transAxes, fontsize=8)
        a.set_title("Verification checks (from the report)")
    else:
        _note(a, "no report JSON in this run")
    stats = run.report.get("stats", {})
    lines = [
        f"run: {run.name}",
        f"events: {len(run.events)}   spans: {len(run.spans)}",
        f"batches: {len(run.batches)}   requests: {len(run.requests)}   "
        f"prompts: {int(sum(r['prompts'] for r in run.requests))}",
        f"session length: {run.t_end - run.t0:.1f} s   workload wall time: {run.wall_s or float('nan'):.1f} s",
        f"queue wait: {_pcts(run.col(run.requests, 'queue'))}",
        f"batch time: {_pcts(run.col(run.batches, 'duration'))}",
        f"model time: {_pcts(run.col(run.batches, 'model_s'))}",
        f"triggers: {stats.get('triggers', dict(collections.Counter(b['trigger'] for b in run.batches)))}",
        f"worker hosts: {stats.get('worker_hosts', sorted({b['host'] for b in run.batches if b['host']}))}",
    ]
    b.set_axis_off()
    b.text(
        0, 1, "\n".join(lines), va="top", family="monospace", fontsize=8.5, transform=b.transAxes
    )
    b.set_title("Key numbers")
    _save(fig, out, "01_overview.png", saved)


def plot_02_batches(run: Run, out: Path, saved: list) -> None:
    if not run.batches:
        return
    fig, ax = plt.subplots(2, 2, figsize=(13, 8))
    ids = [b["id"] for b in run.batches]
    cols = [COLORS.get(b["trigger"], COLORS["grey"]) for b in run.batches]
    ax[0, 0].bar(ids, [b["requests"] for b in run.batches], color=cols)
    ax[0, 0].set(
        title="Requests per batch (blue = size trigger, orange = deadline)",
        xlabel="batch id",
        ylabel="requests",
    )
    ax[0, 1].bar(ids, [b["prompts"] for b in run.batches], color=cols)
    ax[0, 1].set(title="Prompts per batch", xlabel="batch id", ylabel="prompts")
    ax[1, 0].hist(
        [b["requests"] for b in run.batches], bins=min(30, max(5, len(ids))), color=COLORS["batch"]
    )
    ax[1, 0].set(title="Distribution of requests per batch", xlabel="requests", ylabel="batches")
    trig = collections.Counter(b["trigger"] or "?" for b in run.batches)
    ax[1, 1].bar(
        list(trig), list(trig.values()), color=[COLORS.get(k, COLORS["grey"]) for k in trig]
    )
    ax[1, 1].set(title="Why batches were sent", ylabel="batches")
    for i, v in enumerate(trig.values()):
        ax[1, 1].text(i, v, str(v), ha="center", va="bottom")
    _save(fig, out, "02_batches.png", saved)


def plot_03_queue_wait(run: Run, out: Path, saved: list) -> None:
    if not run.requests:
        return
    fig, ax = plt.subplots(1, 3, figsize=(15, 4.5))
    allq = run.col(run.requests, "queue")
    for src in ("task", "api"):
        q = run.col(run.requests, "queue", lambda r, s=src: r["source"] == s)
        if len(q):
            ax[0].hist(
                q, bins=_logbins(allq), alpha=0.6, label=f"{src} (n={len(q)})", color=COLORS[src]
            )
            _ecdf(ax[1], q, src, COLORS[src])
    ax[0].set(
        xscale="log", title="Queue wait, by request source", xlabel="seconds", ylabel="requests"
    )
    ax[0].legend()
    for p, ls in ((50, "-"), (90, "--"), (99, ":")):
        ax[1].axvline(
            np.percentile(allq, p),
            ls=ls,
            color=COLORS["grey"],
            label=f"p{p}={np.percentile(allq, p):.3g}s",
        )
    ax[1].set(
        xscale="log",
        title="Queue wait, cumulative",
        xlabel="seconds",
        ylabel="fraction of requests",
    )
    ax[1].legend()
    data = [
        run.col(run.requests, "queue", lambda r, b=b["id"]: r["batch"] == b) for b in run.batches
    ]
    step = max(1, len(data) // 25)
    _box(ax[2], data[::step], [b["id"] for b in run.batches][::step])
    ax[2].set(
        title="Queue wait per batch (later batches wait for earlier ones)",
        xlabel="batch id",
        ylabel="seconds",
    )
    _save(fig, out, "03_queue_wait.png", saved)


def plot_04_times(run: Run, out: Path, saved: list) -> None:
    if not run.batches:
        return
    batch = run.col(run.batches, "duration")
    model = run.col(run.batches, "model_s")
    fig, ax = plt.subplots(1, 3, figsize=(15, 4.5))
    ax[0].hist(
        batch,
        bins=_logbins(batch, 30),
        alpha=0.6,
        label="batch (measured by RHAPSODY)",
        color=COLORS["batch"],
    )
    if len(model):
        ax[0].hist(
            model,
            bins=_logbins(batch, 30),
            alpha=0.6,
            label="model (reported by Dragon)",
            color=COLORS["model"],
        )
    ax[0].set(xscale="log", title="Batch time and model time", xlabel="seconds", ylabel="batches")
    ax[0].legend()
    _ecdf(ax[1], batch, "batch", COLORS["batch"])
    _ecdf(ax[1], model, "model", COLORS["model"])
    ax[1].set(xscale="log", title="Cumulative", xlabel="seconds", ylabel="fraction of batches")
    ax[1].legend()
    pairs = [(b["duration"], b["model_s"]) for b in run.batches if b["model_s"]]
    if pairs:
        diff = np.array([d - m for d, m in pairs])
        ax[2].hist(diff, bins=30, color=COLORS["queue"])
        ax[2].axvline(0, color="k", lw=0.8)
        ax[2].set(
            title="Batch time minus model time\n(transport, vLLM scheduling; < 0 only by Dragon's 0.01 s rounding)",
            xlabel="seconds",
            ylabel="batches",
        )
    else:
        _note(ax[2], "no batch reported a model time")
    _save(fig, out, "04_batch_and_model_time.png", saved)


def plot_05_prompts(run: Run, out: Path, saved: list) -> None:
    if not run.requests:
        return
    fig, ax = plt.subplots(1, 2, figsize=(11, 4))
    p = run.col(run.requests, "prompts")
    ax[0].hist(p, bins=np.arange(0.5, p.max() + 1.5), color=COLORS["batch"], rwidth=0.9)
    ax[0].set(title="Prompts per request", xlabel="prompts", ylabel="requests")
    counts = collections.Counter(r["source"] for r in run.requests)
    ax[1].bar(list(counts), list(counts.values()), color=[COLORS[k] for k in counts])
    ax[1].set(
        title="Requests by source (task = AITask, api = generate() / HTTP)", ylabel="requests"
    )
    _save(fig, out, "05_prompts_and_requests.png", saved)


def plot_06_scaling(run: Run, out: Path, saved: list) -> None:
    if len(run.batches) < 2:
        return
    x = run.col(run.batches, "prompts")
    y = run.col(run.batches, "duration")
    fig, ax = plt.subplots(1, 3, figsize=(15, 4.5))
    cols = [COLORS.get(b["trigger"], COLORS["grey"]) for b in run.batches]
    ax[0].scatter(x, y, c=cols, s=18, alpha=0.8)
    if len(set(x)) >= 4:  # a line through two sizes would say nothing
        slope, icpt = np.polyfit(x, y, 1)
        xs = np.linspace(x.min(), x.max(), 50)
        ax[0].plot(
            xs,
            slope * xs + icpt,
            color="k",
            lw=1,
            label=f"fit: {icpt:.2f}s + {slope * 1000:.1f} ms/prompt",
        )
        ax[0].legend()
    ax[0].set(
        title="Batch time against batch size"
        + ("" if len(set(x)) >= 4 else " (too few sizes for a fit)"),
        xlabel="prompts in batch",
        ylabel="batch seconds",
    )
    m = [(b["prompts"], b["model_s"]) for b in run.batches if b["model_s"]]
    if m:
        ax[1].scatter(*zip(*m), s=18, color=COLORS["model"])
    ax[1].set(
        title="Dragon model time against batch size",
        xlabel="prompts in batch",
        ylabel="model seconds",
    )
    ax[2].scatter(x, y / np.maximum(x, 1), c=cols, s=18, alpha=0.8)
    ax[2].set(
        title="Seconds per prompt: does batching pay off?",
        xlabel="prompts in batch",
        ylabel="batch s / prompt",
    )
    _save(fig, out, "06_batch_size_scaling.png", saved)


def plot_07_request_scatter(run: Run, out: Path, saved: list) -> None:
    if not run.requests:
        return
    fig, ax = plt.subplots(1, 2, figsize=(14, 5))
    t = run.col(run.requests, "t_enqueue")
    q = run.col(run.requests, "queue")
    b = run.col(run.requests, "batch")
    sc = ax[0].scatter(t, q, c=b, cmap="viridis", s=12, alpha=0.8)
    plt.colorbar(sc, ax=ax[0], label="batch id")
    order = np.argsort(t)
    if len(t) >= 20:
        k = max(5, len(t) // 20)
        smooth = np.convolve(q[order], np.ones(k) / k, mode="valid")
        ax[0].plot(
            t[order][k // 2 : k // 2 + len(smooth)],
            smooth,
            color="k",
            lw=1.2,
            label=f"rolling mean ({k})",
        )
        ax[0].legend()
    ax[0].set(
        title="Every request: when it joined the queue, how long it waited",
        xlabel="seconds since session start",
        ylabel="queue wait (s)",
    )
    ax[1].scatter(run.col(run.requests, "t_dispatch"), q, c=b, cmap="viridis", s=12, alpha=0.8)
    ax[1].set(
        title="Same, by dispatch time (vertical stripes = one batch)",
        xlabel="seconds since session start",
        ylabel="queue wait (s)",
    )
    _save(fig, out, "07_every_request.png", saved)


def _queue_depth(run: Run):
    """Step function: requests that were waiting at each moment (reconstructed)."""
    marks = [(r["t_enqueue"], 1) for r in run.requests] + [
        (r["t_dispatch"], -1) for r in run.requests
    ]
    marks.sort()
    ts, depth, d = [], [], 0
    for t, delta in marks:
        d += delta
        ts.append(t)
        depth.append(d)
    return np.array(ts), np.array(depth)


def _tasks_in_flight(run: Run):
    marks = []
    for s in run.task_spans.values():
        marks += [(s["start_time_s"] - run.t0, 1), (s["end_time_s"] - run.t0, -1)]
    marks.sort()
    ts, n, d = [], [], 0
    for t, delta in marks:
        d += delta
        ts.append(t)
        n.append(d)
    return np.array(ts), np.array(n)


def plot_08_timeline(run: Run, out: Path, saved: list) -> None:
    if not run.batches:
        return
    fig, ax = plt.subplots(4, 1, figsize=(14, 10), sharex=True)
    for b in run.batches:
        ax[0].barh(
            0,
            b["duration"],
            left=b["t_start"],
            height=0.8,
            color=COLORS.get(b["trigger"], COLORS["grey"]),
            edgecolor="white",
            lw=0.3,
        )
        if b["timed_out"]:
            ax[0].barh(0, b["duration"], left=b["t_start"], height=0.8, color=COLORS["bad"])
    ax[0].set(
        yticks=[], title="Batches in flight (blue = size, orange = deadline, red = timed out)"
    )
    ts, depth = _queue_depth(run)
    ax[1].step(ts, depth, where="post", color=COLORS["queue"])
    ax[1].fill_between(ts, depth, step="post", alpha=0.2, color=COLORS["queue"])
    ax[1].set(
        ylabel="requests",
        title="Requests waiting in the batcher queue (reconstructed from queue waits)",
    )
    if run.task_spans:
        ts2, n = _tasks_in_flight(run)
        ax[2].step(ts2, n, where="post", color=COLORS["task"])
        ax[2].set(ylabel="tasks", title="Tasks in flight (open task spans)")
    else:
        _note(ax[2], "no task spans")
    ends = sorted(run.requests, key=lambda r: r["t_dispatch"])
    cum = np.cumsum([r["prompts"] for r in ends])
    ax[3].step([r["t_dispatch"] for r in ends], cum, where="post", color=COLORS["model"])
    ax[3].set(
        ylabel="prompts",
        xlabel="seconds since session start",
        title="Prompts sent to the model, cumulative",
    )
    for a in ax[1:]:
        _shade_batches(a, run)
    _save(fig, out, "08_timeline.png", saved)


def plot_09_throughput(run: Run, out: Path, saved: list) -> None:
    if len(run.batches) < 2:
        return
    fig, ax = plt.subplots(1, 3, figsize=(15, 4.5))
    span = max(b["t_start"] + b["duration"] for b in run.batches)
    width = max(1.0, span / 40)
    edges = np.arange(0, span + width, width)
    t = np.array([b["t_start"] + b["duration"] for b in run.batches])
    w = np.array([b["prompts"] for b in run.batches], dtype=float)
    hist, _ = np.histogram(t, bins=edges, weights=w)
    ax[0].bar(edges[:-1], hist / width, width=width * 0.9, align="edge", color=COLORS["model"])
    ax[0].set(
        title=f"Prompts finished per second (bins of {width:.1f} s)",
        xlabel="seconds since session start",
        ylabel="prompts / s",
    )
    bs = sorted(run.batches, key=lambda b: b["t_start"])
    gaps = np.array([n["t_start"] - (p["t_start"] + p["duration"]) for p, n in zip(bs, bs[1:])])
    ax[1].hist(gaps, bins=30, color=COLORS["queue"])
    ax[1].set(
        title="Gap between one batch ending and the next starting\n(0 = back to back; > 0 = the batcher was idle)",
        xlabel="seconds",
        ylabel="batch pairs",
    )
    busy = sum(b["duration"] for b in bs)
    ax[2].pie(
        [busy, max(span - busy, 0)],
        labels=["a batch in flight", "idle"],
        colors=[COLORS["batch"], "#dddddd"],
        autopct="%1.0f%%",
    )
    ax[2].set_title("Share of the session with a batch running")
    _save(fig, out, "09_throughput_and_gaps.png", saved)


def plot_10_decomposition(run: Run, out: Path, saved: list) -> None:
    rows = []
    batch_dur = {b["id"]: b["duration"] for b in run.batches}
    for r in run.requests:
        s = run.task_spans.get(r["task_id"]) if r["task_id"] else None
        if s is None:
            continue
        total = s["end_time_s"] - s["start_time_s"]
        batch = batch_dur[r["batch"]]
        rows.append(
            (s["start_time_s"], r["queue"], batch, max(total - r["queue"] - batch, 0.0), total)
        )
    if not rows:
        return
    rows.sort()
    arr = np.array(rows)
    fig, ax = plt.subplots(1, 2, figsize=(14, 5))
    step = max(1, len(arr) // 150)
    sample = arr[::step]
    x = np.arange(len(sample))
    ax[0].bar(x, sample[:, 1], color=COLORS["queue"], label="queue wait")
    ax[0].bar(x, sample[:, 2], bottom=sample[:, 1], color=COLORS["batch"], label="batch time")
    ax[0].bar(
        x,
        sample[:, 3],
        bottom=sample[:, 1] + sample[:, 2],
        color=COLORS["grey"],
        label="rest (submit, routing, callbacks)",
    )
    ax[0].set(
        title="Where each task's time went ("
        + ("every task" if step == 1 else f"every {step}th task")
        + ", in submit order)",
        xlabel="task",
        ylabel="seconds",
    )
    ax[0].legend()
    _box(
        ax[1],
        [arr[:, 1], arr[:, 2], arr[:, 3], arr[:, 4]],
        ["queue wait", "batch time", "rest", "task total"],
    )
    ax[1].set(title="Components of task latency", ylabel="seconds")
    _save(fig, out, "10_task_latency_decomposition.png", saved)


def plot_11_lifecycle(run: Run, out: Path, saved: list) -> None:
    order = ["TaskCreated", "TaskSubmitted", "TaskQueued", "TaskStarted", "TaskCompleted"]
    per: dict = collections.defaultdict(dict)
    for name in order:
        for e in run.by_type.get(name, []):
            per[e["task_id"]][name] = e["event_time"]
    phases = {}
    for a, b in zip(order, order[1:]):
        d = [t[b] - t[a] for t in per.values() if a in t and b in t]
        if d:
            phases[f"{a[4:]} -> {b[4:]}"] = np.array(d)
    if not phases:
        return
    fig, ax = plt.subplots(1, 2, figsize=(14, 4.5))
    _box(ax[0], list(phases.values()), list(phases))
    ax[0].set(
        title="Task lifecycle phases (the 'rest' of plot 10 is made of these)", ylabel="seconds"
    )
    ax[0].tick_params(axis="x", labelrotation=15)
    for k, v in phases.items():
        _ecdf(ax[1], v, k)
    ax[1].set(xscale="log", title="Cumulative", xlabel="seconds", ylabel="fraction of tasks")
    ax[1].legend(fontsize=7)
    _save(fig, out, "11_task_lifecycle.png", saved)


def plot_12_hosts(run: Run, out: Path, saved: list) -> None:
    hosts = sorted({b["host"] for b in run.batches if b["host"]})
    if not hosts:
        return
    fig, ax = plt.subplots(1, 3, figsize=(14, 4.5))
    ax[0].bar(
        hosts, [sum(1 for b in run.batches if b["host"] == h) for h in hosts], color=COLORS["batch"]
    )
    ax[0].set(title="Batches per Dragon worker host", ylabel="batches")
    _box(ax[1], [[b["duration"] for b in run.batches if b["host"] == h] for h in hosts], hosts)
    ax[1].set(title="Batch time per host", ylabel="seconds")
    _box(
        ax[2],
        [
            [b["model_s"] for b in run.batches if b["host"] == h and b["model_s"]] or [0]
            for h in hosts
        ],
        hosts,
    )
    ax[2].set(title="Model time per host", ylabel="seconds")
    _save(fig, out, "12_worker_hosts.png", saved)


def plot_13_resources(run: Run, out: Path, saved: list) -> None:
    res = run.by_type.get("ResourceUpdate", [])
    if not res:
        return
    nodes = sorted({e["node_id"] for e in res if e.get("node_id")})
    fig, ax = plt.subplots(3, 1, figsize=(14, 9), sharex=True)
    for node in nodes:
        per = [e for e in res if e["node_id"] == node and e.get("gpu_id") is None]
        t = [e["event_time"] - run.t0 for e in per]
        ax[0].plot(t, [e.get("cpu_percent") or 0 for e in per], label=node)
        ax[1].plot(t, [e.get("memory_percent") or 0 for e in per], label=node)
        for gid in sorted(
            {e["gpu_id"] for e in res if e["node_id"] == node and e.get("gpu_id") is not None}
        ):
            g = [e for e in res if e["node_id"] == node and e.get("gpu_id") == gid]
            ax[2].plot(
                [e["event_time"] - run.t0 for e in g],
                [e.get("gpu_percent") or 0 for e in g],
                label=f"{node} gpu{gid}",
            )
    ax[0].set(
        ylabel="CPU %",
        title="CPU, memory and GPU utilisation with batches shaded: does the GPU work when a batch is in flight?",
    )
    ax[1].set(ylabel="memory %")
    ax[2].set(ylabel="GPU %", xlabel="seconds since session start")
    for a in ax:
        _shade_batches(a, run)
        if a.get_legend_handles_labels()[0]:
            a.legend(fontsize=7, ncol=4)
    _save(fig, out, "13_resources_vs_batches.png", saved)


def plot_14_trust(run: Run, out: Path, saved: list) -> None:
    fig, ax = plt.subplots(1, 3, figsize=(16, 4.8))
    last = run.metrics[-1] if run.metrics else {}
    n_req, n_prompts = len(run.requests), sum(r["prompts"] for r in run.requests)
    pairs = [
        ("batches", len(run.batches), last.get("rhapsody.inference.batches")),
        ("requests", n_req, last.get("rhapsody.inference.requests")),
        ("prompts", n_prompts, last.get("rhapsody.inference.prompts")),
        ("queue histogram", n_req, last.get("rhapsody.inference.request.queue.duration_count")),
        ("batch histogram", len(run.batches), last.get("rhapsody.inference.batch.duration_count")),
    ]
    pairs = [p for p in pairs if p[2] is not None]
    if pairs:
        x = np.arange(len(pairs))
        ax[0].bar(
            x - 0.2, [p[1] for p in pairs], 0.4, label="counted from events", color=COLORS["batch"]
        )
        ax[0].bar(
            x + 0.2,
            [p[2] for p in pairs],
            0.4,
            label="metric (last JSONL snapshot)",
            color=COLORS["model"],
        )
        ax[0].set_xticks(x, [p[0] for p in pairs], rotation=15)
        ax[0].legend(fontsize=7)
        ax[0].set_title("Events agree with metrics (bars must match)")
    else:
        _note(ax[0], "no inference metrics in the JSONL snapshots")
    pts = []
    by_id = {b["id"]: b for b in run.batches}
    for s in run.batch_spans:
        b = by_id.get(s["attributes"].get("rhapsody.inference.batch.id"))
        if b:
            pts.append((b["duration"] * 1000, s["duration_ms"]))
    if pts:
        x, y = zip(*pts)
        ax[1].scatter(x, y, s=16, color=COLORS["batch"])
        lim = [0, max(max(x), max(y)) * 1.05]
        ax[1].plot(lim, lim, "k--", lw=0.8)
        ax[1].set(
            title="Batch span duration against the end event duration (on the line = agree)",
            xlabel="event batch_duration_s x 1000 (ms)",
            ylabel="span duration (ms)",
        )
    else:
        _note(ax[1], "no batch spans")
    links = []
    for s in run.batch_spans:
        b = s["attributes"].get("rhapsody.inference.batch.id")
        task_reqs = sum(1 for r in run.requests if r["batch"] == b and r["source"] == "task")
        links.append((task_reqs, s.get("links", s.get("n_links"))))
    if links and all(isinstance(v, int) for _, v in links):
        x, y = zip(*links)
        ax[2].scatter(x, y, s=16, color=COLORS["model"])
        ax[2].plot([0, max(x) + 1], [0, max(x) + 1], "k--", lw=0.8)
        ax[2].axhline(128, color=COLORS["bad"], ls=":", label="link cap 128")
        ax[2].legend()
        ax[2].set(
            title="Span links against task requests in the batch",
            xlabel="task requests",
            ylabel="links",
        )
    else:
        _note(
            ax[2],
            "span links are not in the JSONL spans\n(the verify script checks them on read_traces())",
        )
    _save(fig, out, "14_events_vs_metrics_vs_spans.png", saved)


def plot_15_volume(run: Run, out: Path, saved: list) -> None:
    if not run.bytes_by_type:
        return
    fig, ax = plt.subplots(1, 3, figsize=(16, 4.8))
    items = run.bytes_by_type.most_common()
    ax[0].barh([k for k, _ in items][::-1], [v for _, v in items][::-1], color=COLORS["batch"])
    ax[0].set(xscale="log", title="Bytes in the JSONL file, by record type", xlabel="bytes")
    cnt = collections.Counter(e["event_type"] for e in run.events)
    ax[1].barh(list(cnt)[::-1], list(cnt.values())[::-1], color=COLORS["model"])
    ax[1].set(xscale="log", title="Event counts", xlabel="events")
    n_req = max(len(run.requests), 1)
    inf_bytes = sum(v for k, v in run.bytes_by_type.items() if k.startswith(PREFIX))
    ended = sum(run.bytes_by_type.get(kind, 0) for kind in ENDED)
    started = run.bytes_by_type.get(STARTED, 0)
    lines = [
        f"JSONL total: {run.jsonl_bytes / 1e6:.2f} MB",
        f"OTLP export: {run.otlp_bytes / 1e6:.2f} MB",
        f"{len(run.events)} events, {run.jsonl_bytes / max(len(run.events), 1):.0f} B avg line",
        f"BatchDone/Failed: {ended / n_req:.0f} B per request",
        f"BatchStarted: {started / n_req:.0f} B per request",
        f"inference events: {inf_bytes / 1e3:.1f} kB",
        f"all spans: {run.bytes_by_type.get('span', 0) / 1e3:.1f} kB",
    ]
    ax[2].set_axis_off()
    ax[2].text(
        0, 1, "\n".join(lines), va="top", family="monospace", fontsize=8, transform=ax[2].transAxes
    )
    ax[2].set_title("What the telemetry costs on disk")
    _save(fig, out, "15_data_volume.png", saved)


def plot_16_timeouts(run: Run, out: Path, saved: list) -> None:
    if not any(b["timed_out"] for b in run.batches):
        return
    fig, ax = plt.subplots(1, 2, figsize=(12, 4.5))
    ax[0].bar(
        [b["id"] for b in run.batches], [b["timed_out"] for b in run.batches], color=COLORS["bad"]
    )
    ax[0].set(title="Timed-out prompts per batch", xlabel="batch id", ylabel="prompts")
    ok = sum(r["prompts"] - r["timed_out"] for r in run.requests)
    bad = sum(r["timed_out"] for r in run.requests)
    ax[1].pie(
        [ok, bad],
        labels=["answered", "timed out"],
        colors=[COLORS["model"], COLORS["bad"]],
        autopct="%1.1f%%",
    )
    ax[1].set_title("Prompts: answered against timed out")
    _save(fig, out, "16_timeouts.png", saved)


PLOTS = [
    plot_01_overview, plot_02_batches, plot_03_queue_wait, plot_04_times, plot_05_prompts,
    plot_06_scaling, plot_07_request_scatter, plot_08_timeline, plot_09_throughput,
    plot_10_decomposition, plot_11_lifecycle, plot_12_hosts, plot_13_resources, plot_14_trust,
    plot_15_volume, plot_16_timeouts,
]  # fmt: skip


def plot_run(run: Run, out: Path) -> list:
    out.mkdir(parents=True, exist_ok=True)
    saved: list = []
    for fn in PLOTS:
        try:
            before = len(saved)
            fn(run, out, saved)
            if len(saved) == before:
                print(f"  skipped {fn.__name__[5:]}: no data")
        except Exception as exc:  # one broken plot must not stop the others
            plt.close("all")
            print(f"  FAILED {fn.__name__[5:]}: {exc!r}")
    return saved


# ---------------------------------------------------------------------------------------------
# Across runs
# ---------------------------------------------------------------------------------------------


def _group(name: str) -> str:
    return re.sub(r"[-_]?\d+$", "", name)


def plot_90_overhead(runs: list[Run], out: Path, saved: list) -> None:
    """Wall times of runs named ``<prefix>-off-N`` and ``<prefix>-on-N`` (telemetry off and on).

    Pass the folder that holds the ``4-Overhead`` folder (the benchmark's ``--out``), not
    ``4-Overhead`` itself, or the names lose their prefix and nothing is drawn. A third arm
    ``<prefix>-nopoll-N`` (telemetry on without resource sampling) is drawn if an older run has it.
    """
    arms = ("off", "on", "nopoll")
    labels = {"off": "telemetry off", "on": "telemetry on", "nopoll": "on, no resource sampling"}
    colors = {"off": COLORS["grey"], "on": COLORS["batch"], "nopoll": COLORS["model"]}
    groups: dict = collections.defaultdict(lambda: {a: [] for a in arms})
    for r in runs:
        match = re.fullmatch(r"(.*?)[-_](off|on|nopoll)[-_]?\d*", r.name)
        if match and r.wall_s is not None:
            groups[match.group(1)][match.group(2)].append(r.wall_s)
    groups = {k: v for k, v in groups.items() if v["off"] and (v["on"] or v["nopoll"])}
    if not groups:
        return
    fig, ax = plt.subplots(1, len(groups), figsize=(6.5 * len(groups), 5.2), squeeze=False)
    for a, (prefix, d) in zip(ax[0], groups.items()):
        present = [k for k in arms if d[k]]
        notes = []
        for i, kind in enumerate(present):
            w = d[kind]
            a.bar(i, np.mean(w), color=colors[kind], alpha=0.5)
            a.scatter([i] * len(w), w, color="k", s=22, zorder=3)
            a.text(i, max(w), f"{np.mean(w):.2f}s (n={len(w)})", ha="center", va="bottom")
            if kind != "off":
                diff = np.mean(w) - np.mean(d["off"])
                notes.append(f"{kind} {diff:+.2f} s ({100 * diff / np.mean(d['off']):+.1f}%)")
        a.set_xticks(range(len(present)), [labels[k] for k in present], rotation=10)
        a.set(ylabel="workload wall time (s)", title=f"{prefix}: " + ", ".join(notes))
    _save(fig, out, "90_telemetry_overhead.png", saved)


def plot_91_compare(runs: list[Run], out: Path, saved: list) -> None:
    runs = [r for r in runs if r.has_telemetry and r.batches]
    if len(runs) < 2:
        return
    fig, ax = plt.subplots(2, 3, figsize=(16, 8))
    names = [r.name for r in runs]
    x = np.arange(len(runs))

    def bar(a, vals, title, ylabel):
        a.bar(x, vals, color=COLORS["batch"])
        a.set_xticks(x, names, rotation=20, ha="right", fontsize=7)
        a.set(title=title, ylabel=ylabel)

    bar(ax[0, 0], [len(r.batches) for r in runs], "Batches", "batches")
    bar(
        ax[0, 1],
        [
            sum(q["prompts"] for q in r.requests) / max(r.wall_s or (r.t_end - r.t0), 1e-9)
            for r in runs
        ],
        "Prompts per second (workload wall time)",
        "prompts / s",
    )
    bar(
        ax[0, 2],
        [r.jsonl_bytes / max(len(r.requests), 1) for r in runs],
        "JSONL bytes per request (all telemetry)",
        "bytes",
    )
    for a, key, title in ((ax[1, 0], "queue", "Queue wait p50 / p90 / p99"),):
        for j, p in enumerate((50, 90, 99)):
            a.bar(
                x + (j - 1) * 0.27,
                [np.percentile(r.col(r.requests, key), p) for r in runs],
                0.27,
                label=f"p{p}",
            )
        a.set_xticks(x, names, rotation=20, ha="right", fontsize=7)
        a.set(title=title, ylabel="seconds", yscale="log")
        a.legend()
    for j, p in enumerate((50, 90, 99)):
        ax[1, 1].bar(
            x + (j - 1) * 0.27,
            [np.percentile(r.col(r.batches, "duration"), p) for r in runs],
            0.27,
            label=f"p{p}",
        )
    ax[1, 1].set_xticks(x, names, rotation=20, ha="right", fontsize=7)
    ax[1, 1].set(title="Batch time p50 / p90 / p99", ylabel="seconds")
    ax[1, 1].legend()
    bar(
        ax[1, 2],
        [np.mean([b["requests"] for b in r.batches]) for r in runs],
        "Mean requests per batch",
        "requests",
    )
    _save(fig, out, "91_run_comparison.png", saved)


# ---------------------------------------------------------------------------------------------
# Command line
# ---------------------------------------------------------------------------------------------


def _is_run(p: Path) -> bool:
    """A run directory holds the telemetry files (or an inference-telemetry/ folder) or a log."""
    return bool(
        glob.glob(str(p / "*.telemetry.jsonl"))
        or (p / "inference-telemetry").is_dir()
        or (p / "inference_telemetry_report.json").exists()
        or _wall_from_log(p) is not None
    )


def _find_runs(paths: list[str]) -> list[tuple[Path, str]]:
    """(directory, name) of every run under the given paths; nested names join with an
    underscore."""
    found: list[tuple[Path, str]] = []
    for root in map(Path, paths):
        if _is_run(root):
            found.append((root, root.name))
            continue
        for dirpath, dirnames, _files in os.walk(root):
            dirnames[:] = sorted(d for d in dirnames if d != "plots")
            here = Path(dirpath)
            if here != root and _is_run(here):
                found.append((here, "_".join(here.relative_to(root).parts)))
                dirnames[:] = []  # a run's own sub-folders are not runs
    return found


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("paths", nargs="+", help="a run directory, or a folder of run directories")
    ap.add_argument("--out", help="where the plots go (default: <first path>/plots)")
    args = ap.parse_args(argv)
    dirs = _find_runs(args.paths)
    if not dirs:
        sys.exit("no run directories found")
    base = Path(args.out) if args.out else Path(args.paths[0]) / "plots"
    runs = [Run(d, name) for d, name in dirs]
    for run in runs:
        if not run.has_telemetry:
            print(
                f"{run.name}: no telemetry file (wall time {run.wall_s}); used only in the overhead plot"
            )
            continue
        print(
            f"{run.name}: {len(run.events)} events, {len(run.batches)} batches, {len(run.requests)} requests"
        )
        saved = plot_run(run, base / run.name)
        print(f"  {len(saved)} plots -> {base / run.name}")
    cross: list = []
    base.mkdir(parents=True, exist_ok=True)
    for fn in (plot_90_overhead, plot_91_compare):
        try:
            fn(runs, base, cross)
        except Exception as exc:
            plt.close("all")
            print(f"FAILED {fn.__name__}: {exc!r}")
    if cross:
        print(f"across runs: {cross} -> {base}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
