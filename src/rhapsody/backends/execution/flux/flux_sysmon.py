"""Read the flux ``sysmon`` shell plugin's per-job resource summary.

With the shell option ``sysmon`` set, every job shell logs two lines into the
job's ``guest.output`` eventlog when its tasks exit::

    log rank=0 component="sysmon" message="memory.peak=205.60156M"
    log rank=0 component="sysmon" message="loadavg-overall=0.98"

The values describe the cgroup that contains the job shell.  That cgroup is
the job's own only when the instance launches jobs via sdexec
(``exec.service = "sdexec"``); otherwise it is the broker's cgroup, shared
with everything else on the node (flux-framework/flux-core#7835).  The
``scope`` field of the summary records which case applies.
"""

from __future__ import annotations

import errno
import time
from typing import Any

_SUFFIXES = {"": 0, "K": 1, "M": 2, "G": 3, "T": 4, "P": 5, "E": 6}


def parse_size(text: str) -> int:
    """Parse flux's ``encode_size()`` output (``"205.60156M"``) into bytes."""
    text = text.strip()
    suffix = text[-1:] if text[-1:] in _SUFFIXES and text[-1:] else ""
    number = text[: len(text) - len(suffix)]
    return int(float(number) * 1024 ** _SUFFIXES[suffix])


def summarize(entries: list[tuple[int, str]], scope: str) -> dict | None:
    """Turn ``(rank, message)`` sysmon log entries into a usage summary.

    Returns:
        ``None`` if no entry parsed, else::

            {"memory_peak_bytes": sum over ranks,
             "cpu_loadavg": sum over ranks (average busy cores),
             "per_rank": {rank: {"memory_peak_bytes": .., "cpu_loadavg": ..}},
             "scope": scope}

        Sums are job totals across nodes; the memory sum is an upper bound,
        since the per-node peaks need not coincide.
    """
    per_rank: dict[int, dict] = {}
    for rank, message in entries:
        key, _, value = message.partition("=")
        try:
            if key == "memory.peak":
                per_rank.setdefault(rank, {})["memory_peak_bytes"] = parse_size(value)
            elif key == "loadavg-overall":
                per_rank.setdefault(rank, {})["cpu_loadavg"] = float(value)
        except ValueError:
            continue
    if not per_rank:
        return None
    summary: dict[str, Any] = {"per_rank": per_rank, "scope": scope}
    for field in ("memory_peak_bytes", "cpu_loadavg"):
        values = [r[field] for r in per_rank.values() if field in r]
        summary[field] = sum(values) if values else None
    return summary


def read_entries(flux_job: Any, handle: Any, jobid: Any, timeout: float) -> list[tuple[int, str]]:
    """Collect the sysmon log entries of a finished job.

    Args:
        flux_job: The ``flux.job`` module.
        handle:   Flux handle.
        jobid:    Flux job id.
        timeout:  Overall deadline in seconds; entries read so far are
                  returned when it expires.
    """
    entries: list[tuple[int, str]] = []
    watcher = flux_job.event_watch_async(handle, jobid, "guest.output")
    deadline = time.monotonic() + timeout
    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                watcher.wait_for(remaining)
            except OSError as exc:
                if exc.errno == errno.ETIMEDOUT:
                    break
                raise
            event = watcher.get_event()
            if event is None:
                return entries
            ctx = event.context
            if event.name == "log" and ctx.get("component") == "sysmon":
                entries.append((int(ctx.get("rank", 0)), str(ctx.get("message", ""))))
    finally:
        try:
            watcher.cancel()
        except Exception:  # noqa: BLE001, S110
            pass
    return entries
