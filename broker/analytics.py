from __future__ import annotations

import json
import math
import sqlite3
from collections import defaultdict
from typing import Any, Iterable


DEFAULT_WINDOWS = (300, 1_800, 10_800, 86_400)


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, math.ceil(percentile * len(ordered)) - 1)
    return round(ordered[index], 6)


def _latencies(values: Iterable[float]) -> dict[str, Any]:
    normalized = [max(0.0, float(value)) for value in values]
    if not normalized:
        return {"count": 0, "avg_seconds": None, "p50_seconds": None,
                "p95_seconds": None, "max_seconds": None}
    return {
        "count": len(normalized),
        "avg_seconds": round(sum(normalized) / len(normalized), 6),
        "p50_seconds": _percentile(normalized, 0.50),
        "p95_seconds": _percentile(normalized, 0.95),
        "max_seconds": round(max(normalized), 6),
    }


def audit_history(
    db: sqlite3.Connection,
    *,
    limit: int = 100,
    job_id: str | None = None,
    source: str | None = None,
    since: float | None = None,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    values: list[Any] = []
    if job_id is not None:
        clauses.append("job_id=?")
        values.append(job_id)
    if source is not None:
        clauses.append("source=?")
        values.append(source)
    if since is not None:
        clauses.append("occurred>=?")
        values.append(since)
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    bounded_limit = max(1, min(int(limit), 1_000))
    rows = db.execute(
        "SELECT sequence,occurred,event_type,job_id,source,attempt_no,"
        "from_state,to_state,reason,metadata_json FROM audit_events"
        f"{where} ORDER BY sequence DESC LIMIT ?",
        (*values, bounded_limit),
    )
    events = []
    for row in rows:
        event = dict(row)
        raw_metadata = event.pop("metadata_json")
        event["metadata"] = json.loads(raw_metadata) if raw_metadata else {}
        events.append(event)
    return events


def attempt_history(db: sqlite3.Connection, job_id: str) -> list[dict[str, Any]]:
    rows = db.execute(
        "SELECT job_id,attempt_no,source,queued_at,selected_at,started,lease_until,"
        "finished,outcome,error,scheduler_mode,selection_reason,policy_json "
        "FROM job_attempts WHERE job_id=? ORDER BY attempt_no",
        (job_id,),
    )
    attempts = []
    for row in rows:
        attempt = dict(row)
        raw_policy = attempt.pop("policy_json")
        attempt["scheduler_context"] = json.loads(raw_policy) if raw_policy else None
        attempts.append(attempt)
    return attempts


def _source_names(
    db: sqlite3.Connection, policy_snapshot: dict[str, Any] | None
) -> set[str]:
    names = {row[0] for row in db.execute("SELECT DISTINCT source FROM jobs")}
    if policy_snapshot:
        names.update(policy_snapshot.get("sources", {}))
    return names


def _current_by_source(db: sqlite3.Connection, source: str) -> dict[str, int]:
    counts = {state: count for state, count in db.execute(
        "SELECT state,count(*) FROM jobs WHERE source=? GROUP BY state", (source,)
    )}
    return {
        "queue_depth": counts.get("queued", 0),
        "running": counts.get("running", 0) + counts.get("cancel_requested", 0),
        "completed": counts.get("completed", 0),
        "failed": counts.get("failed", 0),
        "cancelled": counts.get("cancelled", 0),
    }


def _window_for_source(
    db: sqlite3.Connection, source: str, since: float, window_seconds: int
) -> dict[str, Any]:
    admissions = db.execute(
        "SELECT count(*) FROM jobs WHERE source=? AND created>=?", (source, since)
    ).fetchone()[0]
    terminals = dict(db.execute(
        "SELECT substr(event_type,5),count(*) FROM audit_events "
        "WHERE source=? AND occurred>=? AND event_type IN "
        "('job.completed','job.failed','job.cancelled') GROUP BY event_type",
        (source, since),
    ))
    attempts = list(db.execute(
        "SELECT queued_at,started,finished,outcome FROM job_attempts "
        "WHERE source=? AND selected_at>=?", (source, since)
    ))
    retry_count = db.execute(
        "SELECT count(*) FROM audit_events WHERE source=? AND occurred>=? "
        "AND event_type='job.retry_requested'", (source, since)
    ).fetchone()[0]
    requeue_count = db.execute(
        "SELECT count(*) FROM audit_events WHERE source=? AND occurred>=? "
        "AND event_type='job.requeued' AND reason!='explicit retry'", (source, since)
    ).fetchone()[0]
    end_to_end = [row[0] for row in db.execute(
        "SELECT finished-created FROM jobs WHERE source=? AND finished>=? "
        "AND finished IS NOT NULL", (source, since)
    )]
    completed = terminals.get("completed", 0)
    failed = terminals.get("failed", 0)
    cancelled = terminals.get("cancelled", 0)
    terminal_count = completed + failed + cancelled
    return {
        "admitted": admissions,
        "dispatched": len(attempts),
        "completed": completed,
        "failed": failed,
        "cancelled": cancelled,
        "retries": retry_count,
        "requeues": requeue_count,
        "throughput_per_hour": round(completed * 3_600 / window_seconds, 6),
        "success_rate": round(completed / terminal_count, 6) if terminal_count else None,
        "queue_wait": _latencies(
            row[1] - row[0] for row in attempts if row[0] is not None and row[1] is not None
        ),
        "inference": _latencies(
            row[2] - row[1] for row in attempts if row[1] is not None and row[2] is not None
        ),
        "end_to_end": _latencies(end_to_end),
    }


def _scheduler_window(
    db: sqlite3.Connection, since: float
) -> dict[str, Any]:
    rows = db.execute(
        "SELECT source,metadata_json FROM audit_events "
        "WHERE event_type='scheduler.selected' AND occurred>=? ORDER BY sequence",
        (since,),
    )
    selected: dict[str, int] = defaultdict(int)
    weighted_selected: dict[str, int] = defaultdict(int)
    expected: dict[str, float] = defaultdict(float)
    modes: dict[str, int] = defaultdict(int)
    total = 0
    for source, raw_metadata in rows:
        metadata = json.loads(raw_metadata) if raw_metadata else {}
        total += 1
        selected[source] += 1
        mode = metadata.get("mode", "unknown")
        modes[mode] += 1
        weights = metadata.get("active_weights") or {}
        eligible = metadata.get("eligible_sources") or []
        eligible_weights = {
            name: float(weights.get(name, 1.0)) for name in eligible
        }
        weight_total = sum(eligible_weights.values())
        if mode == "weighted_round_robin" and weight_total > 0:
            weighted_selected[source] += 1
            for name, weight in eligible_weights.items():
                expected[name] += weight / weight_total
    weighted_total = sum(weighted_selected.values())
    names = set(selected) | set(expected)
    sources = {}
    for name in sorted(names):
        actual_share = selected[name] / total if total else 0.0
        fairness_actual_share = (
            weighted_selected[name] / weighted_total if weighted_total else None
        )
        expected_share = expected[name] / weighted_total if weighted_total else None
        sources[name] = {
            "selected": selected[name],
            "actual_share": round(actual_share, 6),
            "weighted_selected": weighted_selected[name],
            "fairness_actual_share": (
                round(fairness_actual_share, 6)
                if fairness_actual_share is not None else None
            ),
            "expected_share": round(expected_share, 6) if expected_share is not None else None,
            "fairness_ratio": (
                round(fairness_actual_share / expected_share, 6)
                if expected_share not in (None, 0.0) else None
            ),
            "absolute_share_error": (
                round(abs(fairness_actual_share - expected_share), 6)
                if expected_share is not None and fairness_actual_share is not None else None
            ),
        }
    return {"selections": total, "weighted_selections": weighted_total,
            "modes": dict(modes), "sources": sources}


def analytics_snapshot(
    db: sqlite3.Connection,
    *,
    now: float,
    policy_snapshot: dict[str, Any] | None = None,
    windows: Iterable[int] = DEFAULT_WINDOWS,
) -> dict[str, Any]:
    normalized_windows = tuple(sorted({int(value) for value in windows if int(value) > 0}))
    source_names = _source_names(db, policy_snapshot)
    sources: dict[str, Any] = {}
    for source in sorted(source_names):
        sources[source] = {
            "current": _current_by_source(db, source),
            "windows": {
                str(window): _window_for_source(db, source, now - window, window)
                for window in normalized_windows
            },
        }
    return {
        "timestamp": now,
        "windows_seconds": list(normalized_windows),
        "sources": sources,
        "scheduler": {
            "active_policy": policy_snapshot,
            "windows": {
                str(window): _scheduler_window(db, now - window)
                for window in normalized_windows
            },
        },
    }
