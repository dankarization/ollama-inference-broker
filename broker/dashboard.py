"""Payload-free operational snapshot and HTML dashboard for the broker."""
from __future__ import annotations

import html
import sqlite3
from typing import Any

def snapshot(
    db: sqlite3.Connection, *, now: float, policy_snapshot: dict[str, Any] | None
) -> dict[str, Any]:
    """Return operational data only; request payloads, results and errors are excluded."""
    policy_sources = (policy_snapshot or {}).get("sources", {})
    policy_active = policy_snapshot is not None
    source_names = {row[0] for row in db.execute("SELECT DISTINCT source FROM jobs")}
    source_names.update(policy_sources)
    schedules = dict(db.execute("SELECT source,next_allowed FROM source_schedules"))
    sources: list[dict[str, Any]] = []
    overall = {name: 0 for name in ("queued", "running", "lease", "retry", "delayed", "completed", "failed", "dead", "cancelled")}

    for source in sorted(source_names):
        configured = policy_sources.get(source)
        counts = dict(db.execute(
            "SELECT state,count(*) FROM jobs WHERE source=? GROUP BY state", (source,)
        ))
        queued = counts.get("queued", 0)
        lease = db.execute(
            "SELECT count(*) FROM jobs WHERE source=? AND state IN ('running','cancel_requested') "
            "AND lease_until IS NOT NULL", (source,)
        ).fetchone()[0]
        retry = db.execute(
            "SELECT count(*) FROM jobs WHERE source=? AND state='queued' AND retry_count>0", (source,)
        ).fetchone()[0]
        next_allowed = schedules.get(source)
        delayed = queued if next_allowed is not None and next_allowed > now else 0
        completed_1h = db.execute(
            "SELECT count(*) FROM jobs WHERE source=? AND state='completed' AND finished>=?",
            (source, now - 3_600),
        ).fetchone()[0]
        completed_24h = db.execute(
            "SELECT count(*) FROM jobs WHERE source=? AND state='completed' AND finished>=?",
            (source, now - 86_400),
        ).fetchone()[0]
        states = {
            "queued": queued,
            "running": counts.get("running", 0) + counts.get("cancel_requested", 0),
            "lease": lease,
            "retry": retry,
            "delayed": delayed,
            "completed": counts.get("completed", 0),
            "failed": counts.get("failed", 0),
            # The broker has no separate `dead` state: terminal jobs are failed.
            "dead": 0,
            "cancelled": counts.get("cancelled", 0),
        }
        for name, value in states.items():
            overall[name] += value
        sources.append({
            "source": source,
            "scheduler": {
                "enabled": configured.get("enabled") if isinstance(configured, dict) else (False if policy_active else None),
                "weight": configured.get("weight") if isinstance(configured, dict) else None,
                "next_allowed": next_allowed,
            },
            "states": states,
            "completed_last_hour": completed_1h,
            "completed_last_24_hours": completed_24h,
        })

    active_jobs = [dict(row) for row in db.execute(
        "SELECT id,source,state,created,started,lease_until,attempt_count,retry_count "
        "FROM jobs WHERE state IN ('running','cancel_requested') ORDER BY started,created,id"
    )]
    return {
        "timestamp": now,
        "sources": sources,
        "active_jobs": active_jobs,
        "overall": {
            "states": overall,
            "completed_last_hour": sum(item["completed_last_hour"] for item in sources),
            "completed_last_24_hours": sum(item["completed_last_24_hours"] for item in sources),
        },
    }


def render(data: dict[str, Any]) -> bytes:
    """Render a self-contained, intentionally local-only dashboard."""
    def cell(value: Any) -> str:
        if value is None:
            return "—"
        if isinstance(value, bool):
            return "yes" if value else "no"
        return html.escape(str(value))

    rows = []
    for item in data["sources"]:
        scheduler, states = item["scheduler"], item["states"]
        rows.append("<tr>" + "".join(f"<td>{cell(value)}</td>" for value in (
            item["source"], scheduler["enabled"], scheduler["weight"],
            states["queued"], states["running"], states["lease"], states["retry"], states["delayed"],
            states["failed"], states["dead"], states["cancelled"], states["completed"],
            item["completed_last_hour"], item["completed_last_24_hours"],
        )) + "</tr>")
    active = data["active_jobs"]
    active_rows = "".join(
        "<tr>" + "".join(f"<td>{cell(job[key])}</td>" for key in
        ("id", "source", "state", "started", "lease_until", "attempt_count", "retry_count")) + "</tr>"
        for job in active
    ) or "<tr><td colspan=7>None</td></tr>"
    overall = data["overall"]
    document = f"""<!doctype html><html lang=en><meta charset=utf-8>
<meta http-equiv=refresh content=15><title>Ollama broker queue</title>
<style>body{{font:14px system-ui,sans-serif;margin:2rem;color:#18212b}}table{{border-collapse:collapse;width:100%;margin:1rem 0}}th,td{{padding:.45rem;border:1px solid #ccd6df;text-align:right}}th:first-child,td:first-child{{text-align:left}}th{{background:#edf3f7}}code{{font-size:.9em}}.summary{{font-size:1.05rem}}</style>
<h1>Ollama inference broker queue</h1><p>Snapshot timestamp: <code>{cell(data['timestamp'])}</code> · refreshes every 15 seconds.</p>
<p class=summary>Completed: <b>{overall['states']['completed']}</b> total · <b>{overall['completed_last_hour']}</b> last hour · <b>{overall['completed_last_24_hours']}</b> last 24 hours.</p>
<h2>Sources</h2><table><thead><tr><th>Source</th><th>Enabled</th><th>Weight</th><th>Queued</th><th>Running</th><th>Lease</th><th>Retry</th><th>Delayed</th><th>Failed</th><th>Dead</th><th>Cancelled</th><th>Completed total</th><th>Completed 1h</th><th>Completed 24h</th></tr></thead><tbody>{''.join(rows) or '<tr><td colspan=14>None</td></tr>'}</tbody></table>
<p><small>“Dead” is always zero because this broker represents exhausted work as terminal “failed”; delayed queued jobs are blocked by a source min-interval.</small></p>
<h2>Active jobs</h2><table><thead><tr><th>ID</th><th>Source</th><th>State</th><th>Started</th><th>Lease until</th><th>Attempts</th><th>Retries</th></tr></thead><tbody>{active_rows}</tbody></table>
"""
    return document.encode("utf-8")
