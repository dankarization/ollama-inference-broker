"""Payload-free operational snapshot and HTML dashboard for the broker."""
from __future__ import annotations

import html
import sqlite3
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo


LOCAL_TIMEZONE = ZoneInfo("Asia/Tbilisi")


def local_timestamp(value: float | None) -> str | None:
    """Format an epoch timestamp for the local-only operator dashboard."""
    if value is None:
        return None
    timestamp = datetime.fromtimestamp(float(value), tz=timezone.utc).astimezone(LOCAL_TIMEZONE)
    offset = timestamp.strftime("%z")
    return (
        f"{timestamp:%Y-%m-%d %H:%M:%S} UTC{offset[:3]}:{offset[3:]} "
        "(Asia/Tbilisi)"
    )


def timestamp_title(value: float | None) -> str | None:
    """Return an operator-facing raw timestamp for a timestamp cell tooltip."""
    if value is None:
        return None
    utc_timestamp = datetime.fromtimestamp(float(value), tz=timezone.utc)
    raw_iso = utc_timestamp.isoformat().replace("+00:00", "Z")
    return f"{raw_iso} (epoch {value})"


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

    def timestamp_cell(value: float | None, *, tag: str = "td") -> str:
        display = local_timestamp(value)
        if display is None:
            return f"<{tag}>—</{tag}>"
        title = html.escape(timestamp_title(value) or "", quote=True)
        return f'<{tag} title="{title}">{html.escape(display)}</{tag}>'

    def weight_cell(source: str, value: float | None) -> str:
        if value is None:
            return "<td>—</td>"
        selected = int(value)
        options = "".join(
            f'<option value="{weight}"{" selected" if weight == selected else ""}>{weight}</option>'
            for weight in range(1, 11)
        )
        source_attribute = html.escape(source, quote=True)
        return (
            '<td><form class="weight-form" data-source="'
            f'{source_attribute}"><select name="weight" aria-label="Weight for '
            f'{source_attribute}">{options}</select><button type="submit">Save</button>'
            '<span class="weight-feedback" aria-live="polite"></span></form></td>'
        )

    rows = []
    for item in data["sources"]:
        scheduler, states = item["scheduler"], item["states"]
        rows.append("<tr>" + "".join((
            f"<td>{cell(item['source'])}</td>",
            f"<td>{cell(scheduler['enabled'])}</td>",
            weight_cell(item["source"], scheduler["weight"]),
            *(f"<td>{cell(value)}</td>" for value in (
            states["queued"], states["running"], states["lease"], states["retry"], states["delayed"],
            states["failed"], states["cancelled"], states["completed"],
            item["completed_last_hour"], item["completed_last_24_hours"],
            )),
        )) + "</tr>")
    active = data["active_jobs"]
    active_rows = "".join(
        "<tr>" + "".join((
            f"<td>{cell(job['id'])}</td>",
            f"<td>{cell(job['source'])}</td>",
            f"<td>{cell(job['state'])}</td>",
            timestamp_cell(job["started"]),
            timestamp_cell(job["lease_until"]),
            f"<td>{cell(job['attempt_count'])}</td>",
            f"<td>{cell(job['retry_count'])}</td>",
        )) + "</tr>"
        for job in active
    ) or "<tr><td colspan=7>None</td></tr>"
    overall = data["overall"]
    document = f"""<!doctype html><html lang=en><meta charset=utf-8>
<meta http-equiv=refresh content=15><title>Ollama broker queue</title>
<style>body{{font:14px system-ui,sans-serif;margin:2rem;color:#18212b}}table{{border-collapse:collapse;width:100%;margin:1rem 0}}th,td{{padding:.45rem;border:1px solid #ccd6df;text-align:right}}th:first-child,td:first-child{{text-align:left}}th{{background:#edf3f7}}code{{font-size:.9em}}.summary{{font-size:1.05rem}}.weight-form{{display:flex;gap:.35rem;align-items:center;justify-content:flex-end}}.weight-feedback{{min-width:4rem;text-align:left}}.weight-feedback.error{{color:#a00}}</style>
<h1>Ollama inference broker queue</h1><p>Snapshot timestamp: {timestamp_cell(data['timestamp'], tag='code')} · refreshes every 15 seconds.</p>
<p class=summary>Completed: <b>{overall['states']['completed']}</b> total · <b>{overall['completed_last_hour']}</b> last hour · <b>{overall['completed_last_24_hours']}</b> last 24 hours.</p>
<h2>Sources</h2><table><thead><tr><th>Source</th><th>Enabled</th><th>Weight</th><th>Queued</th><th>Running</th><th>Lease</th><th>Retry</th><th>Delayed</th><th>Failed</th><th>Cancelled</th><th>Completed total</th><th>Completed 1h</th><th>Completed 24h</th></tr></thead><tbody>{''.join(rows) or '<tr><td colspan=13>None</td></tr>'}</tbody></table>
<p><small>Delayed queued jobs are blocked by a source min-interval.</small></p>
<h2>Active jobs</h2><table><thead><tr><th>ID</th><th>Source</th><th>State</th><th>Started</th><th>Lease until</th><th>Attempts</th><th>Retries</th></tr></thead><tbody>{active_rows}</tbody></table>
<script>
document.querySelectorAll('.weight-form').forEach((form) => {{
  form.addEventListener('submit', async (event) => {{
    event.preventDefault();
    const feedback = form.querySelector('.weight-feedback');
    const weight = Number(form.elements.weight.value);
    feedback.className = 'weight-feedback';
    feedback.textContent = 'Saving…';
    try {{
      const response = await fetch('/v1/sources/' + encodeURIComponent(form.dataset.source) + '/weight', {{
        method: 'POST', headers: {{'Content-Type': 'application/json'}}, body: JSON.stringify({{weight}}),
      }});
      const result = await response.json();
      if (!response.ok) throw new Error(result.error || 'save failed');
      feedback.textContent = 'Saved';
    }} catch (error) {{
      feedback.className = 'weight-feedback error';
      feedback.textContent = error.message || 'Save failed';
    }}
  }});
}});
</script>
"""
    return document.encode("utf-8")
