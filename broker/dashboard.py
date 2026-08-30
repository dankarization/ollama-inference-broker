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
    schedules = dict(db.execute("SELECT source,next_allowed FROM source_schedules"))
    # Do not discover sources by scanning all historical jobs.  `jobs` holds
    # payloads and can be multiple GiB, so even an index-only global scan can
    # exceed the observer deadline on a cold cache.  Policy/schedule sources
    # are known directly; active unconfigured sources remain visible through
    # a covering state/source index.
    source_names = set(policy_sources)
    source_names.update(schedules)
    source_names.update(
        row["source"]
        for row in db.execute(
            "SELECT DISTINCT source FROM jobs INDEXED BY jobs_state_source "
            "WHERE state IN ('queued','running','cancel_requested')"
        )
    )
    sources: list[dict[str, Any]] = []
    overall = {name: 0 for name in ("queued", "running", "lease", "retry", "delayed", "completed", "failed", "dead", "cancelled")}

    for source in sorted(source_names):
        configured = policy_sources.get(source)
        state_counts = {
            row["state"]: row["count"]
            for row in db.execute(
                "SELECT state,count(*) AS count FROM jobs "
                "INDEXED BY jobs_source_state WHERE source=? GROUP BY state",
                (source,),
            )
        }
        queued = state_counts.get("queued", 0)
        next_allowed = schedules.get(source)
        delayed = queued if next_allowed is not None and next_allowed > now else 0
        # Completion audit records are committed with the terminal job update.
        # Their source/time index avoids reading every completed job payload to
        # derive the two recent windows.
        completed_windows = db.execute(
            "SELECT "
            "coalesce(sum(occurred>=?),0) AS completed_1h,"
            "coalesce(sum(occurred>=?),0) AS completed_24h "
            "FROM audit_events INDEXED BY audit_events_source_time "
            "WHERE source=? AND occurred>=? AND event_type='job.completed'",
            (now - 3_600, now - 86_400, source, now - 86_400),
        ).fetchone()
        retry = db.execute(
            "SELECT count(*) FROM jobs INDEXED BY jobs_source_state_retry "
            "WHERE source=? AND state='queued' AND retry_count>0",
            (source,),
        ).fetchone()[0]
        lease = db.execute(
            "SELECT count(*) FROM jobs INDEXED BY jobs_source_state "
            "WHERE source=? AND state IN ('running','cancel_requested') "
            "AND lease_until IS NOT NULL",
            (source,),
        ).fetchone()[0]
        states = {
            "queued": queued,
            "running": state_counts.get("running", 0) + state_counts.get("cancel_requested", 0),
            "lease": lease,
            "retry": retry,
            "delayed": delayed,
            "completed": state_counts.get("completed", 0),
            "failed": state_counts.get("failed", 0),
            # The broker has no separate `dead` state: terminal jobs are failed.
            "dead": 0,
            "cancelled": state_counts.get("cancelled", 0),
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
            "completed_last_hour": completed_windows["completed_1h"],
            "completed_last_24_hours": completed_windows["completed_24h"],
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
    observation = data.get("observation", {"state": "live"})

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

    if observation["state"] == "unavailable":
        reason = html.escape(str(observation.get("reason", "dashboard data unavailable")))
        observed_at = timestamp_cell(observation.get("observed_at"), tag="code")
        return f"""<!doctype html><html lang=en><meta charset=utf-8>
<meta http-equiv=refresh content=15><title>Ollama broker queue unavailable</title>
<style>body{{font:14px system-ui,sans-serif;margin:2rem;color:#18212b}}.error{{color:#a00}}</style>
<h1>Ollama inference broker queue</h1><p class=error>Dashboard data unavailable: {reason}.</p>
<p>Observed at: {observed_at} · refreshes every 15 seconds.</p>
</html>""".encode("utf-8")

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
            f'{source_attribute}">{options}</select>'
            '<span class="weight-feedback" aria-live="polite"></span></form></td>'
        )

    def enabled_cell(source: str, value: bool | None) -> str:
        if value is None: return "<td>—</td>"
        source_attribute = html.escape(source, quote=True)
        return (f'<td><form class="enabled-form" data-source="{source_attribute}">'
                f'<select name="enabled" aria-label="Enabled for {source_attribute}">'
                f'<option value="true"{" selected" if value else ""}>yes</option>'
                f'<option value="false"{" selected" if not value else ""}>no</option></select>'
                '<span class="enabled-feedback" aria-live="polite"></span></form></td>')

    rows = []
    for item in data["sources"]:
        scheduler, states = item["scheduler"], item["states"]
        rows.append("<tr>" + "".join((
            f"<td>{cell(item['source'])}</td>",
            enabled_cell(item["source"], scheduler['enabled']),
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
    history_rows = "".join(
        f"<tr data-history-id='{cell(row['id'])}'><td>{cell(row['id'])}</td><td>{cell(row['source'])}</td><td>{cell(row['profile'])}</td><td>{cell(row['state'])}</td>{timestamp_cell(row['finished'])}<td>{cell(row['attempt_count'])}</td></tr>"
        for row in data.get("history", [])
    ) or "<tr><td colspan=6>None</td></tr>"
    forecast = data.get("forecast")
    forecast_html = ""
    if forecast:
        if forecast.get("unavailable"):
            forecast_html = (
                "<h2>Forecast</h2>"
                f'<p class=error>Forecast unavailable: {html.escape(str(forecast.get("reason", "observer read failed")))}.</p>'
            )
        else:
            current_model = cell(forecast.get("current_model")) or "none"
            selection_rows = "".join(
                "<tr>" + "".join((
                    f"<td>{cell(item['job_id'])}</td>",
                    f"<td>{cell(item['source'])}</td>",
                    f"<td>{cell(item['model'])}</td>",
                    f"<td>{cell(item['weight'])}</td>",
                    f"<td>{cell(item['mode'])}</td>",
                    f"<td>{cell(item['reason'])}</td>",
                    f"<td>{cell(item['wait_seconds'])}s</td>",
                )) + "</tr>"
                for item in forecast.get("next_selections", [])
            ) or "<tr><td colspan=7>None queued</td></tr>"
            forecast_html = (
                "<h2>Forecast</h2>"
                f"<p>Current model: <b>{current_model}</b> · Weight 1 is most important; "
                "lower Weight receives a larger scheduling share.</p>"
                "<table><thead><tr><th>Job</th><th>Source</th><th>Model</th>"
                "<th>Weight</th><th>Mode</th><th>Reason</th><th>Wait</th></tr></thead>"
                f"<tbody>{selection_rows}</tbody></table>"
                f"<p><small>{html.escape(forecast.get('contingency', ''))}.</small></p>"
            )
    overall = data["overall"]
    document = f"""<!doctype html><html lang=en><meta charset=utf-8>
<meta http-equiv=refresh content=15><title>Ollama broker queue</title>
<style>body{{font:14px system-ui,sans-serif;margin:2rem;color:#18212b}}table{{border-collapse:collapse;width:100%;margin:1rem 0}}th,td{{padding:.45rem;border:1px solid #ccd6df;text-align:right}}th:first-child,td:first-child{{text-align:left}}th{{background:#edf3f7}}code{{font-size:.9em}}.summary{{font-size:1.05rem}}.weight-form{{display:flex;gap:.35rem;align-items:center;justify-content:flex-end}}.weight-feedback{{min-width:4rem;text-align:left}}.weight-feedback.error{{color:#a00}}</style>
<h1>Ollama inference broker queue</h1><p>Snapshot timestamp: {timestamp_cell(data['timestamp'], tag='code')} · refreshes every 15 seconds.</p>
{('<p class=error>Showing stale data: ' + html.escape(str(observation.get('reason', 'database observer read unavailable'))) + '.</p>') if observation['state'] == 'stale' else ''}
<p class=summary>Completed: <b>{overall['states']['completed']}</b> total · <b>{overall['completed_last_hour']}</b> last hour · <b>{overall['completed_last_24_hours']}</b> last 24 hours.</p>
<h2>Sources</h2><table><thead><tr><th>Source</th><th>Enabled</th><th>Weight</th><th>Queued</th><th>Running</th><th>Lease</th><th>Retry</th><th>Delayed</th><th>Failed</th><th>Cancelled</th><th>Completed total</th><th>Completed 1h</th><th>Completed 24h</th></tr></thead><tbody>{''.join(rows) or '<tr><td colspan=13>None</td></tr>'}</tbody></table>
<p><small>Delayed queued jobs are blocked by a source min-interval.</small></p>
<h2>Active jobs</h2><table><thead><tr><th>ID</th><th>Source</th><th>State</th><th>Started</th><th>Lease until</th><th>Attempts</th><th>Retries</th></tr></thead><tbody>{active_rows}</tbody></table>
{forecast_html}
<h2>History</h2><table><thead><tr><th>ID</th><th>Source</th><th>Profile</th><th>State</th><th>Finished</th><th>Attempts</th></tr></thead><tbody id=history-body>{history_rows}</tbody></table><div id=history-sentinel data-cursor="{html.escape(str((data.get('history') or [{}])[-1].get('finished','')) + ':' + str((data.get('history') or [{}])[-1].get('id','')), quote=True)}"></div>
<script>
const bindPolicySelect=(form, field)=>{{ const select=form.elements[field], feedback=form.querySelector('.'+field+'-feedback'); let saved=select.value, desired=saved, saving=false; const flush=async()=>{{ if(saving)return; saving=true; while(desired!==saved){{ const value=desired; feedback.className=field+'-feedback'; feedback.textContent='Saving…'; try {{ const response=await fetch('/v1/sources/'+encodeURIComponent(form.dataset.source)+'/'+field,{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{[field]:field==='weight'?Number(value):value==='true'}})}}); const result=await response.json(); if(!response.ok)throw new Error(result.error||'save failed'); saved=String(result[field]); if(desired===value){{ select.value=saved; feedback.textContent='Saved'; }} }} catch(error) {{ if(desired===value){{ select.value=saved; feedback.className=field+'-feedback error'; feedback.textContent=error.message||'Save failed'; return; }} }} }} saving=false; }}; select.addEventListener('change',()=>{{ desired=select.value; flush(); }}); }};
document.querySelectorAll('.weight-form').forEach(form=>bindPolicySelect(form,'weight'));
document.querySelectorAll('.enabled-form').forEach(form=>bindPolicySelect(form,'enabled'));
const sentinel=document.querySelector('#history-sentinel'); let loading=false; const historyBody=document.querySelector('#history-body'); const displayTime=value=>new Date(Number(value)*1000).toLocaleString('en-GB',{{timeZone:'Asia/Tbilisi'}}); new IntersectionObserver(async entries => {{ if(loading||!entries[0].isIntersecting||!sentinel.dataset.cursor) return; loading=true; const response=await fetch('/v1/history?limit=30&cursor='+encodeURIComponent(sentinel.dataset.cursor)); const page=await response.json(); (page.items||[]).forEach(row=>{{ const tr=document.createElement('tr'); tr.dataset.historyId=row.id; [row.id,row.source,row.profile,row.state,displayTime(row.finished),row.attempt_count].forEach(value=>{{ const td=document.createElement('td'); td.textContent=String(value); tr.append(td); }}); historyBody.append(tr); }}); sentinel.dataset.cursor=page.next_cursor||''; loading=false; }}).observe(sentinel);
</script>
"""
    return document.encode("utf-8")
