"""Deterministic, non-LLM hourly status report for the local-video broker.

This module deliberately reads the broker SQLite database and runtime policy
directly.  It never imports the inference adapter and never calls an Ollama
generation endpoint.  Delivery goes through the existing OpenClaw CLI so the
Telegram credential remains in OpenClaw's secret store/configuration.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Iterable
from zoneinfo import ZoneInfo

from .profiles import FIXED_SOURCE_PRIORITIES


TBILISI = ZoneInfo("Asia/Tbilisi")
REPORT_SOURCES = {
    "Shutterstock": ("shutterstock-video",),
    "Olya": ("olya-vision", "olya-decision"),
}
MAX_IDENTIFIERS = 6


@dataclass(frozen=True)
class ClosedHour:
    start: datetime
    end: datetime

    @property
    def key(self) -> str:
        return self.start.strftime("%Y-%m-%dT%H:00:00%z")


class DeliveryError(RuntimeError):
    """A delivery failure with whether another automatic send is safe."""

    def __init__(self, message: str, *, retryable: bool) -> None:
        super().__init__(message)
        self.retryable = retryable


def previous_closed_hour(now: datetime | None = None) -> ClosedHour:
    """Return the exact preceding [HH:00, HH+1:00) Asia/Tbilisi interval."""
    local_now = (now or datetime.now(TBILISI)).astimezone(TBILISI)
    end = local_now.replace(minute=0, second=0, microsecond=0)
    return ClosedHour(start=end - timedelta(hours=1), end=end)


def _open_db(path: str | Path) -> sqlite3.Connection:
    db = sqlite3.connect(str(path), timeout=30)
    db.row_factory = sqlite3.Row
    return db


def _ensure_outbox(db: sqlite3.Connection) -> None:
    db.execute("""CREATE TABLE IF NOT EXISTS status_report_outbox (
        interval_start TEXT PRIMARY KEY,
        interval_end TEXT NOT NULL,
        report_text TEXT NOT NULL,
        state TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        created REAL NOT NULL,
        updated REAL NOT NULL,
        message_id TEXT,
        error TEXT
    )""")


def _policy(path: str | Path | None) -> dict[str, dict[str, Any]]:
    if not path:
        return {}
    try:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    sources = raw.get("sources") if isinstance(raw, dict) else None
    if not isinstance(sources, dict):
        return {}
    normalized: dict[str, dict[str, Any]] = {}
    for source, entry in sources.items():
        if isinstance(source, str) and isinstance(entry, dict):
            enabled = entry.get("enabled")
            weight = entry.get("weight")
            if isinstance(enabled, bool) and isinstance(weight, (int, float)) and not isinstance(weight, bool) and weight > 0:
                normalized[source] = {"enabled": enabled, "weight": float(weight)}
    return normalized


def _source_clause(sources: Iterable[str]) -> tuple[str, tuple[str, ...]]:
    names = tuple(sources)
    return ",".join("?" for _ in names), names


def _identifier(row: sqlite3.Row) -> str:
    return str(row["external_id"] or row["source_item_id"] or row["id"])


def _bounded_identifiers(rows: list[sqlite3.Row]) -> str:
    values = [_identifier(row) for row in rows]
    shown = values[:MAX_IDENTIFIERS]
    text = ", ".join(shown) if shown else "—"
    if len(values) > len(shown):
        text += f" …(+{len(values) - len(shown)})"
    return text


def _age(seconds: float | None) -> str:
    if seconds is None:
        return "—"
    rounded = max(0, int(seconds))
    if rounded < 60:
        return f"{rounded} с"
    if rounded < 3_600:
        return f"{rounded // 60} мин"
    return f"{rounded // 3_600} ч {(rounded % 3_600) // 60} мин"


def _duration(values: list[float]) -> str:
    if not values:
        return "н/д"
    ordered = sorted(values)
    p50 = ordered[(len(ordered) - 1) // 2]
    p95 = ordered[min(len(ordered) - 1, (95 * len(ordered) + 99) // 100 - 1)]
    average = sum(ordered) / len(ordered)
    return f"ср {average:.1f} с / p50 {p50:.1f} с / p95 {p95:.1f} с"


def _probe_url(url: str, timeout: float = 2.0) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return 200 <= response.status < 300
    except (OSError, urllib.error.URLError):
        return False


def _service_active(unit: str) -> bool:
    result = subprocess.run(
        ["systemctl", "--user", "is-active", "--quiet", unit],
        check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    return result.returncode == 0


def health_snapshot(
    *,
    service_probe: Callable[[str], bool] = _service_active,
    http_probe: Callable[[str], bool] = _probe_url,
) -> dict[str, bool]:
    """Only deterministic process/HTTP probes; this never requests inference."""
    broker_url = os.environ.get("BROKER_HEALTH_URL", "http://127.0.0.1:8088/healthz")
    ollama_url = os.environ.get("BROKER_OLLAMA_URL", "http://192.168.2.5:11434").rstrip("/")
    return {
        "broker_service": service_probe("ollama-inference-broker.service"),
        "broker_http": http_probe(broker_url),
        "ollama_http": http_probe(ollama_url + "/api/ps"),
    }


def build_report(
    db: sqlite3.Connection,
    interval: ClosedHour,
    *,
    generated_at: datetime,
    policy: dict[str, dict[str, Any]],
    health: dict[str, bool],
) -> str:
    """Render one fixed Russian report from persisted state only."""
    generated = generated_at.astimezone(TBILISI)
    start_ts, end_ts = interval.start.timestamp(), interval.end.timestamp()
    now_ts = generated.timestamp()
    parts = [
        "Статус локальной video-очереди",
        f"Закрытый час (Asia/Tbilisi): {interval.start:%d.%m.%Y %H:00}–{interval.end:%H:00}",
        f"Снимок сформирован: {generated:%d.%m.%Y %H:%M:%S %Z}",
    ]
    producer_completed: dict[str, int] = {}

    for producer, sources in REPORT_SOURCES.items():
        placeholders, values = _source_clause(sources)
        current = dict(db.execute(
            f"SELECT state, count(*) AS n FROM jobs WHERE source IN ({placeholders}) GROUP BY state",
            values,
        ))
        retry_queued = db.execute(
            f"SELECT count(*) FROM jobs WHERE source IN ({placeholders}) AND state='queued' AND retry_count > 0",
            values,
        ).fetchone()[0]
        active_rows = list(db.execute(
            f"SELECT id,source_item_id,external_id FROM jobs WHERE source IN ({placeholders}) "
            "AND state IN ('running','cancel_requested') ORDER BY started,created,id",
            values,
        ))
        queued_rows = list(db.execute(
            f"SELECT id,source_item_id,external_id FROM jobs WHERE source IN ({placeholders}) "
            "AND state='queued' ORDER BY priority,created,id",
            values,
        ))
        oldest = db.execute(
            f"SELECT min(queued_at) FROM jobs WHERE source IN ({placeholders}) AND state='queued'",
            values,
        ).fetchone()[0]
        terminal = dict(db.execute(
            f"SELECT event_type,count(*) AS n FROM audit_events WHERE source IN ({placeholders}) "
            "AND occurred>=? AND occurred<? AND event_type IN ('job.completed','job.failed','job.cancelled','lease.expired','job.requeued') GROUP BY event_type",
            (*values, start_ts, end_ts),
        ))
        completed_rows = list(db.execute(
            f"SELECT j.id,j.source_item_id,j.external_id FROM audit_events e JOIN jobs j ON j.id=e.job_id "
            f"WHERE e.source IN ({placeholders}) AND e.occurred>=? AND e.occurred<? "
            "AND e.event_type='job.completed' ORDER BY e.occurred,j.id",
            (*values, start_ts, end_ts),
        ))
        failed_rows = list(db.execute(
            f"SELECT j.id,j.source_item_id,j.external_id FROM audit_events e JOIN jobs j ON j.id=e.job_id "
            f"WHERE e.source IN ({placeholders}) AND e.occurred>=? AND e.occurred<? "
            "AND e.event_type='job.failed' ORDER BY e.occurred,j.id",
            (*values, start_ts, end_ts),
        ))
        durations = [row[0] for row in db.execute(
            f"SELECT finished-started FROM job_attempts WHERE source IN ({placeholders}) "
            "AND finished>=? AND finished<? AND outcome='completed' AND started IS NOT NULL",
            (*values, start_ts, end_ts),
        ) if row[0] is not None]
        completed = int(terminal.get("job.completed", 0))
        producer_completed[producer] = completed

        lanes = []
        for source in sources:
            configured = policy.get(source)
            if configured is None:
                lanes.append(f"{source}: выключен/не задан, приоритет {FIXED_SOURCE_PRIORITIES.get(source, 'н/д')}")
            else:
                state = "включен" if configured["enabled"] else "выключен"
                lanes.append(
                    f"{source}: вес {configured['weight']:g}, {state}, приоритет {FIXED_SOURCE_PRIORITIES.get(source, 'н/д')}"
                )
        parts.extend((
            "",
            f"{producer}",
            "Планировщик: " + "; ".join(lanes),
            "Сейчас: "
            f"в очереди {current.get('queued', 0)}; в работе/lease {current.get('running', 0) + current.get('cancel_requested', 0)}; "
            f"повтор/отложено {retry_queued}/0; terminal failed/dead {current.get('failed', 0)}/0; отменено {current.get('cancelled', 0)}.",
            f"Активные: {_bounded_identifiers(active_rows)}.",
            f"Очередь: {_bounded_identifiers(queued_rows)}; самый старый: {_age(now_ts - oldest if oldest is not None else None)}.",
            "За закрытый час: "
            f"завершено {completed}; failed {terminal.get('job.failed', 0)}; lease-expired {terminal.get('lease.expired', 0)}; "
            f"requeue {terminal.get('job.requeued', 0)}; отменено {terminal.get('job.cancelled', 0)}.",
            f"Длительность обработки (completed): {_duration(durations)}.",
            f"Прошли: {_bounded_identifiers(completed_rows)}.",
            f"Ошибки: {_bounded_identifiers(failed_rows)}.",
        ))

    total_completed = sum(producer_completed.values())
    shares = "; ".join(
        f"{name} {producer_completed[name]}/{total_completed} "
        f"({(100 * producer_completed[name] / total_completed) if total_completed else 0:.1f}%)"
        for name in REPORT_SOURCES
    )
    all_placeholders, all_sources = _source_clause(source for sources in REPORT_SOURCES.values() for source in sources)
    current_queued = db.execute(
        f"SELECT count(*) FROM jobs WHERE source IN ({all_placeholders}) AND state='queued'", all_sources
    ).fetchone()[0]
    parts.extend((
        "",
        "Итого",
        f"Пропускная способность: {total_completed} completed/ч. Фактическая доля completed: {shares}.",
        f"Текущая очередь по этим producer: {current_queued}. Δ очереди: не определяется (почасовой snapshot не хранится).",
        "Размеры файлов: не показаны — в broker DB не хранятся надёжно.",
        "Здоровье: "
        f"broker-service {'OK' if health['broker_service'] else 'FAIL'}; "
        f"broker-HTTP {'OK' if health['broker_http'] else 'FAIL'}; "
        f"Ollama /api/ps {'OK' if health['ollama_http'] else 'FAIL'}.",
        "Источник: SQLite jobs/audit_events/job_attempts + sources.json + локальные health probes. LLM/генерация текста не используются.",
    ))
    return "\n".join(parts)


def _extract_message_id(output: str) -> str | None:
    try:
        parsed = json.loads(output)
    except ValueError:
        return None
    if not isinstance(parsed, dict):
        return None
    for key in ("messageId", "message_id"):
        if parsed.get(key) not in (None, ""):
            return str(parsed[key])
    result = parsed.get("result")
    if isinstance(result, dict):
        for key in ("messageId", "message_id"):
            if result.get(key) not in (None, ""):
                return str(result[key])
    return None


def deliver_via_openclaw(text: str) -> str:
    """Deliver through OpenClaw; a started CLI call is ambiguity-safe, not retried."""
    target = os.environ["BROKER_REPORT_TELEGRAM_TARGET"]
    thread_id = os.environ["BROKER_REPORT_TELEGRAM_THREAD_ID"]
    account = os.environ.get("BROKER_REPORT_TELEGRAM_ACCOUNT", "default")
    command = [
        os.environ.get("OPENCLAW_CLI", "openclaw"), "message", "send", "--channel", "telegram", "--account", account,
        "--target", target, "--thread-id", thread_id, "--message", text, "--json",
    ]
    try:
        completed = subprocess.run(command, check=False, text=True, capture_output=True, timeout=45)
    except OSError as exc:
        raise DeliveryError("OpenClaw CLI is unavailable before delivery", retryable=True) from exc
    except subprocess.TimeoutExpired as exc:
        raise DeliveryError("delivery outcome unknown after OpenClaw timeout", retryable=False) from exc
    if completed.returncode != 0:
        raise DeliveryError("delivery outcome unknown after OpenClaw error", retryable=False)
    message_id = _extract_message_id(completed.stdout)
    if message_id is None:
        raise DeliveryError("delivery outcome unknown: OpenClaw returned no message id", retryable=False)
    return message_id


def run_report(
    database: str | Path,
    *,
    interval: ClosedHour | None = None,
    now: datetime | None = None,
    policy_path: str | Path | None = None,
    delivery: Callable[[str], str] = deliver_via_openclaw,
    health: dict[str, bool] | None = None,
) -> dict[str, Any]:
    """Persist one hourly outbox item and deliver it at most once when known."""
    generated = (now or datetime.now(TBILISI)).astimezone(TBILISI)
    closed = interval or previous_closed_hour(generated)
    db = _open_db(database)
    try:
        _ensure_outbox(db)
        db.commit()
        timestamp = time.time()
        db.execute("BEGIN IMMEDIATE")
        row = db.execute(
            "SELECT report_text,state,attempts,message_id FROM status_report_outbox WHERE interval_start=?",
            (closed.key,),
        ).fetchone()
        if row is not None and row["state"] == "sent":
            db.commit()
            return {"state": "already_sent", "message_id": row["message_id"], "text": row["report_text"]}
        if row is not None and row["state"] == "uncertain":
            db.commit()
            return {"state": "uncertain", "message_id": None, "text": row["report_text"]}
        if row is None:
            report_text = build_report(
                db, closed, generated_at=generated, policy=_policy(policy_path),
                health=health if health is not None else health_snapshot(),
            )
            db.execute(
                "INSERT INTO status_report_outbox(interval_start,interval_end,report_text,state,attempts,created,updated) "
                "VALUES(?,?,?,'sending',1,?,?)",
                (closed.key, closed.end.isoformat(), report_text, timestamp, timestamp),
            )
        else:
            report_text = row["report_text"]
            db.execute(
                "UPDATE status_report_outbox SET state='sending',attempts=attempts+1,updated=?,error=NULL WHERE interval_start=?",
                (timestamp, closed.key),
            )
        db.commit()
        try:
            message_id = delivery(report_text)
        except DeliveryError as exc:
            db.execute(
                "UPDATE status_report_outbox SET state=?,updated=?,error=? WHERE interval_start=?",
                ("failed" if exc.retryable else "uncertain", time.time(), str(exc)[:300], closed.key),
            )
            db.commit()
            return {"state": "failed" if exc.retryable else "uncertain", "message_id": None, "text": report_text}
        db.execute(
            "UPDATE status_report_outbox SET state='sent',message_id=?,updated=?,error=NULL WHERE interval_start=?",
            (message_id, time.time(), closed.key),
        )
        db.commit()
        return {"state": "sent", "message_id": message_id, "text": report_text}
    finally:
        db.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", default=os.environ.get("BROKER_DB", "broker.sqlite3"))
    parser.add_argument("--policy", default=os.environ.get("BROKER_SOURCES_POLICY"))
    args = parser.parse_args()
    result = run_report(args.database, policy_path=args.policy)
    # Keep structured service logs payload-free; the durable outbox retains the
    # exact rendered text for an authorized operational inspection.
    print(json.dumps({key: result[key] for key in ("state", "message_id")}, ensure_ascii=False, sort_keys=True))
    if result["state"] in {"failed", "uncertain"}:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
