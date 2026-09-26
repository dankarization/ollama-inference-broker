"""Deterministic, non-LLM hourly status report for the local-video broker.

The report reads broker SQLite and the live source policy directly.  Its
Telegram transport follows the local Airfare monitor convention: Telegram Bot
API with HTML parse mode and the configured OpenClaw account credential.  It
never imports inference code or requests model generation.
"""
from __future__ import annotations

import argparse
import html
import http.client
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
from urllib.parse import quote, urlsplit
from zoneinfo import ZoneInfo

from .policy import SourcePolicyError, normalize_source_policy
from .local_config import load_local_config, ollama_url

TBILISI = ZoneInfo("Asia/Tbilisi")
REPORT_SOURCES = {
    "Shutterstock": ("shutterstock-video",),
    "Olya Vision": ("olya-vision",),
    "Olya Decision": ("olya-decision",),
}
MAX_IDENTIFIERS = 3


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
    db.execute("""CREATE TABLE IF NOT EXISTS status_report_manual_outbox (
        manual_key TEXT PRIMARY KEY,
        interval_start TEXT NOT NULL,
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
    try:
        return normalize_source_policy(raw)
    except SourcePolicyError:
        # Never report an invalid on-disk policy as effective; the dispatcher
        # keeps its last-known-good snapshot after a bad hot reload.
        return {}


def _source_clause(sources: Iterable[str]) -> tuple[str, tuple[str, ...]]:
    names = tuple(sources)
    return ",".join("?" for _ in names), names


def _identifier(row: sqlite3.Row) -> str:
    return str(row["external_id"] or row["source_item_id"] or row["id"])


def _compact_identifier(value: str) -> str:
    """Use a human-useful, bounded identifier without exposing full UUIDs."""
    marker = value.rfind("asset:")
    if marker >= 0:
        return value[marker:marker + 40]
    if len(value) > 12:
        return "…" + value[-8:]
    return value


def _bounded_identifiers(rows: list[sqlite3.Row]) -> str:
    values = [_compact_identifier(_identifier(row)) for row in rows]
    shown = values[:MAX_IDENTIFIERS]
    text = ", ".join(f"<code>{html.escape(value)}</code>" for value in shown) if shown else ""
    if len(values) > len(shown):
        text += f" …(+{len(values) - len(shown)})"
    return text


def _tg_time(value: datetime, display: str) -> str:
    return f'<tg-time unix="{int(value.timestamp())}" format="">{display}</tg-time>'


def _public_weight(policy: dict[str, dict[str, Any]], source: str) -> str:
    """Return the enabled source's effective scheduler weight."""
    configured = policy.get(source)
    if configured is None or not configured["enabled"]:
        return "н/д"
    return f"{configured['weight']:g}"


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
    executor_url = ollama_url(load_local_config(), "BROKER_OLLAMA_URL")
    return {
        "broker_service": service_probe("ollama-inference-broker.service"),
        "broker_http": http_probe(broker_url),
        "ollama_http": http_probe(executor_url + "/api/ps"),
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
    parts = [
        "📼 <b>Локальная очередь</b>",
        "<b><i>Закрытый час</i></b>: "
        f"{_tg_time(interval.start, interval.start.strftime('%H:%M'))}–"
        f"{_tg_time(interval.end, interval.end.strftime('%H:%M'))}",
        f"Снимок: {_tg_time(generated, generated.strftime('%d.%m.%Y %H:%M:%S'))}",
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
            "AND state='queued' ORDER BY queued_at,id",
            values,
        ))
        terminal = dict(db.execute(
            f"SELECT event_type,count(*) AS n FROM audit_events WHERE source IN ({placeholders}) "
            "AND occurred>=? AND occurred<? AND event_type IN ('job.completed','job.failed','job.cancelled','lease.expired','job.requeued') GROUP BY event_type",
            (*values, start_ts, end_ts),
        ))
        completed = int(terminal.get("job.completed", 0))
        producer_completed[producer] = completed
        weight_line = f"<b>Вес {_public_weight(policy, sources[0])}</b>"
        identifiers = _bounded_identifiers(active_rows)
        identifier_label = "Активно"
        if not identifiers:
            identifiers = _bounded_identifiers(queued_rows)
            identifier_label = "Очередь"
        identifier_line = f"\n{identifier_label}: {identifiers}" if identifiers else ""
        parts.extend((
            "",
            f"<u>{producer}</u> · {weight_line}",
            "Сейчас: "
            f"очередь {current.get('queued', 0)} · в работе {current.get('running', 0) + current.get('cancel_requested', 0)} "
            f"· повтор {retry_queued} · ошибки {current.get('failed', 0)}",
            "<b><u>"
            f"Час: завершено {completed} · ошибок {terminal.get('job.failed', 0)} "
            f"· lease-expired {terminal.get('lease.expired', 0)}"
            "</u></b>" + identifier_line,
        ))

    total_completed = sum(producer_completed.values())
    all_placeholders, all_sources = _source_clause(source for sources in REPORT_SOURCES.values() for source in sources)
    current_queued = db.execute(
        f"SELECT count(*) FROM jobs WHERE source IN ({all_placeholders}) AND state='queued'", all_sources
    ).fetchone()[0]
    parts.extend((
        "",
        "<b><u>Итого: "
        f"{total_completed} completed/ч · очередь {current_queued}</u></b>",
        "Здоровье: "
        f"service {'✓' if health['broker_service'] else '✕'} · "
        f"broker {'✓' if health['broker_http'] else '✕'} · "
        f"Ollama {'✓' if health['ollama_http'] else '✕'}",
    ))
    return "\n".join(parts)


def _telegram_account() -> tuple[str, str]:
    """Read only the selected Telegram account; never log its credential."""
    config_path = Path(os.environ.get("OPENCLAW_CONFIG_PATH", Path.home() / ".openclaw" / "openclaw.json"))
    account_name = os.environ.get("BROKER_REPORT_TELEGRAM_ACCOUNT", "default")
    try:
        config = json.loads(config_path.read_text(encoding="utf-8"))
        telegram = config["channels"]["telegram"]
        account = telegram["accounts"][account_name]
        token = account.get("botToken") or account.get("token")
        api_root = account.get("apiRoot") or telegram.get("apiRoot") or "https://api.telegram.org"
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise DeliveryError("Telegram account configuration is unavailable", retryable=True) from exc
    if not isinstance(token, str) or not token or not isinstance(api_root, str) or not api_root:
        raise DeliveryError("Telegram account configuration is incomplete", retryable=True)
    return token, api_root.rstrip("/")


def _multipart(fields: dict[str, str]) -> tuple[bytes, str]:
    boundary = "----broker-status-report-boundary"
    body = bytearray()
    for name, value in fields.items():
        body.extend(f"--{boundary}\r\n".encode())
        body.extend(f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode())
        body.extend(value.encode("utf-8"))
        body.extend(b"\r\n")
    body.extend(f"--{boundary}--\r\n".encode())
    return bytes(body), f"multipart/form-data; boundary={boundary}"


def deliver_via_telegram_html(text: str) -> str:
    """Send HTML in the same Telegram Bot API shape as the Airfare monitor."""
    target = os.environ["BROKER_REPORT_TELEGRAM_TARGET"]
    thread_id = os.environ["BROKER_REPORT_TELEGRAM_THREAD_ID"]
    token, api_root = _telegram_account()
    parsed = urlsplit(api_root)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise DeliveryError("Telegram API endpoint configuration is invalid", retryable=True)
    body, content_type = _multipart({
        "chat_id": target,
        "message_thread_id": thread_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": "true",
    })
    path = f"{parsed.path.rstrip('/')}/bot{quote(token, safe=':-_')}/sendMessage"
    connection: http.client.HTTPConnection
    connection = (http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection)(
        parsed.hostname, parsed.port, timeout=45,
    )
    try:
        connection.request("POST", path, body=body, headers={"Content-Type": content_type, "Content-Length": str(len(body))})
        response = connection.getresponse()
        payload = response.read()
    except (OSError, http.client.HTTPException) as exc:
        raise DeliveryError("Telegram delivery outcome is unknown", retryable=False) from exc
    finally:
        connection.close()
    try:
        parsed_payload = json.loads(payload)
    except ValueError as exc:
        raise DeliveryError("Telegram delivery outcome is unknown", retryable=False) from exc
    if response.status < 200 or response.status >= 300 or not isinstance(parsed_payload, dict) or not parsed_payload.get("ok"):
        raise DeliveryError("Telegram rejected the report before delivery", retryable=True)
    result = parsed_payload.get("result")
    message_id = result.get("message_id") if isinstance(result, dict) else None
    if message_id in (None, ""):
        raise DeliveryError("Telegram delivery outcome is unknown", retryable=False)
    return str(message_id)


def run_report(
    database: str | Path,
    *,
    interval: ClosedHour | None = None,
    now: datetime | None = None,
    policy_path: str | Path | None = None,
    delivery: Callable[[str], str] = deliver_via_telegram_html,
    health: dict[str, bool] | None = None,
    manual_key: str | None = None,
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
        table = "status_report_manual_outbox" if manual_key else "status_report_outbox"
        key_column = "manual_key" if manual_key else "interval_start"
        key = manual_key or closed.key
        row = db.execute(
            f"SELECT report_text,state,attempts,message_id FROM {table} WHERE {key_column}=?", (key,)
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
            if manual_key:
                db.execute(
                    "INSERT INTO status_report_manual_outbox(manual_key,interval_start,interval_end,report_text,state,attempts,created,updated) "
                    "VALUES(?,?,?,?, 'sending',1,?,?)",
                    (manual_key, closed.key, closed.end.isoformat(), report_text, timestamp, timestamp),
                )
            else:
                db.execute(
                    "INSERT INTO status_report_outbox(interval_start,interval_end,report_text,state,attempts,created,updated) "
                    "VALUES(?,?,?,'sending',1,?,?)",
                    (closed.key, closed.end.isoformat(), report_text, timestamp, timestamp),
                )
        else:
            report_text = row["report_text"]
            db.execute(
                f"UPDATE {table} SET state='sending',attempts=attempts+1,updated=?,error=NULL WHERE {key_column}=?",
                (timestamp, key),
            )
        db.commit()
        try:
            message_id = delivery(report_text)
        except DeliveryError as exc:
            db.execute(
                f"UPDATE {table} SET state=?,updated=?,error=? WHERE {key_column}=?",
                ("failed" if exc.retryable else "uncertain", time.time(), str(exc)[:300], key),
            )
            db.commit()
            return {"state": "failed" if exc.retryable else "uncertain", "message_id": None, "text": report_text}
        db.execute(
            f"UPDATE {table} SET state='sent',message_id=?,updated=?,error=NULL WHERE {key_column}=?",
            (message_id, time.time(), key),
        )
        db.commit()
        return {"state": "sent", "message_id": message_id, "text": report_text}
    finally:
        db.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", default=os.environ.get("BROKER_DB", "broker.sqlite3"))
    parser.add_argument("--policy", default=os.environ.get("BROKER_SOURCES_POLICY"))
    parser.add_argument("--manual-key", help="one authorized manual resend key; does not affect hourly dedupe")
    args = parser.parse_args()
    result = run_report(args.database, policy_path=args.policy, manual_key=args.manual_key)
    # Keep structured service logs payload-free; the durable outbox retains the
    # exact rendered text for an authorized operational inspection.
    print(json.dumps({key: result[key] for key in ("state", "message_id")}, ensure_ascii=False, sort_keys=True))
    if result["state"] in {"failed", "uncertain"}:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
