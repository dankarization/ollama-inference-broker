"""Producer-owned artifact receipts, guarded compaction, and WAL policy."""

from __future__ import annotations

import hashlib
import json
import logging
import re
import sqlite3
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlsplit


LOGGER = logging.getLogger("ollama_inference_broker.storage")
STORAGE_SCHEMA_VERSION = 1
DEFAULT_WAL_AUTOCHECKPOINT_PAGES = 4096
DEFAULT_JOURNAL_SIZE_LIMIT_BYTES = 64 * 1024 * 1024
DEFAULT_WAL_BUDGET_BYTES = 128 * 1024 * 1024
DEFAULT_CHECKPOINT_INTERVAL_SECONDS = 60.0
HASH_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
SCHEMA_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
ATTEMPT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$")
STORAGE_MODES = frozenset({"broker_temporary", "producer_owned", "hybrid"})
PRODUCER_STORAGE_JOB_PREDICATE = (
    "(producer_attempt_id IS NOT NULL "
    "OR input_storage_mode!='broker_temporary' "
    "OR result_storage_mode!='broker_temporary')"
)


class StorageContractError(ValueError):
    """A producer storage request is malformed or not enabled for its source."""


class ReceiptConflict(StorageContractError):
    """An ACK disagrees with durable broker identity or result evidence."""

    code = "ack_conflict"

    def __init__(self, message: str, *, job_id: str | None = None) -> None:
        self.job_id = job_id
        super().__init__(message)


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
    ).encode("utf-8")


def content_evidence(value: Any) -> tuple[str, int]:
    encoded = canonical_json_bytes(value)
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}", len(encoded)


def _column_names(db: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in db.execute(f"PRAGMA table_info({table})")}


def migrate_storage_schema(
    db: sqlite3.Connection,
    *,
    wal_autocheckpoint_pages: int = DEFAULT_WAL_AUTOCHECKPOINT_PAGES,
    journal_size_limit_bytes: int = DEFAULT_JOURNAL_SIZE_LIMIT_BYTES,
) -> dict[str, Any]:
    """Apply the metadata-only, additive storage migration idempotently."""
    if wal_autocheckpoint_pages <= 0:
        raise ValueError("wal_autocheckpoint_pages must be positive")
    if journal_size_limit_bytes <= 0:
        raise ValueError("journal_size_limit_bytes must be positive")

    db.execute(f"PRAGMA wal_autocheckpoint={int(wal_autocheckpoint_pages)}")
    db.execute(f"PRAGMA journal_size_limit={int(journal_size_limit_bytes)}")
    db.execute("""CREATE TABLE IF NOT EXISTS broker_schema_migrations (
        version INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)""")

    additions = {
        "input_storage_mode": "TEXT NOT NULL DEFAULT 'broker_temporary'",
        "input_ref": "TEXT",
        "input_hash": "TEXT",
        "input_bytes": "INTEGER",
        "input_received_at": "REAL",
        "result_storage_mode": "TEXT NOT NULL DEFAULT 'broker_temporary'",
        "result_ref": "TEXT",
        "result_hash": "TEXT",
        "result_bytes": "INTEGER",
        "delivery_state": "TEXT NOT NULL DEFAULT 'pending'",
        "delivery_attempt_count": "INTEGER NOT NULL DEFAULT 0",
        "last_delivery_at": "REAL",
        "acked_at": "REAL",
        "retention_until": "REAL",
        "compaction_after": "REAL",
        "artifact_schema_version": "TEXT",
        "compaction_state": "TEXT NOT NULL DEFAULT 'full'",
        "producer_attempt_id": "TEXT",
        "ack_required": "INTEGER NOT NULL DEFAULT 0",
        "legacy_result_fallback": "INTEGER NOT NULL DEFAULT 1",
        "quarantined_at": "REAL",
        "compacted_at": "REAL",
    }
    columns = _column_names(db, "jobs")
    added: list[str] = []
    for name, declaration in additions.items():
        if name not in columns:
            db.execute(f"ALTER TABLE jobs ADD COLUMN {name} {declaration}")
            added.append(name)

    db.execute("""CREATE TABLE IF NOT EXISTS job_artifacts (
        id TEXT PRIMARY KEY,
        job_id TEXT NOT NULL,
        role TEXT NOT NULL,
        storage_backend TEXT NOT NULL,
        storage_ref TEXT,
        content_hash TEXT NOT NULL,
        byte_size INTEGER NOT NULL,
        schema_version TEXT,
        state TEXT NOT NULL,
        created_at REAL NOT NULL,
        persisted_at TEXT,
        acked_at REAL,
        deleted_at REAL,
        UNIQUE(job_id, role))""")
    if "persisted_at" not in _column_names(db, "job_artifacts"):
        db.execute("ALTER TABLE job_artifacts ADD COLUMN persisted_at TEXT")
    db.execute("""CREATE TABLE IF NOT EXISTS job_delivery_acks (
        receipt_id TEXT PRIMARY KEY,
        job_id TEXT NOT NULL,
        producer TEXT NOT NULL,
        producer_attempt_id TEXT NOT NULL,
        storage_ref TEXT NOT NULL,
        result_hash TEXT NOT NULL,
        result_bytes INTEGER NOT NULL,
        schema_version TEXT NOT NULL,
        persisted_at TEXT NOT NULL,
        received_at REAL NOT NULL,
        UNIQUE(job_id, producer, producer_attempt_id))""")
    db.execute("""CREATE TABLE IF NOT EXISTS job_delivery_ack_conflicts (
        sequence INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id TEXT NOT NULL,
        producer TEXT,
        producer_attempt_id TEXT,
        candidate_storage_ref TEXT,
        candidate_result_hash TEXT,
        reason TEXT NOT NULL,
        occurred REAL NOT NULL)""")
    db.execute(
        "CREATE INDEX IF NOT EXISTS job_artifacts_job_state "
        "ON job_artifacts(job_id,role,state)"
    )
    db.execute(
        "CREATE INDEX IF NOT EXISTS delivery_acks_job "
        "ON job_delivery_acks(job_id,received_at)"
    )
    db.execute(
        "CREATE INDEX IF NOT EXISTS ack_conflicts_job "
        "ON job_delivery_ack_conflicts(job_id,occurred)"
    )
    db.execute(
        "CREATE INDEX IF NOT EXISTS jobs_storage_compaction "
        "ON jobs(source,compaction_state,compaction_after,id) "
        "WHERE delivery_state='acked' AND ack_required=1 "
        "AND legacy_result_fallback=0"
    )
    db.execute(
        "CREATE INDEX IF NOT EXISTS jobs_storage_unacked_terminal "
        "ON jobs(state,result_storage_mode,delivery_state,id) WHERE state='completed' "
        "AND result_storage_mode!='broker_temporary' AND delivery_state!='acked'"
    )
    db.execute(
        "CREATE INDEX IF NOT EXISTS jobs_storage_quarantined "
        "ON jobs(id) WHERE compaction_state='quarantined'"
    )
    db.execute(
        "INSERT OR IGNORE INTO broker_schema_migrations(version,name) VALUES(?,?)",
        (STORAGE_SCHEMA_VERSION, "producer_owned_storage_receipts"),
    )
    return {
        "schema_version": STORAGE_SCHEMA_VERSION,
        "added_columns": added,
        "wal_autocheckpoint_pages": int(db.execute(
            "PRAGMA wal_autocheckpoint"
        ).fetchone()[0]),
        "journal_size_limit_bytes": int(db.execute(
            "PRAGMA journal_size_limit"
        ).fetchone()[0]),
    }


def storage_schema_report(db: sqlite3.Connection, database: str) -> dict[str, Any]:
    """Return migration evidence without selecting payload or result bodies."""
    db.row_factory = sqlite3.Row
    states = {
        row["state"]: row["count"]
        for row in db.execute(
            "SELECT state,count(*) AS count FROM jobs GROUP BY state ORDER BY state"
        )
    }
    sources = {
        row["source"]: row["count"]
        for row in db.execute(
            "SELECT source,count(*) AS count FROM jobs GROUP BY source ORDER BY source"
        )
    }
    tables = sorted(
        row[0] for row in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
    )
    database_path = Path(database)
    return {
        "schema_version": STORAGE_SCHEMA_VERSION,
        "jobs_columns": sorted(_column_names(db, "jobs")),
        "tables": tables,
        "job_counts_by_state": states,
        "job_counts_by_source": sources,
        "receipt_count": db.execute("SELECT count(*) FROM job_delivery_acks").fetchone()[0],
        "artifact_count": db.execute("SELECT count(*) FROM job_artifacts").fetchone()[0],
        "quick_check": db.execute("PRAGMA quick_check").fetchone()[0],
        "foreign_key_violation_count": len(list(db.execute("PRAGMA foreign_key_check"))),
        "db_bytes": database_path.stat().st_size if database_path.exists() else 0,
        "wal_bytes": _file_size(f"{database}-wal"),
        "wal_autocheckpoint_pages": db.execute("PRAGMA wal_autocheckpoint").fetchone()[0],
        "journal_size_limit_bytes": db.execute("PRAGMA journal_size_limit").fetchone()[0],
        "report_contains_payloads": False,
    }


def _file_size(path: str) -> int:
    try:
        return Path(path).stat().st_size
    except OSError:
        return 0


def _nonempty(value: Any, name: str, maximum: int = 2048) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise StorageContractError(f"{name} must be a non-empty string up to {maximum} characters")
    return value.strip()


def _hash(value: Any, name: str) -> str:
    value = _nonempty(value, name, 71)
    if not HASH_RE.fullmatch(value):
        raise StorageContractError(f"{name} must be a lowercase sha256 digest")
    return value


def _byte_size(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise StorageContractError(f"{name} must be a non-negative integer")
    return value


def _schema(value: Any, name: str = "schema_version") -> str:
    value = _nonempty(value, name, 128)
    if not SCHEMA_RE.fullmatch(value):
        raise StorageContractError(f"{name} contains unsupported characters")
    return value


def _attempt(value: Any) -> str:
    value = _nonempty(value, "producer_attempt_id", 256)
    if not ATTEMPT_RE.fullmatch(value):
        raise StorageContractError("producer_attempt_id contains unsupported characters")
    return value


def _storage_ref(value: Any) -> str:
    value = _nonempty(value, "storage_ref", 2048)
    try:
        parsed = urlsplit(value)
    except ValueError as exc:
        raise StorageContractError("storage_ref is not a valid URI") from exc
    if not parsed.scheme or not re.fullmatch(r"[A-Za-z][A-Za-z0-9+.-]{1,31}", parsed.scheme):
        raise StorageContractError("storage_ref must use an explicit URI scheme")
    if parsed.query or parsed.fragment or parsed.username or parsed.password:
        raise StorageContractError("storage_ref must not contain query, fragment, or credentials")
    return value


def _persisted_at(value: Any) -> str:
    value = _nonempty(value, "persisted_at", 64)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise StorageContractError("persisted_at must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise StorageContractError("persisted_at must include a timezone")
    return value


class StorageManager:
    """Own producer capability metadata and transactional delivery receipts."""

    def __init__(
        self,
        db: sqlite3.Connection,
        database: str,
        lock: Any,
        clock: Callable[[], float],
        audit: Callable[..., None],
        policy_getter: Callable[[], Any],
        *,
        wal_autocheckpoint_pages: int = DEFAULT_WAL_AUTOCHECKPOINT_PAGES,
        journal_size_limit_bytes: int = DEFAULT_JOURNAL_SIZE_LIMIT_BYTES,
        wal_budget_bytes: int = DEFAULT_WAL_BUDGET_BYTES,
        checkpoint_interval_seconds: float = DEFAULT_CHECKPOINT_INTERVAL_SECONDS,
    ) -> None:
        if wal_budget_bytes <= 0 or checkpoint_interval_seconds <= 0:
            raise ValueError("WAL budget and checkpoint interval must be positive")
        self.db = db
        self.database = database
        self.lock = lock
        self.clock = clock
        self.audit = audit
        self.policy_getter = policy_getter
        self.wal_autocheckpoint_pages = wal_autocheckpoint_pages
        self.journal_size_limit_bytes = journal_size_limit_bytes
        self.wal_budget_bytes = wal_budget_bytes
        self.checkpoint_interval_seconds = float(checkpoint_interval_seconds)
        self.last_checkpoint: dict[str, Any] | None = None
        self._last_checkpoint_at = 0.0

    def _source_config(self, source: str) -> dict[str, Any]:
        policy = self.policy_getter()
        if policy is None:
            return {
                "producer_storage_enabled": False,
                "producer_storage_mode": "broker_temporary",
                "ack_required": False,
                "compaction_enabled": False,
                "legacy_result_fallback": True,
                "compaction_grace_seconds": 86400,
                "quarantine_grace_seconds": 86400,
            }
        try:
            return policy.storage_config(source)
        except (OSError, ValueError) as exc:
            raise StorageContractError("source policy is unavailable or invalid") from exc

    def prepare_admission(
        self, source: str, payload: dict[str, Any], capability: Any,
        *, persisted: sqlite3.Row | None = None,
    ) -> dict[str, Any]:
        payload_hash, payload_bytes = content_evidence(payload)
        defaults = {
            "input_storage_mode": "broker_temporary",
            "input_ref": None,
            "input_hash": payload_hash,
            "input_bytes": payload_bytes,
            "result_storage_mode": "broker_temporary",
            "artifact_schema_version": None,
            "producer_attempt_id": None,
            "ack_required": 0,
            "legacy_result_fallback": 1,
        }
        if capability is None:
            return defaults
        if not isinstance(capability, dict) or set(capability) - {
            "producer_attempt_id", "input", "result",
        }:
            raise StorageContractError(
                "producer_storage must contain only producer_attempt_id, input, and result"
            )
        config = self._source_config(source) if persisted is None else None
        if config is not None and not config["producer_storage_enabled"]:
            raise StorageContractError(f"producer storage is not enabled for source {source!r}")
        attempt = _attempt(capability.get("producer_attempt_id"))
        input_spec = capability.get("input", {})
        result_spec = capability.get("result", {})
        if not isinstance(input_spec, dict) or set(input_spec) - {
            "mode", "storage_ref", "content_hash", "byte_size",
        }:
            raise StorageContractError("producer_storage.input has unsupported fields")
        if not isinstance(result_spec, dict) or set(result_spec) - {"mode", "schema_version"}:
            raise StorageContractError("producer_storage.result has unsupported fields")
        input_mode = input_spec.get("mode", "broker_temporary")
        default_result_mode = (
            config["producer_storage_mode"]
            if config is not None else persisted["result_storage_mode"]
        )
        result_mode = result_spec.get("mode", default_result_mode)
        for field, mode in (("input.mode", input_mode), ("result.mode", result_mode)):
            if mode not in STORAGE_MODES:
                raise StorageContractError(f"producer_storage.{field} is invalid")
        if config is not None:
            allowed_mode = config["producer_storage_mode"]
            if result_mode not in {"broker_temporary", allowed_mode}:
                raise StorageContractError(
                    f"result storage mode {result_mode!r} is not allowed for source {source!r}"
                )
            if input_mode not in {"broker_temporary", allowed_mode}:
                raise StorageContractError(
                    f"input storage mode {input_mode!r} is not allowed for source {source!r}"
                )
        input_ref = input_spec.get("storage_ref")
        if input_mode != "broker_temporary":
            input_ref = _storage_ref(input_ref)
            if _hash(input_spec.get("content_hash"), "input.content_hash") != payload_hash:
                raise StorageContractError("input.content_hash does not match canonical payload")
            if _byte_size(input_spec.get("byte_size"), "input.byte_size") != payload_bytes:
                raise StorageContractError("input.byte_size does not match canonical payload")
        elif input_ref is not None:
            raise StorageContractError("broker_temporary input must not declare storage_ref")
        schema_version = result_spec.get("schema_version")
        if result_mode != "broker_temporary":
            schema_version = _schema(schema_version)
        elif schema_version is not None:
            schema_version = _schema(schema_version)
        return {
            **defaults,
            "input_storage_mode": input_mode,
            "input_ref": input_ref,
            "result_storage_mode": result_mode,
            "artifact_schema_version": schema_version,
            "producer_attempt_id": attempt,
            "ack_required": int(bool(
                (config["ack_required"] if config is not None else persisted["ack_required"])
                and result_mode != "broker_temporary"
            )),
            "legacy_result_fallback": int(bool(
                config["legacy_result_fallback"]
                if config is not None else persisted["legacy_result_fallback"]
            )),
        }

    def record_admission(
        self, job_id: str, source: str, fields: dict[str, Any], created: float,
    ) -> None:
        backend = (
            "broker-temp" if fields["input_storage_mode"] == "broker_temporary" else "producer"
        )
        self.db.execute(
            "INSERT INTO job_artifacts(id,job_id,role,storage_backend,storage_ref,"
            "content_hash,byte_size,schema_version,state,created_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?)",
            (f"{job_id}:input", job_id, "input", backend, fields["input_ref"],
             fields["input_hash"], fields["input_bytes"], None, "temporary", created),
        )

    def record_result(self, job_id: str, source: str, result: dict[str, Any], finished: float) -> tuple[str, int]:
        result_hash, result_bytes = content_evidence(result)
        schema_version = self.db.execute(
            "SELECT artifact_schema_version FROM jobs WHERE id=?", (job_id,)
        ).fetchone()[0]
        self.db.execute(
            "UPDATE jobs SET result_hash=?,result_bytes=?,delivery_state='pending' WHERE id=?",
            (result_hash, result_bytes, job_id),
        )
        self.db.execute(
            "INSERT INTO job_artifacts(id,job_id,role,storage_backend,storage_ref,"
            "content_hash,byte_size,schema_version,state,created_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(job_id,role) DO UPDATE SET storage_backend=excluded.storage_backend,"
            "storage_ref=NULL,content_hash=excluded.content_hash,byte_size=excluded.byte_size,"
            "schema_version=excluded.schema_version,state='temporary',created_at=excluded.created_at,"
            "persisted_at=NULL,acked_at=NULL,deleted_at=NULL",
            (f"{job_id}:result", job_id, "result", "broker-temp", None, result_hash,
             result_bytes, schema_version, "temporary", finished),
        )
        return result_hash, result_bytes

    def _conflict(
        self, row: sqlite3.Row, body: dict[str, Any], reason: str, now: float,
    ) -> None:
        identity = (
            row["id"], body.get("producer"), body.get("producer_attempt_id"),
            body.get("storage_ref"), body.get("result_hash"), reason,
        )
        duplicate = self.db.execute(
            "SELECT 1 FROM job_delivery_ack_conflicts WHERE job_id=? "
            "AND producer IS ? AND producer_attempt_id IS ? "
            "AND candidate_storage_ref IS ? AND candidate_result_hash IS ? "
            "AND reason=? LIMIT 1",
            identity,
        ).fetchone()
        if duplicate is not None:
            return
        self.db.execute(
            "INSERT INTO job_delivery_ack_conflicts(job_id,producer,producer_attempt_id,"
            "candidate_storage_ref,candidate_result_hash,reason,occurred) VALUES(?,?,?,?,?,?,?)",
            (*identity, now),
        )
        self.db.execute(
            "UPDATE jobs SET delivery_state='conflict',"
            "delivery_attempt_count=delivery_attempt_count+1,last_delivery_at=? WHERE id=?",
            (now, row["id"]),
        )
        self.audit(
            "delivery.ack_conflict", job_id=row["id"], source=row["source"],
            attempt_no=row["attempt_count"] or None, from_state=row["state"],
            to_state=row["state"], reason=reason, occurred=now,
            metadata={"code": ReceiptConflict.code},
        )

    @staticmethod
    def _receipt_dict(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "receipt_id": row["receipt_id"],
            "job_id": row["job_id"],
            "producer": row["producer"],
            "producer_attempt_id": row["producer_attempt_id"],
            "storage_ref": row["storage_ref"],
            "result_hash": row["result_hash"],
            "result_bytes": row["result_bytes"],
            "schema_version": row["schema_version"],
            "persisted_at": row["persisted_at"],
            "received_at": row["received_at"],
        }

    def acknowledge_result(self, job_id: str, body: Any) -> dict[str, Any] | None:
        if not isinstance(body, dict):
            raise StorageContractError("ACK body must be an object")
        required = {
            "job_id", "producer", "producer_attempt_id", "storage_ref", "result_hash",
            "result_bytes", "schema_version", "persisted_at",
        }
        if set(body) != required:
            raise StorageContractError("ACK body fields do not match the receipt contract")
        normalized = {
            "job_id": _nonempty(body["job_id"], "job_id", 256),
            "producer": _nonempty(body["producer"], "producer", 256),
            "producer_attempt_id": _attempt(body["producer_attempt_id"]),
            "storage_ref": _storage_ref(body["storage_ref"]),
            "result_hash": _hash(body["result_hash"], "result_hash"),
            "result_bytes": _byte_size(body["result_bytes"], "result_bytes"),
            "schema_version": _schema(body["schema_version"]),
            "persisted_at": _persisted_at(body["persisted_at"]),
        }
        now = self.clock()
        conflict_reason: str | None = None
        receipt: dict[str, Any] | None = None
        with self.lock, self.db:
            row = self.db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                return None
            existing = self.db.execute(
                "SELECT * FROM job_delivery_acks WHERE job_id=? AND producer=? "
                "AND producer_attempt_id=?",
                (job_id, normalized["producer"], normalized["producer_attempt_id"]),
            ).fetchone()
            if existing is not None:
                comparable = {
                    key: existing[key]
                    for key in (
                        "job_id", "producer", "producer_attempt_id", "storage_ref",
                        "result_hash", "result_bytes", "schema_version", "persisted_at",
                    )
                }
                if (
                    comparable == normalized
                    and normalized["job_id"] == job_id
                    and normalized["producer"] == row["source"]
                    and normalized["producer_attempt_id"] == row["producer_attempt_id"]
                ):
                    has_conflict = self.db.execute(
                        "SELECT 1 FROM job_delivery_ack_conflicts WHERE job_id=? LIMIT 1",
                        (job_id,),
                    ).fetchone()
                    if has_conflict is None:
                        self.db.execute(
                            "UPDATE jobs SET delivery_state='acked',result_ref=?,acked_at=? "
                            "WHERE id=?",
                            (existing["storage_ref"], existing["received_at"], job_id),
                        )
                        self.db.execute(
                            "UPDATE job_artifacts SET storage_backend='producer',storage_ref=?,"
                            "schema_version=?,state='acked',persisted_at=?,acked_at=? "
                            "WHERE job_id=? AND role='result'",
                            (existing["storage_ref"], existing["schema_version"],
                             existing["persisted_at"], existing["received_at"], job_id),
                        )
                    return self._receipt_dict(existing)
            config = self._source_config(row["source"])
            checks = [
                (normalized["job_id"] == job_id, "ACK job_id does not match request path"),
                (normalized["producer"] == row["source"], "ACK producer does not match job source"),
                (normalized["producer_attempt_id"] == row["producer_attempt_id"],
                 "ACK producer_attempt_id does not match admission"),
                (row["state"] == "completed", "only completed jobs may be acknowledged"),
                (row["result_storage_mode"] != "broker_temporary",
                 "job did not declare producer-owned result storage"),
                (bool(config["producer_storage_enabled"]),
                 "producer storage is disabled for this source"),
                (normalized["result_hash"] == row["result_hash"],
                 "ACK result_hash does not match broker result"),
                (normalized["result_bytes"] == row["result_bytes"],
                 "ACK result_bytes does not match broker result"),
                (normalized["schema_version"] == row["artifact_schema_version"],
                 "ACK schema_version does not match admission"),
            ]
            conflict_reason = next((reason for valid, reason in checks if not valid), None)
            if conflict_reason is None and existing is not None:
                conflict_reason = "ACK conflicts with the durable receipt"
            if conflict_reason is not None:
                self._conflict(row, normalized, conflict_reason, now)
            elif receipt is None:
                receipt_id = str(uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"ollama-inference-broker:{job_id}:{row['source']}:{row['producer_attempt_id']}",
                ))
                self.db.execute(
                    "INSERT INTO job_delivery_acks(receipt_id,job_id,producer,producer_attempt_id,"
                    "storage_ref,result_hash,result_bytes,schema_version,persisted_at,received_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (receipt_id, job_id, normalized["producer"], normalized["producer_attempt_id"],
                     normalized["storage_ref"], normalized["result_hash"],
                     normalized["result_bytes"], normalized["schema_version"],
                     normalized["persisted_at"], now),
                )
                grace = float(config["compaction_grace_seconds"])
                self.db.execute(
                    "UPDATE jobs SET result_ref=?,delivery_state='acked',"
                    "delivery_attempt_count=delivery_attempt_count+1,last_delivery_at=?,"
                    "acked_at=?,compaction_after=? WHERE id=?",
                    (normalized["storage_ref"], now, now, now + grace, job_id),
                )
                self.db.execute(
                    "UPDATE job_artifacts SET storage_backend='producer',storage_ref=?,"
                    "schema_version=?,state='acked',persisted_at=?,acked_at=? "
                    "WHERE job_id=? AND role='result'",
                    (normalized["storage_ref"], normalized["schema_version"],
                     normalized["persisted_at"], now, job_id),
                )
                self.audit(
                    "delivery.acked", job_id=job_id, source=row["source"],
                    attempt_no=row["attempt_count"] or None, from_state=row["state"],
                    to_state=row["state"], reason="producer persisted canonical result",
                    occurred=now,
                    metadata={
                        "receipt_id": receipt_id,
                        "result_hash": normalized["result_hash"],
                        "result_bytes": normalized["result_bytes"],
                        "schema_version": normalized["schema_version"],
                    },
                )
                persisted = self.db.execute(
                    "SELECT * FROM job_delivery_acks WHERE receipt_id=?", (receipt_id,)
                ).fetchone()
                receipt = self._receipt_dict(persisted)
        if conflict_reason is not None:
            raise ReceiptConflict(conflict_reason, job_id=job_id)
        return receipt

    def acknowledge_input(self, job_id: str, body: Any) -> dict[str, Any] | None:
        if not isinstance(body, dict):
            raise StorageContractError("input receipt body must be an object")
        required = {
            "job_id", "producer", "producer_attempt_id", "storage_ref",
            "input_hash", "input_bytes", "persisted_at",
        }
        if set(body) != required:
            raise StorageContractError("input receipt fields do not match the contract")
        normalized = {
            "job_id": _nonempty(body["job_id"], "job_id", 256),
            "producer": _nonempty(body["producer"], "producer", 256),
            "producer_attempt_id": _attempt(body["producer_attempt_id"]),
            "storage_ref": _storage_ref(body["storage_ref"]),
            "input_hash": _hash(body["input_hash"], "input_hash"),
            "input_bytes": _byte_size(body["input_bytes"], "input_bytes"),
            "persisted_at": _persisted_at(body["persisted_at"]),
        }
        now = self.clock()
        received_at = now
        reason: str | None = None
        with self.lock, self.db:
            row = self.db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                return None
            checks = [
                (normalized["job_id"] == job_id, "input receipt job_id mismatch"),
                (normalized["producer"] == row["source"], "input receipt producer mismatch"),
                (normalized["producer_attempt_id"] == row["producer_attempt_id"],
                 "input receipt attempt mismatch"),
                (row["input_storage_mode"] != "broker_temporary",
                 "job did not declare producer-owned input storage"),
                (normalized["storage_ref"] == row["input_ref"], "input storage_ref mismatch"),
                (normalized["input_hash"] == row["input_hash"], "input_hash mismatch"),
                (normalized["input_bytes"] == row["input_bytes"], "input_bytes mismatch"),
            ]
            reason = next((reason for valid, reason in checks if not valid), None)
            if reason is None and row["input_received_at"] is not None:
                artifact = self.db.execute(
                    "SELECT persisted_at FROM job_artifacts WHERE job_id=? AND role='input'",
                    (job_id,),
                ).fetchone()
                if artifact is not None and artifact["persisted_at"] == normalized["persisted_at"]:
                    return {
                        "job_id": job_id,
                        "producer": normalized["producer"],
                        "producer_attempt_id": normalized["producer_attempt_id"],
                        "storage_ref": normalized["storage_ref"],
                        "input_hash": normalized["input_hash"],
                        "input_bytes": normalized["input_bytes"],
                        "persisted_at": normalized["persisted_at"],
                        "received_at": row["input_received_at"],
                    }
                reason = "input receipt conflicts with the durable receipt"
            if reason is None:
                config = self._source_config(row["source"])
                if not config["producer_storage_enabled"]:
                    reason = "producer storage is disabled for this source"
            if reason is not None:
                self._conflict(row, {
                    "producer": normalized["producer"],
                    "producer_attempt_id": normalized["producer_attempt_id"],
                    "storage_ref": normalized["storage_ref"],
                    "result_hash": normalized["input_hash"],
                }, reason, now)
            else:
                self.db.execute(
                    "UPDATE jobs SET input_received_at=? WHERE id=?", (now, job_id)
                )
                self.db.execute(
                    "UPDATE job_artifacts SET state='acked',persisted_at=?,acked_at=? "
                    "WHERE job_id=? AND role='input'",
                    (normalized["persisted_at"], now, job_id),
                )
                self.audit(
                    "input.received", job_id=job_id, source=row["source"],
                    attempt_no=row["attempt_count"] or None,
                    from_state=row["state"], to_state=row["state"], occurred=now,
                    reason="producer confirmed durable input",
                    metadata={"input_hash": normalized["input_hash"],
                              "input_bytes": normalized["input_bytes"]},
                )
        if reason is not None:
            raise ReceiptConflict(reason, job_id=job_id)
        return {
            "job_id": job_id,
            "producer": normalized["producer"],
            "producer_attempt_id": normalized["producer_attempt_id"],
            "storage_ref": normalized["storage_ref"],
            "input_hash": normalized["input_hash"],
            "input_bytes": normalized["input_bytes"],
            "persisted_at": normalized["persisted_at"],
            "received_at": received_at,
        }

    def compact_status(self, job_id: str) -> dict[str, Any] | None:
        with self.lock:
            row = self.db.execute(
                "SELECT id,profile,kind,source,state,created,started,finished,lease_until,"
                "source_item_id,external_id,queued_at,attempt_count,retry_count,requeue_count,"
                "input_storage_mode,input_ref,input_hash,input_bytes,input_received_at,"
                "result_storage_mode,result_ref,result_hash,result_bytes,delivery_state,"
                "delivery_attempt_count,last_delivery_at,acked_at,retention_until,"
                "compaction_after,artifact_schema_version,compaction_state,producer_attempt_id,"
                "ack_required,legacy_result_fallback,quarantined_at,compacted_at "
                "FROM jobs WHERE id=?",
                (job_id,),
            ).fetchone()
            if row is None:
                return None
            result = dict(row)
            result["ack_required"] = bool(result["ack_required"])
            result["legacy_result_fallback"] = bool(result["legacy_result_fallback"])
            result["artifacts"] = [dict(artifact) for artifact in self.db.execute(
                "SELECT role,storage_backend,storage_ref,content_hash,byte_size,"
                "schema_version,state,created_at,persisted_at,acked_at,deleted_at "
                "FROM job_artifacts WHERE job_id=? ORDER BY role,id",
                (job_id,),
            )]
            return result

    def receipt(self, job_id: str) -> dict[str, Any] | None:
        with self.lock:
            exists = self.db.execute("SELECT 1 FROM jobs WHERE id=?", (job_id,)).fetchone()
            if exists is None:
                return None
            receipt = self.db.execute(
                "SELECT * FROM job_delivery_acks WHERE job_id=? ORDER BY received_at LIMIT 1",
                (job_id,),
            ).fetchone()
            conflicts = [dict(row) for row in self.db.execute(
                "SELECT sequence,producer,producer_attempt_id,candidate_storage_ref,"
                "candidate_result_hash,reason,occurred "
                "FROM job_delivery_ack_conflicts WHERE job_id=? "
                "ORDER BY sequence DESC LIMIT 20",
                (job_id,),
            )]
            return {
                "receipt": self._receipt_dict(receipt) if receipt else None,
                "conflicts": conflicts,
            }

    def reconcile(self) -> int:
        """Repair derived ACK state from the durable receipt log after restart."""
        repaired = 0
        with self.lock, self.db:
            rows = list(self.db.execute(
                "SELECT a.*,j.delivery_state,j.result_ref,j.acked_at,j.source,"
                "j.producer_attempt_id,j.result_hash,j.attempt_count,j.state "
                "FROM job_delivery_acks a JOIN jobs j ON j.id=a.job_id "
                "WHERE a.producer=j.source AND a.producer_attempt_id=j.producer_attempt_id "
                "AND a.result_hash=j.result_hash "
                "AND NOT EXISTS(SELECT 1 FROM job_delivery_ack_conflicts c "
                "WHERE c.job_id=j.id) "
                "AND (j.delivery_state!='acked' OR j.result_ref!=a.storage_ref OR j.acked_at IS NULL)"
            ))
            for row in rows:
                self.db.execute(
                    "UPDATE jobs SET delivery_state='acked',result_ref=?,acked_at=? WHERE id=?",
                    (row["storage_ref"], row["received_at"], row["job_id"]),
                )
                self.db.execute(
                    "UPDATE job_artifacts SET storage_backend='producer',storage_ref=?,"
                    "schema_version=?,state='acked',persisted_at=?,acked_at=? "
                    "WHERE job_id=? AND role='result'",
                    (row["storage_ref"], row["schema_version"], row["persisted_at"],
                     row["received_at"], row["job_id"]),
                )
                self.audit(
                    "delivery.reconciled", job_id=row["job_id"], source=row["source"],
                    attempt_no=row["attempt_count"] or None,
                    from_state=row["state"], to_state=row["state"],
                    reason="durable receipt repaired derived job state",
                    occurred=self.clock(), metadata={"receipt_id": row["receipt_id"]},
                )
                repaired += 1
        return repaired

    def maintenance(
        self, source: str, *, operation: str = "preview", limit: int = 100,
        confirm: bool = False, max_bytes: int = 16 * 1024 * 1024,
    ) -> dict[str, Any]:
        source = _nonempty(source, "source", 256)
        if operation not in {"preview", "quarantine", "compact"}:
            raise StorageContractError("operation must be preview, quarantine, or compact")
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
            raise StorageContractError("limit must be an integer from 1 through 1000")
        if (
            isinstance(max_bytes, bool) or not isinstance(max_bytes, int)
            or not 1 <= max_bytes <= 64 * 1024 * 1024
        ):
            raise StorageContractError("max_bytes must be from 1 through 67108864")
        config = self._source_config(source)
        guards = {
            "producer_storage_enabled": bool(config["producer_storage_enabled"]),
            "ack_required": bool(config["ack_required"]),
            "compaction_enabled": bool(config["compaction_enabled"]),
            "legacy_result_fallback_disabled": not bool(config["legacy_result_fallback"]),
        }
        if not all(guards.values()):
            raise StorageContractError("source compaction guards are not all enabled")
        now = self.clock()
        state = "full" if operation in {"preview", "quarantine"} else "quarantined"
        time_clause = "j.compaction_after IS NOT NULL AND j.compaction_after<=?"
        threshold = now
        if operation == "compact":
            time_clause = "j.quarantined_at IS NOT NULL AND j.quarantined_at<=?"
            threshold = now - float(config["quarantine_grace_seconds"])
        inline_expression = (
            "length(CAST(j.payload AS BLOB))+"
            "coalesce(length(CAST(j.result_json AS BLOB)),0)"
        )
        eligibility = (
            "FROM jobs j WHERE j.source=? AND j.state='completed' "
            "AND j.delivery_state='acked' AND j.compaction_state=? AND " + time_clause + " "
            "AND j.ack_required=1 AND j.legacy_result_fallback=0 "
            "AND j.producer_attempt_id IS NOT NULL "
            "AND EXISTS(SELECT 1 FROM job_delivery_acks a WHERE a.job_id=j.id "
            "AND a.producer=j.source AND a.producer_attempt_id=j.producer_attempt_id "
            "AND a.result_hash=j.result_hash AND a.storage_ref=j.result_ref) "
            "AND NOT EXISTS(SELECT 1 FROM job_delivery_ack_conflicts c WHERE c.job_id=j.id) "
            "AND EXISTS(SELECT 1 FROM job_artifacts a WHERE a.job_id=j.id "
            "AND a.role='result' AND a.state='acked' AND a.content_hash=j.result_hash) "
            "AND j.input_storage_mode!='broker_temporary' "
            "AND EXISTS("
            "SELECT 1 FROM job_artifacts a WHERE a.job_id=j.id AND a.role='input' "
            "AND a.state='acked' AND a.content_hash=j.input_hash "
            "AND a.storage_ref=j.input_ref) "
        )
        sql = (
            "SELECT j.id," + inline_expression + " AS inline_bytes " + eligibility
            + "AND " + inline_expression + "<=? "
            "ORDER BY j.compaction_after,j.id LIMIT 1000"
        )
        oversize_sql = (
            "SELECT count(*) FROM (SELECT 1 " + eligibility
            + "AND " + inline_expression + ">? "
            "ORDER BY j.compaction_after,j.id LIMIT 1000)"
        )
        with self.lock, self.db:
            params = (source, state, threshold, max_bytes)
            rows = list(self.db.execute(sql, params))
            skipped_oversize = int(self.db.execute(oversize_sql, params).fetchone()[0])
            ids: list[str] = []
            inline_bytes = 0
            for row in rows:
                row_bytes = int(row["inline_bytes"] or 0)
                if inline_bytes + row_bytes > max_bytes or len(ids) >= limit:
                    break
                ids.append(row["id"])
                inline_bytes += row_bytes
            changed = 0
            if operation != "preview":
                if confirm is not True:
                    raise StorageContractError("maintenance mutation requires confirm=true")
                for job_id in ids:
                    if operation == "quarantine":
                        cursor = self.db.execute(
                            "UPDATE jobs SET compaction_state='quarantined',quarantined_at=? "
                            "WHERE id=? AND state='completed' AND delivery_state='acked' "
                            "AND compaction_state='full'",
                            (now, job_id),
                        )
                        event = "storage.quarantined"
                    else:
                        cursor = self.db.execute(
                            "UPDATE jobs SET payload='{}',result_json=NULL,"
                            "compaction_state='metadata_only',compacted_at=? "
                            "WHERE id=? AND state='completed' AND delivery_state='acked' "
                            "AND compaction_state='quarantined'",
                            (now, job_id),
                        )
                        self.db.execute(
                            "UPDATE job_artifacts SET state='deleted',deleted_at=? "
                            "WHERE job_id=? AND role='input' AND storage_backend='broker-temp'",
                            (now, job_id),
                        )
                        event = "storage.compacted"
                    if cursor.rowcount:
                        row = self.db.execute(
                            "SELECT source,attempt_count,state FROM jobs WHERE id=?", (job_id,)
                        ).fetchone()
                        self.audit(
                            event, job_id=job_id, source=row["source"],
                            attempt_no=row["attempt_count"] or None,
                            from_state=row["state"], to_state=row["state"], occurred=now,
                            reason="receipt-guarded producer storage maintenance",
                        )
                        changed += 1
        return {
            "source": source,
            "operation": operation,
            "candidate_count": len(ids),
            "candidate_inline_bytes": inline_bytes,
            "max_bytes": max_bytes,
            "skipped_oversize": skipped_oversize,
            "changed": changed,
            "job_ids": ids,
            "guards": guards,
            "vacuum_performed": False,
        }

    def wal_snapshot(self) -> dict[str, Any]:
        wal_bytes = _file_size(f"{self.database}-wal")
        return {
            "db_bytes": _file_size(self.database),
            "wal_bytes": wal_bytes,
            "wal_budget_bytes": self.wal_budget_bytes,
            "wal_over_budget": wal_bytes > self.wal_budget_bytes,
            "wal_autocheckpoint_pages": self.wal_autocheckpoint_pages,
            "journal_size_limit_bytes": self.journal_size_limit_bytes,
            "checkpoint_interval_seconds": self.checkpoint_interval_seconds,
            "last_checkpoint": dict(self.last_checkpoint) if self.last_checkpoint else None,
            "checkpoint_mode": "PASSIVE",
            "truncate_enabled": False,
            "vacuum_enabled": False,
        }

    def maybe_checkpoint(self, *, force: bool = False) -> dict[str, Any] | None:
        now = self.clock()
        if not force and now - self._last_checkpoint_at < self.checkpoint_interval_seconds:
            return None
        with self.lock:
            row = self.db.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
        snapshot = {
            "timestamp": now,
            "busy": int(row[0]),
            "log_frames": int(row[1]),
            "checkpointed_frames": int(row[2]),
            "wal_bytes": _file_size(f"{self.database}-wal"),
            "mode": "PASSIVE",
        }
        self._last_checkpoint_at = now
        self.last_checkpoint = snapshot
        level = logging.WARNING if snapshot["busy"] or snapshot["wal_bytes"] > self.wal_budget_bytes else logging.INFO
        LOGGER.log(level, json.dumps({"event": "wal.checkpoint", **snapshot}, sort_keys=True))
        return dict(snapshot)

    def health(self) -> dict[str, Any]:
        with self.lock:
            counts = dict(self.db.execute(
                "SELECT "
                "(SELECT count(*) FROM job_delivery_acks) AS receipts,"
                "(SELECT count(*) FROM job_delivery_ack_conflicts) AS conflicts,"
                "(SELECT count(*) FROM jobs INDEXED BY jobs_storage_unacked_terminal "
                "WHERE state='completed' "
                "AND result_storage_mode!='broker_temporary' AND delivery_state!='acked') "
                "AS unacked_terminal,"
                "(SELECT count(*) FROM jobs INDEXED BY jobs_storage_quarantined "
                "WHERE compaction_state='quarantined') AS quarantined"
            ).fetchone())
            effective_wal_autocheckpoint_pages = int(
                self.db.execute("PRAGMA wal_autocheckpoint").fetchone()[0]
            )
            effective_journal_size_limit_bytes = int(
                self.db.execute("PRAGMA journal_size_limit").fetchone()[0]
            )
        wal = self.wal_snapshot()
        wal.update({
            "effective_wal_autocheckpoint_pages": effective_wal_autocheckpoint_pages,
            "effective_journal_size_limit_bytes": effective_journal_size_limit_bytes,
        })
        return {
            "schema_version": STORAGE_SCHEMA_VERSION,
            **counts,
            "wal": wal,
            "destructive_compaction_default": False,
        }
