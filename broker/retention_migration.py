"""Read-only retention forecast and out-of-place historical adoption."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from .policy import normalize_source_policy
from .storage import (
    DEFAULT_DATABASE_TARGET_BYTES,
    DEFAULT_GLOBAL_INLINE_BUDGET_BYTES,
    STORAGE_SCHEMA_VERSION,
    _attempt,
    _hash,
    _persisted_at,
    _schema,
    _storage_ref,
    canonical_json_bytes,
    content_evidence,
    migrate_storage_schema,
)
from .storage_policy import same_file


ACTIVE_STATES = ("queued", "running", "cancel_requested")
TERMINAL_STATES = ("completed", "failed", "cancelled")


class RetentionMigrationError(ValueError):
    pass


def _open_read_only(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    return connection


def _database_metrics(db: sqlite3.Connection) -> dict[str, int]:
    page_size = int(db.execute("PRAGMA page_size").fetchone()[0])
    page_count = int(db.execute("PRAGMA page_count").fetchone()[0])
    freelist_count = int(db.execute("PRAGMA freelist_count").fetchone()[0])
    return {
        "page_size": page_size,
        "page_count": page_count,
        "freelist_count": freelist_count,
        "logical_bytes": page_size * page_count,
        "freelist_bytes": page_size * freelist_count,
        "auto_vacuum_mode": int(db.execute("PRAGMA auto_vacuum").fetchone()[0]),
    }


def _queued_at_plan(db: sqlite3.Connection) -> list[str]:
    return [
        str(row[3])
        for row in db.execute(
            "EXPLAIN QUERY PLAN UPDATE jobs SET queued_at=created WHERE queued_at IS NULL"
        )
    ]


def _group_forecast(db: sqlite3.Connection) -> list[dict[str, Any]]:
    # Do not use a SQL GROUP BY here.  SQLite's sorter records retain the
    # aggregate input values, which makes a forecast over multi-megabyte
    # payload/result columns consume database-sized temp/swap space.  Stream
    # only group keys and measured byte counts; the number of groups is bounded
    # by the source/state/storage-mode cardinality, not by job or body size.
    groups: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in db.execute(
        "SELECT source,state,delivery_state,input_storage_mode,result_storage_mode,"
        "compaction_state,length(CAST(payload AS BLOB)) AS payload_bytes,"
        "coalesce(length(CAST(result_json AS BLOB)),0) AS result_bytes "
        "FROM jobs"
    ):
        key = tuple(row[name] for name in (
            "source", "state", "delivery_state", "input_storage_mode",
            "result_storage_mode", "compaction_state",
        ))
        group = groups.setdefault(key, {
            "source": key[0],
            "state": key[1],
            "delivery_state": key[2],
            "input_storage_mode": key[3],
            "result_storage_mode": key[4],
            "compaction_state": key[5],
            "rows": 0,
            "payload_bytes": 0,
            "result_bytes": 0,
            "inline_bytes": 0,
        })
        payload_bytes = int(row["payload_bytes"] or 0)
        result_bytes = int(row["result_bytes"] or 0)
        group["rows"] += 1
        group["payload_bytes"] += payload_bytes
        group["result_bytes"] += result_bytes
        group["inline_bytes"] += payload_bytes + result_bytes
    return sorted(
        groups.values(),
        key=lambda row: (
            str(row["source"]), str(row["state"]), str(row["delivery_state"]),
            str(row["input_storage_mode"]), str(row["result_storage_mode"]),
            str(row["compaction_state"]),
        ),
    )


def _proven_compactable(db: sqlite3.Connection) -> dict[str, int]:
    row = db.execute(
        "SELECT count(*) AS rows,coalesce(sum(length(CAST(j.payload AS BLOB))+"
        "coalesce(length(CAST(j.result_json AS BLOB)),0)),0) AS inline_bytes "
        "FROM jobs j WHERE j.state='completed' AND j.delivery_state='acked' "
        "AND j.input_storage_mode!='broker_temporary' "
        "AND j.result_storage_mode!='broker_temporary' "
        "AND NOT EXISTS(SELECT 1 FROM job_delivery_ack_conflicts c WHERE c.job_id=j.id) "
        "AND EXISTS(SELECT 1 FROM job_delivery_acks a WHERE a.job_id=j.id "
        "AND a.producer=j.source AND a.producer_attempt_id=j.producer_attempt_id "
        "AND a.result_hash=j.result_hash AND a.storage_ref=j.result_ref) "
        "AND EXISTS(SELECT 1 FROM job_artifacts a WHERE a.job_id=j.id "
        "AND a.role='input' AND a.state='acked' AND a.content_hash=j.input_hash "
        "AND a.storage_ref=j.input_ref)"
    ).fetchone()
    return {"rows": int(row["rows"]), "inline_bytes": int(row["inline_bytes"])}


def _group_totals(
    groups: list[dict[str, Any]], states: tuple[str, ...],
) -> dict[str, int]:
    selected = [row for row in groups if row["state"] in states]
    return {
        "rows": sum(int(row["rows"] or 0) for row in selected),
        "inline_bytes": sum(int(row["inline_bytes"] or 0) for row in selected),
    }


def _protected_identity(db: sqlite3.Connection) -> dict[str, Any]:
    digest = hashlib.sha256()
    rows = 0
    for row in db.execute(
        "SELECT id,profile,kind,source,payload,state,created,started,lease_until,"
        "source_item_id,external_id,queued_at,attempt_count,retry_count,requeue_count "
        "FROM jobs WHERE state IN ('queued','running','cancel_requested') "
        "ORDER BY queued_at,id"
    ):
        digest.update(canonical_json_bytes(dict(row)))
        digest.update(b"\n")
        rows += 1
    return {"rows": rows, "sha256": digest.hexdigest()}


def _load_manifests(paths: list[Path]) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    seen: set[str] = set()
    for path in paths:
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict) or raw.get("version") != 1:
            raise RetentionMigrationError(f"{path}: manifest version must be 1")
        source = raw.get("source")
        if not isinstance(source, str) or not source:
            raise RetentionMigrationError(f"{path}: source must be a non-empty string")
        raw_entries = raw.get("entries")
        if not isinstance(raw_entries, list):
            raise RetentionMigrationError(f"{path}: entries must be an array")
        for value in raw_entries:
            if not isinstance(value, dict):
                raise RetentionMigrationError(f"{path}: each entry must be an object")
            entry = {**value, "source": source, "manifest": str(path)}
            job_id = entry.get("job_id")
            if not isinstance(job_id, str) or not job_id or job_id in seen:
                raise RetentionMigrationError(f"{path}: duplicate or invalid job_id")
            seen.add(job_id)
            entries.append(entry)
    return entries


def _artifact(value: Any, role: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RetentionMigrationError(f"{role} evidence must be an object")
    required = {"storage_ref", "content_hash", "byte_size", "persisted_at", "readback_at"}
    if role == "result":
        required.add("schema_version")
    if set(value) != required:
        raise RetentionMigrationError(f"{role} evidence fields do not match the contract")
    byte_size = value["byte_size"]
    if isinstance(byte_size, bool) or not isinstance(byte_size, int) or byte_size < 0:
        raise RetentionMigrationError(f"{role}.byte_size must be a non-negative integer")
    result = {
        "storage_ref": _storage_ref(value["storage_ref"]),
        "content_hash": _hash(value["content_hash"], f"{role}.content_hash"),
        "byte_size": byte_size,
        "persisted_at": _persisted_at(value["persisted_at"]),
        "readback_at": _persisted_at(value["readback_at"]),
    }
    persisted = datetime.fromisoformat(result["persisted_at"].replace("Z", "+00:00"))
    readback = datetime.fromisoformat(result["readback_at"].replace("Z", "+00:00"))
    if readback < persisted:
        raise RetentionMigrationError(f"{role}.readback_at precedes persisted_at")
    if role == "result":
        result["schema_version"] = _schema(value["schema_version"])
    return result


def validate_manifest_entry(db: sqlite3.Connection, value: dict[str, Any]) -> dict[str, Any]:
    row = db.execute("SELECT * FROM jobs WHERE id=?", (value.get("job_id"),)).fetchone()
    if row is None:
        raise RetentionMigrationError("manifest job does not exist")
    if row["source"] != value["source"]:
        raise RetentionMigrationError("manifest source does not match job source")
    if row["state"] != "completed":
        raise RetentionMigrationError("historical adoption accepts only completed jobs")
    if row["delivery_state"] == "acked":
        raise RetentionMigrationError("job already has a durable result ACK")
    if db.execute(
        "SELECT 1 FROM job_delivery_ack_conflicts WHERE job_id=?", (row["id"],)
    ).fetchone() is not None:
        raise RetentionMigrationError("job has a delivery conflict")
    attempt = _attempt(value.get("producer_attempt_id"))
    input_evidence = _artifact(value.get("input"), "input")
    result_evidence = _artifact(value.get("result"), "result")
    try:
        payload = json.loads(row["payload"])
        result = json.loads(row["result_json"])
    except (TypeError, ValueError) as error:
        raise RetentionMigrationError("job inline bodies are not canonical JSON") from error
    input_hash, input_bytes = content_evidence(payload)
    result_hash, result_bytes = content_evidence(result)
    if (input_hash, input_bytes) != (
        input_evidence["content_hash"], input_evidence["byte_size"],
    ):
        raise RetentionMigrationError("input readback evidence disagrees with broker payload")
    if (result_hash, result_bytes) != (
        result_evidence["content_hash"], result_evidence["byte_size"],
    ):
        raise RetentionMigrationError("result readback evidence disagrees with broker result")
    return {
        "job_id": row["id"],
        "source": row["source"],
        "producer_attempt_id": attempt,
        "input": input_evidence,
        "result": result_evidence,
        "inline_bytes": len(row["payload"].encode()) + len(row["result_json"].encode()),
    }


def forecast(
    db: sqlite3.Connection, database: Path, manifests: list[dict[str, Any]],
) -> dict[str, Any]:
    validated: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    for entry in manifests:
        try:
            validated.append(validate_manifest_entry(db, entry))
        except (RetentionMigrationError, ValueError) as error:
            errors.append({"job_id": str(entry.get("job_id")), "error": str(error)})
    groups = _group_forecast(db)
    proven = _proven_compactable(db)
    active = _group_totals(groups, ACTIVE_STATES)
    retryable = _group_totals(groups, ("failed", "cancelled"))
    terminal = _group_totals(groups, TERMINAL_STATES)
    metrics = _database_metrics(db)
    manifest_bytes = sum(item["inline_bytes"] for item in validated)
    estimated_after_proven = max(
        metrics["logical_bytes"] - metrics["freelist_bytes"]
        - proven["inline_bytes"] - manifest_bytes,
        active["inline_bytes"],
    )
    has_migrations = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' "
        "AND name='broker_schema_migrations'"
    ).fetchone() is not None
    return {
        "database": str(database),
        "read_only": True,
        "schema_version": STORAGE_SCHEMA_VERSION,
        "source_schema_migrations": [
            dict(row) for row in db.execute(
                "SELECT version,name,applied_at FROM broker_schema_migrations "
                "ORDER BY version"
            )
        ] if has_migrations else [],
        "source_queued_at_plan": _queued_at_plan(db),
        "groups": groups,
        "physical": metrics,
        "proven_compactable": proven,
        "manifest_validated": {
            "rows": len(validated),
            "inline_bytes": manifest_bytes,
            "errors": errors,
        },
        "protected_active": active,
        "retryable_terminal": retryable,
        "all_terminal": terminal,
        "estimated_repacked_bytes_after_proven": estimated_after_proven,
        "database_target_bytes": DEFAULT_DATABASE_TARGET_BYTES,
        "global_inline_budget_bytes": DEFAULT_GLOBAL_INLINE_BUDGET_BYTES,
        "estimated_target_met": estimated_after_proven < DEFAULT_DATABASE_TARGET_BYTES,
        "expected_wal_bytes_after_repack": 0,
        "private_bodies_reported": False,
    }


def _copy_database(source: Path, destination: Path) -> sqlite3.Connection:
    if destination.exists():
        raise RetentionMigrationError("output database already exists")
    destination.parent.mkdir(parents=True, exist_ok=True)
    source_db = _open_read_only(source)
    target = sqlite3.connect(destination)
    target.row_factory = sqlite3.Row
    try:
        source_db.backup(target)
    finally:
        source_db.close()
    return target


def _policy_for_source(policy: dict[str, dict[str, Any]], source: str) -> dict[str, Any]:
    config = policy.get(source)
    if config is None:
        raise RetentionMigrationError(f"source {source!r} is absent from policy")
    guards = {
        "retention_enabled": config["retention_enabled"],
        "producer_storage_enabled": config["producer_storage_enabled"],
        "ack_required": config["ack_required"],
        "compaction_enabled": config["compaction_enabled"],
        "legacy_result_fallback_disabled": not config["legacy_result_fallback"],
    }
    if not all(guards.values()):
        raise RetentionMigrationError(
            f"source {source!r} retention guards are not all enabled"
        )
    return config


def _apply_entry(
    db: sqlite3.Connection, entry: dict[str, Any], config: dict[str, Any], now: float,
) -> None:
    job_id = entry["job_id"]
    receipt_id = hashlib.sha256(
        f"retention-migration:{job_id}:{entry['producer_attempt_id']}".encode()
    ).hexdigest()
    receipt_id = f"migration-{receipt_id[:32]}"
    metadata_until = now + float(config["metadata_retention_seconds"])
    receipt_until = now + float(config["receipt_retention_seconds"])
    db.execute(
        "UPDATE jobs SET input_storage_mode='producer_owned',input_ref=?,input_hash=?,"
        "input_bytes=?,input_received_at=?,result_storage_mode='producer_owned',result_ref=?,"
        "result_hash=?,result_bytes=?,delivery_state='acked',acked_at=?,"
        "artifact_schema_version=?,producer_attempt_id=?,ack_required=1,"
        "legacy_result_fallback=0,body_retention_until=?,metadata_retention_until=?,"
        "compaction_after=?,compaction_state='metadata_only',retention_class='producer',"
        "payload='{}',result_json=NULL,compacted_at=? WHERE id=? AND state='completed'",
        (entry["input"]["storage_ref"], entry["input"]["content_hash"],
         entry["input"]["byte_size"], now, entry["result"]["storage_ref"],
         entry["result"]["content_hash"], entry["result"]["byte_size"], now,
         entry["result"]["schema_version"], entry["producer_attempt_id"], now,
         metadata_until, now, now, job_id),
    )
    for role in ("input", "result"):
        artifact = entry[role]
        db.execute(
            "INSERT INTO job_artifacts(id,job_id,role,storage_backend,storage_ref,"
            "content_hash,byte_size,schema_version,state,created_at,persisted_at,acked_at,"
            "deleted_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(job_id,role) DO UPDATE SET storage_backend='producer',"
            "storage_ref=excluded.storage_ref,content_hash=excluded.content_hash,"
            "byte_size=excluded.byte_size,schema_version=excluded.schema_version,"
            "state='acked',persisted_at=excluded.persisted_at,acked_at=excluded.acked_at,"
            "deleted_at=excluded.deleted_at",
            (f"{job_id}:{role}", job_id, role, "producer", artifact["storage_ref"],
             artifact["content_hash"], artifact["byte_size"],
             artifact.get("schema_version"), "acked", now, artifact["persisted_at"],
             now, None),
        )
    db.execute(
        "INSERT OR REPLACE INTO job_delivery_acks(receipt_id,job_id,producer,"
        "producer_attempt_id,storage_ref,result_hash,result_bytes,schema_version,"
        "persisted_at,received_at,retention_until) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        (receipt_id, job_id, entry["source"], entry["producer_attempt_id"],
         entry["result"]["storage_ref"], entry["result"]["content_hash"],
         entry["result"]["byte_size"], entry["result"]["schema_version"],
         entry["result"]["persisted_at"], now, receipt_until),
    )
    db.execute(
        "INSERT INTO audit_events(occurred,event_type,job_id,source,from_state,to_state,"
        "reason,metadata_json,producer_storage) VALUES(?,?,?,?,?,?,?,?,1)",
        (now, "storage.historical_adopted", job_id, entry["source"], "completed",
         "completed", "manifest hash/readback evidence adopted out of place", "{}"),
    )


def build_repacked_database(
    source: Path, output: Path, policy_path: Path, entries: list[dict[str, Any]],
) -> dict[str, Any]:
    policy = normalize_source_policy(json.loads(policy_path.read_text(encoding="utf-8")))
    source_db = _open_read_only(source)
    try:
        protected_before = _protected_identity(source_db)
    finally:
        source_db.close()
    staging = output.with_name(f".{output.name}.staging")
    if staging.exists() or output.exists():
        raise RetentionMigrationError("output or staging database already exists")
    db = _copy_database(source, staging)
    now = time.time()
    try:
        with db:
            migrate_storage_schema(db)
            validated = [validate_manifest_entry(db, entry) for entry in entries]
            for entry in validated:
                _apply_entry(db, entry, _policy_for_source(policy, entry["source"]), now)
        db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        db.execute("PRAGMA journal_mode=DELETE")
        db.execute("PRAGMA auto_vacuum=INCREMENTAL")
        db.execute("VACUUM")
        integrity = db.execute("PRAGMA integrity_check").fetchone()[0]
        foreign_keys = len(list(db.execute("PRAGMA foreign_key_check")))
        protected_after = _protected_identity(db)
        queued_at_plan = _queued_at_plan(db)
        if integrity != "ok" or foreign_keys:
            raise RetentionMigrationError("repacked database failed integrity verification")
        if protected_after != protected_before:
            raise RetentionMigrationError("repacked database changed active job identity or FIFO")
    finally:
        db.close()
    os.replace(staging, output)
    descriptor = os.open(output, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    descriptor = os.open(output.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return {
        "output_database": str(output),
        "output_bytes": output.stat().st_size,
        "output_wal_bytes": Path(f"{output}-wal").stat().st_size
        if Path(f"{output}-wal").exists() else 0,
        "adopted_rows": len(entries),
        "integrity_check": "ok",
        "foreign_key_violation_count": 0,
        "auto_vacuum_mode": 2,
        "protected_jobs": protected_after,
        "queued_at_plan": queued_at_plan,
        "source_unchanged": True,
    }


def build_floor_database(source: Path, output: Path) -> dict[str, Any]:
    """Build a non-deployable lower-bound artifact; never claims durability."""
    source_db = _open_read_only(source)
    try:
        protected_before = _protected_identity(source_db)
    finally:
        source_db.close()
    db = _copy_database(source, output)
    try:
        with db:
            migrate_storage_schema(db)
            db.execute(
                "UPDATE jobs SET payload='{}',result_json=NULL "
                "WHERE state NOT IN ('queued','running','cancel_requested')"
            )
        db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        db.execute("PRAGMA journal_mode=DELETE")
        db.execute("PRAGMA auto_vacuum=INCREMENTAL")
        db.execute("VACUUM")
        integrity = db.execute("PRAGMA integrity_check").fetchone()[0]
        foreign_keys = len(list(db.execute("PRAGMA foreign_key_check")))
        protected_after = _protected_identity(db)
        queued_at_plan = _queued_at_plan(db)
        if protected_after != protected_before:
            raise RetentionMigrationError("floor database changed active job identity or FIFO")
    finally:
        db.close()
    return {
        "floor_database": str(output),
        "floor_bytes": output.stat().st_size,
        "expected_wal_bytes": Path(f"{output}-wal").stat().st_size
        if Path(f"{output}-wal").exists() else 0,
        "integrity_check": integrity,
        "foreign_key_violation_count": foreign_keys,
        "protected_jobs": protected_after,
        "queued_at_plan": queued_at_plan,
        "non_deployable": True,
        "reason": "terminal bodies removed without producer durability proof",
        "source_unchanged": True,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Forecast or build an out-of-place retention migration",
    )
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--policy", type=Path)
    parser.add_argument("--manifest", action="append", default=[], type=Path)
    parser.add_argument("--output-database", type=Path)
    parser.add_argument("--floor-output", type=Path)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    if not args.database.is_file():
        parser.error("--database must name an existing SQLite file")
    for path in (args.output_database, args.floor_output, args.report):
        if path is not None and same_file(args.database, path):
            parser.error("output paths must not alias --database")
    if (
        args.report is not None
        and args.output_database is not None
        and same_file(args.report, args.output_database)
    ):
        parser.error("--report must not alias --output-database")
    if (
        args.report is not None
        and args.floor_output is not None
        and same_file(args.report, args.floor_output)
    ):
        parser.error("--report must not alias --floor-output")
    if args.output_database is not None and (args.policy is None or not args.manifest):
        parser.error("--output-database requires --policy and at least one --manifest")
    if args.output_database is not None and args.floor_output is not None:
        parser.error("--output-database and --floor-output are mutually exclusive")
    try:
        manifests = _load_manifests(args.manifest)
        source = _open_read_only(args.database)
        try:
            report = forecast(source, args.database, manifests)
        finally:
            source.close()
        if args.output_database is not None:
            if report["manifest_validated"]["errors"]:
                raise RetentionMigrationError(
                    "manifest validation failed; refusing to create output database"
                )
            report["repacked"] = build_repacked_database(
                args.database, args.output_database, args.policy, manifests,
            )
        if args.floor_output is not None:
            report["physical_floor"] = build_floor_database(
                args.database, args.floor_output,
            )
    except (OSError, ValueError, sqlite3.Error, json.JSONDecodeError) as error:
        parser.error(str(error))
    encoded = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.report is not None:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(encoded, encoding="utf-8")
    print(encoded, end="")


if __name__ == "__main__":
    main()
