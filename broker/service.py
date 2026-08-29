from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
import tempfile
import uuid
from pathlib import Path
from typing import Any

from .analytics import analytics_snapshot, attempt_history, audit_history
from .dashboard import snapshot as dashboard_snapshot
from .compat import (CompatibilityError, validate_olya_decision_payload,
                     validate_olya_vision_payload,
                     validate_shutterstock_canary_payload,
                     validate_shutterstock_video_payload,
                     validate_syncopia_memory_payload)
from .profiles import PROFILES
from .policy import SourcePolicyError, normalize_source_policy


LOGGER = logging.getLogger("ollama_inference_broker.audit")

# Used only to keep upgraded databases rollback-compatible.  The current
# scheduler intentionally does not consult these values.
LEGACY_SOURCE_PRIORITIES = {
    "interactive": 1,
    "cron": 2,
    "shutterstock-video": 3,
    "syncopia-telegram-memory": 4,
    "shutterstock-canary": 5,
    "olya-decision": 6,
    "olya": 8,
    "olya-vision": 8,
}


class SourcePolicy:
    """Runtime-reloadable per-source weights and enablement.

    The policy file is a JSON document replaced atomically (write temp +
    rename).  The broker checks it on every dispatch attempt and reloads when
    the file identity, mtime or size changes, so source/weight changes never
    requires a restart or a queue drain.  A missing or invalid file keeps the
    previously loaded policy (fail closed), so a bad write cannot disable the
    whole broker silently.

    Shape::

        {
          "version": 1,
          "sources": {
            "shutterstock-video": {"enabled": true, "weight": 1.0},
            "pilot-mainpc":      {"enabled": true, "weight": 3.0}
          }
        }
    """

    def __init__(self, path: str | Path, clock=time.time) -> None:
        self.path = Path(path)
        self.clock = clock
        self._lock = threading.RLock()
        self._signature: tuple[int, int, int] | None = None
        self._sources: dict[str, dict[str, Any]] = {}

    def _load_locked(self) -> None:
        try:
            stat = self.path.stat()
        except OSError:
            return  # keep previous policy; file not created yet
        signature = (stat.st_mtime_ns, stat.st_ino, stat.st_size)
        if signature == self._signature:
            return
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return  # atomic replace means this is only a transient partial read
        try:
            normalized = normalize_source_policy(raw)
        except SourcePolicyError:
            if self._signature is None:
                raise
            # A bad hot reload keeps the last known-good policy and cannot
            # terminate the dispatcher thread.
            self._signature = signature
            return
        self._sources = normalized
        self._signature = signature

    def snapshot(self) -> dict[str, Any]:
        """Return the current effective policy for observability (no secrets)."""
        with self._lock:
            self._load_locked()
            return {
                "path": str(self.path),
                "sources": {name: dict(entry) for name, entry in self._sources.items()},
            }

    def enabled_sources(self) -> frozenset[str]:
        with self._lock:
            self._load_locked()
            return frozenset(
                name for name, entry in self._sources.items() if entry["enabled"]
            )

    def weight(self, source: str) -> float | None:
        with self._lock:
            self._load_locked()
            entry = self._sources.get(source)
            return entry["weight"] if entry else None

    def set_weight(self, source: str, weight: Any) -> int:
        """Atomically update one configured source with a dashboard-safe weight."""
        if isinstance(weight, bool) or not isinstance(weight, int) or not 1 <= weight <= 10:
            raise SourcePolicyError("weight must be an integer from 1 through 10")
        with self._lock:
            try:
                raw = json.loads(self.path.read_text(encoding="utf-8"))
                normalized = normalize_source_policy(raw)
            except (OSError, ValueError, SourcePolicyError) as error:
                raise SourcePolicyError("policy is unavailable or invalid") from error
            if source not in normalized:
                raise SourcePolicyError(f"source {source!r} is not configured")

            raw["sources"][source]["weight"] = weight
            try:
                original_mode = self.path.stat().st_mode & 0o7777
                descriptor, temporary = tempfile.mkstemp(
                    prefix=f".{self.path.name}.", suffix=".tmp", dir=self.path.parent,
                )
                with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                    json.dump(raw, handle, indent=2, sort_keys=True)
                    handle.write("\n")
                    handle.flush()
                    os.fsync(handle.fileno())
                os.chmod(temporary, original_mode)
                os.replace(temporary, self.path)
            except OSError as error:
                try:
                    os.unlink(temporary)
                except (OSError, UnboundLocalError):
                    pass
                raise SourcePolicyError("unable to save policy") from error
            self._signature = None
            self._load_locked()
            return weight


class Broker:
    """One-resource scheduler; MAIN-PC remains only the wakeable executor."""
    def __init__(self, database: str | Path, ollama, wol, clock=time.time, lease_seconds=60):
        self.ollama, self.wol, self.clock, self.lease_seconds = ollama, wol, clock, lease_seconds
        self.db = sqlite3.connect(str(database), check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.lock = threading.RLock()
        self.completed = threading.Condition(self.lock)
        self._health_cache = {
            "status": "starting", "resource": "mainpc-gpu", "queue_depth": 0,
            "active_job_id": None, "timestamp": self.clock(),
        }
        self._init_db()
        self.recover()
        with self.lock:
            self._refresh_health_cache_locked()

    def _refresh_health_cache_locked(self) -> None:
        queued = self.db.execute("SELECT count(*) FROM jobs WHERE state='queued'").fetchone()[0]
        active = self.db.execute(
            "SELECT id FROM jobs WHERE state IN ('running','cancel_requested') LIMIT 1"
        ).fetchone()
        self._health_cache = {
            "status": "busy" if active else "ready",
            "resource": "mainpc-gpu",
            "queue_depth": queued,
            "active_job_id": active["id"] if active else None,
            "timestamp": self.clock(),
        }

    def _init_db(self):
        with self.db:
            self.db.execute("PRAGMA journal_mode=WAL")
            jobs_schema = """CREATE TABLE jobs (
                id TEXT PRIMARY KEY, profile TEXT NOT NULL, kind TEXT NOT NULL,
                source TEXT NOT NULL, priority INTEGER NOT NULL DEFAULT 10,
                payload TEXT NOT NULL, state TEXT NOT NULL, created REAL NOT NULL,
                started REAL, finished REAL, lease_until REAL, error TEXT,
                switch_reason TEXT, result_json TEXT,
                source_item_id TEXT, external_id TEXT, queued_at REAL,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                retry_count INTEGER NOT NULL DEFAULT 0,
                requeue_count INTEGER NOT NULL DEFAULT 0)"""
            self.db.execute(jobs_schema.replace(
                "CREATE TABLE jobs", "CREATE TABLE IF NOT EXISTS jobs", 1
            ))
            self.db.execute("""CREATE TABLE IF NOT EXISTS source_schedules (
                source TEXT PRIMARY KEY, next_allowed REAL NOT NULL)""")
            # Compatible with databases created by the first isolated MVP.
            columns = {row[1] for row in self.db.execute("PRAGMA table_info(jobs)")}
            self._has_legacy_priority = "priority" in columns
            if "source" not in columns:
                self.db.execute("ALTER TABLE jobs ADD COLUMN source TEXT NOT NULL DEFAULT 'legacy'")
            if "result_json" not in columns:
                self.db.execute("ALTER TABLE jobs ADD COLUMN result_json TEXT")
            if "source_item_id" not in columns:
                self.db.execute("ALTER TABLE jobs ADD COLUMN source_item_id TEXT")
            if "external_id" not in columns:
                self.db.execute("ALTER TABLE jobs ADD COLUMN external_id TEXT")
            if "queued_at" not in columns:
                self.db.execute("ALTER TABLE jobs ADD COLUMN queued_at REAL")
            if "attempt_count" not in columns:
                self.db.execute(
                    "ALTER TABLE jobs ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0"
                )
            if "retry_count" not in columns:
                self.db.execute(
                    "ALTER TABLE jobs ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0"
                )
            if "requeue_count" not in columns:
                self.db.execute(
                    "ALTER TABLE jobs ADD COLUMN requeue_count INTEGER NOT NULL DEFAULT 0"
                )
            self.db.execute("UPDATE jobs SET queued_at=created WHERE queued_at IS NULL")
            # Keep a legacy ``priority`` column in upgraded databases.  It is
            # never read by admission or dispatch, but preserving it makes a
            # rollback to the prior broker binary lossless and avoids SQLite
            # table-rebuild/index compatibility hazards.  Older schemas can
            # make it NOT NULL without a default, so admissions supply the
            # former fixed source value (or the neutral legacy default) when
            # that retained column exists.
            self.db.execute("""CREATE TABLE IF NOT EXISTS audit_events (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                occurred REAL NOT NULL,
                event_type TEXT NOT NULL,
                job_id TEXT,
                source TEXT,
                attempt_no INTEGER,
                from_state TEXT,
                to_state TEXT,
                reason TEXT,
                metadata_json TEXT NOT NULL DEFAULT '{}')""")
            self.db.execute("""CREATE TABLE IF NOT EXISTS job_attempts (
                job_id TEXT NOT NULL,
                attempt_no INTEGER NOT NULL,
                source TEXT NOT NULL,
                queued_at REAL,
                selected_at REAL NOT NULL,
                started REAL,
                lease_until REAL,
                finished REAL,
                outcome TEXT,
                error TEXT,
                scheduler_mode TEXT NOT NULL,
                selection_reason TEXT NOT NULL,
                policy_json TEXT,
                PRIMARY KEY(job_id, attempt_no))""")
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS audit_events_job ON audit_events(job_id,sequence)"
            )
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS audit_events_source_time "
                "ON audit_events(source,occurred,event_type)"
            )
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS jobs_source_state ON jobs(source,state,created)"
            )
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS jobs_source_item "
                "ON jobs(source,source_item_id)"
            )
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS jobs_external_id ON jobs(external_id)"
            )
            self.db.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS jobs_olya_vision_external "
                "ON jobs(source,external_id) WHERE source='olya-vision' "
                "AND external_id IS NOT NULL"
            )
            self.db.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS jobs_olya_decision_external "
                "ON jobs(source,external_id) WHERE source='olya-decision' "
                "AND external_id IS NOT NULL"
            )
            self.db.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS jobs_syncopia_memory_external "
                "ON jobs(source,external_id) WHERE source='syncopia-telegram-memory' "
                "AND external_id IS NOT NULL"
            )

    def _audit(
        self,
        event_type: str,
        *,
        job_id: str | None = None,
        source: str | None = None,
        attempt_no: int | None = None,
        from_state: str | None = None,
        to_state: str | None = None,
        reason: str | None = None,
        metadata: dict[str, Any] | None = None,
        occurred: float | None = None,
    ) -> None:
        timestamp = self.clock() if occurred is None else occurred
        safe_metadata = metadata or {}
        self.db.execute(
            "INSERT INTO audit_events(occurred,event_type,job_id,source,attempt_no,"
            "from_state,to_state,reason,metadata_json) VALUES(?,?,?,?,?,?,?,?,?)",
            (timestamp, event_type, job_id, source, attempt_no, from_state,
             to_state, reason, json.dumps(safe_metadata, sort_keys=True)),
        )
        # Deliberately omit payload, result, correlation values and error text.
        LOGGER.info(json.dumps({
            "event": event_type,
            "timestamp": timestamp,
            "job_id": job_id,
            "source": source,
            "attempt": attempt_no,
            "from_state": from_state,
            "to_state": to_state,
        }, sort_keys=True))

    def recover(self):
        # The remote outcome of an expired lease is ambiguous.  Do not submit it
        # again automatically: failure releases the scheduler while preserving
        # a durable request for an explicit, audited retry.
        now = self.clock()
        with self.lock, self.db:
            expired = list(self.db.execute(
                "SELECT id,source,state,attempt_count FROM jobs "
                "WHERE state IN ('running','cancel_requested') "
                "AND (lease_until IS NULL OR lease_until < ?)",
                (now,),
            ))
            for row in expired:
                attempt_no = row["attempt_count"] or None
                if attempt_no is not None:
                    self.db.execute(
                        "UPDATE job_attempts SET finished=?,outcome='lease_expired' "
                        "WHERE job_id=? AND attempt_no=? AND finished IS NULL",
                        (now, row["id"], attempt_no),
                    )
                self._audit(
                    "lease.expired", job_id=row["id"], source=row["source"],
                    attempt_no=attempt_no, from_state=row["state"],
                    reason="expired lease; remote outcome is unknown", occurred=now,
                )
                self.db.execute(
                    "UPDATE jobs SET state='failed',finished=?,lease_until=NULL,"
                    "error='lease expired; remote outcome unknown; explicit retry required' "
                    "WHERE id=?",
                    (now, row["id"]),
                )
                self._audit(
                    "job.failed", job_id=row["id"], source=row["source"],
                    attempt_no=attempt_no, from_state=row["state"],
                    to_state="failed", reason="expired lease recovery is fail-closed", occurred=now,
                )
            if expired:
                self._refresh_health_cache_locked()
            self.completed.notify_all()

    def submit(self, profile: str, kind: str, payload: dict, source: str | None = None,
               source_item_id: str | None = None,
               external_id: str | None = None) -> dict:
        if profile not in PROFILES:
            raise ValueError("unknown profile")
        if kind not in {"chat", "generate"}:
            raise ValueError("kind must be chat or generate")
        source = source or profile
        if profile == "shutterstock-canary":
            if source != profile:
                raise ValueError("shutterstock-canary must use its dedicated source")
            try:
                payload = validate_shutterstock_canary_payload(payload)
            except CompatibilityError as exc:
                raise ValueError(str(exc)) from exc
        if profile == "shutterstock-video":
            if source != profile:
                raise ValueError("shutterstock-video must use its dedicated source")
            try:
                payload = validate_shutterstock_video_payload(payload)
            except CompatibilityError as exc:
                raise ValueError(str(exc)) from exc
        if profile in {"olya-vision-gemma", "olya-vision-qwen"}:
            if source != "olya-vision":
                raise ValueError("Olya vision profiles must use their dedicated source")
            try:
                _, payload = validate_olya_vision_payload({
                    **payload,
                    "model": PROFILES[profile].model,
                })
            except CompatibilityError as exc:
                raise ValueError(str(exc)) from exc
        if profile == "olya-decision-qwen38":
            if source != "olya-decision":
                raise ValueError("Olya decision profile must use its dedicated source")
            try:
                payload = validate_olya_decision_payload({
                    **payload,
                    "model": PROFILES[profile].model,
                })
            except CompatibilityError as exc:
                raise ValueError(str(exc)) from exc
        if profile == "syncopia-memory-qwen38":
            if source != "syncopia-telegram-memory":
                raise ValueError("Syncopia memory profile must use its dedicated source")
            try:
                payload = validate_syncopia_memory_payload({
                    **payload,
                    "model": PROFILES[profile].model,
                    "tools": [],
                    "stream": False,
                })
            except CompatibilityError as exc:
                raise ValueError(str(exc)) from exc
        source_item_id = self._correlation_value("source_item_id", source_item_id)
        external_id = self._correlation_value("external_id", external_id)
        job_id, now = str(uuid.uuid4()), self.clock()
        with self.lock, self.db:
            if source in {"olya-vision", "olya-decision", "syncopia-telegram-memory"} and external_id is not None:
                existing = self.db.execute(
                    "SELECT id FROM jobs WHERE source=? AND external_id=? "
                    "ORDER BY created DESC,id DESC LIMIT 1",
                    (source, external_id),
                ).fetchone()
                if existing is not None:
                    return self.status(existing["id"])
            if self._has_legacy_priority:
                legacy_priority = LEGACY_SOURCE_PRIORITIES.get(source, 10)
                self.db.execute(
                    "INSERT INTO jobs(id,profile,kind,source,priority,payload,state,created,"
                    "queued_at,source_item_id,external_id) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (job_id, profile, kind, source, legacy_priority, json.dumps(payload), "queued", now,
                     now, source_item_id, external_id),
                )
            else:
                self.db.execute(
                    "INSERT INTO jobs(id,profile,kind,source,payload,state,created,"
                    "queued_at,source_item_id,external_id) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (job_id, profile, kind, source, json.dumps(payload), "queued", now,
                     now, source_item_id, external_id),
                )
            self._audit(
                "admission.accepted", job_id=job_id, source=source,
                from_state=None, to_state="queued", occurred=now,
                metadata={
                    "profile": profile,
                    "kind": kind,
                    "has_source_item_id": source_item_id is not None,
                    "has_external_id": external_id is not None,
                },
            )
            self._refresh_health_cache_locked()
        return self.status(job_id)

    @staticmethod
    def _correlation_value(name: str, value: str | None) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str) or not value.strip() or len(value) > 256:
            raise ValueError(f"{name} must be a non-empty string up to 256 characters")
        return value.strip()

    def status(self, job_id: str) -> dict | None:
        with self.lock:
            row = self.db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            return self._job(row) if row else None

    def _job(self, row):
        data = dict(row)
        data.pop("priority", None)  # legacy storage is not a scheduling input
        data["payload"] = json.loads(data["payload"])
        if data.get("result_json") is not None:
            data["result"] = json.loads(data.pop("result_json"))
        else:
            data.pop("result_json", None)
        if data["state"] == "queued":
            data["queue_position"] = self._position(row)
        return data

    def _position(self, row):
        before = self.db.execute("SELECT count(*) FROM jobs WHERE state='queued' AND "
            "(queued_at < ? OR (queued_at = ? AND id < ?))",
            (row["queued_at"], row["queued_at"], row["id"])).fetchone()[0]
        return before + 1

    def cancel(self, job_id: str) -> dict | None:
        with self.lock, self.db:
            row = self.db.execute(
                "SELECT state,source,attempt_count FROM jobs WHERE id=?", (job_id,)
            ).fetchone()
            if not row: return None
            if row["state"] == "queued":
                now = self.clock()
                self.db.execute(
                    "UPDATE jobs SET state='cancelled', finished=? WHERE id=?", (now, job_id)
                )
                self._audit(
                    "job.cancelled", job_id=job_id, source=row["source"],
                    from_state="queued", to_state="cancelled", occurred=now,
                )
            # Remote Ollama cancellation is not assumed safe; a running job is
            # marked cancel_requested and releases the slot after its request returns.
            elif row["state"] == "running":
                self.db.execute("UPDATE jobs SET state='cancel_requested' WHERE id=?", (job_id,))
                self._audit(
                    "job.cancel_requested", job_id=job_id, source=row["source"],
                    attempt_no=row["attempt_count"] or None,
                    from_state="running", to_state="cancel_requested",
                )
            self._refresh_health_cache_locked()
            self.completed.notify_all()
        return self.status(job_id)

    def retry(self, job_id: str) -> dict | None:
        """Explicitly requeue a failed/cancelled job without changing payload."""
        with self.lock, self.db:
            row = self.db.execute(
                "SELECT state,source,attempt_count FROM jobs WHERE id=?", (job_id,)
            ).fetchone()
            if row is None:
                return None
            if row["state"] not in {"failed", "cancelled"}:
                raise ValueError("only failed or cancelled jobs may be retried")
            now = self.clock()
            self._audit(
                "job.retry_requested", job_id=job_id, source=row["source"],
                attempt_no=row["attempt_count"] or None,
                from_state=row["state"], reason="explicit retry", occurred=now,
            )
            self.db.execute(
                "UPDATE jobs SET state='queued',queued_at=?,started=NULL,finished=NULL,"
                "lease_until=NULL,error=NULL,switch_reason=NULL,result_json=NULL,"
                "retry_count=retry_count+1 WHERE id=?",
                (now, job_id),
            )
            self._audit(
                "job.requeued", job_id=job_id, source=row["source"],
                attempt_no=row["attempt_count"] or None,
                from_state=row["state"], to_state="queued",
                reason="explicit retry", occurred=now,
            )
            self._refresh_health_cache_locked()
            self.completed.notify_all()
        return self.status(job_id)

    def renew_lease(self, job_id: str, lease_seconds: float | None = None) -> dict | None:
        """Renew an active attempt lease; useful for future external executors."""
        duration = self.lease_seconds if lease_seconds is None else float(lease_seconds)
        if duration <= 0:
            raise ValueError("lease_seconds must be positive")
        with self.lock, self.db:
            row = self.db.execute(
                "SELECT state,source,attempt_count,lease_until FROM jobs WHERE id=?",
                (job_id,),
            ).fetchone()
            if row is None:
                return None
            if row["state"] not in {"running", "cancel_requested"}:
                raise ValueError("only an active job lease may be renewed")
            now = self.clock()
            lease_until = now + duration
            self.db.execute(
                "UPDATE jobs SET lease_until=? WHERE id=?", (lease_until, job_id)
            )
            self.db.execute(
                "UPDATE job_attempts SET lease_until=? WHERE job_id=? AND attempt_no=?",
                (lease_until, job_id, row["attempt_count"]),
            )
            self._audit(
                "lease.renewed", job_id=job_id, source=row["source"],
                attempt_no=row["attempt_count"], from_state=row["state"],
                to_state=row["state"], occurred=now,
                metadata={"previous_lease_until": row["lease_until"],
                          "lease_until": lease_until},
            )
        return self.status(job_id)

    def attempts(self, job_id: str) -> list[dict[str, Any]] | None:
        with self.lock:
            exists = self.db.execute("SELECT 1 FROM jobs WHERE id=?", (job_id,)).fetchone()
            return attempt_history(self.db, job_id) if exists else None

    def audit_events(
        self, *, limit: int = 100, job_id: str | None = None,
        source: str | None = None, since: float | None = None,
    ) -> list[dict[str, Any]]:
        with self.lock:
            return audit_history(
                self.db, limit=limit, job_id=job_id, source=source, since=since
            )

    def analytics(
        self, policy: SourcePolicy | None = None,
        windows: tuple[int, ...] = (300, 1_800, 10_800, 86_400),
    ) -> dict[str, Any]:
        policy_snapshot = policy.snapshot() if policy is not None else None
        with self.lock:
            return analytics_snapshot(
                self.db, now=self.clock(), policy_snapshot=policy_snapshot,
                windows=windows,
            )

    def dashboard(self, policy: SourcePolicy | None = None) -> dict:
        """Payload-free live queue data for the local operational dashboard."""
        policy_snapshot = policy.snapshot() if policy is not None else None
        with self.lock:
            return dashboard_snapshot(
                self.db, now=self.clock(), policy_snapshot=policy_snapshot
            )

    def correlations(
        self, *, source: str | None = None, source_item_id: str | None = None,
        external_id: str | None = None, limit: int = 100,
    ) -> list[dict[str, Any]]:
        if source_item_id is None and external_id is None:
            raise ValueError("source_item_id or external_id is required")
        clauses = []
        values: list[Any] = []
        for column, value in (
            ("source", source),
            ("source_item_id", source_item_id),
            ("external_id", external_id),
        ):
            if value is not None:
                clauses.append(f"{column}=?")
                values.append(value)
        bounded_limit = max(1, min(int(limit), 1_000))
        with self.lock:
            rows = self.db.execute(
                "SELECT id AS job_id,source,profile,state,created,finished,"
                "source_item_id,external_id FROM jobs WHERE "
                + " AND ".join(clauses)
                + " ORDER BY created DESC,id DESC LIMIT ?",
                (*values, bounded_limit),
            )
            return [dict(row) for row in rows]

    def _candidates(self, allowed_sources: frozenset[str] | None = None):
        """Queued rows eligible now, ordered FIFO.

        Per-source concurrency and min-interval backpressure are applied here
        exactly as before; this method returns the full ordered candidate list
        so a weighted policy can pick a source fairly.
        """
        if allowed_sources is not None and not allowed_sources:
            return []
        query = "SELECT * FROM jobs WHERE state='queued'"
        values: tuple = ()
        if allowed_sources is not None:
            placeholders = ",".join("?" for _ in allowed_sources)
            query += f" AND source IN ({placeholders})"
            values = tuple(sorted(allowed_sources))
        query += " ORDER BY queued_at, id"
        now = self.clock()
        candidates = []
        for row in self.db.execute(query, values):
            profile = PROFILES[row["profile"]]
            running = self.db.execute(
                "SELECT count(*) FROM jobs WHERE source=? AND state IN ('running','cancel_requested')",
                (row["source"],),
            ).fetchone()[0]
            scheduled = self.db.execute(
                "SELECT next_allowed FROM source_schedules WHERE source=?", (row["source"],)
            ).fetchone()
            if running < profile.max_concurrency and (scheduled is None or scheduled[0] <= now):
                candidates.append(row)
        return candidates

    def _weighted_pick(self, candidates, policy: SourcePolicy):
        """Pick a candidate by weighted round-robin across sources.

        Weights are relative shares of dispatch opportunities per source; the
        first candidate of each source is ordered FIFO. A deterministic rotating accumulator keeps the
        schedule fair and stable across policy reloads.  When the policy has
        no entry for a source, the source keeps the default weight of 1.
        """
        by_source: dict[str, list] = {}
        for row in candidates:
            by_source.setdefault(row["source"], []).append(row)
        if not by_source:
            return None
        accumulator = getattr(self, "_weight_accumulator", None)
        if accumulator is None:
            accumulator = self._weight_accumulator = {}
        total_weight = 0.0
        for source in by_source:
            weight = policy.weight(source)
            if weight is None:
                weight = 1.0
            total_weight += weight
            accumulator[source] = accumulator.get(source, 0.0) + weight
        # Ignore accumulator entries for sources no longer eligible after a
        # hot policy reload or an emptied queue.
        chosen_source = max(by_source, key=lambda source: accumulator[source])
        accumulator[chosen_source] -= total_weight
        return by_source[chosen_source][0]

    def _next(self, allowed_sources: frozenset[str] | None = None, policy: SourcePolicy | None = None):
        # With a policy, sources share the GPU by their configured weights.
        # Without one, the enabled source set is served in global FIFO order.
        candidates = self._candidates(allowed_sources)
        if not candidates:
            self._last_scheduler_decision = None
            return None
        if policy is None:
            selected = candidates[0]
            mode = "fifo"
            reason = "oldest queued job"
            active_weights: dict[str, float] = {}
        else:
            selected = self._weighted_pick(candidates, policy)
            mode = "weighted_round_robin"
            reason = "weighted accumulator among currently eligible sources"
            policy_sources = policy.snapshot().get("sources", {})
            active_weights = {
                source: float(entry["weight"])
                for source, entry in sorted(policy_sources.items())
                if entry.get("enabled", True)
            }
        self._last_scheduler_decision = {
            "mode": mode,
            "reason": reason,
            "eligible_sources": sorted({row["source"] for row in candidates}),
            "active_weights": active_weights,
            "selected_source": selected["source"],
        }
        return selected

    def dispatch_once(self, allowed_sources: frozenset[str] | None = None, policy: SourcePolicy | None = None) -> bool:
        with self.lock, self.db:
            if self.db.execute("SELECT 1 FROM jobs WHERE state IN ('running','cancel_requested')").fetchone():
                return False
            row = self._next(allowed_sources, policy)
            if not row: return False
            now = self.clock()
            attempt_no = int(row["attempt_count"] or 0) + 1
            # A lease covers the server-owned remote deadline, not just a
            # scheduler tick.  That prevents a healthy VLM request from being
            # recovered while its bounded executor call is still active.
            lease_seconds = max(
                float(self.lease_seconds),
                float(PROFILES[row["profile"]].request_timeout_seconds) + 15.0,
            )
            lease_until = now + lease_seconds
            decision = dict(self._last_scheduler_decision or {})
            self.db.execute(
                "UPDATE jobs SET state='running',started=?,lease_until=?,"
                "attempt_count=? WHERE id=?",
                (now, lease_until, attempt_no, row["id"]),
            )
            self.db.execute(
                "INSERT INTO job_attempts(job_id,attempt_no,source,queued_at,"
                "selected_at,started,lease_until,scheduler_mode,selection_reason,"
                "policy_json) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (row["id"], attempt_no, row["source"], row["queued_at"], now, now,
                 lease_until, decision.get("mode", "unknown"),
                 decision.get("reason", "scheduler selected job"),
                 json.dumps(decision, sort_keys=True)),
            )
            self._audit(
                "scheduler.selected", job_id=row["id"], source=row["source"],
                attempt_no=attempt_no, from_state="queued", reason=decision.get("reason"),
                metadata=decision, occurred=now,
            )
            self._audit(
                "job.running", job_id=row["id"], source=row["source"],
                attempt_no=attempt_no, from_state="queued", to_state="running",
                occurred=now, metadata={"lease_until": lease_until},
            )
            interval = PROFILES[row["profile"]].min_interval_seconds
            if interval:
                self.db.execute(
                    "INSERT INTO source_schedules(source,next_allowed) VALUES(?,?) "
                    "ON CONFLICT(source) DO UPDATE SET next_allowed=excluded.next_allowed",
                    (row["source"], now + interval),
                )
            self._refresh_health_cache_locked()
        try:
            self._execute(row)
        except Exception as exc:
            with self.lock, self.db:
                finished = self.clock()
                self.db.execute("UPDATE jobs SET state='failed',finished=?,lease_until=NULL,error=? WHERE id=?", (finished, str(exc), row["id"]))
                self.db.execute(
                    "UPDATE job_attempts SET finished=?,outcome='failed',error=? "
                    "WHERE job_id=? AND attempt_no=?",
                    (finished, str(exc), row["id"], attempt_no),
                )
                self._audit(
                    "job.failed", job_id=row["id"], source=row["source"],
                    attempt_no=attempt_no, from_state="running", to_state="failed",
                    reason=str(exc), occurred=finished,
                )
                self._refresh_health_cache_locked()
                self.completed.notify_all()
        return True

    def _execute(self, row):
        profile = PROFILES[row["profile"]]
        self.wol.wake()
        loaded = [m.get("name") for m in self.ollama.ps().get("models", [])]
        others = [m for m in loaded if m != profile.model]
        if others:
            for model in others: self.ollama.unload(model)
            reason = "unloaded incompatible model before switch"
        else:
            reason = "target already resident" if profile.model in loaded else "target model requested"
        # A no-op generation is Ollama's explicit model-load/readiness contract.
        if not self.ollama.is_ready(profile.model):
            self.ollama.run("generate", {"model": profile.model, "prompt": "", "keep_alive": f"{profile.keep_alive_seconds}s"})
        wait_ready = getattr(self.ollama, "wait_ready", None)
        ready = (
            wait_ready(profile.model, timeout_seconds=30)
            if wait_ready is not None
            else self.ollama.is_ready(profile.model)
        )
        if not ready:
            raise RuntimeError("target model did not become ready")
        payload = json.loads(row["payload"])
        options = dict(payload.get("options", {}))
        options["num_ctx"] = min(int(options.get("num_ctx", profile.max_context)), profile.max_context)
        options["num_predict"] = min(int(options.get("num_predict", profile.max_output)), profile.max_output)
        request = {k: v for k, v in payload.items() if k not in {"model", "keep_alive"}}
        request.update({"model": profile.model, "stream": False, "options": options,
                        "keep_alive": f"{profile.keep_alive_seconds}s"})
        request["_broker_timeout_seconds"] = profile.request_timeout_seconds
        result = self.ollama.run(row["kind"], request)
        if not isinstance(result, dict):
            raise RuntimeError("Ollama returned a non-object response")
        result_json = json.dumps(result)
        with self.lock, self.db:
            state = self.db.execute("SELECT state FROM jobs WHERE id=?", (row["id"],)).fetchone()[0]
            final = "cancelled" if state == "cancel_requested" else "completed"
            finished = self.clock()
            self.db.execute("UPDATE jobs SET state=?,finished=?,lease_until=NULL,switch_reason=?,result_json=? WHERE id=?",
                            (final, finished, reason, result_json, row["id"]))
            attempt_no = self.db.execute(
                "SELECT attempt_count FROM jobs WHERE id=?", (row["id"],)
            ).fetchone()[0]
            self.db.execute(
                "UPDATE job_attempts SET finished=?,outcome=? "
                "WHERE job_id=? AND attempt_no=?",
                (finished, final, row["id"], attempt_no),
            )
            self._audit(
                f"job.{final}", job_id=row["id"], source=row["source"],
                attempt_no=attempt_no, from_state=state, to_state=final,
                reason=reason, occurred=finished,
            )
            self._refresh_health_cache_locked()
            self.completed.notify_all()

    def wait_for_terminal(self, job_id: str, timeout_seconds: float) -> dict | None:
        """Wait for a persisted terminal state; executor dispatch remains external."""
        deadline = time.monotonic() + timeout_seconds
        with self.completed:
            while True:
                job = self.status(job_id)
                if job is None or job["state"] in {"completed", "failed", "cancelled"}:
                    return job
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return job
                self.completed.wait(remaining)

    def metrics(self) -> dict:
        with self.lock:
            queued = self.db.execute("SELECT count(*) FROM jobs WHERE state='queued'").fetchone()[0]
            active = self.db.execute("SELECT * FROM jobs WHERE state IN ('running','cancel_requested')").fetchone()
        ps = self.ollama.ps()
        return {"resource": "mainpc-gpu", "queue_depth": queued, "active": self._job(active) if active else None,
                "loaded_models": ps.get("models", []), "timestamp": self.clock()}

    def health(self) -> dict:
        """Return a local broker probe without waking or querying MAIN-PC."""
        # Do not touch SQLite or wait for the dispatcher here.  Assignment of
        # the small immutable replacement mapping is atomic under CPython, so
        # this local probe remains available while a large DB transaction runs.
        return dict(self._health_cache)


class Dispatcher(threading.Thread):
    def __init__(
        self, broker: Broker, interval=0.25,
        allowed_sources: frozenset[str] | None = None,
        policy: SourcePolicy | None = None,
    ):
        super().__init__(daemon=True)
        self.broker, self.interval, self.allowed_sources, self.policy, self.stop_event = (
            broker,
            interval,
            allowed_sources,
            policy,
            threading.Event(),
        )
    def run(self):
        while not self.stop_event.is_set():
            # With a runtime policy, the effective allowlist and weights come
            # from the reloaded policy file; without one, the env allowlist is
            # used with global FIFO ordering.
            policy = self.policy
            allowed = self.allowed_sources
            if policy is not None:
                enabled = policy.enabled_sources()
                allowed = enabled if enabled else frozenset()
            self.broker.recover()
            self.broker.dispatch_once(allowed, policy)
            self.stop_event.wait(self.interval)
