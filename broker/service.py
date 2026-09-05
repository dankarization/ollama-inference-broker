from __future__ import annotations

from copy import deepcopy
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
from .compat import (CompatibilityError, UNCENSORED_EVAL_PROFILES,
                     validate_olya_decision_payload,
                     validate_olya_vision_payload,
                     validate_shutterstock_canary_payload,
                     validate_shutterstock_video_payload,
                     validate_syncopia_memory_payload,
                     validate_uncensored_eval_payload)
from .profiles import PROFILES
from .policy import SourcePolicyError, normalize_source_policy, source_policy_write_lock
from .rollback_guard import LEGACY_HIDDEN_STORAGE_FIELDS
from .storage import (
    DEFAULT_CHECKPOINT_INTERVAL_SECONDS,
    DEFAULT_JOURNAL_SIZE_LIMIT_BYTES,
    DEFAULT_WAL_AUTOCHECKPOINT_PAGES,
    DEFAULT_WAL_BUDGET_BYTES,
    PUBLIC_JOB_PREDICATE,
    PRODUCER_STORAGE_JOB_PREDICATE,
    StorageManager,
    migrate_storage_schema,
)


LOGGER = logging.getLogger("ollama_inference_broker.audit")
OBSERVER_READ_DEADLINE_SECONDS = 0.75
SCHEDULING_HORIZON_SECONDS = 60 * 60
FORECAST_DEFAULT_EXECUTION_SECONDS = 300.0


class SourceAdmissionBlocked(Exception):
    """A source policy rejected admission before a job was persisted."""

    code = "source_admission_blocked"

    def __init__(self, source: str) -> None:
        self.source = source
        super().__init__(f"new admissions are blocked for source {source!r}")


class StorageAuthorizationRequired(PermissionError):
    """A bulk mutation selected at least one producer-storage job."""


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

    def admission_allowed(self, source: str) -> bool:
        """Default old policy entries and unknown sources to admission allowed."""
        with self._lock:
            self._load_locked()
            entry = self._sources.get(source)
            return True if entry is None else entry["admission_allowed"]

    def storage_config(self, source: str) -> dict[str, Any]:
        """Return fail-closed producer-storage flags for one source."""
        with self._lock:
            self._load_locked()
            entry = self._sources.get(source)
            if entry is None:
                return {
                    "producer_storage_enabled": False,
                    "producer_storage_mode": "broker_temporary",
                    "ack_required": False,
                    "compaction_enabled": False,
                    "legacy_result_fallback": True,
                    "compaction_grace_seconds": 86400.0,
                    "quarantine_grace_seconds": 86400.0,
                }
            return {
                key: entry[key]
                for key in (
                    "producer_storage_enabled", "producer_storage_mode", "ack_required",
                    "compaction_enabled", "legacy_result_fallback",
                    "compaction_grace_seconds", "quarantine_grace_seconds",
                )
            }

    def _mutate_source(self, source: str, field: str, value: Any) -> Any:
        """Serialize and durably atomically replace one source policy field."""
        with self._lock:
            try:
                with source_policy_write_lock(self.path):
                    raw = json.loads(self.path.read_text(encoding="utf-8"))
                    normalized = normalize_source_policy(raw)
                    if source not in normalized:
                        raise SourcePolicyError(f"source {source!r} is not configured")
                    raw["sources"][source][field] = value
                    temporary: str | None = None
                    try:
                        mode = self.path.stat().st_mode & 0o7777
                        descriptor, temporary = tempfile.mkstemp(
                            prefix=f".{self.path.name}.", suffix=".tmp", dir=self.path.parent,
                        )
                        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                            json.dump(raw, handle, indent=2, sort_keys=True)
                            handle.write("\n")
                            handle.flush()
                            os.fsync(handle.fileno())
                        os.chmod(temporary, mode)
                        os.replace(temporary, self.path)
                        directory = os.open(self.path.parent, os.O_RDONLY)
                        try:
                            os.fsync(directory)
                        finally:
                            os.close(directory)
                    finally:
                        if temporary is not None and os.path.exists(temporary):
                            try:
                                os.unlink(temporary)
                            except OSError:
                                pass
            except SourcePolicyError:
                raise
            except (OSError, ValueError) as error:
                raise SourcePolicyError("policy is unavailable or invalid") from error
            self._signature = None
            self._load_locked()
            return value

    def set_weight(self, source: str, weight: Any) -> int:
        """Atomically update one configured source with a dashboard-safe weight."""
        if isinstance(weight, bool) or not isinstance(weight, int) or not 1 <= weight <= 10:
            raise SourcePolicyError("weight must be an integer from 1 through 10")
        return self._mutate_source(source, "weight", weight)

    def set_enabled(self, source: str, enabled: Any) -> bool:
        if not isinstance(enabled, bool):
            raise SourcePolicyError("enabled must be a boolean")
        return self._mutate_source(source, "enabled", enabled)

    def set_admission_allowed(self, source: str, allowed: Any) -> bool:
        if not isinstance(allowed, bool):
            raise SourcePolicyError("allowed must be a boolean")
        return self._mutate_source(source, "admission_allowed", allowed)


class Broker:
    """One-resource scheduler with FIFO source heads and time-weighted batches."""
    def __init__(self, database: str | Path, ollama, wol, clock=time.time,
                 lease_seconds=60, *,
                 wal_autocheckpoint_pages=DEFAULT_WAL_AUTOCHECKPOINT_PAGES,
                 journal_size_limit_bytes=DEFAULT_JOURNAL_SIZE_LIMIT_BYTES,
                 wal_budget_bytes=DEFAULT_WAL_BUDGET_BYTES,
                 checkpoint_interval_seconds=DEFAULT_CHECKPOINT_INTERVAL_SECONDS):
        self.ollama, self.wol, self.clock, self.lease_seconds = ollama, wol, clock, lease_seconds
        self.database = str(database)
        self.db = sqlite3.connect(self.database, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.lock = threading.RLock()
        self.completed = threading.Condition(self.lock)
        self._health_cache = {
            "status": "starting", "resource": "mainpc-gpu", "queue_depth": 0,
            "active_job_id": None, "timestamp": self.clock(),
        }
        self._loaded_models_cache: list[dict[str, Any]] = []
        self._dashboard_cache: dict[str, Any] | None = None
        self._metrics_cache: dict[str, Any] = {
            "resource": "mainpc-gpu", "queue_depth": 0, "active": None,
        }
        # Kept as an inert compatibility attribute for observer users of older
        # releases.  Policy scheduling is now time-budgeted rather than
        # dispatch-count weighted round-robin.
        self._weight_accumulator: dict[str, float] = {}
        # A cycle is deliberately process-local.  Queue durability and lease
        # recovery stay in SQLite; a restart simply begins a fresh fair
        # 60-minute allocation from the durable ready queue.
        self._time_cycle: dict[str, Any] | None = None
        self._duration_estimates: dict[tuple[str, str], float] = {}
        self._active_time_batch: dict[str, Any] | None = None
        # Immutable copy written under the lock for the lock-free forecast.
        self._scheduler_snapshot: dict[str, Any] | None = None
        self.source_policy: SourcePolicy | None = None
        self.wal_autocheckpoint_pages = int(wal_autocheckpoint_pages)
        self.journal_size_limit_bytes = int(journal_size_limit_bytes)
        self._init_db()
        self.storage = StorageManager(
            self.db, self.database, self.lock, self.clock, self._audit,
            lambda: self.source_policy,
            wal_autocheckpoint_pages=self.wal_autocheckpoint_pages,
            journal_size_limit_bytes=self.journal_size_limit_bytes,
            wal_budget_bytes=int(wal_budget_bytes),
            checkpoint_interval_seconds=float(checkpoint_interval_seconds),
        )
        self.recover()
        self.storage.reconcile()
        with self.lock:
            self._refresh_health_cache_locked()

    def use_source_policy(self, policy: SourcePolicy | None) -> None:
        """Install the shared hot-reload policy used by HTTP admissions."""
        self.source_policy = policy

    def audit_source_control(
        self, event_type: str, source: str, metadata: dict[str, Any]
    ) -> None:
        """Persist payload-free evidence of a successful source policy change."""
        with self.lock, self.db:
            self._audit(
                event_type, source=source, reason="runtime source policy changed",
                metadata=metadata,
            )

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
                source TEXT NOT NULL,
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
            self._retire_priority_column()
            migrate_storage_schema(
                self.db,
                wal_autocheckpoint_pages=self.wal_autocheckpoint_pages,
                journal_size_limit_bytes=self.journal_size_limit_bytes,
            )
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
                "CREATE INDEX IF NOT EXISTS jobs_state_started "
                "ON jobs(state,started,created,id)"
            )
            # Dashboard source discovery and retry totals must stay index-only
            # on payload-heavy production queues.
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS jobs_state_source ON jobs(state,source)"
            )
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS jobs_source_state_retry "
                "ON jobs(source,state,retry_count)"
            )
            # Candidate selection/forecast must not visit queued payload rows.
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS jobs_queued_candidates "
                "ON jobs(state,source,queued_at,id,profile,created,attempt_count)"
            )
            # History is globally newest-first across all terminal states.  Keep
            # its ordering keys first so SQLite can stop at LIMIT without a
            # temporary sort; the remaining projected columns make it covering.
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS jobs_terminal_history_v3 "
                "ON jobs(finished DESC,id DESC,state,source,profile,created,started,attempt_count,retry_count)"
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

    def _retire_priority_column(self) -> None:
        """Remove a legacy priority column only when new inserts need it gone.

        Current deployed schemas have a default and can retain an inert historic
        column without runtime reads or writes.  Very old NOT NULL/no-default
        schemas are rebuilt once, transactionally, so new jobs have no hidden
        priority value while all durable rows are preserved.
        """
        columns = list(self.db.execute("PRAGMA table_info(jobs)"))
        priority = next((row for row in columns if row[1] == "priority"), None)
        if priority is None or priority[4] is not None or not priority[3]:
            return
        for index in list(self.db.execute("PRAGMA index_list(jobs)")):
            name = index[1]
            quoted_name = '"' + name.replace('"', '""') + '"'
            if any(column[2] == "priority" for column in self.db.execute(f"PRAGMA index_info({quoted_name})")):
                self.db.execute(f"DROP INDEX {quoted_name}")
        try:
            self.db.execute("ALTER TABLE jobs DROP COLUMN priority")
            return
        except sqlite3.OperationalError:
            pass
        names = [row[1] for row in columns if row[1] != "priority"]
        self.db.execute("ALTER TABLE jobs RENAME TO jobs_with_legacy_priority")
        schema = """CREATE TABLE jobs (
            id TEXT PRIMARY KEY, profile TEXT NOT NULL, kind TEXT NOT NULL,
            source TEXT NOT NULL, payload TEXT NOT NULL, state TEXT NOT NULL,
            created REAL NOT NULL, started REAL, finished REAL, lease_until REAL,
            error TEXT, switch_reason TEXT, result_json TEXT, source_item_id TEXT,
            external_id TEXT, queued_at REAL, attempt_count INTEGER NOT NULL DEFAULT 0,
            retry_count INTEGER NOT NULL DEFAULT 0, requeue_count INTEGER NOT NULL DEFAULT 0)"""
        self.db.execute(schema)
        joined = ",".join(names)
        self.db.execute(f"INSERT INTO jobs ({joined}) SELECT {joined} FROM jobs_with_legacy_priority")
        self.db.execute("DROP TABLE jobs_with_legacy_priority")

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
        producer_storage = int(
            job_id is not None
            and self.db.execute(
                f"SELECT 1 FROM jobs WHERE id=? AND {PRODUCER_STORAGE_JOB_PREDICATE}",
                (job_id,),
            ).fetchone() is not None
        )
        self.db.execute(
            "INSERT INTO audit_events(occurred,event_type,job_id,source,attempt_no,"
            "from_state,to_state,reason,metadata_json,producer_storage) "
            "VALUES(?,?,?,?,?,?,?,?,?,?)",
            (timestamp, event_type, job_id, source, attempt_no, from_state,
             to_state, reason, json.dumps(safe_metadata, sort_keys=True),
             producer_storage),
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
               external_id: str | None = None,
               producer_storage: dict[str, Any] | None = None) -> dict:
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
        if profile in UNCENSORED_EVAL_PROFILES:
            if source != "uncensored-eval":
                raise ValueError("uncensored evaluation profiles must use source uncensored-eval")
            try:
                payload = validate_uncensored_eval_payload(payload)
            except CompatibilityError as exc:
                raise ValueError(str(exc)) from exc
        source_item_id = self._correlation_value("source_item_id", source_item_id)
        external_id = self._correlation_value("external_id", external_id)
        job_id, now = str(uuid.uuid4()), self.clock()
        with self.lock, self.db:
            existing = None
            existing_has_producer_storage = False
            if external_id is not None:
                existing = self.db.execute(
                    "SELECT * FROM jobs WHERE source=? AND external_id=? "
                    "ORDER BY created DESC,id DESC LIMIT 1",
                    (source, external_id),
                ).fetchone()
                if existing is not None:
                    existing_has_producer_storage = (
                        existing["producer_attempt_id"] is not None
                        or existing["input_storage_mode"] != "broker_temporary"
                        or existing["result_storage_mode"] != "broker_temporary"
                    )
                    if (
                        source in {"olya-vision", "olya-decision", "syncopia-telegram-memory"}
                        or producer_storage is not None
                        or existing_has_producer_storage
                    ):
                        if (producer_storage is not None) != existing_has_producer_storage:
                            raise ValueError(
                                "producer storage idempotency conflict for existing correlation"
                            )
                        if producer_storage is not None:
                            storage_fields = self.storage.prepare_admission(
                                source, payload, producer_storage, persisted=existing,
                            )
                            checks = {
                                "profile": profile,
                                "kind": kind,
                                "producer_attempt_id": storage_fields["producer_attempt_id"],
                                "input_ref": storage_fields["input_ref"],
                                "input_hash": storage_fields["input_hash"],
                                "input_bytes": storage_fields["input_bytes"],
                                "input_storage_mode": storage_fields["input_storage_mode"],
                                "result_storage_mode": storage_fields["result_storage_mode"],
                                "artifact_schema_version": storage_fields["artifact_schema_version"],
                            }
                            if any(existing[key] != value for key, value in checks.items()):
                                raise ValueError(
                                    "producer storage idempotency conflict for existing correlation"
                                )
                        return self.status(existing["id"])
            storage_fields = self.storage.prepare_admission(source, payload, producer_storage)
            if self.source_policy is not None and not self.source_policy.admission_allowed(source):
                self._audit(
                    "admission.rejected", source=source,
                    reason="source admission policy blocked new jobs", occurred=now,
                    metadata={
                        "code": SourceAdmissionBlocked.code,
                        "profile": profile,
                        "kind": kind,
                    },
                )
                # Rejection itself is durable observability, despite aborting admission.
                self.db.commit()
                raise SourceAdmissionBlocked(source)
            self.db.execute(
                "INSERT INTO jobs(id,profile,kind,source,payload,state,created,"
                "queued_at,source_item_id,external_id,input_storage_mode,input_ref,input_hash,"
                "input_bytes,result_storage_mode,artifact_schema_version,producer_attempt_id,"
                "ack_required,legacy_result_fallback) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (job_id, profile, kind, source, json.dumps(payload), "queued", now,
                 now, source_item_id, external_id, storage_fields["input_storage_mode"],
                 storage_fields["input_ref"], storage_fields["input_hash"],
                 storage_fields["input_bytes"], storage_fields["result_storage_mode"],
                 storage_fields["artifact_schema_version"], storage_fields["producer_attempt_id"],
                 storage_fields["ack_required"], storage_fields["legacy_result_fallback"]),
            )
            self.storage.record_admission(job_id, source, storage_fields, now)
            self._audit(
                "admission.accepted", job_id=job_id, source=source,
                from_state=None, to_state="queued", occurred=now,
                metadata={
                    "profile": profile,
                    "kind": kind,
                    "has_source_item_id": source_item_id is not None,
                    "has_external_id": external_id is not None,
                    "producer_storage": producer_storage is not None,
                    "result_storage_mode": storage_fields["result_storage_mode"],
                },
            )
            self._refresh_health_cache_locked()
        return self.status(job_id)

    @staticmethod
    def _bulk_source(source: str) -> str:
        if not isinstance(source, str) or not source.strip() or len(source) > 256:
            raise ValueError("source must be a non-empty string up to 256 characters")
        return source.strip()

    def bulk_cancel_queued(
        self, source: str, *, allow_producer_storage: bool = False,
    ) -> dict[str, Any]:
        """Cancel only queued jobs for one exact source in one transaction."""
        source = self._bulk_source(source)
        now = self.clock()
        with self.lock, self.db:
            rows = list(self.db.execute(
                "SELECT id,attempt_count," + PRODUCER_STORAGE_JOB_PREDICATE +
                " AS producer_storage FROM jobs WHERE source=? AND state='queued' "
                "ORDER BY queued_at,id", (source,),
            ))
            if not allow_producer_storage and any(
                row["producer_storage"] for row in rows
            ):
                raise StorageAuthorizationRequired
            for row in rows:
                self.db.execute(
                    "UPDATE jobs SET state='cancelled',finished=? "
                    "WHERE id=? AND state='queued'", (now, row["id"]),
                )
                self._audit(
                    "job.cancelled", job_id=row["id"], source=source,
                    attempt_no=row["attempt_count"] or None,
                    from_state="queued", to_state="cancelled",
                    reason="bulk source cancellation", occurred=now,
                )
            count = len(rows)
            self._audit(
                "source.bulk_cancel", source=source,
                reason="bulk queued cancellation", occurred=now,
                metadata={"cancelled": count},
            )
            self._refresh_health_cache_locked()
            self.completed.notify_all()
        return {"source": source, "cancelled": count}

    def bulk_retry_failed(
        self, source: str, *, allow_producer_storage: bool = False,
    ) -> dict[str, Any]:
        """Requeue only failed jobs while retaining attempts and audit history."""
        source = self._bulk_source(source)
        now = self.clock()
        with self.lock, self.db:
            rows = list(self.db.execute(
                "SELECT id,attempt_count," + PRODUCER_STORAGE_JOB_PREDICATE +
                " AS producer_storage FROM jobs WHERE source=? AND state='failed' "
                "ORDER BY finished,id", (source,),
            ))
            if not allow_producer_storage and any(
                row["producer_storage"] for row in rows
            ):
                raise StorageAuthorizationRequired
            for row in rows:
                attempt_no = row["attempt_count"] or None
                self._audit(
                    "job.retry_requested", job_id=row["id"], source=source,
                    attempt_no=attempt_no, from_state="failed",
                    reason="bulk source retry", occurred=now,
                )
                self.db.execute(
                    "UPDATE jobs SET state='queued',queued_at=?,started=NULL,finished=NULL,"
                    "lease_until=NULL,error=NULL,switch_reason=NULL,result_json=NULL,"
                    "result_ref=NULL,result_hash=NULL,result_bytes=NULL,delivery_state='pending',"
                    "delivery_attempt_count=0,last_delivery_at=NULL,acked_at=NULL,"
                    "compaction_after=NULL,compaction_state='full',quarantined_at=NULL,"
                    "compacted_at=NULL,retry_count=retry_count+1 "
                    "WHERE id=? AND state='failed'",
                    (now, row["id"]),
                )
                self.db.execute(
                    "DELETE FROM job_artifacts WHERE job_id=? AND role='result'",
                    (row["id"],),
                )
                self._audit(
                    "job.requeued", job_id=row["id"], source=source,
                    attempt_no=attempt_no, from_state="failed", to_state="queued",
                    reason="bulk source retry", occurred=now,
                )
            count = len(rows)
            self._audit(
                "source.bulk_retry", source=source,
                reason="bulk failed retry", occurred=now,
                metadata={"retried": count},
            )
            self._refresh_health_cache_locked()
            self.completed.notify_all()
        return {"source": source, "retried": count}

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

    def compact_status(self, job_id: str) -> dict | None:
        return self.storage.compact_status(job_id)

    def receipt(self, job_id: str) -> dict | None:
        return self.storage.receipt(job_id)

    def acknowledge_result(self, job_id: str, body: dict[str, Any]) -> dict | None:
        return self.storage.acknowledge_result(job_id, body)

    def acknowledge_input(self, job_id: str, body: dict[str, Any]) -> dict | None:
        return self.storage.acknowledge_input(job_id, body)

    def storage_maintenance(
        self, source: str, *, operation: str = "preview", limit: int = 100,
        confirm: bool = False, max_bytes: int = 16 * 1024 * 1024,
    ) -> dict[str, Any]:
        return self.storage.maintenance(
            source, operation=operation, limit=limit, confirm=confirm,
            max_bytes=max_bytes,
        )

    def storage_health(self) -> dict[str, Any]:
        return self.storage.health()

    def producer_storage_job(self, job_id: str) -> bool:
        with self.lock:
            return self.db.execute(
                f"SELECT 1 FROM jobs WHERE id=? AND {PRODUCER_STORAGE_JOB_PREDICATE}",
                (job_id,),
            ).fetchone() is not None

    def _job(self, row):
        data = dict(row)
        data.pop("priority", None)  # inert historic column is never public
        for field in LEGACY_HIDDEN_STORAGE_FIELDS:
            data.pop(field, None)
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
                "result_ref=NULL,result_hash=NULL,result_bytes=NULL,delivery_state='pending',"
                "delivery_attempt_count=0,last_delivery_at=NULL,acked_at=NULL,"
                "compaction_after=NULL,compaction_state='full',quarantined_at=NULL,"
                "compacted_at=NULL,retry_count=retry_count+1 WHERE id=?",
                (now, job_id),
            )
            self.db.execute(
                "DELETE FROM job_artifacts WHERE job_id=? AND role='result'", (job_id,)
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
        include_producer_storage: bool = True,
    ) -> list[dict[str, Any]]:
        with self.lock:
            return audit_history(
                self.db, limit=limit, job_id=job_id, source=source, since=since,
                include_producer_storage=include_producer_storage,
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

    @staticmethod
    def _observer_error(error: sqlite3.Error) -> dict[str, str]:
        """Return safe, actionable SQLite observer diagnostics."""
        sqlite_error = getattr(error, "sqlite_errorname", None)
        if not sqlite_error:
            message = str(error).lower()
            if "interrupted" in message:
                sqlite_error = "SQLITE_INTERRUPT"
            elif "locked" in message or "busy" in message:
                sqlite_error = "SQLITE_BUSY"
            elif "no such table" in message or "no such index" in message:
                sqlite_error = "SQLITE_SCHEMA"
            else:
                sqlite_error = "SQLITE_ERROR"
        LOGGER.warning(json.dumps({
            "event": "observer.read_failed", "sqlite_error": sqlite_error,
        }, sort_keys=True))
        return {
            "reason": "database observer read unavailable",
            "sqlite_error": sqlite_error,
        }

    def _observer_read(self, reader):
        """Run a bounded read without waiting for the dispatcher connection."""
        connection = None
        deadline = time.monotonic() + OBSERVER_READ_DEADLINE_SECONDS
        try:
            connection = sqlite3.connect(
                self.database, timeout=OBSERVER_READ_DEADLINE_SECONDS,
            )
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA query_only=ON")
            connection.execute(
                f"PRAGMA busy_timeout={int(OBSERVER_READ_DEADLINE_SECONDS * 1_000)}"
            )
            connection.set_progress_handler(
                lambda: int(time.monotonic() >= deadline), 1_000,
            )
            return reader(connection), None
        except sqlite3.Error as error:
            return None, self._observer_error(error)
        finally:
            if connection is not None:
                connection.close()

    @staticmethod
    def _observation(
        state: str, now: float, error: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        observation: dict[str, Any] = {"state": state, "observed_at": now}
        if error is not None:
            observation.update(error)
        return observation

    def forecast(self, policy: SourcePolicy | None = None, limit: int = 10) -> dict:
        """Read-only projection of upcoming dispatcher selections.

        The projection never mutates the queue, leases, accumulators or batch
        state: it simulates the same ``_select_candidate`` routine on private
        copies and is explicitly contingent on later admissions and state
        changes.  It uses the same model/source time-batch selector as real
        dispatch and advances only a private cycle copy with observed per-lane
        duration estimates.  A locked database yields ``unavailable`` instead
        of fake emptiness.  Bounded by ``limit`` (1..20) next selections.
        """
        now = self.clock()
        data, error = self._observer_read(
            lambda db: self._forecast(db, policy, now, limit)
        )
        if data is None:
            return {
                "contingent": True,
                "unavailable": True,
                "reason": (error or {}).get("reason", "database observer read unavailable"),
            }
        return data

    def _forecast(self, db, policy, now: float, limit: int) -> dict[str, Any]:
        """Projection body; reads only, never writes."""
        bounded = max(1, min(int(limit), 20))
        allowed = None
        if policy is not None:
            allowed = policy.enabled_sources()
        running = db.execute(
            "SELECT profile FROM jobs WHERE state IN ('running','cancel_requested') LIMIT 1"
        ).fetchone()
        # The next dispatch can occur only after the one active job completes.
        # Project that slot release; keep all other hard eligibility checks.
        candidates = self._candidates(
            allowed, db=db, release_running=running is not None,
            limit_per_source=bounded,
        )
        snapshot = self._scheduler_snapshot or {}
        cycle = deepcopy(snapshot.get("time_cycle"))
        estimates = dict(snapshot.get("duration_estimates") or {})
        active = snapshot.get("active_time_batch")
        # The active lease will release before any projected selection.  Its
        # true duration is unknown until completion, so forecast uses the same
        # lane estimate it uses for all subsequent non-preemptive boundaries.
        if policy is not None and running is not None and active is not None and cycle:
            self._record_time_usage(
                cycle, tuple(active["lane"]),
                self._forecast_duration(tuple(active["lane"]), estimates),
            )
        remaining = [dict(row) for row in candidates]
        selections: list[dict[str, Any]] = []
        for _ in range(bounded):
            if not remaining:
                break
            if policy is None:
                picked = remaining[0]
                mode, reason = "fifo", "oldest queued job"
                eligible = [picked["source"]]
                decision: dict[str, Any] = {}
            else:
                picked, cycle, decision = self._time_batch_pick(
                    remaining, policy, now, cycle,
                )
                if picked is None:
                    break
                mode, reason = decision["mode"], decision["reason"]
                eligible = decision["eligible_sources"]
            queued_at = picked["queued_at"] if picked["queued_at"] is not None else picked["created"]
            wait = max(0.0, now - queued_at)
            lane = self._lane_key(picked)
            selections.append({
                "job_id": picked["id"],
                "source": picked["source"],
                "profile": picked["profile"],
                "model": PROFILES[picked["profile"]].model,
                "weight": (policy.weight(picked["source"]) or 1.0) if policy else 1.0,
                "mode": mode,
                "reason": reason,
                "eligible_sources": eligible,
                "wait_seconds": round(wait, 3),
            })
            remaining = [row for row in remaining if row["id"] != picked["id"]]
            if policy is not None:
                self._record_time_usage(
                    cycle, lane, self._forecast_duration(lane, estimates),
                )
        return {
            "contingent": True,
            "contingency": (
                "read-only time-batch projection; non-preemptive boundaries use recent "
                "execution estimates and change as jobs are admitted, complete or policy reloads"
            ),
            "current_model": PROFILES[running["profile"]].model if running else None,
            "next_selections": selections,
        }

    def dashboard(self, policy: SourcePolicy | None = None) -> dict:
        """Payload-free live queue data for the local operational dashboard."""
        policy_snapshot = policy.snapshot() if policy is not None else None
        now = self.clock()
        snapshot, error = self._observer_read(
            lambda db: {
                **dashboard_snapshot(db, now=now, policy_snapshot=policy_snapshot),
                "forecast": self._forecast(db, policy, now, 10),
                "history": self._terminal_history(db, limit=10),
            }
        )
        if snapshot is not None:
            snapshot["observation"] = self._observation("live", now)
            self._dashboard_cache = snapshot
            return snapshot
        if self._dashboard_cache is not None:
            stale = deepcopy(self._dashboard_cache)
            stale["observation"] = self._observation("stale", now, error)
            return stale
        # An observer timeout or lock is not evidence that the queue is empty.
        return {"observation": self._observation("unavailable", now, error)}

    def _terminal_history(
        self, db, *, limit: int, cursor: tuple[float, str] | None = None,
        include_producer_storage: bool = True,
    ):
        values: list[Any] = []
        clause = ""
        if cursor is not None:
            # A row-value range follows the index ordering without turning the
            # keyset predicate into a multi-index OR that needs a temp sort.
            clause = " AND (finished,id) < (?,?)"
            values.extend(cursor)
        storage_clause = "" if include_producer_storage else " AND " + PUBLIC_JOB_PREDICATE
        history_index = (
            "jobs_terminal_history_v3" if include_producer_storage
            else "jobs_public_terminal_history"
        )
        rows = db.execute(
            "SELECT id,source,profile,state,created,started,finished,attempt_count,retry_count "
            f"FROM jobs INDEXED BY {history_index} "
            "WHERE state IN ('completed','failed','cancelled') AND finished IS NOT NULL"
            + storage_clause + clause +
            " ORDER BY finished DESC,id DESC LIMIT ?",
            (*values, max(1, min(limit, 30))),
        )
        return [dict(row) for row in rows]

    def terminal_history(
        self, *, limit: int = 30, cursor: tuple[float, str] | None = None,
        include_producer_storage: bool = True,
    ):
        data, error = self._observer_read(lambda db: self._terminal_history(
            db, limit=limit, cursor=cursor,
            include_producer_storage=include_producer_storage,
        ))
        if data is None:
            return {"unavailable": True, "reason": (error or {}).get("reason")}
        next_cursor = None if not data else [data[-1]["finished"], data[-1]["id"]]
        return {"items": data, "next_cursor": next_cursor}

    def correlations(
        self, *, source: str | None = None, source_item_id: str | None = None,
        external_id: str | None = None, limit: int = 100,
        include_producer_storage: bool = True,
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
        if not include_producer_storage:
            clauses.append(f"NOT {PRODUCER_STORAGE_JOB_PREDICATE}")
        with self.lock:
            rows = self.db.execute(
                "SELECT id AS job_id,source,profile,state,created,finished,"
                "source_item_id,external_id FROM jobs WHERE "
                + " AND ".join(clauses)
                + " ORDER BY created DESC,id DESC LIMIT ?",
                (*values, bounded_limit),
            )
            return [dict(row) for row in rows]

    def _candidates(self, allowed_sources: frozenset[str] | None = None, db=None,
                    release_running: bool = False, limit_per_source: int | None = None):
        """Queued rows eligible now, ordered FIFO.

        Per-source concurrency and min-interval backpressure are applied here
        exactly as before; this method returns the full ordered candidate list
        so a weighted policy can pick a source fairly.  ``db`` may be an
        observer connection for read-only projections.
        """
        connection = db if db is not None else self.db
        if allowed_sources is not None and not allowed_sources:
            return []
        # Scheduler selection never needs payload/result_json.  Keeping this
        # projection narrow for both observers and the real dispatcher avoids
        # materializing every queued request while sorting a large backlog.
        query = (
            "SELECT id,profile,source,created,queued_at,attempt_count "
            "FROM jobs INDEXED BY jobs_queued_candidates WHERE state='queued'"
        )
        values: tuple = ()
        if allowed_sources is not None:
            placeholders = ",".join("?" for _ in allowed_sources)
            query += f" AND source IN ({placeholders})"
            values = tuple(sorted(allowed_sources))
        if limit_per_source is not None:
            query = (
                "SELECT id,profile,source,created,queued_at,attempt_count FROM ("
                "SELECT id,profile,source,created,queued_at,attempt_count,"
                "row_number() OVER (PARTITION BY source ORDER BY queued_at,id) AS source_rank "
                "FROM (" + query + ")"
                ") WHERE source_rank<=? ORDER BY queued_at,id"
            )
            values += (limit_per_source,)
        else:
            query += " ORDER BY queued_at, id"
        now = self.clock()
        running_by_source = dict(connection.execute(
            "SELECT source,count(*) FROM jobs "
            "WHERE state IN ('running','cancel_requested') GROUP BY source"
        ))
        schedules = dict(connection.execute(
            "SELECT source,next_allowed FROM source_schedules"
        ))
        candidates = []
        for row in connection.execute(query, values):
            profile = PROFILES[row["profile"]]
            running = 0 if release_running else running_by_source.get(row["source"], 0)
            next_allowed = schedules.get(row["source"])
            if running < profile.max_concurrency and (next_allowed is None or next_allowed <= now):
                candidates.append(row)
        return candidates

    @staticmethod
    def _lane_key(row) -> tuple[str, str]:
        return (str(row["source"]), PROFILES[row["profile"]].model)

    @staticmethod
    def _source_heads(candidates) -> dict[tuple[str, str], Any]:
        """Return exactly one FIFO-eligible job for each source/model lane.

        A source's oldest eligible row is its only dispatchable head.  This is
        important when one source has jobs targeting different models: model
        affinity must not let a younger job overtake that source's FIFO head.
        """
        heads_by_source: dict[str, Any] = {}
        for row in candidates:
            heads_by_source.setdefault(row["source"], row)
        return {
            Broker._lane_key(row): row for row in heads_by_source.values()
        }

    @staticmethod
    def _policy_signature(policy: SourcePolicy) -> tuple[tuple[str, float], ...]:
        sources = policy.snapshot().get("sources", {})
        return tuple(
            (source, float(entry["weight"]))
            for source, entry in sorted(sources.items())
            if entry.get("enabled", True)
        )

    @staticmethod
    def _lane_order(lanes: dict[tuple[str, str], dict[str, Any]]) -> list[tuple[str, str]]:
        """Order full model blocks, then source/model lanes within a block."""
        model_totals: dict[str, float] = {}
        model_oldest: dict[str, tuple[float, str]] = {}
        for key, lane in lanes.items():
            source, model = key
            model_totals[model] = model_totals.get(model, 0.0) + lane["weight"]
            candidate_key = (lane["queued_at"], source)
            if model not in model_oldest or candidate_key < model_oldest[model]:
                model_oldest[model] = candidate_key
        models = sorted(
            model_totals,
            key=lambda model: (-model_totals[model], model_oldest[model], model),
        )
        order: list[tuple[str, str]] = []
        for model in models:
            order.extend(sorted(
                (key for key in lanes if key[1] == model),
                key=lambda key: (-lanes[key]["weight"], lanes[key]["queued_at"], key[0]),
            ))
        return order

    def _new_time_cycle(
        self,
        available: dict[tuple[str, str], Any],
        policy: SourcePolicy,
        now: float,
        previous: dict[str, Any] | None = None,
        *,
        preserve_current: bool = False,
    ) -> dict[str, Any]:
        """Create/rebase a cycle from ready model/source FIFO heads.

        Budgets are measured in actual claimed-job execution seconds.  When a
        new lane becomes ready mid-cycle, only the *remaining* horizon is
        divided again; execution already charged to a previous lane is never
        erased.  That makes an empty lane work-conserving without allowing a
        reactivated lane to wait forever.
        """
        previous = previous or {}
        total_used = min(
            SCHEDULING_HORIZON_SECONDS, float(previous.get("total_used", 0.0)),
        )
        remaining = max(0.0, SCHEDULING_HORIZON_SECONDS - total_used)
        weights = {
            key: float(policy.weight(row["source"]) or 1.0)
            for key, row in available.items()
        }
        weight_total = sum(weights.values())
        old_lanes = previous.get("lanes", {})
        lanes: dict[tuple[str, str], dict[str, Any]] = {}
        for key, row in available.items():
            used = float(old_lanes.get(key, {}).get("used", 0.0))
            lanes[key] = {
                "source": key[0],
                "model": key[1],
                "weight": weights[key],
                "queued_at": row["queued_at"] if row["queued_at"] is not None else row["created"],
                "used": used,
                "budget": used + (remaining * weights[key] / weight_total if weight_total else 0.0),
            }
        current = previous.get("current") if preserve_current else None
        if current not in lanes:
            current = None
        return {
            "started_at": previous.get("started_at", now),
            "signature": self._policy_signature(policy),
            "total_used": total_used,
            "lanes": lanes,
            "order": self._lane_order(lanes),
            "current": current,
        }

    @staticmethod
    def _budget_remaining(lane: dict[str, Any]) -> bool:
        # A tiny tolerance avoids treating a floating-point round-off as a
        # distinct dispatchable time slice.
        return lane["used"] + 1e-9 < lane["budget"]

    def _time_batch_pick(
        self,
        candidates,
        policy: SourcePolicy,
        now: float,
        cycle: dict[str, Any] | None = None,
    ) -> tuple[Any | None, dict[str, Any] | None, dict[str, Any]]:
        """Select a source FIFO head using contiguous weighted time batches."""
        available = self._source_heads(candidates)
        if not available:
            return None, cycle, {}
        signature = self._policy_signature(policy)
        if cycle is None or cycle.get("total_used", 0.0) >= SCHEDULING_HORIZON_SECONDS:
            cycle = self._new_time_cycle(available, policy, now)
        elif cycle.get("signature") != signature:
            # A hot policy change takes effect at the next non-preemptive job
            # boundary.  The active request has already completed here.
            cycle = self._new_time_cycle(available, policy, now, cycle)
        elif any(key not in cycle.get("lanes", {}) for key in available):
            # A source/model lane became ready after an empty/failing period.
            # Preserve the current contiguous batch, but divide the remaining
            # horizon so the returning lane receives bounded service.
            cycle = self._new_time_cycle(
                available, policy, now, cycle, preserve_current=True,
            )

        selected_key = cycle.get("current")
        if (
            selected_key not in available
            or selected_key not in cycle["lanes"]
            or not self._budget_remaining(cycle["lanes"][selected_key])
        ):
            selected_key = next(
                (
                    key for key in cycle["order"]
                    if key in available and self._budget_remaining(cycle["lanes"][key])
                ),
                None,
            )
        if selected_key is None:
            # All ready planned lanes exhausted their allocations, or planned
            # lanes are empty.  Start the next horizon from what is ready so
            # no GPU time is intentionally left idle.
            cycle = self._new_time_cycle(available, policy, now)
            selected_key = next(
                key for key in cycle["order"] if key in available
            )

        cycle["current"] = selected_key
        selected = available[selected_key]
        lane = cycle["lanes"][selected_key]
        eligible_sources = sorted({row["source"] for row in available.values()})
        decision = {
            "mode": "time_batch",
            "reason": "60-minute weighted time batch with model affinity",
            "eligible_sources": eligible_sources,
            "active_weights": {
                source: weight for source, weight in self._policy_signature(policy)
            },
            "selected_source": selected["source"],
            "selected_model": selected_key[1],
            "horizon_seconds": SCHEDULING_HORIZON_SECONDS,
            "time_budget_seconds": round(lane["budget"], 6),
            "time_used_seconds": round(lane["used"], 6),
        }
        return selected, cycle, decision

    @staticmethod
    def _record_time_usage(
        cycle: dict[str, Any] | None,
        lane_key: tuple[str, str],
        elapsed: float,
    ) -> None:
        if cycle is None or lane_key not in cycle.get("lanes", {}):
            return
        charged = max(0.0, float(elapsed))
        lane = cycle["lanes"][lane_key]
        lane["used"] += charged
        cycle["total_used"] += charged

    @staticmethod
    def _forecast_duration(
        lane_key: tuple[str, str], estimates: dict[tuple[str, str], float],
    ) -> float:
        return max(1.0, float(estimates.get(lane_key, FORECAST_DEFAULT_EXECUTION_SECONDS)))

    def _publish_scheduler_snapshot(self) -> None:
        self._scheduler_snapshot = {
            "time_cycle": deepcopy(self._time_cycle),
            "duration_estimates": dict(self._duration_estimates),
            "active_time_batch": deepcopy(self._active_time_batch),
            "last": dict(self._last_scheduler_decision)
            if getattr(self, "_last_scheduler_decision", None) else None,
        }

    def _complete_time_batch(self, finished: float) -> None:
        """Charge one completed non-preemptive attempt to its selected lane."""
        active = self._active_time_batch
        if active is None:
            return
        lane = tuple(active["lane"])
        elapsed = max(0.0, float(finished) - float(active["started"]))
        self._record_time_usage(self._time_cycle, lane, elapsed)
        previous = self._duration_estimates.get(lane)
        self._duration_estimates[lane] = (
            elapsed if previous is None else ((previous + elapsed) / 2.0)
        )
        self._active_time_batch = None
        self._publish_scheduler_snapshot()

    def _next(self, allowed_sources: frozenset[str] | None = None, policy: SourcePolicy | None = None):
        # Without policy, retain the durable global FIFO compatibility path.
        # With policy, dispatch source FIFO heads in contiguous target-model
        # batches charged against a recurring 60-minute execution horizon.
        if policy is not None and allowed_sources is None:
            allowed_sources = policy.enabled_sources()
        candidates = self._candidates(allowed_sources)
        if not candidates:
            self._last_scheduler_decision = None
            self._scheduler_snapshot = None
            return None
        if policy is None:
            selected = candidates[0]
            decision = {
                "mode": "fifo",
                "reason": "oldest queued job",
                "eligible_sources": sorted({row["source"] for row in candidates}),
            }
        else:
            selected, self._time_cycle, decision = self._time_batch_pick(
                candidates, policy, self.clock(), self._time_cycle,
            )
            if selected is None:
                self._last_scheduler_decision = None
                self._scheduler_snapshot = None
                return None
        self._last_scheduler_decision = dict(decision)
        self._publish_scheduler_snapshot()
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
            if policy is not None:
                self._active_time_batch = {
                    "lane": self._lane_key(row),
                    "started": now,
                }
                self._publish_scheduler_snapshot()
            interval = PROFILES[row["profile"]].min_interval_seconds
            if interval:
                self.db.execute(
                    "INSERT INTO source_schedules(source,next_allowed) VALUES(?,?) "
                    "ON CONFLICT(source) DO UPDATE SET next_allowed=excluded.next_allowed",
                    (row["source"], now + interval),
                )
            self._refresh_health_cache_locked()
            # Payload is needed only after this job has been durably claimed.
            # Fetching it here keeps the scheduler's full-queue projection
            # payload-free while preserving the exact execution request.
            execution_row = self.db.execute(
                "SELECT * FROM jobs WHERE id=?", (row["id"],)
            ).fetchone()
            if execution_row is None:
                raise RuntimeError("claimed job disappeared before execution")
        try:
            self._execute(execution_row)
        except Exception as exc:
            with self.lock, self.db:
                finished = self.clock()
                self._complete_time_batch(finished)
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
        models = self.ollama.ps().get("models", [])
        self._loaded_models_cache = [dict(model) for model in models if isinstance(model, dict)]
        loaded = [m.get("name") for m in models]
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
        default_context = profile.default_context or profile.max_context
        options["num_ctx"] = min(int(options.get("num_ctx", default_context)), profile.max_context)
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
            if final == "completed":
                self.storage.record_result(row["id"], row["source"], result, finished)
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
            self._complete_time_batch(finished)
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
        snapshot, _ = self._observer_read(lambda db: {
            "queue_depth": db.execute(
                "SELECT count(*) FROM jobs WHERE state='queued'"
            ).fetchone()[0],
            "active": (lambda row: dict(row) if row else None)(db.execute(
                "SELECT id,source,profile,kind,state,created,started,lease_until,attempt_count,retry_count "
                "FROM jobs WHERE state IN ('running','cancel_requested') "
                "ORDER BY started,created,id LIMIT 1"
            ).fetchone()),
        })
        if snapshot is not None:
            self._metrics_cache = {"resource": "mainpc-gpu", **snapshot}
        return {
            **self._metrics_cache,
            "loaded_models": [dict(model) for model in self._loaded_models_cache],
            "storage": self.storage.wal_snapshot(),
            "timestamp": self.clock(),
        }

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
        self.drain_event = threading.Event()

    def drain(self) -> None:
        """Stop new claims while the in-flight remote execution finishes."""
        self.drain_event.set()
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
            self.broker.storage.maybe_checkpoint()
            if not self.drain_event.is_set():
                self.broker.dispatch_once(allowed, policy)
            self.stop_event.wait(self.interval)
