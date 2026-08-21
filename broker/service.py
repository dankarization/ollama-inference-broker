from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from pathlib import Path

from .profiles import FIXED_SOURCE_PRIORITIES, MAX_PRIORITY, MIN_PRIORITY, PROFILES


class Broker:
    """One-resource scheduler; MAIN-PC remains only the wakeable executor."""
    def __init__(self, database: str | Path, ollama, wol, clock=time.time, lease_seconds=60):
        self.ollama, self.wol, self.clock, self.lease_seconds = ollama, wol, clock, lease_seconds
        self.db = sqlite3.connect(str(database), check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.lock = threading.RLock()
        self._init_db()
        self.recover()

    def _init_db(self):
        with self.db:
            self.db.execute("PRAGMA journal_mode=WAL")
            self.db.execute("""CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY, profile TEXT NOT NULL, kind TEXT NOT NULL,
                source TEXT NOT NULL, priority INTEGER NOT NULL,
                payload TEXT NOT NULL, state TEXT NOT NULL, created REAL NOT NULL,
                started REAL, finished REAL, lease_until REAL, error TEXT,
                switch_reason TEXT)""")
            # Compatible with databases created by the first isolated MVP.
            columns = {row[1] for row in self.db.execute("PRAGMA table_info(jobs)")}
            if "source" not in columns:
                self.db.execute("ALTER TABLE jobs ADD COLUMN source TEXT NOT NULL DEFAULT 'legacy'")
            if "priority" not in columns:
                self.db.execute("ALTER TABLE jobs ADD COLUMN priority INTEGER NOT NULL DEFAULT 10")

    def recover(self):
        # A prior process cannot own the remote GPU after its lease. Requeue stale
        # work (rather than falsely claiming success) and retain cancellation.
        now = self.clock()
        with self.lock, self.db:
            self.db.execute("UPDATE jobs SET state='queued', started=NULL, lease_until=NULL, error='requeued after broker restart' "
                            "WHERE state='running' AND (lease_until IS NULL OR lease_until < ?)", (now,))

    def submit(self, profile: str, kind: str, payload: dict, source: str | None = None,
               priority: int | None = None) -> dict:
        if profile not in PROFILES:
            raise ValueError("unknown profile")
        if kind not in {"chat", "generate"}:
            raise ValueError("kind must be chat or generate")
        source = source or profile
        resolved_priority = self._resolve_priority(source, priority)
        job_id, now = str(uuid.uuid4()), self.clock()
        with self.lock, self.db:
            self.db.execute("INSERT INTO jobs(id,profile,kind,source,priority,payload,state,created) VALUES(?,?,?,?,?,?,?,?)",
                            (job_id, profile, kind, source, resolved_priority, json.dumps(payload), "queued", now))
        return self.status(job_id)

    @staticmethod
    def _resolve_priority(source: str, priority: int | None) -> int:
        fixed = FIXED_SOURCE_PRIORITIES.get(source)
        if fixed is not None:
            if priority is not None and priority != fixed:
                raise ValueError(f"source '{source}' has fixed priority {fixed}")
            return fixed
        if isinstance(priority, bool) or not isinstance(priority, int) or not MIN_PRIORITY <= priority <= MAX_PRIORITY:
            raise ValueError(f"priority for non-fixed sources must be an integer from {MIN_PRIORITY} to {MAX_PRIORITY}")
        return priority

    def status(self, job_id: str) -> dict | None:
        with self.lock:
            row = self.db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            return self._job(row) if row else None

    def _job(self, row):
        data = dict(row)
        data["payload"] = json.loads(data["payload"])
        if data["state"] == "queued":
            data["queue_position"] = self._position(row)
        return data

    def _position(self, row):
        before = self.db.execute("SELECT count(*) FROM jobs WHERE state='queued' AND "
            "(priority < ? OR (priority = ? AND (created < ? OR (created = ? AND id < ?))))",
            (row["priority"], row["priority"], row["created"], row["created"], row["id"])).fetchone()[0]
        return before + 1

    def cancel(self, job_id: str) -> dict | None:
        with self.lock, self.db:
            row = self.db.execute("SELECT state FROM jobs WHERE id=?", (job_id,)).fetchone()
            if not row: return None
            if row["state"] == "queued":
                self.db.execute("UPDATE jobs SET state='cancelled', finished=? WHERE id=?", (self.clock(), job_id))
            # Remote Ollama cancellation is not assumed safe; a running job is
            # marked cancel_requested and releases the slot after its request returns.
            elif row["state"] == "running":
                self.db.execute("UPDATE jobs SET state='cancel_requested' WHERE id=?", (job_id,))
        return self.status(job_id)

    def _next(self):
        # Strict priority is deliberate: no aging can promote lower-priority
        # work ahead of a waiting higher-priority job.
        return self.db.execute("SELECT * FROM jobs WHERE state='queued' ORDER BY priority, created, id LIMIT 1").fetchone()

    def dispatch_once(self) -> bool:
        with self.lock, self.db:
            if self.db.execute("SELECT 1 FROM jobs WHERE state IN ('running','cancel_requested')").fetchone():
                return False
            row = self._next()
            if not row: return False
            now = self.clock()
            self.db.execute("UPDATE jobs SET state='running',started=?,lease_until=? WHERE id=?", (now, now+self.lease_seconds, row["id"]))
        try:
            self._execute(row)
        except Exception as exc:
            with self.lock, self.db:
                self.db.execute("UPDATE jobs SET state='failed',finished=?,lease_until=NULL,error=? WHERE id=?", (self.clock(), str(exc), row["id"]))
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
        if not self.ollama.is_ready(profile.model):
            raise RuntimeError("target model did not become ready")
        payload = json.loads(row["payload"])
        options = dict(payload.get("options", {}))
        options["num_ctx"] = min(int(options.get("num_ctx", profile.max_context)), profile.max_context)
        options["num_predict"] = min(int(options.get("num_predict", profile.max_output)), profile.max_output)
        request = {k: v for k, v in payload.items() if k not in {"model", "keep_alive"}}
        request.update({"model": profile.model, "stream": False, "options": options,
                        "keep_alive": f"{profile.keep_alive_seconds}s"})
        self.ollama.run(row["kind"], request)
        with self.lock, self.db:
            state = self.db.execute("SELECT state FROM jobs WHERE id=?", (row["id"],)).fetchone()[0]
            final = "cancelled" if state == "cancel_requested" else "completed"
            self.db.execute("UPDATE jobs SET state=?,finished=?,lease_until=NULL,switch_reason=? WHERE id=?",
                            (final, self.clock(), reason, row["id"]))

    def metrics(self) -> dict:
        with self.lock:
            queued = self.db.execute("SELECT count(*) FROM jobs WHERE state='queued'").fetchone()[0]
            active = self.db.execute("SELECT * FROM jobs WHERE state IN ('running','cancel_requested')").fetchone()
        ps = self.ollama.ps()
        return {"resource": "mainpc-gpu", "queue_depth": queued, "active": self._job(active) if active else None,
                "loaded_models": ps.get("models", []), "timestamp": self.clock()}


class Dispatcher(threading.Thread):
    def __init__(self, broker: Broker, interval=0.25):
        super().__init__(daemon=True); self.broker, self.interval, self.stop_event = broker, interval, threading.Event()
    def run(self):
        while not self.stop_event.is_set():
            self.broker.dispatch_once(); self.stop_event.wait(self.interval)
