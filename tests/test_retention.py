import hashlib
import json
import sqlite3
import tempfile
import threading
import unittest
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from broker.policy import SourcePolicyError, normalize_source_policy
from broker.retention_migration import (
    RetentionMigrationError,
    build_floor_database,
    build_repacked_database,
    build_terminal_repacked_database,
    forecast,
    validate_manifest_entry,
)
from broker.service import Broker, SourcePolicy
from broker.storage import StorageContractError, content_evidence


class Clock:
    def __init__(self, value=100.0):
        self.value = value

    def __call__(self):
        return self.value


class FakeOllama:
    def ps(self):
        return {"models": [{"name": "nemotron3:33b", "size_vram": 1}]}

    def is_ready(self, _model):
        return True

    def unload(self, _model):
        raise AssertionError("no switch expected")

    def run(self, _kind, request):
        return {"done": True, "output": request.get("prompt", "")}


class FakeWol:
    def wake(self):
        pass


class RetentionTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.clock = Clock()
        self.policy_path = self.root / "sources.json"
        self._write_policy()
        self.policy = SourcePolicy(self.policy_path, clock=self.clock)
        self.database = self.root / "broker.sqlite3"
        self.broker = Broker(
            self.database, FakeOllama(), FakeWol(), clock=self.clock,
        )
        self.broker.use_source_policy(self.policy)
        self.addCleanup(self.broker.db.close)

    def _write_policy(self, **overrides):
        source = {
            "enabled": True,
            "admission_allowed": True,
            "weight": 1,
            "producer_storage_enabled": True,
            "producer_storage_mode": "producer_owned",
            "ack_required": True,
            "compaction_enabled": True,
            "legacy_result_fallback": False,
            "compaction_grace_seconds": 0,
            "quarantine_grace_seconds": 0,
            "retention_enabled": True,
            "acked_body_retention_seconds": 0,
            "unacked_terminal_retention_seconds": 5,
            "failed_cancelled_retention_seconds": 5,
            "metadata_retention_seconds": 10,
            "receipt_retention_seconds": 20,
            "inline_budget_bytes": 1024 * 1024,
            **overrides,
        }
        self.policy_path.write_text(json.dumps({
            "version": 1,
            "sources": {"producer": source},
        }), encoding="utf-8")

    def _admit(self, external_id="one", payload=None):
        payload = payload or {"prompt": external_id}
        digest, size = content_evidence(payload)
        return self.broker.submit(
            "interactive", "generate", payload, source="producer",
            source_item_id=external_id, external_id=external_id,
            producer_storage={
                "producer_attempt_id": f"attempt-{external_id}",
                "input": {
                    "mode": "producer_owned",
                    "storage_ref": f"cas://inputs/{external_id}",
                    "content_hash": digest,
                    "byte_size": size,
                },
                "result": {
                    "mode": "producer_owned",
                    "schema_version": "result-v1",
                },
            },
        )

    def _ack_input(self, job):
        status = self.broker.compact_status(job["id"])
        return self.broker.acknowledge_input(job["id"], {
            "job_id": job["id"],
            "producer": "producer",
            "producer_attempt_id": status["producer_attempt_id"],
            "storage_ref": status["input_ref"],
            "input_hash": status["input_hash"],
            "input_bytes": status["input_bytes"],
            "persisted_at": "2026-09-22T00:00:00Z",
        })

    def _ack_result(self, job):
        status = self.broker.compact_status(job["id"])
        return self.broker.acknowledge_result(job["id"], {
            "job_id": job["id"],
            "producer": "producer",
            "producer_attempt_id": status["producer_attempt_id"],
            "storage_ref": f"cas://results/{job['id']}",
            "result_hash": status["result_hash"],
            "result_bytes": status["result_bytes"],
            "schema_version": "result-v1",
            "persisted_at": "2026-09-22T00:00:01Z",
        })

    def test_old_policy_is_compatible_and_retention_defaults_off(self):
        sources = normalize_source_policy({
            "sources": {
                name: {"enabled": True, "weight": 1}
                for name in (
                    "olya-vision", "olya-decision", "shutterstock-video",
                    "syncopia-telegram-memory", "generic-producer",
                )
            },
        })
        self.assertTrue(all(not value["retention_enabled"] for value in sources.values()))
        self.assertTrue(all(value["inline_budget_bytes"] > 0 for value in sources.values()))
        with self.assertRaises(SourcePolicyError):
            normalize_source_policy({
                "sources": {"producer": {"inline_budget_bytes": 0}},
            })
        temporary = normalize_source_policy({
            "sources": {"producer": {"retention_enabled": True}},
        })["producer"]
        self.assertTrue(temporary["retention_enabled"])
        self.assertFalse(temporary["producer_storage_enabled"])
        self.assertEqual(
            self.broker.storage_health()["database_target_bytes"],
            2 * 1024 * 1024 * 1024,
        )

    def test_acked_bodies_compact_then_metadata_and_receipt_expire(self):
        job = self._admit()
        self._ack_input(job)
        self.assertTrue(self.broker.dispatch_once(frozenset({"producer"})))
        receipt = self._ack_result(job)
        cycle = self.broker.storage.maybe_maintain(force=True)
        self.assertEqual(cycle["sources"]["producer"]["compacted"], 1)
        status = self.broker.status(job["id"])
        self.assertEqual(status["payload"], {})
        self.assertNotIn("result", status)
        self.assertEqual(
            self.broker.compact_status(job["id"])["compaction_state"], "metadata_only",
        )
        self.clock.value = 111
        self.broker.storage.maybe_maintain(force=True)
        tombstone = self.broker.status(job["id"])
        self.assertEqual(tombstone["retention_state"], "tombstone")
        self.assertEqual(self.broker.receipt(job["id"])["receipt"]["receipt_id"], receipt["receipt_id"])
        with self.assertRaisesRegex(ValueError, "recover the durable producer artifact"):
            self._admit()
        with self.assertRaisesRegex(ValueError, "idempotency conflict"):
            self._admit(payload={"prompt": "different"})
        self.clock.value = 121
        self.broker.storage.maybe_maintain(force=True)
        self.assertIsNone(self.broker.receipt(job["id"])["receipt"])
        self.clock.value = 157_680_112
        self.broker.storage.maybe_maintain(force=True)
        self.assertIsNone(self.broker.status(job["id"]))

    def test_tombstone_outlives_persisted_receipt_deadline(self):
        self._write_policy(
            receipt_retention_seconds=100,
            tombstone_retention_seconds=100,
        )
        job = self._admit("long-receipt")
        self._ack_input(job)
        self.assertTrue(self.broker.dispatch_once(frozenset({"producer"})))
        receipt = self._ack_result(job)
        self.broker.storage.maybe_maintain(force=True)
        persisted_until = self.broker.db.execute(
            "SELECT retention_until FROM job_delivery_acks WHERE receipt_id=?",
            (receipt["receipt_id"],),
        ).fetchone()[0]
        self.assertEqual(persisted_until, 200)

        self._write_policy(
            receipt_retention_seconds=20,
            tombstone_retention_seconds=20,
        )
        self.clock.value = 111
        self.broker.storage.maybe_maintain(force=True)
        tombstone_until = self.broker.db.execute(
            "SELECT expires_at FROM job_retention_tombstones WHERE job_id=?",
            (job["id"],),
        ).fetchone()[0]
        self.assertEqual(tombstone_until, persisted_until)
        self.clock.value = 132
        self.broker.storage.maybe_maintain(force=True)
        self.assertEqual(self.broker.status(job["id"])["retention_state"], "tombstone")
        self.clock.value = 201
        self.broker.storage.maybe_maintain(force=True)
        self.assertIsNone(self.broker.receipt(job["id"]))
        self.assertIsNone(self.broker.status(job["id"]))

    def test_unacked_terminal_is_reported_and_never_blindly_compacted(self):
        job = self._admit("unacked")
        self._ack_input(job)
        self.assertTrue(self.broker.dispatch_once(frozenset({"producer"})))
        self.clock.value = 106
        self.broker.storage.maybe_maintain(force=True)
        self.assertEqual(self.broker.status(job["id"])["payload"], {})
        self.assertIn("result", self.broker.status(job["id"]))
        self.assertEqual(self.broker.storage_health()["overdue_unacked_terminal"], 1)

    def test_broker_temporary_result_expires_but_active_payload_never_does(self):
        self._write_policy(
            producer_storage_enabled=False,
            producer_storage_mode="broker_temporary",
            ack_required=False,
            compaction_enabled=False,
            legacy_result_fallback=True,
        )
        queued = self.broker.submit(
            "interactive", "generate", {"prompt": "queued"}, source="producer",
        )
        completed = self.broker.submit(
            "interactive", "generate", {"prompt": "completed"}, source="producer",
        )
        self.assertTrue(self.broker.dispatch_once(frozenset({"producer"})))
        # FIFO completed the first row; identify it from durable state.
        completed_id = self.broker.db.execute(
            "SELECT id FROM jobs WHERE state='completed'"
        ).fetchone()[0]
        queued_id = self.broker.db.execute(
            "SELECT id FROM jobs WHERE state='queued'"
        ).fetchone()[0]
        self.assertEqual(self.broker.status(completed_id)["payload"], {})
        self.assertIn("result", self.broker.status(completed_id))
        self.assertIn(
            self.broker.status(queued_id)["payload"],
            ({"prompt": "queued"}, {"prompt": "completed"}),
        )
        self.clock.value = 106
        cycle = self.broker.storage.maybe_maintain(force=True)
        self.assertEqual(cycle["sources"]["producer"]["broker_results_expired"], 1)
        self.assertNotIn("result", self.broker.status(completed_id))
        self.assertNotEqual(self.broker.status(queued_id)["payload"], {})

    def test_retention_disable_race_stops_destructive_maintenance(self):
        temporary = {
            "producer_storage_enabled": False,
            "producer_storage_mode": "broker_temporary",
            "ack_required": False,
            "compaction_enabled": False,
            "legacy_result_fallback": True,
        }
        self._write_policy(**temporary)
        job = self.broker.submit(
            "interactive", "generate", {"prompt": "retain-me"}, source="producer",
        )
        self.assertTrue(self.broker.dispatch_once(frozenset({"producer"})))
        self.clock.value = 106
        observed = self.policy.snapshot()

        def disable_after_snapshot():
            self._write_policy(retention_enabled=False, **temporary)
            return observed

        with patch.object(self.policy, "snapshot", side_effect=disable_after_snapshot):
            cycle = self.broker.storage.maybe_maintain(force=True)
        self.assertEqual(cycle["changed"], 0)
        self.assertIn("result", self.broker.status(job["id"]))

    def test_automatic_maintenance_acquires_broker_before_policy_file_lock(self):
        broker_lock_available = []

        @contextmanager
        def inspect_policy_lock(_path):
            def probe_broker_lock():
                acquired = self.broker.lock.acquire(timeout=0.1)
                broker_lock_available.append(acquired)
                if acquired:
                    self.broker.lock.release()

            probe = threading.Thread(target=probe_broker_lock)
            probe.start()
            probe.join(timeout=1)
            self.assertFalse(probe.is_alive())
            yield

        with patch("broker.storage.source_policy_write_lock", inspect_policy_lock):
            self.broker.storage.maybe_maintain(force=True)
        self.assertEqual(broker_lock_available, [False])

    def test_representative_terminal_regrowth_is_bounded(self):
        self._write_policy(
            producer_storage_enabled=False,
            producer_storage_mode="broker_temporary",
            ack_required=False,
            compaction_enabled=False,
            legacy_result_fallback=True,
            inline_budget_bytes=8 * 1024 * 1024,
        )
        for index in range(20):
            self.broker.submit(
                "interactive", "generate",
                {"prompt": f"{index}:" + "x" * 100_000},
                source="producer",
            )
            self.assertTrue(self.broker.dispatch_once(frozenset({"producer"})))
        usage = self.broker.db.execute(
            "SELECT inline_bytes FROM storage_source_usage WHERE source='producer'"
        ).fetchone()[0]
        # Only the completed results remain during their forensic TTL; the
        # equally large input payloads were discarded at completion.
        self.assertLess(usage, 2_100_000)
        self.clock.value = 106
        cycle = self.broker.storage.maybe_maintain(force=True)
        self.assertEqual(
            cycle["sources"]["producer"]["broker_results_expired"], 20,
        )
        usage = self.broker.db.execute(
            "SELECT inline_bytes FROM storage_source_usage WHERE source='producer'"
        ).fetchone()[0]
        self.assertEqual(usage, 2 * 20)

    def test_failed_retry_window_expires_after_finite_deadline(self):
        job = self._admit("failed")
        self._ack_input(job)
        with self.broker.lock, self.broker.db:
            self.broker.db.execute(
                "UPDATE jobs SET state='failed',finished=? WHERE id=?",
                (self.clock(), job["id"]),
            )
            self.broker.storage.record_terminal_retention(
                job["id"], "producer", "failed", self.clock(),
            )
        self.clock.value = 106
        cycle = self.broker.storage.maybe_maintain(force=True)
        self.assertEqual(cycle["sources"]["producer"]["retry_expired"], 1)
        self.assertEqual(self.broker.status(job["id"])["payload"], {})
        with self.assertRaisesRegex(ValueError, "retry retention has expired"):
            self.broker.retry(job["id"])
        self.clock.value = 111
        self.broker.storage.maybe_maintain(force=True)
        with self.assertRaisesRegex(ValueError, "retry retention has expired"):
            self.broker.retry(job["id"])

    def test_startup_recovery_applies_loaded_retention_policy(self):
        job = self._admit("startup-expired")
        with self.broker.db:
            self.broker.db.execute(
                "UPDATE jobs SET state='running',started=90,lease_until=99,"
                "attempt_count=1 WHERE id=?", (job["id"],),
            )
        self.broker.db.close()
        self.clock.value = 106
        recovered = Broker(
            self.database, FakeOllama(), FakeWol(), clock=self.clock,
            source_policy=self.policy,
        )
        self.addCleanup(recovered.db.close)
        status = recovered.compact_status(job["id"])
        self.assertEqual(status["state"], "failed")
        deadlines = recovered.db.execute(
            "SELECT body_retention_until,metadata_retention_until FROM jobs WHERE id=?",
            (job["id"],),
        ).fetchone()
        self.assertEqual(tuple(deadlines), (111, 116))

    def test_legacy_external_id_is_reusable_after_metadata_purge(self):
        self._write_policy(
            producer_storage_enabled=False,
            producer_storage_mode="broker_temporary",
            ack_required=False,
            compaction_enabled=False,
            legacy_result_fallback=True,
        )
        first = self.broker.submit(
            "interactive", "generate", {"prompt": "legacy-reuse"},
            source="producer", external_id="legacy-reuse",
        )
        self.assertTrue(self.broker.dispatch_once(frozenset({"producer"})))
        self.clock.value = 106
        self.broker.storage.maybe_maintain(force=True)
        self.clock.value = 111
        self.broker.storage.maybe_maintain(force=True)
        self.assertEqual(
            self.broker.status(first["id"])["retention_state"], "tombstone",
        )
        second = self.broker.submit(
            "interactive", "generate", {"prompt": "legacy-reuse"},
            source="producer", external_id="legacy-reuse",
        )
        self.assertNotEqual(second["id"], first["id"])

    def test_oversized_expired_retry_body_does_not_pin_scan(self):
        jobs = []
        for external_id, size in (("old-oversized", 2048), ("later-small", 16)):
            job = self._admit(external_id, payload={"prompt": "x" * size})
            with self.broker.db:
                self.broker.db.execute(
                    "UPDATE jobs SET state='failed',finished=100 WHERE id=?",
                    (job["id"],),
                )
                self.broker.storage.record_terminal_retention(
                    job["id"], "producer", "failed", 100,
                )
            jobs.append(job)
        self.clock.value = 106
        with (
            patch("broker.storage.DEFAULT_MAINTENANCE_OVERSIZE_BYTES", 1024),
            self.broker.lock,
            self.broker.db,
        ):
            result = self.broker.storage._compact_expired_retryable_locked(
                "producer", now=106, limit=1, max_bytes=512,
            )
        self.assertEqual(result["retry_expired"], 1)
        self.assertEqual(
            self.broker.compact_status(jobs[0]["id"])["compaction_state"], "full",
        )
        self.assertEqual(
            self.broker.compact_status(jobs[1]["id"])["compaction_state"],
            "metadata_only",
        )

    def test_inline_budget_ignores_active_payload_but_blocks_terminal_bodies(self):
        self._write_policy(inline_budget_bytes=1)
        queued = self._admit("queued", payload={"prompt": "x" * 10_000})
        self.assertEqual(queued["state"], "queued")
        second = self._admit("second-queued", payload={"prompt": "y" * 10_000})
        self.assertEqual(second["state"], "queued")
        self._write_policy(inline_budget_bytes=1024 * 1024)
        self.broker.cancel(queued["id"])
        self.broker.cancel(second["id"])
        self.clock.value = 106
        self.broker.storage.maybe_maintain(force=True)
        terminal = self._admit("terminal")
        self.assertTrue(self.broker.dispatch_once(frozenset({"producer"})))
        self.assertEqual(self.broker.status(terminal["id"])["state"], "completed")
        self._write_policy(inline_budget_bytes=1)
        with self.assertRaisesRegex(StorageContractError, "budget is exhausted"):
            self._admit("too-large")
        self.assertEqual(
            self.broker.db.execute(
                "SELECT terminal_inline_bytes FROM storage_source_usage "
                "WHERE source='producer'"
            ).fetchone()[0],
            len("{}") + len(json.dumps({"done": True, "output": "terminal"})),
        )

    def test_disk_reserve_blocks_only_new_admission_and_preserves_replay(self):
        existing = self._admit("disk-replay")
        two_gib = 2 * 1024 * 1024 * 1024
        constrained = SimpleNamespace(
            total=20 * 1024 * 1024 * 1024,
            used=18 * 1024 * 1024 * 1024,
            free=two_gib,
        )
        with patch("broker.storage.shutil.disk_usage", return_value=constrained):
            replay = self._admit("disk-replay")
            self.assertEqual(replay["id"], existing["id"])
            with self.assertRaisesRegex(
                StorageContractError, "free-space reserve would be breached",
            ):
                self._admit("disk-blocked")
            disk = self.broker.storage_health()["disk"]
        self.assertEqual(disk["reserve_bytes"], two_gib)
        self.assertEqual(disk["admission_available_bytes"], 0)
        self.assertTrue(disk["under_pressure"])
        self.assertEqual(
            self.broker.db.execute(
                "SELECT count(*) FROM jobs WHERE external_id='disk-blocked'"
            ).fetchone()[0],
            0,
        )

    def test_startup_queued_at_backfill_uses_partial_index(self):
        plan = " ".join(
            str(value)
            for row in self.broker.db.execute(
                "EXPLAIN QUERY PLAN UPDATE jobs SET queued_at=created "
                "WHERE queued_at IS NULL"
            )
            for value in row
        )
        self.assertIn("jobs_missing_queued_at", plan)

    def test_out_of_place_manifest_repack_and_floor_preserve_source(self):
        payload = {"prompt": "x" * 100_000}
        completed = self._admit("historical", payload=payload)
        self.assertTrue(self.broker.dispatch_once(frozenset({"producer"})))
        status = self.broker.compact_status(completed["id"])
        with self.broker.db:
            self.broker.db.execute(
                "INSERT INTO audit_events(occurred,event_type,job_id,source,"
                "producer_storage) VALUES(?,?,?,?,0)",
                (99, "legacy.historical", completed["id"], "producer"),
            )
        queued = self._admit("queued", payload={"prompt": "keep-me"})
        self.broker.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        self.broker.db.close()
        before = hashlib.sha256(self.database.read_bytes()).hexdigest()
        input_hash, input_bytes = content_evidence(payload)
        result = self.broker.ollama.run("generate", payload)
        result_hash, result_bytes = content_evidence(result)
        entry = {
            "job_id": completed["id"],
            "source": "producer",
            "producer_attempt_id": status["producer_attempt_id"],
            "input": {
                "storage_ref": "cas://inputs/historical",
                "content_hash": input_hash,
                "byte_size": input_bytes,
                "persisted_at": "2026-09-22T00:00:00Z",
                "readback_at": "2026-09-22T00:00:01Z",
            },
            "result": {
                "storage_ref": "cas://results/historical",
                "content_hash": result_hash,
                "byte_size": result_bytes,
                "schema_version": "result-v1",
                "persisted_at": "2026-09-22T00:00:02Z",
                "readback_at": "2026-09-22T00:00:03Z",
            },
        }
        validation_db = sqlite3.connect(f"file:{self.database}?mode=ro", uri=True)
        validation_db.row_factory = sqlite3.Row
        self.addCleanup(validation_db.close)
        with self.assertRaisesRegex(RetentionMigrationError, "precedes"):
            validate_manifest_entry(validation_db, {
                **entry,
                "input": {
                    **entry["input"],
                    "readback_at": "2026-09-21T23:59:59Z",
                },
            })
        output = self.root / "repacked.sqlite3"
        report = build_repacked_database(
            self.database, output, self.policy_path, [entry],
        )
        self.assertEqual(report["integrity_check"], "ok")
        self.assertEqual(report["output_wal_bytes"], 0)
        self.assertEqual(report["protected_jobs"]["rows"], 1)
        self.assertIn("jobs_missing_queued_at", " ".join(report["queued_at_plan"]))
        self.assertEqual(hashlib.sha256(self.database.read_bytes()).hexdigest(), before)
        db = sqlite3.connect(output)
        self.addCleanup(db.close)
        body = db.execute(
            "SELECT payload,result_json FROM jobs WHERE id=?", (completed["id"],)
        ).fetchone()
        self.assertEqual(body, ("{}", None))
        self.assertEqual(
            db.execute(
                "SELECT count(*) FROM audit_events WHERE job_id=? "
                "AND producer_storage=0",
                (completed["id"],),
            ).fetchone()[0],
            0,
        )
        queued_body = db.execute(
            "SELECT payload,state FROM jobs WHERE id=?", (queued["id"],)
        ).fetchone()
        self.assertIn("keep-me", queued_body[0])
        self.assertEqual(queued_body[1], "queued")
        source = sqlite3.connect(f"file:{self.database}?mode=ro", uri=True)
        source.row_factory = sqlite3.Row
        self.addCleanup(source.close)
        forecast_statements = []
        source.set_trace_callback(forecast_statements.append)
        summary = forecast(source, self.database, [entry])
        self.assertEqual(summary["manifest_validated"]["rows"], 1)
        self.assertFalse(any(
            "GROUP BY" in statement.upper()
            for statement in forecast_statements
        ))
        self.assertEqual(
            sum(group["rows"] for group in summary["groups"]), 2,
        )
        floor = self.root / "floor.sqlite3"
        floor_report = build_floor_database(self.database, floor)
        self.assertTrue(floor_report["non_deployable"])
        self.assertEqual(floor_report["integrity_check"], "ok")
        self.assertEqual(floor_report["protected_jobs"]["rows"], 1)
        self.assertIn("jobs_missing_queued_at", " ".join(floor_report["queued_at_plan"]))

    def test_terminal_repack_preserves_active_and_recent_retry_bodies(self):
        self._write_policy(
            producer_storage_enabled=False,
            producer_storage_mode="broker_temporary",
            ack_required=False,
            compaction_enabled=False,
            legacy_result_fallback=True,
        )
        completed = self.broker.submit(
            "interactive", "generate", {"prompt": "done"}, source="producer",
        )
        self.assertTrue(self.broker.dispatch_once(frozenset({"producer"})))
        completed_evidence = self.broker.compact_status(completed["id"])
        queued = self.broker.submit(
            "interactive", "generate", {"prompt": "keep-active"}, source="producer",
        )
        failed = self.broker.submit(
            "interactive", "generate", {"prompt": "keep-retry"}, source="producer",
        )
        with self.broker.db:
            self.broker.db.execute(
                "UPDATE jobs SET state='failed',finished=? WHERE id=?",
                (104.0, failed["id"]),
            )
            self.broker.storage.record_terminal_retention(
                failed["id"], "producer", "failed", 104.0,
            )
        self.broker.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        self.broker.db.close()
        before = hashlib.sha256(self.database.read_bytes()).hexdigest()
        output = self.root / "terminal.sqlite3"
        report = build_terminal_repacked_database(
            self.database, output, self.policy_path, now=106.0,
        )
        self.assertTrue(report["target_met"])
        self.assertEqual(report["output_wal_bytes"], 0)
        self.assertEqual(report["protected_jobs"]["rows"], 1)
        self.assertEqual(hashlib.sha256(self.database.read_bytes()).hexdigest(), before)
        db = sqlite3.connect(output)
        self.addCleanup(db.close)
        completed_body = db.execute(
            "SELECT payload,result_json,compaction_state,input_hash,result_hash,input_bytes "
            "FROM jobs WHERE id=?", (completed["id"],),
        ).fetchone()
        self.assertEqual(completed_body[0:3], ("{}", None, "metadata_only"))
        self.assertEqual(completed_body[3], completed_evidence["input_hash"])
        self.assertEqual(completed_body[5], completed_evidence["input_bytes"])
        self.assertTrue(completed_body[4].startswith("sha256:"))
        self.assertIn("keep-active", db.execute(
            "SELECT payload FROM jobs WHERE id=?", (queued["id"],),
        ).fetchone()[0])
        self.assertIn("keep-retry", db.execute(
            "SELECT payload FROM jobs WHERE id=?", (failed["id"],),
        ).fetchone()[0])
        terminal_usage = db.execute(
            "SELECT terminal_inline_bytes FROM storage_source_usage "
            "WHERE source='producer'",
        ).fetchone()[0]
        expected_usage = db.execute(
            "SELECT sum(length(CAST(payload AS BLOB))+"
            "coalesce(length(CAST(result_json AS BLOB)),0)) FROM jobs "
            "WHERE source='producer' AND state IN ('completed','failed','cancelled') "
            "AND compaction_state='full'",
        ).fetchone()[0]
        self.assertEqual(terminal_usage, expected_usage)

    def test_terminal_repack_honors_compaction_guard_and_backfills_receipt_ttl(self):
        job = self._admit("guarded-repack")
        self._ack_input(job)
        self.assertTrue(self.broker.dispatch_once(frozenset({"producer"})))
        receipt = self._ack_result(job)
        with self.broker.db:
            self.broker.db.execute(
                "UPDATE job_delivery_acks SET retention_until=NULL WHERE receipt_id=?",
                (receipt["receipt_id"],),
            )
        self.broker.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        self.broker.db.close()
        self._write_policy(compaction_enabled=False)
        output = self.root / "guarded-terminal.sqlite3"
        build_terminal_repacked_database(
            self.database, output, self.policy_path, now=106,
        )
        db = sqlite3.connect(output)
        self.addCleanup(db.close)
        body = db.execute(
            "SELECT result_json,compaction_state FROM jobs WHERE id=?",
            (job["id"],),
        ).fetchone()
        self.assertIsNotNone(body[0])
        self.assertEqual(body[1], "full")
        self.assertEqual(
            db.execute(
                "SELECT retention_until FROM job_delivery_acks WHERE receipt_id=?",
                (receipt["receipt_id"],),
            ).fetchone()[0],
            126,
        )

    def test_failed_terminal_repack_removes_staging_database(self):
        self.broker.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        self.broker.db.close()
        output = self.root / "too-large.sqlite3"
        staging = self.root / ".too-large.sqlite3.terminal-staging"
        with self.assertRaisesRegex(RetentionMigrationError, "target is < 1"):
            build_terminal_repacked_database(
                self.database, output, self.policy_path, now=106, target_bytes=1,
            )
        self.assertFalse(staging.exists())
        self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
