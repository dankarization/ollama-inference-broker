import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from broker.policy import SourcePolicyError, normalize_source_policy
from broker.retention_migration import (
    RetentionMigrationError,
    build_floor_database,
    build_repacked_database,
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
        with self.assertRaisesRegex(SourcePolicyError, "retention requires"):
            normalize_source_policy({
                "sources": {"producer": {"retention_enabled": True}},
            })

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

    def test_unacked_terminal_is_reported_and_never_blindly_compacted(self):
        job = self._admit("unacked")
        self._ack_input(job)
        self.assertTrue(self.broker.dispatch_once(frozenset({"producer"})))
        self.clock.value = 106
        self.broker.storage.maybe_maintain(force=True)
        self.assertIn("result", self.broker.status(job["id"]))
        self.assertEqual(self.broker.storage_health()["overdue_unacked_terminal"], 1)

    def test_failed_retry_window_expires_only_with_durable_input(self):
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

    def test_inline_budget_blocks_only_opted_in_new_admissions(self):
        self._write_policy(inline_budget_bytes=1)
        with self.assertRaisesRegex(StorageContractError, "budget is exhausted"):
            self._admit("too-large")
        self.assertEqual(
            self.broker.db.execute("SELECT count(*) FROM jobs").fetchone()[0], 0,
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


if __name__ == "__main__":
    unittest.main()
