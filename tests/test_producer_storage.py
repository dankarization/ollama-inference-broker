import json
import io
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from broker.http import serve
from broker.service import Broker, SourcePolicy
from broker.storage import (
    ReceiptConflict,
    StorageContractError,
    content_evidence,
    migrate_storage_schema,
    storage_schema_report,
)
from broker.storage_policy import main as storage_policy_main, stage_storage_policy


class FakeOllama:
    def ps(self):
        return {"models": [{"name": "nemotron3:33b", "size_vram": 1}]}

    def is_ready(self, _model):
        return True

    def unload(self, _model):
        raise AssertionError("no model switch expected")

    def run(self, _kind, request):
        return {"done": True, "prompt": request.get("prompt")}


class FakeWol:
    def wake(self):
        pass


class ProducerStorageTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.policy_path = self.root / "sources.json"
        self._write_policy()
        self.policy = SourcePolicy(self.policy_path)
        self.broker = Broker(self.root / "broker.sqlite3", FakeOllama(), FakeWol())
        self.broker.use_source_policy(self.policy)
        self.addCleanup(self.broker.db.close)

    def _write_policy(self, **overrides):
        source = {
            "enabled": True,
            "admission_allowed": True,
            "weight": 4,
            "producer_storage_enabled": True,
            "producer_storage_mode": "producer_owned",
            "ack_required": False,
            "compaction_enabled": False,
            "legacy_result_fallback": True,
            "compaction_grace_seconds": 0,
            "quarantine_grace_seconds": 0,
            **overrides,
        }
        self.policy_path.write_text(json.dumps({
            "version": 1,
            "sources": {"producer": source},
        }), encoding="utf-8")

    def _admit(
        self, external_id="external-1", producer_attempt_id="attempt-1", input_ref=None,
    ):
        payload = {"prompt": external_id}
        input_hash, input_bytes = content_evidence(payload)
        return self.broker.submit(
            "interactive", "generate", payload, source="producer",
            source_item_id=external_id, external_id=external_id,
            producer_storage={
                "producer_attempt_id": producer_attempt_id,
                "input": {
                    "mode": "producer_owned",
                    "storage_ref": input_ref or f"producer://inputs/{external_id}",
                    "content_hash": input_hash,
                    "byte_size": input_bytes,
                },
                "result": {
                    "mode": "producer_owned",
                    "schema_version": "result-v1",
                },
            },
        )

    def _complete(self, job):
        self.assertTrue(self.broker.dispatch_once(frozenset({"producer"})))
        status = self.broker.compact_status(job["id"])
        self.assertEqual(status["state"], "completed")
        return status

    def _ack_input(self, job):
        status = self.broker.compact_status(job["id"])
        return self.broker.acknowledge_input(job["id"], {
            "job_id": job["id"],
            "producer": "producer",
            "producer_attempt_id": status["producer_attempt_id"],
            "storage_ref": status["input_ref"],
            "input_hash": status["input_hash"],
            "input_bytes": status["input_bytes"],
            "persisted_at": "2026-09-05T03:59:00Z",
        })

    @staticmethod
    def _ack(job, status, **overrides):
        return {
            "job_id": job["id"],
            "producer": "producer",
            "producer_attempt_id": "attempt-1",
            "storage_ref": f"producer://results/{job['id']}",
            "result_hash": status["result_hash"],
            "result_bytes": status["result_bytes"],
            "schema_version": "result-v1",
            "persisted_at": "2026-09-05T04:00:00Z",
            **overrides,
        }

    def test_additive_migration_preserves_inline_values_and_report_is_payload_free(self):
        database = self.root / "production-schema-copy.sqlite3"
        db = sqlite3.connect(database)
        db.execute("""CREATE TABLE jobs (
            id TEXT PRIMARY KEY, source TEXT NOT NULL, payload TEXT NOT NULL,
            state TEXT NOT NULL, result_json TEXT)""")
        db.execute(
            "INSERT INTO jobs VALUES(?,?,?,?,?)",
            ("one", "legacy", '"PRIVATE-PAYLOAD-SENTINEL"', "completed",
             '"PRIVATE-RESULT-SENTINEL"'),
        )
        with db:
            first = migrate_storage_schema(db)
            second = migrate_storage_schema(db)
        self.assertIn("producer_attempt_id", first["added_columns"])
        self.assertEqual(second["added_columns"], [])
        self.assertEqual(
            db.execute("SELECT payload,result_json FROM jobs WHERE id='one'").fetchone(),
            ('"PRIVATE-PAYLOAD-SENTINEL"', '"PRIVATE-RESULT-SENTINEL"'),
        )
        report = storage_schema_report(db, str(database))
        encoded = json.dumps(report)
        self.assertNotIn("PRIVATE-PAYLOAD-SENTINEL", encoded)
        self.assertNotIn("PRIVATE-RESULT-SENTINEL", encoded)
        self.assertFalse(report["report_contains_payloads"])
        self.assertEqual(report["quick_check"], "ok")
        db.close()

    def test_legacy_callers_still_admit_dispatch_and_fetch_full_result(self):
        job = self.broker.submit(
            "interactive", "generate", {"prompt": "legacy"}, source="producer",
        )
        self.assertTrue(self.broker.dispatch_once(frozenset({"producer"})))
        fetched = self.broker.status(job["id"])
        self.assertEqual(fetched["state"], "completed")
        self.assertEqual(fetched["payload"], {"prompt": "legacy"})
        self.assertEqual(fetched["result"]["done"], True)
        self.assertNotIn("result_storage_mode", fetched)
        self.assertNotIn("result_ref", fetched)
        self.assertNotIn("input_ref", fetched)

        producer_job = self._admit(external_id="protected-storage-metadata")
        producer_fetched = self.broker.status(producer_job["id"])
        self.assertNotIn("producer_attempt_id", producer_fetched)
        self.assertNotIn("input_hash", producer_fetched)
        self.assertNotIn("input_ref", producer_fetched)
        self.assertIn("producer_attempt_id", self.broker.compact_status(producer_job["id"]))

    def test_ack_validates_identity_is_idempotent_and_preserves_conflicts(self):
        job = self._admit()
        status = self._complete(job)
        body = self._ack(job, status)
        receipt = self.broker.acknowledge_result(job["id"], body)
        self.assertEqual(self.broker.acknowledge_result(job["id"], body), receipt)
        self.assertEqual(self.broker.compact_status(job["id"])["delivery_state"], "acked")

        cases = (
            {"job_id": "different-job"},
            {"producer": "different-source"},
            {"producer_attempt_id": "different-attempt"},
            {"result_hash": "sha256:" + "0" * 64},
            {"storage_ref": "producer://results/conflicting"},
        )
        for override in cases:
            with self.subTest(override=override), self.assertRaises(ReceiptConflict):
                self.broker.acknowledge_result(job["id"], self._ack(job, status, **override))
            self.assertEqual(self.broker.status(job["id"])["result"]["done"], True)
            self.assertEqual(
                self.broker.receipt(job["id"])["receipt"]["receipt_id"],
                receipt["receipt_id"],
            )
        self.assertEqual(len(self.broker.receipt(job["id"])["conflicts"]), len(cases))
        self.assertEqual(self.broker.acknowledge_result(job["id"], body), receipt)
        self.assertEqual(self.broker.compact_status(job["id"])["delivery_state"], "conflict")
        self.assertEqual(self.broker.storage.reconcile(), 0)
        self.assertEqual(self.broker.compact_status(job["id"])["delivery_state"], "conflict")

    def test_input_receipt_validates_declared_artifact(self):
        job = self._admit()
        status = self.broker.compact_status(job["id"])
        body = {
            "job_id": job["id"],
            "producer": "producer",
            "producer_attempt_id": "attempt-1",
            "storage_ref": status["input_ref"],
            "input_hash": status["input_hash"],
            "input_bytes": status["input_bytes"],
            "persisted_at": "2026-09-05T04:00:00Z",
        }
        receipt = self.broker.acknowledge_input(job["id"], body)
        self.assertEqual(receipt["input_hash"], status["input_hash"])
        self.assertEqual(self.broker.acknowledge_input(job["id"], body), receipt)
        artifact = next(
            item for item in self.broker.compact_status(job["id"])["artifacts"]
            if item["role"] == "input"
        )
        self.assertEqual(artifact["state"], "acked")
        with self.assertRaises(ReceiptConflict):
            self.broker.acknowledge_input(
                job["id"], {**body, "input_hash": "sha256:" + "0" * 64},
            )

    def test_exact_input_receipt_retry_survives_policy_rollback(self):
        job = self._admit()
        receipt = self._ack_input(job)
        self._write_policy(producer_storage_enabled=False)
        self.assertEqual(self._ack_input(job), receipt)
        self.assertEqual(self.broker.receipt(job["id"])["conflicts"], [])

    def test_idempotent_admission_rejects_conflicting_storage_identity(self):
        first = self._admit()
        repeat = self._admit()
        self.assertEqual(repeat["id"], first["id"])
        with self.assertRaisesRegex(ValueError, "idempotency conflict"):
            self._admit(producer_attempt_id="attempt-2")
        with self.assertRaisesRegex(ValueError, "idempotency conflict"):
            self._admit(input_ref="producer://inputs/changed")
        self.assertEqual(
            self.broker.compact_status(first["id"])["producer_attempt_id"], "attempt-1",
        )

    def test_cleanup_requires_all_source_and_receipt_guards(self):
        self._write_policy(ack_required=True, legacy_result_fallback=False)
        job = self._admit()
        status = self._complete(job)
        self._ack_input(job)
        self.broker.acknowledge_result(job["id"], self._ack(job, status))
        unacked = self._admit(external_id="unacked")
        self._complete(unacked)
        input_unacked = self._admit(external_id="input-unacked")
        input_unacked_status = self._complete(input_unacked)
        self.broker.acknowledge_result(
            input_unacked["id"], self._ack(input_unacked, input_unacked_status),
        )
        running = self._admit(external_id="running")
        cancel_requested = self._admit(external_id="cancel-requested")
        with self.broker.db:
            self.broker.db.execute(
                "UPDATE jobs SET state='running' WHERE id=?", (running["id"],),
            )
            self.broker.db.execute(
                "UPDATE jobs SET state='cancel_requested' WHERE id=?",
                (cancel_requested["id"],),
            )
        with self.assertRaises(StorageContractError):
            self.broker.storage_maintenance("producer")

        self._write_policy(
            ack_required=True, compaction_enabled=True, legacy_result_fallback=False,
        )
        tiny_budget = self.broker.storage_maintenance("producer", max_bytes=1)
        self.assertEqual(tiny_budget["candidate_count"], 0)
        self.assertEqual(tiny_budget["skipped_oversize"], 1)
        preview = self.broker.storage_maintenance("producer")
        self.assertEqual(preview["job_ids"], [job["id"]])
        quarantined = self.broker.storage_maintenance(
            "producer", operation="quarantine", confirm=True,
        )
        self.assertEqual(quarantined["changed"], 1)
        compacted = self.broker.storage_maintenance(
            "producer", operation="compact", confirm=True,
        )
        self.assertEqual(compacted["changed"], 1)
        self.assertEqual(self.broker.status(job["id"])["payload"], {})
        self.assertNotIn("result", self.broker.status(job["id"]))
        self.assertIn("result", self.broker.status(unacked["id"]))
        self.assertIn("result", self.broker.status(input_unacked["id"]))
        self.assertNotEqual(self.broker.status(running["id"])["payload"], {})
        self.assertNotEqual(self.broker.status(cancel_requested["id"])["payload"], {})
        self.assertFalse(compacted["vacuum_performed"])

    def test_cleanup_does_not_adopt_jobs_with_legacy_admission_guards(self):
        jobs = []
        for external_id, flags in (
            ("ack-optional", {"ack_required": False, "legacy_result_fallback": False}),
            ("legacy-fallback", {"ack_required": True, "legacy_result_fallback": True}),
        ):
            self._write_policy(**flags)
            job = self._admit(external_id=external_id)
            status = self._complete(job)
            self._ack_input(job)
            self.broker.acknowledge_result(job["id"], self._ack(job, status))
            jobs.append(job["id"])
        self._write_policy(
            ack_required=True, compaction_enabled=True, legacy_result_fallback=False,
        )
        preview = self.broker.storage_maintenance("producer")
        self.assertEqual(preview["job_ids"], [])
        for job_id in jobs:
            self.assertEqual(self.broker.compact_status(job_id)["compaction_state"], "full")

    def test_storage_mutations_require_literal_boolean_confirmation(self):
        self._write_policy(
            ack_required=True, compaction_enabled=True, legacy_result_fallback=False,
        )
        for operation in ("quarantine", "compact"):
            for confirm in ("true", "false", 1):
                with self.subTest(operation=operation, confirm=confirm), self.assertRaisesRegex(
                    StorageContractError, "confirm=true",
                ):
                    self.broker.storage_maintenance(
                        "producer", operation=operation, confirm=confirm,
                    )

    def test_receipt_repairs_derived_state_after_restart(self):
        job = self._admit()
        status = self._complete(job)
        receipt = self.broker.acknowledge_result(job["id"], self._ack(job, status))
        with self.broker.db:
            self.broker.db.execute(
                "UPDATE jobs SET delivery_state='conflict',result_ref=NULL,acked_at=NULL "
                "WHERE id=?", (job["id"],),
            )
        self.broker.db.close()
        restarted = Broker(self.root / "broker.sqlite3", FakeOllama(), FakeWol())
        self.addCleanup(restarted.db.close)
        self.assertEqual(restarted.compact_status(job["id"])["delivery_state"], "acked")
        self.assertEqual(
            restarted.receipt(job["id"])["receipt"]["receipt_id"], receipt["receipt_id"],
        )

    def test_full_receipt_reconciliation_runs_once_at_startup(self):
        database = self.root / "startup-reconcile.sqlite3"
        with patch("broker.service.StorageManager.reconcile", autospec=True, return_value=0) as reconcile:
            broker = Broker(database, FakeOllama(), FakeWol())
            self.addCleanup(broker.db.close)
            broker.recover()
            broker.recover()
        self.assertEqual(reconcile.call_count, 1)

    def test_wal_policy_is_bounded_and_passive(self):
        self.assertEqual(self.broker.db.execute("PRAGMA wal_autocheckpoint").fetchone()[0], 4096)
        self.assertEqual(
            self.broker.db.execute("PRAGMA journal_size_limit").fetchone()[0],
            64 * 1024 * 1024,
        )
        checkpoint = self.broker.storage.maybe_checkpoint(force=True)
        self.assertEqual(checkpoint["mode"], "PASSIVE")
        wal = self.broker.storage_health()["wal"]
        self.assertFalse(wal["truncate_enabled"])
        self.assertFalse(wal["vacuum_enabled"])
        self.assertEqual(wal["last_checkpoint"], checkpoint)
        self.assertEqual(wal["effective_wal_autocheckpoint_pages"], 4096)
        self.assertEqual(wal["effective_journal_size_limit_bytes"], 64 * 1024 * 1024)

    def test_storage_policy_staging_preserves_live_source_controls(self):
        live = {
            "version": 1,
            "sources": {
                "olya-vision": {
                    "enabled": True, "weight": 8, "admission_allowed": False,
                },
                "shutterstock-video": {"enabled": False, "weight": 10},
                "syncopia-telegram-memory": {"enabled": True, "weight": 1},
                "uncensored-eval": {"enabled": True, "weight": 1},
            },
        }
        staged, report = stage_storage_policy(live)
        self.assertTrue(report["scheduler_controls_unchanged"])
        for source in live["sources"]:
            for field in ("enabled", "weight"):
                self.assertEqual(staged["sources"][source][field], live["sources"][source][field])
        self.assertFalse(staged["sources"]["olya-vision"]["admission_allowed"])
        self.assertTrue(staged["sources"]["olya-vision"]["producer_storage_enabled"])
        self.assertNotIn("producer_storage_enabled", staged["sources"]["uncensored-eval"])
        self.assertFalse(staged["sources"]["olya-vision"]["compaction_enabled"])

    def test_storage_policy_rejects_output_aliasing_live_policy(self):
        live = self.root / "live-sources.json"
        original = json.dumps({
            "version": 1,
            "sources": {"olya-vision": {"enabled": True, "weight": 8}},
        })
        live.write_text(original, encoding="utf-8")
        with (
            patch("sys.argv", [
                "storage-policy", "--policy", str(live), "--output", str(live),
            ]),
            patch("sys.stderr", new_callable=io.StringIO),
            self.assertRaises(SystemExit) as caught,
        ):
            storage_policy_main()
        self.assertEqual(caught.exception.code, 2)
        self.assertEqual(live.read_text(encoding="utf-8"), original)

    def test_storage_health_excludes_non_ack_capable_legacy_results(self):
        legacy = self.broker.submit(
            "interactive", "generate", {"prompt": "legacy-health"}, source="producer",
        )
        self.assertTrue(self.broker.dispatch_once(frozenset({"producer"})))
        self.assertEqual(self.broker.status(legacy["id"])["state"], "completed")
        producer = self._admit(external_id="producer-health")
        self._complete(producer)
        self.assertEqual(self.broker.storage_health()["unacked_terminal"], 1)

    def test_http_compact_status_ack_receipt_and_storage_health(self):
        job = self._admit()
        status = self._complete(job)
        token = "test-storage-token-with-at-least-32-characters"
        authorization = {"Authorization": f"Bearer {token}"}
        server = serve(
            self.broker, port=0, policy=self.policy, storage_token=token,
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        base = f"http://127.0.0.1:{server.server_port}"
        with urlopen(Request(
            f"{base}/v1/jobs/{job['id']}/status", headers=authorization,
        )) as response:
            compact = json.loads(response.read())
        self.assertNotIn("payload", compact)
        self.assertNotIn("result", compact)
        request = Request(
            f"{base}/v1/jobs/{job['id']}/ack",
            data=json.dumps(self._ack(job, status)).encode(),
            headers={"Content-Type": "application/json", **authorization}, method="POST",
        )
        with urlopen(request) as response:
            receipt = json.loads(response.read())
        with urlopen(Request(
            f"{base}/v1/jobs/{job['id']}/receipt", headers=authorization,
        )) as response:
            recovered = json.loads(response.read())
        self.assertEqual(recovered["receipt"]["receipt_id"], receipt["receipt_id"])
        with urlopen(f"{base}/v1/storage/health") as response:
            health = json.loads(response.read())
        self.assertEqual(health["receipts"], 1)

        conflict = Request(
            f"{base}/v1/jobs/{job['id']}/ack",
            data=json.dumps(self._ack(
                job, status, result_hash="sha256:" + "0" * 64,
            )).encode(),
            headers={"Content-Type": "application/json", **authorization}, method="POST",
        )
        with self.assertRaises(HTTPError) as caught:
            urlopen(conflict)
        self.assertEqual(caught.exception.code, 409)

    def test_storage_mutations_require_configured_token(self):
        job = self._admit()
        status = self._complete(job)
        server = serve(
            self.broker, port=0, policy=self.policy, storage_token="x" * 32,
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        request = Request(
            f"http://127.0.0.1:{server.server_port}/v1/jobs/{job['id']}/ack",
            data=json.dumps(self._ack(job, status)).encode(),
            headers={"Content-Type": "application/json"}, method="POST",
        )
        with self.assertRaises(HTTPError) as caught:
            urlopen(request)
        self.assertEqual(caught.exception.code, 401)
        self.assertIsNone(self.broker.receipt(job["id"])["receipt"])


if __name__ == "__main__":
    unittest.main()
