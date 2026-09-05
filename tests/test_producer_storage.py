import json
import io
import hashlib
import os
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from broker.http import serve
from broker.migration import main as migration_main
from broker.policy import SourcePolicyError, normalize_source_policy
from broker.rollback_guard import (
    LEGACY_HIDDEN_STORAGE_FIELDS,
    _fsync_tree,
    prepare_rollback_release,
)
from broker.service import Broker, SourcePolicy
from broker.storage import (
    ReceiptConflict,
    StorageContractError,
    content_evidence,
    migrate_storage_schema,
    storage_schema_report,
)
from broker.storage_policy import main as storage_policy_main, stage_storage_policy
from broker.storage_policy import read_snapshot, write_atomic


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
        profile="interactive", kind="generate", source="producer",
    ):
        payload = {"prompt": external_id}
        input_hash, input_bytes = content_evidence(payload)
        return self.broker.submit(
            profile, kind, payload, source=source,
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
        index_sql = db.execute(
            "SELECT sql FROM sqlite_master WHERE type='index' "
            "AND name='jobs_storage_compaction'"
        ).fetchone()[0]
        self.assertIn("WHERE delivery_state='acked' AND ack_required=1", index_sql)
        self.assertIn("legacy_result_fallback=0", index_sql)
        db.close()

    def test_migration_rejects_report_aliasing_database(self):
        database = self.root / "migration.sqlite3"
        original = b"not-opened-because-alias-is-rejected"
        database.write_bytes(original)
        aliases = [database, self.root / "migration-report-hardlink.json"]
        os.link(database, aliases[1])
        for report in aliases:
            with (
                self.subTest(report=report),
                patch("sys.argv", [
                    "migration", "--database", str(database), "--report", str(report),
                ]),
                patch("sys.stderr", new_callable=io.StringIO),
                self.assertRaises(SystemExit) as caught,
            ):
                migration_main()
            self.assertEqual(caught.exception.code, 2)
            self.assertEqual(database.read_bytes(), original)

    def test_rollback_release_hides_additive_metadata(self):
        source = self.root / "parent-release"
        service = source / "broker" / "service.py"
        service.parent.mkdir(parents=True)
        service.write_text(
            'import json\nimport logging\n\n'
            'LOGGER = logging.getLogger("ollama_inference_broker.audit")\n\n'
            'class Broker:\n'
            '    def _job(self, row):\n'
            '        data = dict(row)\n'
            '        data.pop("priority", None)  # inert historic column is never public\n'
            '        data["payload"] = json.loads(data["payload"])\n'
            '        if data.get("result_json") is not None:\n'
            '            data["result"] = json.loads(data.pop("result_json"))\n'
            '        else:\n'
            '            data.pop("result_json", None)\n'
            '        if data["state"] == "queued":\n'
            '            data["queue_position"] = self._position(row)\n'
            '        return data\n',
            encoding="utf-8",
        )
        source_hash = hashlib.sha256(service.read_bytes()).hexdigest()
        descendant = source / "rollback-protected"
        with self.assertRaisesRegex(ValueError, "outside source release"):
            prepare_rollback_release(
                source, descendant, expected_service_sha256=source_hash,
            )
        self.assertFalse(descendant.exists())
        output = self.root / "rollback-protected"
        with patch(
            "broker.rollback_guard._fsync_tree", wraps=_fsync_tree,
        ) as fsync_tree:
            report = prepare_rollback_release(
                source, output, expected_service_sha256=source_hash,
            )
        self.assertEqual(fsync_tree.call_count, 1)
        self.assertEqual(fsync_tree.call_args.args[0].name, "release")
        namespace = {}
        exec((output / "broker" / "service.py").read_text(encoding="utf-8"), namespace)
        row = {
            "id": "legacy", "state": "completed", "priority": 9,
            "payload": '{}', "result_json": '{"done":true}',
            **{
                field: f"private-{field}"
                for field in LEGACY_HIDDEN_STORAGE_FIELDS
            },
        }
        response = namespace["Broker"]()._job(row)
        self.assertEqual(response["result"], {"done": True})
        self.assertTrue(LEGACY_HIDDEN_STORAGE_FIELDS.isdisjoint(response))
        self.assertEqual(
            report["hidden_storage_fields"], len(LEGACY_HIDDEN_STORAGE_FIELDS),
        )
        self.assertFalse(report["payloads_in_report"])

    def test_policy_rejects_non_finite_storage_grace_periods(self):
        for field in ("compaction_grace_seconds", "quarantine_grace_seconds"):
            for value in (float("nan"), float("inf"), float("-inf"), 10 ** 1000):
                raw = {
                    "sources": {"producer": {field: value}},
                }
                with self.subTest(field=field, value=value), self.assertRaisesRegex(
                    SourcePolicyError, "finite non-negative",
                ):
                    normalize_source_policy(raw)
        last_known_good = self.policy.storage_config("producer")
        self._write_policy(compaction_grace_seconds=10 ** 1000)
        self.assertEqual(self.policy.storage_config("producer"), last_known_good)

    def test_non_string_storage_mode_is_rejected_and_hot_reload_keeps_policy(self):
        for value in ([], {}):
            with self.subTest(value=value), self.assertRaisesRegex(
                SourcePolicyError, "producer_storage_mode must be",
            ):
                normalize_source_policy({
                    "sources": {"producer": {"producer_storage_mode": value}},
                })
        self.assertEqual(
            self.policy.storage_config("producer")["producer_storage_mode"],
            "producer_owned",
        )
        self._write_policy(producer_storage_mode=[])
        self.assertEqual(
            self.policy.storage_config("producer")["producer_storage_mode"],
            "producer_owned",
        )

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
        attempts = self.broker.compact_status(job["id"])["delivery_attempt_count"]
        audit_count = self.broker.db.execute(
            "SELECT count(*) FROM audit_events "
            "WHERE job_id=? AND event_type='delivery.ack_conflict'",
            (job["id"],),
        ).fetchone()[0]
        with self.assertRaises(ReceiptConflict):
            self.broker.acknowledge_result(
                job["id"], self._ack(job, status, **cases[-1]),
            )
        self.assertEqual(len(self.broker.receipt(job["id"])["conflicts"]), len(cases))
        self.assertEqual(
            self.broker.compact_status(job["id"])["delivery_attempt_count"], attempts,
        )
        self.assertEqual(
            self.broker.db.execute(
                "SELECT count(*) FROM audit_events "
                "WHERE job_id=? AND event_type='delivery.ack_conflict'",
                (job["id"],),
            ).fetchone()[0],
            audit_count,
        )
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
        with self.assertRaisesRegex(ValueError, "idempotency conflict"):
            self._admit(profile="cron")
        with self.assertRaisesRegex(ValueError, "idempotency conflict"):
            self._admit(kind="chat")
        self.assertEqual(
            self.broker.compact_status(first["id"])["producer_attempt_id"], "attempt-1",
        )

    def test_producer_storage_retry_cannot_drop_capability_or_bypass_auth(self):
        external_id = "protected-retry"
        policy = json.loads(self.policy_path.read_text(encoding="utf-8"))
        policy["sources"]["olya-vision"] = dict(policy["sources"]["producer"])
        self.policy_path.write_text(json.dumps(policy), encoding="utf-8")
        job = self._admit(external_id=external_id, source="olya-vision")
        self.assertTrue(self.broker.dispatch_once(frozenset({"olya-vision"})))
        self.assertEqual(self.broker.status(job["id"])["state"], "completed")
        server = serve(
            self.broker, port=0, policy=self.policy,
            storage_token="test-storage-token-with-at-least-32-characters",
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        request = Request(
            f"http://127.0.0.1:{server.server_port}/v1/jobs",
            data=json.dumps({
                "profile": "interactive",
                "kind": "generate",
                "payload": {"prompt": external_id},
                "source": "olya-vision",
                "source_item_id": external_id,
                "external_id": external_id,
            }).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with self.assertRaises(HTTPError) as caught:
            urlopen(request)
        self.assertEqual(caught.exception.code, 400)
        response = json.loads(caught.exception.read())
        self.assertIn("producer storage idempotency conflict", response["error"])
        self.assertNotIn(job["id"], json.dumps(response))
        self.assertNotIn(external_id, json.dumps(response))

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

    def test_cleanup_preserves_broker_temporary_inputs_without_durable_copy(self):
        self._write_policy(ack_required=True, legacy_result_fallback=False)
        jobs = []
        for external_id, explicit_input in (
            ("omitted-temporary-input", False),
            ("explicit-temporary-input", True),
        ):
            payload = {"prompt": external_id}
            capability = {
                "producer_attempt_id": "attempt-1",
                "result": {"mode": "producer_owned", "schema_version": "result-v1"},
            }
            if explicit_input:
                capability["input"] = {"mode": "broker_temporary"}
            job = self.broker.submit(
                "interactive", "generate", payload, source="producer",
                source_item_id=external_id, external_id=external_id,
                producer_storage=capability,
            )
            status = self._complete(job)
            self.broker.acknowledge_result(job["id"], self._ack(job, status))
            jobs.append((job, payload))
        self._write_policy(
            ack_required=True, compaction_enabled=True, legacy_result_fallback=False,
        )
        preview = self.broker.storage_maintenance("producer")
        self.assertEqual(preview["job_ids"], [])
        for job, payload in jobs:
            self.assertEqual(self.broker.status(job["id"])["payload"], payload)
            self.assertIn("result", self.broker.status(job["id"]))
            self.assertEqual(
                self.broker.compact_status(job["id"])["compaction_state"], "full",
            )

    def test_oversized_compaction_candidate_does_not_starve_later_jobs(self):
        self._write_policy(
            ack_required=True, compaction_enabled=True, legacy_result_fallback=False,
        )
        jobs = []
        for external_id in ("older-oversized", "later-small"):
            job = self._admit(external_id=external_id)
            status = self._complete(job)
            self._ack_input(job)
            self.broker.acknowledge_result(job["id"], self._ack(job, status))
            jobs.append(job)
        with self.broker.db:
            self.broker.db.execute(
                "UPDATE jobs SET payload=?,compaction_after=1 WHERE id=?",
                (json.dumps({"prompt": "x" * 2048}), jobs[0]["id"]),
            )
            self.broker.db.execute(
                "UPDATE jobs SET compaction_after=2 WHERE id=?", (jobs[1]["id"],),
            )
        preview = self.broker.storage_maintenance("producer", max_bytes=1024)
        self.assertEqual(preview["job_ids"], [jobs[1]["id"]])
        self.assertEqual(preview["skipped_oversize"], 1)

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

    def test_storage_policy_apply_preserves_concurrent_source_update(self):
        live = self.root / "live-concurrent-sources.json"
        original = {
            "version": 1,
            "sources": {"olya-vision": {"enabled": True, "weight": 8}},
        }
        concurrent = {
            "version": 1,
            "sources": {
                "olya-vision": {
                    "enabled": True, "weight": 8, "admission_allowed": False,
                },
            },
        }
        live.write_text(json.dumps(original), encoding="utf-8")
        real_stage = stage_storage_policy

        def stage_then_update(raw):
            staged = real_stage(raw)
            live.write_text(json.dumps(concurrent), encoding="utf-8")
            return staged

        with (
            patch("sys.argv", ["storage-policy", "--policy", str(live), "--apply"]),
            patch("broker.storage_policy.stage_storage_policy", side_effect=stage_then_update),
            patch("sys.stderr", new_callable=io.StringIO),
            self.assertRaises(SystemExit) as caught,
        ):
            storage_policy_main()
        self.assertEqual(caught.exception.code, 2)
        self.assertEqual(json.loads(live.read_text(encoding="utf-8")), concurrent)

    def test_storage_policy_apply_serializes_with_runtime_writer(self):
        live = self.root / "live-serialized-sources.json"
        original = {
            "version": 1,
            "sources": {"olya-vision": {"enabled": True, "weight": 8}},
        }
        live.write_text(json.dumps(original), encoding="utf-8")
        encoded, fingerprint = read_snapshot(live)
        staged, _ = stage_storage_policy(json.loads(encoded))
        policy = SourcePolicy(live)
        checked = threading.Event()
        release_check = threading.Event()
        writer_started = threading.Event()
        writer_finished = threading.Event()
        errors = []
        real_read_snapshot = read_snapshot

        def gated_read_snapshot(path):
            snapshot = real_read_snapshot(path)
            checked.set()
            if not release_check.wait(2):
                raise AssertionError("test did not release policy fingerprint check")
            return snapshot

        def cli_writer():
            try:
                write_atomic(live, staged, expected_fingerprint=fingerprint)
            except Exception as error:  # pragma: no cover - asserted below
                errors.append(error)

        def runtime_writer():
            writer_started.set()
            try:
                policy.set_admission_allowed("olya-vision", False)
            except Exception as error:  # pragma: no cover - asserted below
                errors.append(error)
            finally:
                writer_finished.set()

        with patch(
            "broker.storage_policy.read_snapshot", side_effect=gated_read_snapshot,
        ):
            cli_thread = threading.Thread(target=cli_writer)
            cli_thread.start()
            self.assertTrue(checked.wait(2))
            runtime_thread = threading.Thread(target=runtime_writer)
            runtime_thread.start()
            self.assertTrue(writer_started.wait(2))
            self.assertFalse(writer_finished.wait(0.1))
            release_check.set()
            cli_thread.join(2)
            runtime_thread.join(2)
        self.assertFalse(cli_thread.is_alive())
        self.assertFalse(runtime_thread.is_alive())
        self.assertEqual(errors, [])
        persisted = json.loads(live.read_text(encoding="utf-8"))
        self.assertTrue(persisted["sources"]["olya-vision"]["producer_storage_enabled"])
        self.assertFalse(persisted["sources"]["olya-vision"]["admission_allowed"])

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

    def test_malformed_storage_refs_return_json_400(self):
        job = self._admit(external_id="malformed-storage-ref")
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
        input_status = self.broker.compact_status(job["id"])
        cases = (
            ("input-received", {
                "job_id": job["id"],
                "producer": "producer",
                "producer_attempt_id": "attempt-1",
                "storage_ref": "http://[broken",
                "input_hash": input_status["input_hash"],
                "input_bytes": input_status["input_bytes"],
                "persisted_at": "2026-09-05T03:59:00Z",
            }),
            ("ack", self._ack(job, status, storage_ref="http://[broken")),
        )
        for suffix, body in cases:
            request = Request(
                f"{base}/v1/jobs/{job['id']}/{suffix}",
                data=json.dumps(body).encode(),
                headers={"Content-Type": "application/json", **authorization},
                method="POST",
            )
            with self.subTest(suffix=suffix), self.assertRaises(HTTPError) as caught:
                urlopen(request)
            self.assertEqual(caught.exception.code, 400)
            response = json.loads(caught.exception.read())
            self.assertIn("storage_ref", response["error"])

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
