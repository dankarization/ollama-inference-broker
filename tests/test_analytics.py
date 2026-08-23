import io
import json
import logging
import os
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from urllib.request import urlopen

from broker.http import serve
from broker.service import Broker, SourcePolicy


class FakeOllama:
    def __init__(self, fail_once=False):
        self.fail_once = fail_once

    def ps(self):
        return {"models": [{"name": "nemotron3:33b", "size_vram": 1}]}

    def is_ready(self, _model):
        return True

    def unload(self, _model):
        pass

    def run(self, _kind, _payload):
        if self.fail_once:
            self.fail_once = False
            raise RuntimeError("synthetic inference failure")
        return {"done": True}


class FakeWol:
    def wake(self):
        pass


class MutableClock:
    def __init__(self, value):
        self.value = value

    def __call__(self):
        return self.value


class AnalyticsTests(unittest.TestCase):
    def make(self, clock=None, fail_once=False, path=None):
        self.temp = tempfile.NamedTemporaryFile() if path is None else None
        return Broker(
            path or self.temp.name,
            FakeOllama(fail_once=fail_once),
            FakeWol(),
            clock=clock or MutableClock(1_000),
        )

    def policy(self, sources):
        fd, path = tempfile.mkstemp(suffix=".json")
        with os.fdopen(fd, "w") as handle:
            json.dump({"version": 1, "sources": sources}, handle)
        self.addCleanup(lambda: Path(path).unlink(missing_ok=True))
        return SourcePolicy(path)

    def test_additive_migration_preserves_legacy_job_and_api_shape(self):
        fd, path = tempfile.mkstemp(suffix=".sqlite3")
        os.close(fd)
        self.addCleanup(lambda: Path(path).unlink(missing_ok=True))
        db = sqlite3.connect(path)
        db.execute("""CREATE TABLE jobs (
            id TEXT PRIMARY KEY, profile TEXT NOT NULL, kind TEXT NOT NULL,
            payload TEXT NOT NULL, state TEXT NOT NULL, created REAL NOT NULL,
            started REAL, finished REAL, lease_until REAL, error TEXT,
            switch_reason TEXT)""")
        db.execute(
            "INSERT INTO jobs(id,profile,kind,payload,state,created) "
            "VALUES('legacy-job','interactive','generate','{\"prompt\":\"x\"}',"
            "'queued',10)"
        )
        db.commit()
        db.close()

        broker = self.make(path=path, clock=MutableClock(20))
        job = broker.status("legacy-job")
        self.assertEqual(job["state"], "queued")
        self.assertEqual(job["source"], "legacy")
        self.assertEqual(job["priority"], 10)
        self.assertEqual(job["queued_at"], 10)
        self.assertEqual(job["payload"], {"prompt": "x"})
        tables = {row[0] for row in broker.db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}
        self.assertIn("audit_events", tables)
        self.assertIn("job_attempts", tables)

    def test_attempt_retry_correlation_metrics_and_payload_safe_logs(self):
        clock = MutableClock(100)
        broker = self.make(clock=clock, fail_once=True)
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        logger = logging.getLogger("ollama_inference_broker.audit")
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        self.addCleanup(logger.removeHandler, handler)

        job = broker.submit(
            "batch-video", "generate", {"prompt": "private-payload-marker"},
            source="analytics-source", priority=7,
            source_item_id="item-42", external_id="external-7",
        )
        self.assertEqual(job["source_item_id"], "item-42")
        self.assertEqual(job["external_id"], "external-7")
        clock.value = 110
        broker.dispatch_once()
        self.assertEqual(broker.status(job["id"])["state"], "failed")
        clock.value = 120
        self.assertEqual(broker.retry(job["id"])["state"], "queued")
        clock.value = 130
        broker.dispatch_once()
        final = broker.status(job["id"])
        self.assertEqual(final["state"], "completed")
        self.assertEqual(final["attempt_count"], 2)
        self.assertEqual(final["retry_count"], 1)

        attempts = broker.attempts(job["id"])
        self.assertEqual([attempt["outcome"] for attempt in attempts], ["failed", "completed"])
        events = broker.audit_events(job_id=job["id"], limit=100)
        event_types = {event["event_type"] for event in events}
        self.assertTrue({
            "admission.accepted", "scheduler.selected", "job.running",
            "job.failed", "job.retry_requested", "job.requeued", "job.completed",
        }.issubset(event_types))
        snapshot = broker.analytics(windows=(60,))
        source = snapshot["sources"]["analytics-source"]["windows"]["60"]
        self.assertEqual(source["dispatched"], 2)
        self.assertEqual(source["retries"], 1)
        self.assertEqual(source["requeues"], 0)
        self.assertEqual(source["completed"], 1)
        self.assertEqual(source["failed"], 1)
        self.assertEqual(source["queue_wait"]["count"], 2)
        logs = stream.getvalue()
        self.assertNotIn("private-payload-marker", logs)
        self.assertNotIn("item-42", logs)
        self.assertNotIn("external-7", logs)

    def test_expired_lease_is_durably_requeued_across_restart(self):
        fd, path = tempfile.mkstemp(suffix=".sqlite3")
        os.close(fd)
        self.addCleanup(lambda: Path(path).unlink(missing_ok=True))
        clock = MutableClock(100)
        broker = self.make(path=path, clock=clock)
        job_id = broker.submit("interactive", "generate", {"prompt": "x"})["id"]
        with broker.db:
            broker.db.execute(
                "UPDATE jobs SET state='running',started=100,lease_until=105,"
                "attempt_count=1 WHERE id=?", (job_id,)
            )
            broker.db.execute(
                "INSERT INTO job_attempts(job_id,attempt_no,source,queued_at,"
                "selected_at,started,lease_until,scheduler_mode,selection_reason) "
                "VALUES(?,1,'interactive',100,100,100,105,'strict_priority_fifo','test')",
                (job_id,),
            )
        broker.db.close()

        clock.value = 200
        recovered = self.make(path=path, clock=clock)
        status = recovered.status(job_id)
        self.assertEqual(status["state"], "queued")
        self.assertEqual(status["requeue_count"], 1)
        self.assertEqual(recovered.attempts(job_id)[0]["outcome"], "lease_expired")
        types = [event["event_type"] for event in recovered.audit_events(job_id=job_id)]
        self.assertIn("lease.expired", types)
        self.assertIn("job.requeued", types)
        recovered.db.close()
        restarted = self.make(path=path, clock=clock)
        self.assertEqual(restarted.status(job_id)["requeue_count"], 1)

    def test_lease_renewal_updates_attempt_and_audit(self):
        clock = MutableClock(100)
        broker = self.make(clock=clock)
        job_id = broker.submit("interactive", "generate", {"prompt": "x"})["id"]
        with broker.db:
            broker.db.execute(
                "UPDATE jobs SET state='running',attempt_count=1,lease_until=120 WHERE id=?",
                (job_id,),
            )
            broker.db.execute(
                "INSERT INTO job_attempts(job_id,attempt_no,source,queued_at,"
                "selected_at,started,lease_until,scheduler_mode,selection_reason) "
                "VALUES(?,1,'interactive',100,100,100,120,'strict_priority_fifo','test')",
                (job_id,),
            )
        clock.value = 110
        self.assertEqual(broker.renew_lease(job_id, 30)["lease_until"], 140)
        self.assertEqual(broker.attempts(job_id)[0]["lease_until"], 140)
        self.assertEqual(broker.audit_events(job_id=job_id)[0]["event_type"], "lease.renewed")

    def test_cancel_is_audited_and_correlation_lookup_has_no_payload(self):
        broker = self.make()
        job = broker.submit(
            "batch-video", "generate", {"prompt": "private-value"},
            source="lookup-source", priority=7,
            source_item_id="item-a", external_id="external-a",
        )
        self.assertEqual(broker.cancel(job["id"])["state"], "cancelled")
        self.assertEqual(broker.audit_events(job_id=job["id"])[0]["event_type"], "job.cancelled")
        matches = broker.correlations(
            source="lookup-source", source_item_id="item-a"
        )
        self.assertEqual(matches[0]["job_id"], job["id"])
        self.assertNotIn("payload", matches[0])
        self.assertNotIn("result", matches[0])

    def test_weight_8_to_6_to_3_report_tracks_actual_and_expected_share(self):
        clock = MutableClock(1_000)
        broker = self.make(clock=clock)
        policy = self.policy({
            "olya-vision": {"enabled": True, "weight": 8},
            "olya-decision": {"enabled": True, "weight": 6},
            "shutterstock-video": {"enabled": True, "weight": 3},
        })
        for index in range(20):
            broker.submit(
                "olya-vision-gemma",
                "generate",
                {
                    "prompt": f"o-{index}",
                    "images": ["aGVsbG8="],
                    "format": {"type": "object"},
                },
                source="olya-vision",
                external_id=f"o-{index}",
            )
            broker.submit(
                "olya-decision-qwen38", "generate",
                {"prompt": f"d-{index}", "format": {"type": "object"}},
                source="olya-decision", external_id=f"d-{index}",
            )
            broker.submit("shutterstock-video", "generate", {"prompt": f"v-{index}"})
        for _ in range(17):
            self.assertTrue(broker.dispatch_once(policy=policy))
        snapshot = broker.analytics(policy, windows=(3_600,))
        scheduler = snapshot["scheduler"]["windows"]["3600"]
        self.assertEqual(scheduler["selections"], 17)
        self.assertEqual(scheduler["sources"]["olya-vision"]["selected"], 8)
        self.assertEqual(scheduler["sources"]["olya-decision"]["selected"], 6)
        self.assertEqual(scheduler["sources"]["shutterstock-video"]["selected"], 3)
        self.assertAlmostEqual(
            scheduler["sources"]["olya-vision"]["expected_share"], 8 / 17, places=5
        )
        self.assertAlmostEqual(
            scheduler["sources"]["olya-decision"]["expected_share"], 6 / 17, places=5
        )
        self.assertEqual(
            snapshot["scheduler"]["active_policy"]["sources"]["olya-vision"]["weight"],
            8.0,
        )

    def test_hot_weight_reload_remains_visible_without_resetting_history(self):
        broker = self.make()
        policy = self.policy({
            "olya": {"enabled": True, "weight": 8},
            "shutterstock-video": {"enabled": True, "weight": 3},
        })
        broker.submit("olya", "generate", {"prompt": "one"})
        broker.submit("shutterstock-video", "generate", {"prompt": "one"})
        broker.dispatch_once(policy=policy)
        replacement = str(policy.path) + ".tmp"
        with open(replacement, "w") as handle:
            json.dump({"version": 1, "sources": {
                "olya": {"enabled": True, "weight": 3},
                "shutterstock-video": {"enabled": True, "weight": 8},
            }}, handle)
        os.replace(replacement, policy.path)
        broker.dispatch_once(policy=policy)
        snapshot = broker.analytics(policy, windows=(3_600,))
        self.assertEqual(snapshot["scheduler"]["active_policy"]["sources"]["olya"]["weight"], 3.0)
        self.assertEqual(snapshot["scheduler"]["windows"]["3600"]["selections"], 2)

    def test_hot_policy_disable_ignores_stale_accumulator_source(self):
        broker = self.make()
        policy = self.policy({
            "interactive": {"enabled": True, "weight": 1},
            "shutterstock-video": {"enabled": True, "weight": 1},
        })
        first = broker.submit("interactive", "generate", {"prompt": "one"})["id"]
        second = broker.submit("interactive", "generate", {"prompt": "two"})["id"]
        broker.submit("shutterstock-video", "generate", {"prompt": "video"})
        self.assertTrue(broker.dispatch_once(policy.enabled_sources(), policy))
        self.assertEqual(
            {broker.status(first)["state"], broker.status(second)["state"]},
            {"completed", "queued"},
        )
        replacement = str(policy.path) + ".tmp"
        with open(replacement, "w") as handle:
            json.dump({"version": 1, "sources": {
                "interactive": {"enabled": True, "weight": 1},
                "shutterstock-video": {"enabled": False, "weight": 1},
            }}, handle)
        os.replace(replacement, policy.path)
        self.assertTrue(broker.dispatch_once(policy.enabled_sources(), policy))
        self.assertEqual(broker.status(first)["state"], "completed")
        self.assertEqual(broker.status(second)["state"], "completed")

    def test_semantically_invalid_hot_policy_keeps_last_known_good(self):
        policy = self.policy({"olya": {"enabled": True, "weight": 8}})
        self.assertEqual(policy.enabled_sources(), frozenset({"olya"}))
        replacement = str(policy.path) + ".tmp"
        with open(replacement, "w") as handle:
            json.dump({"version": 1, "sources": {
                "olya": {"enabled": True, "weight": 0},
            }}, handle)
        os.replace(replacement, policy.path)
        self.assertEqual(policy.enabled_sources(), frozenset({"olya"}))
        self.assertEqual(policy.weight("olya"), 8.0)

    def test_read_only_analytics_history_and_attempts_http_api(self):
        broker = self.make()
        job_id = broker.submit(
            "batch-video", "generate", {"prompt": "not-in-audit"},
            source="api-source", priority=7, source_item_id="source-1",
        )["id"]
        broker.dispatch_once()
        server = serve(broker, port=0)
        self.addCleanup(server.server_close)

        def get(path):
            worker = threading.Thread(target=server.handle_request)
            worker.start()
            with urlopen(f"http://127.0.0.1:{server.server_port}{path}") as response:
                body = json.loads(response.read())
            worker.join(timeout=1)
            return body

        analytics = get("/v1/analytics?window=3600")
        self.assertIn("api-source", analytics["sources"])
        audit = get(f"/v1/audit-events?job_id={job_id}&limit=20")
        self.assertGreaterEqual(len(audit["events"]), 4)
        self.assertNotIn("not-in-audit", json.dumps(audit))
        attempts = get(f"/v1/jobs/{job_id}/attempts")
        self.assertEqual(attempts["attempts"][0]["outcome"], "completed")
        correlations = get("/v1/correlations?source_item_id=source-1")
        self.assertEqual(correlations["jobs"][0]["job_id"], job_id)
        self.assertNotIn("payload", correlations["jobs"][0])


if __name__ == "__main__":
    unittest.main()
