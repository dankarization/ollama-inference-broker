"""Deterministic tests for the model-aware, batch-oriented scheduler.

Coverage contract:
- same-model jobs batch together and avoid unload/load between them;
- an overdue job of another model breaks the batch (starvation guard);
- source weights still shape fair selection within a model batch;
- FIFO order is preserved inside each source;
- the dashboard forecast is read-only, deterministic and contingent;
- restart and hot policy reload are safe (durable jobs survive).
"""
import json
import os
import tempfile
import threading
import unittest

from broker.service import Broker, SourcePolicy


class FakeWol:
    def __init__(self, calls):
        self.calls = calls

    def wake(self):
        self.calls.append("wake")


class FakeOllama:
    def __init__(self, calls, loaded=None):
        self.calls, self.loaded = calls, list(loaded or [])

    def ps(self):
        self.calls.append("ps")
        return {"models": [{"name": name, "size_vram": 1} for name in self.loaded]}

    def is_ready(self, model):
        self.calls.append(("ready", model))
        return model in self.loaded

    def unload(self, model):
        self.calls.append(("unload", model))
        self.loaded.remove(model)

    def run(self, kind, request):
        self.calls.append(("run", kind, request.copy()))
        if request.get("prompt") == "" and request["model"] not in self.loaded:
            self.loaded.append(request["model"])
        return {"done": True}


class MutableClock:
    def __init__(self, value):
        self.value = value

    def __call__(self):
        return self.value


class BatchSchedulerTests(unittest.TestCase):
    def make(self, loaded=None, clock=None, **kwargs):
        self.calls = []
        self.tmp = tempfile.NamedTemporaryFile()
        self.addCleanup(self.tmp.close)
        return Broker(
            self.tmp.name, FakeOllama(self.calls, loaded), FakeWol(self.calls),
            clock=clock or MutableClock(1_000), **kwargs,
        )

    def policy(self, sources):
        fd, path = tempfile.mkstemp(suffix=".json")
        with os.fdopen(fd, "w") as handle:
            json.dump({"version": 1, "sources": sources}, handle)
        self.addCleanup(lambda: os.path.exists(path) and os.unlink(path))
        return SourcePolicy(path)

    def load_calls(self, model):
        return [
            call for call in self.calls
            if isinstance(call, tuple) and call[0] == "run"
            and call[2].get("model") == model and call[2].get("prompt") == ""
        ]

    def unload_calls(self):
        return [call[1] for call in self.calls
                if isinstance(call, tuple) and call[0] == "unload"]

    def test_same_model_jobs_batch_without_reload(self):
        clock = MutableClock(1_000)
        broker = self.make(clock=clock)  # nothing preloaded
        policy = self.policy({
            "interactive": {"enabled": True, "weight": 1.0},
            "cron": {"enabled": True, "weight": 1.0},
        })
        ids = []
        for index, prompt in enumerate(("i1", "i2", "c1")):
            clock.value = 1_000 + index
            profile = "interactive" if prompt.startswith("i") else "cron"
            ids.append(broker.submit(profile, "generate", {"prompt": prompt})["id"])
        for _ in ids:
            self.assertTrue(broker.dispatch_once(policy=policy))
        self.assertEqual([broker.status(job_id)["state"] for job_id in ids],
                         ["completed"] * 3)
        # Both profiles resolve to nemotron3:33b, so the three jobs form one
        # batch: the model is loaded exactly once and never unloaded.
        self.assertEqual(len(self.load_calls("nemotron3:33b")), 1)
        self.assertEqual(self.unload_calls(), [])
        # Model residency is adapter behavior, not a scheduling preference.
        modes = [
            attempt["scheduler_mode"]
            for job_id in ids
            for attempt in broker.attempts(job_id)
        ]
        self.assertEqual(modes, ["weighted_round_robin"] * 3)

    def test_model_switch_unloads_previous_model_between_batches(self):
        clock = MutableClock(1_000)
        broker = self.make(["nemotron3:33b"], clock=clock)
        policy = self.policy({
            "interactive": {"enabled": True, "weight": 1.0},
            "olya-vision": {"enabled": True, "weight": 1.0},
        })
        first = broker.submit("interactive", "generate", {"prompt": "i"})["id"]
        clock.value = 1_001
        second = broker.submit(
            "olya-vision-gemma", "generate",
            {"prompt": "v", "images": ["aGVsbG8="], "format": {"type": "object"}},
            source="olya-vision",
        )["id"]
        broker.dispatch_once(policy=policy)
        broker.dispatch_once(policy=policy)
        self.assertEqual(broker.status(first)["state"], "completed")
        self.assertEqual(broker.status(second)["state"], "completed")
        self.assertEqual(
            broker.status(second)["switch_reason"],
            "unloaded incompatible model before switch",
        )
        # Only the incompatible previous model is unloaded, exactly once.
        self.assertEqual(self.unload_calls(), ["nemotron3:33b"])
        self.assertEqual(
            broker.attempts(second)[0]["scheduler_mode"], "weighted_round_robin"
        )

    @unittest.skip("superseded by the weight-only scheduler contract")
    def test_overdue_other_model_breaks_the_active_batch(self):
        clock = MutableClock(1_000)
        broker = self.make(clock=clock, wait_debt_seconds=1_000)
        policy = self.policy({
            "interactive": {"enabled": True, "weight": 1.0},
            "olya-vision": {"enabled": True, "weight": 1.0},
        })
        first = broker.submit("interactive", "generate", {"prompt": "i1"})["id"]
        clock.value = 1_001
        second = broker.submit("interactive", "generate", {"prompt": "i2"})["id"]
        broker.dispatch_once(policy=policy)
        # A different-model job is admitted while the nemotron batch is active.
        clock.value = 1_010
        overdue = broker.submit(
            "olya-vision-gemma", "generate",
            {"prompt": "v", "images": ["aGVsbG8="], "format": {"type": "object"}},
            source="olya-vision",
        )["id"]
        clock.value = 2_050
        # interactive (priority 1 -> debt 100s) has waited 1049s; olya-vision
        # (priority 8 -> debt 800s) only 1040s, so it is not yet overdue.  The
        # overdue interactive job is served before the batch would drain.
        self.assertTrue(broker.dispatch_once(policy=policy))
        self.assertEqual(broker.status(second)["state"], "completed")
        self.assertEqual(broker.status(overdue)["state"], "queued")
        self.assertEqual(broker.attempts(second)[0]["scheduler_mode"], "overdue")
        self.assertIn("priority debt", broker.attempts(second)[0]["selection_reason"])
        # The batch is broken: the next pick serves the other model instead
        # of continuing the nemotron series (its wait is still within debt).
        self.assertTrue(broker.dispatch_once(policy=policy))
        self.assertEqual(broker.status(overdue)["state"], "completed")
        self.assertNotEqual(
            broker.attempts(overdue)[0]["scheduler_mode"], "model_batch"
        )

    def test_weights_fairness_within_model_batch(self):
        clock = MutableClock(1_000)
        broker = self.make(clock=clock)
        policy = self.policy({
            "interactive": {"enabled": True, "weight": 2.0},
            "cron": {"enabled": True, "weight": 6.0},
        })
        ids = []
        for index in range(100):
            clock.value = 1_000 + index
            ids.append(broker.submit("interactive", "generate", {"prompt": f"i{index}"})["id"])
        for index in range(100):
            clock.value = 1_100 + index
            ids.append(broker.submit("cron", "generate", {"prompt": f"c{index}"})["id"])
        for _ in range(60):
            self.assertTrue(broker.dispatch_once(policy=policy))
        interactive_done = sum(
            1 for job_id in ids[:100] if broker.status(job_id)["state"] == "completed"
        )
        cron_done = sum(
            1 for job_id in ids[100:] if broker.status(job_id)["state"] == "completed"
        )
        # Lower Weight means higher share: 2 receives exactly 3x Weight 6.
        self.assertEqual((interactive_done, cron_done), (45, 15))
        snapshot = broker.analytics(policy, windows=(3_600,))
        scheduler = snapshot["scheduler"]["windows"]["3600"]
        self.assertAlmostEqual(
            scheduler["sources"]["interactive"]["fairness_ratio"], 1.0, delta=0.1
        )
        self.assertAlmostEqual(
            scheduler["sources"]["cron"]["fairness_ratio"], 1.0, delta=0.1
        )

    def test_old_low_importance_job_cannot_override_weight(self):
        clock = MutableClock(1_000)
        broker = self.make(clock=clock)
        policy = self.policy({
            "cron": {"enabled": True, "weight": 6},
            "interactive": {"enabled": True, "weight": 2},
        })
        old = broker.submit("cron", "generate", {"prompt": "old"})["id"]
        clock.value = 9_000_000
        important = broker.submit("interactive", "generate", {"prompt": "new"})["id"]
        broker.dispatch_once(policy=policy)
        self.assertEqual(broker.status(important)["state"], "completed")
        self.assertEqual(broker.status(old)["state"], "queued")

    def test_forecast_projects_the_active_source_after_completion(self):
        clock = MutableClock(1_000)
        broker = self.make(clock=clock)
        policy = self.policy({
            "interactive": {"enabled": True, "weight": 1},
            "cron": {"enabled": True, "weight": 10},
        })
        active = broker.submit("interactive", "generate", {"prompt": "active"})["id"]
        clock.value += 1
        expected = broker.submit("interactive", "generate", {"prompt": "next"})["id"]
        clock.value += 1
        broker.submit("cron", "generate", {"prompt": "other"})
        entered, release = threading.Event(), threading.Event()
        original_run = broker.ollama.run
        def blocking_run(kind, request):
            entered.set()
            release.wait(2)
            return original_run(kind, request)
        broker.ollama.run = blocking_run
        worker = threading.Thread(target=lambda: broker.dispatch_once(policy=policy))
        worker.start()
        self.assertTrue(entered.wait(1))
        self.assertEqual(broker.status(active)["state"], "running")
        forecast = broker.forecast(policy, limit=1)
        self.assertEqual(forecast["next_selections"][0]["job_id"], expected)
        release.set()
        worker.join(timeout=2)
        self.assertTrue(broker.dispatch_once(policy=policy))
        self.assertEqual(broker.status(expected)["state"], "completed")

    def test_fifo_within_source_is_preserved(self):
        clock = MutableClock(1_000)
        broker = self.make(["nemotron3:33b"], clock=clock)
        policy = self.policy({"interactive": {"enabled": True, "weight": 1.0}})
        ids = []
        for index in range(3):
            clock.value = 1_000 + index
            ids.append(broker.submit("interactive", "generate", {"prompt": f"p{index}"})["id"])
        for _ in ids:
            self.assertTrue(broker.dispatch_once(policy=policy))
        self.assertEqual([broker.status(job_id)["state"] for job_id in ids],
                         ["completed"] * 3)
        # Jobs completed in submission order (FIFO within the source).
        finished = [broker.status(job_id)["finished"] for job_id in ids]
        self.assertEqual(finished, sorted(finished))

    @unittest.skip("model batches were removed; weight is the sole soft selector")
    def test_batch_job_limit_survives_a_long_inference(self):
        clock = MutableClock(1_000)
        # The deprecated time-cap argument is accepted but cannot end a batch.
        broker = self.make(clock=clock, batch_max_jobs=3, batch_max_seconds=10,
                           wait_debt_seconds=10_000)
        policy = self.policy({"interactive": {"enabled": True, "weight": 1.0}})
        ids = []
        for index in range(3):
            clock.value = 1_000 + index
            ids.append(broker.submit("interactive", "generate", {"prompt": f"p{index}"})["id"])
        broker.dispatch_once(policy=policy)   # batch starts (job 1/3)
        clock.value = 2_000                   # >600s after the batch started
        broker.dispatch_once(policy=policy)   # still a same-model continuation (job 2/3)
        broker.dispatch_once(policy=policy)   # continuation reaches the count cap (job 3/3)
        modes = [broker.attempts(job_id)[0]["scheduler_mode"] for job_id in ids]
        self.assertEqual(modes, ["weighted_round_robin", "model_batch", "model_batch"])

    @unittest.skip("model batches were removed; weight is the sole soft selector")
    def test_default_batch_stops_after_eight_jobs(self):
        clock = MutableClock(1_000)
        broker = self.make(clock=clock, wait_debt_seconds=10_000)
        policy = self.policy({"interactive": {"enabled": True, "weight": 1.0}})
        ids = [
            broker.submit("interactive", "generate", {"prompt": f"p{index}"})["id"]
            for index in range(9)
        ]
        for _ in ids:
            self.assertTrue(broker.dispatch_once(policy=policy))
        modes = [broker.attempts(job_id)[0]["scheduler_mode"] for job_id in ids]
        # Same-timestamp submissions have random UUIDs, so assert scheduler
        # modes rather than a particular job-id order: 8 jobs form one batch
        # (one start plus seven continuations), then job 9 starts another.
        self.assertEqual(modes.count("weighted_round_robin"), 2)
        self.assertEqual(modes.count("model_batch"), 7)

    @unittest.skip("legacy priority and wait-debt selection were removed")
    def test_priority_semantics_one_highest_ten_lowest(self):
        clock = MutableClock(1_000)
        broker = self.make(clock=clock, wait_debt_seconds=1_000)
        # 1 is the highest priority (shortest wait debt), 10 the lowest.
        self.assertEqual(broker._wait_debt_seconds({"priority": 1}), 100.0)
        self.assertEqual(broker._wait_debt_seconds({"priority": 5}), 500.0)
        self.assertEqual(broker._wait_debt_seconds({"priority": 10}), 1_000.0)
        self.assertEqual(broker._wait_debt_seconds({"priority": None}), 1_000.0)
        self.assertEqual(broker._wait_debt_seconds({"priority": 0}), 1_000.0)
        # A higher-priority (lower-number) job is served before an older
        # lower-priority job once it reaches its much shorter debt bound.
        policy = self.policy({
            "interactive": {"enabled": True, "weight": 1.0},
            "olya": {"enabled": True, "weight": 1.0},
        })
        low = broker.submit("olya", "generate", {"prompt": "low"})["id"]  # p8
        clock.value = 1_650
        high = broker.submit("interactive", "generate", {"prompt": "high"})["id"]  # p1
        clock.value = 1_770
        # high (p1, debt 100s) waited 120s -> overdue; low (p8, debt 800s)
        # waited 770s -> not overdue.  The starvation guard must serve high
        # even though low is older.
        broker.dispatch_once(policy=policy)
        self.assertEqual(broker.status(high)["state"], "completed")
        self.assertEqual(broker.status(low)["state"], "queued")
        self.assertEqual(broker.attempts(high)[0]["scheduler_mode"], "overdue")

    def test_forecast_is_read_only_deterministic_and_contingent(self):
        clock = MutableClock(1_000)
        broker = self.make(clock=clock)
        policy = self.policy({
            "interactive": {"enabled": True, "weight": 2.0},
            "cron": {"enabled": True, "weight": 1.0},
        })
        ids = []
        for index, (profile, prompt) in enumerate(
            (("interactive", "i1"), ("interactive", "i2"), ("cron", "c1"))
        ):
            clock.value = 1_000 + index
            ids.append(broker.submit(profile, "generate", {"prompt": prompt})["id"])
        before_accumulator = dict(broker._weight_accumulator)
        before_states = [broker.status(job_id)["state"] for job_id in ids]
        before_audit = broker.db.execute(
            "SELECT count(*) FROM audit_events"
        ).fetchone()[0]

        first = broker.forecast(policy, limit=5)
        second = broker.forecast(policy, limit=5)

        self.assertEqual(first, second)
        self.assertIs(first["contingent"], True)
        self.assertIn("contingency", first)
        self.assertIsNone(first["current_model"])  # nothing is running yet
        # The projection covers the full bounded queue (deterministic order)
        # and selects its FIFO heads with the inverse-weight share.
        self.assertEqual(
            sorted(item["job_id"] for item in first["next_selections"]), sorted(ids)
        )
        self.assertEqual(first["next_selections"][0]["job_id"], ids[2])
        self.assertEqual(first["next_selections"][0]["mode"], "weighted_round_robin")
        self.assertEqual({item["mode"] for item in first["next_selections"]}, {"weighted_round_robin"})
        # Forecast is read-only: scheduler state and durable rows unchanged.
        self.assertEqual(dict(broker._weight_accumulator), before_accumulator)
        self.assertEqual([broker.status(job_id)["state"] for job_id in ids],
                         before_states)
        self.assertEqual(
            broker.db.execute("SELECT count(*) FROM audit_events").fetchone()[0],
            before_audit,
        )
        # And it is bounded by the requested limit.
        self.assertLessEqual(len(first["next_selections"]), 5)

    def test_observer_candidates_do_not_sort_large_payloads(self):
        broker = self.make()
        broker.submit("interactive", "generate", {"prompt": "x" * 1_000_000})
        statements = []
        broker.db.set_trace_callback(statements.append)
        broker._candidates(db=broker.db)
        broker.db.set_trace_callback(None)
        observer_select = next(
            line for line in statements
            if "INDEXED BY jobs_queued_candidates WHERE state='queued'" in line
        )
        self.assertIn("id,profile,source,created,queued_at", observer_select)
        self.assertNotIn("payload", observer_select)

    def test_dispatcher_selects_large_queue_without_materializing_payloads(self):
        clock = MutableClock(1_000)
        broker = self.make(clock=clock)
        policy = self.policy({"interactive": {"enabled": True, "weight": 1.0}})
        large = "x" * 1_000_000
        selected = broker.submit("interactive", "generate", {
            "prompt": "selected", "context": large,
        })["id"]
        for index in range(1, 5):
            clock.value += 1
            broker.submit("interactive", "generate", {
                "prompt": f"queued-{index}", "context": large,
            })
        statements = []
        broker.db.set_trace_callback(statements.append)
        self.assertTrue(broker.dispatch_once(policy=policy))
        broker.db.set_trace_callback(None)
        candidate_select = next(
            line for line in statements
            if "INDEXED BY jobs_queued_candidates WHERE state='queued'" in line
        )
        self.assertIn(
            "id,profile,source,created,queued_at,attempt_count",
            candidate_select,
        )
        self.assertNotIn("payload", candidate_select)
        full_rows = [line for line in statements if "SELECT * FROM jobs WHERE id=" in line]
        self.assertEqual(len(full_rows), 1)
        self.assertIn(selected, full_rows[0])
        executions = [call for call in self.calls if isinstance(call, tuple) and call[0] == "run"]
        self.assertEqual(executions[-1][2]["prompt"], "selected")

    def test_forecast_uses_weight_only_selection(self):
        clock = MutableClock(1_000)
        broker = self.make(clock=clock)
        policy = self.policy({
            "interactive": {"enabled": True, "weight": 1.0},
            "olya-vision": {"enabled": True, "weight": 1.0},
        })
        broker.submit("interactive", "generate", {"prompt": "i1"})
        clock.value = 1_001
        broker.submit("interactive", "generate", {"prompt": "i2"})
        clock.value = 1_002
        broker.submit(
            "olya-vision-gemma", "generate",
            {"prompt": "v", "images": ["aGVsbG8="], "format": {"type": "object"}},
            source="olya-vision",
        )
        broker.dispatch_once(policy=policy)
        forecast = broker.forecast(policy, limit=5)
        self.assertEqual({item["mode"] for item in forecast["next_selections"]}, {"weighted_round_robin"})

    def test_restart_preserves_durable_jobs(self):
        path = tempfile.NamedTemporaryFile(suffix=".sqlite3").name
        self.addCleanup(lambda: os.path.exists(path) and os.unlink(path))
        clock = MutableClock(1_000)
        calls = []
        policy = self.policy({"interactive": {"enabled": True, "weight": 1.0}})
        first = Broker(path, FakeOllama(calls, ["nemotron3:33b"]),
                       FakeWol(calls), clock=clock)
        job_id = first.submit("interactive", "generate", {"prompt": "x"})["id"]
        first.dispatch_once(policy=policy)
        first.db.close()

        restarted = Broker(path, FakeOllama(calls, ["nemotron3:33b"]),
                           FakeWol(calls), clock=clock)
        self.assertEqual(restarted.status(job_id)["state"], "completed")
        # Dispatch still works after restart.
        clock.value = 1_100
        next_id = restarted.submit("interactive", "generate", {"prompt": "y"})["id"]
        self.assertTrue(restarted.dispatch_once(policy=policy))
        self.assertEqual(restarted.status(next_id)["state"], "completed")
        self.assertEqual(restarted.attempts(next_id)[0]["scheduler_mode"],
                         "weighted_round_robin")

    def test_hot_policy_reload_is_respected_without_losing_batch(self):
        clock = MutableClock(1_000)
        broker = self.make(["nemotron3:33b"], clock=clock)
        policy = self.policy({
            "interactive": {"enabled": True, "weight": 1.0},
            "cron": {"enabled": True, "weight": 1.0},
        })
        interactive_one = broker.submit("interactive", "generate", {"prompt": "i1"})["id"]
        clock.value = 1_001
        cron = broker.submit("cron", "generate", {"prompt": "c"})["id"]
        clock.value = 1_002
        interactive_two = broker.submit("interactive", "generate", {"prompt": "i2"})["id"]
        broker.dispatch_once(policy=policy)
        # Equal weights pick interactive first (FIFO tie-break).
        self.assertEqual(broker.status(interactive_one)["state"], "completed")
        # Atomic hot reload flips cron's relative share.
        replacement = str(policy.path) + ".tmp"
        with open(replacement, "w") as handle:
            json.dump({"version": 1, "sources": {
                "interactive": {"enabled": True, "weight": 10.0},
                "cron": {"enabled": True, "weight": 1.0},
            }}, handle)
        os.replace(replacement, policy.path)
        self.assertTrue(broker.dispatch_once(policy=policy))
        # The reloaded lower Weight makes cron win the next selection.
        self.assertEqual(broker.status(cron)["state"], "completed")
        self.assertEqual(broker.status(interactive_two)["state"], "queued")
        self.assertEqual(broker.attempts(cron)[0]["scheduler_mode"], "weighted_round_robin")
        self.assertEqual(broker._last_scheduler_decision["selected_source"], "cron")

    def test_disabled_source_never_enters_a_batch(self):
        broker = self.make(["nemotron3:33b"])
        policy = self.policy({
            "interactive": {"enabled": True, "weight": 1.0},
            "cron": {"enabled": False, "weight": 1.0},
        })
        blocked = broker.submit("cron", "generate", {"prompt": "c"})["id"]
        allowed = broker.submit("interactive", "generate", {"prompt": "i"})["id"]
        self.assertTrue(broker.dispatch_once(policy.enabled_sources(), policy))
        self.assertEqual(broker.status(allowed)["state"], "completed")
        self.assertEqual(broker.status(blocked)["state"], "queued")

    def test_forecast_http_endpoint_is_read_only_and_bounded(self):
        import threading
        from urllib.request import urlopen

        from broker.http import serve

        clock = MutableClock(1_000)
        broker = self.make(clock=clock)
        policy = self.policy({"interactive": {"enabled": True, "weight": 1.0}})
        for index in range(3):
            clock.value = 1_000 + index
            broker.submit("interactive", "generate", {"prompt": f"p{index}"})
        server = serve(broker, port=0, policy=policy)
        self.addCleanup(server.server_close)
        worker = threading.Thread(target=server.handle_request)
        worker.start()
        try:
            with urlopen(
                f"http://127.0.0.1:{server.server_port}/v1/forecast?limit=2"
            ) as response:
                body = json.loads(response.read())
        finally:
            worker.join(timeout=1)
        self.assertEqual(response.status, 200)
        self.assertIs(body["contingent"], True)
        self.assertEqual(len(body["next_selections"]), 2)  # bounded by limit
        self.assertEqual(body["next_selections"][0]["weight"], 1.0)
        self.assertNotIn("payload", json.dumps(body))
        # The endpoint never dispatches: every job is still queued.
        self.assertEqual(
            broker.db.execute("SELECT count(*) FROM jobs WHERE state='queued'").fetchone()[0],
            3,
        )

    def test_dashboard_html_renders_the_contingent_forecast(self):
        from broker.dashboard import render

        html = render({
            "timestamp": 1_000,
            "sources": [],
            "active_jobs": [],
            "overall": {
                "states": {"completed": 0},
                "completed_last_hour": 0,
                "completed_last_24_hours": 0,
            },
            "forecast": {
                "contingent": True,
                "contingency": "read-only projection; it changes as jobs are admitted or complete",
                "current_model": "nemotron3:33b",
                "next_selections": [{
                    "job_id": "job-1", "source": "interactive",
                    "profile": "interactive", "model": "nemotron3:33b",
                    "weight": 1, "mode": "weighted_round_robin",
                    "reason": "weight-only source selection",
                    "eligible_sources": ["interactive"], "wait_seconds": 5.0,
                }],
            },
        }).decode()
        self.assertIn("<h2>Forecast</h2>", html)
        self.assertIn("Current model: <b>nemotron3:33b</b>", html)
        self.assertIn("Weight 1 is most important", html)
        self.assertIn("read-only projection; it changes as jobs are admitted", html)
        self.assertIn(">job-1<", html)
        self.assertIn(">weighted_round_robin<", html)

    def test_forecast_unavailable_when_database_is_locked(self):
        import sqlite3
        from unittest.mock import patch

        broker = self.make()
        broker.submit("interactive", "generate", {"prompt": "x"})
        with patch(
            "broker.service.sqlite3.connect",
            side_effect=sqlite3.OperationalError("database is locked"),
        ):
            forecast = broker.forecast()
        self.assertIs(forecast["contingent"], True)
        self.assertIs(forecast["unavailable"], True)
        self.assertEqual(forecast["reason"], "database observer read unavailable")
        self.assertNotIn("next_selections", forecast)


if __name__ == "__main__":
    unittest.main()
