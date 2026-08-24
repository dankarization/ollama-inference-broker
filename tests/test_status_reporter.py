import json
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from broker.service import Broker
from broker.status_reporter import (
    DeliveryError,
    TBILISI,
    build_report,
    previous_closed_hour,
    run_report,
)


class NoopOllama:
    def ps(self):
        return {"models": []}


class NoopWol:
    def wake(self):
        pass


class StatusReporterTests(unittest.TestCase):
    def setUp(self):
        handle = tempfile.NamedTemporaryFile(suffix=".sqlite3", delete=False)
        handle.close()
        self.db_path = handle.name
        self.addCleanup(lambda: Path(self.db_path).unlink(missing_ok=True))
        self.broker = Broker(self.db_path, NoopOllama(), NoopWol(), clock=lambda: 0)
        self.db = self.broker.db
        self.start = datetime(2026, 8, 24, 12, 0, tzinfo=TBILISI)
        self.interval = previous_closed_hour(datetime(2026, 8, 24, 13, 10, tzinfo=TBILISI))
        self.policy = {
            "shutterstock-video": {"enabled": True, "weight": 3.0},
            "olya-vision": {"enabled": True, "weight": 8.0},
            "olya-decision": {"enabled": True, "weight": 6.0},
        }
        self.health = {"broker_service": True, "broker_http": True, "ollama_http": True}

    def tearDown(self):
        self.broker.db.close()

    def add_job(self, ident, source, state, *, created_offset=0, started_offset=None,
                finished_offset=None, retry_count=0):
        created = self.start.timestamp() + created_offset
        self.db.execute(
            "INSERT INTO jobs(id,profile,kind,source,priority,payload,state,created,queued_at,"
            "source_item_id,external_id,started,finished,retry_count) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (ident, "batch-video", "generate", source, 7, "{}", state, created, created,
             ident, None,
             self.start.timestamp() + started_offset if started_offset is not None else None,
             self.start.timestamp() + finished_offset if finished_offset is not None else None,
             retry_count),
        )
        if state in {"completed", "failed", "cancelled"}:
            event = f"job.{state}"
            self.db.execute(
                "INSERT INTO audit_events(occurred,event_type,job_id,source,to_state) VALUES(?,?,?,?,?)",
                (self.start.timestamp() + finished_offset, event, ident, source, state),
            )
        if started_offset is not None:
            outcome = state if state in {"completed", "failed", "cancelled"} else None
            self.db.execute(
                "INSERT INTO job_attempts(job_id,attempt_no,source,queued_at,selected_at,started,finished,outcome,scheduler_mode,selection_reason) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                (ident, 1, source, created, self.start.timestamp() + started_offset,
                 self.start.timestamp() + started_offset,
                 self.start.timestamp() + finished_offset if finished_offset is not None else None,
                 outcome, "weighted_round_robin", "test"),
            )

    def report(self):
        self.db.commit()
        return build_report(
            self.db, self.interval, generated_at=datetime(2026, 8, 24, 13, 10, tzinfo=TBILISI),
            policy=self.policy, health=self.health,
        )

    def test_previous_closed_hour_is_tbilisi_wall_clock_boundary(self):
        interval = previous_closed_hour(datetime(2026, 8, 24, 9, 59, 59, tzinfo=TBILISI))
        self.assertEqual(interval.start.strftime("%H:%M"), "08:00")
        self.assertEqual(interval.end.strftime("%H:%M"), "09:00")
        self.assertEqual(interval.key, "2026-08-24T08:00:00+0400")

    def test_source_breakdown_weights_completed_share_and_duration(self):
        self.add_job("stock-ok", "shutterstock-video", "completed", started_offset=10, finished_offset=30)
        self.add_job("olya-v-ok", "olya-vision", "completed", started_offset=20, finished_offset=50)
        self.add_job("olya-d-fail", "olya-decision", "failed", started_offset=25, finished_offset=55)
        self.db.execute(
            "INSERT INTO audit_events(occurred,event_type,job_id,source) VALUES(?,?,?,?)",
            (self.start.timestamp() + 56, "lease.expired", "olya-d-fail", "olya-decision"),
        )
        text = self.report()
        self.assertIn("shutterstock-video: вес 3, включен, приоритет 5", text)
        self.assertIn("olya-vision: вес 8, включен, приоритет 8", text)
        self.assertIn("olya-decision: вес 6, включен, приоритет 6", text)
        self.assertIn("Фактическая доля completed: Shutterstock 1/2 (50.0%); Olya 1/2 (50.0%)", text)
        self.assertIn("lease-expired 1", text)
        self.assertIn("ср 20.0 с / p50 20.0 с / p95 20.0 с", text)
        self.assertIn("Прошли: stock-ok", text)
        self.assertIn("Ошибки: olya-d-fail", text)

    def test_zero_hour_is_sent_with_explicit_zero_counts(self):
        text = self.report()
        self.assertIn("За закрытый час: завершено 0; failed 0", text)
        self.assertIn("Пропускная способность: 0 completed/ч.", text)
        self.assertIn("Размеры файлов: не показаны", text)

    def test_identifier_truncation_is_bounded_and_deterministic(self):
        for number in range(7):
            self.add_job(f"queued-{number}", "shutterstock-video", "queued", created_offset=number)
        text = self.report()
        self.assertIn("queued-0, queued-1, queued-2, queued-3, queued-4, queued-5 …(+1)", text)

    def test_duplicate_and_retry_semantics_preserve_one_known_delivery(self):
        calls = []

        def sender(text):
            calls.append(text)
            return "501"

        now = datetime(2026, 8, 24, 13, 10, tzinfo=TBILISI)
        first = run_report(self.db_path, interval=self.interval, now=now, delivery=sender, health=self.health)
        second = run_report(self.db_path, interval=self.interval, now=now, delivery=sender, health=self.health)
        self.assertEqual(first["state"], "sent")
        self.assertEqual(first["message_id"], "501")
        self.assertEqual(second["state"], "already_sent")
        self.assertEqual(len(calls), 1)

        other = previous_closed_hour(datetime(2026, 8, 24, 14, 10, tzinfo=TBILISI))
        failures = [True, False]

        def flaky(text):
            if failures.pop(0):
                raise DeliveryError("local preflight failed", retryable=True)
            calls.append(text)
            return "502"

        self.assertEqual(run_report(self.db_path, interval=other, now=now, delivery=flaky, health=self.health)["state"], "failed")
        self.assertEqual(run_report(self.db_path, interval=other, now=now, delivery=flaky, health=self.health)["state"], "sent")
        self.assertEqual(calls[-1], calls[0].replace("12:00–13:00", "13:00–14:00"))

    def test_uncertain_delivery_is_not_automatically_duplicated(self):
        calls = []

        def unknown(_text):
            calls.append("attempt")
            raise DeliveryError("network outcome unknown", retryable=False)

        first = run_report(self.db_path, interval=self.interval, now=self.start, delivery=unknown, health=self.health)
        second = run_report(self.db_path, interval=self.interval, now=self.start, delivery=unknown, health=self.health)
        self.assertEqual(first["state"], "uncertain")
        self.assertEqual(second["state"], "uncertain")
        self.assertEqual(calls, ["attempt"])

    def test_production_timer_configuration_and_non_llm_path(self):
        root = Path(__file__).resolve().parents[1]
        service = (root / "systemd/ollama-inference-broker-report.service").read_text()
        timer = (root / "systemd/ollama-inference-broker-report.timer").read_text()
        module = (root / "broker/status_reporter.py").read_text()
        self.assertIn("BROKER_REPORT_TELEGRAM_TARGET=5775112073", service)
        self.assertIn("BROKER_REPORT_TELEGRAM_THREAD_ID=471305", service)
        self.assertIn("OPENCLAW_CLI=%h/.npm-global/bin/openclaw", service)
        self.assertIn("OnCalendar=*-*-* *:00:30 Asia/Tbilisi", timer)
        self.assertNotIn("OllamaHTTP", module)
        self.assertNotIn("/api/generate", module)
        self.assertNotIn("broker.compat", module)


if __name__ == "__main__":
    unittest.main()
