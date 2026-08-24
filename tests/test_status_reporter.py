import http.server
import json
import os
import sqlite3
import tempfile
import threading
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from broker.service import Broker
from broker.status_reporter import (
    DeliveryError,
    TBILISI,
    build_report,
    deliver_via_telegram_html,
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
            # Source policy weights are scheduler-private and deliberately do
            # not define the operator-facing broker priority.
            "shutterstock-video": {"enabled": True, "weight": 9.0},
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
            self.db.execute(
                "INSERT INTO audit_events(occurred,event_type,job_id,source,to_state) VALUES(?,?,?,?,?)",
                (self.start.timestamp() + finished_offset, f"job.{state}", ident, source, state),
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

    def test_concise_priority_scale_and_closed_hour_breakdown(self):
        self.add_job("stock-ok", "shutterstock-video", "completed", started_offset=10, finished_offset=30)
        self.add_job("olya-v-ok", "olya-vision", "completed", started_offset=20, finished_offset=50)
        self.add_job("olya-d-fail", "olya-decision", "failed", started_offset=25, finished_offset=55)
        self.db.execute(
            "INSERT INTO audit_events(occurred,event_type,job_id,source) VALUES(?,?,?,?)",
            (self.start.timestamp() + 56, "lease.expired", "olya-d-fail", "olya-decision"),
        )
        text = self.report()
        self.assertIn("<u>Shutterstock</u> · <b>Приоритет 3/10</b>", text)
        self.assertIn("<u>Olya</u> · <b>Приоритет: Vision 8/10 · Decision 6/10</b>", text)
        self.assertIn("Час: завершено 1 · ошибок 0 · lease-expired 0", text)
        self.assertIn("Час: завершено 1 · ошибок 1 · lease-expired 1", text)
        self.assertIn("<b>Итого</b>: 2 completed/ч", text)
        self.assertNotIn("вес", text.lower())
        self.assertNotIn("p95", text)
        self.assertLess(len(text), 1_100)

    def test_zero_hour_is_rendered_with_zero_counts_and_active_timestamps(self):
        text = self.report()
        self.assertIn("Час: завершено 0 · ошибок 0 · lease-expired 0", text)
        self.assertIn("<b>Итого</b>: 0 completed/ч", text)
        self.assertIn('<tg-time unix="1787558400" format="">24.08 12:00</tg-time>', text)
        self.assertIn('<tg-time unix="1787562000" format="">13:00</tg-time>', text)
        self.assertIn("<i>Закрытый час, Asia/Tbilisi:</i>", text)

    def test_identifier_truncation_is_compact_bounded_and_escaped(self):
        for number in range(4):
            self.add_job(f"11111111-1111-1111-1111-00000000000{number}", "shutterstock-video", "queued", created_offset=number)
        self.add_job("asset:<unsafe>&value", "olya-vision", "running", started_offset=4)
        text = self.report()
        self.assertIn("<code>…00000000</code>", text)
        self.assertIn("…(+1)", text)
        self.assertIn("<code>asset:&lt;unsafe&gt;&amp;value</code>", text)
        self.assertNotIn("11111111-1111-1111", text)

    def test_duplicate_retry_and_manual_resend_are_separate(self):
        calls = []

        def sender(text):
            calls.append(text)
            return str(500 + len(calls))

        now = datetime(2026, 8, 24, 13, 10, tzinfo=TBILISI)
        first = run_report(self.db_path, interval=self.interval, now=now, delivery=sender, health=self.health)
        second = run_report(self.db_path, interval=self.interval, now=now, delivery=sender, health=self.health)
        manual = run_report(
            self.db_path, interval=self.interval, now=now, delivery=sender, health=self.health,
            manual_key="html-verification-v1",
        )
        manual_again = run_report(
            self.db_path, interval=self.interval, now=now, delivery=sender, health=self.health,
            manual_key="html-verification-v1",
        )
        self.assertEqual((first["state"], second["state"], manual["state"], manual_again["state"]),
                         ("sent", "already_sent", "sent", "already_sent"))
        self.assertEqual(len(calls), 2)

        other = previous_closed_hour(datetime(2026, 8, 24, 14, 10, tzinfo=TBILISI))
        failures = [True, False]

        def flaky(text):
            if failures.pop(0):
                raise DeliveryError("local preflight failed", retryable=True)
            return "503"

        self.assertEqual(run_report(self.db_path, interval=other, now=now, delivery=flaky, health=self.health)["state"], "failed")
        self.assertEqual(run_report(self.db_path, interval=other, now=now, delivery=flaky, health=self.health)["state"], "sent")

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

    def test_telegram_html_delivery_uses_airfare_fields(self):
        seen = {}

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                seen["path"] = self.path
                seen["type"] = self.headers["Content-Type"]
                seen["body"] = self.rfile.read(int(self.headers["Content-Length"])).decode()
                response = b'{"ok":true,"result":{"message_id":712}}'
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(response)))
                self.end_headers()
                self.wfile.write(response)
            def log_message(self, *_args):
                pass

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(thread.join)
        self.addCleanup(lambda: server.shutdown())
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / "openclaw.json"
            config.write_text(json.dumps({"channels": {"telegram": {"accounts": {"default": {
                "botToken": "secret-token", "apiRoot": f"http://127.0.0.1:{server.server_port}",
            }}}}}))
            with patch.dict(os.environ, {
                "OPENCLAW_CONFIG_PATH": str(config),
                "BROKER_REPORT_TELEGRAM_TARGET": "5775112073",
                "BROKER_REPORT_TELEGRAM_THREAD_ID": "471305",
                "BROKER_REPORT_TELEGRAM_ACCOUNT": "default",
            }, clear=False):
                self.assertEqual(deliver_via_telegram_html('<b>x</b><tg-time unix="1" format="">x</tg-time>'), "712")
        self.assertEqual(seen["path"], "/botsecret-token/sendMessage")
        self.assertIn("name=\"parse_mode\"", seen["body"])
        self.assertIn("HTML", seen["body"])
        self.assertIn("name=\"message_thread_id\"", seen["body"])
        self.assertIn("471305", seen["body"])
        self.assertIn('<b>x</b><tg-time unix="1" format="">x</tg-time>', seen["body"])

    def test_production_timer_configuration_and_non_llm_path(self):
        root = Path(__file__).resolve().parents[1]
        service = (root / "systemd/ollama-inference-broker-report.service").read_text()
        timer = (root / "systemd/ollama-inference-broker-report.timer").read_text()
        module = (root / "broker/status_reporter.py").read_text()
        self.assertIn("BROKER_REPORT_TELEGRAM_TARGET=5775112073", service)
        self.assertIn("BROKER_REPORT_TELEGRAM_THREAD_ID=471305", service)
        self.assertIn("OnCalendar=*-*-* *:00:30 Asia/Tbilisi", timer)
        self.assertIn('"parse_mode": "HTML"', module)
        self.assertNotIn("OllamaHTTP", module)
        self.assertNotIn("/api/generate", module)
        self.assertNotIn("broker.compat", module)


if __name__ == "__main__":
    unittest.main()
