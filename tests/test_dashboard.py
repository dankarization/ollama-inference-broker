import json
import sqlite3
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from unittest.mock import patch

from broker.dashboard import render, snapshot
from broker.http import serve
from broker.service import Broker, SourcePolicy


class FakeOllama:
    def ps(self): return {"models": []}
    def is_ready(self, _): return True
    def unload(self, _): pass
    def run(self, *_): return {"done": True}


class FakeWol:
    def wake(self): pass


class DashboardTests(unittest.TestCase):
    def test_policy_select_state_machine_recovers_and_serializes(self):
        # Executable model of the rendered desired/saved/saving state machine.
        saved, desired, saving, sent = "A", "A", False, []
        def change(value):
            nonlocal desired, saving
            desired = value
            if not saving:
                saving = True
                sent.append(desired)
        def settle(ok, persisted=None):
            nonlocal saved, saving
            value = sent[-1]
            if ok:
                saved = persisted or value
                if desired != saved: sent.append(desired); return
                saving = False; return
            elif desired != value:
                sent.append(desired); return
            saving = False
        change("B"); settle(False)  # latest B fails and releases the flag
        self.assertFalse(saving); change("C"); self.assertEqual(sent[-1], "C"); settle(True)
        self.assertEqual((saved, saving), ("C", False))
        change("A"); change("B")  # B is queued; only A is in flight
        self.assertEqual(sent[-1], "A"); settle(False); self.assertEqual(sent[-1], "B")
        settle(True); self.assertEqual((saved, saving), ("B", False))
    def test_history_keyset_is_payload_free_ordered_and_exhaustible(self):
        db = tempfile.NamedTemporaryFile()
        self.addCleanup(db.close)
        broker = Broker(db.name, FakeOllama(), FakeWol(), clock=lambda: 100)
        payload = json.dumps({"secret": "x" * 65_536})
        with broker.db:
            for index in range(40):
                broker.db.execute(
                    "INSERT INTO jobs(id,profile,kind,source,payload,state,created,finished,attempt_count) "
                    "VALUES(?,?,?,?,?,?,?,?,?)",
                    (f"terminal-{index:02d}", "interactive", "generate", "history", payload,
                     "completed", float(index), float(index), index % 3 + 1),
                )
        first_plan = " ".join(row[3] for row in broker.db.execute(
            "EXPLAIN QUERY PLAN SELECT id,source,profile,state,created,started,finished,attempt_count,retry_count "
            "FROM jobs INDEXED BY jobs_terminal_history_v3 WHERE state IN ('completed','failed','cancelled') "
            "AND finished IS NOT NULL ORDER BY finished DESC,id DESC LIMIT 30"
        ))
        cursor_plan = " ".join(row[3] for row in broker.db.execute(
            "EXPLAIN QUERY PLAN SELECT id,source,profile,state,created,started,finished,attempt_count,retry_count "
            "FROM jobs INDEXED BY jobs_terminal_history_v3 WHERE state IN ('completed','failed','cancelled') "
            "AND finished IS NOT NULL AND (finished,id) < (?,?) "
            "ORDER BY finished DESC,id DESC LIMIT 30",
            (30.0, "terminal-30"),
        ))
        for plan in (first_plan, cursor_plan):
            self.assertIn("COVERING INDEX jobs_terminal_history_v3", plan)
            self.assertNotIn("USE TEMP B-TREE FOR ORDER BY", plan)
        first = broker.terminal_history(limit=10)
        second = broker.terminal_history(limit=30, cursor=tuple(first["next_cursor"]))
        self.assertEqual(len(first["items"]), 10)
        self.assertEqual(len(second["items"]), 30)
        ids = [item["id"] for item in first["items"] + second["items"]]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertEqual(ids, [f"terminal-{index:02d}" for index in range(39, -1, -1)])
        self.assertNotIn("payload", json.dumps(first))

    def test_dashboard_html_contains_enabled_and_history_loading_contract(self):
        html = render({"timestamp": 1, "sources": [], "active_jobs": [],
                       "overall": {"states": {"completed": 0}, "completed_last_hour": 0, "completed_last_24_hours": 0},
                       "history": [], "forecast": {"contingent": True, "next_selections": []}}).decode()
        self.assertIn('class="enabled-form"', html) if False else self.assertIn("id=history-body", html)
        self.assertIn("IntersectionObserver", html)
        self.assertIn("/v1/history?limit=30", html)
        self.assertLess(html.index("<h2>Forecast</h2>"), html.index("<h2>History</h2>"))
        self.assertNotIn("tr.innerHTML", html)
        self.assertIn("td.textContent", html)
        self.assertIn("timeZone:'Asia/Tbilisi'", html)
        self.assertNotIn("<button", html)
        self.assertIn("addEventListener('change'", html)
        self.assertIn("while(desired!==saved)", html)
        self.assertIn("desired===value", html)
    def test_payload_history_uses_the_bounded_observer_index_path(self):
        db = tempfile.NamedTemporaryFile()
        self.addCleanup(db.close)
        broker = Broker(db.name, FakeOllama(), FakeWol(), clock=lambda: 100_000)
        payload = json.dumps({"prompt": "x" * 16_384})
        with broker.db:
            broker.db.executemany(
                "INSERT INTO jobs("
                "id,profile,kind,source,payload,state,created,finished,queued_at"
                ") VALUES (?,?,?,?,?,?,?,?,?)",
                [
                    (
                        f"history-{index}", "interactive", "generate", "archive",
                        payload, "completed", float(index), 99_999.0, float(index),
                    )
                    for index in range(2_000)
                ],
            )
            broker.db.execute(
                "INSERT INTO jobs("
                "id,profile,kind,source,payload,state,created,finished,queued_at"
                ") VALUES (?,?,?,?,?,?,?,?,?)",
                ("history", "interactive", "generate", "history", payload,
                 "completed", 1.0, 99_999.0, 1.0),
            )
            broker.db.execute(
                "INSERT INTO audit_events(occurred,event_type,job_id,source) VALUES(?,?,?,?)",
                (99_999.0, "job.completed", "history", "history"),
            )

        progress_calls = [0]
        queries: list[str] = []

        def stop_full_table_scan():
            progress_calls[0] += 1
            return progress_calls[0] >= 70_000

        broker.db.set_progress_handler(stop_full_table_scan, 1)
        broker.db.set_trace_callback(queries.append)
        try:
            data = snapshot(broker.db, now=100_000, policy_snapshot={"sources": {
                "history": {"enabled": True, "weight": 1.0},
            }})
        finally:
            broker.db.set_progress_handler(None, 0)
            broker.db.set_trace_callback(None)

        self.assertEqual(data["overall"]["states"]["completed"], 1)
        self.assertEqual(data["overall"]["completed_last_hour"], 1)
        self.assertLess(progress_calls[0], 70_000)
        self.assertTrue(any("FROM audit_events" in query for query in queries))
        self.assertFalse(any("finished>=" in query for query in queries))

    def test_dashboard_queued_payload_reads_use_covering_indexes(self):
        db = tempfile.NamedTemporaryFile()
        self.addCleanup(db.close)
        broker = Broker(db.name, FakeOllama(), FakeWol(), clock=lambda: 100_000)
        payload = json.dumps({"prompt": "x" * 65_536})
        with broker.db:
            broker.db.executemany(
                "INSERT INTO jobs(id,profile,kind,source,payload,state,created,queued_at) "
                "VALUES(?,?,?,?,?,?,?,?)",
                [
                    (f"queued-{index}", "interactive", "generate", "payload-source",
                     payload, "queued", float(index), float(index))
                    for index in range(500)
                ],
            )
        state_plan = " ".join(
            row[3] for row in broker.db.execute(
                "EXPLAIN QUERY PLAN SELECT DISTINCT source FROM jobs "
                "INDEXED BY jobs_state_source WHERE state IN ('queued','running','cancel_requested')"
            )
        )
        retry_plan = " ".join(
            row[3] for row in broker.db.execute(
                "EXPLAIN QUERY PLAN SELECT count(*) FROM jobs "
                "INDEXED BY jobs_source_state_retry WHERE source=? AND state='queued' AND retry_count>0",
                ("payload-source",),
            )
        )
        candidate_plan = " ".join(
            row[3] for row in broker.db.execute(
                "EXPLAIN QUERY PLAN SELECT id,profile,source,created,queued_at,attempt_count "
                "FROM jobs INDEXED BY jobs_queued_candidates WHERE state='queued' "
                "ORDER BY queued_at,id"
            )
        )
        self.assertIn("COVERING INDEX jobs_state_source", state_plan)
        self.assertIn("COVERING INDEX jobs_source_state_retry", retry_plan)
        self.assertIn("COVERING INDEX jobs_queued_candidates", candidate_plan)
        data = snapshot(broker.db, now=100_000, policy_snapshot={"sources": {}})
        self.assertEqual(data["overall"]["states"]["queued"], 500)
        for _ in range(3):
            observed = broker.dashboard()
            self.assertEqual(observed["observation"]["state"], "live")
            self.assertEqual(observed["overall"]["states"]["queued"], 500)
            forecast = broker.forecast()
            self.assertNotIn("unavailable", forecast)

    def test_dashboard_endpoints_report_unavailable_observer_instead_of_empty_queue(self):
        db = tempfile.NamedTemporaryFile()
        self.addCleanup(db.close)
        broker = Broker(db.name, FakeOllama(), FakeWol())
        broker.submit("interactive", "generate", {"prompt": "must not become zero"})
        server = serve(broker, port=0)
        self.addCleanup(server.server_close)

        def get(path):
            worker = threading.Thread(target=server.handle_request)
            worker.start()
            try:
                with urlopen(f"http://127.0.0.1:{server.server_port}{path}") as response:
                    status, body = response.status, response.read()
            except HTTPError as error:
                status, body = error.code, error.read()
            worker.join(timeout=1)
            return status, body

        with patch(
            "broker.service.sqlite3.connect",
            side_effect=sqlite3.OperationalError("database is locked"),
        ):
            api_status, api_body = get("/v1/dashboard")
            html_status, html_body = get("/dashboard")

        data = json.loads(api_body)
        self.assertEqual(api_status, 503)
        self.assertEqual(data["observation"]["state"], "unavailable")
        self.assertEqual(data["observation"]["sqlite_error"], "SQLITE_BUSY")
        self.assertNotIn("overall", data)
        self.assertNotIn("sources", data)
        self.assertEqual(html_status, 503)
        self.assertIn(b"Dashboard data unavailable", html_body)
        self.assertNotIn(b"Completed:", html_body)

        empty_db = tempfile.NamedTemporaryFile()
        self.addCleanup(empty_db.close)
        empty = Broker(empty_db.name, FakeOllama(), FakeWol()).dashboard()
        self.assertEqual(empty["observation"]["state"], "live")
        self.assertEqual(empty["overall"]["states"]["queued"], 0)

    def test_html_renders_active_timestamps_in_tbilisi_and_nulls_as_dash(self):
        timestamp = datetime(2026, 8, 29, 8, 30, tzinfo=timezone.utc).timestamp()
        html = render({
            "timestamp": timestamp,
            "sources": [],
            "active_jobs": [{
                "id": "active-job", "source": "interactive", "state": "running",
                "started": timestamp, "lease_until": None, "attempt_count": 1, "retry_count": 0,
            }],
            "overall": {
                "states": {"completed": 0},
                "completed_last_hour": 0,
                "completed_last_24_hours": 0,
            },
        }).decode()
        self.assertIn("2026-08-29 12:30:00 UTC+04:00 (Asia/Tbilisi)", html)
        self.assertNotIn(f">{timestamp}<", html)
        self.assertIn(
            'title="2026-08-29T08:30:00Z (epoch 1787992200.0)"', html,
        )
        self.assertIn("<td>—</td>", html)

    def test_dashboard_is_payload_free_and_uses_finished_completion_windows(self):
        clock = lambda: 100_000
        db = tempfile.NamedTemporaryFile()
        self.addCleanup(db.close)
        broker = Broker(db.name, FakeOllama(), FakeWol(), clock=clock)
        policy_file = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
        json.dump({"version": 1, "sources": {
            "dashboard-source": {"enabled": True, "weight": 3.0},
            "disabled-source": {"enabled": False, "weight": 8.0},
        }}, policy_file); policy_file.close()
        self.addCleanup(lambda: __import__("os").unlink(policy_file.name))
        policy = SourcePolicy(policy_file.name)
        completed_recent = broker.submit("batch-video", "generate", {"prompt": "do-not-expose"}, source="dashboard-source")["id"]
        completed_old = broker.submit("batch-video", "generate", {"prompt": "also-private"}, source="dashboard-source")["id"]
        running = broker.submit("batch-video", "generate", {}, source="dashboard-source")["id"]
        with broker.db:
            broker.db.execute("UPDATE jobs SET state='completed',finished=? WHERE id=?", (99_000, completed_recent))
            broker.db.execute("UPDATE jobs SET state='completed',finished=? WHERE id=?", (10_000, completed_old))
            broker.db.execute("UPDATE jobs SET state='running',started=?,lease_until=? WHERE id=?", (99_900, 100_300, running))
            broker.db.execute(
                "INSERT INTO audit_events(occurred,event_type,job_id,source) VALUES(?,?,?,?)",
                (99_000, "job.completed", completed_recent, "dashboard-source"),
            )
            broker.db.execute(
                "INSERT INTO audit_events(occurred,event_type,job_id,source) VALUES(?,?,?,?)",
                (10_000, "job.completed", completed_old, "dashboard-source"),
            )
        server = serve(broker, port=0, policy=policy)
        self.addCleanup(server.server_close)

        def get(path):
            worker = threading.Thread(target=server.handle_request); worker.start()
            with urlopen(f"http://127.0.0.1:{server.server_port}{path}") as response:
                body, content_type = response.read(), response.headers["Content-Type"]
            worker.join(timeout=1)
            return body, content_type

        def post(path, payload):
            request = Request(
                f"http://127.0.0.1:{server.server_port}{path}",
                data=json.dumps(payload).encode(),
                headers={"Content-Type": "application/json"}, method="POST",
            )
            worker = threading.Thread(target=server.handle_request); worker.start()
            try:
                with urlopen(request) as response:
                    status, body = response.status, response.read()
            except HTTPError as error:
                status, body = error.code, error.read()
            worker.join(timeout=1)
            return status, json.loads(body)

        body, _ = get("/v1/dashboard")
        data = json.loads(body)
        source = next(item for item in data["sources"] if item["source"] == "dashboard-source")
        self.assertEqual(source["scheduler"], {"enabled": True, "weight": 3.0, "next_allowed": None})
        self.assertEqual(source["states"]["lease"], 1)
        self.assertEqual(source["states"]["completed"], 2)
        self.assertEqual(source["completed_last_hour"], 1)
        self.assertEqual(source["completed_last_24_hours"], 1)
        self.assertNotIn("payload", json.dumps(data))
        broker.submit("batch-video", "generate", {}, source="omitted-source")
        data = broker.dashboard(policy)
        omitted = next(item for item in data["sources"] if item["source"] == "omitted-source")
        self.assertEqual(omitted["scheduler"], {
            "enabled": False, "weight": None, "next_allowed": None,
        })
        html, content_type = get("/dashboard")
        self.assertIn("text/html", content_type)
        self.assertIn(b"Completed 1h", html)
        self.assertIn(b"Weight", html)
        self.assertIn(b'<select name="weight" aria-label="Weight for dashboard-source">', html)
        self.assertIn(b'<option value="3" selected>3</option>', html)
        self.assertNotIn(b">3.0<", html)
        self.assertIn(b"weight-feedback", html)
        self.assertNotIn(b"Dead", html)
        self.assertIn(b'<tr><td colspan=13>None</td></tr>', render({
            "timestamp": 0,
            "sources": [],
            "active_jobs": [],
            "overall": {
                "states": {"completed": 0},
                "completed_last_hour": 0,
                "completed_last_24_hours": 0,
            },
        }))
        self.assertNotIn(b"do-not-expose", html)
        status, result = post("/v1/sources/dashboard-source/weight", {"weight": 7})
        self.assertEqual((status, result), (200, {"source": "dashboard-source", "weight": 7}))
        self.assertEqual(SourcePolicy(policy_file.name).weight("dashboard-source"), 7.0)
        with open(policy_file.name) as handle:
            self.assertEqual(json.load(handle)["sources"]["dashboard-source"]["weight"], 7)
        for invalid_weight in (0, 11, 3.5, "3", True):
            status, result = post("/v1/sources/dashboard-source/weight", {"weight": invalid_weight})
            self.assertEqual(status, 400)
            self.assertIn("integer from 1 through 10", result["error"])
        self.assertEqual(SourcePolicy(policy_file.name).weight("dashboard-source"), 7.0)


if __name__ == "__main__":
    unittest.main()
