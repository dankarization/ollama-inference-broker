import json
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from urllib.request import urlopen

from broker.dashboard import render
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
        self.assertNotIn(str(timestamp), html)
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
        server = serve(broker, port=0, policy=policy)
        self.addCleanup(server.server_close)

        def get(path):
            worker = threading.Thread(target=server.handle_request); worker.start()
            with urlopen(f"http://127.0.0.1:{server.server_port}{path}") as response:
                body, content_type = response.read(), response.headers["Content-Type"]
            worker.join(timeout=1)
            return body, content_type

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
        self.assertNotIn(b"do-not-expose", html)


if __name__ == "__main__":
    unittest.main()
