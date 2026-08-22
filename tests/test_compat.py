import json
import tempfile
import threading
import unittest
from urllib.request import Request, urlopen

from broker.compat import CompatibilityError, stream_frames, submit
from broker.http import serve
from broker.service import Broker


class NoExternalWol:
    def wake(self): raise AssertionError("compatibility admission must not send WOL")


class NoExternalOllama:
    def ps(self): raise AssertionError("compatibility admission must not query Ollama")


class CompatibilityTests(unittest.TestCase):
    def make(self):
        self.tmp = tempfile.NamedTemporaryFile()
        return Broker(self.tmp.name, NoExternalOllama(), NoExternalWol(), clock=iter(range(1000)).__next__)

    def test_generate_is_admitted_without_model_escalation_or_external_calls(self):
        broker = self.make()
        job = submit(broker, "generate", {"profile": "interactive", "model": "nemotron3:33b", "prompt": "test", "keep_alive": "forever", "options": {"num_ctx": 999999, "num_predict": 999999}})
        self.assertEqual(job["state"], "queued")
        self.assertEqual(job["payload"]["options"], {"num_ctx": 999999, "num_predict": 999999})
        self.assertNotIn("model", job["payload"]); self.assertNotIn("keep_alive", job["payload"])

    def test_request_validation_rejects_unknown_profile_and_model_override(self):
        broker = self.make()
        with self.assertRaisesRegex(CompatibilityError, "server-side profile"): submit(broker, "generate", {"prompt": "x"})
        with self.assertRaisesRegex(CompatibilityError, "server-side profile"): submit(broker, "generate", {"profile": "shutterstock", "prompt": "photo"})
        with self.assertRaisesRegex(CompatibilityError, "caller model"): submit(broker, "generate", {"profile": "cron", "model": "evil", "prompt": "x"})
        with self.assertRaisesRegex(CompatibilityError, "messages"): submit(broker, "chat", {"profile": "interactive", "messages": []})

    def test_admission_uses_strict_broker_priority_order(self):
        broker = self.make()
        low = submit(broker, "generate", {"profile": "olya", "prompt": "low"})
        high = submit(broker, "generate", {"profile": "interactive", "prompt": "high"})
        self.assertEqual(low["queue_position"], 1)
        self.assertEqual(broker.status(low["id"])["queue_position"], 2)
        self.assertEqual(high["queue_position"], 1)

    def test_ndjson_frames_cover_queued_cancellation_and_failure(self):
        broker = self.make()
        queued = submit(broker, "generate", {"profile": "cron", "prompt": "queued"})
        admission = [json.loads(frame) for frame in stream_frames(queued)]
        self.assertEqual(admission[0]["broker"]["state"], "queued"); self.assertFalse(admission[0]["done"])
        cancellation = [json.loads(frame) for frame in stream_frames(broker.cancel(queued["id"]))]
        self.assertEqual(cancellation[-1]["done_reason"], "cancelled")
        failed = submit(broker, "generate", {"profile": "cron", "prompt": "failed"})
        with broker.db: broker.db.execute("UPDATE jobs SET state='failed', error='synthetic' WHERE id=?", (failed["id"],))
        failure = [json.loads(frame) for frame in stream_frames(broker.status(failed["id"]))]
        self.assertTrue(failure[-1]["done"]); self.assertEqual(failure[-1]["error"], "broker job failed")

    def test_http_generate_streams_only_admission_without_dispatch(self):
        broker = self.make(); server = serve(broker, port=0)
        thread = threading.Thread(target=server.handle_request); thread.start()
        try:
            request = Request(
                f"http://127.0.0.1:{server.server_port}/api/generate",
                data=json.dumps({"profile": "cron", "prompt": "local", "stream": True}).encode(),
                headers={"Content-Type": "application/json"}, method="POST",
            )
            with urlopen(request) as response:
                self.assertEqual(response.status, 202)
                self.assertEqual(response.headers["Content-Type"], "application/x-ndjson")
                frame = json.loads(response.read().splitlines()[0])
            self.assertEqual(frame["broker"]["state"], "queued")
            self.assertFalse(frame["done"])
        finally:
            thread.join(timeout=1); server.server_close()
