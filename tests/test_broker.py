import tempfile
import threading
import unittest
from urllib.request import urlopen

from broker.http import serve
from broker.__main__ import dispatch_enabled, dispatch_sources
from broker.adapters import OllamaHTTP
from broker.service import Broker


class FakeWol:
    def __init__(self, calls): self.calls = calls
    def wake(self): self.calls.append("wake")


class FakeOllama:
    def __init__(self, calls, loaded=None): self.calls, self.loaded = calls, list(loaded or [])
    def ps(self): self.calls.append("ps"); return {"models": [{"name": x, "size_vram": 1} for x in self.loaded]}
    def is_ready(self, model): self.calls.append(("ready", model)); return model in self.loaded
    def unload(self, model): self.calls.append(("unload", model)); self.loaded.remove(model)
    def run(self, kind, request):
        self.calls.append(("run", kind, request.copy()))
        if request.get("prompt") == "" and request["model"] not in self.loaded: self.loaded.append(request["model"])
        return {"done": True}


class BrokerTests(unittest.TestCase):
    def make(self, loaded=None, clock=None):
        self.calls=[]; self.tmp=tempfile.NamedTemporaryFile(); self.ol=FakeOllama(self.calls, loaded)
        return Broker(self.tmp.name, self.ol, FakeWol(self.calls), clock=clock or __import__("time").time)
    def test_dispatch_enabled_requires_an_explicit_valid_boolean(self):
        self.assertTrue(dispatch_enabled(None))
        self.assertTrue(dispatch_enabled("YES"))
        self.assertFalse(dispatch_enabled("false"))
        with self.assertRaisesRegex(ValueError, "must be true or false"):
            dispatch_enabled("later")

    def test_dispatch_source_allowlist_requires_a_non_empty_value(self):
        self.assertEqual(
            dispatch_sources("pilot-mainpc, pilot-secondary"),
            {"pilot-mainpc", "pilot-secondary"},
        )
        with self.assertRaisesRegex(ValueError, "is required"):
            dispatch_sources(None)
        with self.assertRaisesRegex(ValueError, "at least one source"):
            dispatch_sources(" , ")

    def test_ollama_timeout_must_be_positive(self):
        with self.assertRaisesRegex(ValueError, "must be positive"):
            OllamaHTTP(timeout_seconds=0)
    def test_fixed_source_priorities_then_fifo(self):
        b=self.make(["nemotron3:33b"], clock=iter(range(1_000)).__next__)
        cron=b.submit("cron", "generate", {"prompt":"cron"})["id"]
        first=b.submit("interactive", "generate", {"prompt":"one"})["id"]
        second=b.submit("interactive", "generate", {"prompt":"two"})["id"]
        shutterstock_video=b.submit("shutterstock-video", "generate", {"prompt":"video"})["id"]
        olya=b.submit("olya", "generate", {"prompt":"olya"})["id"]
        b.dispatch_once(); self.assertEqual(b.status(first)["state"], "completed")
        b.dispatch_once(); self.assertEqual(b.status(second)["state"], "completed")
        b.dispatch_once(); self.assertEqual(b.status(cron)["state"], "completed")
        b.dispatch_once(); self.assertEqual(b.status(shutterstock_video)["state"], "completed")
        b.dispatch_once(); self.assertEqual(b.status(olya)["state"], "completed")

    def test_fixed_source_mapping_is_server_owned(self):
        b=self.make()
        self.assertEqual(b.submit("interactive", "generate", {"prompt":"x"})["priority"], 1)
        self.assertEqual(b.submit("cron", "generate", {"prompt":"x"})["priority"], 2)
        video = b.submit("shutterstock-video", "generate", {"prompt":"x"})
        self.assertEqual(video["priority"], 5)
        self.assertEqual(video["profile"], "shutterstock-video")
        with self.assertRaisesRegex(ValueError, "unknown profile"):
            b.submit("shutterstock", "generate", {"prompt":"photo"})
        self.assertEqual(b.submit("olya", "generate", {"prompt":"x"})["priority"], 8)
        with self.assertRaisesRegex(ValueError, "fixed priority 1"):
            b.submit("interactive", "generate", {"prompt":"x"}, priority=10)

    def test_dynamic_priority_requires_integer_in_range(self):
        b=self.make()
        job=b.submit("batch-video", "generate", {"prompt":"x"}, source="another-submitters", priority=7)
        self.assertEqual(job["priority"], 7)
        for bad in (None, 0, 11, True, "3"):
            with self.assertRaisesRegex(ValueError, "integer from 1 to 10"):
                b.submit("batch-video", "generate", {"prompt":"x"}, source="another-submitters", priority=bad)

    def test_dispatch_allowlist_leaves_non_pilot_work_queued(self):
        b=self.make(["nemotron3:33b"])
        blocked=b.submit("interactive", "generate", {"prompt":"do not run"})["id"]
        pilot=b.submit("interactive", "generate", {"prompt":"pilot"}, source="pilot-mainpc", priority=1)["id"]
        self.assertTrue(b.dispatch_once(frozenset({"pilot-mainpc"})))
        self.assertEqual(b.status(pilot)["state"], "completed")
        self.assertEqual(b.status(blocked)["state"], "queued")
    def test_wol_readiness_unload_and_server_limits(self):
        b=self.make(["incompatible-model:1"])
        job=b.submit("interactive", "generate", {"model":"evil", "prompt":"x", "keep_alive":"forever", "options":{"num_ctx":999999,"num_predict":999999}})["id"]
        b.dispatch_once()
        self.assertEqual(b.status(job)["state"], "completed")
        self.assertLess(self.calls.index(("unload", "incompatible-model:1")), next(i for i,x in enumerate(self.calls) if isinstance(x,tuple) and x[0]=="run"))
        last=[x for x in self.calls if isinstance(x,tuple) and x[0]=="run" and x[2].get("prompt")=="x"][0][2]
        self.assertEqual(last["model"], "nemotron3:33b")
        self.assertEqual(last["options"], {"num_ctx":16384,"num_predict":2048})
        self.assertEqual(self.calls[0], "wake")
    def test_shutterstock_video_switches_to_nemotron_with_server_owned_limits(self):
        b=self.make(["incompatible-model:1"])
        job=b.submit("shutterstock-video", "generate", {"prompt":"video", "options":{"num_ctx":999999,"num_predict":999999}})["id"]
        b.dispatch_once()
        self.assertEqual(b.status(job)["state"], "completed")
        self.assertEqual(b.status(job)["switch_reason"], "unloaded incompatible model before switch")
        self.assertIn(("unload", "incompatible-model:1"), self.calls)
        request=[x[2] for x in self.calls if isinstance(x,tuple) and x[0]=="run" and x[2].get("prompt")=="video"][0]
        self.assertEqual(request["model"], "nemotron3:33b")
        self.assertEqual(request["options"], {"num_ctx":16384,"num_predict":1024})
    def test_cancel_queued_and_recover_expired_lease(self):
        b=self.make(["nemotron3:33b"])
        queued=b.submit("cron", "generate", {"prompt":"x"})["id"]
        self.assertEqual(b.cancel(queued)["state"], "cancelled")
        running=b.submit("interactive", "generate", {"prompt":"y"})["id"]
        with b.db: b.db.execute("UPDATE jobs SET state='running', lease_until=0 WHERE id=?", (running,))
        b.recover()
        self.assertEqual(b.status(running)["state"], "queued")
        b.dispatch_once(); self.assertEqual(b.status(running)["state"], "completed")
    def test_metrics_include_queue_and_vram_source(self):
        b=self.make(["nemotron3:33b"]); b.submit("cron", "generate", {"prompt":"x"})
        data=b.metrics(); self.assertEqual(data["resource"], "mainpc-gpu"); self.assertEqual(data["queue_depth"], 1); self.assertEqual(data["loaded_models"][0]["size_vram"], 1)

    def test_local_health_never_touches_wol_or_ollama(self):
        b=self.make(); b.submit("cron", "generate", {"prompt":"x"})
        data=b.health()
        self.assertEqual(data["status"], "ready")
        self.assertEqual(data["queue_depth"], 1)
        self.assertIsNone(data["active_job_id"])
        self.assertEqual(self.calls, [])

    def test_healthz_http_endpoint_is_local_only(self):
        b=self.make()
        server=serve(b, port=0)
        thread=threading.Thread(target=server.handle_request)
        thread.start()
        try:
            with urlopen(f"http://127.0.0.1:{server.server_port}/healthz") as response:
                self.assertEqual(response.status, 200)
                self.assertEqual(__import__("json").loads(response.read())["status"], "ready")
        finally:
            thread.join(timeout=1)
            server.server_close()
        self.assertEqual(self.calls, [])

if __name__ == "__main__": unittest.main()
