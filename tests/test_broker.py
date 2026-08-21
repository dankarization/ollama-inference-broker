import tempfile
import unittest

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
    def make(self, loaded=None):
        self.calls=[]; self.tmp=tempfile.NamedTemporaryFile(); self.ol=FakeOllama(self.calls, loaded)
        return Broker(self.tmp.name, self.ol, FakeWol(self.calls))
    def test_priority_then_fifo(self):
        b=self.make(["nemotron3:33b"])
        cron=b.submit("cron", "generate", {"prompt":"cron"})["id"]
        first=b.submit("interactive", "generate", {"prompt":"one"})["id"]
        second=b.submit("interactive", "generate", {"prompt":"two"})["id"]
        b.dispatch_once(); self.assertEqual(b.status(first)["state"], "completed")
        b.dispatch_once(); self.assertEqual(b.status(second)["state"], "completed")
        b.dispatch_once(); self.assertEqual(b.status(cron)["state"], "completed")
    def test_wol_readiness_unload_and_server_limits(self):
        b=self.make(["qwen3-vl:32b"])
        job=b.submit("interactive", "generate", {"model":"evil", "prompt":"x", "keep_alive":"forever", "options":{"num_ctx":999999,"num_predict":999999}})["id"]
        b.dispatch_once()
        self.assertEqual(b.status(job)["state"], "completed")
        self.assertLess(self.calls.index(("unload", "qwen3-vl:32b")), next(i for i,x in enumerate(self.calls) if isinstance(x,tuple) and x[0]=="run"))
        last=[x for x in self.calls if isinstance(x,tuple) and x[0]=="run" and x[2].get("prompt")=="x"][0][2]
        self.assertEqual(last["model"], "nemotron3:33b")
        self.assertEqual(last["options"], {"num_ctx":16384,"num_predict":2048})
        self.assertEqual(self.calls[0], "wake")
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

if __name__ == "__main__": unittest.main()
