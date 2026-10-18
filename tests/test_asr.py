import hashlib
import http.client
import io
import json
import os
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import Mock

from broker.asr import ASRAPI, MODEL, PROFILE, Spool, WhisperExecutor, validate_payload
from broker.http import serve
from broker.service import Broker

TOKEN = "test-only-credential-" * 2

class FakeOllama:
    def __init__(self): self.models, self.calls = [], []
    def ps(self, **kwargs): return {"models": [{"name": name} for name in self.models]}
    def is_ready(self, model): return model in self.models
    def unload(self, model): raise AssertionError("ASR must never unload another model")
    def run(self, kind, request):
        self.calls.append(request)
        self.models = [request["model"]]
        return {"done": True}

class FakeWorker:
    def __init__(self):
        self.last, self.released, self.active, self.fail = None, False, 0, False
        self.model_loaded = False
        self.calls = []
    def status(self):
        return {"model": MODEL, "active_jobs": self.active, "model_loaded": self.model_loaded,
                "request_tracking": True, "last_request_id": self.last,
                "last_request_gpu_released": self.released}
    def transcribe(self, path, request_id):
        self.calls.append(request_id)
        self.last = request_id
        if self.fail: raise TimeoutError("private details must not enter database")
        self.released = True
        return {"text": "Russian transcript: ручей и тропинка", "model": MODEL, "device": "cuda"}

    def release(self):
        self.model_loaded = False
        self.released = True

class ASRTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.spool = Spool(self.root / "audio")
        self.worker, self.ollama = FakeWorker(), FakeOllama()
        self.executor = WhisperExecutor(self.spool, self.worker)
        self.broker = Broker(self.root / "queue.db", self.ollama, Mock(), whisper_executor=self.executor)
        self.addCleanup(self.temp.cleanup)
        self.addCleanup(self.broker.db.close)

    def payload(self, key="test-correlation-001", data=b"fake audio"):
        return self.spool.stage(key, io.BytesIO(data), len(data), Mock())

    def job(self, key="test-correlation-001"):
        return self.broker.submit(PROFILE, "transcribe", self.payload(key, b"x" * 1024), external_id=key)

    def test_raw_media_and_text_stay_outside_sqlite(self):
        job = self.job()
        self.assertTrue(self.broker.dispatch_once())
        state = self.broker.status(job["id"])
        self.assertEqual(state["state"], "completed")
        self.assertIsNone(self.broker.executor_fence())
        self.assertNotIn("text", state["result"])
        self.assertIn("ручей", self.spool.result(state["result"]["artifact"])["text"])
        self.assertNotIn("ручей", "\n".join(self.broker.db.iterdump()))
        self.assertFalse(self.ollama.calls)

    def test_timeout_fence_blocks_llm_until_same_request_acknowledged(self):
        self.worker.fail = True
        job = self.job()
        self.broker.dispatch_once()
        self.assertEqual(self.broker.status(job["id"])["state"], "failed")
        self.assertNotIn("private", self.broker.status(job["id"])["error"])
        self.broker.submit("interactive", "generate", {"prompt": "hello"})
        self.assertFalse(self.broker.dispatch_once())
        self.worker.released = True
        real = self.worker.last
        self.worker.last = "other-request"
        self.assertFalse(self.broker.dispatch_once())
        self.worker.last = real
        self.assertTrue(self.broker.dispatch_once())
        self.assertTrue(self.ollama.calls)

    def test_warm_whisper_reuses_owner_fence_for_next_asr(self):
        first = self.job("warm-first-correlation")
        self.assertTrue(self.broker.dispatch_once())
        self.assertEqual(self.broker.status(first["id"])["state"], "completed")
        self.worker.model_loaded = True
        self.worker.released = False
        second = self.job("warm-second-correlation")
        self.broker.set_executor_fence(self.worker.last, first["id"])
        self.assertTrue(self.broker.dispatch_once())
        self.assertEqual(self.broker.status(second["id"])["state"], "completed")
        self.assertIsNotNone(self.broker.executor_fence())
        self.assertEqual(self.broker.executor_fence()["job_id"], second["id"])

    def test_explicit_idle_release_clears_fence_after_worker_ack(self):
        self.worker.model_loaded = True
        job = self.job("warm-release-001")
        self.worker.last = "release-request"
        self.broker.set_executor_fence("release-request", job["id"])
        self.assertTrue(self.executor.release_idle(self.broker))
        self.assertIsNone(self.broker.executor_fence())

    def test_fence_survives_restart_and_disabled_executor(self):
        self.worker.fail = True
        self.job(); self.broker.dispatch_once()
        other = Broker(self.root / "queue.db", self.ollama, Mock())
        try:
            other.submit("interactive", "generate", {"prompt": "hello"})
            self.assertIsNotNone(other.executor_fence())
            self.assertFalse(other.dispatch_once())
        finally: other.db.close()

    def test_ollama_resident_model_is_never_unloaded(self):
        self.ollama.models = ["someone-elses-model"]
        job = self.job(); self.broker.dispatch_once()
        self.assertEqual(self.ollama.models, ["someone-elses-model"])
        self.assertFalse(self.worker.calls)
        self.assertIsNone(self.broker.executor_fence())
        self.assertEqual(self.broker.status(job["id"])["state"], "queued")
        self.ollama.models = []
        self.broker._asr_ready_after = 0
        self.assertTrue(self.broker.dispatch_once())
        self.assertEqual(self.broker.status(job["id"])["state"], "completed")

    def test_idempotency_reuses_job_and_rejects_different_audio(self):
        job = self.job()
        self.assertEqual(job["id"], self.job()["id"])
        self.broker.dispatch_once()
        self.assertEqual(job["id"], self.job()["id"])
        payload = self.payload("test-correlation-002", b"other")
        with self.assertRaises(ValueError):
            self.broker.submit(PROFILE, "transcribe", payload, external_id="test-correlation-001")

    def test_admission_cleanup_preserves_idempotent_artifact(self):
        job = self.job()
        existing = job["payload"]["artifact"]
        self.assertFalse(self.spool.discard_if_unowned(self.broker, job["external_id"], existing))
        self.assertTrue(self.spool.audio(job["payload"]).exists())
        orphan = self.payload("orphan-after-admission-error")
        self.assertTrue(self.spool.discard_if_unowned(self.broker, "orphan-after-admission-error", orphan["artifact"]))
        self.assertFalse(self.spool.path(orphan["artifact"], ".audio").exists())

    def test_audio_hash_path_and_size_are_checked(self):
        payload = self.payload()
        self.spool.path(payload["artifact"], ".audio").write_bytes(b"tampered")
        with self.assertRaises(ValueError): self.spool.audio(payload)
        for value in ("../secret", "http://internal/", "a"*63):
            with self.assertRaises(ValueError): validate_payload({**payload, "artifact": value})

    def test_queued_cancel_does_not_start_worker(self):
        job = self.job()
        self.broker.cancel(job["id"])
        self.assertFalse(self.broker.dispatch_once())
        self.assertFalse(self.worker.calls)

    def test_running_cancel_keeps_common_claim_until_worker_returns(self):
        entered, finish = threading.Event(), threading.Event()
        original = self.worker.transcribe
        def slow(path, request_id):
            entered.set(); finish.wait(5)
            return original(path, request_id)
        self.worker.transcribe = slow
        job = self.job()
        thread = threading.Thread(target=self.broker.dispatch_once)
        thread.start()
        try:
            self.assertTrue(entered.wait(3))
            self.broker.cancel(job["id"])
            self.broker.submit("interactive", "generate", {"prompt": "hello"})
            self.assertFalse(self.broker.dispatch_once())
        finally:
            finish.set(); thread.join(5)
        self.assertEqual(self.broker.status(job["id"])["state"], "cancelled")

    def test_spool_budget_and_conflicting_upload(self):
        payload = self.payload()
        with self.assertRaises(ValueError): self.payload(data=b"different")
        self.spool.max_bytes = 1
        with self.assertRaises(ValueError): self.payload("test-correlation-002")
        self.assertEqual(self.spool.audio(payload).read_bytes(), b"fake audio")

    def test_expiry_preserves_queued_artifacts_and_all_pending_fence_artifacts(self):
        job = self.job()
        orphan = self.payload("test-orphan-correlation")
        for path in self.spool.root.iterdir(): os.utime(path, (1, 1))
        self.broker.set_executor_fence("pending-request", job["id"])
        self.spool.prune(self.broker, now=100000)
        self.assertTrue(self.spool.path(orphan["artifact"], ".audio").exists())
        self.assertEqual(self.broker.health()["blocked_job_id"], job["id"])
        self.broker.clear_executor_fence("pending-request")
        self.spool.prune(self.broker, now=100000)
        self.assertFalse(self.spool.path(orphan["artifact"], ".audio").exists())
        self.assertTrue(self.spool.audio(job["payload"]).exists())

    def start_http(self):
        api = ASRAPI(self.broker, self.executor, TOKEN)
        server = serve(self.broker, port=0, asr_api=api)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return server.server_port

    def request(self, port, method, path, data=None, token=TOKEN, **headers):
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=3)
        headers["Authorization"] = "Bearer " + token
        try:
            conn.request(method, path, data, headers)
            response = conn.getresponse()
            return response.status, json.loads(response.read())
        finally: conn.close()

    def test_http_auth_upload_poll_ack_and_legacy_bypass(self):
        port = self.start_http()
        self.assertEqual(self.request(port, "POST", "/v1/transcriptions", b"audio", token="bad")[0], 401)
        status, job = self.request(port, "POST", "/v1/transcriptions", b"audio",
                                   **{"Idempotency-Key": "http-test-correlation-001"})
        self.assertEqual(status, 202)
        path = "/v1/transcriptions/" + job["id"]
        self.assertEqual(self.request(port, "GET", path, token="bad")[0], 401)
        bypass = json.dumps({"profile": PROFILE, "kind": "transcribe", "payload": {}})
        self.assertEqual(self.request(port, "POST", "/v1/jobs", bypass)[0], 403)
        self.assertEqual(self.request(port, "POST", "/v1/jobs/"+job["id"]+"/cancel", "{}")[0], 403)
        self.broker.dispatch_once()
        status, result = self.request(port, "GET", path)
        self.assertEqual(status, 200)
        self.assertIn("text", result["result"])
        public = self.request(port, "GET", "/v1/jobs/" + job["id"])[1]
        self.assertNotIn("text", public["result"])
        self.assertEqual(self.request(port, "POST", path+"/ack")[0], 200)
        self.assertEqual(self.request(port, "GET", path)[0], 410)

if __name__ == "__main__": unittest.main()
