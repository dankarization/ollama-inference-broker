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
from unittest.mock import Mock, patch

from broker.asr import ASRAPI, MODEL, PROFILE, MAX_RESULT_BYTES, Spool, WhisperExecutor, validate_payload
from broker.http import serve
from broker.service import Broker, Dispatcher, SourceQueueFull, SourceAdmissionBlocked, SourcePolicy
from broker.asr_recovery import recover

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
    def transcribe(self, path, request_id, previous_request_id=None):
        self.calls.append(request_id)
        self.last = request_id
        if self.fail: raise TimeoutError("private details must not enter database")
        self.released = True
        return {"text": "Russian transcript: ручей и тропинка", "model": MODEL, "device": "cuda"}

    def release(self, request_id):
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

    def test_warm_asr_can_pass_older_blocked_llm(self):
        first = self.job('warm-before-llm-correlation')
        self.broker.dispatch_once()
        self.worker.model_loaded, self.worker.released = True, False
        self.broker.set_executor_fence(self.worker.last, first['id'])
        llm = self.broker.submit('interactive','generate',{'prompt':'test'})
        second = self.job('warm-after-llm-correlation')
        self.assertTrue(self.broker.dispatch_once())
        self.assertEqual(self.broker.status(second['id'])['state'],'completed')
        self.assertEqual(self.broker.status(llm['id'])['state'],'queued')
        self.assertFalse(self.ollama.calls)

    def test_opt_in_llm_demand_releases_exact_idle_owner(self):
        first = self.job('warm-demand-release-correlation')
        self.broker.dispatch_once()
        self.worker.model_loaded, self.worker.released = True, False
        self.broker.set_executor_fence(self.worker.last, first['id'])
        owner = self.worker.last
        self.broker.asr_release_on_llm_demand = True
        self.worker.release = Mock(wraps=self.worker.release)
        llm = self.broker.submit('interactive','generate',{'prompt':'test'})
        self.assertTrue(self.broker.dispatch_once())
        self.worker.release.assert_called_once_with(owner)
        self.assertIsNone(self.broker.executor_fence())
        self.assertEqual(self.broker.status(llm['id'])['state'],'completed')

    def test_failed_demand_release_keeps_fence_and_does_not_claim_llm(self):
        first = self.job('warm-demand-failure-correlation')
        self.broker.dispatch_once()
        self.worker.model_loaded, self.worker.released = True, False
        self.broker.set_executor_fence(self.worker.last, first['id'])
        fence = self.broker.executor_fence()
        self.broker.asr_release_on_llm_demand = True
        self.worker.release = Mock(side_effect=OSError('injected release loss'))
        llm = self.broker.submit('interactive','generate',{'prompt':'test'})
        for _ in range(3): self.assertFalse(self.broker.dispatch_once())
        self.worker.release.assert_called_once()
        self.assertEqual(self.broker.executor_fence(),fence)
        self.assertEqual(self.broker.status(llm['id'])['state'],'queued')

    def test_api_rejects_reused_worker_token(self):
        self.executor.worker = __import__('broker.asr',fromlist=['WhisperHTTP']).WhisperHTTP('http://127.0.0.1:9',TOKEN)
        with self.assertRaisesRegex(ValueError,'credentials must be different'):
            ASRAPI(self.broker,self.executor,TOKEN)
        ASRAPI(self.broker,self.executor,TOKEN+'api')

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

    def test_warm_worker_without_durable_fence_fails_closed(self):
        self.worker.model_loaded = True
        self.job("warm-unfenced-correlation")
        self.assertFalse(self.broker.dispatch_once())
        self.assertIsNone(self.broker.executor_fence())

    def test_unusable_unfenced_allocation_also_blocks_ollama(self):
        status=self.worker.status()
        self.worker.status=Mock(return_value={**status,"model_loaded":True,"model_usable":False})
        self.broker.submit("interactive","generate",{"prompt":"test"})
        self.assertFalse(self.broker.dispatch_once())
        self.assertFalse(self.ollama.calls)

    def test_unrelated_warm_owner_cannot_replace_fence(self):
        job = self.job("warm-owner-correlation")
        self.broker.set_executor_fence("original-owner", job["id"])
        self.worker.model_loaded = True
        self.worker.last = "unrelated-owner"
        self.assertFalse(self.broker.dispatch_once())
        self.assertEqual(self.broker.executor_fence()["request_id"], "original-owner")
        self.assertFalse(self.worker.calls)

    def test_operator_recovery_requires_exact_identity_idle_proof_and_confirmations(self):
        job=self.job()
        self.broker.set_executor_fence("old-owner",job["id"])
        worker=Mock()
        worker.status.return_value={"model":MODEL,"active_jobs":0,"model_loaded":False,
                                    "gpu_release_confirmed":True,"last_request_id":None}
        arguments=(self.broker.database,worker,"old-owner",job["id"])
        with self.assertRaises(ValueError): recover(*arguments)
        worker.status.return_value["gpu_release_confirmed"]=False
        with self.assertRaises(ValueError): recover(*arguments,executor_stopped=True,dispatch_stopped=True)
        self.assertIsNotNone(self.broker.executor_fence())
        worker.status.return_value["gpu_release_confirmed"]=True
        with self.assertRaises(ValueError):
            recover(self.broker.database,worker,"wrong-owner",job["id"],executor_stopped=True,dispatch_stopped=True)
        recover(*arguments,executor_stopped=True,dispatch_stopped=True)
        self.assertIsNone(self.broker.executor_fence())
        self.assertEqual(self.broker.db.execute("SELECT request_id FROM asr_fence_recoveries").fetchone()[0],"old-owner")

    def test_probe_does_not_block_status_or_admission(self):
        job = self.job()
        entered, finish = threading.Event(), threading.Event()
        def probe(_ollama):
            entered.set(); finish.wait(3); return False
        with patch.object(self.executor, "available", side_effect=probe):
            thread = threading.Thread(target=self.broker.dispatch_once)
            thread.start()
            try:
                self.assertTrue(entered.wait(2))
                completed = threading.Event()
                def lookup():
                    self.broker.status(job["id"])
                    self.broker.submit("interactive", "generate", {"prompt": "test"})
                    completed.set()
                lookup_thread = threading.Thread(target=lookup)
                lookup_thread.start()
                self.assertTrue(completed.wait(0.5))
            finally:
                finish.set(); thread.join(3); lookup_thread.join(3)

    def test_unavailable_asr_does_not_starve_ollama_and_wakes_host(self):
        job = self.job()
        self.worker.status = Mock(side_effect=OSError("worker offline"))
        other = self.broker.submit("interactive", "generate", {"prompt": "test"})
        self.assertTrue(self.broker.dispatch_once())
        self.assertEqual(self.broker.status(job["id"])["state"], "queued")
        self.assertEqual(self.broker.status(other["id"])["state"], "completed")
        self.broker.wol.wake.assert_called()

    def test_wol_socket_failure_does_not_kill_dispatcher_or_claim_asr(self):
        job=self.job()
        ready_status=self.worker.status
        self.worker.status=Mock(side_effect=OSError('worker offline'))
        attempted=threading.Event()
        def fail_wake():
            attempted.set()
            raise OSError('injected WOL socket failure')
        self.broker.wol.wake=Mock(side_effect=fail_wake)
        dispatcher=Dispatcher(self.broker,interval=.01,allowed_sources=frozenset({PROFILE}))
        dispatcher.start()
        try:
            self.assertTrue(attempted.wait(2))
            time.sleep(.05)
            self.assertTrue(dispatcher.is_alive())
            state=self.broker.status(job['id'])
            self.assertEqual(state['state'],'queued')
            self.assertEqual(state['attempt_count'],0)
            self.worker.status=ready_status
            self.broker._asr_ready_after=0
            deadline=time.monotonic()+3
            while time.monotonic()<deadline and self.broker.status(job['id'])['state']!='completed':
                time.sleep(.01)
            self.assertEqual(self.broker.status(job['id'])['state'],'completed')
            self.assertTrue(dispatcher.is_alive())
        finally:
            dispatcher.stop_event.set();dispatcher.join(3)

    def test_empty_or_disabled_queue_does_not_probe_worker(self):
        with patch.object(self.worker,'status') as status:
            for _ in range(5): self.assertFalse(self.broker.dispatch_once())
            status.assert_not_called()
        self.job()
        with patch.object(self.worker,'status') as status:
            self.assertFalse(self.broker.dispatch_once(allowed_sources=frozenset()))
            status.assert_not_called()

    def test_idle_fence_reconciliation_is_throttled_but_can_clear(self):
        job = self.job()
        self.broker.dispatch_once()
        self.broker.set_executor_fence('idle-fence-request', job['id'])
        self.worker.last = 'idle-fence-request'
        self.worker.released = False
        with patch('broker.service.time.monotonic', return_value=100), patch.object(
                self.worker, 'status', wraps=self.worker.status) as status:
            for _ in range(5): self.assertFalse(self.broker.dispatch_once())
            self.assertEqual(status.call_count, 1)
            self.assertIsNotNone(self.broker.executor_fence())
            self.worker.released = True
            with patch('broker.service.time.monotonic', return_value=106):
                self.assertFalse(self.broker.dispatch_once())
            self.assertEqual(status.call_count, 2)
            self.assertIsNone(self.broker.executor_fence())

    def test_policy_lane_skips_unavailable_asr_without_charging_it(self):
        job=self.job()
        path=self.root/'policy.json'
        path.write_text(json.dumps({"version":1,"sources":{
            PROFILE:{"enabled":True,"weight":9},"interactive":{"enabled":True,"weight":1}}}))
        policy=SourcePolicy(path)
        with self.broker.lock:
            self.assertEqual(self.broker._next(policy=policy)["source"],PROFILE)
        self.worker.status=Mock(side_effect=OSError("worker offline"))
        other=self.broker.submit("interactive","generate",{"prompt":"test"})
        self.assertTrue(self.broker.dispatch_once(policy=policy))
        self.assertEqual(self.broker.status(job["id"])["attempt_count"],0)
        self.assertEqual(self.broker.status(other["id"])["state"],"completed")

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

    def test_retry_at_capacity_and_reserved_result(self):
        self.spool.max_bytes = len(b"fake audio") + MAX_RESULT_BYTES
        payload = self.payload()
        self.assertEqual(self.payload(), payload)
        with self.assertRaises(ValueError): self.payload("new-key-correlation")
        self.spool.save_result(payload["artifact"], {"model": MODEL, "text": "test"})
        self.assertLessEqual(self.spool.accounted_bytes(), self.spool.max_bytes)

    def test_spool_rejects_foreign_uid_even_with_private_mode(self):
        if os.name == "nt": self.skipTest("POSIX ownership check")
        with patch("broker.asr.os.getuid", return_value=os.getuid()+1):
            with self.assertRaisesRegex(ValueError, "owner-only"):
                Spool(self.spool.root)

    def test_artifact_rename_and_delete_fsync_directory(self):
        if os.name=="nt": self.skipTest("directory fsync on POSIX")
        import stat
        snapshots=[]; real_fsync=os.fsync
        def fsync(descriptor):
            if stat.S_ISDIR(os.fstat(descriptor).st_mode):
                snapshots.append(sorted(path.suffix for path in self.spool.root.iterdir()))
            return real_fsync(descriptor)
        with patch("broker.asr.os.fsync",side_effect=fsync):
            payload=self.payload()
            self.spool.save_result(payload["artifact"],{"text":"test"})
            self.spool.discard(payload["artifact"])
        self.assertIn([".audio"],snapshots)
        self.assertIn([".audio",".json"],snapshots)
        self.assertEqual(snapshots[-1],[])

    def test_http_admission_codes_are_preserved_and_orphans_removed(self):
        port=self.start_http()
        for error,expected in ((SourceAdmissionBlocked(PROFILE),403),(SourceQueueFull("test"),429)):
            with patch.object(self.broker,"submit",side_effect=error):
                self.assertEqual(self.request(port,"POST","/v1/transcriptions",b"audio",
                    **{"Idempotency-Key":"rejected-admission-key"})[0],expected)
            self.assertEqual(list(self.spool.root.iterdir()),[])

    def test_concurrent_same_key_rejection_cannot_delete_other_admission(self):
        port = self.start_http()
        original_submit = self.broker.submit
        original_discard = self.spool.discard_if_unowned
        entered, second_attempt, discarded = threading.Event(), threading.Event(), threading.Event()
        counter = 0
        def submit(*args, **kwargs):
            nonlocal counter
            counter += 1
            if counter == 1:
                entered.set(); second_attempt.wait(2); time.sleep(0.05)
                raise SourceQueueFull("test queue full")
            self.assertTrue(discarded.wait(2))
            return original_submit(*args, **kwargs)
        def discard(*args):
            try: return original_discard(*args)
            finally: discarded.set()
        responses = []
        def request(second=False):
            if second: second_attempt.set()
            responses.append(self.request(port, "POST", "/v1/transcriptions", b"audio",
                                          **{"Idempotency-Key": "concurrent-same-key"}))
        with patch.object(self.broker,"submit",side_effect=submit), \
             patch.object(self.spool,"discard_if_unowned",side_effect=discard):
            first = threading.Thread(target=request); first.start()
            self.assertTrue(entered.wait(2))
            second = threading.Thread(target=request,args=(True,));second.start()
            first.join(5);second.join(5)
        self.assertEqual(sorted(code for code,_ in responses),[202,429])
        job_id=next(data["id"] for code,data in responses if code==202)
        self.assertEqual(self.spool.audio(self.broker.status(job_id)["payload"]).read_bytes(),b"audio")

    def test_ack_while_warm_does_not_clear_fence_or_resurrect_media(self):
        port = self.start_http()
        key = "ack-warm-correlation"
        status, job = self.request(port, "POST", "/v1/transcriptions", b"audio",
                                   **{"Idempotency-Key": key})
        self.assertEqual(status, 202)
        self.broker.dispatch_once()
        self.broker.set_executor_fence("still-warm", job["id"])
        path = "/v1/transcriptions/" + job["id"]
        self.assertEqual(self.request(port, "POST", path + "/ack")[0], 200)
        self.assertEqual(self.broker.executor_fence()["request_id"], "still-warm")
        self.assertEqual(self.request(port, "POST", "/v1/transcriptions", b"audio",
                                     **{"Idempotency-Key": key})[0], 410)
        self.assertEqual(list(self.spool.root.iterdir()), [])

    def test_pruned_unacknowledged_terminal_upload_is_not_restaged(self):
        port=self.start_http()
        key='pruned-terminal-correlation'
        status,job=self.request(port,'POST','/v1/transcriptions',b'audio',**{'Idempotency-Key':key})
        self.assertEqual(status,202)
        self.broker.dispatch_once()
        self.assertEqual(self.broker.status(job['id'])['state'],'completed')
        for path in self.spool.root.iterdir(): os.utime(path,(1,1))
        self.spool.prune(self.broker,now=100000)
        self.assertEqual(list(self.spool.root.iterdir()),[])
        for _ in range(2):
            self.assertEqual(self.request(port,'POST','/v1/transcriptions',b'audio',**{'Idempotency-Key':key})[0],410)
            self.assertEqual(list(self.spool.root.iterdir()),[])

    def test_receipt_survives_metadata_purge_without_restaging(self):
        port=self.start_http(); key='purged-receipt-correlation'
        _,job=self.request(port,'POST','/v1/transcriptions',b'audio',**{'Idempotency-Key':key})
        self.broker.dispatch_once()
        self.assertEqual(self.request(port,'POST',f"/v1/transcriptions/{job['id']}/ack")[0],200)
        with self.broker.lock,self.broker.db:
            self.broker.db.execute("INSERT INTO job_retention_tombstones(job_id,source,external_id,profile,kind,final_state,created,purged_at) "
                "SELECT id,source,external_id,profile,kind,state,created,? FROM jobs WHERE id=?",(time.time(),job['id']))
            self.broker.db.execute('DELETE FROM jobs WHERE id=?',(job['id'],))
        with patch.object(self.spool,'stage',wraps=self.spool.stage) as stage:
            for _ in range(2):
                self.assertEqual(self.request(port,'POST','/v1/transcriptions',b'audio',**{'Idempotency-Key':key})[0],410)
                with self.broker.db: self.broker.db.execute('DELETE FROM job_retention_tombstones WHERE job_id=?',(job['id'],))
            stage.assert_not_called()

    def test_authenticated_retry_reuses_artifact_only_after_release(self):
        port=self.start_http(); key='explicit-retry-correlation'
        _,job=self.request(port,'POST','/v1/transcriptions',b'audio',**{'Idempotency-Key':key})
        self.worker.fail=True; self.broker.dispatch_once()
        retry=f"/v1/transcriptions/{job['id']}/retry"
        self.assertEqual(self.request(port,'POST',retry)[0],409)
        self.worker.released=True
        self.assertTrue(self.broker.reconcile_executor_fence())
        audio_files=list(self.spool.root.glob('*.audio'))
        self.assertEqual(self.request(port,'POST',retry)[0],202)
        self.assertEqual(list(self.spool.root.glob('*.audio')),audio_files)
        self.worker.fail=False; self.broker.dispatch_once()
        self.assertEqual(self.broker.status(job['id'])['state'],'completed')
        self.assertEqual(self.request(port,'POST',f"/v1/transcriptions/{job['id']}/ack")[0],200)
        self.assertEqual(self.request(port,'POST',retry)[0],410)

    def test_prune_skips_artifact_while_same_key_admission_is_active(self):
        key = 'prune-active-admission-key'
        payload = self.payload(key)
        audio = self.spool.audio(payload)
        os.utime(audio, (1, 1))
        acquired, release = threading.Event(), threading.Event()
        def hold_admission():
            with self.spool.admission_lock(key):
                acquired.set()
                release.wait(3)
        holder = threading.Thread(target=hold_admission)
        holder.start()
        try:
            self.assertTrue(acquired.wait(2))
            self.spool.prune(self.broker, now=100000)
            self.assertTrue(audio.is_file())
        finally:
            release.set(); holder.join(3)
        self.spool.prune(self.broker, now=100000)
        self.assertFalse(audio.exists())

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

    def test_incompatible_worker_allocations_block_all_gpu_dispatch(self):
        job = self.broker.submit('interactive', 'generate', {'prompt': 'test'})
        for allocation in ({'model_loaded': True}, {'active_jobs': 1},
                           {'gpu_release_confirmed': False}):
            with self.subTest(allocation=allocation):
                status = {'model': 'another-whisper-model', 'model_loaded': False,
                          'active_jobs': 0, 'gpu_release_confirmed': True, **allocation}
                with patch.object(self.worker, 'status', return_value=status):
                    self.assertFalse(self.broker.dispatch_once())
                self.assertEqual(self.broker.status(job['id'])['state'], 'queued')
                self.assertFalse(self.ollama.calls)

    def test_warm_admission_can_turn_cold_only_with_exact_release_proof(self):
        for confirmed in (False, True):
            with self.subTest(confirmed=confirmed):
                if self.broker.executor_fence():
                    self.broker.clear_executor_fence(self.broker.executor_fence()['request_id'])
                self.worker.model_loaded, self.worker.released = True, False
                self.worker.last = 'idle-race-old-owner'
                job = self.job('idle-race-correlation-' + str(confirmed))
                self.broker.set_executor_fence(self.worker.last, job['id'])
                original_run = self.executor.run
                def unload_then_run(broker, row):
                    self.worker.model_loaded = False
                    self.worker.released = confirmed
                    return original_run(broker, row)
                with patch.object(self.executor, 'run', side_effect=unload_then_run):
                    self.assertTrue(self.broker.dispatch_once())
                if confirmed:
                    self.assertEqual(self.broker.status(job['id'])['state'], 'completed')
                    self.assertIsNone(self.broker.executor_fence())
                    self.assertEqual(len(self.worker.calls), 1)
                else:
                    self.assertEqual(self.broker.status(job['id'])['state'], 'failed')
                    self.assertEqual(self.broker.executor_fence()['request_id'], 'idle-race-old-owner')
                    self.assertFalse(self.worker.calls)

    def test_disabled_asr_lane_does_not_probe_or_wake_before_llm(self):
        self.job()
        policy_path = self.root / 'policy.json'
        policy_path.write_text(json.dumps({'version': 1, 'sources': {
            PROFILE: {'enabled': False, 'weight': 1},
            'interactive': {'enabled': True, 'weight': 1}}}))
        for allowed, policy in ((frozenset({'interactive'}), None),
                                (None, SourcePolicy(policy_path)),
                                (frozenset({'interactive', PROFILE}), SourcePolicy(policy_path))):
            self.ollama.models = []
            self.broker.wol.wake.reset_mock()
            job = self.broker.submit('interactive', 'generate', {'prompt': 'test'})
            with patch.object(self.executor, 'available') as available:
                self.assertTrue(self.broker.dispatch_once(allowed_sources=allowed, policy=policy))
                available.assert_not_called()
            # The selected LLM still wakes its host; disabled ASR adds no wake.
            self.broker.wol.wake.assert_called_once()
            self.assertEqual(self.broker.status(job['id'])['state'], 'completed')

    def test_result_replacement_at_capacity_counts_only_new_size(self):
        payload = self.payload()
        artifact = payload['artifact']
        self.spool.save_result(artifact, {'text': 'old'})
        self.spool.max_bytes = self.spool.accounted_bytes()
        self.spool.save_result(artifact, {'text': 'new'})
        self.assertEqual(self.spool.result(artifact), {'text': 'new'})
        self.assertEqual(self.spool.accounted_bytes(), self.spool.max_bytes)
        with self.assertRaisesRegex(ValueError, 'capacity'):
            self.spool.save_result(artifact, {'text': 'too large'})
        self.assertEqual(self.spool.result(artifact), {'text': 'new'})

    def test_ack_rechecks_terminal_state_after_concurrent_retry(self):
        port = self.start_http()
        job = self.job()
        self.broker.cancel(job['id'])
        path = '/v1/transcriptions/' + job['id']
        original_status = self.broker.status
        read, finish = threading.Event(), threading.Event()
        responses = []
        captured = False
        def stale_status(job_id):
            nonlocal captured
            snapshot = original_status(job_id)
            if not captured:
                captured = True
                read.set(); finish.wait(3)
            return snapshot
        with patch.object(self.broker, 'status', side_effect=stale_status):
            ack = threading.Thread(target=lambda: responses.append(self.request(port, 'POST', path + '/ack')))
            ack.start()
            try:
                self.assertTrue(read.wait(2))
                self.assertEqual(self.request(port, 'POST', path + '/retry')[0], 202)
            finally:
                finish.set(); ack.join(4)
        self.assertEqual(responses[0][0], 400)
        self.assertEqual(self.broker.status(job['id'])['state'], 'queued')
        self.assertTrue(self.spool.audio(job['payload']).is_file())
        self.assertIsNone(self.broker.db.execute('SELECT 1 FROM asr_receipts WHERE job_id=?', (job['id'],)).fetchone())
        self.assertTrue(self.broker.dispatch_once())
        self.assertEqual(self.broker.status(job['id'])['state'], 'completed')

    def test_retry_hash_does_not_hold_broker_lock_and_rechecks_fence(self):
        port = self.start_http()
        job = self.job()
        self.broker.cancel(job['id'])
        entered, finish, responsive = threading.Event(), threading.Event(), threading.Event()
        original_audio = self.spool.audio
        responses = []
        def slow_audio(payload):
            entered.set(); finish.wait(3)
            return original_audio(payload)
        def lookup_and_admit():
            self.broker.status(job['id'])
            self.broker.submit('interactive', 'generate', {'prompt': 'test'})
            self.broker.set_executor_fence('new-owner-during-hash', job['id'])
            responsive.set()
        with patch.object(self.spool, 'audio', side_effect=slow_audio):
            retry = threading.Thread(target=lambda: responses.append(self.request(
                port, 'POST', '/v1/transcriptions/' + job['id'] + '/retry')))
            retry.start()
            lookup = threading.Thread(target=lookup_and_admit)
            try:
                self.assertTrue(entered.wait(2))
                lookup.start()
                self.assertTrue(responsive.wait(.5), 'artifact hashing blocked broker operations')
            finally:
                finish.set(); retry.join(4)
                if lookup.ident: lookup.join(4)
        self.assertEqual(responses[0][0], 409)
        self.assertEqual(self.broker.status(job['id'])['state'], 'cancelled')
        self.assertEqual(self.broker.executor_fence()['request_id'], 'new-owner-during-hash')

if __name__ == "__main__": unittest.main()
