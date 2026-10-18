"""Optional cross-project contract test against a separately checked-out STT branch."""
import ipaddress
import os
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from broker.asr import ASRAPI, MODEL, Spool, WhisperExecutor, WhisperHTTP
from broker.http import serve
from broker.service import Broker


class WarmEngine:
    def __init__(self):
        self.model_loaded = False
        self.gpu_release_confirmed = True
        self.calls = 0
        self.fail_unload = False
        self.active = self.max_active = 0

    def transcribe(self, *args, **kwargs):
        self.calls += 1
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        time.sleep(0.03)
        self.model_loaded = True
        self.gpu_release_confirmed = False
        self.active -= 1
        return {"model": MODEL, "device": "cuda", "text": "Контрольная русская речь."}

    def unload_model(self):
        if self.fail_unload:
            raise RuntimeError("native unload failed")
        loaded = self.model_loaded
        self.model_loaded = False
        self.gpu_release_confirmed = True
        return loaded

@unittest.skipUnless(os.environ.get("STT_SOURCE_ROOT"), "set STT_SOURCE_ROOT for the two-project contract test")
class ContractTest(unittest.TestCase):
    def test_cold_warm_result_receipt_idle_and_ollama_over_real_http(self):
        self._warm_protocol(fail_unload=False)

    def test_unload_failure_and_retry_preserve_fence_over_real_http(self):
        self._warm_protocol(fail_unload=True)

    def _warm_protocol(self, fail_unload):
        root = Path(os.environ["STT_SOURCE_ROOT"])
        sys.path[:0] = [str(root / "scripts"), str(root / "windows")]
        import gigaam_worker as transport
        from whisper_broker_client import WhisperBrokerClient
        from dataclasses import replace
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            token = "test-only-contract-credential-" * 2
            worker_root = directory / "worker"
            worker_root.mkdir()
            config = transport.Config("127.0.0.1", 0, token,
                (ipaddress.ip_network("127.0.0.1/32"),), directory, worker_root,
                "ffmpeg", 0, 1, 1024*1024, 10, 60, 60)
            engine = WarmEngine()
            worker = transport.WorkerServer(("127.0.0.1", 0), config, engine=engine,
                                             model_id=MODEL, track_requests=True)
            threading.Thread(target=worker.serve_forever, daemon=True).start()
            executor = WhisperExecutor(Spool(directory / "spool"),
                WhisperHTTP(f"http://127.0.0.1:{worker.server_port}", token))
            ollama = Mock(); ollama.ps.return_value = {"models": []}
            ollama.run.return_value = {"done": True}
            broker = Broker(directory / "queue.db", ollama, Mock(), whisper_executor=executor)
            api = serve(broker, port=0, asr_api=ASRAPI(broker, executor, token+'api'))
            threading.Thread(target=api.serve_forever, daemon=True).start()
            monitor = threading.Thread(target=transport.model_idle_monitor, args=(worker,), daemon=True)
            stop = threading.Event()
            errors = []
            def dispatch():
                while not stop.wait(0.01):
                    try: broker.dispatch_once()
                    except Exception as error: errors.append(error); return
            thread = threading.Thread(target=dispatch); thread.start()
            try:
                client = WhisperBrokerClient({"WHISPER_BROKER_URL": f"http://127.0.0.1:{api.server_port}",
                                              "WHISPER_BROKER_TOKEN": token+'api'}, timeout=10)
                audio = directory / "sample.wav"; audio.write_bytes(b"fake audio")
                with patch.object(transport, "start_process_deadline"), patch.object(transport, "stop_process_deadline"):
                    self.assertEqual(client.transcribe(audio)["text"], "Контрольная русская речь.")
                    first = broker.executor_fence()
                    self.assertIsNotNone(first)
                    self.assertTrue(engine.model_loaded)
                    self.assertFalse(worker.last_request_gpu_released)
                    self.assertEqual(list(executor.spool.root.iterdir()), [])
                    self.assertEqual(client.transcribe(audio)["text"], "Контрольная русская речь.")
                    second = broker.executor_fence()
                    self.assertNotEqual(first["request_id"], second["request_id"])
                    self.assertEqual(worker.last_request_id, second["request_id"])
                    self.assertEqual(engine.calls, 2)
                    self.assertEqual(list(executor.spool.root.iterdir()), [])
                    results = []
                    def concurrent_request():
                        try: results.append(client.transcribe(audio))
                        except Exception as error: errors.append(error)
                    clients = [threading.Thread(target=concurrent_request) for _ in range(2)]
                    for client_thread in clients: client_thread.start()
                    for client_thread in clients: client_thread.join(10)
                    self.assertEqual(len(results), 2)
                    self.assertEqual(engine.calls, 4)
                    self.assertEqual(engine.max_active, 1)
                    second = broker.executor_fence()
                queued = broker.submit("interactive", "generate", {"prompt": "test"})
                time.sleep(0.1)
                ollama.run.assert_not_called()
                if fail_unload:
                    engine.fail_unload = True
                    for _ in range(2):
                        with self.assertRaises(RuntimeError): executor.release_idle(broker)
                        self.assertEqual(broker.executor_fence(), second)
                        self.assertFalse(worker.last_request_gpu_released)
                    engine.fail_unload = False
                    self.assertTrue(executor.release_idle(broker))
                else:
                    monitor.start()
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline and broker.status(queued["id"])["state"] in {"queued", "running"}:
                    time.sleep(0.02)
                self.assertTrue(worker.last_request_gpu_released)
                self.assertFalse(engine.model_loaded)
                self.assertIsNone(broker.executor_fence())
                self.assertEqual(broker.status(queued["id"])["state"], "completed")
                ollama.run.assert_called_once()
                self.assertFalse(errors)
            finally:
                stop.set(); thread.join(3)
                api.shutdown(); api.server_close()
                worker.shutdown(); worker.server_close()
                if monitor.is_alive(): monitor.join(2)
                broker.db.close()

    def test_real_http_client_broker_worker_and_raw_result(self):
        root = Path(os.environ["STT_SOURCE_ROOT"])
        sys.path[:0] = [str(root / "scripts"), str(root / "windows")]
        import gigaam_worker as transport
        from whisper_broker_client import WhisperBrokerClient
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            token = "test-only-contract-credential-" * 2
            config = transport.Config("127.0.0.1", 0, token,
                (ipaddress.ip_network("127.0.0.1/32"),), directory, directory,
                "ffmpeg", 0, 0, 1024*1024, 10, 60, 60)
            engine = Mock(model_loaded=False, gpu_release_confirmed=True)
            engine.transcribe.return_value = {"model": MODEL, "device": "cuda", "text": "Ручей рядом с тропой."}
            worker = transport.WorkerServer(("127.0.0.1", 0), config, engine=engine,
                                             model_id=MODEL, track_requests=True)
            threading.Thread(target=worker.serve_forever, daemon=True).start()
            executor = WhisperExecutor(Spool(directory / "spool"),
                WhisperHTTP(f"http://127.0.0.1:{worker.server_port}", token))
            ollama = Mock(); ollama.ps.return_value = {"models": []}
            broker = Broker(directory / "queue.db", ollama, Mock(), whisper_executor=executor)
            api = serve(broker, port=0, asr_api=ASRAPI(broker, executor, token+'api'))
            threading.Thread(target=api.serve_forever, daemon=True).start()
            stop = threading.Event()
            def dispatch():
                while not stop.wait(0.02): broker.dispatch_once()
            thread = threading.Thread(target=dispatch); thread.start()
            try:
                client = WhisperBrokerClient({"WHISPER_BROKER_URL": f"http://127.0.0.1:{api.server_port}",
                                              "WHISPER_BROKER_TOKEN": token+'api'}, timeout=10)
                audio = directory / "sample.wav"; audio.write_bytes(b"fake audio")
                with patch.object(transport, "start_process_deadline"), patch.object(transport, "stop_process_deadline"):
                    result = client.transcribe(audio)
                self.assertEqual(result["text"], "Ручей рядом с тропой.")
                self.assertIsNone(broker.executor_fence())
                self.assertEqual(list(executor.spool.root.iterdir()), [])
                ollama.unload.assert_not_called()
                ollama.run.assert_not_called()
                self.assertTrue(worker.last_request_gpu_released)
                self.assertIsNotNone(worker.last_request_id)
            finally:
                stop.set(); thread.join(3)
                api.shutdown(); api.server_close()
                worker.shutdown(); worker.server_close()
                broker.db.close()

if __name__ == "__main__": unittest.main()
