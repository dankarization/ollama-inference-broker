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

@unittest.skipUnless(os.environ.get("STT_SOURCE_ROOT"), "set STT_SOURCE_ROOT for the two-project contract test")
class ContractTest(unittest.TestCase):
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
            api = serve(broker, port=0, asr_api=ASRAPI(broker, executor, token))
            threading.Thread(target=api.serve_forever, daemon=True).start()
            stop = threading.Event()
            def dispatch():
                while not stop.wait(0.02): broker.dispatch_once()
            thread = threading.Thread(target=dispatch); thread.start()
            try:
                client = WhisperBrokerClient({"WHISPER_BROKER_URL": f"http://127.0.0.1:{api.server_port}",
                                              "WHISPER_BROKER_TOKEN": token}, timeout=10)
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
