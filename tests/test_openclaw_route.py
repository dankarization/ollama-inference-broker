import json
import socket
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, ProxyHandler, build_opener

from broker.http import serve
from broker.openclaw import MODEL, normalize_request
from broker.profiles import PROFILES
from broker.service import Broker


class FakeWol:
    def wake(self):
        pass


class FakeOllama:
    def __init__(self, result=None):
        self.requests = []
        self.result = result or {
            "model": MODEL, "message": {"role": "assistant", "content": "answer",
                "tool_calls": [{"function": {"name": "read", "arguments": {"path": "a"}}}]},
            "done": True, "done_reason": "stop", "prompt_eval_count": 19, "eval_count": 7,
        }

    def ps(self):
        return {"models": [{"name": MODEL}]}

    def is_ready(self, model):
        return model == MODEL

    def run(self, kind, request):
        self.requests.append((kind, request))
        return self.result


class OpenClawRouteTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.ollama = FakeOllama()
        self.broker = Broker(Path(self.tmp.name) / "jobs.sqlite3", self.ollama, FakeWol())
        self.server = serve(self.broker, port=0)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}/openclaw/api/chat"
        self.opener = build_opener(ProxyHandler({}))

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.broker.db.close()
        self.tmp.cleanup()

    def request(self, body, headers=None):
        return Request(self.url, data=json.dumps(body).encode(),
                       headers={"Content-Type": "application/json", **(headers or {})}, method="POST")

    def sample(self, stream=False):
        return {"model": MODEL, "stream": stream, "messages": [
            {"role": "system", "content": "Use tools"},
            {"role": "user", "content": "First"},
            {"role": "assistant", "content": "", "tool_calls": [
                {"function": {"name": "read", "arguments": {"path": "a"}}}]},
            {"role": "tool", "content": "file content", "tool_call_id": "call-1", "tool_name": "read"},
            {"role": "user", "content": "Continue"},
        ], "tools": [{"type": "function", "function": {
            "name": "read", "description": "Read file", "parameters": {"type": "object"}}}],
            "options": {"num_ctx": 131072, "num_predict": 16384, "temperature": 0.2},
            "think": "low", "truncate": False, "shift": False}

    def wait_queued(self):
        deadline = time.monotonic() + 2
        while self.broker.health()["queue_depth"] == 0 and time.monotonic() < deadline:
            time.sleep(.005)
        self.assertEqual(self.broker.health()["queue_depth"], 1)

    def test_nonstream_full_history_tools_and_real_response(self):
        output = {}
        def client():
            with self.opener.open(self.request(self.sample()), timeout=5) as response:
                output["status"] = response.status
                output["body"] = json.loads(response.read())
                output["job"] = response.headers["X-Broker-Job-Id"]
        caller = threading.Thread(target=client)
        caller.start(); self.wait_queued()
        self.assertTrue(self.broker.dispatch_once(frozenset({"openclaw"})))
        caller.join(timeout=2)
        self.assertFalse(caller.is_alive())
        self.assertEqual(output["status"], 200)
        self.assertEqual(output["body"], self.ollama.result)
        self.assertEqual(self.broker.status(output["job"])["source"], "openclaw")
        kind, request = self.ollama.requests[0]
        self.assertEqual(kind, "chat")
        self.assertEqual(request["messages"], self.sample()["messages"])
        self.assertEqual(request["tools"], self.sample()["tools"])
        self.assertEqual(request["options"]["num_ctx"], 131072)
        self.assertEqual(request["options"]["num_predict"], 16384)
        self.assertEqual(request["think"], "low")
        self.assertFalse(request["stream"])

    def test_stream_heartbeats_then_native_terminal_tool_frame(self):
        output = {}
        def client():
            with self.opener.open(self.request(self.sample(stream=True)), timeout=5) as response:
                output["status"] = response.status
                output["frames"] = [json.loads(line) for line in response.read().splitlines()]
        with patch("broker.http.HEARTBEAT_SECONDS", .02):
            caller = threading.Thread(target=client)
            caller.start(); self.wait_queued(); time.sleep(.07)
            self.broker.dispatch_once(frozenset({"openclaw"}))
            caller.join(timeout=2)
        self.assertFalse(caller.is_alive())
        self.assertEqual(output["status"], 200)
        self.assertTrue(any(frame.get("done") is False for frame in output["frames"]))
        self.assertEqual(output["frames"][-1], self.ollama.result)

    def test_model_and_payload_bounds_and_no_legacy_cron_cap(self):
        self.assertEqual((PROFILES["openclaw"].max_context, PROFILES["openclaw"].max_output),
                         (131072, 16384))
        for change in ({"model": "evil"}, {"messages": []},
                       {"tools": [{"type": "function", "function": {"name": "x"}}]},
                       {"truncate": True}, {"options": {"num_ctx": True}},
                       {"options": {"num_gpu": 99}},
                       {"options": {"temperature": float("nan")}}):
            body = {**self.sample(), **change}
            with self.assertRaises(HTTPError) as caught:
                self.opener.open(self.request(body))
            self.assertEqual(caught.exception.code, 400)
        capped = self.sample()
        capped["options"] = {"num_ctx": 999999, "num_predict": 999999}
        job = self.broker.submit("openclaw", "chat", normalize_request(capped))
        self.broker.dispatch_once(frozenset({"openclaw"}))
        self.assertEqual(self.ollama.requests[-1][1]["options"],
                         {"num_ctx": 131072, "num_predict": 16384})
        self.assertEqual(self.broker.status(job["id"])["state"], "completed")
        with self.assertRaisesRegex(ValueError, "requires the OpenClaw profile"):
            self.broker.submit("cron", "chat", {"messages": [{"role": "user", "content": "x"}]},
                               source="openclaw")

    def test_idempotency_queue_bound_and_failed_executor(self):
        payload = normalize_request(self.sample())
        first = self.broker.submit("openclaw", "chat", payload, external_id="turn-1")
        self.assertEqual(self.broker.submit("openclaw", "chat", payload,
                                            external_id="turn-1")["id"], first["id"])
        self.broker.dispatch_once(frozenset({"openclaw"}))
        self.assertEqual(self.broker.submit("openclaw", "chat", payload,
                                            external_id="turn-1")["id"], first["id"])
        different = normalize_request({**self.sample(), "messages": [{"role": "user", "content": "other"}]})
        with self.assertRaisesRegex(ValueError, "idempotency key"):
            self.broker.submit("openclaw", "chat", different, external_id="turn-1")
        for n in range(8):
            self.broker.submit("openclaw", "chat", payload, external_id=f"turn-{n+2}")
        with self.assertRaises(HTTPError) as caught:
            self.opener.open(self.request(self.sample()))
        self.assertEqual(caught.exception.code, 429)
        generic = Request(
            f"http://127.0.0.1:{self.server.server_port}/v1/jobs",
            data=json.dumps({"profile": "openclaw", "kind": "chat", "source": "openclaw",
                             "payload": payload}).encode(),
            headers={"Content-Type": "application/json"}, method="POST",
        )
        with self.assertRaises(HTTPError) as generic_error:
            self.opener.open(generic)
        self.assertEqual(generic_error.exception.code, 429)
        self.ollama.result = {"done": False, "message": {"role": "assistant", "content": "partial"}}
        self.broker.dispatch_once(frozenset({"openclaw"}))
        self.assertEqual(self.broker.status(self.broker.db.execute(
            "SELECT id FROM jobs WHERE external_id='turn-2'").fetchone()[0])["state"], "failed")

    def test_streaming_failure_has_native_error_and_keyed_timeout_can_reattach(self):
        output = {}
        def client():
            with self.opener.open(self.request(self.sample(stream=True)), timeout=5) as response:
                output["frames"] = [json.loads(line) for line in response.read().splitlines()]
        caller = threading.Thread(target=client)
        caller.start(); self.wait_queued()
        self.ollama.result = {"message": {"role": "assistant", "content": "partial"}, "done": False}
        self.broker.dispatch_once(frozenset({"openclaw"}))
        caller.join(timeout=2)
        self.assertFalse(caller.is_alive())
        self.assertEqual(output["frames"][-1]["status"], 502)
        self.assertEqual(output["frames"][-1]["error"], "broker job failed")

        with patch("broker.http.MAX_WAIT_SECONDS", .05), patch("broker.http.HEARTBEAT_SECONDS", .02):
            with self.assertRaises(HTTPError) as caught:
                self.opener.open(self.request(self.sample(), {"Idempotency-Key": "retry-turn"}),
                                 timeout=2)
            self.assertEqual(caught.exception.code, 504)
        job = self.broker.db.execute("SELECT id,state FROM jobs WHERE external_id='retry-turn'").fetchone()
        self.assertEqual(job["state"], "queued")
        self.assertEqual(self.broker.submit("openclaw", "chat", normalize_request(self.sample()),
                                            external_id="retry-turn")["id"], job["id"])

    def test_disconnect_cancels_unkeyed_and_timeout_returns_error(self):
        with patch("broker.http.HEARTBEAT_SECONDS", .02):
            sock = socket.create_connection(("127.0.0.1", self.server.server_port))
            body = json.dumps(self.sample()).encode()
            sock.sendall(b"POST /openclaw/api/chat HTTP/1.1\r\nHost: localhost\r\n"
                         + f"Content-Length: {len(body)}\r\nContent-Type: application/json\r\n\r\n".encode()
                         + body)
            self.wait_queued()
            sock.close()
            deadline = time.monotonic() + 2
            while self.broker.db.execute("SELECT state FROM jobs WHERE source='openclaw' ORDER BY created DESC LIMIT 1").fetchone()[0] != "cancelled" and time.monotonic() < deadline:
                time.sleep(.02)
            self.assertEqual(self.broker.db.execute(
                "SELECT state FROM jobs WHERE source='openclaw' ORDER BY created DESC LIMIT 1"
            ).fetchone()[0], "cancelled")
        with patch("broker.http.MAX_WAIT_SECONDS", .05), patch("broker.http.HEARTBEAT_SECONDS", .02):
            with self.assertRaises(HTTPError) as caught:
                self.opener.open(self.request(self.sample()), timeout=2)
            self.assertEqual(caught.exception.code, 504)


if __name__ == "__main__":
    unittest.main()
