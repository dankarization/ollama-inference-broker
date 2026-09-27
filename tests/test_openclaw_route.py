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
from broker.profiles import OPENCLAW_PROFILES_BY_MODEL, PROFILES
from broker.service import Broker, SourceQueueFull


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
        return model in OPENCLAW_PROFILES_BY_MODEL

    def unload(self, model):
        pass

    def run(self, kind, request):
        self.requests.append((kind, request))
        return {**self.result, "model": request["model"]}


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

    def test_all_configured_models_route_to_server_owned_profiles(self):
        cases = {
            "gemma4:12b": (262_144, 16_384, True),
            "qwen3-vl:30b": (212_992, 16_384, True),
            "nemotron3:33b": (131_072, 8_192, True),
            "qwen3.8:ad-iq2-xs": (131_072, 16_384, False),
            "qwen3.8:unc-rvn-iq2xxs": (131_072, 16_384, False),
            "frob/ministral-3:14b-thinking-q4_K_M": (262_144, 16_384, True),
            "nemotron-3-nano:30b-a3b-q4_K_M": (262_144, 16_384, False),
            "gpt-oss:20b": (131_072, 16_384, False),
        }
        self.assertEqual(set(OPENCLAW_PROFILES_BY_MODEL), set(cases))
        for model, (context, output, vision) in cases.items():
            with self.subTest(model=model):
                profile = OPENCLAW_PROFILES_BY_MODEL[model]
                self.assertEqual((PROFILES[profile].max_context, PROFILES[profile].max_output),
                                 (context, output))
                body = {**self.sample(), "model": model,
                        "options": {"num_ctx": context, "num_predict": output}}
                if vision:
                    body["messages"] = [{"role": "user", "content": "describe",
                                         "images": ["aGVsbG8="]}]
                result = {}
                def client():
                    with self.opener.open(self.request(body), timeout=5) as response:
                        result["body"] = json.loads(response.read())
                        result["id"] = response.headers["X-Broker-Job-Id"]
                caller = threading.Thread(target=client)
                caller.start(); self.wait_queued()
                self.assertTrue(self.broker.dispatch_once(frozenset({"openclaw"})))
                caller.join(timeout=2)
                self.assertFalse(caller.is_alive())
                self.assertEqual(result["body"]["model"], model)
                self.assertEqual(result["body"]["message"]["tool_calls"],
                                 self.ollama.result["message"]["tool_calls"])
                self.assertEqual(self.broker.status(result["id"])["profile"], profile)
                request = self.ollama.requests[-1][1]
                self.assertEqual(request["model"], model)
                self.assertEqual(request["options"]["num_ctx"], context)
                self.assertEqual(request["options"]["num_predict"], output)
                if vision:
                    self.assertEqual(request["messages"][0]["images"], ["aGVsbG8="])
                with self.assertRaisesRegex(ValueError, "requires source openclaw"):
                    self.broker.submit(profile, "chat", normalize_request(body), source="other")
                with self.assertRaisesRegex(ValueError, "must match its profile"):
                    self.broker.submit(profile, "chat", {**normalize_request(body),
                                                          "model": "caller-chosen:bad"},
                                       source="openclaw")
                if not vision:
                    with self.assertRaises(HTTPError) as caught:
                        self.opener.open(self.request({**body, "messages": [
                            {"role": "user", "content": "x", "images": ["aGVsbG8="]}]}))
                    self.assertEqual(caught.exception.code, 400)
        with self.assertRaises(HTTPError) as caught:
            self.opener.open(self.request({**self.sample(), "model": "caller-chosen:bad"}))
        self.assertEqual(caught.exception.code, 400)

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

    def test_rejects_numeric_thinking_and_fractional_integer_options(self):
        for change in (
            *({"think": value} for value in (0, 1, 0.0, 1.0)),
            *({"options": {field: 1.5}} for field in ("top_k", "repeat_last_n", "seed")),
        ):
            with self.subTest(change=change), self.assertRaises(HTTPError) as caught:
                self.opener.open(self.request({**self.sample(), **change}))
            self.assertEqual(caught.exception.code, 400)
        self.assertEqual(self.broker.health()["queue_depth"], 0)

    def test_expired_reattached_result_returns_410_in_both_modes(self):
        body = self.sample()
        payload = normalize_request(body)
        job = self.broker.submit("openclaw", "chat", payload, external_id="expired-turn")
        self.broker.dispatch_once(frozenset({"openclaw"}))
        with self.broker.db:
            self.broker.db.execute(
                "UPDATE jobs SET result_json=NULL,compaction_state='metadata_only' WHERE id=?",
                (job["id"],),
            )
        for streaming in (False, True):
            with self.subTest(streaming=streaming), self.assertRaises(HTTPError) as caught:
                self.opener.open(self.request(
                    {**body, "stream": streaming}, {"Idempotency-Key": "expired-turn"},
                ))
            self.assertEqual(caught.exception.code, 410)
            self.assertIn("expired", caught.exception.read().decode())

    def test_openclaw_retries_respect_outstanding_limit_atomically(self):
        payload = normalize_request(self.sample())
        cancelled = self.broker.submit("openclaw", "chat", payload)
        self.broker.cancel(cancelled["id"])
        failed = [self.broker.submit("openclaw", "chat", payload) for _ in range(2)]
        with self.broker.db:
            for job in failed:
                self.broker.db.execute(
                    "UPDATE jobs SET state='failed' WHERE id=?", (job["id"],),
                )
        active = [self.broker.submit("openclaw", "chat", payload) for _ in range(8)]
        with self.assertRaises(SourceQueueFull):
            self.broker.retry(cancelled["id"])
        retry_request = Request(
            f"http://127.0.0.1:{self.server.server_port}/v1/jobs/{cancelled['id']}/retry",
            data=b"{}", headers={"Content-Type": "application/json"}, method="POST",
        )
        with self.assertRaises(HTTPError) as caught:
            self.opener.open(retry_request)
        self.assertEqual(caught.exception.code, 429)
        with self.assertRaises(SourceQueueFull):
            self.broker.bulk_retry_failed("openclaw")
        self.assertEqual(
            [self.broker.status(job["id"])["state"] for job in failed],
            ["failed", "failed"],
        )
        self.broker.cancel(active[-1]["id"])
        with self.assertRaises(SourceQueueFull):
            self.broker.bulk_retry_failed("openclaw")
        self.assertEqual(
            [self.broker.status(job["id"])["state"] for job in failed],
            ["failed", "failed"],
        )

    def test_poll_pending_openclaw_job_does_not_hydrate_payload(self):
        job = self.broker.submit("openclaw", "chat", normalize_request(self.sample()))
        with patch.object(self.broker, "status", side_effect=AssertionError("hydrated")):
            pending = self.broker.wait_for_terminal(
                job["id"], 0, hydrate_pending=False,
            )
        self.assertEqual(pending, {"id": job["id"], "state": "queued"})
        self.broker.dispatch_once(frozenset({"openclaw"}))
        terminal = self.broker.wait_for_terminal(job["id"], 0, hydrate_pending=False)
        self.assertEqual(terminal["result"]["done"], True)

    def test_legacy_profiles_and_duplicate_openclaw_keys_survive_restart(self):
        legacy = self.broker.submit(
            "uncensored-eval-rvn-iq2m", "chat",
            {"messages": [{"role": "user", "content": "legacy"}]},
            source="uncensored-eval",
        )
        candidates = self.broker._candidates(frozenset({"uncensored-eval"}))
        self.assertEqual([row["id"] for row in candidates], [legacy["id"]])
        payload = normalize_request(self.sample())
        old = [self.broker.submit("openclaw", "chat", payload) for _ in range(2)]
        with self.broker.db:
            for job in old:
                self.broker.db.execute(
                    "UPDATE jobs SET external_id='legacy-duplicate' WHERE id=?",
                    (job["id"],),
                )
        executor = FakeOllama()
        reopened = Broker(self.broker.database, executor, FakeWol())
        try:
            self.assertEqual(reopened.db.execute(
                "SELECT count(*) FROM jobs WHERE source='openclaw' "
                "AND external_id='legacy-duplicate'"
            ).fetchone()[0], 2)
            self.assertEqual(
                len(reopened._candidates(frozenset({"uncensored-eval"}))), 1,
            )
            with patch.object(executor, "is_ready", return_value=True):
                self.assertTrue(reopened.dispatch_once(frozenset({"uncensored-eval"})))
            self.assertEqual(executor.requests[-1][1]["options"]["num_ctx"], 65_536)
        finally:
            reopened.db.close()

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
