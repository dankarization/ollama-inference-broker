from __future__ import annotations
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlsplit

from .compat import (CompatibilityError, stream_frames, submit as submit_compatibility,
                     submit_olya_decision, submit_olya_vision, submit_shutterstock_canary,
                     submit_shutterstock_video, submit_syncopia_memory)
from .profiles import PROFILES
from .dashboard import render as render_dashboard
from .policy import SourcePolicyError

def serve(broker, host="127.0.0.1", port=8088, policy=None):
    class Handler(BaseHTTPRequestHandler):
        def _json(self, status, value):
            encoded=json.dumps(value).encode(); self.send_response(status); self.send_header("Content-Type","application/json"); self.send_header("Content-Length",str(len(encoded))); self.end_headers(); self.wfile.write(encoded)
        def _stream(self, status, frames):
            encoded=b"".join(frames); self.send_response(status); self.send_header("Content-Type","application/x-ndjson"); self.send_header("Content-Length",str(len(encoded))); self.end_headers(); self.wfile.write(encoded)
        def do_POST(self):
            size=int(self.headers.get("Content-Length", 0))
            try:
                body=json.loads(self.rfile.read(size) or b"{}")
            except (TypeError, ValueError):
                self._json(400, {"error": "request body must be JSON"})
                return
            path = urlsplit(self.path).path
            if path == "/v1/jobs":
                try:
                    self._json(202, broker.submit(body["profile"], body["kind"], body.get("payload", {}),
                                                  body.get("source"), body.get("source_item_id"),
                                                  body.get("external_id")))
                except (KeyError, ValueError) as e: self._json(400, {"error": str(e)})
            elif path.startswith("/v1/jobs/") and path.endswith("/cancel"):
                result=broker.cancel(path.split("/")[3]); self._json(200 if result else 404, result or {"error":"not found"})
            elif path.startswith("/v1/jobs/") and path.endswith("/retry"):
                try:
                    result=broker.retry(path.split("/")[3]); self._json(200 if result else 404, result or {"error":"not found"})
                except ValueError as e: self._json(409, {"error":str(e)})
            elif path.startswith("/v1/sources/") and path.endswith("/weight"):
                source = unquote(path[len("/v1/sources/"):-len("/weight")]).strip("/")
                try:
                    if not isinstance(body, dict) or set(body) != {"weight"}:
                        raise SourcePolicyError("request body must contain only weight")
                    if policy is None:
                        raise SourcePolicyError("source policy is not configured")
                    weight = policy.set_weight(source, body["weight"])
                    self._json(200, {"source": source, "weight": weight})
                except SourcePolicyError as error:
                    self._json(400, {"error": str(error)})
            elif path.startswith("/v1/sources/") and path.endswith("/enabled"):
                source = unquote(path[len("/v1/sources/"):-len("/enabled")]).strip("/")
                try:
                    if not isinstance(body, dict) or set(body) != {"enabled"}:
                        raise SourcePolicyError("request body must contain only enabled")
                    if policy is None: raise SourcePolicyError("source policy is not configured")
                    self._json(200, {"source": source, "enabled": policy.set_enabled(source, body["enabled"])})
                except SourcePolicyError as error:
                    self._json(400, {"error": str(error)})
            elif path in {"/api/chat", "/api/generate"}:
                try:
                    job=submit_compatibility(broker, path.rsplit("/", 1)[-1], body)
                    if body.get("stream", True): self._stream(202, stream_frames(job))
                    else: self._json(202, job)
                except CompatibilityError as e: self._json(400, {"error":str(e)})
            elif path == "/v1/shutterstock-canary/generate":
                try:
                    job = submit_shutterstock_canary(broker, body)
                    result = broker.wait_for_terminal(
                        job["id"], PROFILES["shutterstock-canary"].request_timeout_seconds + 5,
                    )
                    if result is None:
                        self._json(500, {"error": "submitted job disappeared"})
                    elif result["state"] == "completed":
                        response = dict(result["result"])
                        response["broker"] = {"job_id": job["id"], "state": result["state"]}
                        self._json(200, response)
                    elif result["state"] == "failed":
                        self._json(502, {"error": "broker job failed", "job_id": job["id"]})
                    else:
                        self._json(504, {"error": "broker job timed out", "job_id": job["id"]})
                except CompatibilityError as e: self._json(400, {"error":str(e)})
            elif path == "/v1/shutterstock-video/generate":
                try:
                    job = submit_shutterstock_video(broker, body)
                    result = broker.wait_for_terminal(
                        job["id"], PROFILES["shutterstock-video"].request_timeout_seconds + 5,
                    )
                    if result is None:
                        self._json(500, {"error": "submitted job disappeared"})
                    elif result["state"] == "completed":
                        response = dict(result["result"])
                        response["broker"] = {"job_id": job["id"], "state": result["state"]}
                        self._json(200, response)
                    elif result["state"] == "failed":
                        self._json(502, {"error": "broker job failed", "job_id": job["id"]})
                    else:
                        self._json(504, {"error": "broker job timed out", "job_id": job["id"]})
                except CompatibilityError as e: self._json(400, {"error":str(e)})
            elif path == "/v1/olya-vision/generate":
                try:
                    job = submit_olya_vision(broker, body)
                    result = broker.wait_for_terminal(
                        job["id"], PROFILES[job["profile"]].request_timeout_seconds + 5,
                    )
                    if result is None:
                        self._json(500, {"error": "submitted job disappeared"})
                    elif result["state"] == "completed":
                        response = dict(result["result"])
                        response["broker"] = {
                            "job_id": job["id"],
                            "state": result["state"],
                            "source_item_id": result.get("source_item_id"),
                            "external_id": result.get("external_id"),
                            "created": result["created"],
                            "started": result.get("started"),
                            "finished": result.get("finished"),
                        }
                        self._json(200, response)
                    elif result["state"] == "failed":
                        self._json(502, {"error": "broker job failed", "job_id": job["id"]})
                    else:
                        self._json(504, {"error": "broker job timed out", "job_id": job["id"]})
                except CompatibilityError as e: self._json(400, {"error":str(e)})
            elif path == "/v1/olya-decision/generate":
                try:
                    job = submit_olya_decision(broker, body)
                    result = broker.wait_for_terminal(
                        job["id"], PROFILES["olya-decision-qwen38"].request_timeout_seconds + 5,
                    )
                    if result is None:
                        self._json(500, {"error": "submitted job disappeared"})
                    elif result["state"] == "completed":
                        response = dict(result["result"])
                        response["broker"] = {
                            "job_id": job["id"], "state": result["state"],
                            "created": result["created"], "started": result["started"],
                            "finished": result["finished"],
                        }
                        self._json(200, response)
                    elif result["state"] == "failed":
                        self._json(502, {"error": "broker job failed", "job_id": job["id"]})
                    else:
                        self._json(504, {"error": "broker job timed out", "job_id": job["id"]})
                except CompatibilityError as e: self._json(400, {"error":str(e)})
            elif path == "/v1/syncopia-memory/extract":
                try:
                    job = submit_syncopia_memory(broker, body)
                    result = broker.wait_for_terminal(
                        job["id"], PROFILES["syncopia-memory-qwen38"].request_timeout_seconds + 5,
                    )
                    if result is None:
                        self._json(500, {"error": "submitted job disappeared"})
                    elif result["state"] == "completed":
                        response = dict(result["result"])
                        response["broker"] = {
                            "job_id": job["id"], "state": result["state"],
                            "source": result["source"], "profile": result["profile"],
                            "created": result["created"], "started": result["started"],
                            "finished": result["finished"],
                        }
                        self._json(200, response)
                    elif result["state"] == "failed":
                        self._json(502, {"error": "broker job failed", "job_id": job["id"]})
                    else:
                        self._json(504, {"error": "broker job timed out", "job_id": job["id"]})
                except CompatibilityError as e: self._json(400, {"error":str(e)})
            else: self._json(404, {"error":"not found"})
        def do_GET(self):
            parsed = urlsplit(self.path)
            path = parsed.path
            query = parse_qs(parsed.query)
            if path == "/dashboard":
                dashboard = broker.dashboard(policy)
                encoded = render_dashboard(dashboard)
                status = 503 if dashboard["observation"]["state"] == "unavailable" else 200
                self.send_response(status); self.send_header("Content-Type", "text/html; charset=utf-8"); self.send_header("Content-Length", str(len(encoded))); self.end_headers(); self.wfile.write(encoded)
            elif path == "/v1/dashboard":
                dashboard = broker.dashboard(policy)
                self._json(503 if dashboard["observation"]["state"] == "unavailable" else 200, dashboard)
            elif path == "/healthz": self._json(200, broker.health())
            elif path == "/v1/metrics": self._json(200, broker.metrics())
            elif path == "/v1/analytics":
                try:
                    windows = tuple(
                        int(value) for item in query.get("window", [])
                        for value in item.split(",") if value
                    ) or (300, 1_800, 10_800, 86_400)
                    if any(value <= 0 or value > 31_536_000 for value in windows):
                        raise ValueError
                    self._json(200, broker.analytics(policy, windows))
                except ValueError:
                    self._json(400, {"error":"window must be positive seconds up to one year"})
            elif path == "/v1/forecast":
                try:
                    limit = int(query.get("limit", ["5"])[0])
                    self._json(200, broker.forecast(policy, limit=limit))
                except ValueError:
                    self._json(400, {"error":"limit must be an integer"})
            elif path == "/v1/history":
                try:
                    limit = int(query.get("limit", ["30"])[0])
                    raw = query.get("cursor", [None])[0]
                    cursor = None if raw is None else (float(raw.rsplit(":", 1)[0]), raw.rsplit(":", 1)[1])
                    body = broker.terminal_history(limit=limit, cursor=cursor)
                    if body.get("next_cursor"):
                        body["next_cursor"] = f"{body['next_cursor'][0]}:{body['next_cursor'][1]}"
                    self._json(200, body)
                except ValueError:
                    self._json(400, {"error":"invalid history cursor"})
            elif path == "/v1/audit-events":
                try:
                    limit = int(query.get("limit", ["100"])[0])
                    since = query.get("since", [None])[0]
                    self._json(200, {"events": broker.audit_events(
                        limit=limit,
                        job_id=query.get("job_id", [None])[0],
                        source=query.get("source", [None])[0],
                        since=float(since) if since is not None else None,
                    )})
                except ValueError:
                    self._json(400, {"error":"limit and since must be numeric"})
            elif path == "/v1/correlations":
                try:
                    self._json(200, {"jobs": broker.correlations(
                        source=query.get("source", [None])[0],
                        source_item_id=query.get("source_item_id", [None])[0],
                        external_id=query.get("external_id", [None])[0],
                        limit=int(query.get("limit", ["100"])[0]),
                    )})
                except ValueError as error:
                    self._json(400, {"error":str(error)})
            elif path == "/v1/sources":
                if policy is None:
                    self._json(200, {"policy": None})
                else:
                    self._json(200, {"policy": policy.snapshot()})
            elif path.startswith("/v1/jobs/") and path.endswith("/attempts"):
                result=broker.attempts(path.split("/")[3]); self._json(200 if result is not None else 404, {"attempts":result} if result is not None else {"error":"not found"})
            elif path.startswith("/v1/jobs/"):
                result=broker.status(path.split("/")[3]); self._json(200 if result else 404, result or {"error":"not found"})
            else: self._json(404, {"error":"not found"})
        def log_message(self, *_): pass
    return ThreadingHTTPServer((host, port), Handler)
