from __future__ import annotations
import hmac
import json
import select
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlsplit

from .compat import (CompatibilityError, stream_frames, submit as submit_compatibility,
                     submit_olya_decision, submit_olya_vision, submit_shutterstock_canary,
                     submit_shutterstock_video, submit_syncopia_memory)
from .profiles import OPENCLAW_PROFILES_BY_MODEL, PROFILES
from .dashboard import render as render_dashboard
from .policy import SourcePolicyError
from .service import SourceAdmissionBlocked, SourceQueueFull, StorageAuthorizationRequired
from .storage import ReceiptConflict, StorageContractError
from .openclaw import (HEARTBEAT_SECONDS, MAX_OUTSTANDING, MAX_REQUEST_BYTES, MAX_WAIT_SECONDS,
                       OpenClawRequestError, error_frame, heartbeat, normalize_request, terminal_frame)


def serve(broker, host="127.0.0.1", port=8088, policy=None, storage_token=None):
    broker.use_source_policy(policy)
    openclaw_slots = threading.BoundedSemaphore(MAX_OUTSTANDING)
    class Handler(BaseHTTPRequestHandler):
        def _json(self, status, value, headers=None):
            encoded=json.dumps(value).encode(); self.send_response(status); self.send_header("Content-Type","application/json"); self.send_header("Content-Length",str(len(encoded)))
            for name, header_value in (headers or {}).items(): self.send_header(name, header_value)
            self.end_headers(); self.wfile.write(encoded)
        def _stream(self, status, frames):
            encoded=b"".join(frames); self.send_response(status); self.send_header("Content-Type","application/x-ndjson"); self.send_header("Content-Length",str(len(encoded))); self.end_headers(); self.wfile.write(encoded)
        def _admission_blocked(self, error):
            self._json(403, {
                "error": {"code": error.code, "message": str(error)},
                "source": error.source,
                "admission_allowed": False,
            })
        def _configured_source(self, source):
            if policy is None:
                raise SourcePolicyError("source policy is not configured")
            if source not in policy.snapshot()["sources"]:
                raise SourcePolicyError(f"source {source!r} is not configured")
        def _storage_authenticated(self):
            if storage_token is None:
                return False
            prefix = "Bearer "
            authorization = self.headers.get("Authorization", "")
            return authorization.startswith(prefix) and hmac.compare_digest(
                authorization[len(prefix):], storage_token,
            )
        def _storage_authorized(self):
            if storage_token is None:
                self._json(503, {"error": {"code": "storage_api_unavailable"}})
                return False
            if not self._storage_authenticated():
                self._json(401, {"error": {"code": "storage_auth_required"}})
                return False
            return True
        def _dashboard_validator(self, include_producer_storage):
            etag = broker.dashboard_etag(
                policy, include_producer_storage=include_producer_storage,
            )
            headers = {"ETag": etag, "Cache-Control": "private, no-cache"}
            if self.headers.get("If-None-Match") == etag:
                self.send_response(304)
                for name, value in headers.items(): self.send_header(name, value)
                self.end_headers()
                return None, headers
            return etag, headers
        def _peer_closed(self):
            readable, _, _ = select.select([self.connection], [], [], 0)
            if not readable:
                return False
            try:
                return self.connection.recv(1, socket.MSG_PEEK | socket.MSG_DONTWAIT) == b""
            except (BlockingIOError, InterruptedError):
                return False
            except OSError:
                return True

        def _openclaw_chat(self):
            if not openclaw_slots.acquire(blocking=False):
                self._json(429, {"error": "openclaw HTTP waiters are full"})
                return
            try:
                try:
                    size = int(self.headers.get("Content-Length", ""))
                except ValueError:
                    self._json(411, {"error": "Content-Length is required"})
                    return
                if size <= 0 or size > MAX_REQUEST_BYTES:
                    self.close_connection = True
                    self._json(413, {"error": "request exceeds 2 MiB or is empty"})
                    return
                try:
                    self.connection.settimeout(10)
                    try:
                        request = json.loads(self.rfile.read(size))
                    finally:
                        self.connection.settimeout(None)
                    payload = normalize_request(request)
                    key = self.headers.get("Idempotency-Key")
                    job = broker.submit(OPENCLAW_PROFILES_BY_MODEL[request["model"]], "chat", payload, source="openclaw",
                                        external_id=key)
                except socket.timeout:
                    self.close_connection = True
                    self._json(408, {"error": "request body read timed out"})
                    return
                except (OpenClawRequestError, ValueError, TypeError, RecursionError) as error:
                    self._json(400, {"error": str(error)})
                    return
                except SourceAdmissionBlocked as error:
                    self._admission_blocked(error)
                    return
                except SourceQueueFull as error:
                    self._json(429, {"error": str(error)})
                    return
                streaming = request.get("stream", True)
                deadline = time.monotonic() + MAX_WAIT_SECONDS
                try:
                    if streaming:
                        self.send_response(200)
                        self.send_header("Content-Type", "application/x-ndjson")
                        self.send_header("Cache-Control", "no-store")
                        self.send_header("X-Broker-Job-Id", job["id"])
                        self.end_headers()
                        self.close_connection = True
                    while job["state"] not in {"completed", "failed", "cancelled"}:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            if key is None:
                                broker.cancel(job["id"])
                            if streaming:
                                self.wfile.write(error_frame(504, "broker queue wait timed out"))
                                self.wfile.flush()
                            else:
                                self._json(504, {"error": "broker queue wait timed out",
                                                 "job_id": job["id"]})
                            return
                        if not streaming and self._peer_closed():
                            if key is None:
                                broker.cancel(job["id"])
                            return
                        job = broker.wait_for_terminal(job["id"], min(HEARTBEAT_SECONDS, remaining))
                        if job is None:
                            raise RuntimeError("submitted OpenClaw job disappeared")
                        if streaming and job["state"] not in {"completed", "failed", "cancelled"}:
                            self.wfile.write(heartbeat())
                            self.wfile.flush()
                    if job["state"] == "completed":
                        if streaming:
                            self.wfile.write(terminal_frame(job["result"]))
                            self.wfile.flush()
                        else:
                            self._json(200, job["result"], {"X-Broker-Job-Id": job["id"]})
                    else:
                        message = "broker job failed" if job["state"] == "failed" else "broker job cancelled"
                        status = 502 if job["state"] == "failed" else 409
                        if streaming:
                            self.wfile.write(error_frame(status, message))
                            self.wfile.flush()
                        else:
                            self._json(status, {"error": message, "job_id": job["id"]})
                except (BrokenPipeError, ConnectionResetError, OSError):
                    if key is None:
                        broker.cancel(job["id"])
            finally:
                openclaw_slots.release()

        def do_POST(self):
            if urlsplit(self.path).path == "/openclaw/api/chat":
                self._openclaw_chat()
                return
            size=int(self.headers.get("Content-Length", 0))
            try:
                body=json.loads(self.rfile.read(size) or b"{}")
            except (TypeError, ValueError):
                self._json(400, {"error": "request body must be JSON"})
                return
            path = urlsplit(self.path).path
            if path == "/v1/jobs":
                if (
                    isinstance(body, dict) and body.get("producer_storage") is not None
                    and not self._storage_authorized()
                ):
                    return
                try:
                    self._json(202, broker.submit(body["profile"], body["kind"], body.get("payload", {}),
                                                  body.get("source"), body.get("source_item_id"),
                                                  body.get("external_id"),
                                                  body.get("producer_storage")))
                except SourceAdmissionBlocked as error: self._admission_blocked(error)
                except SourceQueueFull as error: self._json(429, {"error": str(error)})
                except (KeyError, TypeError, ValueError, StorageContractError) as e: self._json(400, {"error": str(e)})
            elif path.startswith("/v1/jobs/") and path.endswith("/input-received"):
                if not self._storage_authorized():
                    return
                try:
                    result = broker.acknowledge_input(path.split("/")[3], body)
                    self._json(200 if result else 404, result or {"error": "not found"})
                except ReceiptConflict as error:
                    self._json(409, {"error": {"code": error.code, "message": str(error)}})
                except StorageContractError as error:
                    self._json(400, {"error": str(error)})
            elif path.startswith("/v1/jobs/") and path.endswith("/ack"):
                if not self._storage_authorized():
                    return
                try:
                    result = broker.acknowledge_result(path.split("/")[3], body)
                    self._json(200 if result else 404, result or {"error": "not found"})
                except ReceiptConflict as error:
                    self._json(409, {"error": {"code": error.code, "message": str(error)}})
                except StorageContractError as error:
                    self._json(400, {"error": str(error)})
            elif path == "/v1/maintenance/compact":
                if not self._storage_authorized():
                    return
                try:
                    if not isinstance(body, dict) or set(body) - {
                        "source", "operation", "limit", "confirm", "max_bytes",
                    }:
                        raise StorageContractError("invalid maintenance request fields")
                    result = broker.storage_maintenance(
                        body["source"], operation=body.get("operation", "preview"),
                        limit=body.get("limit", 100), confirm=body.get("confirm", False),
                        max_bytes=body.get("max_bytes", 16 * 1024 * 1024),
                    )
                    self._json(200, result)
                except (KeyError, StorageContractError) as error:
                    self._json(409, {"error": str(error)})
            elif path.startswith("/v1/jobs/") and path.endswith("/cancel"):
                job_id = path.split("/")[3]
                if broker.producer_storage_job(job_id) and not self._storage_authorized():
                    return
                result=broker.cancel(job_id); self._json(200 if result else 404, result or {"error":"not found"})
            elif path.startswith("/v1/jobs/") and path.endswith("/retry"):
                job_id = path.split("/")[3]
                if broker.producer_storage_job(job_id) and not self._storage_authorized():
                    return
                try:
                    result=broker.retry(job_id); self._json(200 if result else 404, result or {"error":"not found"})
                except ValueError as e: self._json(409, {"error":str(e)})
            elif path.startswith("/v1/sources/") and path.endswith("/weight"):
                source = unquote(path[len("/v1/sources/"):-len("/weight")]).strip("/")
                try:
                    if not isinstance(body, dict) or set(body) != {"weight"}:
                        raise SourcePolicyError("request body must contain only weight")
                    if policy is None:
                        raise SourcePolicyError("source policy is not configured")
                    weight = policy.set_weight(source, body["weight"])
                    broker.audit_source_control(
                        "source.weight_changed", source, {"weight": weight}
                    )
                    self._json(200, {"source": source, "weight": weight})
                except SourcePolicyError as error:
                    self._json(400, {"error": str(error)})
            elif path.startswith("/v1/sources/") and path.endswith("/enabled"):
                source = unquote(path[len("/v1/sources/"):-len("/enabled")]).strip("/")
                try:
                    if not isinstance(body, dict) or set(body) != {"enabled"}:
                        raise SourcePolicyError("request body must contain only enabled")
                    if policy is None: raise SourcePolicyError("source policy is not configured")
                    enabled = policy.set_enabled(source, body["enabled"])
                    broker.audit_source_control(
                        "source.dispatch_changed", source,
                        {"enabled": enabled, "paused": not enabled},
                    )
                    self._json(200, {"source": source, "enabled": enabled})
                except SourcePolicyError as error:
                    self._json(400, {"error": str(error)})
            elif path.startswith("/v1/sources/") and path.endswith("/dispatch"):
                source = unquote(path[len("/v1/sources/"):-len("/dispatch")]).strip("/")
                try:
                    if not isinstance(body, dict) or set(body) != {"paused"}:
                        raise SourcePolicyError("request body must contain only paused")
                    if not isinstance(body["paused"], bool):
                        raise SourcePolicyError("paused must be a boolean")
                    if policy is None: raise SourcePolicyError("source policy is not configured")
                    enabled = policy.set_enabled(source, not body["paused"])
                    broker.audit_source_control(
                        "source.dispatch_changed", source,
                        {"enabled": enabled, "paused": not enabled},
                    )
                    self._json(200, {"source": source, "paused": not enabled, "enabled": enabled})
                except SourcePolicyError as error:
                    self._json(400, {"error": str(error)})
            elif path.startswith("/v1/sources/") and path.endswith("/admission"):
                source = unquote(path[len("/v1/sources/"):-len("/admission")]).strip("/")
                try:
                    if not isinstance(body, dict) or set(body) != {"allowed"}:
                        raise SourcePolicyError("request body must contain only allowed")
                    if policy is None: raise SourcePolicyError("source policy is not configured")
                    allowed = policy.set_admission_allowed(source, body["allowed"])
                    broker.audit_source_control(
                        "source.admission_changed", source,
                        {"admission_allowed": allowed},
                    )
                    self._json(200, {"source": source, "admission_allowed": allowed})
                except SourcePolicyError as error:
                    self._json(400, {"error": str(error)})
            elif path.startswith("/v1/sources/") and path.endswith("/queued/cancel"):
                source = unquote(path[len("/v1/sources/"):-len("/queued/cancel")]).strip("/")
                try:
                    if not isinstance(body, dict) or body != {"confirm": True}:
                        raise SourcePolicyError("bulk cancellation requires {\"confirm\": true}")
                    self._configured_source(source)
                    self._json(200, broker.bulk_cancel_queued(
                        source,
                        allow_producer_storage=self._storage_authenticated(),
                    ))
                except StorageAuthorizationRequired:
                    self._storage_authorized()
                except (SourcePolicyError, ValueError) as error:
                    self._json(400, {"error": str(error)})
            elif path.startswith("/v1/sources/") and path.endswith("/failed/retry"):
                source = unquote(path[len("/v1/sources/"):-len("/failed/retry")]).strip("/")
                try:
                    if not isinstance(body, dict) or body != {"confirm": True}:
                        raise SourcePolicyError("bulk retry requires {\"confirm\": true}")
                    self._configured_source(source)
                    self._json(200, broker.bulk_retry_failed(
                        source,
                        allow_producer_storage=self._storage_authenticated(),
                    ))
                except StorageAuthorizationRequired:
                    self._storage_authorized()
                except (SourcePolicyError, ValueError) as error:
                    self._json(400, {"error": str(error)})
            elif path in {"/api/chat", "/api/generate"}:
                try:
                    job=submit_compatibility(broker, path.rsplit("/", 1)[-1], body)
                    if body.get("stream", True): self._stream(202, stream_frames(job))
                    else: self._json(202, job)
                except SourceAdmissionBlocked as error: self._admission_blocked(error)
                except SourceQueueFull as error: self._json(429, {"error": str(error)})
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
                except SourceAdmissionBlocked as error: self._admission_blocked(error)
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
                except SourceAdmissionBlocked as error: self._admission_blocked(error)
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
                except SourceAdmissionBlocked as error: self._admission_blocked(error)
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
                except SourceAdmissionBlocked as error: self._admission_blocked(error)
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
                except SourceAdmissionBlocked as error: self._admission_blocked(error)
                except CompatibilityError as e: self._json(400, {"error":str(e)})
            else: self._json(404, {"error":"not found"})
        def do_GET(self):
            parsed = urlsplit(self.path)
            path = parsed.path
            query = parse_qs(parsed.query)
            if path == "/dashboard":
                include_producer_storage = self._storage_authenticated()
                etag, headers = self._dashboard_validator(include_producer_storage)
                if etag is None:
                    return
                dashboard = broker.dashboard(
                    policy,
                    include_producer_storage=include_producer_storage,
                )
                encoded = render_dashboard(dashboard)
                status = 503 if dashboard["observation"]["state"] == "unavailable" else 200
                self.send_response(status); self.send_header("Content-Type", "text/html; charset=utf-8"); self.send_header("Content-Length", str(len(encoded)))
                for name, value in headers.items(): self.send_header(name, value)
                self.end_headers(); self.wfile.write(encoded)
            elif path == "/v1/dashboard":
                include_producer_storage = self._storage_authenticated()
                etag, headers = self._dashboard_validator(include_producer_storage)
                if etag is None:
                    return
                dashboard = broker.dashboard(
                    policy,
                    include_producer_storage=include_producer_storage,
                )
                self._json(
                    503 if dashboard["observation"]["state"] == "unavailable" else 200,
                    dashboard, headers,
                )
            elif path == "/healthz": self._json(200, broker.health())
            elif path == "/v1/metrics": self._json(200, broker.metrics())
            elif path == "/v1/storage/health": self._json(200, broker.storage_health())
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
                    limit = int(query.get("limit", ["10"])[0])
                    self._json(200, broker.forecast(
                        policy, limit=limit,
                        include_producer_storage=self._storage_authenticated(),
                    ))
                except ValueError:
                    self._json(400, {"error":"limit must be an integer"})
            elif path == "/v1/history":
                try:
                    limit = int(query.get("limit", ["30"])[0])
                    raw = query.get("cursor", [None])[0]
                    cursor = None if raw is None else (float(raw.rsplit(":", 1)[0]), raw.rsplit(":", 1)[1])
                    body = broker.terminal_history(
                        limit=limit, cursor=cursor,
                        include_producer_storage=self._storage_authenticated(),
                    )
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
                        include_producer_storage=self._storage_authenticated(),
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
                        include_producer_storage=self._storage_authenticated(),
                    )})
                except ValueError as error:
                    self._json(400, {"error":str(error)})
            elif path == "/v1/sources":
                if policy is None:
                    self._json(200, {"policy": None})
                else:
                    self._json(200, {"policy": policy.snapshot()})
            elif path.startswith("/v1/jobs/") and path.endswith("/attempts"):
                job_id = path.split("/")[3]
                if broker.producer_storage_job(job_id) and not self._storage_authorized():
                    return
                result=broker.attempts(job_id); self._json(200 if result is not None else 404, {"attempts":result} if result is not None else {"error":"not found"})
            elif path.startswith("/v1/jobs/") and path.endswith("/receipt"):
                if not self._storage_authorized():
                    return
                result=broker.receipt(path.split("/")[3]); self._json(200 if result is not None else 404, result if result is not None else {"error":"not found"})
            elif path.startswith("/v1/jobs/") and path.endswith("/status"):
                if not self._storage_authorized():
                    return
                result=broker.compact_status(path.split("/")[3]); self._json(200 if result is not None else 404, result if result is not None else {"error":"not found"})
            elif path.startswith("/v1/jobs/"):
                job_id = path.split("/")[3]
                if broker.producer_storage_job(job_id) and not self._storage_authorized():
                    return
                result=broker.status(job_id); self._json(200 if result else 404, result or {"error":"not found"})
            else: self._json(404, {"error":"not found"})
        def log_message(self, *_): pass
    return ThreadingHTTPServer((host, port), Handler)
