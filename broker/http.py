from __future__ import annotations
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .compat import CompatibilityError, stream_frames, submit as submit_compatibility

def serve(broker, host="127.0.0.1", port=8088):
    class Handler(BaseHTTPRequestHandler):
        def _json(self, status, value):
            encoded=json.dumps(value).encode(); self.send_response(status); self.send_header("Content-Type","application/json"); self.send_header("Content-Length",str(len(encoded))); self.end_headers(); self.wfile.write(encoded)
        def _stream(self, status, frames):
            encoded=b"".join(frames); self.send_response(status); self.send_header("Content-Type","application/x-ndjson"); self.send_header("Content-Length",str(len(encoded))); self.end_headers(); self.wfile.write(encoded)
        def do_POST(self):
            size=int(self.headers.get("Content-Length", 0)); body=json.loads(self.rfile.read(size) or b"{}")
            if self.path == "/v1/jobs":
                try: self._json(202, broker.submit(body["profile"], body["kind"], body.get("payload", {}),
                                                  body.get("source"), body.get("priority")))
                except (KeyError, ValueError) as e: self._json(400, {"error": str(e)})
            elif self.path.startswith("/v1/jobs/") and self.path.endswith("/cancel"):
                result=broker.cancel(self.path.split("/")[3]); self._json(200 if result else 404, result or {"error":"not found"})
            elif self.path in {"/api/chat", "/api/generate"}:
                try:
                    job=submit_compatibility(broker, self.path.rsplit("/", 1)[-1], body)
                    if body.get("stream", True): self._stream(202, stream_frames(job))
                    else: self._json(202, job)
                except CompatibilityError as e: self._json(400, {"error":str(e)})
            else: self._json(404, {"error":"not found"})
        def do_GET(self):
            if self.path == "/healthz": self._json(200, broker.health())
            elif self.path == "/v1/metrics": self._json(200, broker.metrics())
            elif self.path.startswith("/v1/jobs/"):
                result=broker.status(self.path.split("/")[3]); self._json(200 if result else 404, result or {"error":"not found"})
            else: self._json(404, {"error":"not found"})
        def log_message(self, *_): pass
    return ThreadingHTTPServer((host, port), Handler)
