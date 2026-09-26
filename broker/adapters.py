"""Network seams. Tests inject fakes; no live service is touched by default."""
from __future__ import annotations

import json
import socket
import subprocess
import time


class WakeOnLan:
    def __init__(self, mac: str, broadcast: str = "255.255.255.255"):
        self.mac, self.broadcast = mac, broadcast

    def wake(self) -> None:
        mac = bytes.fromhex(self.mac.replace(":", ""))
        packet = b"\xff" * 6 + mac * 16
        for port in (9, 7):
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                sock.sendto(packet, (self.broadcast, port))


class OllamaHTTP:
    def __init__(
        self, base_url: str = "http://127.0.0.1:11434", timeout_seconds: float = 300
    ):
        self.base_url = base_url.rstrip("/")
        if timeout_seconds <= 0:
            raise ValueError("Ollama timeout must be positive")
        self.timeout_seconds = timeout_seconds

    def _request(self, path: str, body: dict | None = None, timeout_seconds: float | None = None):
        """Make an executor request with an enforceable wall-clock deadline."""
        timeout = float(timeout_seconds or self.timeout_seconds)
        if timeout <= 0:
            raise ValueError("Ollama request timeout must be positive")
        command = [
            "curl", "--silent", "--show-error", "--fail-with-body",
            "--max-time", str(timeout),
            "--connect-timeout", str(min(timeout, 30.0)),
            "--header", "Content-Type: application/json",
            "--request", "POST" if body is not None else "GET",
            self.base_url + path,
        ]
        if body is not None:
            command.extend(["--data-binary", "@-"])
        process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE if body is not None else subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        payload = None if body is None else json.dumps(body).encode()
        try:
            stdout, stderr = process.communicate(payload, timeout=timeout + 2.0)
        except subprocess.TimeoutExpired as exc:
            process.kill()
            process.communicate()
            raise TimeoutError(f"Ollama request exceeded {timeout:g}s deadline") from exc
        if process.returncode != 0:
            detail = stderr.decode("utf-8", "replace").strip()
            raise RuntimeError(f"Ollama request failed (curl exit {process.returncode}): {detail[:300]}")
        try:
            return json.loads(stdout or b"{}")
        except json.JSONDecodeError as exc:
            raise RuntimeError("Ollama returned invalid JSON") from exc

    def ps(self, timeout_seconds: float | None = None) -> dict:
        return self._request("/api/ps", timeout_seconds=timeout_seconds)

    def is_ready(self, model: str) -> bool:
        return any(item.get("name") == model for item in self.ps().get("models", []))

    def wait_ready(
        self,
        model: str,
        timeout_seconds: float = 30,
        poll_seconds: float = 1,
    ) -> bool:
        """Wait for Ollama to publish a successfully loaded model in ``/api/ps``.

        Ollama can return from the explicit load request just before the model
        becomes visible to a concurrent ``/api/ps`` request.  Keep this wait
        bounded so an unhealthy executor still fails closed.
        """
        deadline = time.monotonic() + timeout_seconds
        while True:
            if self.is_ready(model):
                return True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            time.sleep(min(poll_seconds, remaining))

    def unload(self, model: str, timeout_seconds: float | None = None) -> None:
        self._request(
            "/api/generate", {"model": model, "keep_alive": 0}, timeout_seconds
        )

    def ensure_model_ready(
        self,
        model: str,
        *,
        keep_alive: str,
        timeout_seconds: float = 300,
        poll_seconds: float = 1,
    ) -> bool:
        """Make ``model`` the only resident model within one bounded deadline.

        Ollama may acknowledge an unload before ``/api/ps`` stops reporting the
        old model.  Loading the target during that interval can leave the old
        model resident and never make the target visible.  Wait for all
        incompatible models to disappear before issuing the one explicit load,
        then require an exclusive target-model observation.
        """
        if timeout_seconds <= 0 or poll_seconds < 0:
            raise ValueError("model readiness timeouts must be positive")
        deadline = time.monotonic() + timeout_seconds
        unload_requested: set[str] = set()
        load_requested = False
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            models = self.ps(timeout_seconds=remaining).get("models", [])
            loaded = {
                item.get("name")
                for item in models
                if isinstance(item, dict) and isinstance(item.get("name"), str)
            }
            incompatible = loaded - {model}
            if incompatible:
                for resident in sorted(incompatible - unload_requested):
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return False
                    self.unload(resident, timeout_seconds=remaining)
                    unload_requested.add(resident)
            elif model in loaded:
                return True
            elif not load_requested:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._request(
                    "/api/generate",
                    {"model": model, "keep_alive": keep_alive},
                    timeout_seconds=remaining,
                )
                load_requested = True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            time.sleep(min(poll_seconds, remaining))

    def run(self, kind: str, request: dict) -> dict:
        # This private field is set by the broker from a server-owned profile;
        # it is never sent to Ollama or accepted from the public API.
        payload = dict(request)
        timeout_seconds = payload.pop("_broker_timeout_seconds", None)
        return self._request("/api/" + kind, payload, timeout_seconds)
