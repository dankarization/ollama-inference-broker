"""Network seams. Tests inject fakes; no live service is touched by default."""
from __future__ import annotations

import json
import socket
import urllib.request


class WakeOnLan:
    def __init__(self, mac: str, broadcast: str = "192.168.2.255"):
        self.mac, self.broadcast = mac, broadcast

    def wake(self) -> None:
        mac = bytes.fromhex(self.mac.replace(":", ""))
        packet = b"\xff" * 6 + mac * 16
        for port in (9, 7):
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                sock.sendto(packet, (self.broadcast, port))


class OllamaHTTP:
    def __init__(self, base_url: str = "http://192.168.2.5:11434"):
        self.base_url = base_url.rstrip("/")

    def _request(self, path: str, body: dict | None = None):
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(self.base_url + path, data=data,
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.loads(response.read() or b"{}")

    def ps(self) -> dict:
        return self._request("/api/ps")

    def is_ready(self, model: str) -> bool:
        return any(item.get("name") == model for item in self.ps().get("models", []))

    def unload(self, model: str) -> None:
        self._request("/api/generate", {"model": model, "keep_alive": 0})

    def run(self, kind: str, request: dict) -> dict:
        return self._request("/api/" + kind, request)
