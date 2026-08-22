"""Safe, asynchronous Ollama-shaped admission contract."""

from __future__ import annotations

import json
from typing import Any

from .profiles import PROFILES


class CompatibilityError(ValueError):
    """The supplied Ollama-shaped request cannot enter broker admission."""


def submit(broker: Any, kind: str, request: Any) -> dict[str, Any]:
    if kind not in {"chat", "generate"}:
        raise CompatibilityError("unsupported compatibility endpoint")
    if not isinstance(request, dict):
        raise CompatibilityError("request must be a JSON object")
    profile = request.get("profile")
    if not isinstance(profile, str) or profile not in PROFILES:
        raise CompatibilityError("a known server-side profile is required")
    if "model" in request and request["model"] != PROFILES[profile].model:
        raise CompatibilityError("caller model must match the selected server-side profile")
    if kind == "generate" and not isinstance(request.get("prompt"), str):
        raise CompatibilityError("generate requires a string prompt")
    if kind == "chat" and not _valid_messages(request.get("messages")):
        raise CompatibilityError("chat requires a non-empty messages array with string role and content")
    if "stream" in request and not isinstance(request["stream"], bool):
        raise CompatibilityError("stream must be boolean")
    source = request.get("source")
    if source is not None and not isinstance(source, str):
        raise CompatibilityError("source must be a string")
    priority = request.get("priority")
    if priority is not None and (isinstance(priority, bool) or not isinstance(priority, int)):
        raise CompatibilityError("priority must be an integer when supplied")
    payload = {key: value for key, value in request.items() if key not in {
        "profile", "source", "priority", "model", "keep_alive", "stream"
    }}
    try:
        return broker.submit(profile, kind, payload, source, priority)
    except ValueError as exc:
        raise CompatibilityError(str(exc)) from exc


def stream_frames(job: dict[str, Any]) -> list[bytes]:
    """Return persisted job state as NDJSON without polling an executor."""
    model = PROFILES[job["profile"]].model
    frames = [_frame({"model": model, "created_at": job["created"], "done": False,
                      "broker": {"job_id": job["id"], "state": job["state"],
                                 "queue_position": job.get("queue_position")}})]
    if job["state"] == "cancelled":
        frames.append(_frame({"model": model, "done": True, "done_reason": "cancelled"}))
    elif job["state"] == "failed":
        frames.append(_frame({"model": model, "done": True, "error": "broker job failed"}))
    return frames


def _valid_messages(messages: Any) -> bool:
    return isinstance(messages, list) and bool(messages) and all(
        isinstance(message, dict) and isinstance(message.get("role"), str)
        and isinstance(message.get("content"), str) for message in messages
    )


def _frame(value: dict[str, Any]) -> bytes:
    return (json.dumps(value, separators=(",", ":")) + "\n").encode()
