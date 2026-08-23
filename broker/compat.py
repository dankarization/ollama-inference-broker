"""Safe, asynchronous Ollama-shaped admission contract."""

from __future__ import annotations

import json
import base64
import binascii
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


def submit_shutterstock_canary(broker: Any, request: Any) -> dict[str, Any]:
    """Admit the active photo-worker's VLM shape under a distinct canary source."""
    payload = validate_shutterstock_canary_payload(request)
    try:
        return broker.submit("shutterstock-canary", "generate", payload, source="shutterstock-canary")
    except ValueError as exc:
        raise CompatibilityError(str(exc)) from exc


def validate_shutterstock_canary_payload(request: Any) -> dict[str, Any]:
    """Normalize the one bounded VLM payload that may use the canary profile."""
    if not isinstance(request, dict):
        raise CompatibilityError("request must be a JSON object")
    profile = PROFILES["shutterstock-canary"]
    if "model" in request and request["model"] != profile.model:
        raise CompatibilityError("caller model must match the canary profile")
    prompt = request.get("prompt")
    if not isinstance(prompt, str) or not prompt.strip() or len(prompt) > 12_000:
        raise CompatibilityError("prompt must be a non-empty string up to 12000 characters")
    images = request.get("images")
    if not isinstance(images, list) or not 1 <= len(images) <= profile.max_images:
        raise CompatibilityError(f"images must contain 1 to {profile.max_images} base64 images")
    total_bytes = 0
    for image in images:
        if not isinstance(image, str):
            raise CompatibilityError("images must be base64 strings")
        try:
            total_bytes += len(base64.b64decode(image, validate=True))
        except (ValueError, UnicodeEncodeError, binascii.Error):
            raise CompatibilityError("images must be valid base64") from None
    if total_bytes > 8 * 1024 * 1024:
        raise CompatibilityError("decoded images exceed the 8 MiB canary limit")
    schema = request.get("format")
    if not isinstance(schema, dict):
        raise CompatibilityError("format must be a JSON Schema object")
    try:
        schema_bytes = len(json.dumps(schema, separators=(",", ":")).encode())
    except (TypeError, ValueError):
        raise CompatibilityError("format must be JSON serializable") from None
    if schema_bytes > profile.max_schema_bytes:
        raise CompatibilityError("format exceeds the canary schema limit")
    return {
        "prompt": prompt, "images": images, "format": schema,
        "think": "max", "options": {"temperature": 0, "seed": 42},
    }


def validate_shutterstock_video_payload(request: Any) -> dict[str, Any]:
    """Normalize the bounded multimodal payload used by the local video lane.

    Video frames are sampled server-side by the worker into base64 JPEGs; the
    broker accepts at most ``max_images`` frames and a bounded JSON Schema
    (the decision contract), mirroring the canary shape without the canary's
    think/schema strictness.  ``think`` is not forced for the text model.
    """
    if not isinstance(request, dict):
        raise CompatibilityError("request must be a JSON object")
    profile = PROFILES["shutterstock-video"]
    if "model" in request and request["model"] != profile.model:
        raise CompatibilityError("caller model must match the video profile")
    prompt = request.get("prompt")
    if not isinstance(prompt, str) or not prompt.strip() or len(prompt) > 20_000:
        raise CompatibilityError("prompt must be a non-empty string up to 20000 characters")
    images = request.get("images")
    if images is not None:
        # The video lane is multimodal (sampled frames).  When present, frames
        # are validated and bounded; a plain text generate remains supported
        # for compatibility with the earlier server-owned contract.
        if not isinstance(images, list) or not 1 <= len(images) <= profile.max_images:
            raise CompatibilityError(f"images must contain 1 to {profile.max_images} base64 frames")
        total_bytes = 0
        for image in images:
            if not isinstance(image, str):
                raise CompatibilityError("images must be base64 strings")
            try:
                total_bytes += len(base64.b64decode(image, validate=True))
            except (ValueError, UnicodeEncodeError, binascii.Error):
                raise CompatibilityError("images must be valid base64") from None
        if total_bytes > 32 * 1024 * 1024:
            raise CompatibilityError("decoded frames exceed the 32 MiB video limit")
    else:
        images = []
    schema = request.get("format")
    if schema is not None and not isinstance(schema, dict):
        raise CompatibilityError("format must be a JSON Schema object")
    if schema is not None:
        try:
            schema_bytes = len(json.dumps(schema, separators=(",", ":")).encode())
        except (TypeError, ValueError):
            raise CompatibilityError("format must be JSON serializable") from None
        if schema_bytes > profile.max_schema_bytes:
            raise CompatibilityError("format exceeds the video schema limit")
    else:
        schema = {}
    return {
        "prompt": prompt, "images": images, "format": schema,
    }


def submit_shutterstock_video(broker: Any, request: Any) -> dict[str, Any]:
    """Admit one video chunk under the dedicated ``shutterstock-video`` source."""
    payload = validate_shutterstock_video_payload(request)
    try:
        return broker.submit("shutterstock-video", "generate", payload, source="shutterstock-video")
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
