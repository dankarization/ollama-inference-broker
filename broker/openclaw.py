"""Bounded native Ollama /api/chat contract for OpenClaw agent turns."""
from __future__ import annotations

import json
import math
import base64
import binascii
from typing import Any

from .profiles import OPENCLAW_PROFILES_BY_MODEL, PROFILES

MODEL = PROFILES["openclaw"].model
MAX_REQUEST_BYTES = 32 * 1024 * 1024
MAX_TEXT_REQUEST_BYTES = 2 * 1024 * 1024
MAX_IMAGE_BYTES = 24 * 1024 * 1024
MAX_TOOL_BYTES = 512 * 1024
MAX_MESSAGES = 512
MAX_TOOLS = 128
MAX_WAIT_SECONDS = 1_800
MAX_OUTSTANDING = 8
HEARTBEAT_SECONDS = 2


class OpenClawRequestError(ValueError):
    pass


def _json_size(value: Any) -> int:
    try:
        return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode())
    except (TypeError, ValueError) as exc:
        raise OpenClawRequestError("request must be JSON serializable") from exc


def normalize_request(request: Any) -> dict:
    if not isinstance(request, dict):
        raise OpenClawRequestError("request must be a JSON object")
    allowed = {"model", "messages", "tools", "stream", "options", "format", "think",
               "truncate", "shift", "keep_alive"}
    if set(request) - allowed:
        raise OpenClawRequestError("unsupported Ollama chat request fields")
    model = request.get("model")
    profile_name = OPENCLAW_PROFILES_BY_MODEL.get(model) if isinstance(model, str) else None
    if profile_name is None:
        raise OpenClawRequestError("model must be a configured OpenClaw model")
    profile = PROFILES[profile_name]
    if not isinstance(request.get("stream", True), bool):
        raise OpenClawRequestError("stream must be boolean")
    messages = request.get("messages")
    if not isinstance(messages, list) or not 1 <= len(messages) <= MAX_MESSAGES:
        raise OpenClawRequestError("messages must contain 1 to 512 entries")
    image_count = 0
    image_bytes = 0
    for message in messages:
        if not isinstance(message, dict) or message.get("role") not in {
            "system", "user", "assistant", "tool"
        } or not isinstance(message.get("content"), str):
            raise OpenClawRequestError("messages require role and string content")
        if set(message) - {"role", "content", "thinking", "tool_calls", "tool_call_id", "tool_name", "images"}:
            raise OpenClawRequestError("unsupported message fields")
        if "images" in message:
            images = message["images"]
            if (profile.max_images == 0 or message["role"] != "user"
                    or not isinstance(images, list) or not images):
                raise OpenClawRequestError("images require a vision model and user message")
            image_count += len(images)
            if image_count > profile.max_images:
                raise OpenClawRequestError("too many images for OpenClaw model")
            for image in images:
                if not isinstance(image, str):
                    raise OpenClawRequestError("images must be base64 strings")
                try:
                    image_bytes += len(base64.b64decode(image, validate=True))
                except (ValueError, UnicodeEncodeError, binascii.Error):
                    raise OpenClawRequestError("images must be valid base64") from None
                if image_bytes > MAX_IMAGE_BYTES:
                    raise OpenClawRequestError("decoded images exceed 24 MiB")
        if "thinking" in message and (message["role"] != "assistant" or not isinstance(message["thinking"], str)):
            raise OpenClawRequestError("thinking must be assistant text")
        if "tool_calls" in message:
            if message["role"] != "assistant":
                raise OpenClawRequestError("only assistant messages may contain tool calls")
            _validate_tool_calls(message["tool_calls"])
        for field in ("tool_call_id", "tool_name"):
            if field in message and (message["role"] != "tool" or not isinstance(message[field], str)):
                raise OpenClawRequestError(f"{field} must be tool text")
    tools = request.get("tools", [])
    if not isinstance(tools, list) or len(tools) > MAX_TOOLS or _json_size(tools) > MAX_TOOL_BYTES:
        raise OpenClawRequestError("tools exceed the 128-entry/512-KiB limit")
    for tool in tools:
        function = tool.get("function") if isinstance(tool, dict) else None
        if (not isinstance(function, dict) or tool.get("type") != "function"
                or not isinstance(function.get("name"), str) or not function["name"]
                or not isinstance(function.get("parameters"), dict)):
            raise OpenClawRequestError("tools must be Ollama function definitions")
    options = request.get("options", {})
    if not isinstance(options, dict) or set(options) - {
        "num_ctx", "num_predict", "temperature", "top_p", "top_k", "min_p",
        "typical_p", "repeat_last_n", "repeat_penalty", "presence_penalty",
        "frequency_penalty", "seed", "stop"
    }:
        raise OpenClawRequestError("unsupported Ollama options")
    for field in ("num_ctx", "num_predict"):
        value = options.get(field)
        if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value <= 0):
            raise OpenClawRequestError(f"{field} must be a positive integer")
    for field, value in options.items():
        if field in {"num_ctx", "num_predict"}:
            continue
        if field == "stop":
            if not isinstance(value, list) or len(value) > 32 or any(
                not isinstance(item, str) or len(item) > 256 for item in value
            ):
                raise OpenClawRequestError("stop must contain at most 32 bounded strings")
        elif (isinstance(value, bool) or not isinstance(value, (int, float))
              or not math.isfinite(value)):
            raise OpenClawRequestError(f"{field} must be finite numeric")
    if request.get("truncate", False) is not False or request.get("shift", False) is not False:
        raise OpenClawRequestError("context truncation and shifting are not allowed")
    think = request.get("think")
    if think is not None and think not in (True, False, "low", "medium", "high", "max"):
        raise OpenClawRequestError("unsupported thinking level")
    format_value = request.get("format")
    if format_value is not None and format_value != "json" and not isinstance(format_value, dict):
        raise OpenClawRequestError("format must be json or a JSON Schema")
    if format_value is not None and _json_size(format_value) > 65_536:
        raise OpenClawRequestError("format exceeds 64 KiB")
    if _json_size(request) > (MAX_REQUEST_BYTES if image_count else MAX_TEXT_REQUEST_BYTES):
        raise OpenClawRequestError("request exceeds model-specific byte limit")
    payload = {"messages": messages, "options": options, "truncate": False, "shift": False}
    if tools:
        payload["tools"] = tools
    if think is not None:
        payload["think"] = think
    if format_value is not None:
        payload["format"] = format_value
    return payload


def _validate_tool_calls(calls: Any) -> None:
    if not isinstance(calls, list) or len(calls) > MAX_TOOLS:
        raise OpenClawRequestError("tool_calls must be a bounded array")
    for call in calls:
        function = call.get("function") if isinstance(call, dict) else None
        if (not isinstance(function, dict) or not isinstance(function.get("name"), str)
                or not function["name"] or not isinstance(function.get("arguments"), (dict, str))):
            raise OpenClawRequestError("invalid tool call")


def validate_result(result: dict, model: str) -> None:
    message = result.get("message")
    if (result.get("done") is not True or not isinstance(message, dict)
            or message.get("role") != "assistant" or not isinstance(message.get("content"), str)
            or result.get("model", model) != model):
        raise RuntimeError("Ollama returned an invalid OpenClaw chat response")
    for field in ("thinking", "reasoning"):
        if field in message and not isinstance(message[field], str):
            raise RuntimeError("Ollama returned invalid thinking content")
    if "tool_calls" in message:
        try:
            _validate_tool_calls(message["tool_calls"])
        except OpenClawRequestError as exc:
            raise RuntimeError("Ollama returned invalid tool calls") from exc


def frame(value: dict) -> bytes:
    return (json.dumps(value, separators=(",", ":"), ensure_ascii=False) + "\n").encode()


def heartbeat() -> bytes:
    return frame({"message": {"role": "assistant", "content": ""}, "done": False})


def terminal_frame(result: dict) -> bytes:
    return frame(result)


def error_frame(status: int, message: str) -> bytes:
    return frame({"error": message, "status": status})
