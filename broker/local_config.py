"""Owner-local executor settings shared by the broker and status reporter."""

import json
import os
import re
import stat
from pathlib import Path
from urllib.parse import urlsplit


DEFAULT_LOCAL_CONFIG_PATH = Path.home() / ".config" / "ollama-inference-broker" / "local.json"
_KEYS = {"ollama_url", "mainpc_mac"}


def load_local_config(path: Path | None = None) -> dict[str, str]:
    """Read an optional untracked config; reject malformed or unexpected fields."""
    path = path or Path(os.environ.get("BROKER_LOCAL_CONFIG", DEFAULT_LOCAL_CONFIG_PATH)).expanduser()
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except FileNotFoundError:
        return {}
    except OSError as exc:
        raise ValueError(f"Cannot read local executor config at {path}") from exc
    try:
        with os.fdopen(fd, encoding="utf-8") as file:
            info = os.fstat(file.fileno())
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                    or info.st_mode & 0o077):
                raise ValueError(f"Local executor config at {path} must be owner-only")
            data = json.load(file)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Cannot read local executor config at {path}") from exc
    if not isinstance(data, dict) or set(data) - _KEYS or any(
        not isinstance(value, str) or not value.strip() for value in data.values()
    ):
        raise ValueError(f"Invalid local executor config at {path}")
    return data


def executor_setting(config: dict[str, str], key: str, env_name: str) -> str:
    value = os.environ.get(env_name) or config.get(key)
    if not value or not value.strip():
        raise ValueError(f"Set {env_name} or {key} in local.json")
    return value.strip()


def ollama_url(config: dict[str, str], env_name: str = "OLLAMA_URL") -> str:
    value = executor_setting(config, "ollama_url", env_name)
    try:
        url = urlsplit(value)
        valid = (
            url.scheme in {"http", "https"}
            and bool(url.hostname)
            and url.port != 0
            and not url.username
            and not url.password
            and url.path in {"", "/"}
            and not url.query
            and not url.fragment
        )
    except ValueError:
        valid = False
    if not valid:
        raise ValueError(f"{env_name}/ollama_url must be an HTTP(S) origin")
    return value.rstrip("/")


def mainpc_mac(config: dict[str, str]) -> str:
    value = executor_setting(config, "mainpc_mac", "MAINPC_MAC")
    if not re.fullmatch(r"[0-9a-fA-F]{2}(?::[0-9a-fA-F]{2}){5}", value):
        raise ValueError("MAINPC_MAC/mainpc_mac must contain six colon-separated hex octets")
    return value
