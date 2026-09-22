import logging
import math
import os
import signal
from pathlib import Path
from .adapters import OllamaHTTP, WakeOnLan
from .http import serve
from .service import Broker, Dispatcher, SourcePolicy


def dispatch_enabled(value: str | None) -> bool:
    """Parse the explicit deployment guard without silently accepting typos."""
    if value is None:
        return True
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError("BROKER_DISPATCH_ENABLED must be true or false")


def dispatch_sources(value: str | None) -> frozenset[str]:
    """Parse the exact source allowlist required for an enabled dispatcher."""
    if value is None:
        raise ValueError("BROKER_DISPATCH_SOURCES is required when dispatch is enabled")
    sources = frozenset(item.strip() for item in value.split(",") if item.strip())
    if not sources:
        raise ValueError("BROKER_DISPATCH_SOURCES must contain at least one source")
    return sources


def policy_path(value: str | None) -> str | None:
    """Return the runtime source-policy path, or None when unset/empty."""
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def positive_number(value: str | None, default: float, name: str) -> float:
    """Parse a positive numeric operational limit without accepting zero."""
    if value is None:
        return default
    try:
        parsed = float(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a positive number") from exc
    if not math.isfinite(parsed) or parsed <= 0:
        raise ValueError(f"{name} must be a positive number")
    return parsed


def positive_integer(value: str | None, default: int, name: str) -> int:
    """Parse a positive integer operational limit without truncation."""
    if value is None:
        return default
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a positive integer") from exc
    if parsed <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return parsed


def storage_token(path: str | None) -> str | None:
    """Load the storage mutation credential from an owner-only file."""
    configured = policy_path(path)
    if configured is None:
        return None
    token_path = Path(configured)
    stat = token_path.stat()
    if stat.st_uid != os.getuid() or stat.st_mode & 0o077:
        raise ValueError("BROKER_STORAGE_TOKEN_FILE must be owner-only")
    token = token_path.read_text(encoding="utf-8").strip()
    if len(token) < 32:
        raise ValueError("BROKER_STORAGE_TOKEN_FILE must contain at least 32 characters")
    return token


def install_drain_handler(dispatcher: Dispatcher | None) -> None:
    """Keep SIGUSR1 reload safe even for admission-only broker instances."""
    signal.signal(
        signal.SIGUSR1,
        lambda _signum, _frame: dispatcher.drain() if dispatcher is not None else None,
    )


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("BROKER_LOG_LEVEL", "INFO").upper(),
        format="%(message)s",
    )
    configured_storage_token = storage_token(
        os.environ.get("BROKER_STORAGE_TOKEN_FILE")
    )
    broker = Broker(
        os.environ.get("BROKER_DB", "broker.sqlite3"),
        OllamaHTTP(
            os.environ.get("OLLAMA_URL", "http://192.168.2.5:11434"),
            float(os.environ.get("OLLAMA_TIMEOUT_SECONDS", "300")),
        ),
        WakeOnLan(os.environ["MAINPC_MAC"]),
        wal_autocheckpoint_pages=positive_integer(
            os.environ.get("BROKER_WAL_AUTOCHECKPOINT_PAGES"), 4096,
            "BROKER_WAL_AUTOCHECKPOINT_PAGES",
        ),
        journal_size_limit_bytes=positive_integer(
            os.environ.get("BROKER_JOURNAL_SIZE_LIMIT_BYTES"), 64 * 1024 * 1024,
            "BROKER_JOURNAL_SIZE_LIMIT_BYTES",
        ),
        wal_budget_bytes=positive_integer(
            os.environ.get("BROKER_WAL_BUDGET_BYTES"), 128 * 1024 * 1024,
            "BROKER_WAL_BUDGET_BYTES",
        ),
        checkpoint_interval_seconds=positive_number(
            os.environ.get("BROKER_WAL_CHECKPOINT_INTERVAL_SECONDS"), 60,
            "BROKER_WAL_CHECKPOINT_INTERVAL_SECONDS",
        ),
        min_free_space_bytes=positive_integer(
            os.environ.get("BROKER_MIN_FREE_SPACE_BYTES"), 2 * 1024 * 1024 * 1024,
            "BROKER_MIN_FREE_SPACE_BYTES",
        ),
    )
    policy = None
    configured_policy = policy_path(os.environ.get("BROKER_SOURCES_POLICY"))
    if configured_policy is not None:
        policy = SourcePolicy(configured_policy)
    dispatcher = None
    if dispatch_enabled(os.environ.get("BROKER_DISPATCH_ENABLED")):
        allowed_sources = None
        if policy is None:
            # FIFO mode: env allowlist is required and immutable
            # until a restart (previous behaviour).
            allowed_sources = dispatch_sources(os.environ.get("BROKER_DISPATCH_SOURCES"))
        dispatcher = Dispatcher(
            broker,
            allowed_sources=allowed_sources,
            policy=policy,
        )
        dispatcher.start()
    # Local systemd reload: drain claims when enabled; no-op while admission-only.
    install_drain_handler(dispatcher)
    serve(
        broker,
        os.environ.get("BROKER_BIND", "127.0.0.1"),
        int(os.environ.get("BROKER_PORT", "8088")),
        policy=policy,
        storage_token=configured_storage_token,
    ).serve_forever()


if __name__ == "__main__":
    main()
