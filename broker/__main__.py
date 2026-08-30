import logging
import os
import signal
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


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("BROKER_LOG_LEVEL", "INFO").upper(),
        format="%(message)s",
    )
    broker = Broker(
        os.environ.get("BROKER_DB", "broker.sqlite3"),
        OllamaHTTP(
            os.environ.get("OLLAMA_URL", "http://192.168.2.5:11434"),
            float(os.environ.get("OLLAMA_TIMEOUT_SECONDS", "300")),
        ),
        WakeOnLan(os.environ["MAINPC_MAC"]),
    )
    policy = None
    configured_policy = policy_path(os.environ.get("BROKER_SOURCES_POLICY"))
    if configured_policy is not None:
        policy = SourcePolicy(configured_policy)
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
        # Local systemd signal: stop claims but retain HTTP admissions until
        # the active remote call completes, then restart safely.
        signal.signal(signal.SIGUSR1, lambda _signum, _frame: dispatcher.drain())
        dispatcher.start()
    serve(broker, os.environ.get("BROKER_BIND", "127.0.0.1"), int(os.environ.get("BROKER_PORT", "8088")), policy=policy).serve_forever()


if __name__ == "__main__":
    main()
