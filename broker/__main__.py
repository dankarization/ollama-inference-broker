import os
from .adapters import OllamaHTTP, WakeOnLan
from .http import serve
from .service import Broker, Dispatcher


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


def main() -> None:
    broker = Broker(
        os.environ.get("BROKER_DB", "broker.sqlite3"),
        OllamaHTTP(os.environ.get("OLLAMA_URL", "http://192.168.2.5:11434")),
        WakeOnLan(os.environ["MAINPC_MAC"]),
    )
    if dispatch_enabled(os.environ.get("BROKER_DISPATCH_ENABLED")):
        Dispatcher(broker).start()
    serve(broker, os.environ.get("BROKER_BIND", "127.0.0.1"), int(os.environ.get("BROKER_PORT", "8088"))).serve_forever()


if __name__ == "__main__":
    main()
