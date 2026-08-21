import os
from .adapters import OllamaHTTP, WakeOnLan
from .http import serve
from .service import Broker, Dispatcher

broker = Broker(os.environ.get("BROKER_DB", "broker.sqlite3"), OllamaHTTP(os.environ.get("OLLAMA_URL", "http://192.168.2.5:11434")), WakeOnLan(os.environ["MAINPC_MAC"]))
Dispatcher(broker).start()
serve(broker, os.environ.get("BROKER_BIND", "127.0.0.1"), int(os.environ.get("BROKER_PORT", "8088"))).serve_forever()
