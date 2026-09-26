import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from broker import __main__ as app
from broker import local_config
from broker import status_reporter


class LocalConfigTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "local.json"
        self.values = {
            "ollama_url": "http://ollama.test:11434",
            "mainpc_mac": "02:00:00:00:00:01",
        }
        self.path.write_text(json.dumps(self.values), encoding="utf-8")
        self.path.chmod(0o600)

    def test_rejects_non_owner_only_and_symlink_config(self):
        self.path.chmod(0o640)
        with self.assertRaisesRegex(ValueError, "owner-only"):
            local_config.load_local_config(self.path)
        self.path.chmod(0o600)
        link = self.path.with_name("linked.json")
        link.symlink_to(self.path)
        with self.assertRaisesRegex(ValueError, "Cannot read"):
            local_config.load_local_config(link)
        with patch("broker.local_config.os.getuid", return_value=os.getuid() + 1):
            with self.assertRaisesRegex(ValueError, "owner-only"):
                local_config.load_local_config(self.path)

    def test_file_is_read_and_environment_overrides_broker_values(self):
        self.assertEqual(local_config.load_local_config(self.path), self.values)
        with (
            patch.dict(os.environ, {}, clear=True),
            patch.object(local_config, "DEFAULT_LOCAL_CONFIG_PATH", self.path),
        ):
            self.assertEqual(local_config.load_local_config(), self.values)
            self.assertEqual(local_config.ollama_url(self.values), self.values["ollama_url"])
            self.assertEqual(local_config.mainpc_mac(self.values), self.values["mainpc_mac"])
        with patch.dict(os.environ, {
            "OLLAMA_URL": "http://other.test:11434",
            "MAINPC_MAC": "02:00:00:00:00:02",
        }, clear=True):
            self.assertEqual(local_config.ollama_url(self.values), "http://other.test:11434")
            self.assertEqual(local_config.mainpc_mac(self.values), "02:00:00:00:00:02")

    def test_missing_or_invalid_config_has_no_localhost_fallback(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(local_config.load_local_config(self.path.with_name("missing.json")), {})
            with self.assertRaisesRegex(ValueError, "Set OLLAMA_URL"):
                local_config.ollama_url({})
            with self.assertRaisesRegex(ValueError, "Set MAINPC_MAC"):
                local_config.mainpc_mac({})
            for data in ('{', '{"ollama_url": "http://ok", "extra": "x"}'):
                self.path.write_text(data, encoding="utf-8")
                with self.assertRaisesRegex(ValueError, "local executor config"):
                    local_config.load_local_config(self.path)
            for url in ("http://127.0.0.1:bad", "file:///tmp/x", "http://user@host:11434"):
                with self.subTest(url=url), self.assertRaisesRegex(ValueError, r"HTTP\(S\) origin"):
                    local_config.ollama_url({"ollama_url": url})
            with self.assertRaisesRegex(ValueError, "six colon-separated"):
                local_config.mainpc_mac({"mainpc_mac": "invalid"})

    def test_broker_startup_uses_file_without_inline_executor_values(self):
        with (
            patch.dict(os.environ, {"BROKER_DISPATCH_ENABLED": "false"}, clear=True),
            patch.dict(os.environ, {"BROKER_LOCAL_CONFIG": str(self.path)}, clear=False),
            patch.object(app, "Broker"),
            patch.object(app, "OllamaHTTP") as ollama,
            patch.object(app, "WakeOnLan") as wol,
            patch.object(app, "serve") as serve,
            patch.object(app, "install_drain_handler"),
        ):
            app.main()
        self.assertEqual(ollama.call_args.args[0], self.values["ollama_url"])
        self.assertEqual(wol.call_args.args[0], self.values["mainpc_mac"])
        serve.return_value.serve_forever.assert_called_once()

    def test_report_probe_uses_same_file_and_report_override(self):
        urls = []
        with (
            patch.dict(os.environ, {}, clear=True),
            patch.dict(os.environ, {"BROKER_LOCAL_CONFIG": str(self.path)}, clear=False),
        ):
            health = status_reporter.health_snapshot(
                service_probe=lambda _: True,
                http_probe=lambda url: urls.append(url) or True,
            )
        self.assertTrue(all(health.values()))
        self.assertEqual(urls[-1], self.values["ollama_url"] + "/api/ps")
        with (
            patch.dict(os.environ, {"BROKER_OLLAMA_URL": "http://other.test:11434"}, clear=True),
            patch.dict(os.environ, {"BROKER_LOCAL_CONFIG": str(self.path)}, clear=False),
        ):
            urls.clear()
            status_reporter.health_snapshot(
                service_probe=lambda _: True,
                http_probe=lambda url: urls.append(url) or True,
            )
        self.assertEqual(urls[-1], "http://other.test:11434/api/ps")


if __name__ == "__main__":
    unittest.main()
