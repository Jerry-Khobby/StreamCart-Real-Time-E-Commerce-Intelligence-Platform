"""Failure alerts reach a webhook, and a broken webhook never raises."""

import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.getenv("DAG_FOLDER", str(Path(__file__).resolve().parent.parent / "batch" / "dags")))
if not Path(sys.path[0]).exists():
    sys.path[0] = str(Path(__file__).resolve().parent.parent / "dags")

from common import failure_message, send_alert  # noqa: E402


@pytest.fixture
def webhook():
    received = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            received.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            self.send_response(200)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_port}/hook", received
    server.shutdown()


def test_an_alert_is_posted_as_chat_text(webhook):
    url, received = webhook
    assert send_alert("Silver failed", url=url)
    assert received == [{"text": "Silver failed"}]


def test_no_webhook_means_nothing_is_sent(monkeypatch):
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    assert send_alert("ignored") is False


def test_an_unreachable_webhook_does_not_raise():
    assert send_alert("lost", url="http://127.0.0.1:9/nothing-listens-here") is False


def test_the_message_names_the_task_the_error_and_the_log():
    instance = SimpleNamespace(dag_id="silver_pipeline", task_id="silver_transactions",
                               run_id="manual__1", log_url="http://localhost:8080/log")
    text = failure_message({"task_instance": instance, "exception": ValueError("gate failed")})
    assert "silver_pipeline.silver_transactions" in text and "gate failed" in text and "/log" in text
