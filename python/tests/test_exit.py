"""On a normal process exit, unreported usage goes out one last time (atexit). Uses the real HttpTransport and a real child process against a fake API run locally."""

import json
import os
import socketserver
import subprocess
import sys
import textwrap
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from helpers import app_settings, sync_page


class QuickServer(ThreadingHTTPServer):
    def server_bind(self):
        # HTTPServer.server_bind does a reverse lookup of the host name (getfqdn), which takes tens of seconds on some machines
        socketserver.TCPServer.server_bind(self)
        self.server_name, self.server_port = self.server_address[:2]


def test_a_process_that_exits_normally_reports_what_is_left():
    received = []

    class Api(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            self._reply(sync_page(app=app_settings(hash_key=None)))

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            if self.path == "/v1/ingest":
                received.append(body)
                self._reply({"duplicate": False, "accepted_rows": len(body["rows"]), "rejected_rows": [], "unknown_keys": [], "unknown_leases": []})
            else:
                self._reply({"results": [{"consumer": item["consumer"], "status": "unlimited", "quota_version": 0} for item in body["requests"]]})

        def _reply(self, payload):
            data = json.dumps(payload).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *args):
            pass

    server = QuickServer(("127.0.0.1", 0), Api)
    threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True).start()
    script = textwrap.dedent(
        f"""
        from usagerail import create_engine

        engine = create_engine(
            app_key="urk_test_exit",
            auth="custom",
            consumer=lambda context: "user-1",
            base_url="http://127.0.0.1:{server.server_address[1]}",
        )
        engine.sync().result(5)
        decision = engine.begin(method="GET", path="/x", context={{}})
        decision = decision.result(5) if hasattr(decision, "future") else decision
        engine.end(decision.handle, status=200, route="/x")
        # No manual report, just exit: atexit takes care of the last batch
        """
    )
    try:
        completed = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, timeout=30, env={**os.environ, "NO_PROXY": "*"})
    finally:
        server.shutdown()
        server.server_close()
    assert completed.returncode == 0, completed.stderr
    rows = [row for batch in received for row in batch["rows"]]
    assert [(row["consumer"], row["route"], row["result_type"], row["count"]) for row in rows] == [("user-1", "/x", "2xx", 1)]


def test_imports_where_the_platform_has_no_fork() -> None:
    # Windows has neither fork nor os.register_at_fork: remove both in a child process before importing, and every module must still work
    code = (
        "import os\n"
        "del os.fork, os.register_at_fork\n"
        "import usagerail, usagerail.clock, usagerail.transport, usagerail.engine, usagerail.fastapi, usagerail.flask\n"
        "print('ok')\n"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"
