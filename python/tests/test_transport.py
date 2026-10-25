import gzip
import json
import socket
import socketserver
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from usagerail.transport import HttpTransport, TransportError, WorkerPool, is_pausing_status, parse_retry_after


class Recorder:
    def __init__(self):
        self.requests = []
        self.connections = set()
        # (status, headers, body); unset means 200 and some JSON
        self.reply = lambda handler: (200, {}, json.dumps({"ok": True}).encode())


class QuickServer(ThreadingHTTPServer):
    def server_bind(self):
        # HTTPServer.server_bind does a reverse lookup of the host name (getfqdn), which takes tens of seconds on some machines
        socketserver.TCPServer.server_bind(self)
        self.server_name, self.server_port = self.server_address[:2]


@pytest.fixture
def server():
    recorder = Recorder()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def _handle(self):
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length) if length else b""
            recorder.requests.append({"method": self.command, "path": self.path, "headers": dict(self.headers), "body": body})
            recorder.connections.add(self.client_address)
            status, headers, payload = recorder.reply(self)
            self.send_response(status)
            for name, value in headers.items():
                self.send_header(name, value)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        do_GET = _handle
        do_POST = _handle

        def log_message(self, *args):
            pass

    httpd = QuickServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=httpd.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    thread.start()
    recorder.url = f"http://127.0.0.1:{httpd.server_address[1]}/"
    yield recorder
    httpd.shutdown()
    httpd.server_close()


def transport_for(server, pool=None):
    return HttpTransport("urk_test_transport", "inst-1", server.url, pool=pool or WorkerPool(max_workers=1))


def test_requests_carry_the_app_key_and_instance_id_and_the_cursor_is_url_encoded(server):
    transport = transport_for(server)
    assert transport.sync("a+b/c=").result(5) == {"ok": True}
    request = server.requests[0]
    assert request["path"] == "/v1/sync?cursor=a%2Bb%2Fc%3D"
    assert request["headers"]["Authorization"] == "Bearer urk_test_transport"
    assert request["headers"]["X-Instance-Id"] == "inst-1"
    transport.leases({"requests": []}, 299.99958300000003).result(5)
    assert json.loads(server.requests[1]["body"]) == {"requests": []}
    assert server.requests[1]["headers"]["Content-Type"] == "application/json"


def test_connections_are_reused_and_a_gzip_body_is_decoded(server):
    server.reply = lambda handler: (200, {"Content-Encoding": "gzip"}, gzip.compress(json.dumps({"zipped": True}).encode()))
    transport = transport_for(server)
    for _ in range(3):
        assert transport.ingest({"rows": []}).result(5) == {"zipped": True}
    # Three requests from the same thread share one persistent connection
    assert len(server.connections) == 1


def test_non_2xx_responses_become_transport_errors_with_the_status_and_retry_after(server):
    server.reply = lambda handler: (429, {"Retry-After": "7"}, b"slow down")
    transport = transport_for(server)
    with pytest.raises(TransportError) as raised:
        transport.ingest({}).result(5)
    assert raised.value.status == 429
    assert raised.value.retry_after_ms == 7_000


def test_a_kept_alive_connection_the_server_closed_is_replaced_once():
    # The server closes the connection after the first request without saying so in the response (common with keep-alive timeouts): the second request is resent on a new connection
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    port = listener.getsockname()[1]
    served = []

    def serve():
        for _ in range(2):
            connection, _ = listener.accept()
            data = b""
            while b"\r\n\r\n" not in data:
                data += connection.recv(65536)
            served.append(data.split(b"\r\n", 1)[0])
            body = b'{"n": %d}' % len(served)
            connection.sendall(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s" % (len(body), body))
            connection.close()

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    transport = HttpTransport("urk_test", "inst-1", f"http://127.0.0.1:{port}", pool=WorkerPool(max_workers=1))
    assert transport.sync(None).result(5) == {"n": 1}
    assert transport.sync(None).result(5) == {"n": 2}
    thread.join(5)
    listener.close()
    assert served == [b"GET /v1/sync HTTP/1.1", b"GET /v1/sync HTTP/1.1"]


def test_a_server_that_does_not_answer_in_time_is_a_transport_error():
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    transport = HttpTransport("urk_test", "inst-1", f"http://127.0.0.1:{listener.getsockname()[1]}", pool=WorkerPool(max_workers=1))
    with pytest.raises(TransportError) as raised:
        transport.verify_key({"key_id": "k", "hash": "h"}, 50).result(5)
    assert raised.value.status is None
    listener.close()


def test_retry_after_is_seconds_or_an_http_date_and_config_errors_pause():
    now = datetime(2026, 10, 20, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1000
    assert parse_retry_after("3", now) == 3_000
    assert parse_retry_after("Tue, 20 Oct 2026 12:00:30 GMT", now) == 30_000
    assert parse_retry_after("soon", now) is None
    assert parse_retry_after("inf", now) is None
    assert parse_retry_after(None, now) is None
    assert [is_pausing_status(status) for status in (401, 403, 404, 409, 410)] == [True] * 5
    assert [is_pausing_status(status) for status in (None, 400, 408, 413, 429, 500, 503)] == [False] * 7


def test_a_base_url_that_is_not_http_is_refused():
    with pytest.raises(ValueError):
        HttpTransport("urk_test", "inst-1", "ftp://api.example.com")


# ---- Found in review (round two): https proxies aren't used; a dropped lease request isn't resent ----
def test_an_https_proxy_is_refused_before_anything_is_sent_to_it(monkeypatch):
    # http.client would send CONNECT and the proxy credentials in the clear to an https:// proxy: better not to connect
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    listener.settimeout(0.3)
    port = listener.getsockname()[1]
    monkeypatch.setenv("HTTPS_PROXY", f"https://user:secret@127.0.0.1:{port}")
    monkeypatch.delenv("NO_PROXY", raising=False)
    monkeypatch.delenv("no_proxy", raising=False)
    transport = HttpTransport("urk_test", "inst-1", "https://api.usagerail.test", pool=WorkerPool(max_workers=1))
    with pytest.raises(TransportError, match="only http:// proxies"):
        transport.sync(None).result(5)
    with pytest.raises(socket.timeout):
        listener.accept()
    listener.close()


def test_a_lease_request_is_not_sent_again_when_the_connection_drops_before_the_answer():
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    port = listener.getsockname()[1]
    seen = []

    def read_request(connection):
        data = b""
        while b"\r\n\r\n" not in data:
            chunk = connection.recv(65536)
            if not chunk:
                break
            data += chunk
        return data.split(b"\r\n", 1)[0]

    def serve():
        connection, _ = listener.accept()
        seen.append(read_request(connection))
        body = b'{"ok": true}'
        connection.sendall(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s" % (len(body), body))
        # The second request (a lease request): read it and drop the connection without answering. The server may already have issued the lease
        seen.append(read_request(connection))
        connection.close()
        listener.settimeout(0.5)
        try:
            again, _ = listener.accept()
            seen.append(read_request(again))
            again.close()
        except socket.timeout:
            pass

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    transport = HttpTransport("urk_test", "inst-1", f"http://127.0.0.1:{port}", pool=WorkerPool(max_workers=1))
    assert transport.sync(None).result(5) == {"ok": True}
    with pytest.raises(TransportError):
        transport.leases({"requests": []}).result(5)
    thread.join(5)
    listener.close()
    assert seen == [b"GET /v1/sync HTTP/1.1", b"POST /v1/leases HTTP/1.1"]
