"""The four endpoints between the SDK and the server. The engine depends only on the Transport interface, and every method returns a Future.

The default implementation uses only the standard library: http.client plus a set of background threads. Key verification and lease requests have a hard cap of 300 ms,
and a fresh TLS handshake each time would eat most of that, so each thread keeps its own persistent connection.
"""

from __future__ import annotations

import base64
import email.utils
import gzip
import http.client
import json
import math
import os
import queue
import ssl
import threading
import time
import urllib.parse
import urllib.request
from concurrent.futures import Future
from typing import Any, Callable, Protocol, TypeVar

T = TypeVar("T")

DEFAULT_BASE_URL = "https://api.usagerail.com"

# Key verification and lease requests hold up the caller's request, so they are capped at 300 ms. Sync and ingest run in the background and get plenty of time
DEFAULT_TIMEOUTS = {"verify_key": 300, "leases": 300, "sync": 10_000, "ingest": 10_000}

SDK_VERSION = "0.1.1"
USER_AGENT = f"usagerail-python/{SDK_VERSION}"


class Transport(Protocol):
    def ingest(self, request: dict[str, Any], timeout_ms: float | None = None) -> Future[dict[str, Any]]: ...
    def sync(self, cursor: str | None, timeout_ms: float | None = None) -> Future[dict[str, Any]]: ...
    def verify_key(self, request: dict[str, Any], timeout_ms: float | None = None) -> Future[dict[str, Any]]: ...
    def leases(self, request: dict[str, Any], timeout_ms: float | None = None) -> Future[dict[str, Any]]: ...


class TransportError(Exception):
    """Raised for non-2xx responses, timeouts, network errors and unreadable bodies. status is None when there was no usable response; treated as a temporary failure"""

    def __init__(self, message: str, status: int | None, retry_after_ms: float | None = None) -> None:
        super().__init__(message)
        self.status = status
        self.retry_after_ms = retry_after_ms


def is_pausing_status(status: int | None) -> bool:
    """Configuration errors (revoked app key 401, deleted app 410, wrong integration mode 409, wrong URL 404, etc.): retrying won't help.
    Ingest and sync both pause and try again every 10 minutes. 400 (a problem with this batch), 413 (split the batch), 408 and 429 (temporary) don't count"""
    return status is not None and 400 <= status < 500 and status not in (400, 408, 413, 429)


def parse_retry_after(value: str | None, now_ms: float | None = None) -> float | None:
    """Retry-After can be a number of seconds or an HTTP date (RFC 9110). Returns milliseconds"""
    if not value:
        return None
    try:
        seconds = float(value)
    except ValueError:
        seconds = None
    if seconds is not None:
        # Values float() accepts that aren't seconds, like inf and nan, count as unreadable
        return seconds * 1000 if math.isfinite(seconds) and seconds >= 0 else None
    try:
        when = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    now = time.time() * 1000 if now_ms is None else now_ms
    return max(0.0, when.timestamp() * 1000 - now)


def describe_error(error: BaseException) -> str:
    return str(error) or type(error).__name__


class WorkerPool:
    """Daemon threads for blocking calls (HTTP, writing the snapshot file), started as needed, at most max_workers.
    Not ThreadPoolExecutor: at interpreter exit it shuts down before atexit runs and refuses new work, so the final report before exit couldn't be sent"""

    def __init__(self, max_workers: int = 16, name: str = "usagerail-io") -> None:
        self._max_workers = max_workers
        self._name = name
        self._reset()
        # Windows has no fork and no such function
        if hasattr(os, "register_at_fork"):
            os.register_at_fork(after_in_child=self._reset)

    def _reset(self) -> None:
        self._lock = threading.Lock()
        self._queue: queue.SimpleQueue[tuple[Callable[[], Any], Future[Any]]] = queue.SimpleQueue()
        self._threads = 0
        self._idle = 0
        self._pending = 0

    def submit(self, call: Callable[[], T]) -> Future[T]:
        future: Future[T] = Future()
        with self._lock:
            self._pending += 1
            # More jobs queued than idle threads: start another
            if self._pending > self._idle and self._threads < self._max_workers:
                self._threads += 1
                threading.Thread(target=self._work, name=f"{self._name}-{self._threads}", daemon=True).start()
        self._queue.put((call, future))
        return future

    def _work(self) -> None:
        while True:
            with self._lock:
                self._idle += 1
            job = self._queue.get()
            with self._lock:
                self._idle -= 1
                self._pending -= 1
            _run(*job)
            # Drop this job before waiting for the next: if the job and its result (a whole key snapshot, say) stayed in local variables,
            # idle threads would keep holding them, and over ten MB could never be freed
            del job


def _run(call: Callable[[], Any], future: Future[Any]) -> None:
    if not future.set_running_or_notify_cancel():
        return
    try:
        result = call()
    except BaseException as error:  # noqa: BLE001 - handed to whoever awaits the result
        future.set_exception(error)
    else:
        future.set_result(result)


class HttpTransport:
    def __init__(
        self,
        app_key: str,
        instance_id: str,
        base_url: str = DEFAULT_BASE_URL,
        timeouts: dict[str, float] | None = None,
        pool: WorkerPool | None = None,
    ) -> None:
        parsed = urllib.parse.urlsplit(base_url.rstrip("/"))
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            raise ValueError(f"UsageRail: base_url must be an http(s) URL, got {base_url!r}")
        self._scheme = parsed.scheme
        self._host = parsed.hostname
        self._port = parsed.port or (443 if parsed.scheme == "https" else 80)
        self._base_path = parsed.path
        self._timeouts = {**DEFAULT_TIMEOUTS, **(timeouts or {})}
        self._headers = {
            "Authorization": f"Bearer {app_key}",
            "X-Instance-Id": instance_id,
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
        }
        self._pool = pool or WorkerPool()
        self._local = threading.local()
        self._ssl_context = _ssl_context() if parsed.scheme == "https" else None

    def ingest(self, request: dict[str, Any], timeout_ms: float | None = None) -> Future[dict[str, Any]]:
        return self._submit("POST", "/v1/ingest", request, timeout_ms or self._timeouts["ingest"])

    def sync(self, cursor: str | None, timeout_ms: float | None = None) -> Future[dict[str, Any]]:
        path = "/v1/sync" if cursor is None else f"/v1/sync?cursor={urllib.parse.quote(cursor, safe='')}"
        return self._submit("GET", path, None, timeout_ms or self._timeouts["sync"])

    def verify_key(self, request: dict[str, Any], timeout_ms: float | None = None) -> Future[dict[str, Any]]:
        return self._submit("POST", "/v1/keys/verify", request, timeout_ms or self._timeouts["verify_key"])

    def leases(self, request: dict[str, Any], timeout_ms: float | None = None) -> Future[dict[str, Any]]:
        # Lease requests must not be replayed: the server may have issued the lease and only the response was lost, so sending again would get two
        return self._submit("POST", "/v1/leases", request, timeout_ms or self._timeouts["leases"], replayable=False)

    def _submit(self, method: str, path: str, body: Any, timeout_ms: float, replayable: bool = True) -> Future[dict[str, Any]]:
        # The request body is serialized on the network thread: the caller holds the engine lock at this point, and a 5,000-row batch takes around ten ms to serialize.
        # The engine never changes a body after handing it over (batches and lease requests are always new objects)
        return self._pool.submit(lambda: self._call(method, path, body, timeout_ms, replayable))

    def _call(self, method: str, path: str, body: Any, timeout_ms: float, replayable: bool = True) -> dict[str, Any]:
        deadline = time.monotonic() + timeout_ms / 1000
        payload = None if body is None else json.dumps(body, separators=(",", ":")).encode()
        headers = dict(self._headers)
        if payload is not None:
            headers["Content-Type"] = "application/json"
        target = self._base_path + path
        for attempt in (1, 2):
            connection, reused = self._connection()
            try:
                connection.timeout = max(0.001, deadline - time.monotonic())
                if connection.sock is not None:
                    connection.sock.settimeout(connection.timeout)
                connection.request(method, target, body=payload, headers=headers)
                response = connection.getresponse()
                raw = response.read()
            except (http.client.RemoteDisconnected, ConnectionResetError, BrokenPipeError) as error:
                self._drop_connection()
                # The reused connection may have been closed by the server: try once more on a new connection. Only for requests that are safe to replay
                # (sync, ingest deduplicated by batch ID, read-only key verification): if it broke while reading the response, the server may already have processed it
                if replayable and reused and attempt == 1 and time.monotonic() < deadline:
                    continue
                raise TransportError(f"{method} {path} failed: {describe_error(error)}", None) from error
            except (OSError, http.client.HTTPException) as error:
                self._drop_connection()
                raise TransportError(f"{method} {path} failed: {describe_error(error)}", None) from error
            if response.will_close:
                self._drop_connection()
            if not 200 <= response.status < 300:
                retry_after = parse_retry_after(response.getheader("Retry-After"))
                raise TransportError(f"{method} {path} returned {response.status}", response.status, retry_after)
            try:
                if response.getheader("Content-Encoding", "").lower() == "gzip":
                    raw = gzip.decompress(raw)
                parsed = json.loads(raw)
            except (OSError, ValueError) as error:
                raise TransportError(f"{method} {path} returned an unreadable body: {describe_error(error)}", None) from error
            if not isinstance(parsed, dict):
                raise TransportError(f"{method} {path} returned an unexpected body", None)
            return parsed
        raise TransportError(f"{method} {path} failed", None)  # pragma: no cover - the loop above always returns or raises

    def _connection(self) -> tuple[http.client.HTTPConnection, bool]:
        existing = getattr(self._local, "connection", None)
        if existing is not None:
            return existing, True
        connection = self._open()
        self._local.connection = connection
        return connection, False

    def _open(self) -> http.client.HTTPConnection:
        if self._scheme == "http":
            # Plain http is only for local development (http://localhost:8788 and the like), never through a proxy
            return http.client.HTTPConnection(self._host, self._port)
        proxy = _https_proxy_for(self._host)
        if proxy is None:
            return http.client.HTTPSConnection(self._host, self._port, context=self._ssl_context)
        if proxy.scheme not in ("http", ""):
            # An https:// proxy needs TLS to the proxy first, which http.client can't do: it would send CONNECT and the proxy credentials in the clear. Better not to connect
            raise TransportError(f"only http:// proxies are supported (HTTPS_PROXY is {proxy.scheme}://...)", None)
        connection = http.client.HTTPSConnection(proxy.hostname or "", proxy.port or 80, context=self._ssl_context)
        connection.set_tunnel(self._host, self._port, headers=_proxy_headers(proxy))
        return connection

    def _drop_connection(self) -> None:
        connection = getattr(self._local, "connection", None)
        self._local.connection = None
        if connection is not None:
            connection.close()


def _ssl_context() -> ssl.SSLContext:
    """The system's root certificates, plus certifi's if it's installed. Python from uv or python.org on macOS often can't read the system certificates
    and fails every connection with CERTIFICATE_VERIFY_FAILED. certifi isn't a dependency: production (Linux containers) usually doesn't need it"""
    context = ssl.create_default_context()
    try:
        import certifi  # type: ignore[import-not-found]
    except ImportError:
        return context
    context.load_verify_locations(certifi.where())
    return context


def _https_proxy_for(host: str) -> urllib.parse.SplitResult | None:
    """Goes through a proxy according to HTTPS_PROXY / NO_PROXY (same rules as urllib)"""
    proxy = urllib.request.getproxies().get("https")
    if not proxy or urllib.request.proxy_bypass(host):
        return None
    parsed = urllib.parse.urlsplit(proxy if "://" in proxy else f"http://{proxy}")
    return parsed if parsed.hostname else None


def _proxy_headers(proxy: urllib.parse.SplitResult) -> dict[str, str]:
    if not proxy.username:
        return {}
    credentials = f"{urllib.parse.unquote(proxy.username)}:{urllib.parse.unquote(proxy.password or '')}"
    return {"Proxy-Authorization": "Basic " + base64.b64encode(credentials.encode()).decode()}
