"""Fake clock and fake transport for tests: no network, no real waiting. Correspond to the Node SDK's test/helpers.ts.

Fake transport handlers can return a result directly (the Future completes on the spot and callbacks run immediately), raise (the Future fails),
or return an unfinished Future and let the test decide when to release it.
"""

from __future__ import annotations

import copy
import hashlib
from concurrent.futures import Future
from datetime import datetime, timezone
from typing import Any, Callable

from usagerail._time import iso_from_ms


def sha256(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


class FakeClock:
    def __init__(self, wall: float | None = None) -> None:
        # Starts 10 seconds into a whole minute by default; period boundary tests set their own
        self.wall = wall if wall is not None else datetime(2026, 10, 20, 12, 0, 10, tzinfo=timezone.utc).timestamp() * 1000
        self.mono = 1_000_000.0
        self._timers: list[tuple[float, int, Callable[[], None]]] = []
        self._order = 0

    def now(self) -> float:
        return self.wall

    def monotonic(self) -> float:
        return self.mono

    def set_timer(self, callback: Callable[[], None], delay_ms: float) -> Callable[[], None]:
        timer = (self.mono + max(0.0, delay_ms), self._order, callback)
        self._order += 1
        self._timers.append(timer)

        def cancel() -> None:
            if timer in self._timers:
                self._timers.remove(timer)

        return cancel

    def advance(self, ms: float) -> None:
        """Move forward ms milliseconds, firing timers that come due along the way in order"""
        target = self.mono + ms
        while True:
            due = sorted((timer for timer in self._timers if timer[0] <= target), key=lambda timer: (timer[0], timer[1]))
            if not due:
                break
            timer = due[0]
            self._timers.remove(timer)
            self.wall += timer[0] - self.mono
            self.mono = timer[0]
            timer[2]()
        self.wall += target - self.mono
        self.mono = target

    def pending_timers(self) -> int:
        return len(self._timers)


Handler = Callable[..., Any]


def _respond(handler: Handler, *arguments: Any) -> Future[Any]:
    try:
        result = handler(*arguments)
    except Exception as error:  # noqa: BLE001
        failed: Future[Any] = Future()
        failed.set_exception(error)
        return failed
    if isinstance(result, Future):
        return result
    done: Future[Any] = Future()
    done.set_result(result)
    return done


class FakeTransport:
    def __init__(self) -> None:
        self.ingested: list[dict[str, Any]] = []
        self.lease_calls: list[dict[str, Any]] = []
        self.verify_calls: list[dict[str, Any]] = []
        self.sync_calls: list[str | None] = []
        self.timeouts: list[float | None] = []
        self.on_ingest: Handler = lambda request: accepted()
        self.on_sync: Handler = lambda cursor: sync_page()
        self.on_verify: Handler = lambda request: {"valid": False, "reason": "not_found"}
        self.on_leases: Handler = lambda request: {
            "results": [{"consumer": item["consumer"], "status": "unlimited", "quota_version": item["quota_version"]} for item in request["requests"]]
        }

    def ingest(self, request: dict[str, Any], timeout_ms: float | None = None) -> Future[dict[str, Any]]:
        # Store a deep copy, so later changes by the engine don't affect what the test inspects
        self.ingested.append(copy.deepcopy(request))
        return _respond(self.on_ingest, request)

    def sync(self, cursor: str | None, timeout_ms: float | None = None) -> Future[dict[str, Any]]:
        self.sync_calls.append(cursor)
        return _respond(self.on_sync, cursor)

    def verify_key(self, request: dict[str, Any], timeout_ms: float | None = None) -> Future[dict[str, Any]]:
        self.verify_calls.append(request)
        self.timeouts.append(timeout_ms)
        return _respond(self.on_verify, request)

    def leases(self, request: dict[str, Any], timeout_ms: float | None = None) -> Future[dict[str, Any]]:
        self.lease_calls.append(copy.deepcopy(request))
        self.timeouts.append(timeout_ms)
        return _respond(self.on_leases, request)

    def rows(self) -> list[dict[str, Any]]:
        return [row for batch in self.ingested for row in batch["rows"]]


def accepted() -> dict[str, Any]:
    return {"duplicate": False, "accepted_rows": 0, "rejected_rows": [], "unknown_keys": [], "unknown_leases": []}


def app_settings(**overrides: Any) -> dict[str, Any]:
    return {
        "app_id": "app_1",
        "mode": "test",
        "integration": "sdk",
        "hash_key": None,
        "rate_limit_mode": "per_instance",
        "on_key_store_unavailable": "deny",
        "stale_key_snapshot": "allow",
        "on_quota_unavailable": "allow",
        "unit_mode": "requests",
        "routes": [],
        **overrides,
    }


def sync_page(**overrides: Any) -> dict[str, Any]:
    return {
        "cursor": "cursor-1",
        "full": True,
        "has_more": False,
        "app": app_settings(),
        "keys": [],
        "removed_keys": [],
        "consumers": [],
        "active_instances": 1,
        "server_time": iso_from_ms(datetime.now(timezone.utc).timestamp() * 1000),
        **overrides,
    }


def consumer_record(**overrides: Any) -> dict[str, Any]:
    return {
        "consumer": "cons_1",
        "external_id": None,
        "status": "active",
        "quota_behavior": "overage",
        "quota_version": 1,
        "rate_limits": {"per_second": None, "per_minute": None},
        **overrides,
    }


def key_record(key_id: str, hash: str, **overrides: Any) -> dict[str, Any]:  # noqa: A002 - same name as the protocol field
    return {
        "key_id": key_id,
        "hash": hash,
        "consumer": "cons_1",
        "status": "active",
        "expires_at": None,
        "quota_behavior": "overage",
        "quota_version": 1,
        **overrides,
    }


class SilentLogger:
    def warning(self, message: str) -> None:
        pass


class CollectingLogger:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def warning(self, message: str) -> None:
        self.messages.append(message)
