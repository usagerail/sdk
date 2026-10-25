"""Time the SDK adds to each request (target: p99 < 1 ms on Python 3.12 with 2 vCPUs). Caches are warm, no network:
measures begin() + end() alone, i.e. local key check, rate limit, quota reservation and metering aggregation. The same three scenarios as the Node SDK's bench/overhead.ts.
Usage: uv run python bench/overhead.py
"""

from __future__ import annotations

import hashlib
import platform
import sys
import time
from concurrent.futures import Future
from datetime import datetime, timezone

from usagerail import Pending, create_engine
from usagerail._time import iso_from_ms
from usagerail.key_format import generate_api_key

ITERATIONS = 200_000
WARMUP = 20_000


def done(value):
    future: Future = Future()
    future.set_result(value)
    return future


class BenchTransport:
    def __init__(self, sync):
        self._sync = sync

    def sync(self, cursor, timeout_ms=None):
        return done(self._sync)

    def ingest(self, request, timeout_ms=None):
        return done({"duplicate": False, "accepted_rows": 0, "rejected_rows": [], "unknown_keys": [], "unknown_leases": []})

    def verify_key(self, request, timeout_ms=None):
        return done({"valid": False, "reason": "not_found"})

    def leases(self, request, timeout_ms=None):
        period_end = iso_from_ms(time.time() * 1000 + 3_600_000)
        return done(
            {
                "results": [
                    {
                        "consumer": item["consumer"],
                        "status": "granted",
                        "lease": {"lease_id": "1", "consumer": item["consumer"], "granted": 1_000_000_000, "quota_version": 1, "behavior": "stop", "period_end": period_end},
                    }
                    for item in request["requests"]
                ]
            }
        )


def sync_response(key, behavior, hash_key=None):
    return {
        "cursor": "c",
        "full": True,
        "has_more": False,
        "app": {
            "app_id": "app_bench",
            "mode": "test",
            "integration": "sdk",
            "hash_key": hash_key,
            "rate_limit_mode": "per_instance",
            "on_key_store_unavailable": "deny",
            "stale_key_snapshot": "allow",
            "on_quota_unavailable": "allow",
            "unit_mode": "requests",
            "routes": [
                {"method": "POST", "route": "/v1/images", "weight": 10, "reserve": 10},
                {"method": "GET", "route": "/v1/users/{user_id}", "weight": 1, "reserve": 1},
            ],
        },
        "keys": [{"key_id": key.key_id, "hash": hashlib.sha256(key.key.encode()).hexdigest(), "consumer": "cons_bench", "status": "active", "expires_at": None, "quota_behavior": behavior, "quota_version": 1}],
        "removed_keys": [],
        "consumers": [
            {
                "consumer": "cons_bench",
                "external_id": hashlib.sha256(b"unused").hexdigest() if hash_key else None,
                "status": "active",
                "quota_behavior": behavior,
                "quota_version": 1,
                "rate_limits": {"per_second": 1_000_000, "per_minute": 60_000_000},
            }
        ],
        "active_instances": 1,
        "server_time": datetime.now(timezone.utc).isoformat(),
    }


class Quiet:
    def warning(self, message):
        pass


def measure(name, engine, **request):
    # The first call may wait for a lease, so run it once first
    first = engine.begin(**request)
    first = first.result() if isinstance(first, Pending) else first
    if first.allowed:
        engine.end(first.handle, status=200, route="/v1/users/{user_id}")
    samples = []
    clock = time.perf_counter
    for index in range(WARMUP + ITERATIONS):
        started = clock()
        decision = engine.begin(**request)
        if isinstance(decision, Pending):
            raise RuntimeError(f"{name}: begin() had to wait for the network")
        if decision.allowed:
            engine.end(decision.handle, status=200, route="/v1/users/{user_id}")
        elapsed = clock() - started
        if index >= WARMUP:
            samples.append(elapsed)
    samples.sort()

    def at(fraction):
        return samples[min(len(samples) - 1, int(len(samples) * fraction))] * 1_000_000

    print(f"{name:<44} p50 {at(0.5):6.1f} µs   p99 {at(0.99):6.1f} µs   p99.9 {at(0.999):6.1f} µs")
    engine.close().result(5)


def main():
    key = generate_api_key("bench", "test")
    base = {"app_key": "urk_bench", "background_loops": False, "flush_on_exit": False, "logger": Quiet()}
    print(f"Python {platform.python_version()} ({sys.implementation.name}), {platform.processor() or platform.machine()}, {ITERATIONS:,} requests each")

    overage = create_engine(**base, auth="api-key", transport=BenchTransport(sync_response(key, "overage")))
    overage.sync()
    measure("our key, overage plan, rate limited", overage, method="GET", path="/v1/users/42", api_key=key.key)

    stop = create_engine(**base, auth="api-key", transport=BenchTransport(sync_response(key, "stop")))
    stop.sync()
    measure("our key, stop-at-limit (lease reservation)", stop, method="GET", path="/v1/users/42", api_key=key.key)

    custom = create_engine(**base, auth="custom", consumer=lambda context: context["user"], transport=BenchTransport(sync_response(key, "overage", "hash-key")))
    custom.sync()
    measure("own auth, hashed external ID", custom, method="GET", path="/v1/users/42", context={"user": "user-42"})


if __name__ == "__main__":
    main()
