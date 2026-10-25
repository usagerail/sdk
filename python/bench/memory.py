"""How much memory the SDK uses (target: under 50 MB at a typical scale of up to 10,000 keys and 10,000 customers; under 100 MB fully loaded at 100,000 each).
Measures the Python objects the engine keeps with tracemalloc; the sync response itself is discarded after use and not counted. Two scenarios:
- after sync: the key snapshot and consumer records;
- all active: after sync, each customer makes one request with their key, so quota state, leases, rate limit buckets and the metering table all exist.
Usage: uv run python bench/memory.py [number of keys and customers, default 100000]
"""

from __future__ import annotations

import gc
import os
import time
import hashlib
import platform
import sys
import tracemalloc
from concurrent.futures import Future
from datetime import datetime, timedelta, timezone

from usagerail import Pending, create_engine
from usagerail.key_format import generate_api_key
from usagerail.snapshot import FileSnapshotStore

KEYS = 100_000
CONSUMERS = 100_000


def done(value):
    future: Future = Future()
    future.set_result(value)
    return future


def iso(moment: datetime) -> str:
    return moment.isoformat().replace("+00:00", "Z")


class FullSyncTransport:
    """One full sync page: one key per customer, external IDs are 64 hex characters (as with hashing on), hard limits, 10 a second and 300 a minute.
    Every lease request gets 100 units. Raw keys are generated before measuring starts and aren't counted against the engine"""

    def __init__(self, keys: int, consumers: int) -> None:
        self.raw_keys = [generate_api_key("acme", "live") for _ in range(keys)]
        self.consumers = consumers
        self.leases_granted = 0

    def sync(self, cursor, timeout_ms=None):
        records = [
            {
                "key_id": key.key_id,
                "hash": hashlib.sha256(key.key.encode()).hexdigest(),
                "consumer": f"cons_{index:016d}",
                "status": "active",
                "expires_at": None,
                "quota_behavior": "stop",
                "quota_version": 3,
            }
            for index, key in enumerate(self.raw_keys)
        ]
        consumers = [
            {
                "consumer": f"cons_{index:016d}",
                "external_id": hashlib.sha256(f"user-{index}".encode()).hexdigest(),
                "status": "active",
                "quota_behavior": "stop",
                "quota_version": 3,
                "rate_limits": {"per_second": 10, "per_minute": 300},
            }
            for index in range(self.consumers)
        ]
        app = {
            "app_id": "app_bench",
            "mode": "live",
            "integration": "sdk",
            "hash_key": None,
            "rate_limit_mode": "per_instance",
            "on_key_store_unavailable": "deny",
            "stale_key_snapshot": "allow",
            "on_quota_unavailable": "allow",
            "unit_mode": "requests",
            "routes": [],
        }
        now = iso(datetime.now(timezone.utc))
        return done({"cursor": "c", "full": True, "has_more": False, "keys": records, "removed_keys": [], "consumers": consumers, "app": app, "active_instances": 1, "server_time": now})

    def ingest(self, request, timeout_ms=None):
        return done({"duplicate": False, "accepted_rows": 0, "rejected_rows": [], "unknown_keys": [], "unknown_leases": []})

    def verify_key(self, request, timeout_ms=None):
        return done({"valid": False, "reason": "not_found"})

    def leases(self, request, timeout_ms=None):
        period_end = iso(datetime.now(timezone.utc) + timedelta(hours=1))
        results = []
        for item in request["requests"]:
            self.leases_granted += 1
            lease = {"lease_id": str(self.leases_granted), "consumer": item["consumer"], "granted": 100, "quota_version": 3, "behavior": "stop", "period_end": period_end}
            results.append({"consumer": item["consumer"], "status": "granted", "lease": lease})
        return done({"results": results})


def measure(keys: int = KEYS, consumers: int = CONSUMERS, active: bool = False, snapshot_dir: str | None = None) -> int:
    """Bytes the engine adds. active: each customer also made one request with their key. snapshot_dir: save the snapshot here, written by a real background thread"""
    transport = FullSyncTransport(keys, consumers)
    gc.collect()
    tracemalloc.start()
    baseline = tracemalloc.get_traced_memory()[0]
    store = FileSnapshotStore("urk_live_bench", snapshot_dir) if snapshot_dir else None
    engine = create_engine(app_key="urk_live_bench", auth="api-key", transport=transport, background_loops=False, snapshot_store=store)
    engine.sync().result()
    if store is not None:
        # Save to disk right after sync: wait for the background thread to finish writing and go idle, then measure
        deadline = time.monotonic() + 30
        while not os.path.exists(store.file) and time.monotonic() < deadline:
            time.sleep(0.01)
        time.sleep(0.2)
    if active:
        for key in transport.raw_keys:
            decision = engine.begin(method="GET", path="/v1/items", api_key=key.key)
            if isinstance(decision, Pending):
                decision = decision.result()
            if decision.allowed:
                engine.end(decision.handle, status=200, route="/v1/items")
    gc.collect()
    used = tracemalloc.get_traced_memory()[0] - baseline
    tracemalloc.stop()
    engine.close().result()
    return used


if __name__ == "__main__":
    count = int(sys.argv[1]) if len(sys.argv) > 1 else KEYS
    synced = measure(count, count)
    active = measure(count, count, active=True)
    print(f"Python {platform.python_version()} on {platform.machine()}, {count:,} keys and customers:")
    print(f"  synced: {synced / 1024 / 1024:.1f} MiB")
    print(f"  every customer active: {active / 1024 / 1024:.1f} MiB")
