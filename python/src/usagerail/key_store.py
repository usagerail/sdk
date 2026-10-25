"""API key snapshot and local verification, same as key-store.ts in the Node SDK.

The hot path is local only: format and checksum → look up the key ID in the snapshot → constant-time hash comparison → status and expiry.
Only keys missing from the snapshot touch the network (remote verification, 300 ms at most), and those results are cached.
Memory holds key IDs and hashes only, never the keys themselves.
"""

from __future__ import annotations

import hashlib
import hmac
import math
import threading
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any, Callable, Literal, Protocol

from ._futures import error_of, settle
from ._time import iso_from_ms, parse_iso_ms
from .clock import Clock
from .key_format import CHECKSUM_LENGTH, checksum_for, parse_api_key
from .protocol import ConsumerRecord, KeyRecord, QuotaBehavior
from .transport import Transport, describe_error

# Freshness: synced within the last 10 minutes is fresh; older than 24 hours counts as no snapshot at all
FRESH_FOR_MS = 10 * 60_000
SNAPSHOT_MAX_AGE_MS = 24 * 60 * 60_000
# Failed remote verifications are cached by key ID for 60 seconds; each instance makes at most 20 remote verifications a second
NEGATIVE_CACHE_MS = 60_000
REMOTE_VERIFIES_PER_SECOND = 20
# Remote verification holds up the caller's request for at most 300 ms (a hard rule). HTTP library timeouts can't stop a response that trickles in slowly, so the timing is done here
VERIFY_TIMEOUT_MS = 300
# At most 100,000 keys in the snapshot, to keep memory bounded
MAX_SNAPSHOT_KEYS = 100_000
# Cap on cached remote verification results. Failures top out around 20 a second × 60 seconds; successes are cleared on every sync,
# so they only grow while sync keeps failing. The cap stops a long outage from eating memory
MAX_CACHED_VERIFICATIONS = 10_000
# For a few seconds after a remote verification fails, treat new keys as "unavailable" straight away: when the server is down, requests with new keys shouldn't each wait the full 300 ms
VERIFY_OUTAGE_MS = 5_000

Freshness = Literal["fresh", "stale", "none"]


@dataclass(frozen=True, slots=True)
class KeyCheck:
    result: Literal["valid", "invalid", "unavailable"]
    key_id: str | None = None
    # For invalid, only set when the hash matched but the key is revoked or expired: we know who it is, so sdk_401 can be recorded against them
    consumer: str | None = None
    quota_behavior: QuotaBehavior | None = None
    quota_version: int = 0
    # Consumer details from remote verification (rate limits, quota version); None on a snapshot hit, where the engine takes them from synced data
    consumer_record: ConsumerRecord | None = None


INVALID = KeyCheck("invalid")
UNAVAILABLE = KeyCheck("unavailable")


class SnapshotStore(Protocol):
    """The snapshot saved to disk (long-running processes write it to the local temp directory with mode 600, read it first on restart, and apply the same 24-hour limit).
    It leaves out app.hash_key: the external ID hashing key never touches disk and is fetched again on the first sync after a restart. save() blocks, so the engine calls it on a background thread"""

    def load(self) -> dict[str, Any] | None: ...
    def save(self, snapshot: dict[str, Any]) -> None: ...


@dataclass(slots=True)
class _StoredKey:
    # The 32 raw bytes of the SHA-256 rather than a 64-character hex string: saves nearly 5 MB for 100,000 keys
    hash: bytes
    consumer: str
    revoked: bool
    expires_at: float | None
    quota_behavior: QuotaBehavior | None
    quota_version: int
    # Used when a full sync spans several pages: keys absent from the new full sync are deleted once the last page arrives
    generation: int


class KeyStore:
    def __init__(self, clock: Clock, transport: Transport, warn: Callable[[str], None], lock: threading.RLock) -> None:
        self._clock = clock
        self._transport = transport
        self._warn = warn
        # The engine's lock: remote verification results come back on network threads and must hold it before changing state
        self.lock = lock
        self._keys: dict[str, _StoredKey] = {}
        self._generation = 0
        self._sweep_pending = False
        # Monotonic time of the last complete successful sync; None means there's no snapshot yet
        self._synced_at: float | None = None
        # Successful remote verifications, cached until the next sync
        self._verified: dict[str, tuple[_StoredKey, ConsumerRecord, float]] = {}
        # Number of completed syncs. If a sync completed after a remote verification was sent, its answer may be older than the snapshot (the key may have been revoked or deleted meanwhile), so it isn't cached
        self._syncs_completed = 0
        # Failed remote verifications, cached for 60 seconds by "key ID + hash". Key IDs aren't secret; caching by ID alone would let
        # anyone pair a real key's ID with a fake secret and get a freshly created real key rejected for 60 seconds
        self._rejected: dict[str, float] = {}
        # Concurrent requests with the same new key share one remote verification; otherwise a burst would use up the 20-a-second budget and valid keys after it would get 401s
        self._inflight: dict[str, Future[KeyCheck]] = {}
        self._budget_second = -1
        self._budget_used = 0
        self._outage_until = -math.inf
        self._capacity_warned = False

    def freshness(self) -> Freshness:
        if self._synced_at is None:
            return "none"
        age = self._clock.monotonic() - self._synced_at
        if age > SNAPSHOT_MAX_AGE_MS:
            return "none"
        return "fresh" if age < FRESH_FOR_MS else "stale"

    def check(self, raw_key: str | None, stale_key_snapshot: Literal["allow", "deny"]) -> KeyCheck | Future[KeyCheck]:
        """Returns a Future only when remote verification is needed"""
        # 1. Bad format or checksum: 401 straight away, no network
        if not isinstance(raw_key, str):
            return INVALID
        parsed = parse_api_key(raw_key)
        if parsed is None or checksum_for(raw_key[:-CHECKSUM_LENGTH]) != parsed.checksum:
            return INVALID
        key_id = parsed.key_id
        key_hash = hashlib.sha256(raw_key.encode()).digest()
        freshness = self.freshness()
        # 2–3. In the snapshot: compare hashes, then check status and expiry. Past 24 hours, or with no snapshot, it's as if there were none
        if freshness != "none":
            stored = self._keys.get(key_id)
            if stored is not None:
                return self._judge(key_id, stored, key_hash, None)
        verified = self._verified.get(key_id)
        if verified is not None and self._clock.monotonic() - verified[2] < SNAPSHOT_MAX_AGE_MS:
            return self._judge(key_id, verified[0], key_hash, verified[1])
        # Snapshot is stale and the seller chose deny: while disconnected, keys missing from the snapshot aren't accepted
        if freshness == "stale" and stale_key_snapshot == "deny":
            return INVALID
        return self._verify_remotely(key_id, key_hash)

    def _judge(self, key_id: str, stored: _StoredKey, key_hash: bytes, consumer_record: ConsumerRecord | None) -> KeyCheck:
        if not hmac.compare_digest(stored.hash, key_hash):
            return INVALID
        # 4. Status and expiry are judged by the local clock; expiry times from the server already include a 30-second margin
        if stored.revoked or (stored.expires_at is not None and stored.expires_at <= self._clock.now()):
            return KeyCheck("invalid", consumer=stored.consumer)
        return KeyCheck(
            "valid",
            key_id=key_id,
            consumer=stored.consumer,
            quota_behavior=stored.quota_behavior,
            quota_version=stored.quota_version,
            consumer_record=consumer_record,
        )

    def _verify_remotely(self, key_id: str, key_hash: bytes) -> KeyCheck | Future[KeyCheck]:
        now = self._clock.monotonic()
        cache_key = f"{key_id}:{key_hash.hex()}"
        rejected_until = self._rejected.get(cache_key)
        if rejected_until is not None:
            if now < rejected_until:
                return INVALID
            del self._rejected[cache_key]
        pending = self._inflight.get(cache_key)
        if pending is not None:
            return pending
        if now < self._outage_until:
            return UNAVAILABLE
        # Over 20 a second gets a 401 straight away, so random keys can't be used to hammer the server. Not while there's no usable snapshot yet:
        # just after startup, before the first sync completes (several seconds for a big app), every key is unknown, and real customers would be told their key is invalid.
        # In that case treat it as "can't reach the key store" (on_key_store_unavailable, 503 by default, which clients retry)
        if not self._take_verify_budget(now):
            return UNAVAILABLE if self.freshness() == "none" else INVALID

        result: Future[KeyCheck] = Future()
        # Register before sending: a fake transport answers on the spot, running the callback immediately
        self._inflight[cache_key] = result

        def finish(check: KeyCheck) -> None:
            if self._inflight.get(cache_key) is result:
                del self._inflight[cache_key]
            settle(result, check)

        def give_up(reason: str) -> None:
            self._outage_until = self._clock.monotonic() + VERIFY_OUTAGE_MS
            self._warn(f"UsageRail: remote key verification failed ({reason}), applying onKeyStoreUnavailable")
            finish(UNAVAILABLE)

        def on_timeout() -> None:
            with self.lock:
                if not result.done():
                    give_up(f"no answer within {VERIFY_TIMEOUT_MS} ms")

        syncs_at_send = self._syncs_completed
        call = self._transport.verify_key({"key_id": key_id, "hash": key_hash.hex()}, VERIFY_TIMEOUT_MS)
        cancel_timer = self._clock.set_timer(on_timeout, VERIFY_TIMEOUT_MS)

        def on_done(completed: Future[dict[str, Any]]) -> None:
            with self.lock:
                cancel_timer()
                error = error_of(completed)
                if error is not None:
                    if not result.done():
                        give_up(describe_error(error))
                    return
                try:
                    # A result that comes back after the timeout is still recorded, so the next request doesn't have to ask
                    check = self._apply_verification(key_id, key_hash, completed.result(), syncs_at_send)
                except (KeyError, TypeError, ValueError) as parse_error:
                    if not result.done():
                        give_up(f"unreadable answer: {describe_error(parse_error)}")
                    return
                finish(check)

        call.add_done_callback(on_done)
        return result

    def _apply_verification(self, key_id: str, key_hash: bytes, response: dict[str, Any], syncs_at_send: int) -> KeyCheck:
        now = self._clock.monotonic()
        if not response["valid"]:
            _remember(self._rejected, f"{key_id}:{key_hash.hex()}", now + NEGATIVE_CACHE_MS)
            return INVALID
        stored = _to_stored_key(response["key"], self._generation)
        consumer_record: ConsumerRecord = response["consumer"]
        if syncs_at_send != self._syncs_completed:
            # A sync completed after this was sent, so the answer may be older than the snapshot. Don't cache it; only return it to the requests waiting on it,
            # and if the snapshot now has this key, the snapshot wins. Caching it would let a revoked key through for as long as sync keeps failing
            current = self._keys.get(key_id) if self.freshness() != "none" else None
            if current is not None:
                return self._judge(key_id, current, key_hash, None)
            return self._judge(key_id, stored, key_hash, consumer_record)
        _remember(self._verified, key_id, (stored, consumer_record, now))
        return self._judge(key_id, stored, key_hash, consumer_record)

    def _take_verify_budget(self, now: float) -> bool:
        second = math.floor(now / 1000)
        if second != self._budget_second:
            self._budget_second = second
            self._budget_used = 0
        if self._budget_used >= REMOTE_VERIFIES_PER_SECOND:
            return False
        self._budget_used += 1
        return True

    # ---- Sync (the engine calls this page by page) ---------------------------------

    def apply_sync_page(self, full: bool, keys: list[KeyRecord], removed_keys: list[str]) -> None:
        # One full sync can span several pages, each with full = true: start a new generation only on the first, or the last page would delete every key from the earlier pages as stale
        if full and not self._sweep_pending:
            self._generation += 1
            self._sweep_pending = True
        for record in keys:
            self._store(record)
        for key_id in removed_keys:
            self._keys.pop(key_id, None)

    def complete_sync(self) -> None:
        """Last page (has_more = false): delete keys absent from the full sync, mark the snapshot fresh, discard cached remote successes"""
        if self._sweep_pending:
            for key_id in [key_id for key_id, stored in self._keys.items() if stored.generation < self._generation]:
                del self._keys[key_id]
            self._sweep_pending = False
        self._synced_at = self._clock.monotonic()
        self._verified.clear()
        self._syncs_completed += 1

    def load(self, records: list[KeyRecord], saved_at: float) -> None:
        """Loads the snapshot saved on disk at restart. Freshness is based on the snapshot's age, with the same 24-hour limit"""
        age = max(0.0, self._clock.now() - saved_at)
        if age > SNAPSHOT_MAX_AGE_MS:
            return
        for record in records:
            self._store(record)
        self._synced_at = self._clock.monotonic() - age

    def records(self) -> list[KeyRecord]:
        return [
            {
                "key_id": key_id,
                "hash": stored.hash.hex(),
                "consumer": stored.consumer,
                "status": "revoked" if stored.revoked else "active",
                "expires_at": None if stored.expires_at is None else iso_from_ms(stored.expires_at),
                "quota_behavior": stored.quota_behavior,
                "quota_version": stored.quota_version,
            }
            for key_id, stored in self._keys.items()
        ]

    def reset_after_fork(self) -> None:
        """In a forked child: the parent's in-flight remote verifications will never complete here, so drop them rather than have the same key wait on them forever"""
        self._inflight.clear()
        self._outage_until = -math.inf

    def _store(self, record: KeyRecord) -> None:
        if record["key_id"] not in self._keys and len(self._keys) >= MAX_SNAPSHOT_KEYS:
            # Keys over the cap go through remote verification: they still work, only the first request is a bit slower
            if not self._capacity_warned:
                self._warn(f"UsageRail: key snapshot is full ({MAX_SNAPSHOT_KEYS} keys); extra keys are verified remotely")
            self._capacity_warned = True
            return
        self._keys[record["key_id"]] = _to_stored_key(record, self._generation)


def _to_stored_key(record: KeyRecord, generation: int) -> _StoredKey:
    expires_at: float | None = None
    if record["expires_at"] is not None:
        # An unparseable expiry counts as already expired: better to reject than to keep a key that should have expired working
        expires_at = parse_iso_ms(record["expires_at"]) or 0.0
    return _StoredKey(
        hash=_hash_bytes(record["hash"]),
        consumer=record["consumer"],
        revoked=record["status"] != "active",
        expires_at=expires_at,
        quota_behavior=record["quota_behavior"],
        quota_version=record["quota_version"],
        generation=generation,
    )


def _hash_bytes(value: str) -> bytes:
    """Converts a hex hash to bytes; anything unparseable is stored empty and never matches (better to reject)"""
    try:
        return bytes.fromhex(value)
    except ValueError:
        return b""


def _remember(cache: dict[str, Any], key: str, value: Any) -> None:
    """dicts keep insertion order, so the oldest entry is removed when over the cap"""
    cache.pop(key, None)
    cache[key] = value
    if len(cache) > MAX_CACHED_VERIFICATIONS:
        del cache[next(iter(cache))]
