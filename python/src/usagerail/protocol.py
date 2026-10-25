"""The wire protocol between the SDK and the server.

The Node SDK's protocol.ts is the source of truth for the wire format (the server uses it too). This file only holds the constants the Python SDK needs,
plus a few TypedDicts to make the fields easy to see. Fields on the wire are snake_case.
Every endpoint: Authorization: Bearer <appKey>, plus X-Instance-Id: <instance ID>.
"""

from __future__ import annotations

from typing import Literal, TypedDict

# Outcomes. Only 2xx, 3xx, ws_open and check_allowed are billable; the rest go to analytics only.
# sdk_403 is a suspended consumer
Outcome = Literal["2xx", "3xx", "4xx", "5xx", "aborted", "ws_open", "sdk_401", "sdk_403", "sdk_429", "sdk_402", "check_allowed"]
BILLABLE_OUTCOMES: frozenset[str] = frozenset({"2xx", "3xx", "ws_open", "check_allowed"})

QuotaBehavior = Literal["overage", "stop", "prepaid"]
# What a consumer reference is: a consumer ID when you use our keys, an external ID when you use your own auth
ConsumerKind = Literal["id", "external"]

# Per-row caps: a row over either is rejected whole. The SDK starts a new row before reaching them; the server merges rows with the same key on insert
MAX_ROW_COUNT = 10_000_000
MAX_ROW_UNITS = 10_000_000_000


class RouteSetting(TypedDict):
    method: str
    route: str
    weight: int
    reserve: int


class RateLimits(TypedDict):
    per_second: int | None
    per_minute: int | None


class KeyRecord(TypedDict):
    key_id: str
    # Hex SHA-256 of the full key; the SDK compares it in constant time
    hash: str
    consumer: str
    status: Literal["active", "revoked"]
    # ISO 8601; the server already subtracts a 30-second margin
    expires_at: str | None
    quota_behavior: QuotaBehavior | None
    quota_version: int


class ConsumerRecord(TypedDict):
    consumer: str
    # With your own auth, the external ID the SDK reported (HMAC'd hex if hashing is on); None with our keys
    external_id: str | None
    # suspended: the seller suspended the consumer, or it's being deleted. The SDK blocks all its requests with 403 consumer_suspended
    status: Literal["active", "suspended"]
    quota_behavior: QuotaBehavior | None
    quota_version: int
    rate_limits: RateLimits


class AppSettings(TypedDict):
    app_id: str
    mode: Literal["test", "live"]
    integration: Literal["sdk", "http"]
    # With your own auth, external IDs are HMAC-SHA256'd with this key by default; None when hashing is off
    hash_key: str | None
    rate_limit_mode: Literal["per_instance", "divided"]
    on_key_store_unavailable: Literal["deny", "allow"]
    stale_key_snapshot: Literal["allow", "deny"]
    on_quota_unavailable: Literal["allow", "deny"]
    unit_mode: Literal["requests", "units"]
    routes: list[RouteSetting]
