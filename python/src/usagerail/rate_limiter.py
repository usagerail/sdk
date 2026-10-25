"""Per-consumer token buckets. They live in SDK memory, one set per instance, with no network calls.

Per-second and per-minute limits can both be set; a request passes only if both buckets have a token. Same as rate-limiter.ts in the Node SDK.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Literal

from .clock import Clock
from .protocol import RateLimits

RateLimitMode = Literal["per_instance", "divided"]

# Caches state for at most 40,000 consumers, evicting the least recently used, same as quota state (quota.py) to keep memory bounded. An evicted bucket starts full next time
MAX_RATE_LIMITED_CONSUMERS = 40_000

_WINDOWS = (("per_second", 1), ("per_minute", 60))


@dataclass(slots=True)
class _Bucket:
    capacity: int
    tokens: float
    updated_at: float


@dataclass(frozen=True, slots=True)
class RateDecision:
    allowed: bool
    retry_after_seconds: int = 0
    headers: dict[str, str] = field(default_factory=dict)


_ALLOWED = RateDecision(True)


def per_instance_limit(limit: int, mode: RateLimitMode, active_instances: int) -> int:
    """With "divided", the limit ÷ the active instance count from sync, rounded up, at least 1; with "per_instance", the limit itself"""
    if mode != "divided":
        return max(1, limit)
    return max(1, math.ceil(limit / max(1, active_instances)))


class RateLimiter:
    def __init__(self, clock: Clock) -> None:
        self._clock = clock
        self._buckets: dict[str, list[_Bucket | None]] = {}

    def check(self, consumer: str, limits: RateLimits | None, mode: RateLimitMode, active_instances: int) -> RateDecision:
        if not limits or (limits.get("per_second") is None and limits.get("per_minute") is None):
            return _ALLOWED
        now = self._clock.monotonic()
        # Take it out and put it back: dict order is then least to most recently used
        buckets = self._buckets.pop(consumer, None) or [None, None]
        self._buckets[consumer] = buckets
        if len(self._buckets) > MAX_RATE_LIMITED_CONSUMERS:
            del self._buckets[next(iter(self._buckets))]

        limited = False
        for index, (_, seconds) in enumerate(_WINDOWS):
            limit = limits.get("per_second") if index == 0 else limits.get("per_minute")
            if limit is None:
                buckets[index] = None
                continue
            bucket = _refill(buckets[index], per_instance_limit(limit, mode, active_instances), seconds, now)
            buckets[index] = bucket
            if bucket.tokens < 1:
                limited = True
        if not limited:
            for held in buckets:
                if held is not None:
                    held.tokens -= 1
            return _ALLOWED
        return _rejection(buckets)


def _refill(bucket: _Bucket | None, capacity: int, window_seconds: int, now: float) -> _Bucket:
    """Lazy refill: add tokens for the time since the last update, capped at capacity. Multiply before dividing so integer cases have no floating-point error"""
    if bucket is None:
        return _Bucket(capacity, capacity, now)
    earned = ((now - bucket.updated_at) * capacity) / (window_seconds * 1000)
    bucket.tokens = min(capacity, bucket.tokens + earned)
    bucket.capacity = capacity
    bucket.updated_at = now
    return bucket


@dataclass(frozen=True, slots=True)
class RateLimitPolicy:
    name: str
    quota: int
    window_seconds: int
    remaining: int
    reset_seconds: int


def _rejection(buckets: list[_Bucket | None]) -> RateDecision:
    retry_after = 0
    policies = []
    for index, (name, seconds) in enumerate(_WINDOWS):
        bucket = buckets[index]
        if bucket is None:
            continue
        exhausted = bucket.tokens < 1
        # A bucket that blocked: time until it has 1 token, rounded up (waiting a little longer never earns another 429). A bucket that didn't: time until it's full
        if exhausted:
            reset = math.ceil(((1 - bucket.tokens) * seconds) / bucket.capacity)
            retry_after = max(retry_after, reset)
        else:
            reset = math.ceil(((bucket.capacity - bucket.tokens) * seconds) / bucket.capacity)
        policies.append(RateLimitPolicy(name, bucket.capacity, seconds, math.floor(bucket.tokens), reset))
    return RateDecision(False, retry_after, rate_limit_headers(policies, retry_after))


def rate_limit_headers(policies: list[RateLimitPolicy], retry_after_seconds: int) -> dict[str, str]:
    """Headers for a 429: Retry-After plus the two fields from draft-ietf-httpapi-ratelimit-headers, version 11.
    Token buckets are expressed the same way as in the usagerail.com rate limiting guide: q is the bucket capacity, w is seconds from empty to full"""
    return {
        "Retry-After": str(retry_after_seconds),
        "RateLimit-Policy": ", ".join(f'"{policy.name}";q={policy.quota};w={policy.window_seconds}' for policy in policies),
        "RateLimit": ", ".join(f'"{policy.name}";r={policy.remaining};t={policy.reset_seconds}' for policy in policies),
    }
