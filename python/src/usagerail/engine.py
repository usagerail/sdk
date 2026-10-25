"""The SDK engine: ties key checks, rate limits, quotas and metering together, independent of any framework. Same rules as engine.ts in the Node SDK.

Order of checks: key format and checksum → status and expiry → consumer suspended → rate limit → quota.
The hot path stays off the network. begin() returns Pending only when it really has to wait for the server (remote verification of a new key, a limited consumer's first lease,
a process that just started without a snapshot); otherwise it returns the decision directly.
Internal errors are swallowed with a warning (at most one a minute) and never affect requests; only keys, suspensions, rate limits and quotas reject requests.

Threads: Flask and Django call in from several request threads at once, network results come back on background threads, and timers run on the timer thread,
so the state of the engine and every module is changed under one lock. The seller's consumer() is called outside the lock.
"""

from __future__ import annotations

import atexit
import hashlib
import hmac
import logging
import math
import os
import re
import threading
import uuid
import weakref
from collections.abc import Mapping
from concurrent.futures import Future
from dataclasses import dataclass, field
from typing import Any, Callable, Generic, Literal, Protocol, TypeVar, Union

from ._futures import Pending, error_of, settle, then
from ._time import iso_from_ms, parse_iso_ms
from .clock import Clock, system_clock
from .key_store import SNAPSHOT_MAX_AGE_MS, KeyCheck, KeyStore, SnapshotStore
from .meter import ANONYMOUS_CONSUMER, UNMATCHED_ROUTE, Meter, Usage, classify_status
from .protocol import BILLABLE_OUTCOMES, AppSettings, ConsumerRecord, RateLimits, RouteSetting
from .quota import UNKNOWN_BEHAVIOR, QuotaDecision, QuotaInfo, QuotaManager, Reservation
from .rate_limiter import RateDecision, RateLimiter
from .routes import RouteTable
from .transport import DEFAULT_BASE_URL, HttpTransport, Transport, TransportError, WorkerPool, describe_error, is_pausing_status

Context = TypeVar("Context")

# Sync every 15 seconds
SYNC_INTERVAL_MS = 15_000
# Errors like a revoked app key: try again every 10 minutes
PAUSED_SYNC_MS = 10 * 60_000
# Right after startup with no usable snapshot (or, with your own auth, before the hashing settings arrive), requests first wait for the first sync, up to 1 second,
# then fall back to remote verification (the same rule as a serverless cold start). Without the wait, the first burst of traffic would all go to remote verification,
# and keys beyond 20 a second would get a 401 straight away
FIRST_SYNC_WAIT_MS = 1_000
# Save to disk at most once a minute: a snapshot of 100,000 keys is over ten MB, no need to write it every 15 seconds
PERSIST_INTERVAL_MS = 60_000
# At most 100,000 consumer records from sync, to keep memory bounded
MAX_CONSUMER_RECORDS = 100_000
# Remember at most 1,000 shared rate limit configurations: when limits follow plans there are only a handful. If every customer has their own and they change often, once full, new ones are stored individually,
# so stale configurations don't hold memory forever
MAX_SHARED_RATE_LIMITS = 1_000
# Cache of HMAC'd external IDs, so hot consumers aren't hashed on every request
MAX_HASH_CACHE = 10_000
MAX_SYNC_PAGES = 1_000
WARNING_INTERVAL_MS = 60_000
CLOCK_SKEW_WARNING_MS = 60_000
CLOSE_TIMEOUT_MS = 5_000
# track() takes at most 10,000,000 per call
MAX_TRACK_UNITS = 10_000_000
# External IDs are at most 256 characters
MAX_EXTERNAL_ID_LENGTH = 256

RejectionCode = Literal[
    "invalid_api_key", "consumer_suspended", "rate_limited", "quota_exceeded", "insufficient_credits", "auth_unavailable", "quota_unavailable"
]


@dataclass(frozen=True, slots=True)
class ConsumerIdentity:
    id: str
    stripe_customer_id: str | None = None


ConsumerResult = Union[ConsumerIdentity, Mapping[str, Any], str, None]


class Logger(Protocol):
    def warning(self, message: str) -> Any: ...


class RequestHandle(Protocol):
    def track(self, units: int) -> None: ...


@dataclass(frozen=True, slots=True)
class Rejection:
    status: Literal[401, 402, 403, 429, 503]
    error: RejectionCode
    retry_after_seconds: int | None = None
    headers: Mapping[str, str] = field(default_factory=dict)
    allowed: Literal[False] = False


@dataclass(frozen=True, slots=True)
class Admission:
    handle: RequestHandle
    allowed: Literal[True] = True


Decision = Union[Admission, Rejection]


_BEARER = re.compile(r"bearer[ \t]+(\S+)\s*", re.IGNORECASE)


def read_api_key(authorization: str | None, x_api_key: str | None) -> str | None:
    """Authorization: Bearer <key> (scheme is case-insensitive, RFC 6750); otherwise x-api-key"""
    if authorization:
        match = _BEARER.fullmatch(authorization.strip())
        if match:
            return match.group(1)
    trimmed = (x_api_key or "").strip()
    return trimmed or None


def rejection_body(rejection: Rejection) -> dict[str, Any]:
    """Default body for rejections. You can write your own in the adapter's on_reject"""
    if rejection.retry_after_seconds is None:
        return {"error": rejection.error}
    return {"error": rejection.error, "retry_after": rejection.retry_after_seconds}


def _rejection(status: Any, error: RejectionCode, retry_after_seconds: int | None = None, headers: Mapping[str, str] | None = None) -> Rejection:
    all_headers = dict(headers or {})
    if retry_after_seconds is not None and "Retry-After" not in all_headers:
        all_headers["Retry-After"] = str(retry_after_seconds)
    return Rejection(status, error, retry_after_seconds, all_headers)


INVALID_API_KEY = _rejection(401, "invalid_api_key")
CONSUMER_SUSPENDED = _rejection(403, "consumer_suspended")
INSUFFICIENT_CREDITS = _rejection(402, "insufficient_credits")
AUTH_UNAVAILABLE = _rejection(503, "auth_unavailable")
QUOTA_UNAVAILABLE = _rejection(503, "quota_unavailable")


@dataclass(frozen=True, slots=True)
class _KnownConsumer:
    # Consumer reference for reporting: our consumer ID, or the external ID (HMAC'd by default); the raw external ID until the hashing settings arrive
    consumer: str
    consumer_kind: Literal["id", "external"]
    pending_hash: bool
    key_id: str | None
    stripe_customer_id: str | None
    quota: QuotaInfo
    rate_limits: RateLimits | None
    suspended: bool


@dataclass(frozen=True, slots=True)
class _Invalid:
    consumer: str | None


class _Anonymous:
    pass


class _Unavailable:
    pass


_ANONYMOUS = _Anonymous()
_UNAVAILABLE = _Unavailable()
_Identity = Union[_KnownConsumer, _Invalid, _Anonymous, _Unavailable]


@dataclass(frozen=True, slots=True)
class _RequestStart:
    method: str
    # The template the framework provided at the start (Flask, Django)
    route: str | None
    # Otherwise, the seller template the path matched: only used to record requests the SDK rejects (they never reach a route), never for settlement
    guessed_route: str | None
    # Units this request reserves
    reserve: int
    started_at: float
    minute_start: float


class _ActiveRequest:
    __slots__ = (
        "method",
        "route_at_start",
        "minute_start",
        "consumer",
        "consumer_kind",
        "pending_hash",
        "stripe_customer_id",
        "reservation",
        "tracked_units",
        "ended",
        "stream_open",
        "_warn",
    )

    def __init__(self, start: _RequestStart, identity: _KnownConsumer | None, reservation: Reservation | None, warn: Callable[[str], None]) -> None:
        self.method = start.method
        # Route template the framework provided when the request started (Flask, Django). If the framework gives none at the end either, it's _unmatched; the path-based guess from the start is never used for settlement:
        # for a Flask redirect that adds a trailing slash, say, settling by the guessed seller template would bill for a route that didn't run
        self.route_at_start = start.route
        self.minute_start = start.minute_start
        self.consumer = identity.consumer if identity else ANONYMOUS_CONSUMER
        self.consumer_kind = identity.consumer_kind if identity else "anonymous"
        self.pending_hash = identity.pending_hash if identity else False
        self.stripe_customer_id = identity.stripe_customer_id if identity else None
        self.reservation = reservation
        self._warn = warn
        self.tracked_units: int | None = None
        self.ended = False
        # The SSE stream started and the connection was counted once; units are added when it ends (stream_started)
        self.stream_open = False

    def track(self, units: int) -> None:
        """Meter custom units: a non-negative integer, at most 10,000,000 per call; the last call wins.
        Invalid values are ignored with a warning; never raises into your code"""
        if isinstance(units, bool) or not isinstance(units, int) or units < 0 or units > MAX_TRACK_UNITS:
            self._warn(f"UsageRail: track() needs an integer from 0 to {MAX_TRACK_UNITS}, got {units!r}; ignored")
            return
        self.tracked_units = units


@dataclass(slots=True)
class _ConsumerEntry:
    """One consumer record from sync. The raw dict isn't kept: a dict holding another dict for rate limits takes 60 to 70 MB for 100,000 records,
    over the memory budget"""

    consumer: str
    external_id: str | None
    suspended: bool
    quota_behavior: Any
    quota_version: int
    rate_limits: RateLimits
    # Used when a full sync spans several pages: records absent from the new full sync are deleted once the last page arrives
    generation: int

    def record(self) -> ConsumerRecord:
        """Turned back into the protocol shape when the snapshot is saved to disk"""
        return {
            "consumer": self.consumer,
            "external_id": self.external_id,
            "status": "suspended" if self.suspended else "active",
            "quota_behavior": self.quota_behavior,
            "quota_version": self.quota_version,
            "rate_limits": dict(self.rate_limits),  # type: ignore[typeddict-item]
        }


class Engine(Generic[Context]):
    def __init__(
        self,
        *,
        app_key: str,
        auth: Literal["api-key", "custom"],
        consumer: Callable[[Context], ConsumerResult] | None = None,
        base_url: str = DEFAULT_BASE_URL,
        clock: Clock | None = None,
        transport: Transport | None = None,
        on_key_store_unavailable: Literal["deny", "allow"] | None = None,
        stale_key_snapshot: Literal["allow", "deny"] | None = None,
        on_quota_unavailable: Literal["allow", "deny"] | None = None,
        snapshot_store: SnapshotStore | None = None,
        logger: Logger | None = None,
        background_loops: bool = True,
        flush_on_exit: bool = True,
        instance_id: str | None = None,
    ) -> None:
        if not isinstance(app_key, str) or not app_key:
            raise ValueError("UsageRail: app_key is required (USAGERAIL_APP_KEY from the console)")
        if auth not in ("api-key", "custom"):
            raise ValueError('UsageRail: auth must be "api-key" or "custom"')
        if auth == "custom" and not callable(consumer):
            raise ValueError('UsageRail: auth "custom" needs a consumer(request) function')
        self._app_key = app_key
        self._auth = auth
        self._consumer = consumer
        self._base_url = base_url
        self._overrides = {
            "on_key_store_unavailable": on_key_store_unavailable,
            "stale_key_snapshot": stale_key_snapshot,
            "on_quota_unavailable": on_quota_unavailable,
        }
        self._clock: Clock = clock or system_clock
        self._own_transport = transport is None
        self.instance_id = instance_id or str(uuid.uuid4())
        self._transport: Transport = transport or HttpTransport(app_key, self.instance_id, base_url)
        self._warn = _rate_limited_warn(logger or logging.getLogger("usagerail"), self._clock)
        self._lock = threading.RLock()
        self._key_store = KeyStore(self._clock, self._transport, self._warn, self._lock) if auth == "api-key" else None
        # A snapshot only makes sense for apps using our keys
        self._snapshot_store = snapshot_store if auth == "api-key" else None
        self._snapshot_pool: WorkerPool | None = None
        self._rate_limiter = RateLimiter(self._clock)
        self._quota = self._new_quota()
        self._meter = self._new_meter()
        # App settings from sync (protocol.AppSettings); None before the first sync
        self._settings: dict[str, Any] | None = None
        self._routes = RouteTable([])
        self._hash_known = False
        self._hash_key: str | None = None
        self._hash_cache: dict[str, str] = {}
        # Keyed by consumer ID with our keys, and by external_id with your own auth (the SDK only knows external IDs)
        self._consumer_records: dict[str, _ConsumerEntry] = {}
        # Identical rate limit configurations (usually from the same plan) share one dict. The rate limiter only reads it
        self._shared_limits: dict[tuple[Any, Any], RateLimits] = {}
        self._consumer_generation = 0
        self._consumer_sweep_pending = False
        self._consumer_capacity_warned = False
        self._active_instances = 1
        self._cursor: str | None = None
        self._syncing: Future[None] | None = None
        self._sync_paused = False
        self._first_sync_gate: Future[None] | None = None
        self._cancel_sync_timer: Callable[[], None] | None = None
        self._last_persisted_at = -math.inf
        self._persisting = False
        self._closing: Future[None] | None = None
        self._closed = False
        self._background_loops = background_loops
        self._load_snapshot()
        if background_loops:
            self._start_background()
        _ENGINES.add(self)
        if flush_on_exit:
            _CLOSE_AT_EXIT.add(self)

    def _setting(self, name: str) -> Any:
        return self._settings.get(name) if self._settings is not None else None

    def _new_quota(self) -> QuotaManager:
        return QuotaManager(
            self._clock,
            self._transport,
            self._warn,
            lambda: self._overrides["on_quota_unavailable"] or self._setting("on_quota_unavailable") or "allow",
            self._lock,
        )

    def _new_meter(self) -> Meter:
        return Meter(
            self._clock,
            self._transport,
            self.instance_id,
            self._warn,
            self._quota,
            lambda: self._hash_consumer if self._hash_known else None,
            self._lock,
        )

    # ---- Request start -----------------------------------------------------------------

    def begin(
        self,
        *,
        method: str,
        path: str,
        route: str | None = None,
        api_key: str | None = None,
        context: Context | None = None,
    ) -> Decision | Pending[Decision]:
        """path: the path without query string, matched against route weights and reservations; it must agree with how the framework sees it.
        route: frameworks that have already routed when the request starts (Flask, Django) pass the matched template, or None if nothing matched. If the seller configured that template, it wins.
        Returns Pending when the decision has to wait for the server: .result() in WSGI, await in ASGI"""
        # The seller's consumer() is called outside the lock: it may hit a database, and other requests mustn't queue behind it
        custom = self._call_consumer(context) if self._auth == "custom" else None
        with self._lock:
            started_at = self._clock.now()
            try:
                identity = self._identify(api_key, custom)
            except Exception as error:  # noqa: BLE001 - internal errors must not affect requests
                identity = self._identify_failed(error)
            if isinstance(identity, Future):
                decision: Future[Decision] = then(
                    identity,
                    lambda resolved_identity: self._locked_decide(method, path, route, resolved_identity, started_at),
                    lambda error: self._locked_decide(method, path, route, self._identify_failed(error), started_at),
                )
                return Pending(decision)
            decided = self._decide(method, path, route, identity, started_at)
            return Pending(decided) if isinstance(decided, Future) else decided

    def _call_consumer(self, context: Any) -> ConsumerIdentity | _Anonymous:
        try:
            result = self._consumer(context) if self._consumer else None
        except Exception as error:  # noqa: BLE001
            self._warn(f"UsageRail: consumer() raised ({describe_error(error)}); the request was counted as anonymous")
            return _ANONYMOUS
        if result is None:
            return _ANONYMOUS
        if isinstance(result, ConsumerIdentity):
            identity = result
        elif isinstance(result, str):
            identity = ConsumerIdentity(result)
        elif isinstance(result, Mapping):
            identity = ConsumerIdentity(result.get("id"), result.get("stripe_customer_id"))  # type: ignore[arg-type]
        else:
            # The seller returned some other type: handled below like a bad id
            identity = ConsumerIdentity(None)  # type: ignore[arg-type, unused-ignore]
        if not isinstance(identity.id, str) or not 0 < len(identity.id) <= MAX_EXTERNAL_ID_LENGTH:
            self._warn(f"UsageRail: consumer() must return an id of 1-{MAX_EXTERNAL_ID_LENGTH} characters; the request was counted as anonymous")
            return _ANONYMOUS
        return identity

    def _locked_decide(self, method: str, path: str, route: str | None, identity: _Identity, started_at: float) -> Decision | Future[Decision]:
        with self._lock:
            return self._decide(method, path, route, identity, started_at)

    def _identify(self, api_key: str | None, custom: ConsumerIdentity | _Anonymous | None) -> _Identity | Future[_Identity]:
        gate = self._first_sync_gate
        if gate is not None:
            return then(gate, lambda _: self._locked_identify_now(api_key, custom))
        return self._identify_now(api_key, custom)

    def _locked_identify_now(self, api_key: str | None, custom: ConsumerIdentity | _Anonymous | None) -> _Identity | Future[_Identity]:
        with self._lock:
            return self._identify_now(api_key, custom)

    def _identify_now(self, api_key: str | None, custom: ConsumerIdentity | _Anonymous | None) -> _Identity | Future[_Identity]:
        if self._key_store is not None:
            stale = self._overrides["stale_key_snapshot"] or self._setting("stale_key_snapshot") or "allow"
            check = self._key_store.check(api_key, stale)
            if isinstance(check, Future):
                return then(check, self._locked_identity_from_key)
            return self._identity_from_key(check)
        return self._identity_from_custom(custom)

    def _identify_failed(self, error: BaseException) -> _Identity:
        """On an internal error: with our keys, treat it as "can't verify" (onKeyStoreUnavailable); with your own auth, let it through as anonymous"""
        self._warn(f"UsageRail: {describe_error(error)}")
        return _UNAVAILABLE if self._key_store is not None else _ANONYMOUS

    def _locked_identity_from_key(self, check: KeyCheck) -> _Identity:
        with self._lock:
            return self._identity_from_key(check)

    def _identity_from_key(self, check: KeyCheck) -> _Identity:
        if check.result == "invalid":
            return _Invalid(check.consumer)
        if check.result == "unavailable":
            return _UNAVAILABLE
        consumer = check.consumer or ""
        # Consumer details from remote verification (rare) are converted on the spot; otherwise use what sync stored
        record = self._entry_of(check.consumer_record, -1) if check.consumer_record is not None else self._consumer_records.get(consumer)
        # Both the key record and the consumer record carry a quota version; use the newer
        if record is not None and record.quota_version >= check.quota_version:
            quota = QuotaInfo(record.quota_behavior, record.quota_version)
        else:
            quota = QuotaInfo(check.quota_behavior, check.quota_version)
        return _KnownConsumer(
            consumer=consumer,
            consumer_kind="id",
            pending_hash=False,
            key_id=check.key_id,
            stripe_customer_id=None,
            quota=quota,
            rate_limits=record.rate_limits if record else None,
            suspended=record is not None and record.suspended,
        )

    def _identity_from_custom(self, custom: ConsumerIdentity | _Anonymous | None) -> _Identity:
        if not isinstance(custom, ConsumerIdentity):
            return _ANONYMOUS
        raw_id = custom.id
        stripe_customer_id = custom.stripe_customer_id if isinstance(custom.stripe_customer_id, str) and custom.stripe_customer_id else None
        if not self._hash_known:
            # Don't know yet whether to hash or with what key: count by raw ID in memory for now and convert once the settings arrive
            return _KnownConsumer(raw_id, "external", True, None, stripe_customer_id, QuotaInfo(UNKNOWN_BEHAVIOR, 0), None, False)
        consumer = self._hash_consumer(raw_id)
        record = self._consumer_records.get(consumer)
        quota = QuotaInfo(record.quota_behavior, record.quota_version) if record else QuotaInfo(UNKNOWN_BEHAVIOR, 0)
        return _KnownConsumer(
            consumer,
            "external",
            False,
            None,
            stripe_customer_id,
            quota,
            record.rate_limits if record else None,
            record is not None and record.suspended,
        )

    def _hash_consumer(self, raw_id: str) -> str:
        """External IDs are HMAC-SHA256'd with the app's hashing key, UTF-8, lowercase hex; reported as is when the seller turns hashing off"""
        if self._hash_key is None:
            return raw_id
        cached = self._hash_cache.get(raw_id)
        if cached is not None:
            return cached
        hashed = hmac.new(self._hash_key.encode(), raw_id.encode(), hashlib.sha256).hexdigest()
        if len(self._hash_cache) >= MAX_HASH_CACHE:
            self._hash_cache.clear()
        self._hash_cache[raw_id] = hashed
        return hashed

    def _decide(self, method: str, path: str, route: str | None, identity: _Identity, started_at: float) -> Decision | Future[Decision]:
        start = self._request_start(method, path, route, started_at)
        if isinstance(identity, _Invalid):
            self._record_rejection(start, identity.consumer, None if identity.consumer is None else "id", "sdk_401")
            return INVALID_API_KEY
        if isinstance(identity, _Unavailable):
            choice = self._overrides["on_key_store_unavailable"] or self._setting("on_key_store_unavailable") or "deny"
            if choice == "allow":
                return self._admit(start, None, None)
            # There's no outcome for the SDK's own 503, so record the status it actually returns
            self._record_rejection(start, None, None, "5xx")
            return AUTH_UNAVAILABLE
        if isinstance(identity, _Anonymous):
            return self._admit(start, None, None)
        return self._decide_for_consumer(start, identity)

    def _request_start(self, method: str, path: str, route: str | None, started_at: float) -> _RequestStart:
        method = method.upper()
        # When the framework provides the template at the start (Flask, Django), reserve by it: 1 if the seller didn't configure it, matching settlement. Otherwise (FastAPI middleware
        # runs before routing) match the path against the seller's templates. Several may match and which one handles it is only known afterwards, so reserve for the largest
        # (better too much, refunded at the end). Settlement only trusts the template the framework reports at the end (_actual_units); this guess only sets the reservation
        if route:
            exact = self._routes.by_template(method, route)
            candidates = [exact] if exact is not None else []
        else:
            candidates = self._routes.candidates_by_path(method, path)
        reserve = max([1, *(self._units_to_reserve(candidate) for candidate in candidates)])
        minute_start = started_at - started_at % 60_000
        return _RequestStart(method, route, candidates[0]["route"] if candidates else None, reserve, started_at, minute_start)

    def _decide_for_consumer(self, start: _RequestStart, identity: _KnownConsumer) -> Decision | Future[Decision]:
        if identity.key_id:
            self._meter.record_key_use(identity.key_id)
        # Consumers the seller suspended or is deleting: block everything (protocol.ts ConsumerRecord.status)
        if identity.suspended:
            self._record_rejection(start, identity.consumer, identity.consumer_kind, "sdk_403", identity)
            return CONSUMER_SUSPENDED
        if not identity.pending_hash:
            rate = RateDecision(True)
            try:
                mode: Literal["per_instance", "divided"] = self._setting("rate_limit_mode") or "per_instance"
                rate = self._rate_limiter.check(identity.consumer, identity.rate_limits, mode, self._active_instances)
            except Exception as error:  # noqa: BLE001
                self._warn(f"UsageRail: rate limiter failed ({describe_error(error)})")
            if not rate.allowed:
                # Requests blocked by the rate limit aren't billed; recorded as sdk_429
                self._record_rejection(start, identity.consumer, identity.consumer_kind, "sdk_429", identity)
                return _rejection(429, "rate_limited", rate.retry_after_seconds, rate.headers)
        quota: QuotaDecision | Future[QuotaDecision]
        try:
            # The raw external ID can't be converted to a consumer ID yet, so no lease is possible: treat the control plane as unavailable
            if identity.pending_hash:
                quota = self._quota.when_unavailable()
            else:
                quota = self._quota.reserve(identity.consumer, identity.consumer_kind, identity.quota, start.reserve, start.started_at)
        except Exception as error:  # noqa: BLE001
            self._warn(f"UsageRail: quota check failed ({describe_error(error)})")
            quota = self._quota.when_unavailable()
        if isinstance(quota, Future):
            return then(
                quota,
                lambda decision: self._locked_after_quota(start, identity, decision),
                lambda error: self._locked_after_quota(start, identity, self._quota_failed(error)),
            )
        return self._after_quota(start, identity, quota)

    def _quota_failed(self, error: BaseException) -> QuotaDecision:
        with self._lock:
            self._warn(f"UsageRail: quota check failed ({describe_error(error)})")
            return self._quota.when_unavailable()

    def _locked_after_quota(self, start: _RequestStart, identity: _KnownConsumer, decision: QuotaDecision) -> Decision:
        with self._lock:
            return self._after_quota(start, identity, decision)

    def _after_quota(self, start: _RequestStart, identity: _KnownConsumer, decision: QuotaDecision) -> Decision:
        if decision.allowed:
            return self._admit(start, identity, decision.reservation)
        if decision.error == "quota_exceeded":
            self._record_rejection(start, identity.consumer, identity.consumer_kind, "sdk_429", identity)
            return _rejection(429, "quota_exceeded", decision.retry_after_seconds)
        if decision.error == "insufficient_credits":
            self._record_rejection(start, identity.consumer, identity.consumer_kind, "sdk_402", identity)
            return INSUFFICIENT_CREDITS
        if decision.error == "consumer_suspended":
            self._record_rejection(start, identity.consumer, identity.consumer_kind, "sdk_403", identity)
            return CONSUMER_SUSPENDED
        self._record_rejection(start, identity.consumer, identity.consumer_kind, "5xx", identity)
        return QUOTA_UNAVAILABLE

    def _units_to_reserve(self, setting: RouteSetting | None) -> int:
        """How much one request reserves from the lease: the route weight when metering requests. When metering units we don't know whether the handler will call track(),
        so take the larger of the weight and the per-request reservation. Better to reserve too much (refunded at the end) than admit too much"""
        if setting is None:
            return 1
        if self._setting("unit_mode") == "requests":
            return setting["weight"]
        return max(setting["weight"], setting["reserve"])

    def _admit(self, start: _RequestStart, identity: _KnownConsumer | None, reservation: Reservation | None) -> Admission:
        return Admission(_ActiveRequest(start, identity, reservation, self._warn))

    def _record_rejection(self, start: _RequestStart, consumer: str | None, kind: str | None, outcome: str, identity: _KnownConsumer | None = None) -> None:
        try:
            self._meter.record(
                Usage(
                    consumer=consumer if consumer is not None else ANONYMOUS_CONSUMER,
                    kind="anonymous" if consumer is None or kind is None else kind,
                    stripe_customer_id=identity.stripe_customer_id if identity else None,
                    method=start.method,
                    route=start.route or start.guessed_route or UNMATCHED_ROUTE,
                    outcome=outcome,
                    minute_start=start.minute_start,
                    units=0,
                ),
                identity.pending_hash if identity else False,
            )
        except Exception as error:  # noqa: BLE001
            self._warn(f"UsageRail: {describe_error(error)}")

    # ---- Request end -------------------------------------------------------------------

    def stream_started(self, handle: RequestHandle, route: str | None = None) -> None:
        """Call when an SSE stream starts sending: the long-lived connection counts once right away. Apps metering custom units only know what track() recorded when the connection ends,
        so the reservation stays until end(), which settles by actual units and adds them"""
        if not isinstance(handle, _ActiveRequest):
            return
        with self._lock:
            if handle.ended or handle.stream_open:
                return
            route_name = route or handle.route_at_start or UNMATCHED_ROUTE
            # For apps metering requests, units are just the route weight, so it can be settled now
            if self._setting("unit_mode") == "requests":
                self.end(handle, outcome="ws_open", route=route_name)
                return
            handle.stream_open = True
            try:
                self._meter.record(self._usage_of(handle, route_name, "ws_open", 0), handle.pending_hash)
            except Exception as error:  # noqa: BLE001
                self._warn(f"UsageRail: {describe_error(error)}")

    def end(self, handle: RequestHandle, *, status: int | None = None, outcome: str | None = None, route: str | None = None) -> None:
        """End of a request: a status code, or an outcome directly (aborted, ws_open); route is the template the framework resolved. Only the first call counts"""
        if not isinstance(handle, _ActiveRequest):
            return
        with self._lock:
            if handle.ended:
                return
            handle.ended = True
            if handle.stream_open:
                self._end_stream(handle, route)
                return
            try:
                result = outcome if outcome is not None else classify_status(status if status is not None else 500)
                route_name = route or handle.route_at_start or UNMATCHED_ROUTE
                # Only billable outcomes count units; 4xx, 5xx and aborted refund the whole reservation
                units = self._actual_units(handle, route_name) if result in BILLABLE_OUTCOMES else 0
                if handle.reservation is not None:
                    self._quota.settle(handle.reservation, units)
                self._meter.record(self._usage_of(handle, route_name, result, units), handle.pending_hash)
            except Exception as error:  # noqa: BLE001
                self._warn(f"UsageRail: {describe_error(error)}")

    def _end_stream(self, handle: _ActiveRequest, route: str | None) -> None:
        """SSE connection ended: settle the reservation by track() (or the route weight if it wasn't called) and add the units. The connection was already counted once at the start, so the count here is 0"""
        try:
            route_name = route or handle.route_at_start or UNMATCHED_ROUTE
            units = self._actual_units(handle, route_name)
            if handle.reservation is not None:
                self._quota.settle(handle.reservation, units)
            if units > 0:
                self._meter.record(self._usage_of(handle, route_name, "ws_open", units), handle.pending_hash, 0)
        except Exception as error:  # noqa: BLE001
            self._warn(f"UsageRail: {describe_error(error)}")

    def _usage_of(self, handle: _ActiveRequest, route: str, outcome: str, units: int) -> Usage:
        return Usage(handle.consumer, handle.consumer_kind, handle.stripe_customer_id, handle.method, route, outcome, handle.minute_start, units)

    def _actual_units(self, handle: _ActiveRequest, route: str) -> int:
        """track() wins if it was called; otherwise the route weight; otherwise 1. track() doesn't count when the app meters requests.
        Weights only come from the template the framework reports; unconfigured routes count as 1, never settled by the path-based guess from the start"""
        if handle.tracked_units is not None and self._setting("unit_mode") != "requests":
            return handle.tracked_units
        setting = self._routes.by_template(handle.method, route)
        return setting["weight"] if setting else 1

    # ---- Sync --------------------------------------------------------------------------

    def sync(self) -> Future[None]:
        with self._lock:
            if self._syncing is None:
                self._syncing = Future()
                syncing = self._syncing
                self._sync_pages()
                return syncing
            return self._syncing

    def _sync_pages(self) -> None:
        """Paged: when has_more is true, fetch again right away with the new cursor (protocol.ts). When the transport answers on the spot, keep fetching in this loop instead of recursing"""
        for _ in range(MAX_SYNC_PAGES):
            call = self._transport.sync(self._cursor)
            if not call.done():
                call.add_done_callback(self._page_arrived_later)
                return
            if not self._apply_page(call):
                return
        self._warn("UsageRail: sync kept returning more pages; continuing on the next sync")
        self._finish_sync()

    def _page_arrived_later(self, call: Future[dict[str, Any]]) -> None:
        with self._lock:
            if self._apply_page(call):
                self._sync_pages()

    def _apply_page(self, call: Future[dict[str, Any]]) -> bool:
        """True: there's another page to fetch"""
        error = error_of(call)
        if error is not None:
            self._sync_paused = isinstance(error, TransportError) and is_pausing_status(error.status)
            self._warn(f"UsageRail: sync failed ({describe_error(error)})")
            self._finish_sync()
            return False
        response = call.result()
        try:
            self._apply_sync_page(response)
            has_more = bool(response.get("has_more"))
            if not has_more:
                self._complete_sync()
        except (KeyError, TypeError, ValueError, AttributeError) as parse_error:
            self._warn(f"UsageRail: sync returned an unreadable page ({describe_error(parse_error)})")
            self._finish_sync()
            return False
        if has_more:
            return True
        self._sync_paused = False
        self._finish_sync()
        return False

    def _finish_sync(self) -> None:
        syncing, self._syncing = self._syncing, None
        if syncing is not None:
            settle(syncing, None)

    def _apply_sync_page(self, response: dict[str, Any]) -> None:
        self._cursor = response["cursor"]
        self._apply_settings(response["app"])
        if self._key_store is not None:
            self._key_store.apply_sync_page(bool(response["full"]), response["keys"], response["removed_keys"])
        # Full sync: replace consumer records entirely (protocol.ts). When paged, start on the first page and delete absent records only when the last page arrives
        if response["full"] and not self._consumer_sweep_pending:
            self._consumer_generation += 1
            self._consumer_sweep_pending = True
        for record in response["consumers"]:
            self._store_consumer(record)
        active = response.get("active_instances")
        if isinstance(active, (int, float)) and math.isfinite(active):
            self._active_instances = max(1, int(active))
        self._check_clock_skew(response.get("server_time"))

    def _complete_sync(self) -> None:
        if self._key_store is not None:
            self._key_store.complete_sync()
        if self._consumer_sweep_pending:
            for key in [key for key, entry in self._consumer_records.items() if entry.generation < self._consumer_generation]:
                del self._consumer_records[key]
                # Gone from the full sync (e.g. deleted): cached local results like "unlimited" and "suspended" are dropped too, and the next request asks the server again.
                # Otherwise a suspended consumer with your own auth, blocked only by its synced record, would be let through again once the record disappears
                self._quota.forget(key)
            self._consumer_sweep_pending = False
        self._persist_snapshot()

    def _apply_settings(self, app: AppSettings) -> None:
        if self._hash_key != app.get("hash_key"):
            self._hash_cache.clear()
        self._settings = dict(app)
        self._hash_key = app.get("hash_key")
        self._hash_known = True
        self._routes = RouteTable(app.get("routes") or [])

    def _store_consumer(self, record: ConsumerRecord) -> None:
        key = record.get("external_id") if self._auth == "custom" else record["consumer"]
        if key is None:
            return
        # The cap only applies to active records: suspended ones are always stored, since they're what needs blocking (dropping them would let suspended consumers through)
        if key not in self._consumer_records and len(self._consumer_records) >= MAX_CONSUMER_RECORDS and record["status"] == "active":
            if not self._consumer_capacity_warned:
                self._warn(f"UsageRail: more than {MAX_CONSUMER_RECORDS} consumers synced; the rest have no local limits")
            self._consumer_capacity_warned = True
            # The record can't be stored, but it changed: existing quota state still follows (a new quota version retires old leases and drops "unlimited")
            self._quota.observe_record(key, record["quota_version"], record["quota_behavior"], record["status"])
            return
        self._consumer_records[key] = self._entry_of(record, self._consumer_generation)
        self._quota.observe_record(key, record["quota_version"], record["quota_behavior"], record["status"])

    def _entry_of(self, record: ConsumerRecord, generation: int) -> _ConsumerEntry:
        limits = record.get("rate_limits") or {}
        pair = (limits.get("per_second"), limits.get("per_minute"))
        shared = self._shared_limits.get(pair)
        if shared is None:
            shared = {"per_second": pair[0], "per_minute": pair[1]}
            if len(self._shared_limits) < MAX_SHARED_RATE_LIMITS:
                self._shared_limits[pair] = shared
        # With our keys the external ID is never used (lookups are by consumer ID), so it's not kept: a hashed external ID is 64 hex characters, 10 MB for 100,000 records
        external_id = record.get("external_id") if self._auth == "custom" else None
        return _ConsumerEntry(record["consumer"], external_id, record["status"] == "suspended", record["quota_behavior"], record["quota_version"], shared, generation)

    def _check_clock_skew(self, server_time: Any) -> None:
        """Minute buckets and lease periods use this machine's clock; if it's too far off, the server rejects usage rows and lease requests"""
        server = parse_iso_ms(server_time)
        if server is None:
            return
        skew = self._clock.now() - server
        if abs(skew) > CLOCK_SKEW_WARNING_MS:
            self._warn(f"UsageRail: this machine's clock is {round(skew / 1000)} s off the server's; usage minutes depend on it, check NTP")

    # ---- Snapshot on disk ----------------------------------------------------------------

    def _load_snapshot(self) -> None:
        if self._snapshot_store is None or self._key_store is None:
            return
        try:
            snapshot = self._snapshot_store.load()
        except Exception as error:  # noqa: BLE001
            self._warn(f"UsageRail: could not read the key snapshot ({describe_error(error)})")
            return
        if not snapshot:
            return
        saved_at = parse_iso_ms(snapshot.get("saved_at"))
        # A snapshot older than 24 hours counts as none
        if saved_at is None or self._clock.now() - saved_at > SNAPSHOT_MAX_AGE_MS:
            return
        try:
            self._key_store.load(snapshot["keys"], saved_at)
            for record in snapshot["consumers"]:
                self._store_consumer(record)
            app = snapshot.get("app")
            if app:
                # The hashing key isn't saved to disk, so _hash_known stays False until the first sync
                self._settings = {**app, "hash_key": None}
                self._routes = RouteTable(app.get("routes") or [])
            self._cursor = snapshot.get("cursor")
        except (KeyError, TypeError, ValueError, AttributeError) as error:
            self._warn(f"UsageRail: the key snapshot is unreadable ({describe_error(error)}); ignoring it")

    def _persist_snapshot(self) -> None:
        if self._snapshot_store is None or self._key_store is None or self._persisting:
            return
        now = self._clock.monotonic()
        if now - self._last_persisted_at < PERSIST_INTERVAL_MS:
            return
        self._last_persisted_at = now
        self._persisting = True
        settings = {key: value for key, value in self._settings.items() if key != "hash_key"} if self._settings else None
        snapshot = {
            "saved_at": iso_from_ms(self._clock.now()),
            "cursor": self._cursor,
            "app": settings,
            "keys": self._key_store.records(),
            "consumers": [entry.record() for entry in self._consumer_records.values()],
        }
        store = self._snapshot_store
        if self._snapshot_pool is None:
            self._snapshot_pool = WorkerPool(max_workers=1, name="usagerail-snapshot")
        # Writing the file (over ten MB of JSON) happens on a background thread, without holding the lock
        self._snapshot_pool.submit(lambda: store.save(snapshot)).add_done_callback(self._snapshot_saved)

    def _snapshot_saved(self, saved: Future[None]) -> None:
        with self._lock:
            self._persisting = False
            error = error_of(saved)
            if error is not None:
                self._warn(f"UsageRail: could not save the key snapshot ({describe_error(error)})")

    # ---- Background loops and exit -------------------------------------------------------

    def _start_background(self) -> None:
        with self._lock:
            needs_first_sync = self._key_store.freshness() == "none" if self._key_store is not None else not self._hash_known
            first_sync = self.sync()
            if needs_first_sync and not first_sync.done():
                gate: Future[None] = Future()
                self._first_sync_gate = gate

                def open_gate(_: Any = None) -> None:
                    with self._lock:
                        if self._first_sync_gate is gate:
                            self._first_sync_gate = None
                    settle(gate, None)

                cancel = self._clock.set_timer(open_gate, FIRST_SYNC_WAIT_MS)

                def synced(_: Future[None]) -> None:
                    cancel()
                    open_gate()

                first_sync.add_done_callback(synced)
            first_sync.add_done_callback(lambda _: self._schedule_next_sync())
            self._meter.start()

    def _schedule_next_sync(self) -> None:
        with self._lock:
            if self._closed:
                return
            delay = PAUSED_SYNC_MS if self._sync_paused else SYNC_INTERVAL_MS
            self._cancel_sync_timer = self._clock.set_timer(lambda: self.sync().add_done_callback(lambda _: self._schedule_next_sync()), delay)

    def flush(self) -> Future[None]:
        with self._lock:
            return self._meter.flush()

    def drain(self) -> None:
        """Call when the app is shutting down and draining requests: switches to reporting once a second, so requests that finish during the drain are reported too. Still call close() at the end"""
        with self._lock:
            self._meter.drain()

    def close(self) -> Future[None]:
        """Before exit: stop background loops, retire all leases, and try to report remaining usage and "lease closed", waiting up to 5 seconds"""
        with self._lock:
            if self._closing is None:
                self._closed = True
                _CLOSE_AT_EXIT.discard(self)
                if self._cancel_sync_timer is not None:
                    self._cancel_sync_timer()
                self._quota.retire_all()
                self._quota.stop()
                closing: Future[None] = Future()
                self._closing = closing

                def finished(reported: Future[None]) -> None:
                    error = error_of(reported)
                    if error is not None:
                        self._warn(f"UsageRail: final report failed ({describe_error(error)})")
                    settle(closing, None)

                cancel = self._clock.set_timer(lambda: settle(closing, None), CLOSE_TIMEOUT_MS)
                closing.add_done_callback(lambda _: cancel())
                self._meter.close().add_done_callback(finished)
            return self._closing

    # ---- fork ------------------------------------------------------------------------

    def _before_fork(self) -> None:
        self._lock.acquire()

    def _after_fork_in_parent(self) -> None:
        self._lock.release()

    def _after_fork_in_child(self) -> None:
        """With gunicorn --preload and the like, the app is built before forking: the child is a new instance. Synced keys, consumers and settings are kept,
        but the parent's usage, leases, in-flight requests and background timers don't belong to it and start over. The lock is replaced too: at fork time it may be held by a parent thread"""
        self._lock = threading.RLock()
        self.instance_id = str(uuid.uuid4())
        if self._own_transport:
            self._transport = HttpTransport(self._app_key, self.instance_id, self._base_url)
        if self._key_store is not None:
            self._key_store.lock = self._lock
            self._key_store._transport = self._transport  # noqa: SLF001 - module in the same package
            self._key_store.reset_after_fork()
        self._quota = self._new_quota()
        self._meter = self._new_meter()
        self._syncing = None
        self._first_sync_gate = None
        self._cancel_sync_timer = None
        self._persisting = False
        self._snapshot_pool = None
        if self._background_loops and not self._closed:
            # Other fork hooks (the transport's thread pool) haven't finished yet; start once the timer thread is up
            self._clock.set_timer(self._start_background, 0)


def create_engine(**options: Any) -> Engine[Any]:
    """Framework-agnostic entry point: use it for a framework without an adapter, get a decision from begin(), and call end() when the request finishes"""
    return Engine(**options)


def _rate_limited_warn(logger: Logger, clock: Clock) -> Callable[[str], None]:
    """At most one warning a minute: when something breaks it tends to fire on every request, and mustn't flood the logs"""
    state = {"last_at": -math.inf, "suppressed": 0}

    def warn(message: str) -> None:
        now = clock.monotonic()
        if now - state["last_at"] < WARNING_INTERVAL_MS:
            state["suppressed"] += 1
            return
        state["last_at"] = now
        suppressed = state["suppressed"]
        state["suppressed"] = 0
        try:
            logger.warning(f"{message} ({suppressed} more warnings in the last minute were dropped)" if suppressed else message)
        except Exception:  # noqa: BLE001 - even a failing logger mustn't affect requests
            pass

    return warn


# Live engines: each needs handling at fork and a final report at exit. Weak references, so engines can still be garbage collected
_ENGINES: weakref.WeakSet[Engine[Any]] = weakref.WeakSet()
_CLOSE_AT_EXIT: weakref.WeakSet[Engine[Any]] = weakref.WeakSet()


def _before_fork() -> None:
    for engine in list(_ENGINES):
        engine._before_fork()  # noqa: SLF001


def _after_fork_in_parent() -> None:
    for engine in list(_ENGINES):
        engine._after_fork_in_parent()  # noqa: SLF001


def _after_fork_in_child() -> None:
    for engine in list(_ENGINES):
        engine._after_fork_in_child()  # noqa: SLF001


def _close_at_exit() -> None:
    """Before a normal process exit (gunicorn drains requests and exits normally after SIGTERM), report the remaining usage, waiting up to 5 seconds.
    uvicorn re-raises SIGTERM after finishing up, so this doesn't run there: FastAPI reports through lifespan shutdown, and Django through the usagerail.django.asgi wrapper"""
    pending = [engine.close() for engine in list(_CLOSE_AT_EXIT)]
    for closing in pending:
        try:
            closing.result(timeout=CLOSE_TIMEOUT_MS / 1000 + 1)
        except Exception:  # noqa: BLE001
            pass


# Windows has no fork and no such function
if hasattr(os, "register_at_fork"):
    os.register_at_fork(before=_before_fork, after_in_parent=_after_fork_in_parent, after_in_child=_after_fork_in_child)
atexit.register(_close_at_exit)

__all__ = [
    "Admission",
    "ConsumerIdentity",
    "Decision",
    "Engine",
    "MAX_TRACK_UNITS",
    "Pending",
    "Rejection",
    "RejectionCode",
    "RequestHandle",
    "create_engine",
    "read_api_key",
    "rejection_body",
]
