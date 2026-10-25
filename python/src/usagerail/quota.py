"""Quota leases, same as quota.ts in the Node SDK.

For consumers with hard limits or prepaid credits, the SDK first gets a lease from the server, reserves from it when a request starts, settles by actual units when it ends,
and renews in the background when it runs low. Consumers billed for overage don't get leases.

Conventions that hold throughout this file:
- A lease's available is what can still be admitted locally; usage (unreported) is tracked separately, and what's reported is the units actually used. The two never mix.
- When actual usage exceeds the reservation (more custom units than reserved), the excess becomes a debt on the consumer. Later leases pay it off first,
  and no new requests are admitted until it's paid. That's the "once negative, admit nothing until a renewal covers it" rule: leases the server issued before it knew about the excess
  hand out too much, and the debt offsets that. Leases requested after the server confirmed receiving that usage already account for it and don't pay it off.
- One request can gather its reservation from several leases, so quota split across leases never blocks anyone.
- After a lease is retired, the next batch carries "lease closed" only once every request it admitted has finished and all its usage was reported successfully.
"""

from __future__ import annotations

import math
import threading
from concurrent.futures import Future
from dataclasses import dataclass, field
from typing import Any, Callable, Literal

from ._futures import error_of, settle
from ._time import iso_from_ms, parse_iso_ms
from .clock import Clock
from .protocol import MAX_ROW_UNITS, QuotaBehavior
from .transport import Transport, describe_error

MINUTE_MS = 60_000
# Local lifetime: retired 60 seconds after it arrives, by the monotonic clock
LEASE_TTL_MS = 60_000
# Renew in the background when less than 30% is left
RENEW_BELOW_FRACTION = 0.3
# Leases still in use are renewed early once they're 45 seconds old; otherwise a low-traffic consumer would have one request a minute wait for a new lease
RENEW_AFTER_MS = 45_000
# A request waits for a lease for at most 300 ms (a hard rule)
SYNC_WAIT_MS = 300
# Background renewals have no request waiting, so their timeout is more generous: after a timeout the server may already have issued the lease, and that quota only comes back when it's force-settled 30 minutes later
RENEW_TIMEOUT_MS = 2_000
# At most 500 consumers per batched renewal
MAX_LEASE_REQUESTS_PER_CALL = 500
# How long "exhausted" and "suspended" results are cached before asking again. Quota comes back when other instances settle, but that doesn't change the version,
# so caching by version alone would block this consumer until the next period
DENIAL_RECHECK_MS = 10_000
# For a few seconds after a lease request fails, don't wait at all and apply onQuotaUnavailable: when the server is down, requests shouldn't each wait the full 300 ms
LEASE_OUTAGE_MS = 5_000
# Caches quota state for at most 40,000 consumers, evicting the least recently used. Kept to 40,000 to stay under 100 MB fully loaded:
# an active customer (quota state, leases, rate limit buckets) takes about 0.9 KB, and a full key snapshot plus consumer records is over 40 MB (bench/memory.py).
# An evicted consumer gets a new lease when it comes back, one extra round trip
MAX_QUOTA_CONSUMERS = 40_000
# Look at no more than 64 entries per eviction: when the table is full and everyone is busy, a new customer shouldn't trigger a scan of all 40,000
MAX_EVICTION_SCAN = 64
# at can't be more than 24 hours before server time (protocol.ts); anything older is left out and the server uses its current time
_MAX_AT_AGE_MS = 23 * 60 * MINUTE_MS

_UNKNOWN: Any = object()


@dataclass(frozen=True, slots=True)
class QuotaInfo:
    # behavior _UNKNOWN means not known yet (a consumer with your own auth that hasn't appeared in a sync): the first request gets a lease synchronously
    behavior: QuotaBehavior | None | Any
    version: int


UNKNOWN_BEHAVIOR = _UNKNOWN


@dataclass(frozen=True, slots=True)
class QuotaDecision:
    allowed: bool
    reservation: Reservation | None = None
    error: Literal["quota_exceeded", "insufficient_credits", "quota_unavailable", "consumer_suspended"] | None = None
    retry_after_seconds: int | None = None


@dataclass(eq=False, slots=True)
class LocalLease:
    id: str
    owner: _ConsumerState
    granted: int
    version: int
    behavior: Literal["stop", "prepaid"]
    period_end: float
    # On the monotonic clock: when the request for this lease was sent, and when it's retired locally
    requested_at: float
    expires_at: float
    available: int
    in_flight: int = 0
    # Settled usage not yet put into a report batch
    unreported: int = 0
    # Number of batches carrying its usage that the server hasn't confirmed yet
    unacked: int = 0
    retired: bool = False
    close_sent: bool = False


@dataclass(eq=False, slots=True)
class Reservation:
    """One reservation can come from several leases"""

    parts: list[tuple[LocalLease, int]]
    units: int
    settled: bool = False
    owner: _ConsumerState | None = None


@dataclass(eq=False, slots=True)
class _Debt:
    """A debt: usage beyond the reservation. Once the batch reporting it is confirmed by the server (acked_at), leases requested afterwards no longer pay it off"""

    amount: int
    lease: LocalLease
    period_end: float
    reported: bool = False
    acked_at: float | None = None


@dataclass(slots=True)
class LeaseReport:
    """The lease part of a report batch. Handed back to finish_report once the batch is confirmed or given up on"""

    entries: list[dict[str, Any]] = field(default_factory=list)
    used: list[LocalLease] = field(default_factory=list)
    closed: list[LocalLease] = field(default_factory=list)
    # Debts whose usage this batch carries
    debts: list[_Debt] = field(default_factory=list)


@dataclass(slots=True)
class _Denial:
    reason: Literal["exhausted", "suspended"]
    version: int
    behavior: Literal["stop", "prepaid"] | None
    # "Exhausted" was judged for a reservation this large: smaller requests might still get a lease, so the result doesn't apply to them
    reserve: int
    # Hard limits: when the period ends (wall clock), derived from the server's retry_after
    period_end: float | None
    checked_at: float


@dataclass(eq=False, slots=True)
class _ConsumerState:
    consumer: str
    kind: str
    window_start: float
    version: int = 0
    behavior: Any = _UNKNOWN
    unlimited_version: int | None = None
    denial: _Denial | None = None
    # Leases that can still admit requests, oldest first
    leases: list[LocalLease] = field(default_factory=list)
    debts: list[_Debt] = field(default_factory=list)
    # Requests that reserved and haven't settled, and requests waiting for a lease: a consumer with either nonzero isn't evicted (_evict_idle)
    in_flight: int = 0
    waiting: int = 0
    fetching: Future[None] | None = None
    # When the lease request currently in flight was sent (monotonic clock)
    fetching_since: float = 0.0
    queued: bool = False
    # Reservation to ask for on the next lease request: that of the most recent request that needed quota
    wanted_reserve: int = 1
    last_minute_end: float = 0.0
    # Usage over the last 60 seconds: estimated from two adjacent one-minute windows, weighted, so each consumer takes only three numbers
    usage_current: int = 0
    usage_previous: int = 0


UNMETERED = QuotaDecision(True)
QUOTA_UNAVAILABLE = QuotaDecision(False, error="quota_unavailable")
INSUFFICIENT_CREDITS = QuotaDecision(False, error="insufficient_credits")
SUSPENDED = QuotaDecision(False, error="consumer_suspended")


def minute_end_of(wall_time: float) -> float:
    return math.floor(wall_time / MINUTE_MS) * MINUTE_MS + MINUTE_MS


class QuotaManager:
    def __init__(
        self,
        clock: Clock,
        transport: Transport,
        warn: Callable[[str], None],
        on_unavailable: Callable[[], Literal["allow", "deny"]],
        lock: threading.RLock,
    ) -> None:
        self._clock = clock
        self._transport = transport
        self._warn = warn
        self._on_unavailable = on_unavailable
        # The engine's lock: leases come back on network threads and renewals go out on the timer thread, and both must hold it before changing state
        self.lock = lock
        self._consumers: dict[str, _ConsumerState] = {}
        # Every lease not yet reported as closed, including retired ones still waiting on in-flight requests
        self._leases: dict[str, LocalLease] = {}
        self._renew_queue: dict[int, _ConsumerState] = {}
        self._cancel_drain: Callable[[], None] | None = None
        self._last_drain_at = -math.inf
        self._outage_until = -math.inf
        self._stopped = False

    def reserve(self, consumer: str, kind: str, info: QuotaInfo, units: int, started_at: float) -> QuotaDecision | Future[QuotaDecision]:
        """Called when a request starts. Returns a Future only when it really has to wait for the server (first lease, next period's lease, leases used up concurrently)"""
        # Consumers billed for overage, or with no quota, don't get leases and don't need state
        if info.behavior in (None, "overage") and consumer not in self._consumers:
            return UNMETERED
        state = self._state_for(consumer, kind)
        self._observe(state, info.version, info.behavior)
        minute_end = minute_end_of(started_at)
        if minute_end > state.last_minute_end:
            state.last_minute_end = minute_end
        state.wanted_reserve = units
        decision = self._decide_locally(state, units, minute_end)
        if decision is not None:
            return decision
        if self._clock.monotonic() < self._outage_until:
            return self.when_unavailable()
        return self._wait_for_lease(state, units, minute_end)

    def when_unavailable(self) -> QuotaDecision:
        """When the control plane is unavailable, or the consumer ID can't be worked out (your own auth, hashing key not here yet): apply onQuotaUnavailable"""
        return QUOTA_UNAVAILABLE if self._on_unavailable() == "deny" else UNMETERED

    def settle(self, reservation: Reservation, actual_units: int) -> None:
        """Called when a request ends: refund what was over-reserved, charge any excess; for non-billable outcomes actual_units is 0 and everything is refunded.
        Actual usage is spread over the leases in reservation order, with anything beyond the reservation charged to the last one"""
        if reservation.settled:
            return
        reservation.settled = True
        state = reservation.owner
        if state is not None:
            state.in_flight -= 1
        remaining = actual_units
        for index, (lease, units) in enumerate(reservation.parts):
            used = remaining if index == len(reservation.parts) - 1 else min(units, remaining)
            remaining -= used
            lease.in_flight -= 1
            lease.unreported += used
            lease.available += units - used
            if lease.available < 0:
                lease.owner.debts.append(_Debt(-lease.available, lease, lease.period_end))
                lease.available = 0
        if state is not None and state.debts:
            self._absorb_debts(state)
            if state.debts:
                self._queue_renewal(state)

    def observe_record(self, consumer: str, version: int, behavior: QuotaBehavior | None, status: str) -> None:
        """Consumer details from sync: a version change retires old leases; status back to active lifts a suspension"""
        state = self._consumers.get(consumer)
        if state is None:
            return
        self._observe(state, version, behavior)
        if status == "active" and state.denial is not None and state.denial.reason == "suspended":
            state.denial = None

    # ---- Reporting (called by the meter when it builds a batch) ------------------------

    def collect_reports(self) -> LeaseReport:
        now = self._clock.monotonic()
        minute_end = minute_end_of(self._clock.now())
        report = LeaseReport()
        for lease in list(self._leases.values()):
            if not lease.retired and (now >= lease.expires_at or lease.period_end < minute_end or lease.version < lease.owner.version):
                self._retire(lease)
            if lease.unreported != 0:
                # Each entry's usage has to stay under the protocol cap (the server merges entries for the same lease ID within a batch)
                left = lease.unreported
                while left > 0:
                    report.entries.append({"lease_id": lease.id, "used": min(left, MAX_ROW_UNITS)})
                    left -= MAX_ROW_UNITS
                lease.unreported = 0
                lease.unacked += 1
                report.used.append(lease)
                for debt in lease.owner.debts:
                    if debt.lease is lease and not debt.reported:
                        debt.reported = True
                        report.debts.append(debt)
            elif lease.retired and not lease.close_sent and lease.in_flight == 0 and lease.unacked == 0:
                # Retired, every admitted request finished, all usage reported: this batch carries "lease closed", which the server settles on
                report.entries.append({"lease_id": lease.id, "used": 0, "closed": True})
                lease.close_sent = True
                report.closed.append(lease)
        return report

    def finish_report(self, report: LeaseReport) -> None:
        """The batch was confirmed, or given up on (recorded as a known loss). If given up, the usage is lost but we carry on:
        reporting "closed" and waiting for the server's 30-minute forced settlement release the same amount, just sooner or later"""
        for lease in report.used:
            lease.unacked -= 1
        for lease in report.closed:
            self._leases.pop(lease.id, None)
        # The batch with the debt usage was confirmed (or given up on): from now on the server counts it in what's available
        now = self._clock.monotonic()
        owners: dict[int, _ConsumerState] = {}
        for debt in report.debts:
            debt.acked_at = now
            owners[id(debt.lease.owner)] = debt.lease.owner
        for state in owners.values():
            self._absorb_debts(state)

    def forget_leases(self, lease_ids: list[str]) -> None:
        """Leases the server doesn't recognize (already force-settled, etc.): stop reporting them"""
        for lease_id in lease_ids:
            lease = self._leases.pop(lease_id, None)
            if lease is not None:
                self._retire(lease)

    def retire_all(self) -> None:
        """Before exit: retire everything, so the last two batches report usage and "closed" to the server"""
        for lease in list(self._leases.values()):
            self._retire(lease)

    def stop(self) -> None:
        self._stopped = True
        if self._cancel_drain is not None:
            self._cancel_drain()
            self._cancel_drain = None

    # ---- Local decisions ---------------------------------------------------------------

    def _decide_locally(self, state: _ConsumerState, units: int, minute_end: float) -> QuotaDecision | None:
        denial = state.denial
        if denial is not None and denial.reason == "suspended" and denial.version == state.version:
            self._recheck_later(state, denial)
            return SUSPENDED
        if state.behavior in (None, "overage") or state.unlimited_version == state.version:
            return UNMETERED
        self._drop_unusable_leases(state, minute_end)
        if self._can_reserve(state, units):
            return QuotaDecision(True, reservation=self._take(state, units))
        if (
            denial is not None
            and denial.reason == "exhausted"
            and denial.version == state.version
            and units >= denial.reserve
            and not (denial.period_end is not None and minute_end > denial.period_end)
        ):
            self._recheck_later(state, denial)
            if denial.behavior == "prepaid":
                return INSUFFICIENT_CREDITS
            retry_after = None if denial.period_end is None else max(1, math.ceil((denial.period_end - self._clock.now()) / 1000))
            return QuotaDecision(False, error="quota_exceeded", retry_after_seconds=retry_after)
        return None

    def _recheck_later(self, state: _ConsumerState, denial: _Denial) -> None:
        if self._clock.monotonic() - denial.checked_at >= DENIAL_RECHECK_MS:
            self._queue_renewal(state)

    def _drop_unusable_leases(self, state: _ConsumerState, minute_end: float) -> None:
        """Retire leases whose version is stale, whose local lifetime has run out, or whose period ends before the end of the request's minute (strictly)"""
        if not state.leases:
            return
        now = self._clock.monotonic()
        kept = []
        for lease in state.leases:
            if lease.version < state.version or now >= lease.expires_at or minute_end > lease.period_end:
                lease.retired = True
            else:
                kept.append(lease)
        state.leases = kept

    def _can_reserve(self, state: _ConsumerState, units: int) -> bool:
        """Debts paid off, and the leases held add up to enough for this reservation"""
        self._absorb_debts(state)
        if state.debts:
            return False
        return sum(lease.available for lease in state.leases) >= units

    def _absorb_debts(self, state: _ConsumerState) -> None:
        """Pay off debts with the leases held. A debt is only paid by leases requested while the server didn't know about it;
        once the server has confirmed it, with no earlier lease left to pay it and no earlier lease request in flight, it needn't be paid anymore"""
        if not state.debts:
            return
        remaining = []
        for debt in state.debts:
            for lease in state.leases:
                if debt.amount == 0:
                    break
                # Hard limits reset each period: excess in one period was counted in that period and doesn't take quota from later ones
                if lease.behavior == "stop" and lease.period_end > debt.period_end:
                    continue
                if debt.acked_at is not None and lease.requested_at >= debt.acked_at:
                    continue
                absorbed = min(debt.amount, lease.available)
                lease.available -= absorbed
                debt.amount -= absorbed
            if debt.amount == 0:
                continue
            if any(lease.behavior == "stop" and lease.period_end > debt.period_end for lease in state.leases):
                continue
            if debt.acked_at is not None:
                earlier_lease = any(lease.requested_at < debt.acked_at and lease.available > 0 for lease in state.leases)
                earlier_request = state.fetching is not None and state.fetching_since < debt.acked_at
                if not earlier_lease and not earlier_request:
                    continue
            remaining.append(debt)
        state.debts = remaining

    def _take(self, state: _ConsumerState, units: int) -> Reservation:
        """Gather from the oldest lease first; one request can span several"""
        parts: list[tuple[LocalLease, int]] = []
        left = units
        for lease in state.leases:
            if left == 0:
                break
            taken = min(lease.available, left)
            if taken <= 0:
                continue
            lease.available -= taken
            lease.in_flight += 1
            parts.append((lease, taken))
            left -= taken
        self._count_usage(state, units)
        state.in_flight += 1
        newest = state.leases[-1]
        available = sum(lease.available for lease in state.leases)
        aging = self._clock.monotonic() >= newest.expires_at - (LEASE_TTL_MS - RENEW_AFTER_MS)
        if available < newest.granted * RENEW_BELOW_FRACTION or aging:
            self._queue_renewal(state)
        return Reservation(parts, units, owner=state)

    def _wait_for_lease(self, state: _ConsumerState, units: int, minute_end: float) -> Future[QuotaDecision]:
        """No usable lease locally: get one synchronously (or wait for the renewal already in flight), 300 ms at most in total.
        Only a timeout or failed request means the control plane is unavailable and onQuotaUnavailable applies; if the server answered and it's still not enough, the quota is exhausted"""
        result: Future[QuotaDecision] = Future()
        deadline = self._clock.monotonic() + SYNC_WAIT_MS
        # This customer can't be evicted while waiting: the lease would land on discarded state, this request would reserve from it, and any excess debt would be lost too
        state.waiting += 1

        def done_waiting(_: Future[QuotaDecision]) -> None:
            state.waiting -= 1

        result.add_done_callback(done_waiting)

        def attempt(number: int) -> None:
            # A new lease may be used up by concurrent requests arriving at the same moment, so get one at most twice more within the same 300 ms
            if number == 3:
                settle(result, self._out_of_quota(state))
                return
            remaining = deadline - self._clock.monotonic()
            if remaining <= 0 or self._clock.monotonic() < self._outage_until:
                settle(result, self.when_unavailable())
                return
            state.wanted_reserve = units
            fetching = state.fetching or self._request_leases([state], remaining)

            def after(arrived: bool) -> None:
                if not arrived or self._clock.monotonic() < self._outage_until:
                    settle(result, self.when_unavailable())
                    return
                decision = self._decide_locally(state, units, minute_end)
                if decision is not None:
                    settle(result, decision)
                else:
                    attempt(number + 1)

            self._settles_within(fetching, remaining, after)

        attempt(0)
        return result

    def _out_of_quota(self, state: _ConsumerState) -> QuotaDecision:
        """The server issued a lease every time, but this request still doesn't fit locally: debt isn't paid off, or concurrent requests used up the new lease.
        That means not enough quota, so onQuotaUnavailable=allow must not let it through. A renewal will cover it soon, so tell the client to retry in 1 second"""
        if state.behavior == "prepaid":
            return INSUFFICIENT_CREDITS
        return QuotaDecision(False, error="quota_exceeded", retry_after_seconds=1)

    def _settles_within(self, future: Future[None], timeout_ms: float, callback: Callable[[bool], None]) -> None:
        """callback(True) if future completes within timeout_ms, otherwise callback(False); called once, under the lock"""
        fired = False

        def fire(arrived: bool) -> None:
            nonlocal fired
            with self.lock:
                if fired:
                    return
                fired = True
                cancel()
                callback(arrived)

        cancel = self._clock.set_timer(lambda: fire(False), timeout_ms)
        future.add_done_callback(lambda _: fire(True))

    # ---- Requesting leases -------------------------------------------------------------

    def _request_leases(self, states: list[_ConsumerState], timeout_ms: float) -> Future[None]:
        oldest_at = self._clock.now() - _MAX_AT_AGE_MS
        requests: list[dict[str, Any]] = []
        for state in states:
            item: dict[str, Any] = {
                "consumer": state.consumer,
                "consumer_kind": state.kind,
                "reserve": max(1, state.wanted_reserve),
                "recent_usage_60s": self._recent_usage(state),
                "quota_version": state.version,
            }
            # Include the end of the request's minute, so near a period boundary the lease is for the period this request belongs to
            if state.last_minute_end > oldest_at:
                item["at"] = iso_from_ms(state.last_minute_end)
            requests.append(item)
        sent_at = self._clock.monotonic()
        call: Future[None] = Future()
        # Mark them in flight before sending: a fake transport answers on the spot, running the callback immediately
        for state in states:
            state.fetching = call
            state.fetching_since = sent_at

        def on_done(completed: Future[dict[str, Any]]) -> None:
            with self.lock:
                error = error_of(completed)
                try:
                    if error is not None:
                        raise error
                    self._apply_results(states, requests, completed.result()["results"], sent_at)
                except Exception as failure:  # noqa: BLE001 - any failure is treated as the control plane being unavailable
                    self._outage_until = self._clock.monotonic() + LEASE_OUTAGE_MS
                    self._warn(f"UsageRail: lease request failed ({describe_error(failure)}), applying onQuotaUnavailable")
                finally:
                    for state in states:
                        if state.fetching is call:
                            state.fetching = None
                settle(call, None)

        self._transport.leases({"requests": requests}, timeout_ms).add_done_callback(on_done)
        return call

    def _apply_results(self, states: list[_ConsumerState], requests: list[dict[str, Any]], results: list[dict[str, Any]], sent_at: float) -> None:
        by_consumer = {state.consumer: state for state in states}
        reserve_of = {request["consumer"]: request["reserve"] for request in requests}
        now = self._clock.monotonic()
        for result in results:
            state = by_consumer.get(result["consumer"])
            if state is None:
                continue
            status = result["status"]
            if status == "granted":
                lease = result["lease"]
                self._observe(state, lease["quota_version"], lease["behavior"])
                self._add_lease(state, lease, sent_at)
                continue
            # When the server's version is newer, the server wins (protocol.ts); results older than what we hold are ignored
            self._observe(state, result["quota_version"], result["behavior"] if status == "exhausted" else _UNKNOWN)
            if result["quota_version"] < state.version:
                continue
            if status == "unlimited":
                state.unlimited_version = result["quota_version"]
                state.denial = None
            elif status == "exhausted":
                retry_after = result.get("retry_after")
                period_end = None if retry_after is None else self._clock.now() + retry_after * 1000
                state.denial = _Denial("exhausted", result["quota_version"], result["behavior"], reserve_of.get(result["consumer"], 1), period_end, now)
            else:
                # Suspended: block every request and retire the leases held, until a sync says it's active again (protocol.ts)
                for lease in state.leases:
                    lease.retired = True
                state.leases = []
                state.denial = _Denial("suspended", result["quota_version"], None, 1, None, now)

    def _add_lease(self, state: _ConsumerState, granted: dict[str, Any], requested_at: float) -> None:
        period_end = parse_iso_ms(granted["period_end"])
        lease = LocalLease(
            id=granted["lease_id"],
            owner=state,
            granted=granted["granted"],
            version=granted["quota_version"],
            behavior=granted["behavior"],
            period_end=period_end if period_end is not None else math.nan,
            requested_at=requested_at,
            expires_at=self._clock.monotonic() + LEASE_TTL_MS,
            available=granted["granted"],
        )
        self._leases[lease.id] = lease
        # An older version than we hold, or an unreadable period: don't use it, go straight to closing so the server gets it back quickly
        if lease.version < state.version or period_end is None:
            lease.retired = True
            return
        state.leases.append(lease)
        state.behavior = lease.behavior
        state.denial = None
        state.unlimited_version = None
        self._absorb_debts(state)

    def _observe(self, state: _ConsumerState, version: int, behavior: Any) -> None:
        if version > state.version:
            state.version = version
            was_active = bool(state.leases)
            # The quota version changed (plan change, new period, top-up, refund, grant expiry, manual adjustment): retire old leases at once and drop cached results
            for lease in state.leases:
                lease.retired = True
            state.leases = []
            state.denial = None
            state.unlimited_version = None
            if behavior is not _UNKNOWN:
                state.behavior = behavior
            # It had leases, so this consumer is active: get a new one in the background so the next request doesn't wait
            if was_active and state.behavior in ("stop", "prepaid"):
                self._queue_renewal(state)
        elif behavior is not _UNKNOWN and state.behavior is _UNKNOWN:
            state.behavior = behavior

    def _retire(self, lease: LocalLease) -> None:
        lease.retired = True
        held = lease.owner.leases
        if lease in held:
            held.remove(lease)

    # ---- Background renewal: renewals within the same second are batched into one request ----

    def _queue_renewal(self, state: _ConsumerState) -> None:
        if self._stopped or state.queued or state.fetching is not None or self._clock.monotonic() < self._outage_until:
            return
        state.queued = True
        self._renew_queue[id(state)] = state
        self._schedule_drain()

    def _schedule_drain(self) -> None:
        if self._cancel_drain is not None or not self._renew_queue or self._stopped:
            return
        # Less than 1 second since the last batched renewal: wait until a full second has passed, sending everything queued meanwhile together, at most once a second
        delay = max(0.0, self._last_drain_at + 1000 - self._clock.monotonic())
        self._cancel_drain = self._clock.set_timer(self._drain, delay)

    def _drain(self) -> None:
        with self.lock:
            self._cancel_drain = None
            self._last_drain_at = self._clock.monotonic()
            due: list[_ConsumerState] = []
            for key in list(self._renew_queue):
                state = self._renew_queue.pop(key)
                state.queued = False
                if state.fetching is None:
                    due.append(state)
                if len(due) == MAX_LEASE_REQUESTS_PER_CALL:
                    break
            if due:
                self._request_leases(due, RENEW_TIMEOUT_MS)
            self._schedule_drain()

    # ---- Consumer state ----------------------------------------------------------------

    def _state_for(self, consumer: str, kind: str) -> _ConsumerState:
        existing = self._consumers.pop(consumer, None)
        if existing is not None:
            # Put it back at the end: dict order is then least to most recently used
            self._consumers[consumer] = existing
            return existing
        state = _ConsumerState(consumer, kind, window_start=self._clock.monotonic())
        if len(self._consumers) >= MAX_QUOTA_CONSUMERS:
            self._evict_idle()
        self._consumers[consumer] = state
        return state

    def _evict_idle(self) -> None:
        """Evict from the least recently used until under the cap. Skip consumers with requests waiting for a lease or unsettled, or with unpaid debt: once evicted, leases coming back,
        usage beyond the reservation from requests that end later, and unpaid debt would all land on discarded state, and a new lease next time wouldn't know about them and would admit too much
        (breaking "once negative, admit nothing"). Look at no more than 64 per call; busy ones seen are moved to the end so the next call continues with unseen ones. If all are busy, evict nothing for now:
        the excess is evicted in batches by later new customers once those consumers go idle"""
        busy: list[str] = []
        idle: list[str] = []
        left = len(self._consumers)
        for consumer, state in self._consumers.items():
            if left < MAX_QUOTA_CONSUMERS or len(busy) + len(idle) == MAX_EVICTION_SCAN:
                break
            if state.in_flight > 0 or state.waiting > 0 or state.debts:
                busy.append(consumer)
            else:
                idle.append(consumer)
                left -= 1
        for consumer in busy:
            self._consumers[consumer] = self._consumers.pop(consumer)
        for consumer in idle:
            self.forget(consumer)

    def forget(self, consumer: str) -> None:
        """Drop a consumer's local state: its leases are retired as usual, the next batch reports them closed, and the next request asks the server again.
        Used in two places: evicted from memory (the eviction rule), and a sync says it's gone (missing from a full sync, e.g. deleted)"""
        state = self._consumers.pop(consumer, None)
        if state is None:
            return
        self._renew_queue.pop(id(state), None)
        for lease in state.leases:
            lease.retired = True
        state.leases = []

    def _roll_usage_window(self, state: _ConsumerState) -> None:
        elapsed = math.floor((self._clock.monotonic() - state.window_start) / MINUTE_MS)
        if elapsed < 1:
            return
        state.usage_previous = state.usage_current if elapsed == 1 else 0
        state.usage_current = 0
        state.window_start += elapsed * MINUTE_MS

    def _count_usage(self, state: _ConsumerState, units: int) -> None:
        self._roll_usage_window(state)
        state.usage_current += units

    def _recent_usage(self, state: _ConsumerState) -> int:
        self._roll_usage_window(state)
        previous_weight = 1 - (self._clock.monotonic() - state.window_start) / MINUTE_MS
        # Round half up like JavaScript's Math.round (Python's round uses banker's rounding)
        return math.floor(state.usage_current + state.usage_previous * previous_weight + 0.5)
