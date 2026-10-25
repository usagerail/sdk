import itertools
import time
import threading
from concurrent.futures import Future
from datetime import datetime, timezone

from helpers import FakeClock, FakeTransport

import usagerail.quota as quota_module
from usagerail._time import iso_from_ms
from usagerail.quota import (
    LEASE_OUTAGE_MS,
    LEASE_TTL_MS,
    SYNC_WAIT_MS,
    UNKNOWN_BEHAVIOR,
    UNMETERED,
    QuotaDecision,
    QuotaInfo,
    QuotaManager,
    Reservation,
)

STOP = QuotaInfo("stop", 1)
_lease_ids = itertools.count(1)


def setup(on_unavailable: str = "allow"):
    clock = FakeClock()
    transport = FakeTransport()
    quota = QuotaManager(clock, transport, lambda message: None, lambda: on_unavailable, threading.RLock())
    return clock, transport, quota


def utc(*parts) -> float:
    return datetime(*parts, tzinfo=timezone.utc).timestamp() * 1000


def grant(granted, period_end, behavior: str = "stop"):
    """Grants a lease of granted units to every consumer in the request"""

    def respond(request):
        results = []
        for item in request["requests"]:
            results.append(
                {
                    "consumer": item["consumer"],
                    "status": "granted",
                    "lease": {
                        "lease_id": str(next(_lease_ids)),
                        "consumer": item["consumer"],
                        "granted": granted if isinstance(granted, int) else granted(item["consumer"]),
                        "quota_version": item["quota_version"] or 1,
                        "behavior": behavior,
                        "period_end": iso_from_ms(period_end(item["consumer"])),
                    },
                }
            )
        return {"results": results}

    return respond


def in_an_hour(clock: FakeClock):
    return lambda consumer: clock.now() + 3_600_000


def allowed(decision) -> Reservation:
    assert not isinstance(decision, Future), "expected a local decision"
    assert decision.allowed and decision.reservation is not None
    return decision.reservation


def waited(decision) -> QuotaDecision:
    assert isinstance(decision, Future), "expected to wait for the server"
    return decision.result(0)


def test_overage_consumers_and_consumers_without_a_quota_never_take_a_lease():
    clock, transport, quota = setup()
    assert quota.reserve("cons_over", "id", QuotaInfo("overage", 1), 1, clock.now()) == UNMETERED
    assert quota.reserve("cons_none", "id", QuotaInfo(None, 1), 1, clock.now()) == UNMETERED
    assert transport.lease_calls == []


def test_the_first_request_waits_for_a_lease_and_later_requests_reserve_locally():
    clock, transport, quota = setup()
    transport.on_leases = grant(10, in_an_hour(clock))
    first = waited(quota.reserve("cons_1", "id", STOP, 1, clock.now()))
    assert first.allowed and first.reservation.units == 1
    allowed(quota.reserve("cons_1", "id", STOP, 1, clock.now()))
    assert len(transport.lease_calls) == 1
    # at is the end of the minute the request falls in
    assert transport.lease_calls[0]["requests"] == [
        {"consumer": "cons_1", "consumer_kind": "id", "reserve": 1, "recent_usage_60s": 0, "quota_version": 1, "at": "2026-10-20T12:01:00.000Z"}
    ]
    assert transport.timeouts == [SYNC_WAIT_MS]


def test_settling_refunds_unused_units_and_overspending_blocks_until_a_new_lease_covers_it():
    clock, transport, quota = setup()
    transport.on_leases = grant(10, in_an_hour(clock))
    big = waited(quota.reserve("cons_1", "id", STOP, 5, clock.now()))
    # Reserved 5, used 2: refund 3
    quota.settle(big.reservation, 2)
    # Non-billable outcome: full refund
    quota.settle(allowed(quota.reserve("cons_1", "id", STOP, 1, clock.now())), 0)
    # 8 available now: reserve 8, actually use 20 (more custom units than reserved), owing 12
    quota.settle(allowed(quota.reserve("cons_1", "id", STOP, 8, clock.now())), 20)
    transport.on_leases = grant(30, in_an_hour(clock))
    # Nothing is admitted until the debt is paid, so this waits for a new lease; the new lease of 30 pays off the 12 first
    assert waited(quota.reserve("cons_1", "id", STOP, 1, clock.now())).allowed
    # The first lease reports actual usage of 2 + 0 + 20
    assert [entry["used"] for entry in quota.collect_reports().entries] == [22]
    for _ in range(17):
        allowed(quota.reserve("cons_1", "id", STOP, 1, clock.now()))
    # 30 − 12 = 18: 1 used when the lease arrived, 17 more admitted, and the 19th waits for a renewal
    assert isinstance(quota.reserve("cons_1", "id", STOP, 1, clock.now()), Future)


def test_renewals_queued_within_the_same_second_go_out_as_one_request():
    clock, transport, quota = setup()
    transport.on_leases = grant(10, in_an_hour(clock))
    waited(quota.reserve("cons_a", "id", STOP, 1, clock.now()))
    waited(quota.reserve("cons_b", "id", STOP, 1, clock.now()))
    assert len(transport.lease_calls) == 2
    # Both used down below 30%
    for consumer in ("cons_a", "cons_b"):
        for _ in range(7):
            allowed(quota.reserve(consumer, "id", STOP, 1, clock.now()))
    clock.advance(0)
    assert len(transport.lease_calls) == 3
    assert sorted(item["consumer"] for item in transport.lease_calls[2]["requests"]) == ["cons_a", "cons_b"]


def test_a_lease_stops_being_used_60_seconds_after_it_arrived_on_the_monotonic_clock():
    clock, transport, quota = setup()
    transport.on_leases = grant(100, in_an_hour(clock))
    waited(quota.reserve("cons_1", "id", STOP, 1, clock.now()))
    # Moving the wall clock back has no effect
    clock.wall -= 3_600_000
    clock.mono += LEASE_TTL_MS - 1
    allowed(quota.reserve("cons_1", "id", STOP, 1, clock.now() + 3_600_000))
    clock.mono += 1
    assert isinstance(quota.reserve("cons_1", "id", STOP, 1, clock.now() + 3_600_000), Future)


def test_a_higher_quota_version_retires_held_leases_and_asks_for_a_new_one():
    clock, transport, quota = setup()
    transport.on_leases = grant(100, in_an_hour(clock))
    waited(quota.reserve("cons_1", "id", STOP, 1, clock.now()))
    quota.observe_record("cons_1", 2, "stop", "active")
    # It had a lease, so a renewal goes out in the background first
    clock.advance(0)
    assert len(transport.lease_calls) == 2
    assert transport.lease_calls[1]["requests"][0]["quota_version"] == 2
    allowed(quota.reserve("cons_1", "id", QuotaInfo("stop", 2), 1, clock.now()))


def test_d6_a_minute_ending_exactly_at_period_end_uses_the_lease():
    clock, transport, quota = setup()
    period_end = utc(2026, 10, 20, 12, 1, 0)
    transport.on_leases = grant(100, lambda consumer: period_end)
    # The request starts at 12:00:10; its minute [12:00, 12:01) ends exactly when the period ends, so it belongs to this period
    waited(quota.reserve("cons_1", "id", STOP, 1, clock.now()))
    allowed(quota.reserve("cons_1", "id", STOP, 1, utc(2026, 10, 20, 12, 0, 59, 999_000)))
    # A request starting at 12:01:00 belongs to the next period: this lease can't be used, so it asks for the next one with at = 12:02
    transport.on_leases = grant(100, lambda consumer: period_end + 3_600_000)
    assert waited(quota.reserve("cons_1", "id", STOP, 1, period_end)).allowed
    assert transport.lease_calls[-1]["requests"][0]["at"] == "2026-10-20T12:02:00.000Z"


def test_lease_closed_goes_out_only_after_in_flight_requests_end_and_their_usage_is_acknowledged():
    clock, transport, quota = setup()
    transport.on_leases = grant(100, in_an_hour(clock))
    reservation = waited(quota.reserve("cons_1", "id", STOP, 1, clock.now())).reservation
    lease_id = reservation.parts[0][0].id
    # The version changed and the lease is retired, but one request is still in flight
    quota.observe_record("cons_1", 2, "stop", "active")
    assert quota.collect_reports().entries == []
    quota.settle(reservation, 1)
    with_usage = quota.collect_reports()
    assert with_usage.entries == [{"lease_id": lease_id, "used": 1}]
    # The batch with its usage isn't confirmed yet, so don't report "closed" yet
    assert [entry for entry in quota.collect_reports().entries if entry["lease_id"] == lease_id] == []
    quota.finish_report(with_usage)
    closing = quota.collect_reports()
    assert [entry for entry in closing.entries if entry["lease_id"] == lease_id] == [{"lease_id": lease_id, "used": 0, "closed": True}]
    quota.finish_report(closing)
    assert [entry for entry in quota.collect_reports().entries if entry["lease_id"] == lease_id] == []


def test_exhausted_quotas_are_quota_exceeded_or_insufficient_credits():
    clock, transport, quota = setup()

    def respond(request):
        results = []
        for item in request["requests"]:
            if item["consumer"] == "cons_stop":
                results.append({"consumer": item["consumer"], "status": "exhausted", "behavior": "stop", "quota_version": 1, "retry_after": 120})
            else:
                results.append({"consumer": item["consumer"], "status": "exhausted", "behavior": "prepaid", "quota_version": 1, "retry_after": None})
        return {"results": results}

    transport.on_leases = respond
    assert waited(quota.reserve("cons_stop", "id", STOP, 1, clock.now())) == QuotaDecision(False, error="quota_exceeded", retry_after_seconds=120)
    assert waited(quota.reserve("cons_prepaid", "id", QuotaInfo("prepaid", 1), 1, clock.now())) == QuotaDecision(False, error="insufficient_credits")
    # The result is cached locally; not every request asks the server
    clock.wall += 30_000
    assert quota.reserve("cons_stop", "id", STOP, 1, clock.now()) == QuotaDecision(False, error="quota_exceeded", retry_after_seconds=90)
    assert len(transport.lease_calls) == 2


def test_a_suspended_consumer_is_blocked_until_sync_says_it_is_active_again():
    clock, transport, quota = setup()
    transport.on_leases = lambda request: {"results": [{"consumer": item["consumer"], "status": "suspended", "quota_version": 1} for item in request["requests"]]}
    info = QuotaInfo(UNKNOWN_BEHAVIOR, 0)
    assert waited(quota.reserve("a1b2", "external", info, 1, clock.now())) == QuotaDecision(False, error="consumer_suspended")
    assert quota.reserve("a1b2", "external", info, 1, clock.now()) == QuotaDecision(False, error="consumer_suspended")
    assert transport.lease_calls[0]["requests"][0]["consumer_kind"] == "external"
    # Sync says it's active again and billed for overage: let it through, no more leases needed
    quota.observe_record("a1b2", 1, "overage", "active")
    assert quota.reserve("a1b2", "external", info, 1, clock.now()) == UNMETERED
    assert len(transport.lease_calls) == 1


def test_when_leases_cannot_be_fetched_on_quota_unavailable_decides():
    for policy in ("allow", "deny"):
        clock, transport, quota = setup(policy)

        def refuse(request):
            raise ConnectionRefusedError("connection refused")

        transport.on_leases = refuse
        expected = UNMETERED if policy == "allow" else QuotaDecision(False, error="quota_unavailable")
        assert waited(quota.reserve("cons_1", "id", STOP, 1, clock.now())) == expected
        assert quota.reserve("cons_1", "id", STOP, 1, clock.now()) == expected
        assert len(transport.lease_calls) == 1
        clock.mono += LEASE_OUTAGE_MS
        assert isinstance(quota.reserve("cons_1", "id", STOP, 1, clock.now()), Future)


def test_a_request_waits_at_most_300_ms_for_a_lease():
    clock, transport, quota = setup()
    transport.on_leases = lambda request: Future()
    pending = quota.reserve("cons_1", "id", STOP, 1, clock.now())
    clock.advance(SYNC_WAIT_MS - 1)
    assert not pending.done()
    clock.advance(1)
    assert pending.result(0) == UNMETERED


def test_debt_the_servers_leases_cannot_cover_yet_blocks_instead_of_falling_back_to_allow():
    clock, transport, quota = setup("allow")
    transport.on_leases = grant(10, in_an_hour(clock))
    # Reserved 10, actually used 100 (more custom units than reserved): owing 90
    reservation = waited(quota.reserve("cons_1", "id", STOP, 10, clock.now())).reservation
    quota.settle(reservation, 100)
    # The server keeps answering normally with 10 at a time, all absorbed by the debt
    assert waited(quota.reserve("cons_1", "id", STOP, 1, clock.now())) == QuotaDecision(False, error="quota_exceeded", retry_after_seconds=1)
    assert len(transport.lease_calls) == 4


def test_leases_ask_for_the_reservation_the_request_needs():
    clock, transport, quota = setup()

    def respond(request):
        results = []
        for item in request["requests"]:
            if item["reserve"] > 10:
                results.append({"consumer": item["consumer"], "status": "exhausted", "behavior": "stop", "quota_version": 1, "retry_after": 60})
            else:
                lease = {
                    "lease_id": str(next(_lease_ids)),
                    "consumer": item["consumer"],
                    "granted": 10,
                    "quota_version": 1,
                    "behavior": "stop",
                    "period_end": iso_from_ms(clock.now() + 3_600_000),
                }
                results.append({"consumer": item["consumer"], "status": "granted", "lease": lease})
        return {"results": results}

    transport.on_leases = respond
    # 10 available: a request for 100 is told it's used up, a request for 1 still gets a lease
    assert waited(quota.reserve("cons_1", "id", STOP, 100, clock.now())) == QuotaDecision(False, error="quota_exceeded", retry_after_seconds=60)
    assert transport.lease_calls[0]["requests"][0]["reserve"] == 100
    assert waited(quota.reserve("cons_1", "id", STOP, 1, clock.now())).allowed
    assert transport.lease_calls[1]["requests"][0]["reserve"] == 1
    # Big requests are still blocked
    big = quota.reserve("cons_1", "id", STOP, 100, clock.now())
    assert not (big.result(0) if isinstance(big, Future) else big).allowed


def test_one_request_can_reserve_across_several_leases():
    clock, transport, quota = setup()
    transport.on_leases = grant(4, in_an_hour(clock))
    # First lease of 4: reserve 4, refund it all, 4 left
    first = waited(quota.reserve("cons_1", "id", STOP, 4, clock.now()))
    quota.settle(first.reservation, 0)
    # Needs 5: the 4 held aren't enough, so get another lease of 4 and combine them
    decision = waited(quota.reserve("cons_1", "id", STOP, 5, clock.now()))
    assert decision.allowed
    assert [units for _, units in decision.reservation.parts] == [4, 1]
    quota.settle(decision.reservation, 5)
    assert [entry["used"] for entry in quota.collect_reports().entries] == [4, 1]


def test_debt_the_server_has_acknowledged_is_not_charged_again_to_a_lease_requested_afterwards():
    clock, transport, quota = setup()
    transport.on_leases = grant(10, in_an_hour(clock))
    # Quota of 20: lease of 10, reserved 10, actually used 15, owing 5
    reservation = waited(quota.reserve("cons_1", "id", STOP, 10, clock.now())).reservation
    quota.settle(reservation, 15)
    # The batch carrying those 15 is confirmed by the server: what it has available is now 20 − 15 = 5
    report = quota.collect_reports()
    assert report.entries == [{"lease_id": reservation.parts[0][0].id, "used": 15}]
    clock.mono += 1
    quota.finish_report(report)
    # A lease requested after that (5) doesn't have to pay off the 5 again
    transport.on_leases = grant(5, in_an_hour(clock))
    assert waited(quota.reserve("cons_1", "id", STOP, 5, clock.now())).allowed


def test_lease_usage_above_the_per_entry_limit_is_reported_in_several_entries():
    clock, transport, quota = setup()
    transport.on_leases = grant(10, in_an_hour(clock))
    reservation = waited(quota.reserve("cons_1", "id", STOP, 1, clock.now())).reservation
    quota.settle(reservation, 20_000_000_005)
    assert [entry["used"] for entry in quota.collect_reports().entries] == [10_000_000_000, 10_000_000_000, 5]


def test_concurrent_requests_share_one_lease_call_and_are_answered_when_it_arrives():
    # A real transport answers on a network thread: concurrent requests for one consumer share one lease call, and each is decided from the local lease once it's back
    clock, transport, quota = setup()
    answer: Future = Future()
    transport.on_leases = lambda request: answer
    first = quota.reserve("cons_1", "id", STOP, 1, clock.now())
    second = quota.reserve("cons_1", "id", STOP, 1, clock.now())
    assert len(transport.lease_calls) == 1
    answer.set_result(grant(10, in_an_hour(clock))(transport.lease_calls[0]))
    assert first.result(0).allowed and second.result(0).allowed
    assert sum(units for _, units in first.result(0).reservation.parts + second.result(0).reservation.parts) == 2


# ---- Found in review (round four): consumers with unsettled requests or unpaid debt aren't evicted ----
def test_a_consumer_with_debt_or_unsettled_requests_is_not_evicted(monkeypatch):
    monkeypatch.setattr(quota_module, "MAX_QUOTA_CONSUMERS", 2)
    clock, transport, quota = setup()
    transport.on_leases = grant(10, in_an_hour(clock))
    # cons_1 reserved 10 and used 100: owing 90. cons_2 has a request still in progress
    debt = waited(quota.reserve("cons_1", "id", STOP, 10, clock.now())).reservation
    quota.settle(debt, 100)
    running = waited(quota.reserve("cons_2", "id", STOP, 10, clock.now())).reservation
    # Two more customers arrive: the idle one is evicted, not cons_1 or cons_2
    quota.settle(waited(quota.reserve("cons_3", "id", STOP, 1, clock.now())).reservation, 1)
    quota.settle(waited(quota.reserve("cons_4", "id", STOP, 1, clock.now())).reservation, 1)
    # cons_1's debt is still there: every lease of 10 the server sends goes to it, so it's still blocked
    assert not waited(quota.reserve("cons_1", "id", STOP, 1, clock.now())).allowed
    # cons_2's request ends late and uses too much: the debt lands on state still in memory, so the next request is still blocked
    quota.settle(running, 100)
    assert not waited(quota.reserve("cons_2", "id", STOP, 1, clock.now())).allowed


def test_a_consumer_whose_first_request_is_still_waiting_for_its_lease_is_not_evicted(monkeypatch):
    monkeypatch.setattr(quota_module, "MAX_QUOTA_CONSUMERS", 2)
    clock, transport, quota = setup()
    leases = grant(10, in_an_hour(clock))
    answer: Future = Future()
    transport.on_leases = lambda request: answer if request["requests"][0]["consumer"] == "cons_waiting" else leases(request)
    waiting = quota.reserve("cons_waiting", "id", STOP, 10, clock.now())
    assert isinstance(waiting, Future)
    # Two customers arrive while it waits
    for consumer in ("cons_a", "cons_b"):
        quota.settle(waited(quota.reserve(consumer, "id", STOP, 1, clock.now())).reservation, 1)
    answer.set_result(leases({"requests": [{"consumer": "cons_waiting", "reserve": 10, "quota_version": 1}]}))
    # The lease arrives, reserve 10, actually use 100: the debt lands on state still in memory, so the next request is still blocked
    quota.settle(waiting.result(0).reservation, 100)
    transport.on_leases = leases
    assert not waited(quota.reserve("cons_waiting", "id", STOP, 1, clock.now())).allowed


def test_after_a_burst_where_every_consumer_was_busy_the_cache_shrinks_back_to_its_limit(monkeypatch):
    monkeypatch.setattr(quota_module, "MAX_QUOTA_CONSUMERS", 2)
    clock, transport, quota = setup()
    transport.on_leases = grant(10, in_an_hour(clock))
    # 4 customers all have requests in progress at once: none can be evicted, so the cache runs over for now
    running = [waited(quota.reserve(f"cons_{index}", "id", STOP, 1, clock.now())).reservation for index in range(4)]
    for reservation in running:
        quota.settle(reservation, 1)
    # A new customer arrives once they've all finished: evict until under the cap, so the three least recently used (0, 1, 2) go and 3 stays
    waited(quota.reserve("cons_late", "id", STOP, 1, clock.now()))
    assert not isinstance(quota.reserve("cons_3", "id", STOP, 1, clock.now()), Future)
    assert isinstance(quota.reserve("cons_2", "id", STOP, 1, clock.now()), Future)


def test_while_every_cached_consumer_is_busy_a_new_consumer_costs_a_short_scan(monkeypatch):
    monkeypatch.setattr(quota_module, "MAX_QUOTA_CONSUMERS", 10_000)
    clock, transport, quota = setup()
    transport.on_leases = grant(10, in_an_hour(clock))
    for index in range(10_000):
        waited(quota.reserve(f"cons_{index}", "id", STOP, 1, clock.now()))
    # The table is full and all have requests in progress. Before, every new customer scanned and moved the whole table, about 0.4 seconds for 500 new customers on a 10,000 table
    started = time.perf_counter()
    for index in range(500):
        waited(quota.reserve(f"cons_new_{index}", "id", STOP, 1, clock.now()))
    elapsed = time.perf_counter() - started
    assert elapsed < 0.15, f"{elapsed * 1000:.0f} ms for 500 new consumers"

