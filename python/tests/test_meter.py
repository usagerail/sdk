import threading
from datetime import datetime, timezone

from helpers import FakeClock, FakeTransport, accepted

from usagerail.meter import (
    MAX_BACKOFF_MS,
    MAX_QUEUE_AGE_MS,
    MAX_ROUTES_PER_MINUTE,
    MAX_TABLE_KEYS,
    PAUSED_RETRY_MS,
    PENDING_MAX_AGE_MS,
    Meter,
    Usage,
    classify_status,
)
from usagerail.protocol import MAX_ROW_COUNT, MAX_ROW_UNITS
from usagerail.quota import LeaseReport
from usagerail.transport import TransportError

MINUTE = datetime(2026, 10, 20, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1000


class NoLeases:
    def __init__(self, entries=None):
        self.entries = entries or []

    def collect_reports(self):
        return LeaseReport(entries=list(self.entries))

    def finish_report(self, report):
        pass

    def forget_leases(self, lease_ids):
        pass


def setup(leases=None, hash=None):  # noqa: A002
    clock = FakeClock()
    transport = FakeTransport()
    resolver = {"hash": hash}
    meter = Meter(clock, transport, "inst-1", lambda message: None, leases or NoLeases(), lambda: resolver["hash"], threading.RLock())

    def set_hash(function):
        resolver["hash"] = function

    return clock, transport, meter, set_hash


def usage(**overrides) -> Usage:
    values = {
        "consumer": "cons_1",
        "kind": "id",
        "stripe_customer_id": None,
        "method": "GET",
        "route": "/v1/items/:id",
        "outcome": "2xx",
        "minute_start": MINUTE,
        "units": 1,
    }
    values.update(overrides)
    return Usage(**values)


def failing(status):
    def handler(batch):
        raise TransportError(f"POST /v1/ingest returned {status}", status)

    return handler


def test_outcomes_follow_prd_5_1():
    assert [classify_status(status) for status in (101, 200, 204, 301, 304, 400, 404, 429, 500, 503, 99, 600)] == [
        "ws_open", "2xx", "2xx", "3xx", "3xx", "4xx", "4xx", "4xx", "5xx", "5xx", "5xx", "5xx",
    ]  # fmt: skip


def test_usage_aggregates_by_consumer_endpoint_outcome_and_minute():
    _, transport, meter, _ = setup()
    meter.record(usage(units=3), False)
    meter.record(usage(units=4), False)
    meter.record(usage(outcome="4xx", units=0), False)
    meter.record(usage(minute_start=MINUTE + 60_000), False)
    meter.record(usage(consumer="", kind="anonymous"), False)
    meter.record_key_use("Key00001")
    meter.record_key_use("Key00001")
    meter.flush().result(0)
    assert len(transport.ingested) == 1
    batch = transport.ingested[0]
    assert batch["batch_id"] == "inst-1:1"
    assert batch["key_usage"] == [{"key_id": "Key00001", "count": 2}]
    row = {"consumer": "cons_1", "method": "GET", "route": "/v1/items/:id", "consumer_kind": "id"}
    assert batch["rows"] == [
        {**row, "result_type": "2xx", "minute": "2026-10-20T12:00:00.000Z", "count": 2, "units": 7},
        {**row, "result_type": "4xx", "minute": "2026-10-20T12:00:00.000Z", "count": 1, "units": 0},
        {**row, "result_type": "2xx", "minute": "2026-10-20T12:01:00.000Z", "count": 1, "units": 1},
        # Anonymous request: analytics only (protocol.ts)
        {**row, "consumer": "", "consumer_kind": "anonymous", "result_type": "2xx", "minute": "2026-10-20T12:00:00.000Z", "count": 1, "units": 1},
    ]


def test_a_table_that_reaches_5000_keys_is_reported_at_once():
    _, transport, meter, _ = setup()
    for index in range(MAX_TABLE_KEYS):
        meter.record(usage(consumer=f"cons_{index}"), False)
    assert len(transport.ingested) == 1
    assert len(transport.ingested[0]["rows"]) == MAX_TABLE_KEYS


def test_past_500_distinct_routes_in_a_minute_new_routes_are_reported_as_other():
    _, transport, meter, _ = setup()
    for index in range(MAX_ROUTES_PER_MINUTE + 1):
        meter.record(usage(route=f"/r/{index}"), False)
    meter.record(usage(route="/r/0"), False)
    # Counting starts over in the next minute
    meter.record(usage(route="/next-minute", minute_start=MINUTE + 60_000), False)
    meter.flush()
    routes = [row["route"] for row in transport.rows()]
    assert routes.count("_other") == 1
    assert "/r/499" in routes
    assert "/r/500" not in routes
    assert "/next-minute" in routes


def test_a_failed_batch_is_retried_with_the_same_batch_id_after_a_backoff():
    clock, transport, meter, _ = setup()
    failures = [1]

    def ingest(batch):
        if failures[0] > 0:
            failures[0] -= 1
            raise TransportError("POST /v1/ingest returned 503", 503)
        return accepted()

    transport.on_ingest = ingest
    meter.record(usage(), False)
    meter.flush()
    assert len(transport.ingested) == 1
    clock.advance(999)
    assert len(transport.ingested) == 1
    clock.advance(1)
    assert [batch["batch_id"] for batch in transport.ingested] == ["inst-1:1", "inst-1:1"]


def test_413_splits_a_batch_billing_rows_first_then_lease_key_and_loss_data():
    _, transport, meter, _ = setup(leases=NoLeases([{"lease_id": "7", "used": 3}]))
    first = [True]

    def ingest(batch):
        if first[0]:
            first[0] = False
            raise TransportError("POST /v1/ingest returned 413", 413)
        return accepted()

    transport.on_ingest = ingest
    for index in range(4):
        meter.record(usage(consumer=f"cons_{index}"), False)
    meter.record_key_use("Key00001")
    meter.flush()
    original, *parts = transport.ingested
    assert original["batch_id"] == "inst-1:1"
    # Sub-batches from a split each get a new sequence number
    assert [batch["batch_id"] for batch in parts] == ["inst-1:2", "inst-1:3", "inst-1:4"]
    assert [len(batch["rows"]) for batch in parts] == [2, 2, 0]
    assert [len(batch["lease_used"]) + len(batch["key_usage"]) for batch in parts] == [0, 0, 2]


def test_batches_older_than_10_minutes_are_given_up_and_counted_as_known_loss():
    clock, transport, meter, _ = setup()
    transport.on_ingest = failing(None)
    meter.record(usage(consumer="cons_lost", units=5), False)
    meter.record(usage(consumer="cons_lost", outcome="4xx", units=0), False)
    meter.flush()
    clock.advance(MAX_QUEUE_AGE_MS + 1)
    transport.on_ingest = lambda batch: accepted()
    # The first flush gives up on that batch, the second puts the loss in a new batch; if the backoff hasn't passed yet, it waits
    meter.flush()
    meter.flush()
    clock.advance(MAX_BACKOFF_MS)
    last = transport.ingested[-1]
    assert last["batch_id"] != "inst-1:1"
    # Only billable outcomes count as losses
    assert last["dropped"] == [{"consumer": "cons_lost", "billable_requests": 1, "units": 5}]


def test_rows_the_server_rejects_become_known_loss_in_the_next_batch():
    _, transport, meter, _ = setup()
    transport.on_ingest = lambda batch: {**accepted(), "rejected_rows": [{"index": 0, "reason": "unknown_consumer"}] if batch["rows"] else []}
    meter.record(usage(consumer="cons_gone", units=2), False)
    meter.flush()
    meter.flush()
    assert transport.ingested[1]["dropped"] == [{"consumer": "cons_gone", "billable_requests": 1, "units": 2}]


def test_a_batch_the_server_cannot_parse_is_dropped_and_counted_as_unattributed_loss():
    _, transport, meter, _ = setup()

    def ingest(batch):
        if batch["rows"]:
            raise TransportError("POST /v1/ingest returned 400", 400)
        return accepted()

    transport.on_ingest = ingest
    meter.record(usage(consumer="cons_bad", units=9), False)
    meter.flush()
    meter.flush()
    # A 400 may be caused by the consumer ID itself, so don't record by consumer, or the next batch would carry the same ID and be rejected again
    assert transport.ingested[1]["dropped"] == []
    assert transport.ingested[1]["unattributed_dropped"] == {"billable_requests": 1, "units": 9}


def test_a_401_pauses_reporting_for_10_minutes():
    clock, transport, meter, _ = setup()
    transport.on_ingest = failing(401)
    meter.record(usage(), False)
    meter.flush()
    clock.advance(PAUSED_RETRY_MS - 1)
    assert len(transport.ingested) == 1
    clock.advance(1)
    assert len(transport.ingested) == 2


def test_raw_external_ids_wait_for_the_hash_key_then_go_out_hashed_or_become_unattributed_loss():
    _, transport, meter, set_hash = setup()
    meter.record(usage(consumer="user-42", kind="external", units=2), True)
    meter.flush()
    assert transport.ingested == []
    set_hash(lambda raw: f"hashed:{raw}")
    meter.record(usage(consumer="hashed:user-42", kind="external", units=1), False)
    meter.flush()
    assert [(row["consumer"], row["consumer_kind"], row["count"], row["units"]) for row in transport.rows()] == [("hashed:user-42", "external", 2, 3)]

    late_clock, late_transport, late_meter, _ = setup()
    late_meter.record(usage(consumer="user-7", kind="external", units=4), True)
    late_clock.advance(PENDING_MAX_AGE_MS + 1)
    late_meter.record_key_use("Key00001")
    late_meter.flush()
    batch = late_transport.ingested[-1]
    assert batch["rows"] == []
    assert batch["unattributed_dropped"] == {"billable_requests": 1, "units": 4}


def test_an_aggregate_about_to_pass_a_rows_limits_is_sealed_and_a_new_row_started():
    _, transport, meter, _ = setup()
    # 10,000,000 units per request: 1,000 of them hit the row cap of 10,000,000,000 exactly, the 1,001st starts a new row
    for _ in range(1_001):
        meter.record(usage(units=10_000_000), False)
    meter.flush()
    assert [(row["count"], row["units"]) for row in transport.rows()] == [(1_000, 10_000_000_000), (1, 10_000_000)]


def test_a_transport_that_answers_later_keeps_batches_in_order():
    # A real transport answers on a network thread: the next batch isn't sent until this one is confirmed
    from concurrent.futures import Future

    _, transport, meter, _ = setup()
    answers: list[Future] = []

    def ingest(batch):
        answer: Future = Future()
        answers.append(answer)
        return answer

    transport.on_ingest = ingest
    meter.record(usage(consumer="a"), False)
    first = meter.flush()
    meter.record(usage(consumer="b"), False)
    second = meter.flush()
    assert first is second
    assert len(transport.ingested) == 1
    answers[0].set_result(accepted())
    assert len(transport.ingested) == 2
    assert not first.done()
    answers[1].set_result(accepted())
    assert first.done()


# ---- Found in review (round four): losses split by the per-entry cap, and what doesn't fit in one batch goes in another ----
def server_checks_dropped(then):
    # Server validation: each loss entry has at most 1 billion billable requests and 10 billion units, at most 1,000 entries a batch, otherwise the whole batch gets a 400
    def ingest(batch):
        entries = batch["dropped"]
        if len(entries) > 1_000 or any(entry["billable_requests"] > MAX_ROW_COUNT * 100 or entry["units"] > MAX_ROW_UNITS for entry in entries):
            raise TransportError("POST /v1/ingest returned 400", 400)
        return then(batch)

    return ingest


def test_known_loss_above_the_per_entry_limits_is_split_so_the_batch_carrying_it_is_still_accepted():
    _, transport, meter, _ = setup()
    rejected = {"done": False}

    def first_rows_rejected(batch):
        if batch["rows"] and not rejected["done"]:
            rejected["done"] = True
            return {**accepted(), "rejected_rows": [{"index": index, "reason": "unknown_consumer"} for index in range(len(batch["rows"]))]}
        return accepted()

    transport.on_ingest = server_checks_dropped(first_rows_rejected)
    # 1,001 requests of 10 million units each, rejected by the server: 10.01 billion units of losses in total, over the per-entry cap
    for _ in range(1001):
        meter.record(usage(consumer="cons_big", units=10_000_000), False)
    meter.flush()
    # The batch carrying this loss also has valid usage, which must not be rejected along with it
    meter.record(usage(consumer="cons_ok", units=3), False)
    meter.flush()
    recovery = transport.ingested[-1]
    assert [(row["consumer"], row["units"]) for row in recovery["rows"]] == [("cons_ok", 3)]
    assert recovery["dropped"] == [
        {"consumer": "cons_big", "billable_requests": 1001, "units": 10_000_000_000},
        {"consumer": "cons_big", "billable_requests": 0, "units": 10_000_000},
    ]


def test_known_loss_that_needs_more_than_1000_entries_goes_out_in_several_batches():
    _, transport, meter, _ = setup()
    rejected = {"done": False}

    def first_rows_rejected(batch):
        if batch["rows"] and not rejected["done"]:
            rejected["done"] = True
            return {**accepted(), "rejected_rows": [{"index": index, "reason": "unknown_consumer"} for index in range(len(batch["rows"]))]}
        return accepted()

    transport.on_ingest = server_checks_dropped(first_rows_rejected)
    # 1,000 customers each lost 15 billion units: each splits into two entries, 2,000 in total
    for index in range(1000):
        for _ in range(2):
            meter.record(usage(consumer=f"cons_{index}", units=7_500_000_000), False)
    meter.flush()
    meter.flush()
    batches = [batch for batch in transport.ingested[1:] if batch["dropped"]]
    assert [len(batch["dropped"]) for batch in batches] == [1000, 1000]
    assert sum(entry["units"] for batch in batches for entry in batch["dropped"]) == 1000 * 15_000_000_000



def test_a_413_on_one_key_usage_and_one_lease_entry_splits_them_instead_of_resending_the_same_batch():
    leases = NoLeases([{"lease_id": "lease_1", "used": 3}])
    _, transport, meter, _ = setup(leases=leases)
    sizes = []

    def ingest(batch):
        entries = len(batch["key_usage"]) + len(batch["lease_used"]) + len(batch["dropped"])
        sizes.append(entries)
        # More than one metadata entry is too big
        if entries > 1:
            raise TransportError("POST /v1/ingest returned 413", 413)
        return accepted()

    transport.on_ingest = ingest
    meter.record_key_use("key_1")
    meter.flush()
    meter.flush()
    # After the 413 it splits into two batches of one entry each (the batch after that is one more entry from the fake lease, ignored here)
    assert sizes[:3] == [2, 1, 1]
