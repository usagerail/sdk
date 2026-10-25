import hashlib
import hmac
import os
import re
import sys
import threading
from concurrent.futures import Future
from pathlib import Path

import pytest
from helpers import FakeClock, FakeTransport, SilentLogger, app_settings, consumer_record, key_record, sha256, sync_page

import usagerail.engine as engine_module
from usagerail import Pending, Rejection, create_engine, read_api_key, rejection_body
from usagerail._time import iso_from_ms
from usagerail.key_format import generate_api_key
from usagerail.transport import SDK_VERSION


def engine_with(**options):
    clock = options.pop("clock", None) or FakeClock()
    transport = options.pop("transport", None) or FakeTransport()
    settings = {
        "app_key": "urk_test_engine",
        "auth": "api-key",
        "clock": clock,
        "transport": transport,
        "background_loops": False,
        "flush_on_exit": False,
        "logger": SilentLogger(),
        "instance_id": "inst-1",
    }
    settings.update(options)
    return clock, transport, create_engine(**settings)


def admitted(decision):
    assert not isinstance(decision, Pending), "expected a decision without waiting for the network"
    assert decision.allowed, f"expected an admission, got {decision!r}"
    return decision.handle


def rejected(decision) -> Rejection:
    assert not isinstance(decision, Pending)
    assert not decision.allowed
    return decision


def waited(decision):
    assert isinstance(decision, Pending), "expected to wait for the server"
    return decision.result(0)


def lease_grant(clock, lease_id: str, granted: int, version: int = 1):
    def respond(request):
        return {
            "results": [
                {
                    "consumer": item["consumer"],
                    "status": "granted",
                    "lease": {
                        "lease_id": lease_id,
                        "consumer": item["consumer"],
                        "granted": granted,
                        "quota_version": version,
                        "behavior": "stop",
                        "period_end": iso_from_ms(clock.now() + 3_600_000),
                    },
                }
                for item in request["requests"]
            ]
        }

    return respond


def test_the_sdk_version_sent_with_each_batch_matches_pyproject():
    # tomllib needs 3.11 and the SDK supports 3.10, so just find the line
    version = re.search(r'^version = "([^"]+)"$', (Path(__file__).parent.parent / "pyproject.toml").read_text(), re.MULTILINE)
    assert version is not None and SDK_VERSION == version.group(1)


def test_read_api_key_takes_a_bearer_token_or_x_api_key():
    assert read_api_key("Bearer abc", None) == "abc"
    assert read_api_key("bearer\tabc  ", None) == "abc"
    assert read_api_key("Bearer a b", "fallback") == "fallback"
    assert read_api_key("Basic abc", None) is None
    assert read_api_key(None, "  xyz ") == "xyz"
    assert read_api_key(None, "   ") is None


def test_with_our_keys_a_synced_key_is_checked_locally_and_metered_under_its_consumer():
    key = generate_api_key("acme", "test")
    _, transport, engine = engine_with()
    transport.on_sync = lambda cursor: sync_page(
        app=app_settings(routes=[{"method": "POST", "route": "/v1/images", "weight": 10, "reserve": 10}]),
        keys=[key_record(key.key_id, sha256(key.key), consumer="cons_1")],
        consumers=[consumer_record(consumer="cons_1")],
    )
    engine.sync()
    get = admitted(engine.begin(method="get", path="/v1/items/42", api_key=key.key))
    engine.end(get, status=200, route="/v1/items/{item_id}")
    # Route weight: this route counts 10 per request
    image = admitted(engine.begin(method="POST", path="/v1/images", api_key=key.key))
    # In an app metering requests, track() doesn't count. Billing only trusts the template passed in, here at the end
    image.track(99)
    engine.end(image, status=201, route="/v1/images")
    failed = admitted(engine.begin(method="GET", path="/v1/items/43", api_key=key.key))
    engine.end(failed, status=404, route="/v1/items/{item_id}")
    engine.flush()
    assert [(row["consumer"], row["consumer_kind"], row["method"], row["route"], row["result_type"], row["count"], row["units"]) for row in transport.rows()] == [
        ("cons_1", "id", "GET", "/v1/items/{item_id}", "2xx", 1, 1),
        ("cons_1", "id", "POST", "/v1/images", "2xx", 1, 10),
        ("cons_1", "id", "GET", "/v1/items/{item_id}", "4xx", 1, 0),
    ]
    assert transport.ingested[0]["key_usage"] == [{"key_id": key.key_id, "count": 3}]
    assert transport.verify_calls == []


def test_sdk_rejections_are_recorded_and_never_billed():
    good = generate_api_key("acme", "test")
    suspended = generate_api_key("acme", "test")
    _, transport, engine = engine_with()
    transport.on_sync = lambda cursor: sync_page(
        keys=[
            key_record(good.key_id, sha256(good.key), consumer="cons_limited"),
            key_record(suspended.key_id, sha256(suspended.key), consumer="cons_suspended"),
        ],
        consumers=[
            consumer_record(consumer="cons_limited", rate_limits={"per_second": 1, "per_minute": None}),
            consumer_record(consumer="cons_suspended", status="suspended"),
        ],
    )
    engine.sync()
    invalid = rejected(engine.begin(method="GET", path="/x", api_key="nope"))
    assert (invalid.status, invalid.error) == (401, "invalid_api_key")
    assert rejection_body(invalid) == {"error": "invalid_api_key"}
    blocked = rejected(engine.begin(method="GET", path="/x", api_key=suspended.key))
    assert (blocked.status, blocked.error) == (403, "consumer_suspended")
    engine.end(admitted(engine.begin(method="GET", path="/x", api_key=good.key)), status=200)
    limited = rejected(engine.begin(method="GET", path="/x", api_key=good.key))
    assert (limited.status, limited.error, limited.headers["Retry-After"]) == (429, "rate_limited", "1")
    assert limited.headers["RateLimit-Policy"]
    assert rejection_body(limited) == {"error": "rate_limited", "retry_after": 1}
    engine.flush()
    assert [(row["consumer"], row["consumer_kind"], row["result_type"], row["units"]) for row in transport.rows()] == [
        ("", "anonymous", "sdk_401", 0),
        ("cons_suspended", "id", "sdk_403", 0),
        ("cons_limited", "id", "2xx", 1),
        ("cons_limited", "id", "sdk_429", 0),
    ]


def test_stop_at_limit_consumers_reserve_from_a_lease_and_the_lease_usage_goes_out_with_the_next_batch():
    key = generate_api_key("acme", "test")
    clock, transport, engine = engine_with()
    transport.on_sync = lambda cursor: sync_page(
        app=app_settings(unit_mode="units"),
        keys=[key_record(key.key_id, sha256(key.key), consumer="cons_stop", quota_behavior="stop", quota_version=4)],
        consumers=[consumer_record(consumer="cons_stop", quota_behavior="stop", quota_version=4)],
    )
    transport.on_leases = lease_grant(clock, "11", 50, version=4)
    engine.sync()
    decision = waited(engine.begin(method="POST", path="/v1/generate", api_key=key.key))
    assert decision.allowed
    # Metering units: track() wins
    decision.handle.track(25)
    engine.end(decision.handle, status=200, route="/v1/generate")
    engine.flush()
    batch = transport.ingested[0]
    assert batch["lease_used"] == [{"lease_id": "11", "used": 25}]
    assert [row["units"] for row in batch["rows"]] == [25]
    assert transport.lease_calls[0]["requests"][0]["quota_version"] == 4


def test_with_your_own_auth_raw_ids_are_held_until_the_hash_key_arrives():
    _, transport, engine = engine_with(auth="custom", consumer=lambda context: context.get("user"))
    # Not synced yet: don't know whether to hash, so let it through and hold on to it
    engine.end(admitted(engine.begin(method="GET", path="/x", context={"user": "user-1"})), status=200, route="/x")
    engine.end(admitted(engine.begin(method="GET", path="/x", context={})), status=200, route="/x")
    transport.on_sync = lambda cursor: sync_page(app=app_settings(hash_key="secret-hash-key"))
    engine.sync()
    # The sync has no record of this consumer: the first time this instance sees it, it gets a lease synchronously, and the server says it's unlimited
    decision = waited(engine.begin(method="GET", path="/x", context={"user": "user-1"}))
    assert decision.allowed
    engine.end(decision.handle, status=200, route="/x")
    # Cached as "unlimited" from then on, no more waiting
    engine.end(admitted(engine.begin(method="GET", path="/x", context={"user": "user-1"})), status=200, route="/x")
    engine.flush()
    hashed = hmac.new(b"secret-hash-key", b"user-1", hashlib.sha256).hexdigest()
    assert sorted((row["consumer"], row["consumer_kind"], row["count"]) for row in transport.rows()) == [("", "anonymous", 1), (hashed, "external", 3)]


def test_consumer_may_return_a_mapping_and_a_bad_id_counts_as_anonymous():
    results = iter([{"id": "user-9", "stripe_customer_id": "cus_9"}, {"id": "x" * 300}, 42])
    _, transport, engine = engine_with(auth="custom", consumer=lambda context: next(results))
    transport.on_sync = lambda cursor: sync_page(app=app_settings(hash_key=None))
    engine.sync()
    for _ in range(3):
        decision = engine.begin(method="GET", path="/x", context={})
        decision = decision.result(0) if isinstance(decision, Pending) else decision
        engine.end(decision.handle, status=200, route="/x")
    engine.flush()
    assert [(row["consumer"], row.get("stripe_customer_id")) for row in transport.rows()] == [("user-9", "cus_9"), ("", None)]
    assert transport.rows()[1]["count"] == 2


def test_with_your_own_auth_synced_records_are_matched_by_external_id():
    def hashed(value: str) -> str:
        return hmac.new(b"k", value.encode(), hashlib.sha256).hexdigest()

    _, transport, engine = engine_with(auth="custom", consumer=lambda context: context.get("user") or "")
    transport.on_sync = lambda cursor: sync_page(
        app=app_settings(hash_key="k"),
        consumers=[
            consumer_record(consumer="cons_a", external_id=hashed("alice"), rate_limits={"per_second": 1, "per_minute": None}),
            consumer_record(consumer="cons_b", external_id=hashed("bob"), status="suspended"),
        ],
    )
    engine.sync()
    admitted(engine.begin(method="GET", path="/x", context={"user": "alice"}))
    assert rejected(engine.begin(method="GET", path="/x", context={"user": "alice"})).error == "rate_limited"
    assert rejected(engine.begin(method="GET", path="/x", context={"user": "bob"})).error == "consumer_suspended"


def test_a_consumer_function_that_raises_counts_the_request_as_anonymous():
    def broken(context):
        raise RuntimeError("no user")

    _, transport, engine = engine_with(auth="custom", consumer=broken)
    engine.end(admitted(engine.begin(method="GET", path="/x", context={})), status=200, route="/x")
    engine.flush()
    assert [row["consumer_kind"] for row in transport.rows()] == ["anonymous"]


@pytest.mark.parametrize("policy", ["deny", "allow"])
def test_a_new_key_that_cannot_be_verified_follows_on_key_store_unavailable(policy):
    _, transport, engine = engine_with(on_key_store_unavailable=policy)

    def refuse(request):
        raise ConnectionRefusedError("connection refused")

    transport.on_verify = refuse
    decision = waited(engine.begin(method="GET", path="/x", api_key=generate_api_key("acme", "test").key))
    if policy == "deny":
        assert (decision.allowed, decision.status, decision.error) == (False, 503, "auth_unavailable")
    else:
        assert decision.allowed


def test_close_reports_what_is_left_including_the_final_lease_closed():
    key = generate_api_key("acme", "test")
    clock, transport, engine = engine_with()
    transport.on_sync = lambda cursor: sync_page(keys=[key_record(key.key_id, sha256(key.key), consumer="cons_stop", quota_behavior="stop", quota_version=1)])
    transport.on_leases = lease_grant(clock, "21", 10)
    engine.sync()
    decision = waited(engine.begin(method="GET", path="/x", api_key=key.key))
    engine.end(decision.handle, status=200)
    engine.close().result(0)
    assert [batch["lease_used"] for batch in transport.ingested] == [[{"lease_id": "21", "used": 1}], [{"lease_id": "21", "used": 0, "closed": True}]]


def test_a_consumer_that_disappears_from_a_full_sync_is_asked_about_again():
    _, transport, engine = engine_with(auth="custom", consumer=lambda context: context.get("user") or "")
    hashed = hmac.new(b"k", b"alice", hashlib.sha256).hexdigest()
    transport.on_sync = lambda cursor: sync_page(app=app_settings(hash_key="k"))
    engine.sync()
    # First sighting: the server says unlimited, and that's cached
    assert waited(engine.begin(method="GET", path="/x", context={"user": "alice"})).allowed
    # The seller suspends it: an incremental sync brings the suspended record
    transport.on_sync = lambda cursor: sync_page(full=False, app=app_settings(hash_key="k"), consumers=[consumer_record(consumer="cons_a", external_id=hashed, status="suspended")])
    engine.sync()
    assert rejected(engine.begin(method="GET", path="/x", context={"user": "alice"})).error == "consumer_suspended"
    # Then deletes it: it's gone from the full sync. The cached "unlimited" can't be used anymore; ask the server again
    transport.on_sync = lambda cursor: sync_page(app=app_settings(hash_key="k"))
    transport.on_leases = lambda request: {"results": [{"consumer": item["consumer"], "status": "suspended", "quota_version": 2} for item in request["requests"]]}
    engine.sync()
    decision = waited(engine.begin(method="GET", path="/x", context={"user": "alice"}))
    assert (decision.allowed, decision.error) == (False, "consumer_suspended")


def test_drain_reports_every_second_while_the_app_is_shutting_down():
    clock, transport, engine = engine_with(auth="custom", consumer=lambda context: None)
    engine.end(admitted(engine.begin(method="GET", path="/x", context={})), status=200, route="/x")
    engine.drain()
    assert len(transport.ingested) == 1
    # Another request finished during the drain: reported within 1 second
    engine.end(admitted(engine.begin(method="GET", path="/x", context={})), status=200, route="/x")
    clock.advance(1_000)
    assert len(transport.ingested) == 2
    engine.close()


def test_stream_started_counts_a_stream_once_and_adds_its_units_when_it_ends():
    _, transport, engine = engine_with(auth="custom", consumer=lambda context: "user-1")
    transport.on_sync = lambda cursor: sync_page(app=app_settings(unit_mode="units"))
    engine.sync()
    decision = waited(engine.begin(method="GET", path="/v1/chat", context={}))
    engine.stream_started(decision.handle, "/v1/chat")
    engine.flush()
    assert [(row["result_type"], row["count"], row["units"]) for row in transport.rows()] == [("ws_open", 1, 0)]
    decision.handle.track(42)
    engine.end(decision.handle, status=200, route="/v1/chat")
    engine.flush()
    assert [(row["result_type"], row["count"], row["units"]) for row in transport.rows()[1:]] == [("ws_open", 0, 42)]


def test_until_the_first_sync_arrives_requests_wait_for_it_at_most_one_second():
    key = generate_api_key("acme", "test")
    clock = FakeClock()
    transport = FakeTransport()
    first_sync: Future = Future()
    transport.on_sync = lambda cursor: first_sync
    _, _, engine = engine_with(clock=clock, transport=transport, background_loops=True)
    pending = engine.begin(method="GET", path="/x", api_key=key.key)
    assert isinstance(pending, Pending) and not pending.future.done()
    # The first sync arrived: judged locally from the snapshot, no remote verification
    first_sync.set_result(sync_page(keys=[key_record(key.key_id, sha256(key.key))]))
    assert pending.result(0).allowed
    assert transport.verify_calls == []
    assert not isinstance(engine.begin(method="GET", path="/x", api_key=key.key), Pending)

    # Sync never answers: after a full second, stop waiting and fall back to remote verification
    slow_clock = FakeClock()
    slow = FakeTransport()
    slow.on_sync = lambda cursor: Future()
    _, _, slow_engine = engine_with(clock=slow_clock, transport=slow, background_loops=True)
    waiting = slow_engine.begin(method="GET", path="/x", api_key=key.key)
    slow_clock.advance(999)
    assert not waiting.future.done()
    slow_clock.advance(1)
    assert waiting.result(0).error == "invalid_api_key"
    assert len(slow.verify_calls) == 1


def test_a_saved_snapshot_is_used_at_startup_without_the_hash_key():
    key = generate_api_key("acme", "test")
    clock = FakeClock()

    class Store:
        def __init__(self):
            self.saved = []

        def load(self):
            return {
                "saved_at": iso_from_ms(clock.now() - 60_000),
                "cursor": "c-saved",
                "app": {k: v for k, v in app_settings(routes=[{"method": "GET", "route": "/x", "weight": 3, "reserve": 3}]).items() if k != "hash_key"},
                "keys": [key_record(key.key_id, sha256(key.key), consumer="cons_saved")],
                "consumers": [consumer_record(consumer="cons_saved")],
            }

        def save(self, snapshot):
            self.saved.append(snapshot)

    transport = FakeTransport()
    transport.on_sync = lambda cursor: Future()
    _, _, engine = engine_with(clock=clock, transport=transport, snapshot_store=Store(), background_loops=True)
    # The snapshot counts as fresh: no waiting for the first sync, judged locally from the snapshot
    handle = admitted(engine.begin(method="GET", path="/x", api_key=key.key))
    # Billing only trusts the template passed in
    engine.end(handle, status=200, route="/x")
    engine.flush()
    assert [(row["consumer"], row["units"]) for row in transport.rows()] == [("cons_saved", 3)]
    assert transport.sync_calls == ["c-saved"]


def test_after_a_sync_the_snapshot_is_saved_in_the_background():
    key = generate_api_key("acme", "test")
    saved = threading.Event()
    received = []

    class Store:
        def load(self):
            return None

        def save(self, snapshot):
            received.append(snapshot)
            saved.set()

    _, transport, engine = engine_with(snapshot_store=Store())
    transport.on_sync = lambda cursor: sync_page(app=app_settings(hash_key="never-on-disk"), keys=[key_record(key.key_id, sha256(key.key))])
    engine.sync()
    assert saved.wait(5)
    # The external ID hashing key isn't saved to disk
    assert "hash_key" not in received[0]["app"]
    assert [record["key_id"] for record in received[0]["keys"]] == [key.key_id]


def test_threads_admit_and_meter_concurrently_without_losing_counts():
    key = generate_api_key("acme", "test")
    _, transport, engine = engine_with()
    transport.on_sync = lambda cursor: sync_page(keys=[key_record(key.key_id, sha256(key.key))])
    engine.sync()

    def work():
        for _ in range(500):
            engine.end(admitted(engine.begin(method="GET", path="/x", api_key=key.key)), status=200, route="/x")

    threads = [threading.Thread(target=work) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    engine.flush()
    assert sum(row["count"] for row in transport.rows()) == 4_000
    assert sum(entry["count"] for batch in transport.ingested for entry in batch["key_usage"]) == 4_000


@pytest.mark.skipif(not hasattr(os, "fork") or sys.platform == "win32", reason="fork only")
# Earlier tests started threads, and from Python 3.12 forking a multi-threaded process warns; the forked child is exactly what this tests
@pytest.mark.filterwarnings("ignore:This process .* is multi-threaded:DeprecationWarning")
def test_a_forked_child_is_a_new_instance_that_keeps_the_synced_keys():
    key = generate_api_key("acme", "test")
    clock, transport, engine = engine_with(instance_id=None, background_loops=False)
    transport.on_sync = lambda cursor: sync_page(keys=[key_record(key.key_id, sha256(key.key))])
    engine.sync()
    engine.end(admitted(engine.begin(method="GET", path="/x", api_key=key.key)), status=200, route="/x")
    parent_id = engine.instance_id
    read, write = os.pipe()
    child = os.fork()
    if child == 0:
        try:
            os.close(read)
            transport.ingested.clear()
            # Child: a new instance ID; usage the parent hadn't reported isn't its own; synced keys still work
            engine.flush()
            checks = [engine.instance_id != parent_id, transport.ingested == []]
            engine.end(admitted(engine.begin(method="GET", path="/x", api_key=key.key)), status=200, route="/x")
            engine.flush()
            checks.append([batch["instance_id"] for batch in transport.ingested] == [engine.instance_id])
            os.write(write, b"".join(b"1" if check else b"0" for check in checks))
        finally:
            os._exit(0)
    os.close(write)
    result = os.read(read, 16)
    os.waitpid(child, 0)
    assert result == b"111"
    # The parent's own usage is still there and reported as usual
    engine.flush()
    assert [batch["instance_id"] for batch in transport.ingested] == [parent_id]


def test_a_suspended_consumer_is_blocked_even_beyond_the_local_record_limit():
    key = generate_api_key("acme", "test")
    _, transport, engine = engine_with()
    # 100,000 active consumer records fill the local cap, and this key's consumer comes after them, so the first sync can't store its record
    crowd = [consumer_record(consumer=f"cons_{index:06d}") for index in range(100_000)]
    transport.on_sync = lambda cursor: sync_page(keys=[key_record(key.key_id, sha256(key.key), consumer="cons_zz_late")], consumers=crowd)
    engine.sync()
    admitted(engine.begin(method="GET", path="/x", api_key=key.key))
    transport.on_sync = lambda cursor: sync_page(full=False, consumers=[consumer_record(consumer="cons_zz_late", status="suspended")])
    engine.sync()
    assert rejected(engine.begin(method="GET", path="/x", api_key=key.key)).error == "consumer_suspended"


def test_a_customer_whose_record_does_not_fit_still_has_its_cached_quota_decision_follow_a_plan_change(monkeypatch):
    monkeypatch.setattr(engine_module, "MAX_CONSUMER_RECORDS", 2)

    def hashed(value: str) -> str:
        return hmac.new(b"k", value.encode(), hashlib.sha256).hexdigest()

    _, transport, engine = engine_with(auth="custom", consumer=lambda context: context.get("user") or "")
    # Records are full: two customers
    crowd = [consumer_record(consumer=f"cons_{index}", external_id=hashed(f"user-{index}")) for index in range(2)]
    transport.on_sync = lambda cursor: sync_page(app=app_settings(hash_key="k"), consumers=crowd)
    engine.sync()
    # alice's record can't be stored; the server says she's unlimited, and that's remembered locally
    first = waited(engine.begin(method="GET", path="/x", context={"user": "alice"}))
    assert first.allowed
    engine.end(first.handle, status=200)
    # Incremental sync: she moves to a plan with a hard limit, and her quota is already used up
    transport.on_sync = lambda cursor: sync_page(
        full=False,
        app=app_settings(hash_key="k"),
        consumers=[consumer_record(consumer="cons_alice", external_id=hashed("alice"), quota_behavior="stop", quota_version=2)],
    )
    engine.sync()
    transport.on_leases = lambda request: {
        "results": [{"consumer": item["consumer"], "status": "exhausted", "behavior": "stop", "quota_version": 2, "retry_after": 60} for item in request["requests"]]
    }
    decision = engine.begin(method="GET", path="/x", context={"user": "alice"})
    decision = decision.result(0) if isinstance(decision, Pending) else decision
    assert (decision.allowed, getattr(decision, "error", None)) == (False, "quota_exceeded")

