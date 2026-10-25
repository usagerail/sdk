import threading
from concurrent.futures import Future

from helpers import FakeClock, FakeTransport, consumer_record, key_record, sha256

from usagerail._time import iso_from_ms
from usagerail.key_format import checksum_for, generate_api_key
from usagerail.key_store import (
    FRESH_FOR_MS,
    INVALID,
    NEGATIVE_CACHE_MS,
    REMOTE_VERIFIES_PER_SECOND,
    SNAPSHOT_MAX_AGE_MS,
    UNAVAILABLE,
    VERIFY_OUTAGE_MS,
    VERIFY_TIMEOUT_MS,
    KeyCheck,
    KeyStore,
)


def setup():
    clock = FakeClock()
    transport = FakeTransport()
    store = KeyStore(clock, transport, lambda message: None, threading.RLock())
    return clock, transport, store


def new_key():
    return generate_api_key("acme", "test")


def sync_with(store: KeyStore, records: list) -> None:
    store.apply_sync_page(True, records, [])
    store.complete_sync()


def resolve(check):
    return check.result(0) if isinstance(check, Future) else check


def test_a_bad_format_or_checksum_is_rejected_locally_without_a_network_call():
    _, transport, store = setup()
    key = new_key().key
    tampered = key[:-1] + ("B" if key.endswith("A") else "A")
    for candidate in (None, "", "not-a-key", tampered):
        assert store.check(candidate, "allow") == INVALID
    assert transport.verify_calls == []


def test_snapshot_keys_are_checked_by_hash_then_status_and_local_expiry():
    clock, _, store = setup()
    good, revoked, expiring = new_key(), new_key(), new_key()
    sync_with(
        store,
        [
            key_record(good.key_id, sha256(good.key), consumer="cons_good", quota_behavior="stop", quota_version=3),
            key_record(revoked.key_id, sha256(revoked.key), consumer="cons_revoked", status="revoked"),
            key_record(expiring.key_id, sha256(expiring.key), consumer="cons_expiring", expires_at=iso_from_ms(clock.now() + 60_000)),
        ],
    )
    assert store.check(good.key, "allow") == KeyCheck("valid", key_id=good.key_id, consumer="cons_good", quota_behavior="stop", quota_version=3)
    # Same key ID with a different secret and a correct checksum: passes the format check, but the hash doesn't match
    body = f"acme_test_{good.key_id}{new_key().key[18:-6]}"
    assert store.check(body + checksum_for(body), "allow") == INVALID
    # Revoked: we know who it is, so sdk_401 is recorded against them
    assert store.check(revoked.key, "allow") == KeyCheck("invalid", consumer="cons_revoked")
    assert store.check(expiring.key, "allow").result == "valid"
    clock.wall += 60_000
    assert store.check(expiring.key, "allow") == KeyCheck("invalid", consumer="cons_expiring")


def test_the_freshness_table_decides_what_happens_to_keys_missing_from_the_snapshot():
    clock, transport, store = setup()
    known, unknown = new_key(), new_key()
    gate: Future = Future()
    transport.on_verify = lambda request: gate
    # No snapshot: remote verification
    assert store.freshness() == "none"
    assert isinstance(store.check(unknown.key, "deny"), Future)
    sync_with(store, [key_record(known.key_id, sha256(known.key))])
    assert store.freshness() == "fresh"

    # Stale (no successful sync for 10 minutes) + allow: keys in the snapshot are judged locally, others are verified remotely
    clock.mono += FRESH_FOR_MS + 1
    assert store.freshness() == "stale"
    assert store.check(known.key, "allow").result == "valid"
    assert isinstance(store.check(new_key().key, "allow"), Future)
    # Stale + deny: keys missing from the snapshot get a 401 straight away, no remote verification
    calls = len(transport.verify_calls)
    assert store.check(new_key().key, "deny") == INVALID
    assert len(transport.verify_calls) == calls
    assert store.check(known.key, "deny").result == "valid"

    # Past 24 hours: the whole snapshot counts as none, and even keys in it are verified remotely
    clock.mono += SNAPSHOT_MAX_AGE_MS
    assert store.freshness() == "none"
    assert isinstance(store.check(known.key, "deny"), Future)


def test_remote_verification_positive_results_last_until_the_next_sync_negative_ones_60_seconds():
    clock, transport, store = setup()
    sync_with(store, [])
    fresh, unknown = new_key(), new_key()

    def verify(request):
        if request["key_id"] == fresh.key_id:
            return {"valid": True, "key": key_record(fresh.key_id, sha256(fresh.key), consumer="cons_new"), "consumer": consumer_record(consumer="cons_new")}
        return {"valid": False, "reason": "not_found"}

    transport.on_verify = verify
    first = resolve(store.check(fresh.key, "allow"))
    assert first.result == "valid"
    assert first.consumer_record["consumer"] == "cons_new"
    # No remote verification the second time
    assert store.check(fresh.key, "allow").result == "valid"
    assert len(transport.verify_calls) == 1
    # Dropped after the next sync (a new key should be in the snapshot by then; this sync doesn't have it, so it's verified again)
    sync_with(store, [])
    assert isinstance(store.check(fresh.key, "allow"), Future)

    assert resolve(store.check(unknown.key, "allow")).result == "invalid"
    calls = len(transport.verify_calls)
    assert store.check(unknown.key, "allow") == INVALID
    clock.mono += NEGATIVE_CACHE_MS
    assert isinstance(store.check(unknown.key, "allow"), Future)
    assert len(transport.verify_calls) == calls + 1


def test_before_the_first_sync_checks_over_the_budget_are_unavailable_not_401_because_every_key_is_unknown_then():
    # Right after startup, before the first sync completes, real customers' keys aren't in the snapshot either, so going over the budget mustn't mean the key is invalid
    _, transport, store = setup()
    transport.on_verify = lambda request: Future()
    for _ in range(REMOTE_VERIFIES_PER_SECOND):
        assert isinstance(store.check(new_key().key, "allow"), Future)
    assert store.check(new_key().key, "allow") == UNAVAILABLE
    # Once synced, keys missing from the snapshot beyond the budget still get a 401
    sync_with(store, [])
    assert store.check(new_key().key, "allow") == INVALID


def test_at_most_20_remote_verifications_per_second_and_concurrent_checks_of_one_key_share_a_call():
    clock, transport, store = setup()
    sync_with(store, [])
    gate: Future = Future()
    transport.on_verify = lambda request: gate
    shared = new_key()
    a = store.check(shared.key, "allow")
    b = store.check(shared.key, "allow")
    assert a is b
    results = [a] + [store.check(new_key().key, "allow") for _ in range(REMOTE_VERIFIES_PER_SECOND - 1)]
    assert len(transport.verify_calls) == REMOTE_VERIFIES_PER_SECOND
    # The 21st: 401 straight away
    assert store.check(new_key().key, "allow") == INVALID
    gate.set_result({"valid": False, "reason": "not_found"})
    assert all(result.result(0) == INVALID for result in results)
    clock.mono += 1_000
    assert isinstance(store.check(new_key().key, "allow"), Future)


def test_when_remote_verification_fails_new_keys_are_unavailable_for_a_few_seconds():
    clock, transport, store = setup()
    sync_with(store, [])

    def refuse(request):
        raise ConnectionRefusedError("connection refused")

    transport.on_verify = refuse
    assert resolve(store.check(new_key().key, "allow")) == UNAVAILABLE
    assert store.check(new_key().key, "allow") == UNAVAILABLE
    assert len(transport.verify_calls) == 1
    clock.mono += VERIFY_OUTAGE_MS
    assert isinstance(store.check(new_key().key, "allow"), Future)


def test_a_slow_answer_counts_as_unavailable_after_300_ms_but_is_still_remembered():
    # HTTP library timeouts can't stop a response that trickles in slowly, so the engine enforces the 300 ms itself (a hard rule)
    clock, transport, store = setup()
    sync_with(store, [])
    key = new_key()
    answer: Future = Future()
    transport.on_verify = lambda request: answer
    pending = store.check(key.key, "allow")
    assert transport.timeouts == [VERIFY_TIMEOUT_MS]
    clock.advance(VERIFY_TIMEOUT_MS)
    assert pending.result(0) == UNAVAILABLE
    # The late result is still recorded: once the few seconds of outage pass, this key doesn't need asking again
    answer.set_result({"valid": True, "key": key_record(key.key_id, sha256(key.key), consumer="cons_new"), "consumer": consumer_record(consumer="cons_new")})
    clock.mono += VERIFY_OUTAGE_MS
    assert store.check(key.key, "allow").result == "valid"
    assert len(transport.verify_calls) == 1


def test_a_paged_full_sync_removes_unlisted_keys_only_after_the_last_page():
    _, _, store = setup()
    a, b = new_key(), new_key()
    sync_with(store, [key_record(a.key_id, sha256(a.key)), key_record(b.key_id, sha256(b.key))])
    # A new full sync: the first page has only a, the second has nothing
    store.apply_sync_page(True, [key_record(a.key_id, sha256(a.key))], [])
    assert store.check(b.key, "allow").result == "valid"
    store.apply_sync_page(True, [], [])
    store.complete_sync()
    assert isinstance(store.check(b.key, "allow"), Future)
    # a from the first page is still there
    assert store.check(a.key, "allow").result == "valid"
    # A key removed in an incremental sync is gone at once
    store.apply_sync_page(False, [], [a.key_id])
    assert isinstance(store.check(a.key, "allow"), Future)


def test_a_forged_secret_for_a_known_key_id_does_not_get_the_real_key_refused():
    _, transport, store = setup()
    sync_with(store, [])
    real = new_key()

    def verify(request):
        if request["hash"] == sha256(real.key):
            return {"valid": True, "key": key_record(real.key_id, sha256(real.key), consumer="cons_new"), "consumer": consumer_record(consumer="cons_new")}
        return {"valid": False, "reason": "not_found"}

    transport.on_verify = verify
    # Same key ID, made-up secret, correct checksum
    body = f"acme_test_{real.key_id}{new_key().key[18:-6]}"
    assert resolve(store.check(body + checksum_for(body), "allow")).result == "invalid"
    assert resolve(store.check(real.key, "allow")).result == "valid"


# ---- Found in review (round four): a late remote verification doesn't override a newer sync ----
def test_a_verification_that_comes_back_after_a_newer_sync_is_not_remembered():
    # A sync completed while the verification was in flight (the key may have been revoked or deleted meanwhile): the late "valid" isn't remembered,
    # or a revoked key would keep getting through for as long as sync keeps failing
    clock, transport, store = setup()
    sync_with(store, [])
    key = new_key()
    answer: Future = Future()
    transport.on_verify = lambda request: answer
    pending = store.check(key.key, "allow")
    clock.advance(VERIFY_TIMEOUT_MS)
    assert pending.result(0) == UNAVAILABLE
    sync_with(store, [])
    answer.set_result({"valid": True, "key": key_record(key.key_id, sha256(key.key), consumer="cons_new"), "consumer": consumer_record(consumer="cons_new")})
    clock.mono += VERIFY_OUTAGE_MS
    transport.on_verify = lambda request: {"valid": False, "reason": "not_found"}
    assert resolve(store.check(key.key, "allow")) == INVALID
    assert len(transport.verify_calls) == 2


def test_a_request_still_waiting_when_a_sync_revokes_its_key_is_refused():
    _, transport, store = setup()
    sync_with(store, [])
    key = new_key()
    answer: Future = Future()
    transport.on_verify = lambda request: answer
    pending = store.check(key.key, "allow")
    sync_with(store, [key_record(key.key_id, sha256(key.key), consumer="cons_new", status="revoked")])
    answer.set_result({"valid": True, "key": key_record(key.key_id, sha256(key.key), consumer="cons_new"), "consumer": consumer_record(consumer="cons_new")})
    assert pending.result(0).result == "invalid"
