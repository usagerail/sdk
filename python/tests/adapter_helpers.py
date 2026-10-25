"""Fake backends shared by the tests of the three framework adapters. Correspond to backend() and stopBackend() in the Node SDK's test/adapters.test.ts."""

from __future__ import annotations

from helpers import FakeClock, FakeTransport, SilentLogger, app_settings, consumer_record, key_record, sha256, sync_page

from usagerail import create_engine
from usagerail._time import iso_from_ms
from usagerail.key_format import generate_api_key


def options(clock, transport, **overrides):
    values = {
        "app_key": "urk_test_adapter",
        "auth": "api-key",
        "clock": clock,
        "transport": transport,
        "background_loops": False,
        "flush_on_exit": False,
        "logger": SilentLogger(),
    }
    values.update(overrides)
    return values


def backend(routes=None, **engine_overrides):
    """One normal key and one key limited to 1 request a second, metering units"""
    key = generate_api_key("acme", "test")
    limited = generate_api_key("acme", "test")
    clock = FakeClock()
    transport = FakeTransport()
    transport.on_sync = lambda cursor: sync_page(
        app=app_settings(unit_mode="units", routes=routes or []),
        keys=[
            key_record(key.key_id, sha256(key.key), consumer="cons_1"),
            key_record(limited.key_id, sha256(limited.key), consumer="cons_limited"),
        ],
        consumers=[consumer_record(consumer="cons_1"), consumer_record(consumer="cons_limited", rate_limits={"per_second": 1, "per_minute": None})],
    )
    engine = create_engine(**options(clock, transport, **engine_overrides))
    engine.sync()
    return engine, transport, key.key, limited.key


def stop_backend(routes):
    """A consumer with a hard limit and only 5 units left on the server each time: a reservation above 5 is told it's used up"""
    key = generate_api_key("acme", "test")
    clock = FakeClock()
    transport = FakeTransport()
    transport.on_sync = lambda cursor: sync_page(
        app=app_settings(unit_mode="requests", routes=routes),
        keys=[key_record(key.key_id, sha256(key.key), consumer="cons_stop", quota_behavior="stop", quota_version=1)],
        consumers=[consumer_record(consumer="cons_stop", quota_behavior="stop")],
    )

    def leases(request):
        results = []
        for item in request["requests"]:
            if item["reserve"] > 5:
                results.append({"consumer": item["consumer"], "status": "exhausted", "behavior": "stop", "quota_version": 1, "retry_after": 60})
            else:
                lease = {"lease_id": "1", "consumer": item["consumer"], "granted": 5, "quota_version": 1, "behavior": "stop", "period_end": iso_from_ms(clock.now() + 3_600_000)}
                results.append({"consumer": item["consumer"], "status": "granted", "lease": lease})
        return {"results": results}

    transport.on_leases = leases
    engine = create_engine(**options(clock, transport))
    engine.sync()
    return engine, transport, key.key


def summary(transport):
    return [(row["method"], row["route"], row["result_type"], row["units"]) for row in transport.rows()]


def bearer(key):
    return {"Authorization": f"Bearer {key}"}
