from helpers import FakeClock

from usagerail.rate_limiter import RateLimiter, per_instance_limit


def test_a_per_second_bucket_allows_the_limit_then_refills_over_the_second():
    clock = FakeClock()
    limiter = RateLimiter(clock)
    limits = {"per_second": 2, "per_minute": None}
    assert limiter.check("cons_1", limits, "per_instance", 1).allowed
    assert limiter.check("cons_1", limits, "per_instance", 1).allowed
    assert not limiter.check("cons_1", limits, "per_instance", 1).allowed
    # Half a second earns back 1 token
    clock.mono += 500
    assert limiter.check("cons_1", limits, "per_instance", 1).allowed
    assert not limiter.check("cons_1", limits, "per_instance", 1).allowed
    # Other consumers are counted separately
    assert limiter.check("cons_2", limits, "per_instance", 1).allowed


def _take_all(limiter: RateLimiter, limits, count: int) -> None:
    for _ in range(count):
        assert limiter.check("cons_1", limits, "per_instance", 1).allowed


def _reject(limiter: RateLimiter, limits):
    denied = limiter.check("cons_1", limits, "per_instance", 1)
    assert not denied.allowed
    return denied


# In RateLimit, only buckets with no tokens left carry w (draft-12 section 3.1.2: seconds in which the client can use no more than a,
# so the wait for the next token rounded down). A bucket that still has tokens can be further from its next token than
# Retry-After, and section 5 says Retry-After shouldn't end before w
def test_only_the_per_second_bucket_is_empty():
    clock = FakeClock()
    limiter = RateLimiter(clock)
    limits = {"per_second": 1, "per_minute": 10}
    _take_all(limiter, limits, 1)
    denied = _reject(limiter, limits)
    assert denied.retry_after_seconds == 1
    assert denied.headers == {
        "Retry-After": "1",
        "RateLimit-Policy": '"per_second";q=1;w=1, "per_minute";q=10;w=60',
        "RateLimit": '"per_second";a=0;w=1, "per_minute";a=9',
    }
    clock.mono += 1000
    assert limiter.check("cons_1", limits, "per_instance", 1).allowed


def test_only_the_per_minute_bucket_is_empty():
    clock = FakeClock()
    limiter = RateLimiter(clock)
    limits = {"per_second": 10, "per_minute": 3}
    _take_all(limiter, limits, 3)
    denied = _reject(limiter, limits)
    # 3 a minute: 20 seconds to earn 1 token
    assert denied.retry_after_seconds == 20
    assert denied.headers == {
        "Retry-After": "20",
        "RateLimit-Policy": '"per_second";q=10;w=1, "per_minute";q=3;w=60',
        "RateLimit": '"per_second";a=7, "per_minute";a=0;w=20',
    }
    clock.mono += 20_000
    assert limiter.check("cons_1", limits, "per_instance", 1).allowed


def test_both_buckets_are_empty():
    limits = {"per_second": 1, "per_minute": 1}
    clock = FakeClock()
    limiter = RateLimiter(clock)
    _take_all(limiter, limits, 1)
    denied = _reject(limiter, limits)
    assert denied.retry_after_seconds == 60
    assert denied.headers == {
        "Retry-After": "60",
        "RateLimit-Policy": '"per_second";q=1;w=1, "per_minute";q=1;w=60',
        "RateLimit": '"per_second";a=0;w=1, "per_minute";a=0;w=60',
    }
    # Waiting only for the per-second bucket isn't enough
    clock.mono += 1000
    _reject(limiter, limits)

    fresh = FakeClock()
    waited = RateLimiter(fresh)
    _take_all(waited, limits, 1)
    _reject(waited, limits)
    fresh.mono += 60_000
    assert waited.check("cons_1", limits, "per_instance", 1).allowed


def test_after_the_last_token_is_taken_w_is_the_wait_for_one_token():
    clock = FakeClock()
    limiter = RateLimiter(clock)
    limits = {"per_second": None, "per_minute": 10}
    # The request that takes the last token succeeds; the SDK sends rate limit headers only on a 429
    _take_all(limiter, limits, 10)
    denied = _reject(limiter, limits)
    # 10 a minute: one token every 6 seconds. A full bucket would take 60
    assert denied.retry_after_seconds == 6
    assert denied.headers["RateLimit"] == '"per_minute";a=0;w=6'
    clock.mono += 6000
    assert limiter.check("cons_1", limits, "per_instance", 1).allowed


def test_a_partial_token_rounds_retry_after_up_and_w_down():
    clock = FakeClock()
    limiter = RateLimiter(clock)
    limits = {"per_second": 2, "per_minute": 3}
    _take_all(limiter, limits, 2)
    # 2 a second: half a second to the next per-second token, too short for a whole-second w
    assert _reject(limiter, limits).headers["RateLimit"] == '"per_second";a=0, "per_minute";a=1'
    clock.mono += 500
    _take_all(limiter, limits, 1)
    # The per-minute bucket now holds 0.275 tokens (0.025 left after the third request, plus 5 seconds at 3 a minute): 14.5 seconds to the next one
    clock.mono += 5000
    denied = _reject(limiter, limits)
    assert denied.retry_after_seconds == 15
    assert denied.headers["RateLimit"] == '"per_second";a=2, "per_minute";a=0;w=14'


def test_divided_mode_splits_the_limit_across_active_instances():
    assert per_instance_limit(10, "per_instance", 3) == 10
    assert per_instance_limit(10, "divided", 3) == 4
    assert per_instance_limit(2, "divided", 5) == 1
    assert per_instance_limit(10, "divided", 0) == 10
    limiter = RateLimiter(FakeClock())
    limits = {"per_second": 10, "per_minute": None}
    assert sum(limiter.check("cons_1", limits, "divided", 3).allowed for _ in range(6)) == 4


def test_no_limits_means_no_bucket_and_no_rejection():
    limiter = RateLimiter(FakeClock())
    for _ in range(100):
        assert limiter.check("cons_1", {"per_second": None, "per_minute": None}, "per_instance", 1).allowed
        assert limiter.check("cons_1", None, "per_instance", 1).allowed
