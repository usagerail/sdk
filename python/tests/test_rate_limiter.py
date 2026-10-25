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


def test_both_windows_must_have_a_token_and_the_headers_describe_both():
    limiter = RateLimiter(FakeClock())
    limits = {"per_second": 10, "per_minute": 3}
    for _ in range(3):
        assert limiter.check("cons_1", limits, "per_instance", 1).allowed
    denied = limiter.check("cons_1", limits, "per_instance", 1)
    assert not denied.allowed
    # 3 a minute: 20 seconds to earn 1 token
    assert denied.retry_after_seconds == 20
    assert denied.headers == {
        "Retry-After": "20",
        "RateLimit-Policy": '"per_second";q=10;w=1, "per_minute";q=3;w=60',
        "RateLimit": '"per_second";r=7;t=1, "per_minute";r=0;t=20',
    }


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
