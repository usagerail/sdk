import { test } from "node:test";
import assert from "node:assert/strict";

import { perInstanceLimit, RateLimiter, type RateLimits } from "../src/rate-limiter.ts";
import { FakeClock } from "./helpers.ts";

test("a per-second bucket allows the limit, then refills over the second", () => {
  const clock = new FakeClock();
  const limiter = new RateLimiter(clock);
  const limits = { per_second: 2, per_minute: null };
  assert.equal(limiter.check("cons_1", limits, "per_instance", 1).allowed, true);
  assert.equal(limiter.check("cons_1", limits, "per_instance", 1).allowed, true);
  const denied = limiter.check("cons_1", limits, "per_instance", 1);
  assert.equal(denied.allowed, false);
  // Half a second earns back 1 token
  clock.mono += 500;
  assert.equal(limiter.check("cons_1", limits, "per_instance", 1).allowed, true);
  assert.equal(limiter.check("cons_1", limits, "per_instance", 1).allowed, false);
  // Other consumers are counted separately
  assert.equal(limiter.check("cons_2", limits, "per_instance", 1).allowed, true);
});

function takeAll(limiter: RateLimiter, limits: RateLimits, count: number) {
  for (let index = 0; index < count; index++) assert.equal(limiter.check("cons_1", limits, "per_instance", 1).allowed, true);
}

function reject(limiter: RateLimiter, limits: RateLimits) {
  const denied = limiter.check("cons_1", limits, "per_instance", 1);
  if (denied.allowed) throw new Error("expected a 429");
  return denied;
}

// In RateLimit, only buckets with no tokens left carry w (draft-12 section 3.1.2: seconds in which the client can use no more than a,
// so the wait for the next token rounded down). A bucket that still has tokens can be further from its next token than
// Retry-After, and section 5 says Retry-After shouldn't end before w
test("only the per-second bucket is empty: Retry-After is its wait, and the per-minute bucket reports a without w", () => {
  const clock = new FakeClock();
  const limiter = new RateLimiter(clock);
  const limits = { per_second: 1, per_minute: 10 };
  takeAll(limiter, limits, 1);
  const denied = reject(limiter, limits);
  assert.equal(denied.retryAfterSeconds, 1);
  assert.deepEqual(denied.headers, {
    "Retry-After": "1",
    "RateLimit-Policy": '"per_second";q=1;w=1, "per_minute";q=10;w=60',
    RateLimit: '"per_second";a=0;w=1, "per_minute";a=9',
  });
  clock.mono += 1000;
  assert.equal(limiter.check("cons_1", limits, "per_instance", 1).allowed, true);
});

test("only the per-minute bucket is empty: Retry-After is its wait for one token", () => {
  const clock = new FakeClock();
  const limiter = new RateLimiter(clock);
  const limits = { per_second: 10, per_minute: 3 };
  takeAll(limiter, limits, 3);
  const denied = reject(limiter, limits);
  // 3 a minute: 20 seconds to earn 1 token
  assert.equal(denied.retryAfterSeconds, 20);
  assert.deepEqual(denied.headers, {
    "Retry-After": "20",
    "RateLimit-Policy": '"per_second";q=10;w=1, "per_minute";q=3;w=60',
    RateLimit: '"per_second";a=7, "per_minute";a=0;w=20',
  });
  clock.mono += 20_000;
  assert.equal(limiter.check("cons_1", limits, "per_instance", 1).allowed, true);
});

test("both buckets are empty: Retry-After is the longer wait, and each bucket reports its own w", () => {
  const limits = { per_second: 1, per_minute: 1 };
  const clock = new FakeClock();
  const limiter = new RateLimiter(clock);
  takeAll(limiter, limits, 1);
  const denied = reject(limiter, limits);
  assert.equal(denied.retryAfterSeconds, 60);
  assert.deepEqual(denied.headers, {
    "Retry-After": "60",
    "RateLimit-Policy": '"per_second";q=1;w=1, "per_minute";q=1;w=60',
    RateLimit: '"per_second";a=0;w=1, "per_minute";a=0;w=60',
  });
  // Waiting only for the per-second bucket isn't enough
  clock.mono += 1000;
  reject(limiter, limits);

  const fresh = new FakeClock();
  const waited = new RateLimiter(fresh);
  takeAll(waited, limits, 1);
  reject(waited, limits);
  fresh.mono += 60_000;
  assert.equal(waited.check("cons_1", limits, "per_instance", 1).allowed, true);
});

test("after the last token is taken, w is the wait for one token, not for a full bucket", () => {
  const clock = new FakeClock();
  const limiter = new RateLimiter(clock);
  const limits = { per_second: null, per_minute: 10 };
  // The request that takes the last token succeeds; the SDK sends rate limit headers only on a 429
  takeAll(limiter, limits, 10);
  const denied = reject(limiter, limits);
  // 10 a minute: one token every 6 seconds. A full bucket would take 60
  assert.equal(denied.retryAfterSeconds, 6);
  assert.equal(denied.headers.RateLimit, '"per_minute";a=0;w=6');
  clock.mono += 6000;
  assert.equal(limiter.check("cons_1", limits, "per_instance", 1).allowed, true);
});

test("a partial token: Retry-After rounds the wait up, w rounds it down, and under a second there's no w", () => {
  const clock = new FakeClock();
  const limiter = new RateLimiter(clock);
  const limits = { per_second: 2, per_minute: 3 };
  takeAll(limiter, limits, 2);
  // 2 a second: half a second to the next per-second token, too short for a whole-second w
  assert.equal(reject(limiter, limits).headers.RateLimit, '"per_second";a=0, "per_minute";a=1');
  clock.mono += 500;
  takeAll(limiter, limits, 1);
  // The per-minute bucket now holds 0.275 tokens (0.025 left after the third request, plus 5 seconds at 3 a minute): 14.5 seconds to the next one
  clock.mono += 5000;
  const denied = reject(limiter, limits);
  assert.equal(denied.retryAfterSeconds, 15);
  assert.equal(denied.headers.RateLimit, '"per_second";a=2, "per_minute";a=0;w=14');
  assert.deepEqual(
    denied.policies.map((policy) => [policy.name, policy.retryAfterSeconds, policy.effectiveWindowSeconds]),
    [["per_second", null, null], ["per_minute", 15, 14]],
  );
});

test("divided mode splits the limit across active instances, rounding up, at least 1", () => {
  assert.equal(perInstanceLimit(10, "per_instance", 3), 10);
  assert.equal(perInstanceLimit(10, "divided", 3), 4);
  assert.equal(perInstanceLimit(2, "divided", 5), 1);
  assert.equal(perInstanceLimit(10, "divided", 0), 10);

  const limiter = new RateLimiter(new FakeClock());
  const limits = { per_second: 10, per_minute: null };
  const allowed = Array.from({ length: 6 }, () => limiter.check("cons_1", limits, "divided", 3).allowed).filter(Boolean).length;
  assert.equal(allowed, 4);
});

test("no limits means no bucket and no rejection", () => {
  const limiter = new RateLimiter(new FakeClock());
  for (let index = 0; index < 100; index++) {
    assert.equal(limiter.check("cons_1", { per_second: null, per_minute: null }, "per_instance", 1).allowed, true);
    assert.equal(limiter.check("cons_1", null, "per_instance", 1).allowed, true);
  }
});
