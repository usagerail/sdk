import { test } from "node:test";
import assert from "node:assert/strict";

import { perInstanceLimit, RateLimiter } from "../src/rate-limiter.ts";
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

test("both windows must have a token; Retry-After and the draft-11 headers describe both", () => {
  const clock = new FakeClock();
  const limiter = new RateLimiter(clock);
  const limits = { per_second: 10, per_minute: 3 };
  for (let index = 0; index < 3; index++) assert.equal(limiter.check("cons_1", limits, "per_instance", 1).allowed, true);
  const denied = limiter.check("cons_1", limits, "per_instance", 1);
  assert.equal(denied.allowed, false);
  if (denied.allowed) return;
  // 3 a minute: 20 seconds to earn 1 token
  assert.equal(denied.retryAfterSeconds, 20);
  assert.deepEqual(denied.headers, {
    "Retry-After": "20",
    "RateLimit-Policy": '"per_second";q=10;w=1, "per_minute";q=3;w=60',
    RateLimit: '"per_second";r=7;t=1, "per_minute";r=0;t=20',
  });
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
