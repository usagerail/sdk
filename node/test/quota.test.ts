import { test } from "node:test";
import assert from "node:assert/strict";

import type { LeaseRequest, LeaseResponse, LeaseResult } from "../src/protocol.ts";
import { LEASE_OUTAGE_MS, LEASE_TTL_MS, MAX_QUOTA_CONSUMERS, QuotaManager, SYNC_WAIT_MS, type QuotaDecision, type Reservation } from "../src/quota.ts";
import { FakeClock, FakeTransport, settle } from "./helpers.ts";

const STOP = { behavior: "stop" as const, version: 1 };

function setup(onUnavailable: "allow" | "deny" = "allow") {
  const clock = new FakeClock();
  const transport = new FakeTransport();
  const quota = new QuotaManager({ clock, transport, warn: () => {}, onUnavailable: () => onUnavailable });
  return { clock, transport, quota };
}

let leaseSequence = 0;
// Grants a lease of granted units to every consumer in the request; the period ends an hour from now by default
function grant(granted: number | ((consumer: string) => number), periodEnd: (consumer: string) => number, behavior: "stop" | "prepaid" = "stop") {
  return (request: LeaseRequest): LeaseResponse => ({
    results: request.requests.map((item): LeaseResult => ({
      consumer: item.consumer,
      status: "granted",
      lease: {
        lease_id: String(++leaseSequence),
        consumer: item.consumer,
        granted: typeof granted === "number" ? granted : granted(item.consumer),
        quota_version: item.quota_version || 1,
        behavior,
        period_end: new Date(periodEnd(item.consumer)).toISOString(),
      },
    })),
  });
}

const inAnHour = (clock: FakeClock) => () => clock.now() + 3_600_000;

function allowed(decision: QuotaDecision | Promise<QuotaDecision>): Reservation {
  assert.ok(!(decision instanceof Promise), "expected a local decision");
  assert.equal(decision.allowed, true);
  assert.ok(decision.allowed && decision.reservation);
  return (decision as { reservation: Reservation }).reservation;
}

test("overage consumers and consumers without a quota never take a lease", () => {
  const { clock, transport, quota } = setup();
  assert.deepEqual(quota.reserve("cons_over", "id", { behavior: "overage", version: 1 }, 1, clock.now()), { allowed: true, reservation: null });
  assert.deepEqual(quota.reserve("cons_none", "id", { behavior: null, version: 1 }, 1, clock.now()), { allowed: true, reservation: null });
  assert.equal(transport.leaseCalls.length, 0);
});

test("the first request waits for a lease; later requests reserve locally", async () => {
  const { clock, transport, quota } = setup();
  transport.onLeases = grant(10, inAnHour(clock));
  const first = await quota.reserve("cons_1", "id", STOP, 1, clock.now());
  assert.equal(first.allowed && first.reservation?.units, 1);
  allowed(quota.reserve("cons_1", "id", STOP, 1, clock.now()));
  assert.equal(transport.leaseCalls.length, 1);
  const [item] = transport.leaseCalls[0].requests;
  // at is the end of the minute the request falls in
  assert.deepEqual(item, { consumer: "cons_1", consumer_kind: "id", reserve: 1, recent_usage_60s: 0, quota_version: 1, at: "2026-10-20T12:01:00.000Z" });
});

test("settling refunds unused and non-billable units; overspending blocks until a new lease covers it", async () => {
  const { clock, transport, quota } = setup();
  transport.onLeases = grant(10, inAnHour(clock));
  const big = (await quota.reserve("cons_1", "id", STOP, 5, clock.now())) as { reservation: Reservation };
  // Reserved 5, used 2: refund 3
  quota.settle(big.reservation, 2);
  // Non-billable outcome: full refund
  quota.settle(allowed(quota.reserve("cons_1", "id", STOP, 1, clock.now())), 0);
  // 8 available now: reserve 8, actually use 20 (more custom units than reserved), owing 12
  quota.settle(allowed(quota.reserve("cons_1", "id", STOP, 8, clock.now())), 20);
  transport.onLeases = grant(30, inAnHour(clock));
  // Nothing is admitted until the debt is paid, so this waits for a new lease; the new lease of 30 pays off the 12 first
  const next = quota.reserve("cons_1", "id", STOP, 1, clock.now());
  assert.ok(next instanceof Promise);
  assert.equal((await next).allowed, true);
  const report = quota.collectReports();
  // The first lease reports actual usage of 2 + 0 + 20
  assert.deepEqual(report.entries.map((entry) => entry.used), [22]);
  for (let index = 0; index < 17; index++) allowed(quota.reserve("cons_1", "id", STOP, 1, clock.now()));
  // 30 − 12 = 18: 1 used when the lease arrived, 17 more admitted, and the 19th waits for a renewal
  assert.ok(quota.reserve("cons_1", "id", STOP, 1, clock.now()) instanceof Promise);
});

test("renewals queued within the same second go out as one request", async () => {
  const { clock, transport, quota } = setup();
  transport.onLeases = grant(10, inAnHour(clock));
  await quota.reserve("cons_a", "id", STOP, 1, clock.now());
  await quota.reserve("cons_b", "id", STOP, 1, clock.now());
  assert.equal(transport.leaseCalls.length, 2);
  // Both used down below 30%
  for (const consumer of ["cons_a", "cons_b"]) {
    for (let index = 0; index < 7; index++) allowed(quota.reserve(consumer, "id", STOP, 1, clock.now()));
  }
  await clock.advance(0);
  assert.equal(transport.leaseCalls.length, 3);
  assert.deepEqual(transport.leaseCalls[2].requests.map((item) => item.consumer).sort(), ["cons_a", "cons_b"]);
});

test("a lease stops being used 60 seconds after it arrived, on the monotonic clock", async () => {
  const { clock, transport, quota } = setup();
  transport.onLeases = grant(100, inAnHour(clock));
  await quota.reserve("cons_1", "id", STOP, 1, clock.now());
  // Moving the wall clock back has no effect
  clock.wall -= 3_600_000;
  clock.mono += LEASE_TTL_MS - 1;
  allowed(quota.reserve("cons_1", "id", STOP, 1, clock.now() + 3_600_000));
  clock.mono += 1;
  assert.ok(quota.reserve("cons_1", "id", STOP, 1, clock.now() + 3_600_000) instanceof Promise);
});

test("a higher quota version retires held leases immediately and asks for a new one", async () => {
  const { clock, transport, quota } = setup();
  transport.onLeases = grant(100, inAnHour(clock));
  await quota.reserve("cons_1", "id", STOP, 1, clock.now());
  quota.observeRecord("cons_1", 2, "stop", "active");
  // It had a lease, so a renewal goes out in the background first
  await clock.advance(0);
  assert.equal(transport.leaseCalls.length, 2);
  assert.equal(transport.leaseCalls[1].requests[0].quota_version, 2);
  allowed(quota.reserve("cons_1", "id", { behavior: "stop", version: 2 }, 1, clock.now()));
});

test("a minute ending exactly at period_end uses the lease; the next minute needs the next period's lease", async () => {
  const { clock, transport, quota } = setup();
  const periodEnd = Date.UTC(2026, 9, 20, 12, 1, 0);
  transport.onLeases = grant(100, () => periodEnd);
  // The request starts at 12:00:10; its minute [12:00, 12:01) ends exactly when the period ends, so it belongs to this period
  await quota.reserve("cons_1", "id", STOP, 1, clock.now());
  allowed(quota.reserve("cons_1", "id", STOP, 1, Date.UTC(2026, 9, 20, 12, 0, 59, 999)));
  // A request starting at 12:01:00 belongs to the next period: this lease can't be used, so it asks for the next one with at = 12:02
  transport.onLeases = grant(100, () => periodEnd + 3_600_000);
  const next = quota.reserve("cons_1", "id", STOP, 1, periodEnd);
  assert.ok(next instanceof Promise);
  assert.equal((await next).allowed, true);
  assert.equal(transport.leaseCalls.at(-1)?.requests[0].at, "2026-10-20T12:02:00.000Z");
});

test("'lease closed' goes out only after in-flight requests end and their usage is acknowledged", async () => {
  const { clock, transport, quota } = setup();
  transport.onLeases = grant(100, inAnHour(clock));
  const { reservation } = (await quota.reserve("cons_1", "id", STOP, 1, clock.now())) as { reservation: Reservation };
  const leaseId = reservation.parts[0].lease.id;
  // The version changed and the lease is retired, but one request is still in flight
  quota.observeRecord("cons_1", 2, "stop", "active");
  assert.deepEqual(quota.collectReports().entries, []);
  quota.settle(reservation, 1);
  const withUsage = quota.collectReports();
  assert.deepEqual(withUsage.entries, [{ lease_id: leaseId, used: 1 }]);
  // The batch with its usage isn't confirmed yet, so don't report "closed" yet
  assert.deepEqual(quota.collectReports().entries.filter((entry) => entry.lease_id === leaseId), []);
  quota.finishReport(withUsage);
  const closing = quota.collectReports();
  assert.deepEqual(closing.entries.filter((entry) => entry.lease_id === leaseId), [{ lease_id: leaseId, used: 0, closed: true }]);
  quota.finishReport(closing);
  assert.deepEqual(quota.collectReports().entries.filter((entry) => entry.lease_id === leaseId), []);
});

test("exhausted quotas: stop-at-limit is quota_exceeded with Retry-After, prepaid is insufficient_credits", async () => {
  const { clock, transport, quota } = setup();
  transport.onLeases = (request) => ({
    results: request.requests.map((item): LeaseResult =>
      item.consumer === "cons_stop"
        ? { consumer: item.consumer, status: "exhausted", behavior: "stop", quota_version: 1, retry_after: 120 }
        : { consumer: item.consumer, status: "exhausted", behavior: "prepaid", quota_version: 1, retry_after: null },
    ),
  });
  assert.deepEqual(await quota.reserve("cons_stop", "id", STOP, 1, clock.now()), { allowed: false, error: "quota_exceeded", retryAfterSeconds: 120 });
  assert.deepEqual(await quota.reserve("cons_prepaid", "id", { behavior: "prepaid", version: 1 }, 1, clock.now()), {
    allowed: false,
    error: "insufficient_credits",
  });
  // The result is cached locally; not every request asks the server
  clock.wall += 30_000;
  assert.deepEqual(quota.reserve("cons_stop", "id", STOP, 1, clock.now()), { allowed: false, error: "quota_exceeded", retryAfterSeconds: 90 });
  assert.equal(transport.leaseCalls.length, 2);
});

test("a suspended consumer is blocked until sync says it is active again", async () => {
  const { clock, transport, quota } = setup();
  transport.onLeases = (request) => ({ results: request.requests.map((item): LeaseResult => ({ consumer: item.consumer, status: "suspended", quota_version: 1 })) });
  const info = { behavior: undefined, version: 0 };
  assert.deepEqual(await quota.reserve("a1b2", "external", info, 1, clock.now()), { allowed: false, error: "consumer_suspended" });
  assert.deepEqual(quota.reserve("a1b2", "external", info, 1, clock.now()), { allowed: false, error: "consumer_suspended" });
  assert.equal(transport.leaseCalls[0].requests[0].consumer_kind, "external");
  // Sync says it's active again and billed for overage: let it through, no more leases needed
  quota.observeRecord("a1b2", 1, "overage", "active");
  assert.deepEqual(quota.reserve("a1b2", "external", info, 1, clock.now()), { allowed: true, reservation: null });
  assert.equal(transport.leaseCalls.length, 1);
});

test("when leases can't be fetched, onQuotaUnavailable decides, and nobody waits again for a few seconds", async () => {
  for (const policy of ["allow", "deny"] as const) {
    const { clock, transport, quota } = setup(policy);
    transport.onLeases = () => {
      throw new Error("connection refused");
    };
    const expected = policy === "allow" ? { allowed: true, reservation: null } : { allowed: false, error: "quota_unavailable" };
    assert.deepEqual(await quota.reserve("cons_1", "id", STOP, 1, clock.now()), expected);
    assert.deepEqual(quota.reserve("cons_1", "id", STOP, 1, clock.now()), expected);
    assert.equal(transport.leaseCalls.length, 1);
    clock.mono += LEASE_OUTAGE_MS;
    assert.ok(quota.reserve("cons_1", "id", STOP, 1, clock.now()) instanceof Promise);
  }
});

test("a request waits at most 300 ms for a lease", async () => {
  const { clock, transport, quota } = setup();
  transport.onLeases = () => new Promise(() => {});
  let result: QuotaDecision | undefined;
  void (quota.reserve("cons_1", "id", STOP, 1, clock.now()) as Promise<QuotaDecision>).then((decision) => (result = decision));
  await clock.advance(SYNC_WAIT_MS - 1);
  assert.equal(result, undefined);
  await clock.advance(1);
  await settle();
  assert.deepEqual(result, { allowed: true, reservation: null });
});

test("debt the server's leases can't cover yet blocks the request instead of falling back to onQuotaUnavailable=allow", async () => {
  const { clock, transport, quota } = setup("allow");
  transport.onLeases = grant(10, inAnHour(clock));
  // Reserved 10, actually used 100 (more custom units than reserved): owing 90
  const { reservation } = (await quota.reserve("cons_1", "id", STOP, 10, clock.now())) as { reservation: Reservation };
  quota.settle(reservation, 100);
  // The server keeps answering normally with 10 at a time, all absorbed by the debt
  const decision = await quota.reserve("cons_1", "id", STOP, 1, clock.now());
  assert.deepEqual(decision, { allowed: false, error: "quota_exceeded", retryAfterSeconds: 1 });
  assert.equal(transport.leaseCalls.length, 4);
});

test("leases ask for the reservation the request needs, and 'used up' for a big request doesn't block small ones", async () => {
  const { clock, transport, quota } = setup();
  // 10 available: a request for 100 is told it's used up, a request for 1 still gets a lease
  transport.onLeases = (request) => ({
    results: request.requests.map((item): LeaseResult =>
      item.reserve > 10
        ? { consumer: item.consumer, status: "exhausted", behavior: "stop", quota_version: 1, retry_after: 60 }
        : {
            consumer: item.consumer,
            status: "granted",
            lease: { lease_id: String(++leaseSequence), consumer: item.consumer, granted: 10, quota_version: 1, behavior: "stop", period_end: new Date(clock.now() + 3_600_000).toISOString() },
          },
    ),
  });
  assert.deepEqual(await quota.reserve("cons_1", "id", STOP, 100, clock.now()), { allowed: false, error: "quota_exceeded", retryAfterSeconds: 60 });
  assert.equal(transport.leaseCalls[0].requests[0].reserve, 100);
  const small = quota.reserve("cons_1", "id", STOP, 1, clock.now());
  assert.ok(small instanceof Promise);
  assert.equal((await small).allowed, true);
  assert.equal(transport.leaseCalls[1].requests[0].reserve, 1);
  // Big requests are still blocked
  assert.equal((await quota.reserve("cons_1", "id", STOP, 100, clock.now())).allowed, false);
});

test("one request can reserve across several leases instead of being refused while capacity is split", async () => {
  const { clock, transport, quota } = setup();
  transport.onLeases = grant(4, inAnHour(clock));
  // First lease of 4: reserve 4, refund it all, 4 left
  const first = (await quota.reserve("cons_1", "id", STOP, 4, clock.now())) as { reservation: Reservation };
  quota.settle(first.reservation, 0);
  // Needs 5: the 4 held aren't enough, so get another lease of 4 and combine them
  const decision = await quota.reserve("cons_1", "id", STOP, 5, clock.now());
  assert.equal(decision.allowed, true);
  const reservation = (decision as { reservation: Reservation }).reservation;
  assert.deepEqual(reservation.parts.map((part) => part.units), [4, 1]);
  quota.settle(reservation, 5);
  assert.deepEqual(quota.collectReports().entries.map((entry) => entry.used), [4, 1]);
});

test("debt the server has acknowledged isn't charged again to a lease requested afterwards", async () => {
  const { clock, transport, quota } = setup();
  transport.onLeases = grant(10, inAnHour(clock));
  // Quota of 20: lease of 10, reserved 10, actually used 15, owing 5
  const { reservation } = (await quota.reserve("cons_1", "id", STOP, 10, clock.now())) as { reservation: Reservation };
  quota.settle(reservation, 15);
  // The batch carrying those 15 is confirmed by the server: what it has available is now 20 − 15 = 5
  const report = quota.collectReports();
  assert.deepEqual(report.entries, [{ lease_id: reservation.parts[0].lease.id, used: 15 }]);
  clock.mono += 1;
  quota.finishReport(report);
  // A lease requested after that (5) doesn't have to pay off the 5 again
  transport.onLeases = grant(5, inAnHour(clock));
  const decision = await quota.reserve("cons_1", "id", STOP, 5, clock.now());
  assert.equal(decision.allowed, true);
});

test("lease usage above the per-entry limit is reported in several entries", async () => {
  const { clock, transport, quota } = setup();
  transport.onLeases = grant(10, inAnHour(clock));
  const { reservation } = (await quota.reserve("cons_1", "id", STOP, 1, clock.now())) as { reservation: Reservation };
  quota.settle(reservation, 20_000_000_005);
  assert.deepEqual(quota.collectReports().entries.map((entry) => entry.used), [10_000_000_000, 10_000_000_000, 5]);
});

// ---- Found in review: serverless batched renewals also go out at most one batch a second ----
test("serverless renewals: a burst of renewNow() calls sends one batch now and one after a second, never one per call", async () => {
  const clock = new FakeClock();
  const transport = new FakeTransport();
  const quota = new QuotaManager({ clock, transport, warn: () => {}, onUnavailable: () => "allow", background: false });
  transport.onLeases = grant(10, inAnHour(clock));
  const consumers = Array.from({ length: 61 }, (_, index) => `cons_${index}`);
  for (const consumer of consumers) await quota.reserve(consumer, "id", STOP, 1, clock.now());
  const firstLeases = transport.leaseCalls.length;
  // 61 requests finish one after another, each queuing its renewal and calling renewNow(), with the clock standing still
  const pending: Promise<void>[] = [];
  for (const consumer of consumers) {
    for (let index = 0; index < 7; index++) allowed(quota.reserve(consumer, "id", STOP, 1, clock.now()));
    pending.push(quota.renewNow());
  }
  await settle();
  assert.equal(transport.leaseCalls.length - firstLeases, 1, "one batch right away");
  await clock.advance(1_000);
  await Promise.all(pending);
  assert.equal(transport.leaseCalls.length - firstLeases, 2, "the rest wait for the next second, in one batch");
});

// ---- Found in review (round three): more than 500 renewals go out a batch a second ----
test("serverless renewals: more than 500 queued go out 500 at a time, a second apart", async () => {
  const clock = new FakeClock();
  const transport = new FakeTransport();
  const quota = new QuotaManager({ clock, transport, warn: () => {}, onUnavailable: () => "allow", background: false });
  const leases = grant(10, inAnHour(clock));
  const sentAt: number[] = [];
  transport.onLeases = (request) => {
    sentAt.push(clock.monotonic());
    return leases(request);
  };
  const consumers = Array.from({ length: 501 }, (_, index) => `cons_${index}`);
  for (const consumer of consumers) await quota.reserve(consumer, "id", STOP, 1, clock.now());
  const firstLeases = transport.leaseCalls.length;
  // Every consumer uses enough to need renewing, queuing a renewal
  for (const consumer of consumers) for (let index = 0; index < 7; index++) allowed(quota.reserve(consumer, "id", STOP, 1, clock.now()));
  const renewed = quota.renewNow();
  await settle();
  const sizes = () => transport.leaseCalls.slice(firstLeases).map((call) => call.requests.length);
  assert.deepEqual(sizes(), [500]);
  await clock.advance(1_000);
  await renewed;
  assert.deepEqual(sizes(), [500, 1]);
  assert.equal(sentAt[sentAt.length - 1] - sentAt[sentAt.length - 2], 1_000);
});

// ---- Found while reviewing the Python SDK, same in Node: consumers with unsettled requests or unpaid debt aren't evicted ----
test("a consumer with debt or an unsettled request is not evicted when the state cache is full", async () => {
  const { clock, transport, quota } = setup();
  transport.onLeases = grant(10, inAnHour(clock));
  // cons_debt reserved 10 and used 100: owing 90. cons_running has a request still in progress
  quota.settle(allowed(await quota.reserve("cons_debt", "id", STOP, 10, clock.now())), 100);
  const running = allowed(await quota.reserve("cons_running", "id", STOP, 10, clock.now()));
  // Then 40,000 more customers arrive, each settling as they go: only idle ones are evicted
  for (let index = 0; index < MAX_QUOTA_CONSUMERS; index++) {
    quota.settle(allowed(await quota.reserve(`cons_${index}`, "id", STOP, 1, clock.now())), 1);
  }
  // cons_debt's debt is still there: every lease of 10 the server sends goes to it, so it's still blocked
  assert.equal((await quota.reserve("cons_debt", "id", STOP, 1, clock.now())).allowed, false);
  // cons_running's request ends late and uses too much: the debt lands on state still in memory, so the next request is still blocked
  quota.settle(running, 100);
  assert.equal((await quota.reserve("cons_running", "id", STOP, 1, clock.now())).allowed, false);
});

test("a consumer whose first request is still waiting for its lease is not evicted", async () => {
  const { clock, transport, quota } = setup();
  const leases = grant(10, inAnHour(clock));
  let answer!: (response: LeaseResponse) => void;
  transport.onLeases = (request) => (request.requests[0].consumer === "cons_waiting" ? new Promise<LeaseResponse>((resolve) => (answer = resolve)) : leases(request));
  const waiting = quota.reserve("cons_waiting", "id", STOP, 10, clock.now());
  assert.ok(waiting instanceof Promise);
  // 40,000 customers arrive while it waits
  for (let index = 0; index < MAX_QUOTA_CONSUMERS; index++) {
    quota.settle(allowed(await quota.reserve(`cons_${index}`, "id", STOP, 1, clock.now())), 1);
  }
  answer(leases({ requests: [{ consumer: "cons_waiting", consumer_kind: "id", reserve: 10, recent_usage_60s: 0, quota_version: 1 }] }));
  // The lease arrives, reserve 10, actually use 100: the debt lands on state still in memory, so the next request is still blocked
  quota.settle(allowed(await waiting), 100);
  transport.onLeases = leases;
  assert.equal((await quota.reserve("cons_waiting", "id", STOP, 1, clock.now())).allowed, false);
});

test("after a burst where every consumer was busy, the cache shrinks back to its limit once they finish", async () => {
  const { clock, transport, quota } = setup();
  transport.onLeases = grant(10, inAnHour(clock));
  // 40,002 customers all have requests in progress at once: none can be evicted, so the cache runs over for now
  const running: Reservation[] = [];
  for (let index = 0; index < MAX_QUOTA_CONSUMERS + 2; index++) running.push(allowed(await quota.reserve(`cons_${index}`, "id", STOP, 1, clock.now())));
  for (const reservation of running) quota.settle(reservation, 1);
  // Report this usage first, so the next batch has only the "lease closed" entries of evicted customers
  quota.finishReport(quota.collectReports());
  // A new customer arrives once they've all finished: evict until under the cap, 40,002 plus this 1 means 3 evicted
  allowed(await quota.reserve("cons_late", "id", STOP, 1, clock.now()));
  assert.equal(quota.collectReports().entries.filter((entry) => entry.closed).length, 3);
});

test("while every cached consumer is busy, a new consumer costs a short scan, not a pass over the whole cache", async () => {
  const { clock, transport, quota } = setup();
  transport.onLeases = grant(10, inAnHour(clock));
  for (let index = 0; index < MAX_QUOTA_CONSUMERS; index++) allowed(await quota.reserve(`cons_${index}`, "id", STOP, 1, clock.now()));
  // The table is full and all have requests in progress. Before, every new customer scanned and moved all 40,000, about a second for 500
  const started = performance.now();
  for (let index = 0; index < 500; index++) allowed(await quota.reserve(`cons_new_${index}`, "id", STOP, 1, clock.now()));
  const elapsed = performance.now() - started;
  assert.ok(elapsed < 300, `${elapsed.toFixed(0)} ms for 500 new consumers`);
});

