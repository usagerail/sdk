import { test } from "node:test";
import assert from "node:assert/strict";

import {
  classifyStatus,
  MAX_BACKOFF_MS,
  MAX_QUEUE_AGE_MS,
  MAX_ROUTES_PER_MINUTE,
  MAX_TABLE_KEYS,
  Meter,
  PAUSED_RETRY_MS,
  PENDING_MAX_AGE_MS,
  type LeaseReporter,
  type Usage,
} from "../src/meter.ts";
import { MAX_ROW_COUNT, MAX_ROW_UNITS, type IngestRequest, type IngestResponse } from "../src/protocol.ts";
import type { LeaseReport } from "../src/quota.ts";
import { TransportError } from "../src/transport.ts";
import { FakeClock, FakeTransport } from "./helpers.ts";

const MINUTE = Date.UTC(2026, 9, 20, 12, 0, 0);

function noLeases(): LeaseReporter {
  return { collectReports: () => ({ entries: [], used: [], closed: [], debts: [] }), finishReport: () => {}, forgetLeases: () => {} };
}

function setup(options: { leases?: LeaseReporter; hash?: (raw: string) => string } = {}) {
  const clock = new FakeClock();
  const transport = new FakeTransport();
  let resolver: ((raw: string) => string) | null = options.hash ?? null;
  const meter = new Meter({
    clock,
    transport,
    instanceId: "inst-1",
    warn: () => {},
    leases: options.leases ?? noLeases(),
    resolvePending: () => resolver,
  });
  return { clock, transport, meter, setHash: (hash: (raw: string) => string) => (resolver = hash) };
}

function usage(overrides: Partial<Usage> = {}): Usage {
  return { consumer: "cons_1", kind: "id", stripeCustomerId: undefined, method: "GET", route: "/v1/items/:id", outcome: "2xx", minuteStart: MINUTE, units: 1, ...overrides };
}

test("outcomes: 101 opens a connection, anything unexpected counts as 5xx", () => {
  assert.deepEqual([101, 200, 204, 301, 304, 400, 404, 429, 500, 503, 99, 600].map(classifyStatus), [
    "ws_open", "2xx", "2xx", "3xx", "3xx", "4xx", "4xx", "4xx", "5xx", "5xx", "5xx", "5xx",
  ]);
});

test("usage aggregates by consumer, endpoint, outcome and minute, with request count and units", async () => {
  const { transport, meter } = setup();
  meter.record(usage({ units: 3 }), false);
  meter.record(usage({ units: 4 }), false);
  meter.record(usage({ outcome: "4xx", units: 0 }), false);
  meter.record(usage({ minuteStart: MINUTE + 60_000 }), false);
  meter.record(usage({ consumer: "", kind: "anonymous" }), false);
  meter.recordKeyUse("Key00001");
  meter.recordKeyUse("Key00001");
  await meter.flush();
  assert.equal(transport.ingested.length, 1);
  const [batch] = transport.ingested;
  assert.equal(batch.batch_id, "inst-1:1");
  assert.deepEqual(batch.key_usage, [{ key_id: "Key00001", count: 2 }]);
  assert.deepEqual(batch.rows, [
    { consumer: "cons_1", method: "GET", route: "/v1/items/:id", result_type: "2xx", minute: "2026-10-20T12:00:00.000Z", count: 2, units: 7, consumer_kind: "id" },
    { consumer: "cons_1", method: "GET", route: "/v1/items/:id", result_type: "4xx", minute: "2026-10-20T12:00:00.000Z", count: 1, units: 0, consumer_kind: "id" },
    { consumer: "cons_1", method: "GET", route: "/v1/items/:id", result_type: "2xx", minute: "2026-10-20T12:01:00.000Z", count: 1, units: 1, consumer_kind: "id" },
    // Anonymous request: analytics only (protocol.ts)
    { consumer: "", method: "GET", route: "/v1/items/:id", result_type: "2xx", minute: "2026-10-20T12:00:00.000Z", count: 1, units: 1, consumer_kind: "anonymous" },
  ]);
});

test("a table that reaches 5,000 keys is reported at once, without waiting for the minute", async () => {
  const { clock, transport, meter } = setup();
  for (let index = 0; index < MAX_TABLE_KEYS; index++) meter.record(usage({ consumer: `cons_${index}` }), false);
  await clock.advance(0);
  assert.equal(transport.ingested.length, 1);
  assert.equal(transport.ingested[0].rows.length, MAX_TABLE_KEYS);
});

test("past 500 distinct routes in a minute, new routes are reported as _other", async () => {
  const { transport, meter } = setup();
  for (let index = 0; index <= MAX_ROUTES_PER_MINUTE; index++) meter.record(usage({ route: `/r/${index}` }), false);
  meter.record(usage({ route: "/r/0" }), false);
  // Counting starts over in the next minute
  meter.record(usage({ route: "/next-minute", minuteStart: MINUTE + 60_000 }), false);
  await meter.flush();
  const routes = transport.rows().map((row) => row.route);
  assert.equal(routes.filter((route) => route === "_other").length, 1);
  assert.ok(routes.includes("/r/499"));
  assert.ok(!routes.includes("/r/500"));
  assert.ok(routes.includes("/next-minute"));
});

test("a failed batch is retried with the same batch_id after a backoff", async () => {
  const { clock, transport, meter } = setup();
  let failures = 1;
  transport.onIngest = () => {
    if (failures-- > 0) throw new TransportError("POST /v1/ingest returned 503", 503);
    return { duplicate: false, accepted_rows: 1, rejected_rows: [], unknown_keys: [], unknown_leases: [] };
  };
  meter.record(usage(), false);
  await meter.flush();
  assert.equal(transport.ingested.length, 1);
  await clock.advance(999);
  assert.equal(transport.ingested.length, 1);
  await clock.advance(1);
  assert.deepEqual(transport.ingested.map((batch) => batch.batch_id), ["inst-1:1", "inst-1:1"]);
});

test("413 splits a batch: billing rows go first in halves, then lease, key and loss data on their own", async () => {
  const leases: LeaseReporter = {
    collectReports: (): LeaseReport => ({ entries: [{ lease_id: "7", used: 3 }], used: [], closed: [], debts: [] }),
    finishReport: () => {},
    forgetLeases: () => {},
  };
  const { transport, meter } = setup({ leases });
  let first = true;
  transport.onIngest = () => {
    if (first) {
      first = false;
      throw new TransportError("POST /v1/ingest returned 413", 413);
    }
    return { duplicate: false, accepted_rows: 0, rejected_rows: [], unknown_keys: [], unknown_leases: [] };
  };
  for (let index = 0; index < 4; index++) meter.record(usage({ consumer: `cons_${index}` }), false);
  meter.recordKeyUse("Key00001");
  await meter.flush();
  const [original, ...parts] = transport.ingested;
  assert.equal(original.batch_id, "inst-1:1");
  // Sub-batches from a split each get a new sequence number
  assert.deepEqual(parts.map((batch) => batch.batch_id), ["inst-1:2", "inst-1:3", "inst-1:4"]);
  assert.deepEqual(parts.map((batch) => batch.rows.length), [2, 2, 0]);
  assert.deepEqual(parts.map((batch) => batch.lease_used.length + batch.key_usage.length), [0, 0, 2]);
});

test("413 on a batch holding one key-usage entry and one lease entry sends them apart instead of resending the same batch", async () => {
  const leases: LeaseReporter = {
    collectReports: (): LeaseReport => ({ entries: [{ lease_id: "7", used: 3 }], used: [], closed: [], debts: [] }),
    finishReport: () => {},
    forgetLeases: () => {},
  };
  const { transport, meter } = setup({ leases });
  // The server only takes one entry per batch: two get a 413. It rejects at most 10 times, so the test still ends if splitting doesn't work
  transport.onIngest = (batch) => {
    if (transport.ingested.length <= 10 && batch.key_usage.length + batch.lease_used.length > 1) throw new TransportError("POST /v1/ingest returned 413", 413);
    return { duplicate: false, accepted_rows: 0, rejected_rows: [], unknown_keys: [], unknown_leases: [] };
  };
  meter.recordKeyUse("Key00001");
  await meter.flush();
  // Key usage first, lease usage after
  assert.deepEqual(transport.ingested.map((batch) => [batch.key_usage.length, batch.lease_used.length]), [[1, 1], [1, 0], [0, 1]]);
});

test("batches older than 10 minutes are given up and counted per consumer as known loss in the next batch", async () => {
  const { clock, transport, meter } = setup();
  transport.onIngest = () => {
    throw new TransportError("POST /v1/ingest failed", null);
  };
  meter.record(usage({ consumer: "cons_lost", units: 5 }), false);
  meter.record(usage({ consumer: "cons_lost", outcome: "4xx", units: 0 }), false);
  await meter.flush();
  await clock.advance(MAX_QUEUE_AGE_MS + 1);
  transport.onIngest = () => ({ duplicate: false, accepted_rows: 0, rejected_rows: [], unknown_keys: [], unknown_leases: [] });
  // The first flush gives up on that batch, the second puts the loss in a new batch; if the backoff hasn't passed yet, it waits
  await meter.flush();
  await meter.flush();
  await clock.advance(MAX_BACKOFF_MS);
  const last = transport.ingested.at(-1)!;
  assert.notEqual(last.batch_id, "inst-1:1");
  // Only billable outcomes count as losses
  assert.deepEqual(last.dropped, [{ consumer: "cons_lost", billable_requests: 1, units: 5 }]);
});

test("rows the server rejects become known loss in the next batch, attributed to their consumer", async () => {
  const { transport, meter } = setup();
  transport.onIngest = (batch) => ({
    duplicate: false,
    accepted_rows: 0,
    rejected_rows: batch.rows.length ? [{ index: 0, reason: "unknown_consumer" }] : [],
    unknown_keys: [],
    unknown_leases: [],
  });
  meter.record(usage({ consumer: "cons_gone", units: 2 }), false);
  await meter.flush();
  await meter.flush();
  assert.deepEqual(transport.ingested[1].dropped, [{ consumer: "cons_gone", billable_requests: 1, units: 2 }]);
});

test("a batch the server can't parse (400) is dropped and counted as unattributed loss", async () => {
  const { transport, meter } = setup();
  transport.onIngest = (batch) => {
    if (batch.rows.length > 0) throw new TransportError("POST /v1/ingest returned 400", 400);
    return { duplicate: false, accepted_rows: 0, rejected_rows: [], unknown_keys: [], unknown_leases: [] };
  };
  meter.record(usage({ consumer: "cons_bad", units: 9 }), false);
  await meter.flush();
  await meter.flush();
  // A 400 may be caused by the consumer ID itself, so don't record by consumer, or the next batch would carry the same ID and be rejected again
  assert.deepEqual(transport.ingested[1].dropped, []);
  assert.deepEqual(transport.ingested[1].unattributed_dropped, { billable_requests: 1, units: 9 });
});

test("a 401 pauses reporting for 10 minutes instead of retrying quickly", async () => {
  const { clock, transport, meter } = setup();
  transport.onIngest = () => {
    throw new TransportError("POST /v1/ingest returned 401", 401);
  };
  meter.record(usage(), false);
  await meter.flush();
  await clock.advance(PAUSED_RETRY_MS - 1);
  assert.equal(transport.ingested.length, 1);
  await clock.advance(1);
  assert.equal(transport.ingested.length, 2);
});

test("raw external IDs wait for the hash key, then are reported hashed; after 10 minutes they become unattributed loss", async () => {
  const { clock, transport, meter, setHash } = setup();
  meter.record(usage({ consumer: "user-42", kind: "external", units: 2 }), true);
  await meter.flush();
  assert.equal(transport.ingested.length, 0);
  setHash((raw) => `hashed:${raw}`);
  meter.record(usage({ consumer: "hashed:user-42", kind: "external", units: 1 }), false);
  await meter.flush();
  assert.deepEqual(
    transport.rows().map((row) => [row.consumer, row.consumer_kind, row.count, row.units]),
    [["hashed:user-42", "external", 2, 3]],
  );

  const late = setup();
  late.meter.record(usage({ consumer: "user-7", kind: "external", units: 4 }), true);
  await late.clock.advance(PENDING_MAX_AGE_MS + 1);
  late.meter.recordKeyUse("Key00001");
  await late.meter.flush();
  const batch = late.transport.ingested.at(-1)!;
  assert.deepEqual(batch.rows, []);
  assert.deepEqual(batch.unattributed_dropped, { billable_requests: 1, units: 4 });
});

test("an aggregate about to pass a row's limits is sealed and a new row started, so the server accepts both", async () => {
  const { transport, meter } = setup();
  // 10,000,000 units per request: 1,000 of them hit the row cap of 10,000,000,000 exactly, the 1,001st starts a new row
  for (let index = 0; index < 1_001; index++) meter.record(usage({ units: 10_000_000 }), false);
  await meter.flush();
  assert.deepEqual(transport.rows().map((row) => [row.count, row.units]), [[1_000, 10_000_000_000], [1, 10_000_000]]);
});

// ---- Found while reviewing the Python SDK, same in Node: losses split by the per-entry cap, and what doesn't fit in one batch goes in another ----
// Server validation: each loss entry has at most 1 billion billable requests and 10 billion units, at most 1,000 entries a batch, otherwise the whole batch gets a 400.
// The first batch carrying usage is rejected whole, so that usage becomes the next batch's losses
function rejectFirstRowsThenCheckLoss() {
  let rejected = false;
  return (batch: IngestRequest): IngestResponse => {
    if (batch.dropped.length > 1_000 || batch.dropped.some((entry) => entry.billable_requests > MAX_ROW_COUNT * 100 || entry.units > MAX_ROW_UNITS)) {
      throw new TransportError("POST /v1/ingest returned 400", 400);
    }
    const rejectAll = batch.rows.length > 0 && !rejected;
    if (rejectAll) rejected = true;
    return {
      duplicate: false,
      accepted_rows: 0,
      rejected_rows: rejectAll ? batch.rows.map((_, index) => ({ index, reason: "unknown_consumer" })) : [],
      unknown_keys: [],
      unknown_leases: [],
    };
  };
}

test("known loss above the per-entry limits is split, so the batch carrying it is still accepted", async () => {
  const { transport, meter } = setup();
  transport.onIngest = rejectFirstRowsThenCheckLoss();
  // 1,001 requests of 10 million units each, rejected by the server: 10.01 billion units of losses in total, over the per-entry cap
  for (let index = 0; index < 1001; index++) meter.record(usage({ consumer: "cons_big", units: 10_000_000 }), false);
  await meter.flush();
  // The batch carrying this loss also has valid usage, which must not be rejected along with it
  meter.record(usage({ consumer: "cons_ok", units: 3 }), false);
  await meter.flush();
  const recovery = transport.ingested[transport.ingested.length - 1];
  assert.deepEqual(recovery.rows.map((row) => [row.consumer, row.units]), [["cons_ok", 3]]);
  assert.deepEqual(recovery.dropped, [
    { consumer: "cons_big", billable_requests: 1001, units: 10_000_000_000 },
    { consumer: "cons_big", billable_requests: 0, units: 10_000_000 },
  ]);
});

test("known loss that needs more than 1,000 entries goes out in several batches", async () => {
  const { transport, meter } = setup();
  transport.onIngest = rejectFirstRowsThenCheckLoss();
  // 1,000 customers each lost 15 billion units: each splits into two entries, 2,000 in total
  for (let index = 0; index < 1000; index++) {
    for (let request = 0; request < 2; request++) meter.record(usage({ consumer: `cons_${index}`, units: 7_500_000_000 }), false);
  }
  await meter.flush();
  await meter.flush();
  const batches = transport.ingested.slice(1).filter((batch) => batch.dropped.length > 0);
  assert.deepEqual(batches.map((batch) => batch.dropped.length), [1000, 1000]);
  assert.equal(batches.flatMap((batch) => batch.dropped).reduce((sum, entry) => sum + entry.units, 0), 1000 * 15_000_000_000);
});
