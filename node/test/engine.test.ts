import { test } from "node:test";
import assert from "node:assert/strict";
import { createHash, createHmac } from "node:crypto";
import { readFileSync } from "node:fs";

import { createEngine, type Decision, type EngineOptions, type Rejection } from "../src/engine.ts";
import { generateApiKey } from "../src/key-format.ts";
import { SDK_VERSION } from "../src/meter.ts";
import type { LeaseResult } from "../src/protocol.ts";
import { appSettings, consumerRecord, FakeClock, FakeTransport, keyRecord, silentLogger, syncPage } from "./helpers.ts";

const sha256 = (value: string) => createHash("sha256").update(value).digest("hex");

function engineWith(options: Partial<EngineOptions<{ user?: string }>> = {}) {
  const clock = new FakeClock();
  const transport = new FakeTransport();
  const engine = createEngine<{ user?: string }>({
    appKey: "urk_test_engine",
    auth: "api-key",
    clock,
    transport,
    backgroundLoops: false,
    logger: silentLogger,
    instanceId: "inst-1",
    ...options,
  });
  return { clock, transport, engine };
}

function admitted(decision: Decision | Promise<Decision>) {
  assert.ok(!(decision instanceof Promise), "expected a decision without waiting for the network");
  assert.equal(decision.allowed, true, `expected an admission, got ${JSON.stringify(decision)}`);
  return (decision as { handle: { track(units: number): void } }).handle;
}

function rejected(decision: Decision | Promise<Decision>): Rejection {
  assert.ok(!(decision instanceof Promise));
  assert.equal(decision.allowed, false);
  return decision as Rejection;
}

test("the SDK version sent with each batch matches package.json", () => {
  const pkg = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8")) as { version: string };
  assert.equal(SDK_VERSION, pkg.version);
});

test("with our keys: a synced key is checked locally, and the request is metered under its consumer", async () => {
  const key = generateApiKey({ prefix: "acme", environment: "test" });
  const { transport, engine } = engineWith();
  transport.onSync = () =>
    syncPage({
      app: appSettings({ routes: [{ method: "POST", route: "/v1/images", weight: 10, reserve: 10 }] }),
      keys: [keyRecord({ key_id: key.keyId, hash: sha256(key.key), consumer: "cons_1" })],
      consumers: [consumerRecord({ consumer: "cons_1" })],
    });
  await engine.sync();
  const get = admitted(engine.begin({ method: "get", path: "/v1/items/42", apiKey: key.key }));
  engine.end(get, { status: 200, route: "/v1/items/:id" });
  // Route weight: this route counts 10 per request
  const image = admitted(engine.begin({ method: "POST", path: "/v1/images", apiKey: key.key }));
  // In an app metering requests, track() doesn't count. Billing only trusts the template passed in, here at the end
  image.track(99);
  engine.end(image, { status: 201, route: "/v1/images" });
  const failed = admitted(engine.begin({ method: "GET", path: "/v1/items/43", apiKey: key.key }));
  engine.end(failed, { status: 404, route: "/v1/items/:id" });
  await engine.flush();
  assert.deepEqual(
    transport.rows().map((row) => [row.consumer, row.consumer_kind, row.method, row.route, row.result_type, row.count, row.units]),
    [
      ["cons_1", "id", "GET", "/v1/items/:id", "2xx", 1, 1],
      ["cons_1", "id", "POST", "/v1/images", "2xx", 1, 10],
      ["cons_1", "id", "GET", "/v1/items/:id", "4xx", 1, 0],
    ],
  );
  assert.deepEqual(transport.ingested[0].key_usage, [{ key_id: key.keyId, count: 3 }]);
  assert.equal(transport.verifyCalls.length, 0);
});

test("SDK rejections: unknown key 401, suspended consumer 403, rate limit 429 — recorded, never billed", async () => {
  const good = generateApiKey({ prefix: "acme", environment: "test" });
  const suspended = generateApiKey({ prefix: "acme", environment: "test" });
  const { transport, engine } = engineWith();
  transport.onSync = () =>
    syncPage({
      keys: [
        keyRecord({ key_id: good.keyId, hash: sha256(good.key), consumer: "cons_limited" }),
        keyRecord({ key_id: suspended.keyId, hash: sha256(suspended.key), consumer: "cons_suspended" }),
      ],
      consumers: [
        consumerRecord({ consumer: "cons_limited", rate_limits: { per_second: 1, per_minute: null } }),
        consumerRecord({ consumer: "cons_suspended", status: "suspended" }),
      ],
    });
  await engine.sync();

  const invalid = rejected(engine.begin({ method: "GET", path: "/x", apiKey: "nope" }));
  assert.deepEqual([invalid.status, invalid.error], [401, "invalid_api_key"]);
  const blocked = rejected(engine.begin({ method: "GET", path: "/x", apiKey: suspended.key }));
  assert.deepEqual([blocked.status, blocked.error], [403, "consumer_suspended"]);
  engine.end(admitted(engine.begin({ method: "GET", path: "/x", apiKey: good.key })), { status: 200 });
  const limited = rejected(engine.begin({ method: "GET", path: "/x", apiKey: good.key }));
  assert.deepEqual([limited.status, limited.error, limited.headers["Retry-After"]], [429, "rate_limited", "1"]);
  assert.ok(limited.headers["RateLimit-Policy"]);

  await engine.flush();
  assert.deepEqual(
    transport.rows().map((row) => [row.consumer, row.consumer_kind, row.result_type, row.units]),
    [
      ["", "anonymous", "sdk_401", 0],
      ["cons_suspended", "id", "sdk_403", 0],
      ["cons_limited", "id", "2xx", 1],
      ["cons_limited", "id", "sdk_429", 0],
    ],
  );
});

test("stop-at-limit consumers reserve from a lease, and the lease usage goes out with the next batch", async () => {
  const key = generateApiKey({ prefix: "acme", environment: "test" });
  const { clock, transport, engine } = engineWith();
  transport.onSync = () =>
    syncPage({
      app: appSettings({ unit_mode: "units" }),
      keys: [keyRecord({ key_id: key.keyId, hash: sha256(key.key), consumer: "cons_stop", quota_behavior: "stop", quota_version: 4 })],
      consumers: [consumerRecord({ consumer: "cons_stop", quota_behavior: "stop", quota_version: 4 })],
    });
  transport.onLeases = (request) => ({
    results: request.requests.map((item): LeaseResult => ({
      consumer: item.consumer,
      status: "granted",
      lease: { lease_id: "11", consumer: item.consumer, granted: 50, quota_version: 4, behavior: "stop", period_end: new Date(clock.now() + 3_600_000).toISOString() },
    })),
  });
  await engine.sync();
  const pending = engine.begin({ method: "POST", path: "/v1/generate", apiKey: key.key });
  assert.ok(pending instanceof Promise);
  const decision = await pending;
  assert.equal(decision.allowed, true);
  if (!decision.allowed) return;
  // Metering units: track() wins
  decision.handle.track(25);
  engine.end(decision.handle, { status: 200, route: "/v1/generate" });
  await engine.flush();
  const [batch] = transport.ingested;
  assert.deepEqual(batch.lease_used, [{ lease_id: "11", used: 25 }]);
  assert.deepEqual(batch.rows.map((row) => row.units), [25]);
  assert.equal(transport.leaseCalls[0].requests[0].quota_version, 4);
});

test("with your own auth: raw IDs are held until the hash key arrives, then reported as HMAC-SHA256 hex", async () => {
  const { transport, engine } = engineWith({ auth: "custom", consumer: (context) => (context.user ? { id: context.user } : null) });
  // Not synced yet: don't know whether to hash, so let it through and hold on to it
  engine.end(admitted(engine.begin({ method: "GET", path: "/x", context: { user: "user-1" } })), { status: 200, route: "/x" });
  engine.end(admitted(engine.begin({ method: "GET", path: "/x", context: {} })), { status: 200, route: "/x" });
  transport.onSync = () => syncPage({ app: appSettings({ hash_key: "secret-hash-key" }) });
  await engine.sync();
  // The sync has no record of this consumer: the first time this instance sees it, it gets a lease synchronously, and the server says it's unlimited
  const first = engine.begin({ method: "GET", path: "/x", context: { user: "user-1" } });
  assert.ok(first instanceof Promise);
  const decision = await first;
  assert.equal(decision.allowed, true);
  if (decision.allowed) engine.end(decision.handle, { status: 200, route: "/x" });
  // Cached as "unlimited" from then on, no more waiting
  engine.end(admitted(engine.begin({ method: "GET", path: "/x", context: { user: "user-1" } })), { status: 200, route: "/x" });
  await engine.flush();
  const hashed = createHmac("sha256", "secret-hash-key").update("user-1", "utf8").digest("hex");
  assert.deepEqual(
    transport.rows().map((row) => [row.consumer, row.consumer_kind, row.count]).sort(),
    [["", "anonymous", 1], [hashed, "external", 3]],
  );
});

test("with your own auth: synced records are matched by external_id for rate limits and suspension", async () => {
  const hash = (id: string) => createHmac("sha256", "k").update(id, "utf8").digest("hex");
  const { transport, engine } = engineWith({ auth: "custom", consumer: (context) => ({ id: context.user ?? "" }) });
  transport.onSync = () =>
    syncPage({
      app: appSettings({ hash_key: "k" }),
      consumers: [
        consumerRecord({ consumer: "cons_a", external_id: hash("alice"), rate_limits: { per_second: 1, per_minute: null } }),
        consumerRecord({ consumer: "cons_b", external_id: hash("bob"), status: "suspended" }),
      ],
    });
  await engine.sync();
  admitted(engine.begin({ method: "GET", path: "/x", context: { user: "alice" } }));
  assert.equal(rejected(engine.begin({ method: "GET", path: "/x", context: { user: "alice" } })).error, "rate_limited");
  assert.equal(rejected(engine.begin({ method: "GET", path: "/x", context: { user: "bob" } })).error, "consumer_suspended");
});

test("a new key that can't be verified follows onKeyStoreUnavailable: deny is a 503, allow lets it through", async () => {
  for (const policy of ["deny", "allow"] as const) {
    const { transport, engine } = engineWith({ onKeyStoreUnavailable: policy });
    transport.onVerify = () => {
      throw new Error("connection refused");
    };
    const decision = await engine.begin({ method: "GET", path: "/x", apiKey: generateApiKey({ prefix: "acme", environment: "test" }).key });
    if (policy === "deny") {
      assert.deepEqual([decision.allowed, !decision.allowed && decision.status, !decision.allowed && decision.error], [false, 503, "auth_unavailable"]);
    } else {
      assert.equal(decision.allowed, true);
    }
  }
});

test("close() reports what is left, including the final 'lease closed'", async () => {
  const key = generateApiKey({ prefix: "acme", environment: "test" });
  const { clock, transport, engine } = engineWith();
  transport.onSync = () =>
    syncPage({ keys: [keyRecord({ key_id: key.keyId, hash: sha256(key.key), consumer: "cons_stop", quota_behavior: "stop", quota_version: 1 })] });
  transport.onLeases = (request) => ({
    results: request.requests.map((item): LeaseResult => ({
      consumer: item.consumer,
      status: "granted",
      lease: { lease_id: "21", consumer: item.consumer, granted: 10, quota_version: 1, behavior: "stop", period_end: new Date(clock.now() + 3_600_000).toISOString() },
    })),
  });
  await engine.sync();
  const decision = await engine.begin({ method: "GET", path: "/x", apiKey: key.key });
  assert.equal(decision.allowed, true);
  if (decision.allowed) engine.end(decision.handle, { status: 200 });
  await engine.close();
  assert.deepEqual(
    transport.ingested.map((batch) => batch.lease_used),
    [[{ lease_id: "21", used: 1 }], [{ lease_id: "21", used: 0, closed: true }]],
  );
});

test("a consumer that disappears from a full sync is asked about again, not let through on a cached 'unlimited'", async () => {
  const { transport, engine } = engineWith({ auth: "custom", consumer: (context) => ({ id: context.user ?? "" }) });
  const hashed = createHmac("sha256", "k").update("alice", "utf8").digest("hex");
  transport.onSync = () => syncPage({ app: appSettings({ hash_key: "k" }) });
  await engine.sync();
  // First sighting: the server says unlimited, and that's cached
  assert.equal((await engine.begin({ method: "GET", path: "/x", context: { user: "alice" } })).allowed, true);
  // The seller suspends it: an incremental sync brings the suspended record
  transport.onSync = () => syncPage({ full: false, app: appSettings({ hash_key: "k" }), consumers: [consumerRecord({ consumer: "cons_a", external_id: hashed, status: "suspended" })] });
  await engine.sync();
  assert.equal(rejected(engine.begin({ method: "GET", path: "/x", context: { user: "alice" } })).error, "consumer_suspended");
  // Then deletes it: it's gone from the full sync. The cached "unlimited" can't be used anymore; ask the server again
  transport.onSync = () => syncPage({ app: appSettings({ hash_key: "k" }) });
  transport.onLeases = (request) => ({ results: request.requests.map((item): LeaseResult => ({ consumer: item.consumer, status: "suspended", quota_version: 2 })) });
  await engine.sync();
  const after = engine.begin({ method: "GET", path: "/x", context: { user: "alice" } });
  assert.ok(after instanceof Promise);
  const decision = await after;
  assert.deepEqual([decision.allowed, !decision.allowed && decision.error], [false, "consumer_suspended"]);
});

test("drain() reports every second while the app is shutting down", async () => {
  const { clock, transport, engine } = engineWith({ auth: "custom", consumer: () => null });
  engine.end(admitted(engine.begin({ method: "GET", path: "/x", context: {} })), { status: 200, route: "/x" });
  engine.drain();
  await clock.advance(0);
  assert.equal(transport.ingested.length, 1);
  // Another request finished during the drain: reported within 1 second
  engine.end(admitted(engine.begin({ method: "GET", path: "/x", context: {} })), { status: 200, route: "/x" });
  await clock.advance(1_000);
  assert.equal(transport.ingested.length, 2);
  await engine.close();
});

test("streamStarted counts a stream once at the start; in units mode its track() units are added when it ends", async () => {
  const { transport, engine } = engineWith({ auth: "custom", consumer: () => ({ id: "user-1" }) });
  transport.onSync = () => syncPage({ app: appSettings({ unit_mode: "units" }) });
  await engine.sync();
  const decision = await engine.begin({ method: "GET", path: "/v1/chat", context: {} });
  assert.equal(decision.allowed, true);
  if (!decision.allowed) return;
  engine.streamStarted(decision.handle, "/v1/chat");
  await engine.flush();
  assert.deepEqual(transport.rows().map((row) => [row.result_type, row.count, row.units]), [["ws_open", 1, 0]]);
  decision.handle.track(42);
  engine.end(decision.handle, { status: 200, route: "/v1/chat" });
  await engine.flush();
  assert.deepEqual(transport.rows().slice(1).map((row) => [row.result_type, row.count, row.units]), [["ws_open", 0, 42]]);
});

test("a suspended consumer is blocked even when the app has more consumers than the local record limit", async () => {
  const key = generateApiKey({ prefix: "acme", environment: "test" });
  const { transport, engine } = engineWith();
  // 100,000 active consumer records fill the local cap, and this key's consumer comes after them, so the first sync can't store its record
  const crowd = Array.from({ length: 100_000 }, (_, index) => consumerRecord({ consumer: `cons_${String(index).padStart(6, "0")}` }));
  transport.onSync = () => syncPage({ keys: [keyRecord({ key_id: key.keyId, hash: sha256(key.key), consumer: "cons_zz_late" })], consumers: crowd });
  await engine.sync();
  assert.equal(admitted(engine.begin({ method: "GET", path: "/x", apiKey: key.key })) !== undefined, true);
  transport.onSync = () => syncPage({ full: false, keys: [], consumers: [consumerRecord({ consumer: "cons_zz_late", status: "suspended" })] });
  await engine.sync();
  assert.equal(rejected(engine.begin({ method: "GET", path: "/x", apiKey: key.key })).error, "consumer_suspended");
});

// ---- Found in review: a stream's units go in the minute it started (same period as the lease); too-old minutes move to the earliest one the server still accepts ----
test("a stream's units are recorded in the minute it started, the period its reservation came from", async () => {
  const { clock, transport, engine } = engineWith({ auth: "custom", consumer: () => ({ id: "user-1" }) });
  transport.onSync = () => syncPage({ app: appSettings({ unit_mode: "units" }) });
  await engine.sync();
  const decision = await engine.begin({ method: "GET", path: "/v1/chat", context: {} });
  assert.equal(decision.allowed, true);
  if (!decision.allowed) return;
  engine.streamStarted(decision.handle, "/v1/chat");
  await engine.flush();
  // It ends two hours later, crossing the hour (at month end, that would be a new period)
  await clock.advance(2 * 3_600_000);
  decision.handle.track(42);
  engine.end(decision.handle, { status: 200, route: "/v1/chat" });
  await engine.flush();
  const [opened, units] = transport.rows();
  assert.deepEqual([opened.count, units.count, units.units], [1, 0, 42]);
  assert.equal(units.minute, opened.minute);
});

test("a stream that ends 25 hours after it started records its units in the oldest minute the server still accepts", async () => {
  const { clock, transport, engine } = engineWith({ auth: "custom", consumer: () => ({ id: "user-1" }) });
  transport.onSync = () => syncPage({ app: appSettings({ unit_mode: "units" }) });
  await engine.sync();
  const decision = await engine.begin({ method: "GET", path: "/v1/chat", context: {} });
  assert.equal(decision.allowed, true);
  if (!decision.allowed) return;
  engine.streamStarted(decision.handle, "/v1/chat");
  await engine.flush();
  await clock.advance(25 * 3_600_000);
  decision.handle.track(42);
  engine.end(decision.handle, { status: 200, route: "/v1/chat" });
  await engine.flush();
  const [, units] = transport.rows();
  const age = clock.now() - Date.parse(units.minute);
  assert.ok(age <= 23 * 3_600_000 && age > 23 * 3_600_000 - 120_000, `${age} ms old`);
});

test("a customer whose record doesn't fit in the cache still has its cached quota decision follow a plan change", async () => {
  const hash = (id: string) => createHmac("sha256", "k").update(id, "utf8").digest("hex");
  const { transport, engine } = engineWith({ auth: "custom", consumer: (context) => ({ id: context.user ?? "" }) });
  // Records are full: 100,000 customers
  transport.onSync = () =>
    syncPage({ app: appSettings({ hash_key: "k" }), consumers: Array.from({ length: 100_000 }, (_, index) => consumerRecord({ consumer: `cons_${index}`, external_id: hash(`user-${index}`) })) });
  await engine.sync();
  // alice's record can't be stored; the server says she's unlimited, and that's remembered locally
  const first = await engine.begin({ method: "GET", path: "/x", context: { user: "alice" } });
  assert.equal(first.allowed, true);
  if (first.allowed) engine.end(first.handle, { status: 200 });
  // Incremental sync: she moves to a plan with a hard limit, and her quota is already used up
  transport.onSync = () =>
    syncPage({ full: false, app: appSettings({ hash_key: "k" }), consumers: [consumerRecord({ consumer: "cons_alice", external_id: hash("alice"), quota_behavior: "stop", quota_version: 2 })] });
  await engine.sync();
  transport.onLeases = (request) => ({
    results: request.requests.map((item): LeaseResult => ({ consumer: item.consumer, status: "exhausted", behavior: "stop", quota_version: 2, retry_after: 60 })),
  });
  const decision = await engine.begin({ method: "GET", path: "/x", context: { user: "alice" } });
  assert.deepEqual([decision.allowed, !decision.allowed && decision.error], [false, "quota_exceeded"]);
});

test("a suspension stays enforced for a customer whose record doesn't fit, even after a late sync record still says active", async () => {
  const hash = (id: string) => createHmac("sha256", "k").update(id, "utf8").digest("hex");
  const { transport, engine } = engineWith({ auth: "custom", consumer: (context) => ({ id: context.user ?? "" }) });
  const settings = appSettings({ hash_key: "k" });
  // Records are full: 100,000 customers
  transport.onSync = () => syncPage({ app: settings, consumers: Array.from({ length: 100_000 }, (_, index) => consumerRecord({ consumer: `cons_${index}`, external_id: hash(`user-${index}`) })) });
  await engine.sync();
  // alice's record can't be stored; the lease request says she's suspended
  transport.onLeases = (request) => ({ results: request.requests.map((item): LeaseResult => ({ consumer: item.consumer, status: "suspended", quota_version: 1 })) });
  const first = await engine.begin({ method: "GET", path: "/x", context: { user: "alice" } });
  assert.deepEqual([first.allowed, !first.allowed && first.error], [false, "consumer_suspended"]);
  // A late sync record still says active (can't be stored), then the next sync brings the suspension: suspended records aren't capped, so it's stored and still blocks
  for (const status of ["active", "suspended"] as const) {
    transport.onSync = () => syncPage({ full: false, app: settings, consumers: [consumerRecord({ consumer: "cons_alice", external_id: hash("alice"), quota_version: 1, status })] });
    await engine.sync();
  }
  const decision = await engine.begin({ method: "GET", path: "/x", context: { user: "alice" } });
  assert.deepEqual([decision.allowed, !decision.allowed && decision.error], [false, "consumer_suspended"]);
});

