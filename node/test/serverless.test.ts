import { test } from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash, createHmac } from "node:crypto";

import { Hono } from "hono";

import { createEngine, type Decision, type EngineOptions } from "../src/engine.ts";
import { usageRail as honoUsageRail, type UsageRailVariables } from "../src/hono.ts";
import { generateApiKey } from "../src/key-format.ts";
import { usageRail } from "../src/next.ts";
import type { IngestResponse } from "../src/protocol.ts";
import { createFetchTransport, TransportError } from "../src/transport.ts";
import { accepted, appSettings, consumerRecord, FakeClock, FakeTransport, keyRecord, settle, silentLogger, syncPage } from "./helpers.ts";

const sha256 = (value: string) => createHash("sha256").update(value).digest("hex");

function serverless(options: Partial<EngineOptions> = {}) {
  const key = generateApiKey({ prefix: "acme", environment: "test" });
  const clock = new FakeClock();
  const transport = new FakeTransport();
  transport.onSync = () => syncPage({ app: appSettings({ unit_mode: "units" }), keys: [keyRecord({ key_id: key.keyId, hash: sha256(key.key), consumer: "cons_1" })] });
  const engine = createEngine({ appKey: "urk_test_serverless", auth: "api-key", clock, transport, logger: silentLogger, instanceId: "inst-1", serverless: true, ...options });
  return { key: key.key, keyId: key.keyId, clock, transport, engine };
}

async function finished(decision: Decision | Promise<Decision>) {
  const resolved = await decision;
  assert.equal(resolved.allowed, true);
  return (resolved as { handle: { track(units: number): void } }).handle;
}

function gate<T>() {
  let release!: (value: T) => void;
  const promise = new Promise<T>((resolve) => (release = resolve));
  return { promise, release };
}

test("serverless: nothing runs in the background; a cold start waits for the first sync, at most a second", async () => {
  const { key, clock, transport, engine } = serverless();
  const first = gate<ReturnType<typeof syncPage>>();
  const syncResponse = transport.onSync;
  transport.onSync = () => first.promise;
  // Creating the engine does nothing: no sync, no timers
  assert.equal(transport.syncCalls.length, 0);
  const pending = engine.begin({ method: "GET", path: "/x", apiKey: key });
  assert.ok(pending instanceof Promise);
  assert.equal(transport.syncCalls.length, 1);
  first.release(await syncResponse(null));
  assert.equal((await pending).allowed, true);
  // The sync brought a snapshot: the key is judged locally, no remote verification
  assert.equal(transport.verifyCalls.length, 0);

  // Sync never answers: after a full second, fall back to remote verification
  const slow = serverless();
  slow.transport.onSync = () => new Promise(() => {});
  let decided = false;
  void Promise.resolve(slow.engine.begin({ method: "GET", path: "/x", apiKey: slow.key })).then(() => (decided = true));
  await slow.clock.advance(999);
  assert.equal(decided, false);
  await slow.clock.advance(1);
  await settle();
  assert.equal(decided, true);
  assert.equal(slow.transport.verifyCalls.length, 1);
  void clock;
});

test("serverless: submissions are at least 0.5 s apart, requests that end meanwhile ride the next one, and the last request carries the tail", async () => {
  const { key, clock, transport, engine } = serverless();
  await engine.sync();
  const gates: ReturnType<typeof gate<IngestResponse>>[] = [];
  transport.onIngest = () => {
    const next = gate<IngestResponse>();
    gates.push(next);
    return next.promise;
  };
  const end = async (route: string) => engine.end(await finished(engine.begin({ method: "GET", path: route, apiKey: key })), { status: 200, route });

  await end("/a");
  const afterA = engine.afterResponse();
  await settle();
  assert.equal(transport.ingested.length, 1, "the first request's counts go out right away");

  // The first batch is still in flight: B and C finish and queue behind it, sharing the next submission
  await end("/b");
  const afterB = engine.afterResponse();
  await end("/c");
  const afterC = engine.afterResponse();
  gates[0].release(accepted());
  await afterA;
  await settle();
  assert.equal(transport.ingested.length, 1, "the next submission waits for the 0.5 s gap");
  await clock.advance(499);
  assert.equal(transport.ingested.length, 1);
  await clock.advance(1);
  assert.equal(transport.ingested.length, 2);
  gates[1].release(accepted());
  await Promise.all([afterB, afterC]);
  assert.deepEqual(transport.ingested.map((batch) => batch.rows.map((row) => row.route).sort()), [["/a"], ["/b", "/c"]]);
});

test("serverless: a failed submission stays in memory and the next request retries it, without backing off", async () => {
  const { key, clock, transport, engine } = serverless();
  await engine.sync();
  let fail = true;
  transport.onIngest = () => {
    if (fail) throw new TransportError("POST /v1/ingest returned 503", 503);
    return accepted();
  };
  engine.end(await finished(engine.begin({ method: "GET", path: "/x", apiKey: key })), { status: 200, route: "/x" });
  await engine.afterResponse();
  assert.equal(transport.ingested.length, 1);
  fail = false;
  await clock.advance(500);
  await engine.afterResponse();
  assert.deepEqual(transport.ingested.map((batch) => batch.batch_id), ["inst-1:1", "inst-1:1"]);
});

test("serverless: requests sync every 15 s after responding, and a snapshot older than 20 s is refreshed before the request is decided", async () => {
  const { key, keyId, clock, transport, engine } = serverless();
  await engine.sync();
  assert.equal(transport.syncCalls.length, 1);
  await clock.advance(15_000);
  await engine.afterResponse();
  assert.equal(transport.syncCalls.length, 2);

  // The instance sat idle for 21 seconds, and the key was revoked meanwhile. This request syncs before deciding instead of trusting the old snapshot
  await clock.advance(21_000);
  transport.onSync = () => syncPage({ full: false, keys: [keyRecord({ key_id: keyId, hash: sha256(key), consumer: "cons_1", status: "revoked" })] });
  const decision = await engine.begin({ method: "GET", path: "/x", apiKey: key });
  assert.deepEqual(!decision.allowed && [decision.status, decision.error], [401, "invalid_api_key"]);
  assert.equal(transport.syncCalls.length, 3);
});

test("serverless instances tell the server so it allows them 120 batches a minute", async () => {
  let headers: HeadersInit | undefined;
  const transport = createFetchTransport({
    appKey: "urk_test",
    instanceId: "inst-1",
    baseUrl: "http://api.test",
    instanceKind: "serverless",
    fetch: (async (_url: string, init?: RequestInit) => {
      headers = init?.headers;
      return Response.json(accepted());
    }) as typeof fetch,
  });
  await transport.ingest({} as never);
  assert.equal((headers as Record<string, string>)["x-instance-kind"], "serverless");
});

function nextApp() {
  const { key, clock, transport } = serverless();
  const tasks: Promise<unknown>[] = [];
  const rail = usageRail({ appKey: "urk_test_next", auth: "api-key", clock, transport, logger: silentLogger, instanceId: "inst-next", waitUntil: (task) => tasks.push(task) });
  // Wait for all work handed to waitUntil to finish: submissions are 0.5 seconds apart, so the fake clock has to move
  const drained = async () => {
    const all = Promise.all(tasks);
    await clock.advance(1_000);
    await all;
  };
  return { key, clock, transport, rail, tasks, drained };
}

test("Next.js: next build without an app key goes through; a route that runs at build time says why, and at run time a missing key still throws", async () => {
  const phase = process.env.NEXT_PHASE;
  process.env.NEXT_PHASE = "phase-production-build";
  try {
    const rail = usageRail({ appKey: "", auth: "api-key", waitUntil: () => {} });
    const handler = rail.route("/v1/items/[id]", async () => new Response("ok"));
    await assert.rejects(handler(new Request("https://api.example/v1/items/1"), {}), /next build ran without it/);
    assert.throws(() => rail.engine, /appKey is required/);
  } finally {
    if (phase === undefined) delete process.env.NEXT_PHASE;
    else process.env.NEXT_PHASE = phase;
  }
  assert.throws(() => usageRail({ appKey: "", auth: "api-key", waitUntil: () => {} }), /appKey is required/);
});

test("Next.js: route handlers are checked and metered under their template; rejections and track() work; reporting goes to waitUntil", async () => {
  const { key, transport, rail, tasks, drained } = nextApp();
  const get = rail.route<{ params: Promise<{ id: string }> }>("/v1/items/[id]", async (_request, { params }) => Response.json({ id: (await params).id }));
  const generate = rail.route("/v1/generate", async (_request, { usage }) => {
    usage.track(42);
    return Response.json({ ok: true }, { status: 201 });
  });
  const bearer = (value: string) => ({ authorization: `Bearer ${value}` });

  const ok = await get(new Request("https://api.example.com/v1/items/7?expand=1", { headers: bearer(key) }), { params: Promise.resolve({ id: "7" }) });
  assert.deepEqual(await ok.json(), { id: "7" });
  assert.equal((await generate(new Request("https://api.example.com/v1/generate", { method: "POST", headers: { "x-api-key": key } }), {})).status, 201);
  const refused = await get(new Request("https://api.example.com/v1/items/7", { headers: bearer("acme_test_nope") }), { params: Promise.resolve({ id: "7" }) });
  assert.equal(refused.status, 401);
  assert.deepEqual(await refused.json(), { error: "invalid_api_key" });

  // Each request handed over one waitUntil task; once they finish, the usage has been reported
  assert.equal(tasks.length, 3);
  await drained();
  assert.deepEqual(
    transport.rows().map((row) => [row.method, row.route, row.result_type, row.units]),
    [
      ["GET", "/v1/items/[id]", "2xx", 1],
      ["POST", "/v1/generate", "2xx", 42],
      ["GET", "/v1/items/[id]", "sdk_401", 0],
    ],
  );
});

test("Next.js: an SSE response counts once when it starts, and its units are reported after the stream ends", async () => {
  const { key, clock, transport, rail, tasks, drained } = nextApp();
  let push!: (chunk: string) => void;
  let close!: () => void;
  const events = rail.route("/v1/events", async (_request, { usage }) => {
    const encoder = new TextEncoder();
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        push = (chunk) => controller.enqueue(encoder.encode(chunk));
        close = () => {
          usage.track(9);
          controller.close();
        };
      },
    });
    return new Response(stream, { headers: { "content-type": "text/event-stream" } });
  });
  const response = await events(new Request("https://api.example.com/v1/events", { headers: { authorization: `Bearer ${key}` } }), {});
  const reader = response.body!.getReader();
  push("data: hello\n\n");
  await reader.read();
  // The stream is still open: the task handed to waitUntil when it opened already reported the connection's one count; the one for the stream's end is still waiting
  assert.equal(tasks.length, 2);
  const opened = tasks[0];
  await clock.advance(1_000);
  await opened;
  assert.deepEqual(transport.rows().map((row) => [row.route, row.result_type, row.count, row.units]), [["/v1/events", "ws_open", 1, 0]]);
  close();
  await reader.read();
  await drained();
  assert.deepEqual(transport.rows().map((row) => [row.route, row.result_type, row.count, row.units]), [
    ["/v1/events", "ws_open", 1, 0],
    ["/v1/events", "ws_open", 0, 9],
  ]);
});

test("Hono on Cloudflare Workers: with serverless on, each response hands reporting to executionCtx.waitUntil", async () => {
  const { key, clock, transport } = serverless();
  const app = new Hono();
  app.use("*", honoUsageRail({ appKey: "urk_test_hono", auth: "api-key", clock, transport, logger: silentLogger, instanceId: "inst-hono", serverless: true }));
  app.get("/v1/items/:id", (c) => c.json({ id: c.req.param("id") }));
  const tasks: Promise<unknown>[] = [];
  const executionCtx = { waitUntil: (task: Promise<unknown>) => void tasks.push(task), passThroughOnException: () => {}, props: {} };
  assert.equal((await app.request("/v1/items/7", { headers: { authorization: `Bearer ${key}` } }, {}, executionCtx)).status, 200);
  assert.equal((await app.request("/v1/items/7", {}, {}, executionCtx)).status, 401);
  assert.equal(tasks.length, 2);
  const all = Promise.all(tasks);
  await clock.advance(1_000);
  await all;
  assert.deepEqual(transport.rows().map((row) => [row.route, row.result_type]), [
    ["/v1/items/:id", "2xx"],
    ["_unmatched", "sdk_401"],
  ]);
});

// ---- Found in review: no retry timers of our own; a cold start waits for sync before submitting; redirect and notFound record their real status; every submission is 0.5 seconds apart;
// after a failed sync not every request retries; lease requests in flight are also awaited through waitUntil ----
test("serverless: a failed submission is not retried on a timer; only the next request's afterResponse retries", async () => {
  const { key, clock, transport, engine } = serverless();
  await engine.sync();
  transport.onIngest = () => {
    throw new TransportError("POST /v1/ingest returned 503", 503);
  };
  engine.end(await finished(engine.begin({ method: "GET", path: "/x", apiKey: key })), { status: 200, route: "/x" });
  await engine.afterResponse();
  assert.equal(transport.ingested.length, 1);
  await clock.advance(60_000);
  await settle();
  assert.equal(transport.ingested.length, 1);
});

test("serverless with your own auth: a cold start's usage goes out once the first sync brings the hash key, in the same waitUntil", async () => {
  const clock = new FakeClock();
  const transport = new FakeTransport();
  const firstSync = gate<ReturnType<typeof syncPage>>();
  transport.onSync = () => firstSync.promise;
  const engine = createEngine<{ user?: string }>({
    appKey: "urk_test_serverless",
    auth: "custom",
    consumer: (context) => (context.user ? { id: context.user } : null),
    clock,
    transport,
    logger: silentLogger,
    instanceId: "inst-1",
    serverless: true,
  });
  // Sync is slow to answer: the request waits the full second and goes through, still not knowing whether to hash external IDs
  const pending = engine.begin({ method: "GET", path: "/x", context: { user: "user-1" } });
  await clock.advance(1_000);
  engine.end(await finished(pending), { status: 200, route: "/x" });
  const after = engine.afterResponse();
  await settle();
  assert.equal(transport.ingested.length, 0, "nothing goes out before the hash key is known");
  firstSync.release(syncPage({ app: appSettings({ hash_key: "secret-hash-key" }) }));
  await after;
  const hashed = createHmac("sha256", "secret-hash-key").update("user-1", "utf8").digest("hex");
  assert.deepEqual(transport.rows().map((row) => [row.consumer, row.count]), [[hashed, 1]]);
});

test("Next.js: redirect() and notFound() are recorded with their real status and passed on to Next.js", async () => {
  const { key, transport, rail, drained } = nextApp();
  const thrown = (digest: string) => Object.assign(new Error(digest), { digest });
  const moved = rail.route("/v1/old", async () => {
    throw thrown("NEXT_REDIRECT;replace;/v1/new;307;");
  });
  const missing = rail.route("/v1/missing", async () => {
    throw thrown("NEXT_HTTP_ERROR_FALLBACK;404");
  });
  const broken = rail.route("/v1/broken", async () => {
    throw new Error("boom");
  });
  const request = (path: string) => new Request(`https://api.example.com${path}`, { headers: { authorization: `Bearer ${key}` } });
  await assert.rejects(moved(request("/v1/old"), {}), (error: { digest?: string }) => error.digest === "NEXT_REDIRECT;replace;/v1/new;307;");
  await assert.rejects(missing(request("/v1/missing"), {}), (error: { digest?: string }) => error.digest === "NEXT_HTTP_ERROR_FALLBACK;404");
  await assert.rejects(broken(request("/v1/broken"), {}), /boom/);
  await drained();
  assert.deepEqual(
    transport.rows().map((row) => [row.route, row.result_type, row.units]),
    [
      ["/v1/old", "3xx", 1],
      ["/v1/missing", "4xx", 0],
      ["/v1/broken", "5xx", 0],
    ],
  );
});

test("serverless: every submission is at least 0.5 s after the last, even when one flush sends several batches", async () => {
  const { key, clock, transport, engine } = serverless();
  await engine.sync();
  const sentAt: number[] = [];
  let tooLarge = true;
  transport.onIngest = () => {
    sentAt.push(clock.monotonic());
    if (tooLarge) {
      tooLarge = false;
      throw new TransportError("POST /v1/ingest returned 413", 413);
    }
    return accepted();
  };
  for (const route of ["/a", "/b"]) engine.end(await finished(engine.begin({ method: "GET", path: route, apiKey: key })), { status: 200, route });
  const after = engine.afterResponse();
  for (let step = 0; step < 10; step++) await clock.advance(500);
  await after;
  // After a 413 the batch is split into smaller ones, submitted one at a time, each at least 0.5 seconds apart
  assert.ok(sentAt.length >= 3, String(sentAt.length));
  for (let index = 1; index < sentAt.length; index++) assert.ok(sentAt[index] - sentAt[index - 1] >= 500, `${sentAt}`);
});

test("serverless: when syncing fails, requests don't each try again; the next try waits 10 s", async () => {
  const { key, clock, transport, engine } = serverless();
  await engine.sync();
  transport.onSync = () => {
    throw new TransportError("GET /v1/sync returned 503", 503);
  };
  // The snapshot is past 20 seconds: the first request syncs and fails; later requests don't retry until 10 seconds have passed.
  // The clock stands still, so the waitUntil tasks are just collected (submissions are 0.5 seconds apart, and they wait until the fake clock moves)
  await clock.advance(21_000);
  const waiting: Promise<void>[] = [];
  for (let index = 0; index < 10; index++) {
    engine.end(await finished(engine.begin({ method: "GET", path: "/x", apiKey: key })), { status: 200, route: "/x" });
    waiting.push(engine.afterResponse());
  }
  await settle();
  assert.equal(transport.syncCalls.length, 2);
  for (let step = 0; step < 20; step++) await clock.advance(500);
  await Promise.all(waiting);
  engine.end(await finished(engine.begin({ method: "GET", path: "/x", apiKey: key })), { status: 200, route: "/x" });
  const last = engine.afterResponse();
  await clock.advance(500);
  await last;
  assert.equal(transport.syncCalls.length, 3);
});

test("serverless: afterResponse also waits for a lease request that was already on its way", async () => {
  const key = generateApiKey({ prefix: "acme", environment: "test" });
  const clock = new FakeClock();
  const transport = new FakeTransport();
  transport.onSync = () => syncPage({ keys: [keyRecord({ key_id: key.keyId, hash: sha256(key.key), consumer: "cons_stop", quota_behavior: "stop" })] });
  const engine = createEngine({ appKey: "urk_test_serverless", auth: "api-key", clock, transport, logger: silentLogger, instanceId: "inst-1", serverless: true });
  await engine.sync();
  const lease = gate<Awaited<ReturnType<typeof transport.onLeases>>>();
  transport.onLeases = () => lease.promise;
  // The first lease request hasn't come back after 300 ms, so onQuotaUnavailable lets the request through; the lease request is still in flight
  const pending = engine.begin({ method: "GET", path: "/x", apiKey: key.key });
  await clock.advance(300);
  engine.end(await finished(pending), { status: 200, route: "/x" });
  let done = false;
  const after = engine.afterResponse().then(() => (done = true));
  await clock.advance(1_000);
  await settle();
  assert.equal(done, false, "waitUntil must not end while the lease request is in flight");
  lease.release({ results: [{ consumer: "cons_stop", status: "unlimited", quota_version: 1 }] } as never);
  await after;
  assert.equal(done, true);
});

// ---- Found in review (round two): no engine at module top level on Workers; Next.js notices client disconnects; renewals wait for sync ----
test("Hono with serverless on doesn't create its engine until the first request (Workers forbid randomness at startup)", async () => {
  const { key, transport } = serverless();
  const randomUUID = globalThis.crypto.randomUUID;
  // Simulates Workers: generating random numbers at module top level throws
  Object.defineProperty(globalThis.crypto, "randomUUID", { configurable: true, value: () => { throw new Error("Disallowed operation called within global scope"); } });
  let middleware: ReturnType<typeof honoUsageRail>;
  try {
    middleware = honoUsageRail({ appKey: "urk_test_workers", auth: "api-key", clock: new FakeClock(), transport, logger: silentLogger, serverless: true });
  } finally {
    Object.defineProperty(globalThis.crypto, "randomUUID", { configurable: true, value: randomUUID });
  }
  const app = new Hono();
  app.use(middleware);
  app.get("/x", (c) => c.json({ ok: true }));
  const tasks: Promise<unknown>[] = [];
  const response = await app.request("/x", { headers: { authorization: `Bearer ${key}` } }, undefined, { waitUntil: (task: Promise<unknown>) => tasks.push(task), passThroughOnException() {}, props: {} } as never);
  assert.equal(response.status, 200);
  assert.equal(tasks.length, 1);
});

test("Next.js: a client that leaves while the handler runs, or before it starts, is settled as aborted and not billed", async () => {
  const { key, transport, rail, drained } = nextApp();
  const running = new AbortController();
  const slow = rail.route("/v1/slow", async () => {
    running.abort();
    return Response.json({ ok: true });
  });
  let ran = false;
  const never = rail.route("/v1/never", async () => {
    ran = true;
    return Response.json({ ok: true });
  });
  const headers = { authorization: `Bearer ${key}` };
  await slow(new Request("https://api.example.com/v1/slow", { headers, signal: running.signal }), {});
  const gone = new AbortController();
  gone.abort();
  await never(new Request("https://api.example.com/v1/never", { headers, signal: gone.signal }), {});
  assert.equal(ran, false);
  await drained();
  assert.deepEqual(
    transport.rows().map((row) => [row.route, row.result_type, row.units]),
    [
      ["/v1/slow", "aborted", 0],
      ["/v1/never", "aborted", 0],
    ],
  );
});

test("serverless: renewals queued by a sync that's still running go out in the same afterResponse", async () => {
  const key = generateApiKey({ prefix: "acme", environment: "test" });
  const clock = new FakeClock();
  const transport = new FakeTransport();
  let version = 1;
  transport.onSync = () =>
    syncPage({
      keys: [keyRecord({ key_id: key.keyId, hash: sha256(key.key), consumer: "cons_stop", quota_behavior: "stop", quota_version: version })],
      consumers: [consumerRecord({ consumer: "cons_stop", quota_behavior: "stop", quota_version: version })],
    });
  let leaseId = 0;
  transport.onLeases = (request) => ({
    results: request.requests.map((item) => ({
      consumer: item.consumer,
      status: "granted" as const,
      lease: { lease_id: String(++leaseId), consumer: item.consumer, granted: 5, quota_version: item.quota_version, behavior: "stop" as const, period_end: new Date(clock.now() + 3_600_000).toISOString() },
    })),
  });
  const engine = createEngine({ appKey: "urk_test_serverless", auth: "api-key", clock, transport, logger: silentLogger, instanceId: "inst-1", serverless: true });
  await engine.sync();
  engine.end(await finished(engine.begin({ method: "GET", path: "/x", apiKey: key.key })), { status: 200, route: "/x" });
  const first = engine.afterResponse();
  await clock.advance(1_000);
  await first;
  const leasesBefore = transport.leaseCalls.length;
  // The seller changed its quota (version 2): the next sync retires the old lease and queues a renewal, which should be in hand by the end of this request
  version = 2;
  await clock.advance(15_000);
  engine.end(await finished(engine.begin({ method: "GET", path: "/x", apiKey: key.key })), { status: 200, route: "/x" });
  const after = engine.afterResponse();
  await clock.advance(1_000);
  await after;
  assert.equal(transport.leaseCalls.length, leasesBefore + 1);
});

// ---- Found in review (round three): SSE is reported when the stream opens, not when it ends; a very long stream still gets its units in ----
test("Hono on Workers: an SSE response is reported when it opens, and a stream open for 25 hours still gets its units in", async () => {
  const { key, clock, transport } = serverless();
  const app = new Hono<{ Variables: UsageRailVariables }>();
  app.use("*", honoUsageRail({ appKey: "urk_test_hono", auth: "api-key", clock, transport, logger: silentLogger, instanceId: "inst-hono-sse", serverless: true }));
  let close!: () => void;
  app.get("/v1/events", (c) => {
    const usage = c.get("usageRail");
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new TextEncoder().encode("data: hello\n\n"));
        close = () => {
          usage.track(9);
          controller.close();
        };
      },
    });
    return new Response(stream, { headers: { "content-type": "text/event-stream" } });
  });
  const tasks: Promise<unknown>[] = [];
  const executionCtx = { waitUntil: (task: Promise<unknown>) => void tasks.push(task), passThroughOnException: () => {}, props: {} };
  const response = await app.request("/v1/events", { headers: { authorization: `Bearer ${key}` } }, {}, executionCtx);
  const reader = response.body!.getReader();
  await reader.read();
  // The stream is still open: the task handed over when it opened reported the connection's one count without waiting for the end
  assert.equal(tasks.length, 2);
  const opened = tasks[0];
  await clock.advance(1_000);
  await opened;
  const rows = () => transport.rows().map((row) => [row.route, row.result_type, row.count, row.units, row.minute]);
  const startMinute = new Date(Date.UTC(2026, 9, 20, 12, 0)).toISOString();
  assert.deepEqual(rows(), [["/v1/events", "ws_open", 1, 0, startMinute]]);
  // It ends 25 hours later: the server no longer accepts the starting minute, so the units go in the earliest minute it still accepts
  await clock.advance(25 * 3_600_000);
  close();
  await reader.read();
  const all = Promise.all(tasks);
  await clock.advance(1_000);
  await all;
  const [, units] = rows();
  assert.deepEqual(units.slice(0, 4), ["/v1/events", "ws_open", 0, 9]);
  assert.ok(clock.now() - Date.parse(units[4] as string) <= 23 * 3_600_000 + 1_000);
});

// ---- Found in review (round four): HEAD isn't treated as a stream; a throwing waitUntil doesn't affect the response ----
test("Next.js: a HEAD request to an SSE route is settled at once, its stream cancelled", async () => {
  const { key, transport, rail, tasks, drained } = nextApp();
  let cancelled = false;
  const events = rail.route("/v1/events", async () => {
    const stream = new ReadableStream<Uint8Array>({ cancel: () => void (cancelled = true) });
    return new Response(stream, { headers: { "content-type": "text/event-stream" } });
  });
  const response = await events(new Request("https://api.example.com/v1/events", { method: "HEAD", headers: { authorization: `Bearer ${key}` } }), {});
  assert.equal(response.body, null);
  assert.equal(tasks.length, 1);
  await drained();
  assert.equal(cancelled, true);
  assert.deepEqual(transport.rows().map((row) => [row.method, row.result_type, row.count]), [["HEAD", "2xx", 1]]);
});

test("Hono on Workers: a HEAD request to an SSE route is settled at once, its stream cancelled", async () => {
  const { key, clock, transport } = serverless();
  const app = new Hono<{ Variables: UsageRailVariables }>();
  app.use("*", honoUsageRail({ appKey: "urk_test_hono", auth: "api-key", clock, transport, logger: silentLogger, instanceId: "inst-hono-head", serverless: true }));
  let cancelled = false;
  app.get("/v1/events", () => new Response(new ReadableStream<Uint8Array>({ cancel: () => void (cancelled = true) }), { headers: { "content-type": "text/event-stream" } }));
  const tasks: Promise<unknown>[] = [];
  const executionCtx = { waitUntil: (task: Promise<unknown>) => void tasks.push(task), passThroughOnException: () => {}, props: {} };
  const response = await app.request("/v1/events", { method: "HEAD", headers: { authorization: `Bearer ${key}` } }, {}, executionCtx);
  assert.equal(response.status, 200);
  assert.equal(tasks.length, 1);
  const all = Promise.all(tasks);
  await clock.advance(1_000);
  await all;
  assert.equal(cancelled, true);
  assert.deepEqual(transport.rows().map((row) => [row.result_type, row.count]), [["2xx", 1]]);
});

test("a waitUntil that throws doesn't turn a successful response into an error", async () => {
  const { key, clock, transport } = serverless();
  const warnings: string[] = [];
  const logger = { warn: (message: string) => void warnings.push(message) };
  const rail = usageRail({ appKey: "urk_test_next", auth: "api-key", clock, transport, logger, instanceId: "inst-next-throw", waitUntil: () => { throw new Error("after() called outside a request scope"); } });
  const get = rail.route("/v1/items", async () => Response.json({ ok: true }));
  const response = await get(new Request("https://api.example.com/v1/items", { headers: { authorization: `Bearer ${key}` } }), {});
  assert.equal(response.status, 200);
  assert.ok(warnings.some((message) => message.includes("waitUntil failed")), warnings.join("\n"));
  const app = new Hono();
  app.use("*", honoUsageRail({ appKey: "urk_test_hono", auth: "api-key", clock, transport, logger, instanceId: "inst-hono-throw", serverless: true }));
  app.get("/v1/items", (c) => c.json({ ok: true }));
  const executionCtx = { waitUntil: () => { throw new Error("no execution context"); }, passThroughOnException: () => {}, props: {} };
  assert.equal((await app.request("/v1/items", { headers: { authorization: `Bearer ${key}` } }, {}, executionCtx)).status, 200);
});

// ---- Found in review (round five): units the source tracks while being cancelled count; a stale snapshot waits for the sync already in flight ----
test("Next.js: units a stream's source tracks while it is being cancelled are billed, even with a read still pending", async () => {
  for (const asynchronous of [false, true]) {
    const { key, transport, rail, drained } = nextApp();
    const events = rail.route("/v1/events", async (_request, { usage }) => {
      const stream = new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(new TextEncoder().encode("data: hi\n\n"));
        },
        // On client disconnect, record what was already generated
        async cancel() {
          if (asynchronous) await new Promise((resolve) => setTimeout(resolve, 5));
          usage.track(42);
        },
      });
      return new Response(stream, { headers: { "content-type": "text/event-stream" } });
    });
    const response = await events(new Request("https://api.example.com/v1/events", { headers: { authorization: `Bearer ${key}` } }), {});
    const reader = response.body!.getReader();
    await reader.read();
    // The second read is still pending (the source has nothing more) when the client disconnects
    const pendingRead = reader.read();
    await reader.cancel();
    await pendingRead;
    await drained();
    // The row for the stream opening was reported right away; the units added on disconnect are the 42 the source tracked in cancel(), not the 1 used without track()
    assert.deepEqual(transport.rows().map((row) => [row.result_type, row.count, row.units]), [["ws_open", 1, 0], ["ws_open", 0, 42]], `asynchronous: ${asynchronous}`);
  }
});

test("serverless: a stale snapshot waits for a sync already on its way, even when a new attempt would be too soon", async () => {
  const { key, keyId, clock, transport, engine } = serverless();
  assert.equal((await engine.begin({ method: "GET", path: "/x", apiKey: key })).allowed, true);
  // 20 seconds later the previous request ends and starts a sync, whose answer is slow to come
  await clock.advance(20_000);
  const answer = gate<ReturnType<typeof syncPage>>();
  transport.onSync = () => answer.promise;
  const after = engine.afterResponse();
  // Another second passes: the snapshot is 21 seconds old, but the last sync started only 1 second ago, so the cooldown forbids a new one; this request waits for that one instead
  await clock.advance(1_000);
  const pending = engine.begin({ method: "GET", path: "/x", apiKey: key });
  assert.ok(pending instanceof Promise, "waits for the sync on its way");
  answer.release(syncPage({ full: false, keys: [keyRecord({ key_id: keyId, hash: sha256(key), consumer: "cons_1", status: "revoked" })] }));
  assert.equal((await pending).allowed, false);
  await clock.advance(1_000);
  await after;
});

// Cloudflare Workers judges a request hung, and answers it with a 500, when all it waits on belongs to another request: here the sync, and the gate with its timer,
// were started by the first request. Each waiting request holds a timer of its own until it's decided, and the same goes for afterResponse in waitUntil
test("serverless: a request waiting on another request's sync or submission holds a timer of its own until the wait ends", async () => {
  const { key, clock, transport, engine } = serverless();
  const answer = gate<ReturnType<typeof syncPage>>();
  const syncResponse = transport.onSync;
  transport.onSync = () => answer.promise;
  const first = engine.begin({ method: "GET", path: "/x", apiKey: key });
  const opened = clock.pending();
  const second = engine.begin({ method: "GET", path: "/x", apiKey: key });
  assert.ok(second instanceof Promise);
  assert.equal(clock.pending(), opened + 1, "the second request sets a timer of its own while it waits on the first request's sync");
  answer.release(await syncResponse(null));
  assert.equal((await first).allowed, true);
  const handle = await finished(second);
  await settle();
  assert.equal(clock.pending(), 0, "both timers are cleared once the requests are decided");

  // afterResponse: the second one waits on the first one's submission
  const ingest = gate<IngestResponse>();
  transport.onIngest = () => ingest.promise;
  engine.end(handle, { status: 200, route: "/x" });
  const afterFirst = engine.afterResponse();
  await settle();
  const before = clock.pending();
  const afterSecond = engine.afterResponse();
  assert.equal(clock.pending(), before + 1);
  ingest.release(accepted());
  await clock.advance(500);
  await Promise.all([afterFirst, afterSecond]);
  assert.equal(clock.pending(), 0);
});

// A self-hosted Next.js on SIGTERM closes its HTTP server first and then waits for after() tasks: by then the SDK's timers are all that's left in the process,
// and if they were unref'd Node would exit at once, losing batches still waiting for the submission gap
test("serverless waits keep the process alive until they fire; background timers don't", () => {
  const clockModule = new URL("../src/clock.ts", import.meta.url).href;
  for (const [clock, expected] of [["waitingClock", "fired"], ["systemClock", ""]] as const) {
    const script = `import { ${clock} } from ${JSON.stringify(clockModule)}; ${clock}.setTimer(() => console.log("fired"), 50);`;
    const result = spawnSync(process.execPath, ["--input-type=module", "-e", script], { encoding: "utf8" });
    assert.equal(result.status, 0, result.stderr);
    assert.equal(result.stdout.trim(), expected, clock);
  }
});

