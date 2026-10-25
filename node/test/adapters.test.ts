import { test } from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { request as httpRequest } from "node:http";
import { connect, type AddressInfo } from "node:net";

import express from "express";
import fastifyWebsocket from "@fastify/websocket";
import Fastify from "fastify";
import { Hono } from "hono";

import { usageRail as expressUsageRail, type UsageRailRequest } from "../src/express.ts";
import { usageRail as fastifyUsageRail } from "../src/fastify.ts";
import { usageRail as honoUsageRail } from "../src/hono.ts";
import { generateApiKey } from "../src/key-format.ts";
import { appSettings, consumerRecord, FakeClock, FakeTransport, keyRecord, silentLogger, syncPage } from "./helpers.ts";

const sha256 = (value: string) => createHash("sha256").update(value).digest("hex");

function backend(routes: { method: string; route: string; weight: number; reserve: number }[] = []) {
  const key = generateApiKey({ prefix: "acme", environment: "test" });
  const limited = generateApiKey({ prefix: "acme", environment: "test" });
  const transport = new FakeTransport();
  transport.onSync = () =>
    syncPage({
      app: appSettings({ unit_mode: "units", routes }),
      keys: [
        keyRecord({ key_id: key.keyId, hash: sha256(key.key), consumer: "cons_1" }),
        keyRecord({ key_id: limited.keyId, hash: sha256(limited.key), consumer: "cons_limited" }),
      ],
      consumers: [consumerRecord({ consumer: "cons_1" }), consumerRecord({ consumer: "cons_limited", rate_limits: { per_second: 1, per_minute: null } })],
    });
  const options = { appKey: "urk_test_adapter", auth: "api-key" as const, clock: new FakeClock(), transport, backgroundLoops: false, logger: silentLogger };
  return { key: key.key, limited: limited.key, transport, options };
}

const summary = (transport: FakeTransport) => transport.rows().map((row) => [row.method, row.route, row.result_type, row.units]);

test("Express: route templates include the router mount point; track(), rejections and unmatched paths", async (t) => {
  const { key, limited, transport, options } = backend();
  const middleware = expressUsageRail({ ...options, snapshot: false, flushOnExit: false });
  await middleware.engine.sync();
  const app = express();
  app.use(middleware);
  const router = express.Router();
  router.get("/users/:id", (req, res) => {
    res.json({ id: req.params.id });
  });
  router.post("/generate", (req, res) => {
    (req as UsageRailRequest).usageRail.track(42);
    res.status(201).json({ ok: true });
  });
  let disconnected!: Promise<void>;
  router.get("/slow", (req, res) => {
    // Never responds: waits for the client to disconnect
    disconnected = new Promise((resolve) => res.once("close", () => resolve()));
  });
  app.use("/v1", router);
  const server = app.listen(0, "127.0.0.1");
  await new Promise((resolve) => server.once("listening", resolve));
  t.after(() => {
    server.closeAllConnections();
    server.close();
  });
  const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  const bearer = (value: string) => ({ authorization: `Bearer ${value}` });

  assert.deepEqual(await (await fetch(`${base}/v1/users/7`, { headers: bearer(key) })).json(), { id: "7" });
  assert.equal((await fetch(`${base}/v1/generate`, { method: "POST", headers: { "x-api-key": key } })).status, 201);
  const invalid = await fetch(`${base}/v1/users/7`, { headers: bearer("acme_test_nope") });
  assert.equal(invalid.status, 401);
  assert.deepEqual(await invalid.json(), { error: "invalid_api_key" });
  assert.equal((await fetch(`${base}/nowhere`, { headers: bearer(key) })).status, 404);
  assert.equal((await fetch(`${base}/v1/users/1`, { headers: bearer(limited) })).status, 200);
  const tooMany = await fetch(`${base}/v1/users/1`, { headers: bearer(limited) });
  assert.equal(tooMany.status, 429);
  assert.equal(tooMany.headers.get("retry-after"), "1");
  assert.ok(tooMany.headers.get("ratelimit-policy"));
  assert.equal((await tooMany.json()).error, "rate_limited");
  // Client disconnects before the response: aborted
  const controller = new AbortController();
  const slow = fetch(`${base}/v1/slow`, { headers: bearer(key), signal: controller.signal }).catch(() => null);
  while (!disconnected) await new Promise((resolve) => setImmediate(resolve));
  controller.abort();
  await slow;
  await disconnected;
  // The finish event fires after the response is sent; give it a moment
  await new Promise((resolve) => setTimeout(resolve, 20));
  await middleware.engine.flush();

  assert.deepEqual(summary(transport), [
    ["GET", "/v1/users/:id", "2xx", 1],
    ["POST", "/v1/generate", "2xx", 42],
    // Express has to decide before routing, so rejected requests have no template and are recorded as _unmatched
    ["GET", "_unmatched", "sdk_401", 0],
    ["GET", "_unmatched", "4xx", 0],
    ["GET", "/v1/users/:id", "2xx", 1],
    ["GET", "_unmatched", "sdk_429", 0],
    ["GET", "/v1/slow", "aborted", 0],
  ]);
});

test("Hono: route templates, track(), rejections and unmatched paths", async () => {
  const { key, limited, transport, options } = backend();
  const middleware = honoUsageRail(options);
  await middleware.engine.sync();
  const app = new Hono<{ Variables: { usageRail: { track(units: number): void } } }>();
  app.use("*", middleware);
  app.get("/v1/users/:id", (c) => c.json({ id: c.req.param("id") }));
  app.post("/v1/generate", (c) => {
    c.get("usageRail").track(42);
    return c.json({ ok: true }, 201);
  });
  const bearer = (value: string) => ({ authorization: `Bearer ${value}` });

  assert.deepEqual(await (await app.request("/v1/users/7", { headers: bearer(key) })).json(), { id: "7" });
  assert.equal((await app.request("/v1/generate", { method: "POST", headers: { "x-api-key": key } })).status, 201);
  const invalid = await app.request("/v1/users/7", { headers: bearer("acme_test_nope") });
  assert.equal(invalid.status, 401);
  assert.deepEqual(await invalid.json(), { error: "invalid_api_key" });
  assert.equal((await app.request("/nowhere", { headers: bearer(key) })).status, 404);
  assert.equal((await app.request("/v1/users/1", { headers: bearer(limited) })).status, 200);
  const tooMany = await app.request("/v1/users/1", { headers: bearer(limited) });
  assert.equal(tooMany.status, 429);
  assert.equal(tooMany.headers.get("retry-after"), "1");
  assert.equal((await tooMany.json()).error, "rate_limited");
  await middleware.engine.flush();

  assert.deepEqual(summary(transport), [
    ["GET", "/v1/users/:id", "2xx", 1],
    ["POST", "/v1/generate", "2xx", 42],
    // Rejected requests never reached a route; like Express, they're recorded under the seller's matching template or _unmatched
    ["GET", "_unmatched", "sdk_401", 0],
    ["GET", "_unmatched", "4xx", 0],
    ["GET", "/v1/users/:id", "2xx", 1],
    ["GET", "_unmatched", "sdk_429", 0],
  ]);
});

test("Express: an SSE response is metered once, as soon as it starts streaming", async (t) => {
  const { key, transport, options } = backend();
  const middleware = expressUsageRail({ ...options, snapshot: false, flushOnExit: false });
  await middleware.engine.sync();
  const app = express();
  app.use(middleware);
  let release!: () => void;
  const finished = new Promise<void>((resolve) => (release = resolve));
  app.get("/v1/events", (req, res) => {
    // Both ways of setting headers must be recognized: passing them straight to writeHead, and setHeader followed by write
    if (req.query.style === "set") {
      res.setHeader("Content-Type", "text/event-stream");
      res.write("data: hello\n\n");
    } else {
      res.writeHead(200, { "Content-Type": "text/event-stream; charset=utf-8" });
      res.write("data: hello\n\n");
    }
    void finished.then(() => res.end());
  });
  const server = app.listen(0, "127.0.0.1");
  await new Promise((resolve) => server.once("listening", resolve));
  t.after(() => {
    release();
    server.closeAllConnections();
    server.close();
  });
  const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  for (const style of ["head", "set"]) {
    const response = await fetch(`${base}/v1/events?style=${style}`, { headers: { authorization: `Bearer ${key}` } });
    const reader = response.body!.getReader();
    await reader.read();
  }
  // Both connections are still open and each counted once; in an app metering units, the units are added when the connection ends
  await middleware.engine.flush();
  assert.deepEqual(transport.rows().map((row) => [row.route, row.result_type, row.count, row.units]), [["/v1/events", "ws_open", 2, 0]]);
  release();
  await new Promise((resolve) => setTimeout(resolve, 20));
  await middleware.engine.flush();
  assert.deepEqual(transport.rows().slice(1).map((row) => [row.result_type, row.count, row.units]), [["ws_open", 0, 2]]);
});

test("Hono: the route that actually handled the request is recorded, even with a wildcard fallback after it", async () => {
  const { key, transport, options } = backend();
  const middleware = honoUsageRail(options);
  await middleware.engine.sync();
  const app = new Hono();
  app.use("*", middleware);
  app.use("/v1/*", async (_c, next) => {
    await next();
  });
  app.get("/v1/users/:id", (c) => c.json({ id: c.req.param("id") }));
  app.all("/v1/rpc", (c) => c.json({ ok: true }));
  app.get("*", (c) => c.json({ error: "not_found" }, 404));
  const headers = { authorization: `Bearer ${key}` };
  assert.equal((await app.request("/v1/users/7", { headers })).status, 200);
  assert.equal((await app.request("/v1/rpc", { method: "POST", headers })).status, 200);
  assert.equal((await app.request("/elsewhere", { headers })).status, 404);
  await middleware.engine.flush();
  assert.deepEqual(summary(transport), [
    ["GET", "/v1/users/:id", "2xx", 1],
    ["POST", "/v1/rpc", "2xx", 1],
    // A 404 the catch-all route handled itself
    ["GET", "/*", "4xx", 0],
  ]);
});

// A consumer with a hard limit; the server hands out at most 5 units
function stopBackend(routes: { method: string; route: string; weight: number; reserve: number }[]) {
  const key = generateApiKey({ prefix: "acme", environment: "test" });
  const transport = new FakeTransport();
  const clock = new FakeClock();
  transport.onSync = () =>
    syncPage({
      app: appSettings({ unit_mode: "requests", routes }),
      keys: [keyRecord({ key_id: key.keyId, hash: sha256(key.key), consumer: "cons_stop", quota_behavior: "stop", quota_version: 1 })],
      consumers: [consumerRecord({ consumer: "cons_stop", quota_behavior: "stop" })],
    });
  transport.onLeases = (request) => ({
    results: request.requests.map((item) =>
      item.reserve > 5
        ? { consumer: item.consumer, status: "exhausted" as const, behavior: "stop" as const, quota_version: 1, retry_after: 60 }
        : {
            consumer: item.consumer,
            status: "granted" as const,
            lease: { lease_id: "1", consumer: item.consumer, granted: 5, quota_version: 1, behavior: "stop" as const, period_end: new Date(clock.now() + 3_600_000).toISOString() },
          },
    ),
  });
  return { key: key.key, transport, options: { appKey: "urk_test_stop", auth: "api-key" as const, clock, transport, backgroundLoops: false, logger: silentLogger } };
}

test("Express mounted under a prefix still finds the seller's full-path route weight", async (t) => {
  const { key, options } = stopBackend([{ method: "GET", route: "/api/expensive", weight: 10, reserve: 10 }]);
  const middleware = expressUsageRail({ ...options, snapshot: false, flushOnExit: false });
  await middleware.engine.sync();
  const app = express();
  app.use("/api", middleware);
  app.get("/api/expensive", (_req, res) => {
    res.json({ ok: true });
  });
  const server = app.listen(0, "127.0.0.1");
  await new Promise((resolve) => server.once("listening", resolve));
  t.after(() => server.close());
  // A request needs 10, the server has only 5 left: block it, don't let it in as 1. Express ignores case and treats a trailing slash as optional by default, so other spellings reach the same handler and must be blocked too
  for (const path of ["/api/expensive", "/API/EXPENSIVE", "/api/expensive/"]) {
    const response = await fetch(`http://127.0.0.1:${(server.address() as AddressInfo).port}${path}`, { headers: { authorization: `Bearer ${key}` } });
    assert.equal(response.status, 429, path);
  }
});

test("Express: a differently-cased path into a sub-router is billed and recorded as the seller's template", async (t) => {
  const { key, transport, options } = backend([{ method: "POST", route: "/v1/generate", weight: 10, reserve: 10 }]);
  const middleware = expressUsageRail({ ...options, snapshot: false, flushOnExit: false });
  await middleware.engine.sync();
  const app = express();
  app.use(middleware);
  const v1 = express.Router();
  v1.post("/generate", (_req, res) => {
    res.json({ ok: true });
  });
  app.use("/v1", v1);
  const server = app.listen(0, "127.0.0.1");
  await new Promise((resolve) => server.once("listening", resolve));
  t.after(() => server.close());
  const response = await fetch(`http://127.0.0.1:${(server.address() as AddressInfo).port}/V1/GENERATE`, { method: "POST", headers: { authorization: `Bearer ${key}` } });
  assert.equal(response.status, 200);
  await middleware.engine.flush();
  // Express gives the template as /V1/generate (the mount path as the request wrote it). Express is case-insensitive anyway, so this is the seller's /v1/generate
  assert.deepEqual(summary(transport), [["POST", "/v1/generate", "2xx", 10]]);
});

test("Express: an app with case-sensitive routing keeps routes that differ only in case apart", async (t) => {
  // The server has only 5 left: the expensive /expensive needs 10; /Expensive differs only in case and isn't priced, needs only 1, and must not reserve or bill like the expensive one
  const { key, transport, options } = stopBackend([{ method: "POST", route: "/expensive", weight: 10, reserve: 10 }]);
  const middleware = expressUsageRail({ ...options, snapshot: false, flushOnExit: false });
  await middleware.engine.sync();
  const app = express();
  app.set("case sensitive routing", true);
  app.use(middleware);
  app.post("/expensive", (_req, res) => {
    res.json({ ok: true });
  });
  app.post("/Expensive", (_req, res) => {
    res.json({ ok: true });
  });
  const server = app.listen(0, "127.0.0.1");
  await new Promise((resolve) => server.once("listening", resolve));
  t.after(() => server.close());
  const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  assert.equal((await fetch(`${base}/expensive`, { method: "POST", headers: { authorization: `Bearer ${key}` } })).status, 429);
  assert.equal((await fetch(`${base}/Expensive`, { method: "POST", headers: { authorization: `Bearer ${key}` } })).status, 200);
  await middleware.engine.flush();
  assert.deepEqual(
    summary(transport).filter((row) => row[2] === "2xx"),
    [["POST", "/Expensive", "2xx", 1]],
  );
});

test("Express: a route the seller didn't price is billed 1, even when a priced template matched its path at the start", async (t) => {
  // The seller configured /v1/*, the app handles the request with /v1/items/:id: at the start it reserves 3 from the lease by /v1/*, but settlement only trusts the framework's template and counts 1
  const { key, transport, options } = stopBackend([{ method: "ALL", route: "/v1/*", weight: 3, reserve: 3 }]);
  const middleware = expressUsageRail({ ...options, snapshot: false, flushOnExit: false });
  await middleware.engine.sync();
  const app = express();
  app.use(middleware);
  app.get("/v1/items/:id", (_req, res) => {
    res.json({ ok: true });
  });
  const server = app.listen(0, "127.0.0.1");
  await new Promise((resolve) => server.once("listening", resolve));
  t.after(() => server.close());
  assert.equal((await fetch(`http://127.0.0.1:${(server.address() as AddressInfo).port}/v1/items/7`, { headers: { authorization: `Bearer ${key}` } })).status, 200);
  await middleware.engine.flush();
  assert.deepEqual(summary(transport), [["GET", "/v1/items/:id", "2xx", 1]]);
});

test("Hono: an app.all() route with a weight followed by a cheap fallback reserves the larger weight", async () => {
  const { key, options } = stopBackend([{ method: "*", route: "/expensive", weight: 10, reserve: 10 }, { method: "GET", route: "/*", weight: 1, reserve: 1 }]);
  const middleware = honoUsageRail(options);
  await middleware.engine.sync();
  const app = new Hono();
  app.use("*", middleware);
  app.all("/expensive", (c) => c.json({ ok: true }));
  app.get("*", (c) => c.json({ error: "not_found" }, 404));
  assert.equal((await app.request("/expensive", { headers: { authorization: `Bearer ${key}` } })).status, 429);
});

test("Express: a client that disconnects while the SDK waits for a lease is settled as aborted and never reaches the handler", async (t) => {
  const { key, transport, options } = stopBackend([]);
  let releaseLease!: () => void;
  const leaseGate = new Promise<void>((resolve) => (releaseLease = resolve));
  const grantLease = transport.onLeases;
  transport.onLeases = async (request) => {
    await leaseGate;
    return grantLease(request);
  };
  const middleware = expressUsageRail({ ...options, snapshot: false, flushOnExit: false });
  await middleware.engine.sync();
  const app = express();
  app.use(middleware);
  let handlerRuns = 0;
  app.get("/v1/thing", (_req, res) => {
    handlerRuns += 1;
    res.json({ ok: true });
  });
  const server = app.listen(0, "127.0.0.1");
  await new Promise((resolve) => server.once("listening", resolve));
  t.after(() => {
    server.closeAllConnections();
    server.close();
  });
  const controller = new AbortController();
  const pending = fetch(`http://127.0.0.1:${(server.address() as AddressInfo).port}/v1/thing`, { headers: { authorization: `Bearer ${key}` }, signal: controller.signal }).catch(() => null);
  while (transport.leaseCalls.length === 0) await new Promise((resolve) => setImmediate(resolve));
  controller.abort();
  await pending;
  await new Promise((resolve) => setTimeout(resolve, 20));
  // The lease arrives after the disconnect: the admitted request is settled as aborted right away instead of holding its reservation
  releaseLease();
  await new Promise((resolve) => setTimeout(resolve, 20));
  await middleware.engine.flush();
  assert.deepEqual(transport.rows().map((row) => [row.route, row.result_type, row.units]), [["_unmatched", "aborted", 0]]);
  assert.deepEqual(transport.ingested[0].lease_used, []);
  // Nobody receives the response, so the handler doesn't run: otherwise disconnecting would be a free ride
  assert.equal(handlerRuns, 0);
});

test("Hono: an SSE stream counts once when it starts and gets its track() units when it ends", async () => {
  const { streamSSE } = await import("hono/streaming");
  const { key, transport, options } = backend();
  const middleware = honoUsageRail(options);
  await middleware.engine.sync();
  const app = new Hono<{ Variables: { usageRail: { track(units: number): void } } }>();
  app.use("*", middleware);
  let finishStream!: () => void;
  const streamDone = new Promise<void>((resolve) => (finishStream = resolve));
  app.get("/v1/chat", (c) =>
    streamSSE(c, async (stream) => {
      await stream.writeSSE({ data: "first token" });
      await streamDone;
      c.get("usageRail").track(42);
      await stream.writeSSE({ data: "done" });
    }),
  );
  const response = await app.request("/v1/chat", { headers: { authorization: `Bearer ${key}` } });
  const reader = response.body!.getReader();
  await reader.read();
  await middleware.engine.flush();
  assert.deepEqual(transport.rows().map((row) => [row.route, row.result_type, row.count, row.units]), [["/v1/chat", "ws_open", 1, 0]]);
  finishStream();
  while (!(await reader.read()).done) {}
  await middleware.engine.flush();
  assert.deepEqual(transport.rows().slice(1).map((row) => [row.result_type, row.count, row.units]), [["ws_open", 0, 42]]);
});

test("Fastify: route templates with the register prefix; track(), rejections, unmatched paths and early disconnects", async (t) => {
  const { key, limited, transport, options } = backend();
  const plugin = fastifyUsageRail({ ...options, snapshot: false, flushOnExit: false });
  await plugin.engine.sync();
  const app = Fastify();
  await app.register(plugin);
  let disconnected!: Promise<void>;
  await app.register(
    async (v1) => {
      v1.get("/users/:id", async (request) => ({ id: (request.params as { id: string }).id }));
      v1.post("/generate", async (request, reply) => {
        request.usageRail.track(42);
        reply.code(201);
        return { ok: true };
      });
      v1.get("/slow", (_request, reply) => {
        // Never responds: waits for the client to disconnect
        disconnected = new Promise((resolve) => reply.raw.once("close", () => resolve()));
      });
    },
    { prefix: "/v1" },
  );
  await app.listen({ port: 0, host: "127.0.0.1" });
  t.after(() => {
    // fetch leaves a connection open that never sent a request; Node doesn't count it as idle, so without closing it close() waits for Fastify's keep-alive timeout (72 seconds)
    app.server.closeAllConnections();
    return app.close();
  });
  const base = `http://127.0.0.1:${(app.server.address() as AddressInfo).port}`;
  const bearer = (value: string) => ({ authorization: `Bearer ${value}` });

  assert.deepEqual(await (await fetch(`${base}/v1/users/7?expand=1`, { headers: bearer(key) })).json(), { id: "7" });
  assert.equal((await fetch(`${base}/v1/generate`, { method: "POST", headers: { "x-api-key": key } })).status, 201);
  const invalid = await fetch(`${base}/v1/users/7`, { headers: bearer("acme_test_nope") });
  assert.equal(invalid.status, 401);
  assert.deepEqual(await invalid.json(), { error: "invalid_api_key" });
  assert.equal((await fetch(`${base}/nowhere`, { headers: bearer(key) })).status, 404);
  assert.equal((await fetch(`${base}/v1/users/1`, { headers: bearer(limited) })).status, 200);
  const tooMany = await fetch(`${base}/v1/users/1`, { headers: bearer(limited) });
  assert.equal(tooMany.status, 429);
  assert.equal(tooMany.headers.get("retry-after"), "1");
  assert.ok(tooMany.headers.get("ratelimit-policy"));
  assert.equal((await tooMany.json()).error, "rate_limited");
  const controller = new AbortController();
  const slow = fetch(`${base}/v1/slow`, { headers: bearer(key), signal: controller.signal }).catch(() => null);
  while (!disconnected) await new Promise((resolve) => setImmediate(resolve));
  controller.abort();
  await slow;
  await disconnected;
  await new Promise((resolve) => setTimeout(resolve, 20));
  await plugin.engine.flush();

  // Fastify routes before the hooks, so even requests the SDK rejects get a template (Express can't, see the Express test with the same name)
  assert.deepEqual(summary(transport), [
    ["GET", "/v1/users/:id", "2xx", 1],
    ["POST", "/v1/generate", "2xx", 42],
    ["GET", "/v1/users/:id", "sdk_401", 0],
    ["GET", "_unmatched", "4xx", 0],
    ["GET", "/v1/users/:id", "2xx", 1],
    ["GET", "/v1/users/:id", "sdk_429", 0],
    ["GET", "/v1/slow", "aborted", 0],
  ]);
});

test("Fastify: an SSE response is metered once, as soon as it starts streaming", async (t) => {
  const { key, transport, options } = backend();
  const plugin = fastifyUsageRail({ ...options, snapshot: false, flushOnExit: false });
  await plugin.engine.sync();
  const app = Fastify();
  await app.register(plugin);
  let streamOpen!: () => void;
  const opened = new Promise<void>((resolve) => (streamOpen = resolve));
  app.get("/v1/events", (_request, reply) => {
    reply.hijack();
    reply.raw.writeHead(200, { "content-type": "text/event-stream" });
    reply.raw.write("data: hello\n\n");
    streamOpen();
  });
  await app.listen({ port: 0, host: "127.0.0.1" });
  t.after(() => {
    app.server.closeAllConnections();
    return app.close();
  });
  const controller = new AbortController();
  const response = fetch(`http://127.0.0.1:${(app.server.address() as AddressInfo).port}/v1/events`, { headers: { authorization: `Bearer ${key}` }, signal: controller.signal }).catch(() => null);
  await opened;
  await new Promise((resolve) => setTimeout(resolve, 20));
  await plugin.engine.flush();
  // Counted once while the stream is still open (long-lived connections count as ws_open; units are added at the end)
  assert.deepEqual(transport.rows().map((row) => [row.route, row.result_type, row.count, row.units]), [["/v1/events", "ws_open", 1, 0]]);
  controller.abort();
  await response;
  await new Promise((resolve) => setTimeout(resolve, 20));
  await plugin.engine.flush();
  // Units are added at the end: no track() here, so it's the route weight of 1 (same as Express)
  assert.deepEqual(transport.rows().map((row) => [row.route, row.result_type, row.count, row.units]), [
    ["/v1/events", "ws_open", 1, 0],
    ["/v1/events", "ws_open", 0, 1],
  ]);
});

test("Fastify: a client that disconnects while the SDK waits for a lease is settled as aborted and never reaches the handler", async (t) => {
  const { key, transport, options } = stopBackend([]);
  let releaseLease!: () => void;
  const leaseGate = new Promise<void>((resolve) => (releaseLease = resolve));
  const grantLease = transport.onLeases;
  transport.onLeases = async (request) => {
    await leaseGate;
    return grantLease(request);
  };
  const plugin = fastifyUsageRail({ ...options, snapshot: false, flushOnExit: false });
  await plugin.engine.sync();
  const app = Fastify();
  await app.register(plugin);
  let handlerRuns = 0;
  app.get("/v1/thing", async () => {
    handlerRuns += 1;
    return { ok: true };
  });
  await app.listen({ port: 0, host: "127.0.0.1" });
  t.after(() => app.close());
  const controller = new AbortController();
  const pending = fetch(`http://127.0.0.1:${(app.server.address() as AddressInfo).port}/v1/thing`, { headers: { authorization: `Bearer ${key}` }, signal: controller.signal }).catch(() => null);
  while (transport.leaseCalls.length === 0) await new Promise((resolve) => setImmediate(resolve));
  controller.abort();
  await pending;
  await new Promise((resolve) => setTimeout(resolve, 20));
  releaseLease();
  await new Promise((resolve) => setTimeout(resolve, 20));
  await plugin.engine.flush();
  assert.deepEqual(transport.rows().map((row) => [row.route, row.result_type, row.units]), [["/v1/thing", "aborted", 0]]);
  assert.deepEqual(transport.ingested[0].lease_used, []);
  assert.equal(handlerRuns, 0);
});

test("Fastify: a path Fastify's router rewrites still reserves the weight of the route it lands on", async (t) => {
  const { key, options } = stopBackend([
    { method: "POST", route: "/v1/generate", weight: 10, reserve: 10 },
    { method: "POST", route: "/v2/:name(^foo\\(bar$)/generate", weight: 10, reserve: 10 },
    { method: "POST", route: "/v3/:operation?", weight: 10, reserve: 10 },
  ]);
  const plugin = fastifyUsageRail({ ...options, snapshot: false, flushOnExit: false });
  await plugin.engine.sync();
  const app = Fastify({ routerOptions: { ignoreDuplicateSlashes: true } });
  await app.register(plugin);
  let handlerRuns = 0;
  const handler = async () => {
    handlerRuns += 1;
    return { ok: true };
  };
  // The seller writes templates the way the app registered them (optional parameters, escaped parentheses in regex parameters too), and the reservation follows them
  app.post("/v1/generate", handler);
  app.post("/v2/:name(^foo\\(bar$)/generate", handler);
  app.post("/v3/:operation?", handler);
  await app.listen({ port: 0, host: "127.0.0.1" });
  t.after(() => app.close());
  const base = `http://127.0.0.1:${(app.server.address() as AddressInfo).port}`;
  // A request needs 10, the server has only 5 left: all must be blocked. Fastify decodes percent-encoding and merges repeated slashes before routing; the reservation follows the template it routed to
  for (const path of ["/v1/generate", "/v1/%67enerate", "//v1/generate", "/v1//generate", "/v2/foo(bar/generate", "/v3/anything", "/v3"]) {
    const response = await fetch(`${base}${path}`, { method: "POST", headers: { authorization: `Bearer ${key}` } });
    assert.equal(response.status, 429, path);
  }
  assert.equal(handlerRuns, 0);
});

test("Fastify: a route the seller didn't price is billed 1, whatever priced templates its path looks like", async (t) => {
  // The seller configured /v3/* and /v3/users/:id, the app registered /v3/:kind/:id: settlement only trusts the framework's template, so both count 1
  const { key, transport, options } = backend([
    { method: "ALL", route: "/v3/*", weight: 2, reserve: 2 },
    { method: "POST", route: "/v3/users/:id", weight: 10, reserve: 10 },
  ]);
  const plugin = fastifyUsageRail({ ...options, snapshot: false, flushOnExit: false });
  await plugin.engine.sync();
  const app = Fastify();
  await app.register(plugin);
  app.post("/v3/:kind/:id", async () => ({ ok: true }));
  await app.listen({ port: 0, host: "127.0.0.1" });
  t.after(() => app.close());
  const base = `http://127.0.0.1:${(app.server.address() as AddressInfo).port}`;
  for (const path of ["/v3/users/7", "/v3/orders/7"]) {
    assert.equal((await fetch(`${base}${path}`, { method: "POST", headers: { authorization: `Bearer ${key}` } })).status, 200, path);
  }
  await plugin.engine.flush();
  assert.deepEqual(summary(transport), [["POST", "/v3/:kind/:id", "2xx", 2]]);
});

test("Fastify: a route the seller didn't price reserves 1, not the price of a template its path looks like", async (t) => {
  // The server has only 5 left. The seller priced /v3/users/:id at 10, the app registered /v3/:kind/:id: Fastify has already routed at the start, so it reserves 1 by its template
  const { key, options } = stopBackend([{ method: "POST", route: "/v3/users/:id", weight: 10, reserve: 10 }]);
  const plugin = fastifyUsageRail({ ...options, snapshot: false, flushOnExit: false });
  await plugin.engine.sync();
  const app = Fastify();
  await app.register(plugin);
  app.post("/v3/:kind/:id", async () => ({ ok: true }));
  await app.listen({ port: 0, host: "127.0.0.1" });
  t.after(() => app.close());
  const response = await fetch(`http://127.0.0.1:${(app.server.address() as AddressInfo).port}/v3/users/7`, { method: "POST", headers: { authorization: `Bearer ${key}` } });
  assert.equal(response.status, 200);
});

// ---- Found in review (round three): the client leaves during an earlier hook; Hono is case-sensitive ----
test("Fastify: a client that leaves while an earlier hook runs is settled as aborted and never reaches the handler", async (t) => {
  const { key, transport, options } = backend([]);
  const plugin = fastifyUsageRail({ ...options, snapshot: false, flushOnExit: false });
  await plugin.engine.sync();
  const app = Fastify();
  let entered!: () => void;
  const inHook = new Promise<void>((resolve) => (entered = resolve));
  let release!: () => void;
  const released = new Promise<void>((resolve) => (release = resolve));
  // Your own async auth, registered before the plugin
  app.addHook("onRequest", async () => {
    entered();
    await released;
  });
  await app.register(plugin);
  let handlerRuns = 0;
  app.get("/v1/items", async () => {
    handlerRuns += 1;
    return { ok: true };
  });
  await app.listen({ port: 0, host: "127.0.0.1" });
  t.after(() => app.close());
  const socket = connect((app.server.address() as AddressInfo).port, "127.0.0.1", () => socket.write(`GET /v1/items HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer ${key}\r\n\r\n`));
  socket.on("error", () => {});
  await inHook;
  socket.destroy();
  await new Promise((resolve) => setTimeout(resolve, 50));
  release();
  await new Promise((resolve) => setTimeout(resolve, 50));
  await plugin.engine.flush();
  assert.equal(handlerRuns, 0);
  assert.deepEqual(summary(transport), [["GET", "/v1/items", "aborted", 0]]);
});

test("Hono: routes that differ only in case are reserved and billed separately", async () => {
  const { key, transport, options } = stopBackend([
    { method: "POST", route: "/v1/generate", weight: 10, reserve: 10 },
    { method: "POST", route: "/v1/GENERATE", weight: 1, reserve: 1 },
    { method: "POST", route: "/v1/other", weight: 10, reserve: 10 },
  ]);
  const middleware = honoUsageRail(options);
  await middleware.engine.sync();
  const app = new Hono();
  app.use(middleware);
  app.post("/v1/generate", (c) => c.json({ ok: true }));
  app.post("/v1/GENERATE", (c) => c.json({ ok: true }));
  app.post("/v1/Other", (c) => c.json({ ok: true }));
  const post = (path: string) => app.request(path, { method: "POST", headers: { authorization: `Bearer ${key}` } });
  // The server has only 5 left: the expensive route needs 10 and is blocked; the cheap one differing only in case, and the unpriced /v1/Other, need only 1
  assert.equal((await post("/v1/generate")).status, 429);
  assert.equal((await post("/v1/GENERATE")).status, 200);
  assert.equal((await post("/v1/Other")).status, 200);
  await middleware.engine.flush();
  assert.deepEqual(
    summary(transport).filter((row) => row[2] === "2xx"),
    [
      ["POST", "/v1/GENERATE", "2xx", 1],
      ["POST", "/v1/Other", "2xx", 1],
    ],
  );
});

test("Hono: a template with an empty segment in the middle (/v1//users/:id) is reserved at its weight", async () => {
  const { key, options } = stopBackend([{ method: "POST", route: "/v1//users/:id", weight: 10, reserve: 10 }]);
  const middleware = honoUsageRail(options);
  await middleware.engine.sync();
  const app = new Hono();
  let handlerRuns = 0;
  app.use(middleware);
  app.post("/v1//users/:id", (c) => {
    handlerRuns += 1;
    return c.json({ ok: true });
  });
  const response = await app.request("/v1//users/7", { method: "POST", headers: { authorization: `Bearer ${key}` } });
  assert.equal(response.status, 429);
  assert.equal(handlerRuns, 0);
});

// ---- Found in review (round four): Express mount paths as declared; Fastify WebSocket handshakes; no template at the end means _unmatched ----
async function listen(app: express.Express, t: { after: (fn: () => void) => void }) {
  const server = app.listen(0, "127.0.0.1");
  await new Promise((resolve) => server.once("listening", resolve));
  t.after(() => server.close());
  return `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
}

test("Express: routes under parameterized mounts and nested routers are recorded and billed by their declared template; sub-apps by the path taken", async (t) => {
  const { key, transport, options } = backend([
    { method: "POST", route: "/tenants/:tenantId/generate", weight: 10, reserve: 10 },
    { method: "GET", route: "/v1/teams/:teamId/items/:itemId", weight: 3, reserve: 3 },
    { method: "GET", route: "/accounts/:accountId/ping", weight: 2, reserve: 2 },
  ]);
  const middleware = expressUsageRail({ ...options, snapshot: false, flushOnExit: false });
  await middleware.engine.sync();
  const app = express();
  app.use(middleware);
  const tenants = express.Router();
  tenants.post("/generate", (_req, res) => void res.json({ ok: true }));
  app.use("/tenants/:tenantId", tenants);
  const v1 = express.Router();
  const teams = express.Router();
  teams.get("/items/:itemId", (_req, res) => void res.json({ ok: true }));
  v1.use("/teams/:teamId", teams);
  app.use("/v1", v1);
  // Sub-apps (created with express()) are billed by the actual path
  const accounts = express();
  accounts.get("/ping", (_req, res) => void res.json({ ok: true }));
  app.use("/accounts/:accountId", accounts);
  const base = await listen(app, t);
  const auth = { authorization: `Bearer ${key}` };
  for (const tenant of ["42", "43"]) assert.equal((await fetch(`${base}/tenants/${tenant}/generate`, { method: "POST", headers: auth })).status, 200);
  assert.equal((await fetch(`${base}/V1/teams/7/items/9`, { headers: auth })).status, 200);
  assert.equal((await fetch(`${base}/accounts/5/ping`, { headers: auth })).status, 200);
  await middleware.engine.flush();
  assert.deepEqual(summary(transport), [
    ["POST", "/tenants/:tenantId/generate", "2xx", 20],
    ["GET", "/v1/teams/:teamId/items/:itemId", "2xx", 3],
    ["GET", "/accounts/5/ping", "2xx", 1],
  ]);
});

test("Express: a router mounted at two paths can't tell which one was used, so it falls back to the request's mount point", async (t) => {
  const { key, transport, options } = backend([]);
  const middleware = expressUsageRail({ ...options, snapshot: false, flushOnExit: false });
  await middleware.engine.sync();
  const app = express();
  app.use(middleware);
  const shared = express.Router();
  shared.get("/x", (_req, res) => void res.json({ ok: true }));
  app.use("/a", shared);
  app.use("/b", shared);
  const base = await listen(app, t);
  for (const path of ["/a/x", "/b/x"]) assert.equal((await fetch(`${base}${path}`, { headers: { authorization: `Bearer ${key}` } })).status, 200);
  await middleware.engine.flush();
  assert.deepEqual(summary(transport), [
    ["GET", "/a/x", "2xx", 1],
    ["GET", "/b/x", "2xx", 1],
  ]);
});

test("Hono: a response no route produced is _unmatched at 1 unit, not the seller template its path looked like", async () => {
  // The seller priced /v1/* at 10; no route handled this request and notFound answered 200: settling by the guessed /v1/* would bill for a route that didn't run
  const { key, transport, options } = backend([{ method: "ALL", route: "/v1/*", weight: 10, reserve: 10 }]);
  const middleware = honoUsageRail(options);
  await middleware.engine.sync();
  const app = new Hono();
  app.use(middleware);
  app.notFound((c) => c.text("ok", 200));
  assert.equal((await app.request("/v1/missing", { headers: { authorization: `Bearer ${key}` } })).status, 200);
  await middleware.engine.flush();
  assert.deepEqual(summary(transport), [["GET", "_unmatched", "2xx", 1]]);
});

test("Fastify: a successful WebSocket handshake counts once, and its track() units are added when the connection closes", async (t) => {
  const { key, transport, options } = backend([]);
  const plugin = fastifyUsageRail({ ...options, snapshot: false, flushOnExit: false });
  await plugin.engine.sync();
  const app = Fastify();
  await app.register(plugin);
  await app.register(fastifyWebsocket);
  app.get("/v1/live", { websocket: true }, (socket, request) => {
    request.usageRail.track(7);
    socket.on("message", () => socket.close());
  });
  await app.listen({ port: 0, host: "127.0.0.1" });
  t.after(() => app.close());
  const port = (app.server.address() as AddressInfo).port;
  // The handshake needs the key: Node's built-in WebSocket (undici) accepts headers
  const ws = new WebSocket(`ws://127.0.0.1:${port}/v1/live`, { headers: { authorization: `Bearer ${key}` } } as never);
  await new Promise((resolve, reject) => {
    ws.onopen = resolve;
    ws.onerror = reject;
  });
  ws.send("bye");
  await new Promise((resolve) => (ws.onclose = resolve));
  await new Promise((resolve) => setTimeout(resolve, 50));
  await plugin.engine.flush();
  assert.deepEqual(transport.rows().map((row) => [row.route, row.result_type, row.count, row.units]), [["/v1/live", "ws_open", 1, 7]]);
  // A handshake without a key is blocked in the hook, recorded as sdk_401, and not counted as a connection
  const refused = new WebSocket(`ws://127.0.0.1:${port}/v1/live`);
  await new Promise((resolve) => {
    refused.onerror = resolve;
    refused.onclose = resolve;
  });
});

// ---- Found in review (round five): sub-app mount prefixes; one sub-router in two apps; a sub-app mounted twice; SSE with an Upgrade header; unrouted requests ----
test("Express: requests that end inside a sub-app are billed by the path they took", async (t) => {
  // Express doesn't fully record where sub-apps are mounted, so anything that enters a sub-app is billed by the actual path: mounts without parameters still match the seller's templates,
  // ones with parameters are billed by the concrete path, and unconfigured paths count as 1
  const { key, transport, options } = backend([
    { method: "GET", route: "/admin/x", weight: 4, reserve: 4 },
    { method: "GET", route: "/t/:tenant/x", weight: 10, reserve: 10 },
    { method: "GET", route: "/s/y", weight: 3, reserve: 3 },
  ]);
  const middleware = expressUsageRail({ ...options, snapshot: false, flushOnExit: false });
  await middleware.engine.sync();
  const app = express();
  app.use(middleware);
  const admin = express();
  admin.get("/x", (_req, res) => void res.json({ ok: true }));
  app.use("/admin", admin);
  // Inside a sub-app mounted at the root, mount another one with a parameter
  const root = express();
  const tenant = express();
  tenant.get("/x", (_req, res) => void res.json({ ok: true }));
  root.use("/t/:tenant", tenant);
  app.use(root);
  // A mount path with a trailing slash
  const slashed = express();
  slashed.get("/y", (_req, res) => void res.json({ ok: true }));
  app.use("/s/", slashed);
  const base = await listen(app, t);
  const auth = { authorization: `Bearer ${key}` };
  for (const path of ["/admin/x", "/t/7/x", "/s/y"]) assert.equal((await fetch(`${base}${path}`, { headers: auth })).status, 200, path);
  await middleware.engine.flush();
  assert.deepEqual(summary(transport), [
    ["GET", "/admin/x", "2xx", 4],
    ["GET", "/t/7/x", "2xx", 1],
    ["GET", "/s/y", "2xx", 3],
  ]);
});

test("Express: one router used in two apps is billed by each app's own mount", async (t) => {
  const { key, transport, options } = backend([
    { method: "GET", route: "/a/:org/x", weight: 2, reserve: 2 },
    { method: "GET", route: "/b/:org/x", weight: 10, reserve: 10 },
  ]);
  const shared = express.Router();
  shared.get("/x", (_req, res) => void res.json({ ok: true }));
  const bases: string[] = [];
  const middlewares = [];
  for (const mount of ["/a", "/b"]) {
    const middleware = expressUsageRail({ ...options, snapshot: false, flushOnExit: false });
    await middleware.engine.sync();
    middlewares.push(middleware);
    const app = express();
    app.use(middleware);
    app.use(`${mount}/:org`, shared);
    bases.push(`${await listen(app, t)}${mount}/7`);
  }
  for (const base of bases) assert.equal((await fetch(`${base}/x`, { headers: { authorization: `Bearer ${key}` } })).status, 200);
  for (const middleware of middlewares) await middleware.engine.flush();
  assert.deepEqual(summary(transport), [
    ["GET", "/a/:org/x", "2xx", 2],
    ["GET", "/b/:org/x", "2xx", 10],
  ]);
});

test("Express: a sub-app mounted at two paths is billed by the path each request actually took", async (t) => {
  const { key, transport, options } = backend([
    { method: "GET", route: "/a/x", weight: 2, reserve: 2 },
    { method: "GET", route: "/b/x", weight: 10, reserve: 10 },
  ]);
  const middleware = expressUsageRail({ ...options, snapshot: false, flushOnExit: false });
  await middleware.engine.sync();
  const app = express();
  app.use(middleware);
  const sub = express();
  sub.get("/x", (_req, res) => void res.json({ ok: true }));
  app.use("/a", sub);
  app.use("/b", sub);
  const base = await listen(app, t);
  for (const path of ["/a/x", "/b/x"]) assert.equal((await fetch(`${base}${path}`, { headers: { authorization: `Bearer ${key}` } })).status, 200);
  await middleware.engine.flush();
  assert.deepEqual(summary(transport), [
    ["GET", "/a/x", "2xx", 2],
    ["GET", "/b/x", "2xx", 10],
  ]);
});

test("Fastify: an SSE response is metered even when the request carries an Upgrade header", async (t) => {
  const { key, transport, options } = backend([]);
  const plugin = fastifyUsageRail({ ...options, snapshot: false, flushOnExit: false });
  await plugin.engine.sync();
  const app = Fastify();
  await app.register(plugin);
  app.get("/v1/events", async (_request, reply) => {
    reply.raw.writeHead(200, { "content-type": "text/event-stream" });
    reply.raw.end("data: hi\n\n");
    return reply;
  });
  await app.listen({ port: 0, host: "127.0.0.1" });
  t.after(() => app.close());
  // Only an Upgrade header, without Connection: upgrade: Node doesn't treat it as an upgrade and hands it to Fastify as usual (fetch won't send this header, so use node:http)
  await new Promise<void>((resolve, reject) => {
    const request = httpRequest({ host: "127.0.0.1", port: (app.server.address() as AddressInfo).port, path: "/v1/events", headers: { authorization: `Bearer ${key}`, upgrade: "websocket" } }, (response) => {
      response.resume();
      response.on("end", resolve);
    });
    request.on("error", reject);
    request.end();
  });
  await new Promise((resolve) => setTimeout(resolve, 50));
  await plugin.engine.flush();
  assert.deepEqual(transport.rows().map((row) => [row.route, row.result_type, row.count]), [["/v1/events", "ws_open", 1]]);
});

test("Fastify: a request no route matches reserves 1, not the price of a template its URL looks like", async (t) => {
  // The server has only 5 left, and the seller priced /v1/* at 10: unrouted /v1/missing is a 404 and must not be blocked with a 429 as /v1/*
  const { key, transport, options } = stopBackend([{ method: "ALL", route: "/v1/*", weight: 10, reserve: 10 }]);
  const plugin = fastifyUsageRail({ ...options, snapshot: false, flushOnExit: false });
  await plugin.engine.sync();
  const app = Fastify();
  await app.register(plugin);
  await app.listen({ port: 0, host: "127.0.0.1" });
  t.after(() => app.close());
  const response = await fetch(`http://127.0.0.1:${(app.server.address() as AddressInfo).port}/v1/missing`, { headers: { authorization: `Bearer ${key}` } });
  assert.equal(response.status, 404);
  await plugin.engine.flush();
  assert.deepEqual(transport.rows().map((row) => [row.route, row.result_type]), [["_unmatched", "4xx"]]);
});

// ---- Found in review (fix-only round): how routes are written and each layer's own case setting don't affect the joined template ----
test("Express: routes with a param inside a segment or an optional part keep their template under a parameterised mount", async (t) => {
  const { key, transport, options } = backend([
    { method: "GET", route: "/tenants/:tenantId/file-:id", weight: 10, reserve: 10 },
    { method: "GET", route: "/tenants/:tenantId/files{/:id}", weight: 4, reserve: 4 },
  ]);
  const middleware = expressUsageRail({ ...options, snapshot: false, flushOnExit: false });
  await middleware.engine.sync();
  const app = express();
  app.use(middleware);
  const tenants = express.Router();
  tenants.get("/file-:id", (_req, res) => void res.json({ ok: true }));
  tenants.get("/files{/:id}", (_req, res) => void res.json({ ok: true }));
  app.use("/tenants/:tenantId", tenants);
  const base = await listen(app, t);
  for (const path of ["/tenants/7/file-3", "/tenants/7/files", "/tenants/7/files/3"]) {
    assert.equal((await fetch(`${base}${path}`, { headers: { authorization: `Bearer ${key}` } })).status, 200);
  }
  await middleware.engine.flush();
  assert.deepEqual(summary(transport), [
    ["GET", "/tenants/:tenantId/file-:id", "2xx", 10],
    ["GET", "/tenants/:tenantId/files{/:id}", "2xx", 8],
  ]);
});

test("Express: a case-insensitive router inside a case-sensitive app keeps its template", async (t) => {
  const { key, transport, options } = backend([{ method: "GET", route: "/tenants/:tenantId/files", weight: 10, reserve: 10 }]);
  const middleware = expressUsageRail({ ...options, snapshot: false, flushOnExit: false });
  await middleware.engine.sync();
  const app = express();
  app.set("case sensitive routing", true);
  app.use(middleware);
  const tenants = express.Router({ caseSensitive: false });
  tenants.get("/files", (_req, res) => void res.json({ ok: true }));
  app.use("/tenants/:tenantId", tenants);
  const base = await listen(app, t);
  assert.equal((await fetch(`${base}/tenants/7/FILES`, { headers: { authorization: `Bearer ${key}` } })).status, 200);
  await middleware.engine.flush();
  assert.deepEqual(summary(transport), [["GET", "/tenants/:tenantId/files", "2xx", 10]]);
});

test("Express: a sub-app mounted twice is billed by the path the request took, however alike the two mount paths look", async (t) => {
  // Each mount has its own price: when we can't tell which one the request took, bill by its actual path, never at the other one's price
  const cases = [
    { mounts: ["/a", "/b{/:id}"], paths: ["/a/x"], caseSensitive: false },
    { mounts: ["/a", "/A"], paths: ["/a/x", "/A/x"], caseSensitive: true },
    { mounts: ["/a//b", "/a/b"], paths: ["/a//b/x", "/a/b/x"], caseSensitive: false },
    { mounts: ["/%61", "/a"], paths: ["/%61/x", "/a/x"], caseSensitive: false },
  ];
  for (const { mounts, paths, caseSensitive } of cases) {
    const weights = new Map(paths.map((path, index) => [path, index === 0 ? 2 : 10]));
    const { key, transport, options } = backend([...weights].map(([route, weight]) => ({ method: "GET", route, weight, reserve: weight })));
    const middleware = expressUsageRail({ ...options, snapshot: false, flushOnExit: false });
    await middleware.engine.sync();
    const app = express();
    if (caseSensitive) app.set("case sensitive routing", true);
    app.use(middleware);
    const sub = express();
    sub.get("/x", (_req, res) => void res.json({ ok: true }));
    for (const mount of mounts) app.use(mount, sub);
    const base = await listen(app, t);
    for (const path of paths) assert.equal((await fetch(`${base}${path}`, { headers: { authorization: `Bearer ${key}` } })).status, 200, path);
    await middleware.engine.flush();
    assert.deepEqual(summary(transport), paths.map((path) => ["GET", path, "2xx", weights.get(path)]), mounts.join(" + "));
  }
});

// ---- Found in review (fix-only round): a sub-app mounted in two places, under a router, or mounted again while running is billed by the path the request took ----
test("Express: a sub-app mounted twice, or under a router, is billed by the path the request took", async (t) => {
  const { key, transport, options } = backend([
    { method: "GET", route: "/a/x", weight: 2, reserve: 2 },
    { method: "GET", route: "/b/x", weight: 10, reserve: 10 },
    { method: "GET", route: "/r/x", weight: 5, reserve: 5 },
  ]);
  const middleware = expressUsageRail({ ...options, snapshot: false, flushOnExit: false });
  await middleware.engine.sync();
  const app = express();
  app.use(middleware);
  // Mounted at /a and /b: Express only remembers the last mount path
  const twice = express();
  twice.get("/x", (_req, res) => void res.json({ ok: true }));
  app.use("/a", twice);
  app.use("/b", twice);
  // Mounted directly under a router: Express records neither the level above nor the mount path
  const underRouter = express();
  underRouter.get("/x", (_req, res) => void res.json({ ok: true }));
  const router = express.Router();
  router.use("/r", underRouter);
  app.use(router);
  const base = await listen(app, t);
  for (const path of ["/a/x", "/r/x"]) assert.equal((await fetch(`${base}${path}`, { headers: { authorization: `Bearer ${key}` } })).status, 200, path);
  await middleware.engine.flush();
  assert.deepEqual(summary(transport), [
    ["GET", "/a/x", "2xx", 2],
    ["GET", "/r/x", "2xx", 5],
  ]);
});

test("Express: a sub-app mounted a second time while running is no longer billed by its first mount", async (t) => {
  const { key, transport, options } = backend([
    { method: "GET", route: "/a/x", weight: 2, reserve: 2 },
    { method: "GET", route: "/b/x", weight: 10, reserve: 10 },
  ]);
  const middleware = expressUsageRail({ ...options, snapshot: false, flushOnExit: false });
  await middleware.engine.sync();
  const app = express();
  app.use(middleware);
  const sub = express();
  sub.get("/x", (_req, res) => void res.json({ ok: true }));
  app.use("/a", sub);
  const base = await listen(app, t);
  const auth = { authorization: `Bearer ${key}` };
  assert.equal((await fetch(`${base}/a/x`, { headers: auth })).status, 200);
  // Mounted in one more place after the first request: the template found earlier can't be reused
  app.use("/b", sub);
  assert.equal((await fetch(`${base}/b/x`, { headers: auth })).status, 200);
  await middleware.engine.flush();
  assert.deepEqual(summary(transport), [
    ["GET", "/a/x", "2xx", 2],
    ["GET", "/b/x", "2xx", 10],
  ]);
});

