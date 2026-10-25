import { test } from "node:test";
import assert from "node:assert/strict";

import { createFetchTransport, isPausingStatus, parseRetryAfter, TransportError } from "../src/transport.ts";

function transportReturning(response: () => Response, seen: { url?: string; init?: RequestInit } = {}) {
  return createFetchTransport({
    appKey: "urk_test_transport",
    instanceId: "inst-1",
    baseUrl: "http://api.test/",
    fetch: (async (url: string, init?: RequestInit) => {
      seen.url = url;
      seen.init = init;
      return response();
    }) as typeof fetch,
  });
}

test("requests carry the appKey and instance ID; the sync cursor is URL-encoded", async () => {
  const seen: { url?: string; init?: RequestInit } = {};
  const transport = transportReturning(() => Response.json({ cursor: "c" }), seen);
  await transport.sync("a+b/c=");
  assert.equal(seen.url, "http://api.test/v1/sync?cursor=a%2Bb%2Fc%3D");
  assert.deepEqual(seen.init?.headers, { authorization: "Bearer urk_test_transport", "x-instance-id": "inst-1" });
});

test("a timeout with fractional milliseconds (from the monotonic clock) is accepted", async () => {
  const seen: { init?: RequestInit } = {};
  const transport = transportReturning(() => Response.json({ results: [] }), seen);
  await transport.leases({ requests: [] }, { timeoutMs: 299.99958300000003 });
  assert.ok(seen.init?.signal instanceof AbortSignal);
});

test("non-2xx responses become TransportError with the status and Retry-After", async () => {
  const transport = transportReturning(() => new Response("slow down", { status: 429, headers: { "retry-after": "7" } }));
  await assert.rejects(transport.ingest({} as never), (error: unknown) => {
    assert.ok(error instanceof TransportError);
    assert.equal(error.status, 429);
    assert.equal(error.retryAfterMs, 7_000);
    return true;
  });
});

test("Retry-After is seconds or an HTTP date; config errors pause, transient ones don't", () => {
  const now = Date.parse("2026-10-20T12:00:00Z");
  assert.equal(parseRetryAfter("3", now), 3_000);
  assert.equal(parseRetryAfter("Tue, 20 Oct 2026 12:00:30 GMT", now), 30_000);
  assert.equal(parseRetryAfter("soon", now), null);
  assert.equal(parseRetryAfter(null, now), null);
  assert.deepEqual([401, 403, 404, 409, 410].map(isPausingStatus), [true, true, true, true, true]);
  assert.deepEqual([null, 400, 408, 413, 429, 500, 503].map(isPausingStatus), [false, false, false, false, false, false, false]);
});
