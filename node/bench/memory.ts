// How much memory the SDK uses (target: under 50 MB at a typical scale of up to 10,000 keys and 10,000 customers; under 100 MB fully loaded at 100,000 each).
// Measures the V8 heap (after gc); the sync response itself is discarded after use and not counted. Two scenarios, same as Python's bench/memory.py:
// - after sync: the key snapshot and consumer records;
// - all active: after sync, each customer makes one request with their key, so quota state, leases, rate limit buckets and the metering table all exist.
// Usage: pnpm --filter @usagerail/node bench:memory [number of keys and customers, default 100000]
import { createHash } from "node:crypto";

import { createEngine } from "../src/engine.ts";
import { generateApiKey } from "../src/key-format.ts";
import type { IngestResponse, LeaseResponse, SyncResponse } from "../src/protocol.ts";
import type { Transport } from "../src/transport.ts";

const count = Number(process.argv[2] ?? 100_000);
const gc = globalThis.gc;
if (!gc) throw new Error("run with node --expose-gc");

// One key per customer, external IDs are 64 hex characters (as with hashing on), hard limits, 10 a second and 300 a minute.
// Raw keys are generated before measuring starts and aren't counted against the engine
const rawKeys = Array.from({ length: count }, () => generateApiKey({ prefix: "acme", environment: "live" }));
const consumerId = (index: number) => `cons_${String(index).padStart(16, "0")}`;

function fullSync(): SyncResponse {
  return {
    cursor: "c",
    full: true,
    has_more: false,
    keys: rawKeys.map((key, index) => ({
      key_id: key.keyId,
      hash: createHash("sha256").update(key.key).digest("hex"),
      consumer: consumerId(index),
      status: "active",
      expires_at: null,
      quota_behavior: "stop",
      quota_version: 3,
    })),
    removed_keys: [],
    consumers: rawKeys.map((_, index) => ({
      consumer: consumerId(index),
      external_id: createHash("sha256").update(`user-${index}`).digest("hex"),
      status: "active",
      quota_behavior: "stop",
      quota_version: 3,
      rate_limits: { per_second: 10, per_minute: 300 },
    })),
    app: {
      app_id: "app_bench",
      mode: "live",
      integration: "sdk",
      hash_key: null,
      rate_limit_mode: "per_instance",
      on_key_store_unavailable: "deny",
      stale_key_snapshot: "allow",
      on_quota_unavailable: "allow",
      unit_mode: "requests",
      routes: [],
    },
    active_instances: 1,
    server_time: new Date().toISOString(),
  } as SyncResponse;
}

let leaseId = 0;
const transport: Transport = {
  sync: async () => fullSync(),
  ingest: async (): Promise<IngestResponse> => ({ duplicate: false, accepted_rows: 0, rejected_rows: [], unknown_keys: [], unknown_leases: [] }),
  verifyKey: async () => ({ valid: false, reason: "not_found" }),
  // Every lease request gets 100 units
  leases: async (request): Promise<LeaseResponse> => ({
    results: request.requests.map((item) => ({
      consumer: item.consumer,
      status: "granted",
      lease: { lease_id: String(++leaseId), consumer: item.consumer, granted: 100, quota_version: 3, behavior: "stop", period_end: new Date(Date.now() + 3_600_000).toISOString() },
    })),
  }),
};

const heap = () => {
  gc();
  return process.memoryUsage().heapUsed;
};
const mib = (bytes: number) => `${(bytes / 2 ** 20).toFixed(1)} MiB`;

const before = heap();
const engine = createEngine({ appKey: "urk_live_bench", auth: "api-key", transport, backgroundLoops: false, logger: { warn() {} } });
await engine.sync();
const synced = heap() - before;
for (const key of rawKeys) {
  let decision = engine.begin({ method: "GET", path: "/v1/items", apiKey: key.key, context: {} });
  if (decision instanceof Promise) decision = await decision;
  if (decision.allowed) engine.end(decision.handle, { status: 200, route: "/v1/items" });
}
const active = heap() - before;
console.log(`Node ${process.version}, ${count.toLocaleString("en-US")} keys and customers`);
console.log(`  synced: ${mib(synced)}`);
console.log(`  every customer has made a request: ${mib(active)}`);
