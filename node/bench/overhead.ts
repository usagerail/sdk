// Time the SDK adds to each request (target: p99 < 1 ms on 2 vCPUs). Caches are warm, no network:
// measures begin() + end() alone, i.e. local key check, rate limit, quota reservation and metering aggregation.
// Usage: pnpm --filter @usagerail/node bench
import { createHash } from "node:crypto";
import { cpus } from "node:os";

import { createEngine, type Engine } from "../src/engine.ts";
import { generateApiKey } from "../src/key-format.ts";
import type { IngestResponse, LeaseResponse, SyncResponse } from "../src/protocol.ts";
import type { Transport } from "../src/transport.ts";

const ITERATIONS = 200_000;
const WARMUP = 20_000;

function transportWith(sync: SyncResponse): Transport {
  return {
    sync: async () => sync,
    ingest: async (): Promise<IngestResponse> => ({ duplicate: false, accepted_rows: 0, rejected_rows: [], unknown_keys: [], unknown_leases: [] }),
    verifyKey: async () => ({ valid: false, reason: "not_found" }),
    leases: async (request): Promise<LeaseResponse> => ({
      results: request.requests.map((item) => ({
        consumer: item.consumer,
        status: "granted",
        lease: { lease_id: "1", consumer: item.consumer, granted: 1_000_000_000, quota_version: 1, behavior: "stop", period_end: new Date(Date.now() + 3_600_000).toISOString() },
      })),
    }),
  };
}

function syncResponse(key: { keyId: string; key: string }, behavior: "overage" | "stop", hashKey: string | null = null): SyncResponse {
  return {
    cursor: "c",
    full: true,
    has_more: false,
    app: {
      app_id: "app_bench",
      mode: "test",
      integration: "sdk",
      hash_key: hashKey,
      rate_limit_mode: "per_instance",
      on_key_store_unavailable: "deny",
      stale_key_snapshot: "allow",
      on_quota_unavailable: "allow",
      unit_mode: "requests",
      routes: [
        { method: "POST", route: "/v1/images", weight: 10, reserve: 10 },
        { method: "GET", route: "/v1/users/:id", weight: 1, reserve: 1 },
      ],
    },
    keys: [{ key_id: key.keyId, hash: createHash("sha256").update(key.key).digest("hex"), consumer: "cons_bench", status: "active", expires_at: null, quota_behavior: behavior, quota_version: 1 }],
    removed_keys: [],
    consumers: [
      {
        consumer: "cons_bench",
        external_id: hashKey ? createHash("sha256").update("unused").digest("hex") : null,
        status: "active",
        quota_behavior: behavior,
        quota_version: 1,
        rate_limits: { per_second: 1_000_000, per_minute: 60_000_000 },
      },
    ],
    active_instances: 1,
    server_time: new Date().toISOString(),
  };
}

async function measure(name: string, engine: Engine<{ user: string }>, request: Parameters<Engine<{ user: string }>["begin"]>[0]) {
  // The first call may wait for a lease, so run it once first
  const first = await engine.begin(request);
  if (first.allowed) engine.end(first.handle, { status: 200, route: "/v1/users/:id" });
  const samples = new Float64Array(ITERATIONS);
  for (let index = 0; index < WARMUP + ITERATIONS; index++) {
    const started = performance.now();
    const decision = engine.begin(request);
    if (decision instanceof Promise) throw new Error(`${name}: begin() had to wait for the network`);
    if (decision.allowed) engine.end(decision.handle, { status: 200, route: "/v1/users/:id" });
    const elapsed = performance.now() - started;
    if (index >= WARMUP) samples[index - WARMUP] = elapsed;
  }
  samples.sort();
  const at = (fraction: number) => samples[Math.min(samples.length - 1, Math.floor(samples.length * fraction))] * 1000;
  console.log(`${name.padEnd(44)} p50 ${at(0.5).toFixed(1).padStart(6)} µs   p99 ${at(0.99).toFixed(1).padStart(6)} µs   p99.9 ${at(0.999).toFixed(1).padStart(6)} µs`);
  await engine.close();
}

const key = generateApiKey({ prefix: "bench", environment: "test" });
const base = { appKey: "urk_bench", backgroundLoops: false, logger: { warn: () => {} } } as const;
console.log(`Node ${process.version}, ${cpus()[0]?.model ?? "unknown CPU"}, ${ITERATIONS.toLocaleString()} requests each`);

const overage = createEngine<{ user: string }>({ ...base, auth: "api-key", transport: transportWith(syncResponse(key, "overage")) });
await overage.sync();
await measure("our key, overage plan, rate limited", overage, { method: "GET", path: "/v1/users/42", apiKey: key.key });

const stop = createEngine<{ user: string }>({ ...base, auth: "api-key", transport: transportWith(syncResponse(key, "stop")) });
await stop.sync();
await measure("our key, stop-at-limit (lease reservation)", stop, { method: "GET", path: "/v1/users/42", apiKey: key.key });

const custom = createEngine<{ user: string }>({ ...base, auth: "custom", consumer: (context) => ({ id: context.user }), transport: transportWith(syncResponse(key, "overage", "hash-key")) });
await custom.sync();
await measure("own auth, hashed external ID", custom, { method: "GET", path: "/v1/users/42", context: { user: "user-42" } });
