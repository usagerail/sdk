// Fake clock and fake transport for tests: no network, no real waiting.
import type { Clock } from "../src/clock.ts";
import type {
  AppSettings,
  ConsumerRecord,
  IngestRequest,
  IngestResponse,
  KeyRecord,
  LeaseRequest,
  LeaseResponse,
  SyncResponse,
  VerifyKeyRequest,
  VerifyKeyResponse,
} from "../src/protocol.ts";
import type { CallOptions, Transport } from "../src/transport.ts";

// Let every queued Promise callback run (setImmediate runs after microtasks)
export const settle = () => new Promise<void>((resolve) => setImmediate(resolve));

type Timer = { at: number; order: number; callback: () => void };

export class FakeClock implements Clock {
  wall: number;
  mono = 1_000_000;
  private timers: Timer[] = [];
  private order = 0;

  // Starts 10 seconds into a whole minute by default; period boundary tests set their own
  constructor(wall = Date.UTC(2026, 9, 20, 12, 0, 10)) {
    this.wall = wall;
  }

  now() {
    return this.wall;
  }

  monotonic() {
    return this.mono;
  }

  setTimer(callback: () => void, delayMs: number) {
    const timer: Timer = { at: this.mono + Math.max(0, delayMs), order: this.order++, callback };
    this.timers.push(timer);
    return () => {
      this.timers = this.timers.filter((item) => item !== timer);
    };
  }

  // Move forward ms milliseconds, firing timers that come due along the way in order, letting Promise chains run after each
  async advance(ms: number) {
    const target = this.mono + ms;
    for (;;) {
      await settle();
      const due = this.timers.filter((timer) => timer.at <= target).sort((a, b) => a.at - b.at || a.order - b.order)[0];
      if (!due) break;
      this.timers = this.timers.filter((timer) => timer !== due);
      this.wall += due.at - this.mono;
      this.mono = due.at;
      due.callback();
    }
    this.wall += target - this.mono;
    this.mono = target;
    await settle();
  }
}

type Handler<Request, Response> = (request: Request, options?: CallOptions) => Response | Promise<Response>;

export class FakeTransport implements Transport {
  readonly ingested: IngestRequest[] = [];
  readonly leaseCalls: LeaseRequest[] = [];
  readonly verifyCalls: VerifyKeyRequest[] = [];
  readonly syncCalls: (string | null)[] = [];
  onIngest: Handler<IngestRequest, IngestResponse> = () => accepted();
  onSync: Handler<string | null, SyncResponse> = () => syncPage();
  onVerify: Handler<VerifyKeyRequest, VerifyKeyResponse> = () => ({ valid: false, reason: "not_found" });
  onLeases: Handler<LeaseRequest, LeaseResponse> = (request) => ({
    results: request.requests.map((item) => ({ consumer: item.consumer, status: "unlimited" as const, quota_version: item.quota_version })),
  });

  async ingest(request: IngestRequest, options?: CallOptions) {
    // Store a deep copy, so later changes by the engine don't affect what the test inspects
    this.ingested.push(structuredClone(request));
    return this.onIngest(request, options);
  }

  async sync(cursor: string | null, options?: CallOptions) {
    this.syncCalls.push(cursor);
    return this.onSync(cursor, options);
  }

  async verifyKey(request: VerifyKeyRequest, options?: CallOptions) {
    this.verifyCalls.push(request);
    return this.onVerify(request, options);
  }

  async leases(request: LeaseRequest, options?: CallOptions) {
    this.leaseCalls.push(structuredClone(request));
    return this.onLeases(request, options);
  }

  rows() {
    return this.ingested.flatMap((batch) => batch.rows);
  }
}

export function accepted(): IngestResponse {
  return { duplicate: false, accepted_rows: 0, rejected_rows: [], unknown_keys: [], unknown_leases: [] };
}

export function appSettings(overrides: Partial<AppSettings> = {}): AppSettings {
  return {
    app_id: "app_1",
    mode: "test",
    integration: "sdk",
    hash_key: null,
    rate_limit_mode: "per_instance",
    on_key_store_unavailable: "deny",
    stale_key_snapshot: "allow",
    on_quota_unavailable: "allow",
    unit_mode: "requests",
    routes: [],
    ...overrides,
  };
}

export function syncPage(overrides: Partial<SyncResponse> = {}): SyncResponse {
  return {
    cursor: "cursor-1",
    full: true,
    has_more: false,
    app: appSettings(),
    keys: [],
    removed_keys: [],
    consumers: [],
    active_instances: 1,
    server_time: new Date().toISOString(),
    ...overrides,
  };
}

export function consumerRecord(overrides: Partial<ConsumerRecord> = {}): ConsumerRecord {
  return {
    consumer: "cons_1",
    external_id: null,
    status: "active",
    quota_behavior: "overage",
    quota_version: 1,
    rate_limits: { per_second: null, per_minute: null },
    ...overrides,
  };
}

export function keyRecord(overrides: Partial<KeyRecord> & Pick<KeyRecord, "key_id" | "hash">): KeyRecord {
  return { consumer: "cons_1", status: "active", expires_at: null, quota_behavior: "overage", quota_version: 1, ...overrides };
}

export const silentLogger = { warn: () => {} };

export function collectingLogger() {
  const messages: string[] = [];
  return { messages, warn: (message: string) => messages.push(message) };
}
