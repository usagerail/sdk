// The SDK engine: ties key checks, rate limits, quotas and metering together, independent of any framework.
// Order of checks: key format and checksum → status and expiry → consumer suspended → rate limit → quota.
// The hot path stays off the network. begin() returns a Promise only when it really has to wait for the server (remote verification of a new key, a limited consumer's first lease,
// a process that just started without a snapshot); otherwise it returns the decision synchronously.
// Internal errors are swallowed with a warning (at most one a minute) and never affect requests; only keys, suspensions, rate limits and quotas reject requests.
import { createHmac } from "node:crypto";

import { type Clock, systemClock, waitingClock } from "./clock.ts";
import { KeyStore, SNAPSHOT_MAX_AGE_MS, type KeyCheck, type SnapshotStore, type SyncSnapshot } from "./key-store.ts";
import { ANONYMOUS_CONSUMER, Meter, UNMATCHED_ROUTE, classifyStatus } from "./meter.ts";
import {
  BILLABLE_OUTCOMES,
  type AppSettings,
  type ConsumerKind,
  type ConsumerRecord,
  type Outcome,
  type RouteSetting,
  type SyncResponse,
} from "./protocol.ts";
import { QuotaManager, type QuotaDecision, type QuotaInfo, type Reservation } from "./quota.ts";
import { RateLimiter, type RateDecision, type RateLimits } from "./rate-limiter.ts";
import { RouteTable } from "./routes.ts";
import { createFetchTransport, describeError, isPausingStatus, TransportError, type Transport } from "./transport.ts";

// Sync every 15 seconds
export const SYNC_INTERVAL_MS = 15_000;
// Errors like a revoked app key: try again every 10 minutes
const PAUSED_SYNC_MS = 10 * 60_000;
// Right after startup with no usable snapshot (or, with your own auth, before the hashing settings arrive), requests first wait for the first sync, up to 1 second,
// then fall back to remote verification (the same rule as a serverless cold start). Without the wait, the first burst of traffic would all go to remote verification,
// and keys beyond 20 a second would get a 401 straight away
export const FIRST_SYNC_WAIT_MS = 1_000;
// Serverless: a request that finds the snapshot older than 20 seconds syncs first (waiting up to 1 second), so a revoked key stops working within about 20 seconds.
// An instance that sat idle for a long time may hold an old snapshot when a request arrives
export const SERVERLESS_STALE_MS = 20_000;
// Serverless: at least 0.5 seconds between two submissions from one instance, no more than 120 batches a minute
export const SERVERLESS_SUBMIT_GAP_MS = 500;
// How far back a reported minute can be: the server only accepts the last 24 hours, so leave an hour of margin (same as at in lease requests)
const MAX_ROW_AGE_MS = 23 * 60 * 60_000;
// Serverless: minimum wait before retrying a failed sync; the server allows at most 6 syncs per instance per minute. Configuration errors (like a revoked app key) wait 10 minutes
export const SERVERLESS_SYNC_RETRY_MS = 10_000;
// Save to disk at most once a minute: a snapshot of 100,000 keys is over ten MB, no need to write it every 15 seconds
const PERSIST_INTERVAL_MS = 60_000;
// At most 100,000 consumer records from sync, to keep memory bounded
const MAX_CONSUMER_RECORDS = 100_000;
// Remember at most 1,000 shared rate limit configurations: when limits follow plans there are only a handful. If every customer has their own and they change often, once full, new ones are stored individually,
// so stale configurations don't hold memory forever
const MAX_SHARED_RATE_LIMITS = 1_000;
// Cache of HMAC'd external IDs, so hot consumers aren't hashed on every request
const MAX_HASH_CACHE = 10_000;
const MAX_SYNC_PAGES = 1_000;
const WARNING_INTERVAL_MS = 60_000;
const CLOCK_SKEW_WARNING_MS = 60_000;
const CLOSE_TIMEOUT_MS = 5_000;
// track() takes at most 10,000,000 per call
export const MAX_TRACK_UNITS = 10_000_000;
// External IDs are at most 256 characters
const MAX_EXTERNAL_ID_LENGTH = 256;

export type ConsumerIdentity = { id: string; stripeCustomerId?: string };

export type EngineOptions<Context = unknown> = {
  // Created in the console, used only for reporting and sync
  appKey: string;
  // "api-key": use the keys we issue; "custom": you authenticate requests yourself and tell the SDK who it is through consumer()
  auth: "api-key" | "custom";
  // Required with your own auth. Returning null means anonymous: analytics only, no rate limit, no quota, not billed
  consumer?: (context: Context) => ConsumerIdentity | null | undefined;
  baseUrl?: string;
  clock?: Clock;
  transport?: Transport;
  // The three availability settings. If not passed, the console's settings apply (delivered by sync); before the first sync, the defaults apply
  onKeyStoreUnavailable?: "deny" | "allow";
  staleKeySnapshot?: "allow" | "deny";
  onQuotaUnavailable?: "allow" | "deny";
  // Saving the snapshot to disk (Node only, see fileSnapshotStore in node.ts)
  snapshotStore?: SnapshotStore;
  logger?: { warn(message: string): void };
  // Background sync and periodic reporting. Turned off in tests, which call sync() / flush() by hand
  backgroundLoops?: boolean;
  // Serverless (Vercel, Cloudflare Workers, etc.): no background timers, since the instance may be frozen between requests. Sync and reporting happen in
  // afterResponse() after each request, handed to the platform's waitUntil
  serverless?: boolean;
  instanceId?: string;
};

export type BeginRequest<Context = unknown> = {
  method: string;
  // Raw path, without query string. When the template isn't known yet at the start, it's matched against the seller's templates to decide the reservation
  path: string;
  // Frameworks that have already routed when the request starts (Fastify, Next.js) pass the matched template: the reservation follows it instead of guessing from the path. Pass undefined if nothing matched
  route?: string;
  // Pass true when the framework routes case-insensitively (Express's default): matching paths to templates and looking up weights at the end both ignore case
  ignoreCase?: boolean;
  // With our keys: the key the request carries (adapters read it from Authorization: Bearer or x-api-key)
  apiKey?: string | null;
  // With your own auth: passed to consumer() as is
  context?: Context;
};

export type RejectionCode =
  | "invalid_api_key"
  | "consumer_suspended"
  | "rate_limited"
  | "quota_exceeded"
  | "insufficient_credits"
  | "auth_unavailable"
  | "quota_unavailable";

export type Rejection = {
  readonly allowed: false;
  readonly status: 401 | 402 | 403 | 429 | 503;
  readonly error: RejectionCode;
  readonly retryAfterSeconds: number | null;
  readonly headers: Readonly<Record<string, string>>;
};

export type RequestHandle = { track(units: number): void };
export type Admission = { readonly allowed: true; readonly handle: RequestHandle };
export type Decision = Admission | Rejection;

// End of a request: a status code, or an outcome directly (aborted, ws_open).
// route: the route template from the framework. Optional if it was passed to begin(); with neither, the request is recorded as _unmatched and counts as 1
export type EndResult = { status: number; route?: string } | { outcome: Outcome; route?: string };

export type Engine<Context = unknown> = {
  readonly instanceId: string;
  begin(request: BeginRequest<Context>): Decision | Promise<Decision>;
  // Call when an SSE stream starts sending: the long-lived connection counts once right away. Apps metering custom units only know what track() recorded when the connection ends,
  // so the reservation stays until end(), which settles by actual units and adds them
  streamStarted(handle: RequestHandle, route?: string): void;
  end(handle: RequestHandle, result: EndResult): void;
  sync(): Promise<void>;
  flush(): Promise<void>;
  // Call when the app is shutting down and draining requests: switches to reporting once a second, so requests that finish during the drain are reported too. Still call close() at the end
  drain(): void;
  // Serverless: call after each request and hand the returned Promise to the platform's waitUntil. Syncs if it's time, sends queued renewals right away,
  // then submits usage (at least 0.5 seconds between submissions; if one is in flight it waits for it, so the last request always takes the final batch)
  afterResponse(): Promise<void>;
  close(): Promise<void>;
};

export function createEngine<Context = unknown>(options: EngineOptions<Context>): Engine<Context> {
  return new UsageRailEngine(options);
}

// Authorization: Bearer <key> (scheme is case-insensitive, RFC 6750); otherwise x-api-key
export function readApiKey(authorization: string | null | undefined, xApiKey: string | null | undefined): string | undefined {
  if (authorization) {
    const match = /^bearer[ \t]+(\S+)\s*$/i.exec(authorization.trim());
    if (match) return match[1];
  }
  const trimmed = xApiKey?.trim();
  return trimmed ? trimmed : undefined;
}

// Default body for rejections. You can write your own in the adapter's onReject
export function rejectionBody(rejection: Rejection): { error: RejectionCode; retry_after?: number } {
  return rejection.retryAfterSeconds === null ? { error: rejection.error } : { error: rejection.error, retry_after: rejection.retryAfterSeconds };
}

function rejection(status: Rejection["status"], error: RejectionCode, retryAfterSeconds: number | null = null, headers: Record<string, string> = {}): Rejection {
  const allHeaders = retryAfterSeconds === null || "Retry-After" in headers ? headers : { ...headers, "Retry-After": String(retryAfterSeconds) };
  return Object.freeze({ allowed: false, status, error, retryAfterSeconds, headers: Object.freeze(allHeaders) });
}

const INVALID_API_KEY = rejection(401, "invalid_api_key");
const CONSUMER_SUSPENDED = rejection(403, "consumer_suspended");
const INSUFFICIENT_CREDITS = rejection(402, "insufficient_credits");
const AUTH_UNAVAILABLE = rejection(503, "auth_unavailable");
const QUOTA_UNAVAILABLE = rejection(503, "quota_unavailable");

type KnownConsumer = {
  kind: "consumer";
  // Consumer reference for reporting: our consumer ID, or the external ID (HMAC'd by default); the raw external ID until the hashing settings arrive
  consumer: string;
  consumerKind: ConsumerKind;
  pendingHash: boolean;
  keyId: string | null;
  stripeCustomerId: string | undefined;
  quota: QuotaInfo;
  rateLimits: RateLimits | null;
  suspended: boolean;
};

type Identity = KnownConsumer | { kind: "anonymous" } | { kind: "invalid"; consumer: string | null } | { kind: "unavailable" };

const ANONYMOUS: Identity = Object.freeze({ kind: "anonymous" });
const UNAVAILABLE: Identity = Object.freeze({ kind: "unavailable" });

type RequestStart = {
  method: string;
  // The template the framework provided at the start (Fastify, Next.js)
  route: string | undefined;
  // Otherwise, the seller template the path matched: only used to record requests the SDK rejects (they never reach a route), never for settlement
  guessedRoute: string | undefined;
  ignoreCase: boolean;
  // Units this request reserves
  reserve: number;
  startedAt: number;
  minuteStart: number;
};

class ActiveRequest implements RequestHandle {
  readonly method: string;
  // Route template the framework provided when the request started (Fastify, Next.js)
  readonly routeAtStart: string | undefined;
  readonly ignoreCase: boolean;
  readonly minuteStart: number;
  readonly consumer: string;
  readonly consumerKind: ConsumerKind | "anonymous";
  readonly pendingHash: boolean;
  readonly stripeCustomerId: string | undefined;
  readonly reservation: Reservation | null;
  private readonly warn: (message: string) => void;
  trackedUnits: number | null = null;
  ended = false;
  // The SSE stream started and the connection was counted once; units are added when it ends (streamStarted)
  streamOpen = false;

  constructor(start: RequestStart, identity: KnownConsumer | null, reservation: Reservation | null, warn: (message: string) => void) {
    this.method = start.method;
    this.routeAtStart = start.route;
    this.ignoreCase = start.ignoreCase;
    this.minuteStart = start.minuteStart;
    this.consumer = identity ? identity.consumer : ANONYMOUS_CONSUMER;
    this.consumerKind = identity ? identity.consumerKind : "anonymous";
    this.pendingHash = identity ? identity.pendingHash : false;
    this.stripeCustomerId = identity?.stripeCustomerId;
    this.reservation = reservation;
    this.warn = warn;
  }

  // Meter custom units: a non-negative integer, at most 10,000,000 per call; the last call wins.
  // Invalid values are ignored with a warning; never throws into your code
  track(units: number) {
    if (!Number.isInteger(units) || units < 0 || units > MAX_TRACK_UNITS) {
      this.warn(`UsageRail: track() needs an integer from 0 to ${MAX_TRACK_UNITS}, got ${String(units)}; ignored`);
      return;
    }
    this.trackedUnits = units;
  }
}

class UsageRailEngine<Context> implements Engine<Context> {
  readonly instanceId: string;
  private readonly options: EngineOptions<Context>;
  private readonly clock: Clock;
  private readonly transport: Transport;
  private readonly warn: (message: string) => void;
  private readonly keyStore: KeyStore | null;
  private readonly rateLimiter: RateLimiter;
  private readonly quota: QuotaManager;
  private readonly meter: Meter;
  private readonly snapshotStore: SnapshotStore | null;
  private settings: AppSettings | null = null;
  private routes = new RouteTable([]);
  private hashKnown = false;
  private hashKey: string | null = null;
  private readonly hashCache = new Map<string, string>();
  // Keyed by consumer ID with our keys, and by external_id with your own auth (the SDK only knows external IDs)
  private readonly consumerRecords = new Map<string, { record: ConsumerRecord; generation: number }>();
  private consumerGeneration = 0;
  private consumerSweepPending = false;
  private consumerCapacityWarned = false;
  // Identical rate limit configurations (usually from the same plan) share one object. The rate limiter only reads it
  private readonly sharedRateLimits = new Map<string, ConsumerRecord["rate_limits"]>();
  private activeInstances = 1;
  private cursor: string | null = null;
  private syncing: Promise<void> | null = null;
  private syncPaused = false;
  // Monotonic time of the last successful sync (serverless uses it to decide whether to sync first)
  private lastSyncedAt = -Infinity;
  // Monotonic time of the last sync attempt, failed or not; serverless uses it to space out retries
  private lastSyncAttemptAt = -Infinity;
  // Serverless submissions: the one in flight, and the next one queued behind it but not started (it builds its batch when it starts, so requests ending before then ride along)
  private submitting: Promise<void> | null = null;
  private nextSubmit: Promise<void> | null = null;
  private nextTicket: object | null = null;
  private lastSubmitAt = -Infinity;
  private firstSyncGate: Promise<void> | null = null;
  private cancelSyncTimer: (() => void) | null = null;
  private lastPersistedAt = -Infinity;
  private persisting = false;
  private closing: Promise<void> | null = null;
  private closed = false;

  constructor(options: EngineOptions<Context>) {
    if (typeof options.appKey !== "string" || options.appKey.length === 0) {
      throw new Error("UsageRail: appKey is required (USAGERAIL_APP_KEY from the console)");
    }
    if (options.auth !== "api-key" && options.auth !== "custom") throw new Error('UsageRail: auth must be "api-key" or "custom"');
    if (options.auth === "custom" && typeof options.consumer !== "function") {
      throw new Error('UsageRail: auth "custom" needs a consumer(request) function');
    }
    this.options = options;
    this.clock = options.clock ?? (options.serverless ? waitingClock : systemClock);
    // Web Crypto's randomUUID: available on Node, Workers and Vercel
    this.instanceId = options.instanceId ?? globalThis.crypto.randomUUID();
    this.transport =
      options.transport ??
      createFetchTransport({ appKey: options.appKey, instanceId: this.instanceId, baseUrl: options.baseUrl, instanceKind: options.serverless ? "serverless" : undefined });
    this.warn = rateLimitedWarn(options.logger ?? console, this.clock);
    this.keyStore = options.auth === "api-key" ? new KeyStore({ clock: this.clock, transport: this.transport, warn: this.warn }) : null;
    // A snapshot only makes sense for apps using our keys
    this.snapshotStore = options.auth === "api-key" ? (options.snapshotStore ?? null) : null;
    this.rateLimiter = new RateLimiter(this.clock);
    this.quota = new QuotaManager({
      clock: this.clock,
      transport: this.transport,
      warn: this.warn,
      onUnavailable: () => this.options.onQuotaUnavailable ?? this.settings?.on_quota_unavailable ?? "allow",
      background: !options.serverless,
    });
    this.meter = new Meter({
      clock: this.clock,
      transport: this.transport,
      instanceId: this.instanceId,
      warn: this.warn,
      leases: this.quota,
      resolvePending: () => (this.hashKnown ? (rawId: string) => this.hashConsumer(rawId) : null),
      // On serverless, failed submissions stay in memory and are retried when the next request ends: no backoff (a Retry-After from the server is still honored),
      // and no retry timers of our own; 0.5 seconds between submissions
      maxBackoffMs: options.serverless ? 0 : undefined,
      retryTimers: !options.serverless,
      minSubmitGapMs: options.serverless ? SERVERLESS_SUBMIT_GAP_MS : 0,
    });
    this.loadSnapshot();
    if (options.backgroundLoops !== false && !options.serverless) this.startBackground();
  }

  // ---- Request start -----------------------------------------------------------------

  begin(request: BeginRequest<Context>): Decision | Promise<Decision> {
    const startedAt = this.clock.now();
    let identity: Identity | Promise<Identity>;
    try {
      identity = this.identify(request);
    } catch (error) {
      identity = this.identifyFailed(error);
    }
    if (identity instanceof Promise) {
      return identity
        .catch((error: unknown) => this.identifyFailed(error))
        .then((resolved) => this.decide(request, resolved, startedAt));
    }
    return this.decide(request, identity, startedAt);
  }

  private identify(request: BeginRequest<Context>): Identity | Promise<Identity> {
    if (this.options.serverless) this.syncIfStale();
    const gate = this.firstSyncGate;
    if (gate) return gate.then(() => this.identifyNow(request));
    return this.identifyNow(request);
  }

  private identifyNow(request: BeginRequest<Context>): Identity | Promise<Identity> {
    if (this.keyStore) {
      const staleKeySnapshot = this.options.staleKeySnapshot ?? this.settings?.stale_key_snapshot ?? "allow";
      const check = this.keyStore.check(request.apiKey, staleKeySnapshot);
      return check instanceof Promise ? check.then((result) => this.identityFromKey(result)) : this.identityFromKey(check);
    }
    return this.identityFromCustom(request);
  }

  // On an internal error: with our keys, treat it as "can't verify" (onKeyStoreUnavailable); with your own auth, let it through as anonymous
  private identifyFailed(error: unknown): Identity {
    this.warn(`UsageRail: ${describeError(error)}`);
    return this.keyStore ? UNAVAILABLE : ANONYMOUS;
  }

  private identityFromKey(check: KeyCheck): Identity {
    if (check.result === "invalid") return { kind: "invalid", consumer: check.consumer };
    if (check.result === "unavailable") return UNAVAILABLE;
    const record = check.consumerRecord ?? this.consumerRecords.get(check.consumer)?.record ?? null;
    // Both the key record and the consumer record carry a quota version; use the newer
    const quota: QuotaInfo =
      record && record.quota_version >= check.quotaVersion
        ? { behavior: record.quota_behavior, version: record.quota_version }
        : { behavior: check.quotaBehavior, version: check.quotaVersion };
    return {
      kind: "consumer",
      consumer: check.consumer,
      consumerKind: "id",
      pendingHash: false,
      keyId: check.keyId,
      stripeCustomerId: undefined,
      quota,
      rateLimits: record?.rate_limits ?? null,
      suspended: record?.status === "suspended",
    };
  }

  private identityFromCustom(request: BeginRequest<Context>): Identity {
    let identity: ConsumerIdentity | null | undefined;
    try {
      identity = this.options.consumer?.(request.context as Context);
    } catch (error) {
      this.warn(`UsageRail: consumer() threw (${describeError(error)}); the request was counted as anonymous`);
      return ANONYMOUS;
    }
    if (identity == null) return ANONYMOUS;
    const rawId = identity.id;
    if (typeof rawId !== "string" || rawId.length === 0 || rawId.length > MAX_EXTERNAL_ID_LENGTH) {
      this.warn(`UsageRail: consumer() must return an id of 1-${MAX_EXTERNAL_ID_LENGTH} characters; the request was counted as anonymous`);
      return ANONYMOUS;
    }
    const stripeCustomerId = typeof identity.stripeCustomerId === "string" && identity.stripeCustomerId ? identity.stripeCustomerId : undefined;
    if (!this.hashKnown) {
      // Don't know yet whether to hash or with what key: count by raw ID in memory for now and convert once the settings arrive
      return {
        kind: "consumer",
        consumer: rawId,
        consumerKind: "external",
        pendingHash: true,
        keyId: null,
        stripeCustomerId,
        quota: { behavior: undefined, version: 0 },
        rateLimits: null,
        suspended: false,
      };
    }
    const consumer = this.hashConsumer(rawId);
    const record = this.consumerRecords.get(consumer)?.record;
    return {
      kind: "consumer",
      consumer,
      consumerKind: "external",
      pendingHash: false,
      keyId: null,
      stripeCustomerId,
      quota: record ? { behavior: record.quota_behavior, version: record.quota_version } : { behavior: undefined, version: 0 },
      rateLimits: record?.rate_limits ?? null,
      suspended: record?.status === "suspended",
    };
  }

  // External IDs are HMAC-SHA256'd with the app's hashing key, UTF-8, lowercase hex; reported as is when the seller turns hashing off
  private hashConsumer(rawId: string) {
    if (this.hashKey === null) return rawId;
    const cached = this.hashCache.get(rawId);
    if (cached !== undefined) return cached;
    const hashed = createHmac("sha256", this.hashKey).update(rawId, "utf8").digest("hex");
    if (this.hashCache.size >= MAX_HASH_CACHE) this.hashCache.clear();
    this.hashCache.set(rawId, hashed);
    return hashed;
  }

  private decide(request: BeginRequest<Context>, identity: Identity, startedAt: number): Decision | Promise<Decision> {
    const start = this.requestStart(request, startedAt);
    switch (identity.kind) {
      case "invalid":
        this.recordRejection(start, identity.consumer, identity.consumer === null ? null : "id", "sdk_401");
        return INVALID_API_KEY;
      case "unavailable":
        if ((this.options.onKeyStoreUnavailable ?? this.settings?.on_key_store_unavailable ?? "deny") === "allow") {
          return this.admit(start, null, null);
        }
        // There's no outcome for the SDK's own 503, so record the status it actually returns
        this.recordRejection(start, null, null, "5xx");
        return AUTH_UNAVAILABLE;
      case "anonymous":
        return this.admit(start, null, null);
      case "consumer":
        return this.decideForConsumer(start, identity);
    }
  }

  private requestStart(request: BeginRequest<Context>, startedAt: number): RequestStart {
    const method = request.method.toUpperCase();
    // When the framework provides the template at the start (Fastify, Next.js), reserve by it: 1 if the seller didn't configure it, matching settlement. Otherwise (Express and Hono
    // middleware run before routing) match the path against the seller's templates. Several may match and which one handles it is only known afterwards, so reserve for the largest
    // (better too much, refunded at the end). Settlement only trusts the template the framework reports at the end (actualUnits); this guess only sets the reservation
    const ignoreCase = request.ignoreCase ?? false;
    const exact = request.route ? this.routes.byTemplate(method, request.route, ignoreCase) : undefined;
    const candidates = request.route ? (exact ? [exact] : []) : this.routes.candidatesByPath(method, request.path, ignoreCase);
    const reserve = candidates.reduce((largest, candidate) => Math.max(largest, this.unitsToReserve(candidate)), 1);
    return { method, route: request.route, guessedRoute: candidates[0]?.route, ignoreCase, reserve, startedAt, minuteStart: startedAt - (startedAt % 60_000) };
  }

  private decideForConsumer(start: RequestStart, identity: KnownConsumer): Decision | Promise<Decision> {
    if (identity.keyId) this.meter.recordKeyUse(identity.keyId);
    // Consumers the seller suspended or is deleting: block everything (protocol.ts ConsumerRecord.status)
    if (identity.suspended) {
      this.recordRejection(start, identity.consumer, identity.consumerKind, "sdk_403", identity);
      return CONSUMER_SUSPENDED;
    }
    if (!identity.pendingHash) {
      let rate: RateDecision = { allowed: true };
      try {
        rate = this.rateLimiter.check(identity.consumer, identity.rateLimits, this.settings?.rate_limit_mode ?? "per_instance", this.activeInstances);
      } catch (error) {
        this.warn(`UsageRail: rate limiter failed (${describeError(error)})`);
      }
      if (!rate.allowed) {
        // Requests blocked by the rate limit aren't billed; recorded as sdk_429
        this.recordRejection(start, identity.consumer, identity.consumerKind, "sdk_429", identity);
        return rejection(429, "rate_limited", rate.retryAfterSeconds, rate.headers);
      }
    }
    let quota: QuotaDecision | Promise<QuotaDecision>;
    try {
      // The raw external ID can't be converted to a consumer ID yet, so no lease is possible: treat the control plane as unavailable
      quota = identity.pendingHash
        ? this.quota.whenUnavailable()
        : this.quota.reserve(identity.consumer, identity.consumerKind, identity.quota, start.reserve, start.startedAt);
    } catch (error) {
      this.warn(`UsageRail: quota check failed (${describeError(error)})`);
      quota = this.quota.whenUnavailable();
    }
    if (quota instanceof Promise) {
      return quota
        .catch((error: unknown) => {
          this.warn(`UsageRail: quota check failed (${describeError(error)})`);
          return this.quota.whenUnavailable();
        })
        .then((decision) => this.afterQuota(start, identity, decision));
    }
    return this.afterQuota(start, identity, quota);
  }

  private afterQuota(start: RequestStart, identity: KnownConsumer, decision: QuotaDecision): Decision {
    if (decision.allowed) return this.admit(start, identity, decision.reservation);
    switch (decision.error) {
      case "quota_exceeded":
        this.recordRejection(start, identity.consumer, identity.consumerKind, "sdk_429", identity);
        return rejection(429, "quota_exceeded", decision.retryAfterSeconds);
      case "insufficient_credits":
        this.recordRejection(start, identity.consumer, identity.consumerKind, "sdk_402", identity);
        return INSUFFICIENT_CREDITS;
      case "consumer_suspended":
        this.recordRejection(start, identity.consumer, identity.consumerKind, "sdk_403", identity);
        return CONSUMER_SUSPENDED;
      case "quota_unavailable":
        this.recordRejection(start, identity.consumer, identity.consumerKind, "5xx", identity);
        return QUOTA_UNAVAILABLE;
    }
  }

  // How much one request reserves from the lease: the route weight when metering requests. When metering units we don't know whether the handler will call track(),
  // so take the larger of the weight and the per-request reservation. Better to reserve too much (refunded at the end) than admit too much
  private unitsToReserve(setting: RouteSetting | undefined) {
    if (!setting) return 1;
    return this.settings?.unit_mode === "requests" ? setting.weight : Math.max(setting.weight, setting.reserve);
  }

  private admit(start: RequestStart, identity: KnownConsumer | null, reservation: Reservation | null): Admission {
    return { allowed: true, handle: new ActiveRequest(start, identity, reservation, this.warn) };
  }

  private recordRejection(start: RequestStart, consumer: string | null, kind: ConsumerKind | null, outcome: Outcome, identity?: KnownConsumer) {
    try {
      this.meter.record(
        {
          consumer: consumer ?? ANONYMOUS_CONSUMER,
          kind: consumer === null || kind === null ? "anonymous" : kind,
          stripeCustomerId: identity?.stripeCustomerId,
          method: start.method,
          route: start.route ?? start.guessedRoute ?? UNMATCHED_ROUTE,
          outcome,
          minuteStart: start.minuteStart,
          units: 0,
        },
        identity?.pendingHash ?? false,
      );
    } catch (error) {
      this.warn(`UsageRail: ${describeError(error)}`);
    }
  }

  // ---- Request end -------------------------------------------------------------------

  streamStarted(handle: RequestHandle, route?: string) {
    if (!(handle instanceof ActiveRequest) || handle.ended || handle.streamOpen) return;
    const routeName = this.routeName(handle, route);
    // For apps metering requests, units are just the route weight, so it can be settled now
    if (this.settings?.unit_mode === "requests") {
      this.end(handle, { outcome: "ws_open", route: routeName });
      return;
    }
    handle.streamOpen = true;
    try {
      this.meter.record(this.usageOf(handle, routeName, "ws_open", 0), handle.pendingHash);
    } catch (error) {
      this.warn(`UsageRail: ${describeError(error)}`);
    }
  }

  end(handle: RequestHandle, result: EndResult) {
    if (!(handle instanceof ActiveRequest) || handle.ended) return;
    handle.ended = true;
    if (handle.streamOpen) {
      this.endStream(handle, result.route);
      return;
    }
    try {
      const outcome = "outcome" in result ? result.outcome : classifyStatus(result.status);
      const route = this.routeName(handle, result.route);
      // Only billable outcomes count units; 4xx, 5xx and aborted refund the whole reservation
      const units = BILLABLE_OUTCOMES.has(outcome) ? this.actualUnits(handle, route) : 0;
      if (handle.reservation) this.quota.settle(handle.reservation, units);
      this.meter.record(
        {
          consumer: handle.consumer,
          kind: handle.consumerKind,
          stripeCustomerId: handle.stripeCustomerId,
          method: handle.method,
          route,
          outcome,
          minuteStart: handle.minuteStart,
          units,
        },
        handle.pendingHash,
      );
    } catch (error) {
      this.warn(`UsageRail: ${describeError(error)}`);
    }
  }

  // SSE connection ended: settle the reservation by track() (or the route weight if it wasn't called) and add the units. The connection was already counted once at the start, so the count here is 0
  private endStream(handle: ActiveRequest, route: string | undefined) {
    try {
      const routeName = this.routeName(handle, route);
      const units = this.actualUnits(handle, routeName);
      if (handle.reservation) this.quota.settle(handle.reservation, units);
      // Units go in the minute the stream started, the same period as the lease that settles them. Putting them in the ending minute would let a connection spanning periods
      // move usage settled against last period's lease into the next period, bypassing its quota. If the starting minute is over 23 hours ago (the server only accepts the last 24 hours), use the
      // earliest minute still accepted. If the period changed by then (open for over a day and across a period boundary), those units still land in the new period; connections that long aren't billed exactly
      const oldest = this.clock.now() - MAX_ROW_AGE_MS;
      const minuteStart = Math.max(handle.minuteStart, oldest - (oldest % 60_000) + 60_000);
      if (units > 0) this.meter.record({ ...this.usageOf(handle, routeName, "ws_open", units), minuteStart }, handle.pendingHash, 0);
    } catch (error) {
      this.warn(`UsageRail: ${describeError(error)}`);
    }
  }

  private usageOf(handle: ActiveRequest, route: string, outcome: Outcome, units: number) {
    return {
      consumer: handle.consumer,
      kind: handle.consumerKind,
      stripeCustomerId: handle.stripeCustomerId,
      method: handle.method,
      route,
      outcome,
      minuteStart: handle.minuteStart,
      units,
    };
  }

  // Which route to record: the template the framework gave at the end, else the one it gave at the start, else _unmatched. The path-based guess from the start
  // isn't used: if, say, the handler never ran and a catch-all middleware answered 200, settling by the guessed template would bill for a route that didn't run.
  // When the framework routes case-insensitively (Express's default) and the seller configured this route, record the seller's spelling, so one route isn't split into several rows by client casing
  private routeName(handle: ActiveRequest, route: string | undefined) {
    const name = route ?? handle.routeAtStart ?? UNMATCHED_ROUTE;
    return handle.ignoreCase ? (this.routes.byTemplate(handle.method, name, true)?.route ?? name) : name;
  }

  // track() wins if it was called; otherwise the route weight; otherwise 1. track() doesn't count when the app meters requests.
  // Weights only come from the template the framework reports; unconfigured routes count as 1, never settled by the path-based guess from the start
  private actualUnits(handle: ActiveRequest, route: string) {
    if (handle.trackedUnits !== null && this.settings?.unit_mode !== "requests") return handle.trackedUnits;
    return this.routes.byTemplate(handle.method, route, handle.ignoreCase)?.weight ?? 1;
  }

  // ---- Sync --------------------------------------------------------------------------

  sync(): Promise<void> {
    if (!this.syncing) {
      this.syncing = this.runSync().finally(() => {
        this.syncing = null;
      });
    }
    return this.syncing;
  }

  private async runSync() {
    this.lastSyncAttemptAt = this.clock.monotonic();
    try {
      for (let page = 0; page < MAX_SYNC_PAGES; page++) {
        const response = await this.transport.sync(this.cursor);
        this.applySyncPage(response);
        // Paged: when has_more is true, fetch again right away with the new cursor (protocol.ts)
        if (!response.has_more) {
          this.completeSync();
          this.syncPaused = false;
          this.lastSyncedAt = this.clock.monotonic();
          return;
        }
      }
      this.warn("UsageRail: sync kept returning more pages; continuing on the next sync");
    } catch (error) {
      this.syncPaused = error instanceof TransportError && isPausingStatus(error.status);
      this.warn(`UsageRail: sync failed (${describeError(error)})`);
    }
  }

  private applySyncPage(response: SyncResponse) {
    this.cursor = response.cursor;
    this.applySettings(response.app);
    this.keyStore?.applySyncPage(response);
    // Full sync: replace consumer records entirely (protocol.ts). When paged, start on the first page and delete absent records only when the last page arrives
    if (response.full && !this.consumerSweepPending) {
      this.consumerGeneration += 1;
      this.consumerSweepPending = true;
    }
    for (const record of response.consumers) this.storeConsumer(record);
    if (Number.isFinite(response.active_instances)) this.activeInstances = Math.max(1, response.active_instances);
    this.checkClockSkew(response.server_time);
  }

  private completeSync() {
    this.keyStore?.completeSync();
    if (this.consumerSweepPending) {
      for (const [key, entry] of this.consumerRecords) {
        if (entry.generation >= this.consumerGeneration) continue;
        this.consumerRecords.delete(key);
        // Gone from the full sync (e.g. deleted): cached local results like "unlimited" and "suspended" are dropped too, and the next request asks the server again.
        // Otherwise a suspended consumer with your own auth, blocked only by its synced record, would be let through again once the record disappears
        this.quota.forget(key);
      }
      this.consumerSweepPending = false;
    }
    this.persistSnapshot();
  }

  private applySettings(app: AppSettings) {
    if (this.hashKey !== app.hash_key) this.hashCache.clear();
    this.settings = app;
    this.hashKey = app.hash_key;
    this.hashKnown = true;
    this.routes = new RouteTable(app.routes ?? []);
  }

  private storeConsumer(record: ConsumerRecord) {
    const key = this.options.auth === "custom" ? record.external_id : record.consumer;
    if (key === null || key === undefined) return;
    // The cap only applies to active records: suspended ones are always stored, since they're what needs blocking (dropping them would let suspended consumers through)
    if (!this.consumerRecords.has(key) && this.consumerRecords.size >= MAX_CONSUMER_RECORDS && record.status === "active") {
      if (!this.consumerCapacityWarned) this.warn(`UsageRail: more than ${MAX_CONSUMER_RECORDS} consumers synced; the rest have no local limits`);
      this.consumerCapacityWarned = true;
      // The record can't be stored, but it changed: existing quota state still follows (a new quota version retires old leases and drops "unlimited")
      this.quota.observeRecord(key, record.quota_version, record.quota_behavior, record.status);
      return;
    }
    this.consumerRecords.set(key, { record: this.compact(record), generation: this.consumerGeneration });
    this.quota.observeRecord(key, record.quota_version, record.quota_behavior, record.status);
  }

  // Store it compactly to keep memory bounded: with our keys the external ID is never used (lookups are by consumer ID), so it's not kept; a hashed external ID is
  // 64 hex characters, 10 MB for 100,000 records. Identical rate limit configurations share one object
  private compact(record: ConsumerRecord): ConsumerRecord {
    const perSecond = record.rate_limits?.per_second ?? null;
    const perMinute = record.rate_limits?.per_minute ?? null;
    const shape = `${perSecond}/${perMinute}`;
    let limits = this.sharedRateLimits.get(shape);
    if (!limits) {
      limits = { per_second: perSecond, per_minute: perMinute };
      if (this.sharedRateLimits.size < MAX_SHARED_RATE_LIMITS) this.sharedRateLimits.set(shape, limits);
    }
    return { ...record, external_id: this.options.auth === "custom" ? record.external_id : null, rate_limits: limits };
  }

  // Minute buckets and lease periods use this machine's clock; if it's too far off, the server rejects usage rows and lease requests
  private checkClockSkew(serverTime: string) {
    const server = Date.parse(serverTime);
    if (!Number.isFinite(server)) return;
    const skew = this.clock.now() - server;
    if (Math.abs(skew) > CLOCK_SKEW_WARNING_MS) {
      this.warn(`UsageRail: this machine's clock is ${Math.round(skew / 1000)} s off the server's; usage minutes depend on it, check NTP`);
    }
  }

  // ---- Snapshot on disk ----------------------------------------------------------------

  private loadSnapshot() {
    if (!this.snapshotStore || !this.keyStore) return;
    let snapshot: SyncSnapshot | null = null;
    try {
      snapshot = this.snapshotStore.load();
    } catch (error) {
      this.warn(`UsageRail: could not read the key snapshot (${describeError(error)})`);
    }
    if (!snapshot) return;
    const savedAt = Date.parse(snapshot.saved_at);
    // A snapshot older than 24 hours counts as none
    if (!Number.isFinite(savedAt) || this.clock.now() - savedAt > SNAPSHOT_MAX_AGE_MS) return;
    this.keyStore.load(snapshot.keys, savedAt);
    for (const record of snapshot.consumers) this.storeConsumer(record);
    if (snapshot.app) {
      // The hashing key isn't saved to disk, so hashKnown stays false until the first sync
      this.settings = { ...snapshot.app, hash_key: null };
      this.routes = new RouteTable(snapshot.app.routes ?? []);
    }
    this.cursor = snapshot.cursor;
  }

  private persistSnapshot() {
    if (!this.snapshotStore || !this.keyStore || this.persisting) return;
    const now = this.clock.monotonic();
    if (now - this.lastPersistedAt < PERSIST_INTERVAL_MS) return;
    this.lastPersistedAt = now;
    this.persisting = true;
    const snapshot: SyncSnapshot = {
      saved_at: new Date(this.clock.now()).toISOString(),
      cursor: this.cursor,
      app: withoutHashKey(this.settings),
      keys: this.keyStore.records(),
      consumers: [...this.consumerRecords.values()].map((entry) => entry.record),
    };
    this.snapshotStore
      .save(snapshot)
      .catch((error: unknown) => this.warn(`UsageRail: could not save the key snapshot (${describeError(error)})`))
      .finally(() => {
        this.persisting = false;
      });
  }

  // ---- Background loops and exit -------------------------------------------------------

  private startBackground() {
    const firstSync = this.sync();
    const needsFirstSync = this.keyStore ? this.keyStore.freshness() === "none" : !this.hashKnown;
    if (needsFirstSync) {
      this.firstSyncGate = new Promise<void>((resolve) => {
        const cancel = this.clock.setTimer(resolve, FIRST_SYNC_WAIT_MS);
        void firstSync.then(() => {
          cancel();
          resolve();
        });
      }).then(() => {
        this.firstSyncGate = null;
      });
    }
    const scheduleNext = () => {
      if (this.closed) return;
      this.cancelSyncTimer = this.clock.setTimer(() => void this.sync().then(scheduleNext), this.syncPaused ? PAUSED_SYNC_MS : SYNC_INTERVAL_MS);
    };
    void firstSync.then(scheduleNext);
    this.meter.start();
  }

  // ---- Serverless ------------------------------------------------------------------

  // Cold start, or a snapshot older than 20 seconds: this request waits for a sync, up to 1 second, then falls back to remote verification
  private syncIfStale() {
    if (this.firstSyncGate || this.clock.monotonic() - this.lastSyncedAt <= SERVERLESS_STALE_MS) return;
    // One is already in flight (started when the previous request ended): wait for it regardless of the retry cooldown, which only decides whether to start a new one.
    // Without waiting, a key this sync is about to revoke would still be let through on the old snapshot
    const syncing = this.syncing ?? (this.syncRetryAllowed() ? this.sync() : null);
    if (!syncing) return;
    this.firstSyncGate = new Promise<void>((resolve) => {
      const cancel = this.clock.setTimer(resolve, FIRST_SYNC_WAIT_MS);
      void syncing.then(() => {
        cancel();
        resolve();
      });
    }).then(() => {
      this.firstSyncGate = null;
    });
  }

  // Whether enough time has passed since a failed sync to try again (the server allows 6 syncs per instance per minute)
  private syncRetryAllowed() {
    return this.clock.monotonic() - this.lastSyncAttemptAt >= (this.syncPaused ? PAUSED_SYNC_MS : SERVERLESS_SYNC_RETRY_MS);
  }

  afterResponse(): Promise<void> {
    // Sync every 15 seconds: revoked keys, rate limit and quota changes. If one is in flight, wait for it
    const syncing = this.syncing ?? (this.clock.monotonic() - this.lastSyncedAt >= SYNC_INTERVAL_MS && this.syncRetryAllowed() ? this.sync() : null);
    // With a sync in flight (especially the cold start one), wait for it before submitting: external IDs with your own auth can't be converted until the sync brings the hashing key,
    // and submitting first would fail this batch with no later afterResponse to submit it
    const submit = syncing ? syncing.then(() => this.submitSoon()) : this.submitSoon();
    // Renewals also wait for the sync: a sync that finds a new quota version retires old leases and queues renewals; renewing first would make the next request get them synchronously
    const renew = syncing ? syncing.then(() => this.quota.renewNow()) : this.quota.renewNow();
    const tasks: Promise<unknown>[] = [renew, submit];
    return Promise.all(tasks).then(
      () => undefined,
      (error: unknown) => this.warn(`UsageRail: ${describeError(error)}`),
    );
  }

  // At least 0.5 seconds between submissions. If one is queued but not started, ride along with it (it builds its batch when it starts).
  // If one is in flight, queue behind it and submit once it finishes and the gap has passed. Failures stay in memory for the next request to retry
  private submitSoon(): Promise<void> {
    if (this.nextSubmit) return this.nextSubmit;
    const ticket = {};
    this.nextTicket = ticket;
    const run = async () => {
      if (this.submitting) await this.submitting;
      const wait = this.lastSubmitAt + SERVERLESS_SUBMIT_GAP_MS - this.clock.monotonic();
      if (wait > 0) await new Promise<void>((resolve) => this.clock.setTimer(resolve, wait));
      // Requests ending from this point on wait for the next one
      if (this.nextTicket === ticket) {
        this.nextTicket = null;
        this.nextSubmit = null;
      }
      this.lastSubmitAt = this.clock.monotonic();
      this.submitting = this.meter.flush().finally(() => {
        this.submitting = null;
      });
      await this.submitting;
    };
    const next = run();
    // If nothing was in flight and the gap had passed, run() started submitting immediately: it's no longer "the queued next one", so requests ending later can't ride along
    if (this.nextTicket === ticket) this.nextSubmit = next;
    return next;
  }

  flush(): Promise<void> {
    return this.meter.flush();
  }

  drain() {
    this.meter.drain();
  }

  // Before exit: stop background loops, retire all leases, and try to report remaining usage and "lease closed", waiting up to 5 seconds
  close(): Promise<void> {
    if (!this.closing) {
      this.closed = true;
      this.cancelSyncTimer?.();
      this.quota.retireAll();
      this.quota.stop();
      this.closing = new Promise<void>((resolve) => {
        const cancel = this.clock.setTimer(resolve, CLOSE_TIMEOUT_MS);
        this.meter
          .close()
          .catch((error: unknown) => this.warn(`UsageRail: final report failed (${describeError(error)})`))
          .finally(() => {
            cancel();
            resolve();
          });
      });
    }
    return this.closing;
  }
}

function withoutHashKey(settings: AppSettings | null): Omit<AppSettings, "hash_key"> | null {
  if (!settings) return null;
  const copy: Partial<AppSettings> = { ...settings };
  delete copy.hash_key;
  return copy as Omit<AppSettings, "hash_key">;
}

// At most one warning a minute: when something breaks it tends to fire on every request, and mustn't flood the logs
function rateLimitedWarn(logger: { warn(message: string): void }, clock: Clock) {
  let lastAt = -Infinity;
  let suppressed = 0;
  return (message: string) => {
    const now = clock.monotonic();
    if (now - lastAt < WARNING_INTERVAL_MS) {
      suppressed += 1;
      return;
    }
    lastAt = now;
    try {
      logger.warn(suppressed > 0 ? `${message} (${suppressed} more warnings in the last minute were dropped)` : message);
    } catch {
      // Even a failing logger mustn't affect requests
    }
    suppressed = 0;
  };
}
