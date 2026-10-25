// The wire protocol between the SDK and the server.
// The SDK and the UsageRail server both import these types, so there is only one definition. Fields on the wire are snake_case.
// Every endpoint: Authorization: Bearer <appKey>, plus X-Instance-Id: <instance ID>.

// ---- Outcomes ----------------------------------------------------------------
// Only 2xx, 3xx, ws_open and check_allowed are billable; the rest go to analytics only. sdk_403 is a suspended consumer
export const OUTCOMES = ["2xx", "3xx", "4xx", "5xx", "aborted", "ws_open", "sdk_401", "sdk_403", "sdk_429", "sdk_402", "check_allowed"] as const;
export type Outcome = (typeof OUTCOMES)[number];
export const BILLABLE_OUTCOMES: ReadonlySet<Outcome> = new Set(["2xx", "3xx", "ws_open", "check_allowed"]);

export type QuotaBehavior = "overage" | "stop" | "prepaid";

// What a consumer reference is: a consumer ID when you use our keys, an external ID when you use your own auth
export type ConsumerKind = "id" | "external";

// ---- POST /v1/ingest ---------------------------------------------------------
// Per-row caps: a row over either is rejected whole. The SDK starts a new row before reaching them; the server merges rows with the same key on insert
export const MAX_ROW_COUNT = 10_000_000;
export const MAX_ROW_UNITS = 10_000_000_000;

export type IngestRow = {
  // Consumer reference: a consumer ID (cons_...) with our keys; an external ID with your own auth (HMAC'd hex by default); an empty string when anonymous
  consumer: string;
  // Which kind consumer is. Missing means "id": if the SDK forgets it, the lookup fails and the row is rejected (undercounting), never charged to the wrong customer.
  // "anonymous": anonymous requests (consumer() returned null with your own auth, unknown keys) go to analytics only, never to quotas or invoices
  consumer_kind?: ConsumerKind | "anonymous";
  stripe_customer_id?: string;
  method: string;
  // Route template, e.g. /v1/users/:id. Past 500 distinct routes the rest are folded into _other; unmatched requests are recorded as _unmatched
  route: string;
  result_type: Outcome;
  // Start of the minute, UTC, ISO 8601 (seconds are 0)
  minute: string;
  // Request count. 0 on rows that only add units (an SSE stream counted 1 when it started and adds its track() units when it ends); units must then be > 0
  count: number;
  units: number;
};

export type IngestRequest = {
  // Instance ID + increasing sequence number. Unchanged on retry; each sub-batch gets a new number when a batch is split
  batch_id: string;
  sdk_version: string;
  instance_id: string;
  rows: IngestRow[];
  // Known losses: billable usage dropped (e.g. retry queue overflow), per consumer for up to 1,000 consumers, the rest folded into unattributed
  dropped: { consumer: string; billable_requests: number; units: number }[];
  unattributed_dropped: { billable_requests: number; units: number };
  // How many times each key was used in this batch; only used to update "last used"
  key_usage: { key_id: string; count: number }[];
  // Usage added to each lease in this batch (in metering units). closed means the lease is retired and every request it admitted has been reported
  lease_used: { lease_id: string; used: number; closed?: boolean }[];
};

export type IngestResponse = {
  duplicate: boolean;
  accepted_rows: number;
  rejected_rows: { index: number; reason: string }[];
  unknown_keys: string[];
  unknown_leases: string[];
};

// ---- GET /v1/sync?cursor= ----------------------------------------------------
export type KeyRecord = {
  key_id: string;
  // Hex SHA-256 of the full key; the SDK compares it in constant time
  hash: string;
  consumer: string;
  status: "active" | "revoked";
  // ISO 8601; the server already subtracts a 30-second margin
  expires_at: string | null;
  quota_behavior: QuotaBehavior | null;
  quota_version: number;
};

export type ConsumerRecord = {
  consumer: string;
  // With your own auth, the external ID the SDK reported (HMAC'd hex if hashing is on); null with our keys.
  // An SDK using your own auth only knows external IDs, so this is how synced rate limits, quota versions and status match up with locally cached consumers
  external_id: string | null;
  // suspended: the seller suspended the consumer, or it's being deleted. The SDK blocks all its requests with 403 consumer_suspended
  status: "active" | "suspended";
  quota_behavior: QuotaBehavior | null;
  quota_version: number;
  rate_limits: { per_second: number | null; per_minute: number | null };
};

export type RouteSetting = { method: string; route: string; weight: number; reserve: number };

export type AppSettings = {
  app_id: string;
  mode: "test" | "live";
  integration: "sdk" | "http";
  // With your own auth, external IDs are HMAC-SHA256'd with this key by default; null when hashing is off
  hash_key: string | null;
  rate_limit_mode: "per_instance" | "divided";
  on_key_store_unavailable: "deny" | "allow";
  stale_key_snapshot: "allow" | "deny";
  on_quota_unavailable: "allow" | "deny";
  unit_mode: "requests" | "units";
  routes: RouteSetting[];
};

export type SyncResponse = {
  // Opaque cursor, sent back unchanged next time. Without one, or when it has expired, the server returns everything (full = true)
  cursor: string;
  full: boolean;
  // Pages are capped at 5 MB: when has_more is true, fetch again right away with the new cursor
  has_more: boolean;
  app: AppSettings;
  keys: KeyRecord[];
  removed_keys: string[];
  // A full sync includes suspended consumers (status = suspended) but not deleted ones; the SDK replaces its whole copy on a full sync
  consumers: ConsumerRecord[];
  // Active instance count, for the "divided" rate limit mode
  active_instances: number;
  server_time: string;
};

// ---- POST /v1/keys/verify ----------------------------------------------------
// A key that isn't in the snapshot (perhaps just created). Only the key ID and hash are sent, never the key itself
export type VerifyKeyRequest = { key_id: string; hash: string };
export type VerifyKeyResponse =
  | { valid: true; key: KeyRecord; consumer: ConsumerRecord }
  | { valid: false; reason: "not_found" | "revoked" | "expired" };

// ---- POST /v1/leases ---------------------------------------------------------
// One call can request leases for several consumers (renewals within the same second are batched, up to 500)
export type LeaseRequestItem = {
  consumer: string;
  consumer_kind?: ConsumerKind;
  // Units one request on this route reserves up front
  reserve: number;
  // This consumer's usage on this instance over the last 60 seconds, used to size the lease
  recent_usage_60s: number;
  // The quota version the SDK holds; if the server's is newer, the server wins
  quota_version: number;
  // End of the minute the requesting call falls in (ISO 8601). The server returns a lease for the quota period containing it,
  // so period boundaries don't depend on the server's clock right now. Omitted means the server's current time; outside 24 hours before to 6 minutes after server time, the whole request is a 400
  at?: string;
};

export type Lease = {
  lease_id: string;
  consumer: string;
  granted: number;
  quota_version: number;
  behavior: "stop" | "prepaid";
  // End of the quota period this lease belongs to (ISO 8601)
  period_end: string;
} & QuotaFigures;

// This consumer's quota for the period and what's left right now: for hard limits, the period limit and "limit − usage recorded so far"; for prepaid, the original amount and balance of unexpired grants.
// The SDK doesn't use these; the HTTP check service uses them for limit and remaining in /v1/check responses
export type QuotaFigures = { quota_limit?: number; quota_remaining?: number };

export type LeaseResult =
  | { consumer: string; status: "granted"; lease: Lease }
  // Not limited (overage billing, or no quota): cache until the quota version changes
  | { consumer: string; status: "unlimited"; quota_version: number }
  // Less available than one reservation: 429 for hard limits, 402 for prepaid. retry_after is seconds until the period ends (hard limits)
  | ({ consumer: string; status: "exhausted"; behavior: "stop" | "prepaid"; quota_version: number; retry_after: number | null } & QuotaFigures)
  // Consumer suspended (same as ConsumerRecord.status): block every request until a sync brings status = active
  | { consumer: string; status: "suspended"; quota_version: number };

export type LeaseRequest = { requests: LeaseRequestItem[] };
export type LeaseResponse = { results: LeaseResult[] };
