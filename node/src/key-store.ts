// API key snapshot and local verification.
// The hot path is local only: format and checksum → look up the key ID in the snapshot → constant-time hash comparison → status and expiry.
// Only keys missing from the snapshot touch the network (remote verification, 300 ms at most), and those results are cached.
// Memory holds key IDs and hashes only, never the keys themselves.
import { createHash } from "node:crypto";

import type { Clock } from "./clock.ts";
import { CHECKSUM_LENGTH, checksumFor, parseApiKey } from "./key-format.ts";
import type { AppSettings, ConsumerRecord, KeyRecord, QuotaBehavior, VerifyKeyResponse } from "./protocol.ts";
import { describeError, type Transport } from "./transport.ts";

// Freshness: synced within the last 10 minutes is fresh; older than 24 hours counts as no snapshot at all
export const FRESH_FOR_MS = 10 * 60_000;
export const SNAPSHOT_MAX_AGE_MS = 24 * 60 * 60_000;
// Failed remote verifications are cached by key ID for 60 seconds; each instance makes at most 20 remote verifications a second
export const NEGATIVE_CACHE_MS = 60_000;
export const REMOTE_VERIFIES_PER_SECOND = 20;
// At most 100,000 keys in the snapshot, to keep memory bounded
export const MAX_SNAPSHOT_KEYS = 100_000;
// Cap on cached remote verification results. Failures top out around 20 a second × 60 seconds; successes are cleared on every sync,
// so they only grow while sync keeps failing. The cap stops a long outage from eating memory
const MAX_CACHED_VERIFICATIONS = 10_000;
// For a few seconds after a remote verification fails, treat new keys as "unavailable" straight away: when the server is down, requests with new keys shouldn't each wait the full 300 ms
export const VERIFY_OUTAGE_MS = 5_000;

export type Freshness = "fresh" | "stale" | "none";

export type KeyCheck =
  | {
      result: "valid";
      keyId: string;
      consumer: string;
      quotaBehavior: QuotaBehavior | null;
      quotaVersion: number;
      // Consumer details from remote verification (rate limits, quota version); null on a snapshot hit, where the engine takes them from synced data
      consumerRecord: ConsumerRecord | null;
    }
  // consumer is only set when the hash matched but the key is revoked or expired: we know who it is, so sdk_401 can be recorded against them
  | { result: "invalid"; consumer: string | null }
  | { result: "unavailable" };

// The snapshot saved to disk (long-running processes write it to the local temp directory with mode 600, read it first on restart, and apply the same 24-hour limit).
// It leaves out app.hash_key: the external ID hashing key never touches disk and is fetched again on the first sync after a restart
export type SyncSnapshot = {
  saved_at: string;
  cursor: string | null;
  app: Omit<AppSettings, "hash_key"> | null;
  keys: KeyRecord[];
  consumers: ConsumerRecord[];
};

// Runtimes without a local filesystem, like Workers, don't pass one, and nothing is saved
export type SnapshotStore = {
  load(): SyncSnapshot | null;
  save(snapshot: SyncSnapshot): Promise<void>;
};

type StoredKey = {
  hash: string;
  consumer: string;
  revoked: boolean;
  expiresAt: number | null;
  quotaBehavior: QuotaBehavior | null;
  quotaVersion: number;
  // Used when a full sync spans several pages: keys absent from the new full sync are deleted once the last page arrives
  generation: number;
};

const INVALID: KeyCheck = Object.freeze({ result: "invalid", consumer: null });
const UNAVAILABLE: KeyCheck = Object.freeze({ result: "unavailable" });

export class KeyStore {
  private readonly clock: Clock;
  private readonly transport: Transport;
  private readonly warn: (message: string) => void;
  private readonly keys = new Map<string, StoredKey>();
  private generation = 0;
  private sweepPending = false;
  // Monotonic time of the last complete successful sync; null means there's no snapshot yet
  private syncedAt: number | null = null;
  // Successful remote verifications, cached until the next sync
  private readonly verified = new Map<string, { key: StoredKey; consumerRecord: ConsumerRecord; at: number }>();
  // Number of completed syncs. If a sync completed after a remote verification was sent, its answer may be older than the snapshot (the key may have been revoked or deleted meanwhile), so it isn't cached
  private syncsCompleted = 0;
  // Failed remote verifications, cached for 60 seconds by "key ID + hash". Key IDs aren't secret; caching by ID alone would let
  // anyone pair a real key's ID with a fake secret and get a freshly created real key rejected for 60 seconds
  private readonly rejected = new Map<string, number>();
  // Concurrent requests with the same new key share one remote verification; otherwise a burst would use up the 20-a-second budget and valid keys after it would get 401s
  private readonly inflight = new Map<string, Promise<KeyCheck>>();
  private budgetSecond = -1;
  private budgetUsed = 0;
  private outageUntil = -Infinity;
  private capacityWarned = false;

  constructor(options: { clock: Clock; transport: Transport; warn: (message: string) => void }) {
    this.clock = options.clock;
    this.transport = options.transport;
    this.warn = options.warn;
  }

  freshness(): Freshness {
    if (this.syncedAt === null) return "none";
    const age = this.clock.monotonic() - this.syncedAt;
    if (age > SNAPSHOT_MAX_AGE_MS) return "none";
    return age < FRESH_FOR_MS ? "fresh" : "stale";
  }

  // Returns a Promise only when remote verification is needed
  check(rawKey: string | null | undefined, staleKeySnapshot: "allow" | "deny"): KeyCheck | Promise<KeyCheck> {
    // 1. Bad format or checksum: 401 straight away, no network
    if (typeof rawKey !== "string") return INVALID;
    const parsed = parseApiKey(rawKey);
    if (!parsed || checksumFor(rawKey.slice(0, -CHECKSUM_LENGTH)) !== parsed.checksum) return INVALID;
    const keyId = parsed.keyId;
    const hash = createHash("sha256").update(rawKey).digest("hex");
    const freshness = this.freshness();
    // 2–3. In the snapshot: compare hashes, then check status and expiry. Past 24 hours, or with no snapshot, it's as if there were none
    if (freshness !== "none") {
      const stored = this.keys.get(keyId);
      if (stored) return this.judge(keyId, stored, hash, null);
    }
    const verified = this.verified.get(keyId);
    if (verified && this.clock.monotonic() - verified.at < SNAPSHOT_MAX_AGE_MS) {
      return this.judge(keyId, verified.key, hash, verified.consumerRecord);
    }
    // Snapshot is stale and the seller chose deny: while disconnected, keys missing from the snapshot aren't accepted
    if (freshness === "stale" && staleKeySnapshot === "deny") return INVALID;
    return this.verifyRemotely(keyId, hash);
  }

  private judge(keyId: string, stored: StoredKey, hash: string, consumerRecord: ConsumerRecord | null): KeyCheck {
    if (!constantTimeEqual(stored.hash, hash)) return INVALID;
    // 4. Status and expiry are judged by the local clock; expiry times from the server already include a 30-second margin
    if (stored.revoked || (stored.expiresAt !== null && stored.expiresAt <= this.clock.now())) {
      return { result: "invalid", consumer: stored.consumer };
    }
    return {
      result: "valid",
      keyId,
      consumer: stored.consumer,
      quotaBehavior: stored.quotaBehavior,
      quotaVersion: stored.quotaVersion,
      consumerRecord,
    };
  }

  private verifyRemotely(keyId: string, hash: string): KeyCheck | Promise<KeyCheck> {
    const now = this.clock.monotonic();
    const cacheKey = `${keyId}:${hash}`;
    const rejectedUntil = this.rejected.get(cacheKey);
    if (rejectedUntil !== undefined) {
      if (now < rejectedUntil) return INVALID;
      this.rejected.delete(cacheKey);
    }
    const pending = this.inflight.get(cacheKey);
    if (pending) return pending;
    if (now < this.outageUntil) return UNAVAILABLE;
    // Over 20 a second gets a 401 straight away, so random keys can't be used to hammer the server. Not while there's no usable snapshot yet:
    // just after startup, before the first sync completes (several seconds for a big app), every key is unknown, and real customers would be told their key is invalid.
    // In that case treat it as "can't reach the key store" (onKeyStoreUnavailable, 503 by default, which clients retry)
    if (!this.takeVerifyBudget(now)) return this.freshness() === "none" ? UNAVAILABLE : INVALID;
    const syncsAtSend = this.syncsCompleted;
    const request = this.transport
      .verifyKey({ key_id: keyId, hash })
      .then(
        (response) => this.applyVerification(keyId, hash, response, syncsAtSend),
        (error: unknown) => {
          this.outageUntil = this.clock.monotonic() + VERIFY_OUTAGE_MS;
          this.warn(`UsageRail: remote key verification failed (${describeError(error)}), applying onKeyStoreUnavailable`);
          return UNAVAILABLE;
        },
      )
      .finally(() => this.inflight.delete(cacheKey));
    this.inflight.set(cacheKey, request);
    return request;
  }

  private applyVerification(keyId: string, hash: string, response: VerifyKeyResponse, syncsAtSend: number): KeyCheck {
    const now = this.clock.monotonic();
    if (!response.valid) {
      remember(this.rejected, `${keyId}:${hash}`, now + NEGATIVE_CACHE_MS);
      return INVALID;
    }
    const stored = toStoredKey(response.key, this.generation);
    if (syncsAtSend !== this.syncsCompleted) {
      // A sync completed after this was sent, so the answer may be older than the snapshot. Don't cache it; only return it to the requests waiting on it,
      // and if the snapshot now has this key, the snapshot wins. Caching it would let a revoked key through for as long as sync keeps failing
      const current = this.freshness() === "none" ? undefined : this.keys.get(keyId);
      if (current) return this.judge(keyId, current, hash, null);
      return this.judge(keyId, stored, hash, response.consumer);
    }
    remember(this.verified, keyId, { key: stored, consumerRecord: response.consumer, at: now });
    return this.judge(keyId, stored, hash, response.consumer);
  }

  private takeVerifyBudget(now: number) {
    const second = Math.floor(now / 1000);
    if (second !== this.budgetSecond) {
      this.budgetSecond = second;
      this.budgetUsed = 0;
    }
    if (this.budgetUsed >= REMOTE_VERIFIES_PER_SECOND) return false;
    this.budgetUsed += 1;
    return true;
  }

  // ---- Sync (the engine calls this page by page) ---------------------------------

  applySyncPage(page: { full: boolean; keys: KeyRecord[]; removed_keys: string[] }) {
    // One full sync can span several pages, each with full = true: start a new generation only on the first, or the last page would delete every key from the earlier pages as stale
    if (page.full && !this.sweepPending) {
      this.generation += 1;
      this.sweepPending = true;
    }
    for (const record of page.keys) this.store(record);
    for (const keyId of page.removed_keys) this.keys.delete(keyId);
  }

  // Last page (has_more = false): delete keys absent from the full sync, mark the snapshot fresh, discard cached remote successes
  completeSync() {
    if (this.sweepPending) {
      for (const [keyId, stored] of this.keys) {
        if (stored.generation < this.generation) this.keys.delete(keyId);
      }
      this.sweepPending = false;
    }
    this.syncedAt = this.clock.monotonic();
    this.verified.clear();
    this.syncsCompleted += 1;
  }

  // Loads the snapshot saved on disk at restart. Freshness is based on the snapshot's age, with the same 24-hour limit
  load(records: KeyRecord[], savedAt: number) {
    const age = Math.max(0, this.clock.now() - savedAt);
    if (age > SNAPSHOT_MAX_AGE_MS) return;
    for (const record of records) this.store(record);
    this.syncedAt = this.clock.monotonic() - age;
  }

  records(): KeyRecord[] {
    const records: KeyRecord[] = [];
    for (const [keyId, stored] of this.keys) {
      records.push({
        key_id: keyId,
        hash: stored.hash,
        consumer: stored.consumer,
        status: stored.revoked ? "revoked" : "active",
        expires_at: stored.expiresAt === null ? null : new Date(stored.expiresAt).toISOString(),
        quota_behavior: stored.quotaBehavior,
        quota_version: stored.quotaVersion,
      });
    }
    return records;
  }

  private store(record: KeyRecord) {
    if (!this.keys.has(record.key_id) && this.keys.size >= MAX_SNAPSHOT_KEYS) {
      // Keys over the cap go through remote verification: they still work, only the first request is a bit slower
      if (!this.capacityWarned) this.warn(`UsageRail: key snapshot is full (${MAX_SNAPSHOT_KEYS} keys); extra keys are verified remotely`);
      this.capacityWarned = true;
      return;
    }
    this.keys.set(record.key_id, toStoredKey(record, this.generation));
  }
}

function toStoredKey(record: KeyRecord, generation: number): StoredKey {
  let expiresAt: number | null = null;
  if (record.expires_at !== null) {
    const parsed = Date.parse(record.expires_at);
    // An unparseable expiry counts as already expired: better to reject than to keep a key that should have expired working
    expiresAt = Number.isFinite(parsed) ? parsed : 0;
  }
  return {
    hash: record.hash.toLowerCase(),
    consumer: record.consumer,
    revoked: record.status !== "active",
    expiresAt,
    quotaBehavior: record.quota_behavior,
    quotaVersion: record.quota_version,
    generation,
  };
}

// Constant-time comparison of two hex SHA-256 strings: doesn't return early at the first differing character
function constantTimeEqual(a: string, b: string) {
  if (a.length !== b.length) return false;
  let difference = 0;
  for (let index = 0; index < a.length; index++) difference |= a.charCodeAt(index) ^ b.charCodeAt(index);
  return difference === 0;
}

// Maps iterate in insertion order, so the oldest entry is removed when over the cap
function remember<Value>(map: Map<string, Value>, key: string, value: Value) {
  map.delete(key);
  map.set(key, value);
  if (map.size > MAX_CACHED_VERIFICATIONS) map.delete(map.keys().next().value as string);
}
