// Per-consumer token buckets. They live in SDK memory, one set per instance, with no network calls.
// Per-second and per-minute limits can both be set; a request passes only if both buckets have a token.
import type { Clock } from "./clock.ts";
import type { AppSettings, ConsumerRecord } from "./protocol.ts";

export type RateLimits = ConsumerRecord["rate_limits"];
export type RateLimitMode = AppSettings["rate_limit_mode"];

// Caches state for at most 40,000 consumers, evicting the least recently used, same as quota state (quota.ts) to keep memory bounded. An evicted bucket starts full next time
export const MAX_RATE_LIMITED_CONSUMERS = 40_000;

const WINDOWS = [
  { name: "per_second", seconds: 1 },
  { name: "per_minute", seconds: 60 },
] as const;

type Bucket = { capacity: number; tokens: number; updatedAt: number };

export type RateDecision =
  | { allowed: true }
  | { allowed: false; retryAfterSeconds: number; headers: Record<string, string>; policies: RateLimitPolicy[] };

const ALLOWED: RateDecision = Object.freeze({ allowed: true });

// "divided": the limit ÷ the active instance count from sync, rounded up, at least 1. "per_instance": the limit itself
export function perInstanceLimit(limit: number, mode: RateLimitMode, activeInstances: number) {
  if (mode !== "divided") return Math.max(1, limit);
  return Math.max(1, Math.ceil(limit / Math.max(1, activeInstances)));
}

export class RateLimiter {
  private readonly clock: Clock;
  private readonly buckets = new Map<string, (Bucket | null)[]>();

  constructor(clock: Clock) {
    this.clock = clock;
  }

  check(consumer: string, limits: RateLimits | null, mode: RateLimitMode, activeInstances: number): RateDecision {
    if (!limits || (limits.per_second == null && limits.per_minute == null)) return ALLOWED;
    const now = this.clock.monotonic();
    let buckets = this.buckets.get(consumer);
    if (buckets) {
      this.buckets.delete(consumer);
    } else {
      buckets = [null, null];
    }
    // Re-insert at the end: Map iteration order is then least to most recently used
    this.buckets.set(consumer, buckets);
    if (this.buckets.size > MAX_RATE_LIMITED_CONSUMERS) this.buckets.delete(this.buckets.keys().next().value as string);

    let limited = false;
    for (let index = 0; index < WINDOWS.length; index++) {
      const limit = index === 0 ? limits.per_second : limits.per_minute;
      if (limit == null) {
        buckets[index] = null;
        continue;
      }
      const bucket = refill(buckets[index], perInstanceLimit(limit, mode, activeInstances), WINDOWS[index].seconds, now);
      buckets[index] = bucket;
      if (bucket.tokens < 1) limited = true;
    }
    if (!limited) {
      for (const bucket of buckets) if (bucket) bucket.tokens -= 1;
      return ALLOWED;
    }
    return rejection(buckets);
  }
}

// Lazy refill: add tokens for the time since the last update, capped at capacity. Multiply before dividing so integer cases have no floating-point error
function refill(bucket: Bucket | null, capacity: number, windowSeconds: number, now: number): Bucket {
  if (!bucket) return { capacity, tokens: capacity, updatedAt: now };
  const earned = ((now - bucket.updatedAt) * capacity) / (windowSeconds * 1000);
  bucket.tokens = Math.min(capacity, bucket.tokens + earned);
  bucket.capacity = capacity;
  bucket.updatedAt = now;
  return bucket;
}

function rejection(buckets: (Bucket | null)[]): RateDecision {
  let retryAfterSeconds = 0;
  const policies: RateLimitPolicy[] = [];
  for (let index = 0; index < WINDOWS.length; index++) {
    const bucket = buckets[index];
    if (!bucket) continue;
    const { name, seconds } = WINDOWS[index];
    const exhausted = bucket.tokens < 1;
    // A bucket that blocked: time until it has 1 token, rounded up (waiting a little longer never earns another 429). A bucket that didn't: time until it's full
    const resetSeconds = exhausted
      ? Math.ceil(((1 - bucket.tokens) * seconds) / bucket.capacity)
      : Math.ceil(((bucket.capacity - bucket.tokens) * seconds) / bucket.capacity);
    if (exhausted) retryAfterSeconds = Math.max(retryAfterSeconds, resetSeconds);
    policies.push({ name, quota: bucket.capacity, windowSeconds: seconds, remaining: Math.floor(bucket.tokens), resetSeconds });
  }
  return { allowed: false, retryAfterSeconds, headers: rateLimitHeaders(policies, retryAfterSeconds), policies };
}

export type RateLimitPolicy = { name: string; quota: number; windowSeconds: number; remaining: number; resetSeconds: number };

// Headers for a 429: Retry-After plus the two fields from draft-ietf-httpapi-ratelimit-headers, version 11.
// If the draft changes again, only this function changes. Token buckets are expressed the same way as in the usagerail.com rate limiting guide: q is the bucket capacity, w is seconds from empty to full
export function rateLimitHeaders(policies: RateLimitPolicy[], retryAfterSeconds: number): Record<string, string> {
  return {
    "Retry-After": String(retryAfterSeconds),
    "RateLimit-Policy": policies.map((policy) => `"${policy.name}";q=${policy.quota};w=${policy.windowSeconds}`).join(", "),
    RateLimit: policies.map((policy) => `"${policy.name}";r=${policy.remaining};t=${policy.resetSeconds}`).join(", "),
  };
}
