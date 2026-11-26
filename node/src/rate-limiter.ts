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
    const remaining = Math.floor(bucket.tokens);
    if (remaining >= 1) {
      policies.push({ name, quota: bucket.capacity, windowSeconds: seconds, remaining, retryAfterSeconds: null, effectiveWindowSeconds: null });
      continue;
    }
    const nextToken = ((1 - bucket.tokens) * seconds) / bucket.capacity;
    // Rounded up for Retry-After: retrying a little late never earns another 429.
    // Rounded down for w: until then the client can't use a token, which is what the draft's w promises (section 3.1.2).
    // Under a second rounds down to 0, a window that promises nothing, so w is left out
    const waitSeconds = Math.ceil(nextToken);
    const effectiveWindowSeconds = Math.floor(nextToken);
    // The request succeeds once every empty bucket has a token again, so Retry-After is the longest of their waits
    retryAfterSeconds = Math.max(retryAfterSeconds, waitSeconds);
    policies.push({
      name,
      quota: bucket.capacity,
      windowSeconds: seconds,
      remaining,
      retryAfterSeconds: waitSeconds,
      effectiveWindowSeconds: effectiveWindowSeconds > 0 ? effectiveWindowSeconds : null,
    });
  }
  return { allowed: false, retryAfterSeconds, headers: rateLimitHeaders(policies, retryAfterSeconds), policies };
}

// retryAfterSeconds and effectiveWindowSeconds are set only for a bucket with no whole token left (null otherwise):
// the wait for its next token rounded up, and rounded down (null under a second)
export type RateLimitPolicy = {
  name: string;
  quota: number;
  windowSeconds: number;
  remaining: number;
  retryAfterSeconds: number | null;
  effectiveWindowSeconds: number | null;
};

// Headers for a 429: Retry-After plus the RateLimit-Policy and RateLimit fields from draft-ietf-httpapi-ratelimit-headers, version 12.
// Token buckets are expressed the same way as in the usagerail.com
// rate limiting guide: q is the bucket capacity and the policy's w is seconds from empty to full.
// In RateLimit, w is the effective window (draft section 3.1.2), and only empty buckets carry it. Draft section 5 says
// Retry-After shouldn't end before an effective window it's sent with. A bucket that still has tokens can be further from
// its next token than Retry-After; an empty bucket's window ends no later than Retry-After, the longest of their waits
export function rateLimitHeaders(policies: RateLimitPolicy[], retryAfterSeconds: number): Record<string, string> {
  return {
    "Retry-After": String(retryAfterSeconds),
    "RateLimit-Policy": policies.map((policy) => `"${policy.name}";q=${policy.quota};w=${policy.windowSeconds}`).join(", "),
    RateLimit: policies
      .map((policy) => `"${policy.name}";a=${policy.remaining}` + (policy.effectiveWindowSeconds == null ? "" : `;w=${policy.effectiveWindowSeconds}`))
      .join(", "),
  };
}
