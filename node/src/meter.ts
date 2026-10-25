// Usage aggregation and reporting.
// When each request ends, add 1 in memory to (consumer, method + route template, outcome, minute), with units tracked separately;
// every 60 seconds, or when the table reaches 5,000 keys, send it all as a batch. Batches go out one at a time, in order; failed ones stay queued for retry.
import type { Clock } from "./clock.ts";
import {
  BILLABLE_OUTCOMES,
  MAX_ROW_COUNT,
  MAX_ROW_UNITS,
  type ConsumerKind,
  type IngestRequest,
  type IngestResponse,
  type IngestRow,
  type Outcome,
} from "./protocol.ts";
import type { LeaseReport } from "./quota.ts";
import { TransportError, describeError, isPausingStatus, type Transport } from "./transport.ts";

// Must match the version in package.json (test/engine.test.ts checks it)
export const SDK_VERSION = "0.1.1";
export const FLUSH_INTERVAL_MS = 60_000;
// While the app drains requests during shutdown, report once a second: that uses exactly the ingest limit of 60 batches per instance per minute
export const DRAIN_FLUSH_INTERVAL_MS = 1_000;
// Report as soon as the table reaches 5,000 keys; at most 5,000 rows per batch
export const MAX_TABLE_KEYS = 5_000;
// Past 500 distinct routes within one minute, new ones are folded into _other
export const MAX_ROUTES_PER_MINUTE = 500;
// The retry queue holds at most 100,000 rows or 10 minutes, whichever comes first
export const MAX_QUEUED_ROWS = 100_000;
export const MAX_QUEUE_AGE_MS = 10 * 60_000;
// dropped lists at most 1,000 consumers; the rest are combined into unattributed losses
export const MAX_DROPPED_CONSUMERS = 1_000;
// Per-entry loss caps (validated by the server): 1 billion billable requests, and 10 billion units like a usage row. Exceeding them gets the whole batch a 400, losing the valid usage in it too
export const MAX_DROPPED_REQUESTS = MAX_ROW_COUNT * 100;
// Errors like a revoked app key or a deleted app: stop reporting and try again every 10 minutes
export const PAUSED_RETRY_MS = 10 * 60_000;
export const MAX_BACKOFF_MS = 60_000;
// With your own auth, before the hashing key arrives, counts by raw external ID stay in memory for at most 10 minutes
export const PENDING_MAX_AGE_MS = 10 * 60_000;

export const OTHER_ROUTE = "_other";
export const UNMATCHED_ROUTE = "_unmatched";
// Anonymous requests (consumer() returned null with your own auth, or an invalid key so we don't know who): analytics only, never quotas or invoices.
// Reported with consumer as an empty string and consumer_kind "anonymous" (protocol.ts)
export const ANONYMOUS_CONSUMER = "";

export type Usage = {
  consumer: string;
  kind: ConsumerKind | "anonymous";
  stripeCustomerId: string | undefined;
  method: string;
  route: string;
  outcome: Outcome;
  // Start of the minute the request started in (wall clock, ms)
  minuteStart: number;
  units: number;
};

// The meter collects lease usage from the quota module and hands it back once the batch is confirmed or given up on
export type LeaseReporter = {
  collectReports(): LeaseReport;
  finishReport(report: LeaseReport): void;
  forgetLeases(leaseIds: string[]): void;
};

type Aggregate = Usage & { count: number; firstSeen: number };

type Metadata = Pick<IngestRequest, "dropped" | "unattributed_dropped" | "key_usage" | "lease_used">;

type QueuedBatch = { batch: IngestRequest; createdAt: number; report: LeaseReport | null; sending: boolean };

// A fresh object each time, with no shared arrays: batches must not affect each other
function noMetadata(): Metadata {
  return { dropped: [], unattributed_dropped: { billable_requests: 0, units: 0 }, key_usage: [], lease_used: [] };
}

// Outcomes. 101 means a long-lived connection opened; anything uncertain counts as 5xx, which isn't billed
export function classifyStatus(status: number): Outcome {
  if (status === 101) return "ws_open";
  if (status >= 200 && status < 300) return "2xx";
  if (status >= 300 && status < 400) return "3xx";
  if (status >= 400 && status < 500) return "4xx";
  return "5xx";
}

export class Meter {
  private readonly clock: Clock;
  private readonly transport: Transport;
  private readonly instanceId: string;
  private readonly warn: (message: string) => void;
  private readonly leases: LeaseReporter;
  private readonly resolvePending: () => ((rawId: string) => string) | null;
  private readonly maxBackoffMs: number;
  private readonly maxQueueAgeMs: number;
  private readonly retryTimers: boolean;
  private readonly minSubmitGapMs: number;
  private lastIngestAt = -Infinity;
  private table = new Map<string, Aggregate>();
  // Rows sealed when their count or units neared the per-row cap (protocol.ts MAX_ROW_COUNT / MAX_ROW_UNITS), reported along with the table
  private sealed: Aggregate[] = [];
  // With your own auth, raw external IDs from before the hashing settings arrived
  private readonly pending = new Map<string, Aggregate>();
  private sealedPending: Aggregate[] = [];
  private keyUses = new Map<string, number>();
  private dropped = new Map<string, { billable_requests: number; units: number }>();
  private unattributed = { billable_requests: 0, units: 0 };
  private readonly queue: QueuedBatch[] = [];
  private queuedRows = 0;
  private sequence = 0;
  private failures = 0;
  private nextAttemptAt = -Infinity;
  private pumping: Promise<void> | null = null;
  private cancelRetry: (() => void) | null = null;
  private cancelFlush: (() => void) | null = null;
  private closed = false;
  private readonly routes = new Set<string>();
  private routesMinute = -Infinity;

  constructor(options: {
    clock: Clock;
    transport: Transport;
    instanceId: string;
    warn: (message: string) => void;
    leases: LeaseReporter;
    resolvePending: () => ((rawId: string) => string) | null;
    // Longest backoff after a failure. 0 on serverless: failed batches wait for the next request. The HTTP check service sits next to its database
    // and needs to clear a backlog quickly once it recovers, so it uses less than the SDK
    maxBackoffMs?: number;
    // How long queued batches are kept; older ones are dropped and recorded as known losses. The HTTP check service sets no limit: with a backlog over 60 seconds it stops admitting requests,
    // so no new usage arrives and the queue doesn't grow. Dropping would "clear" the backlog, start admitting again, and lose more and more usage the database can't take
    maxQueueAgeMs?: number;
    // Schedule our own retries after a failure. Off on serverless: the instance may be frozen between requests so timers are unreliable, and with zero backoff
    // it would retry immediately and spin outside waitUntil. Failed batches are submitted again when the next request ends
    retryTimers?: boolean;
    // Minimum gap between two submissions (every ingest call counts, including split batches and several batches at once). 0.5 seconds on serverless,
    // to stay under the server's per-instance ingest limit per minute
    minSubmitGapMs?: number;
  }) {
    this.clock = options.clock;
    this.maxBackoffMs = options.maxBackoffMs ?? MAX_BACKOFF_MS;
    this.maxQueueAgeMs = options.maxQueueAgeMs ?? MAX_QUEUE_AGE_MS;
    this.retryTimers = options.retryTimers ?? true;
    this.minSubmitGapMs = options.minSubmitGapMs ?? 0;
    this.transport = options.transport;
    this.instanceId = options.instanceId;
    this.warn = options.warn;
    this.leases = options.leases;
    this.resolvePending = options.resolvePending;
  }

  start(intervalMs = FLUSH_INTERVAL_MS) {
    this.cancelFlush?.();
    const tick = () => {
      if (this.closed) return;
      this.cancelFlush = this.clock.setTimer(tick, intervalMs);
      void this.flush();
    };
    this.cancelFlush = this.clock.setTimer(tick, intervalMs);
  }

  // The app is shutting down and draining requests: report once now, then once a second until close()
  drain() {
    if (this.closed) return;
    this.start(DRAIN_FLUSH_INTERVAL_MS);
    void this.flush();
  }

  // pending: consumer is still the raw external ID, converted once the hashing settings arrive. count 0 is a record that only adds units (when an SSE stream ends, see protocol.ts)
  record(usage: Usage, pending: boolean, count: 0 | 1 = 1) {
    const route = this.limitRoutes(usage.route, usage.minuteStart);
    const key = aggregateKey(usage, route);
    const table = pending ? this.pending : this.table;
    const entry = table.get(key);
    if (entry) {
      if (fits(entry, count, usage.units)) {
        entry.count += count;
        entry.units += usage.units;
        return;
      }
      // Adding more would exceed the per-row cap and the server would reject the whole row: seal this row and start a new one below
      (pending ? this.sealedPending : this.sealed).push(entry);
      table.delete(key);
    }
    if (pending && this.pending.size + this.sealedPending.length + this.queuedRows >= MAX_QUEUED_ROWS) {
      // Shares the 100,000-row cap with the retry queue: when full we can't tell who it was, so it goes to unattributed losses
      if (BILLABLE_OUTCOMES.has(usage.outcome)) this.addUnattributed(1, usage.units);
      return;
    }
    table.set(key, { ...usage, route, count, firstSeen: this.clock.monotonic() });
    if (!pending && this.table.size + this.sealed.length >= MAX_TABLE_KEYS) void this.flush();
  }

  // How long the oldest queued batch has waited (monotonic, ms); 0 with no backlog. The HTTP check service stops admitting requests once its backlog passes 60 seconds
  oldestBacklogMs() {
    const oldest = this.queue[0];
    return oldest ? Math.max(0, this.clock.monotonic() - oldest.createdAt) : 0;
  }

  recordKeyUse(keyId: string) {
    this.keyUses.set(keyId, (this.keyUses.get(keyId) ?? 0) + 1);
  }

  flush(): Promise<void> {
    this.enqueueBatches();
    return this.pump();
  }

  // One last attempt to report before exit, ignoring backoff. Retired leases can only be reported "closed" after the previous batch is confirmed, so send one more batch
  async close() {
    this.closed = true;
    this.cancelFlush?.();
    this.cancelRetry?.();
    this.cancelRetry = null;
    this.nextAttemptAt = -Infinity;
    await this.flush();
    if (this.queue.length === 0) await this.flush();
  }

  private limitRoutes(route: string, minuteStart: number) {
    if (minuteStart > this.routesMinute) {
      this.routesMinute = minuteStart;
      this.routes.clear();
    }
    if (this.routes.has(route)) return route;
    if (this.routes.size >= MAX_ROUTES_PER_MINUTE) return OTHER_ROUTE;
    this.routes.add(route);
    return route;
  }

  // ---- Building batches ------------------------------------------------------------

  private enqueueBatches() {
    this.mergePending();
    const rows: IngestRow[] = this.sealed.map(toRow);
    for (const entry of this.table.values()) rows.push(toRow(entry));
    this.table = new Map();
    this.sealed = [];
    const keyUsage = [...this.keyUses].map(([key_id, count]) => ({ key_id, count }));
    this.keyUses = new Map();
    const report = this.leases.collectReports();
    const dropped = this.droppedBatches();
    const unattributed = this.unattributed;
    this.unattributed = { billable_requests: 0, units: 0 };
    const metadata: Metadata = { dropped: dropped[0], unattributed_dropped: unattributed, key_usage: keyUsage, lease_used: report.entries };
    if (rows.length === 0 && isEmptyMetadata(metadata)) return;
    const now = this.clock.monotonic();
    // Merged raw-ID rows can push the total past 5,000: split first, with leases, keys and losses in the last batch
    for (let start = 0; start < rows.length || start === 0; start += MAX_TABLE_KEYS) {
      const last = start + MAX_TABLE_KEYS >= rows.length;
      this.queue.push({
        batch: this.newBatch(rows.slice(start, start + MAX_TABLE_KEYS), last ? metadata : noMetadata()),
        createdAt: now,
        report: last ? report : null,
        sending: false,
      });
      this.queuedRows += Math.min(MAX_TABLE_KEYS, rows.length - start);
      if (last) break;
    }
    // Losses that don't fit in one batch after splitting go out in batches of their own, carrying only losses
    for (const extra of dropped.slice(1)) {
      this.queue.push({ batch: this.newBatch([], { ...noMetadata(), dropped: extra }), createdAt: now, report: null, sending: false });
    }
    this.enforceLimits();
  }

  // Known losses are split into entries within the protocol's per-entry caps (the server adds up entries for the same consumer within a batch), 1,000 entries per batch
  private droppedBatches(): Metadata["dropped"][] {
    const entries: Metadata["dropped"] = [];
    for (const [consumer, loss] of this.dropped) {
      let { billable_requests: requests, units } = loss;
      for (;;) {
        const part = { consumer, billable_requests: Math.min(requests, MAX_DROPPED_REQUESTS), units: Math.min(units, MAX_ROW_UNITS) };
        entries.push(part);
        requests -= part.billable_requests;
        units -= part.units;
        if (requests === 0 && units === 0) break;
      }
    }
    this.dropped = new Map();
    const batches: Metadata["dropped"][] = [];
    for (let start = 0; start < entries.length; start += MAX_DROPPED_CONSUMERS) batches.push(entries.slice(start, start + MAX_DROPPED_CONSUMERS));
    return batches.length > 0 ? batches : [[]];
  }

  // Hashing settings arrived: convert raw external IDs to reported IDs and merge them into the table; rows still waiting after 10 minutes can't be attributed and go to unattributed losses
  private mergePending() {
    if (this.pending.size === 0 && this.sealedPending.length === 0) return;
    const resolve = this.resolvePending();
    const now = this.clock.monotonic();
    if (resolve) {
      for (const entry of this.sealedPending) this.sealed.push({ ...entry, consumer: resolve(entry.consumer) });
      this.sealedPending = [];
    } else {
      this.sealedPending = this.sealedPending.filter((entry) => !this.expirePending(entry, now));
    }
    for (const [key, entry] of this.pending) {
      if (resolve) {
        const consumer = resolve(entry.consumer);
        const resolvedKey = aggregateKey({ ...entry, consumer }, entry.route);
        const existing = this.table.get(resolvedKey);
        if (existing && fits(existing, entry.count, entry.units)) {
          existing.count += entry.count;
          existing.units += entry.units;
        } else if (existing) {
          this.sealed.push({ ...entry, consumer });
        } else {
          this.table.set(resolvedKey, { ...entry, consumer });
        }
        this.pending.delete(key);
      } else if (this.expirePending(entry, now)) {
        this.pending.delete(key);
      }
    }
  }

  // Raw-ID rows that waited 10 minutes without hashing settings: can't tell who they were, so record them as unattributed losses
  private expirePending(entry: Aggregate, now: number) {
    if (now - entry.firstSeen <= PENDING_MAX_AGE_MS) return false;
    if (BILLABLE_OUTCOMES.has(entry.outcome)) this.addUnattributed(entry.count, entry.units);
    return true;
  }

  private newBatch(rows: IngestRow[], metadata: Metadata): IngestRequest {
    this.sequence += 1;
    return {
      // Instance ID + increasing sequence number: unchanged on retry, a new number for each sub-batch when split
      batch_id: `${this.instanceId}:${this.sequence}`,
      sdk_version: SDK_VERSION,
      instance_id: this.instanceId,
      rows,
      ...metadata,
    };
  }

  // ---- Sending ---------------------------------------------------------------------

  private pump(): Promise<void> {
    if (!this.pumping) {
      this.pumping = this.drainQueue().finally(() => {
        this.pumping = null;
      });
    }
    return this.pumping;
  }

  private async drainQueue() {
    for (;;) {
      this.enforceLimits();
      const item = this.queue[0];
      if (!item) return;
      if (this.clock.monotonic() < this.nextAttemptAt) {
        this.scheduleRetry();
        return;
      }
      const gap = this.lastIngestAt + this.minSubmitGapMs - this.clock.monotonic();
      if (gap > 0) await new Promise<void>((resolve) => this.clock.setTimer(resolve, gap));
      // While we waited out the gap, another path may have handled the head of the queue (a forced report on close), so take it again
      if (this.queue[0] !== item) continue;
      this.lastIngestAt = this.clock.monotonic();
      item.sending = true;
      let response: IngestResponse;
      try {
        response = await this.transport.ingest(item.batch);
      } catch (error) {
        item.sending = false;
        if (this.handleFailure(item, error)) continue;
        this.scheduleRetry();
        return;
      }
      item.sending = false;
      this.failures = 0;
      this.removeHead(item);
      this.handleAccepted(item, response);
    }
  }

  private handleAccepted(item: QueuedBatch, response: IngestResponse) {
    if (item.report) this.leases.finishReport(item.report);
    if (response.unknown_leases?.length) this.leases.forgetLeases(response.unknown_leases);
    const rejected = response.rejected_rows ?? [];
    if (rejected.length === 0) return;
    // Rows the server rejected won't be sent again; record them as known losses in the next batch's dropped
    for (const { index } of rejected) {
      const row = item.batch.rows[index];
      if (row && BILLABLE_OUTCOMES.has(row.result_type)) this.addDropped(row.consumer, row.count, row.units);
    }
    this.warn(`UsageRail: the server rejected ${rejected.length} usage rows (${rejected[0].reason}); they won't be billed`);
  }

  // true: this batch is dealt with (split or dropped), move on to the next; false: wait and retry later
  private handleFailure(item: QueuedBatch, error: unknown) {
    const status = error instanceof TransportError ? error.status : null;
    if (status === 413) {
      this.removeHead(item);
      const parts = this.split(item);
      if (parts) {
        this.queue.unshift(...parts);
        for (const part of parts) this.queuedRows += part.batch.rows.length;
      } else {
        this.dropAsKnownLoss(item, true);
      }
      return true;
    }
    if (status === 400) {
      // The whole batch is malformed: drop it and record a known loss, no retry. A consumer ID itself may be what's invalid,
      // so don't record by consumer; fold everything into unattributed, or the next batch would carry the same ID and be rejected again
      this.removeHead(item);
      this.dropAsKnownLoss(item, false);
      this.warn("UsageRail: the server could not parse a usage batch (400); it was dropped");
      return true;
    }
    if (isPausingStatus(status)) {
      this.nextAttemptAt = this.clock.monotonic() + PAUSED_RETRY_MS;
      this.warn(`UsageRail: usage reporting returned ${status} (check the app key); retrying every 10 minutes`);
      return false;
    }
    // 429, 5xx, network errors: exponential backoff, waiting at least as long as any Retry-After from the server
    this.failures += 1;
    const backoff = Math.min(this.maxBackoffMs, 1000 * 2 ** (this.failures - 1));
    const retryAfter = error instanceof TransportError ? (error.retryAfterMs ?? 0) : 0;
    this.nextAttemptAt = this.clock.monotonic() + Math.max(backoff, retryAfter);
    this.warn(`UsageRail: usage reporting failed (${describeError(error)}); retrying`);
    return false;
  }

  // Splitting on 413: send sub-batches with only billable rows first, and the one with only lease_used, key_usage and dropped last.
  // Billable rows draw down balances before lease usage is recorded, so in between the server can only undercount what's available, never overcount
  private split(item: QueuedBatch): QueuedBatch[] | null {
    const { rows } = item.batch;
    const metadata = metadataOf(item.batch);
    const parts: { rows: IngestRow[]; metadata: Metadata }[] = [];
    if (rows.length > 1) {
      const half = Math.ceil(rows.length / 2);
      parts.push({ rows: rows.slice(0, half), metadata: noMetadata() }, { rows: rows.slice(half), metadata: noMetadata() });
      if (!isEmptyMetadata(metadata)) parts.push({ rows: [], metadata });
    } else if (rows.length === 1 && !isEmptyMetadata(metadata)) {
      parts.push({ rows, metadata: noMetadata() }, { rows: [], metadata });
    } else if (rows.length === 0 && metadataSize(metadata) > 1) {
      const [first, second] = halveMetadata(metadata);
      parts.push({ rows: [], metadata: first }, { rows: [], metadata: second });
    } else {
      return null;
    }
    // The lease usage receipt rides on the last sub-batch: once it's confirmed, so are all earlier ones (they're sent in order)
    return parts.map((part, index) => ({
      batch: this.newBatch(part.rows, part.metadata),
      createdAt: item.createdAt,
      report: index === parts.length - 1 ? item.report : null,
      sending: false,
    }));
  }

  private removeHead(item: QueuedBatch) {
    if (this.queue[0] !== item) return;
    this.queue.shift();
    this.queuedRows -= item.batch.rows.length;
  }

  private enforceLimits() {
    const now = this.clock.monotonic();
    // Kept for at most 10 minutes: older batches aren't retried and are recorded per consumer in the next batch's dropped
    while (this.queue.length > 0 && !this.queue[0].sending && now - this.queue[0].createdAt > this.maxQueueAgeMs) {
      const item = this.queue.shift() as QueuedBatch;
      this.queuedRows -= item.batch.rows.length;
      this.dropAsKnownLoss(item, true);
    }
    // At most 100,000 rows (shared with raw-ID rows not yet converted): beyond that, drop the oldest
    while (this.queuedRows + this.pending.size + this.sealedPending.length > MAX_QUEUED_ROWS) {
      const index = this.queue.findIndex((queued) => !queued.sending);
      if (index < 0) break;
      const [item] = this.queue.splice(index, 1);
      this.queuedRows -= item.batch.rows.length;
      this.dropAsKnownLoss(item, true);
    }
  }

  private dropAsKnownLoss(item: QueuedBatch, attribute: boolean) {
    const { batch } = item;
    for (const row of batch.rows) {
      if (!BILLABLE_OUTCOMES.has(row.result_type)) continue;
      if (attribute) this.addDropped(row.consumer, row.count, row.units);
      else this.addUnattributed(row.count, row.units);
    }
    // Losses this batch was meant to carry move to the next batch too, or the losses themselves would be lost
    for (const entry of batch.dropped) {
      if (attribute) this.addDropped(entry.consumer, entry.billable_requests, entry.units);
      else this.addUnattributed(entry.billable_requests, entry.units);
    }
    this.addUnattributed(batch.unattributed_dropped.billable_requests, batch.unattributed_dropped.units);
    if (item.report) this.leases.finishReport(item.report);
  }

  private addDropped(consumer: string, requests: number, units: number) {
    // Anonymous usage is never billed, so losing it isn't a billing loss
    if (consumer === ANONYMOUS_CONSUMER) return;
    const entry = this.dropped.get(consumer);
    if (entry) {
      entry.billable_requests += requests;
      entry.units += units;
    } else if (this.dropped.size < MAX_DROPPED_CONSUMERS) {
      this.dropped.set(consumer, { billable_requests: requests, units });
    } else {
      this.addUnattributed(requests, units);
    }
  }

  private addUnattributed(requests: number, units: number) {
    this.unattributed.billable_requests += requests;
    this.unattributed.units += units;
  }

  private scheduleRetry() {
    if (!this.retryTimers || this.cancelRetry || this.closed) return;
    const delay = Math.max(0, this.nextAttemptAt - this.clock.monotonic());
    this.cancelRetry = this.clock.setTimer(() => {
      this.cancelRetry = null;
      void this.pump();
    }, delay);
  }
}

function fits(entry: Aggregate, count: number, units: number) {
  return entry.count + count <= MAX_ROW_COUNT && entry.units + units <= MAX_ROW_UNITS;
}

function aggregateKey(usage: Usage, route: string) {
  return `${usage.kind}\u0000${usage.consumer}\u0000${usage.stripeCustomerId ?? ""}\u0000${usage.method}\u0000${route}\u0000${usage.outcome}\u0000${usage.minuteStart}`;
}

function toRow(entry: Aggregate): IngestRow {
  const row: IngestRow = {
    consumer: entry.consumer,
    method: entry.method,
    route: entry.route,
    result_type: entry.outcome,
    minute: new Date(entry.minuteStart).toISOString(),
    count: entry.count,
    units: entry.units,
  };
  row.consumer_kind = entry.kind;
  if (entry.stripeCustomerId) row.stripe_customer_id = entry.stripeCustomerId;
  return row;
}

function metadataOf(batch: IngestRequest): Metadata {
  return {
    dropped: batch.dropped,
    unattributed_dropped: batch.unattributed_dropped,
    key_usage: batch.key_usage,
    lease_used: batch.lease_used,
  };
}

function metadataSize(metadata: Metadata) {
  return metadata.dropped.length + metadata.key_usage.length + metadata.lease_used.length;
}

function isEmptyMetadata(metadata: Metadata) {
  return (
    metadataSize(metadata) === 0 &&
    metadata.unattributed_dropped.billable_requests === 0 &&
    metadata.unattributed_dropped.units === 0
  );
}

// Concatenate the entries of all three lists and cut in the middle: halving each list separately would leave something like one key usage plus one lease usage
// unchanged in the first half, getting 413 and resending the same batch forever
function halveMetadata(metadata: Metadata): [Metadata, Metadata] {
  let firstShare = Math.ceil(metadataSize(metadata) / 2);
  const take = <Item>(items: Item[]): [Item[], Item[]] => {
    const count = Math.min(items.length, firstShare);
    firstShare -= count;
    return [items.slice(0, count), items.slice(count)];
  };
  const [droppedA, droppedB] = take(metadata.dropped);
  const [keysA, keysB] = take(metadata.key_usage);
  const [leasesA, leasesB] = take(metadata.lease_used);
  return [
    { dropped: droppedA, unattributed_dropped: { billable_requests: 0, units: 0 }, key_usage: keysA, lease_used: leasesA },
    { dropped: droppedB, unattributed_dropped: metadata.unattributed_dropped, key_usage: keysB, lease_used: leasesB },
  ];
}
