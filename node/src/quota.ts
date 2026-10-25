// Quota leases. For consumers with hard limits or prepaid credits, the SDK first gets a lease from the server,
// reserves from it when a request starts, settles by actual units when it ends, and renews in the background when it runs low. Consumers billed for overage don't get leases.
//
// Conventions that hold throughout this file:
// - A lease's available is what can still be admitted locally; usage (unreported) is tracked separately, and what's reported is the units actually used. The two never mix.
// - When actual usage exceeds the reservation (more custom units than reserved), the excess becomes a debt on the consumer. Later leases pay it off first,
//   and no new requests are admitted until it's paid. That's the "once negative, admit nothing until a renewal covers it" rule: leases the server issued before it knew about the excess
//   hand out too much, and the debt offsets that. Leases requested after the server confirmed receiving that usage already account for it and don't pay it off.
// - One request can gather its reservation from several leases, so quota split across leases never blocks anyone.
// - After a lease is retired, the next batch carries "lease closed" only once every request it admitted has finished and all its usage was reported successfully.
import type { Clock } from "./clock.ts";
import { MAX_ROW_UNITS, type ConsumerKind, type Lease, type LeaseRequestItem, type LeaseResult, type QuotaBehavior } from "./protocol.ts";
import { describeError, type Transport } from "./transport.ts";

const MINUTE_MS = 60_000;
// Local lifetime: retired 60 seconds after it arrives, by the monotonic clock
export const LEASE_TTL_MS = 60_000;
// Renew in the background when less than 30% is left
export const RENEW_BELOW_FRACTION = 0.3;
// Leases still in use are renewed early once they're 45 seconds old; otherwise a low-traffic consumer would have one request a minute wait for a new lease
export const RENEW_AFTER_MS = 45_000;
// A request waits for a lease for at most 300 ms (a hard rule)
export const SYNC_WAIT_MS = 300;
// Background renewals have no request waiting, so their timeout is more generous: after a timeout the server may already have issued the lease, and that quota only comes back when it's force-settled 30 minutes later
export const RENEW_TIMEOUT_MS = 2_000;
// At most 500 consumers per batched renewal
export const MAX_LEASE_REQUESTS_PER_CALL = 500;
// How long "exhausted" and "suspended" results are cached before asking again. Quota comes back when other instances settle, but that doesn't change the version,
// so caching by version alone would block this consumer until the next period
export const DENIAL_RECHECK_MS = 10_000;
// For a few seconds after a lease request fails, don't wait at all and apply onQuotaUnavailable: when the server is down, requests shouldn't each wait the full 300 ms
export const LEASE_OUTAGE_MS = 5_000;
// Caches quota state for at most 40,000 consumers, evicting the least recently used, to keep memory bounded. Consumers with unsettled requests or unpaid debt aren't evicted
export const MAX_QUOTA_CONSUMERS = 40_000;
// Look at no more than 64 entries per eviction: when the table is full and everyone is busy, a new customer shouldn't trigger a scan of all 40,000 (milliseconds each time, blocking the event loop)
const MAX_EVICTION_SCAN = 64;
// at can't be more than 24 hours before server time (protocol.ts); anything older is left out and the server uses its current time
const MAX_AT_AGE_MS = 23 * 60 * MINUTE_MS;

// behavior undefined means not known yet (a consumer with your own auth that hasn't appeared in a sync): the first request gets a lease synchronously
export type QuotaInfo = { behavior: QuotaBehavior | null | undefined; version: number };

export type QuotaDecision =
  | { allowed: true; reservation: Reservation | null }
  | { allowed: false; error: "quota_exceeded"; retryAfterSeconds: number | null }
  | { allowed: false; error: "insufficient_credits" | "quota_unavailable" | "consumer_suspended" };

export type LocalLease = {
  readonly id: string;
  readonly owner: ConsumerState;
  readonly granted: number;
  readonly version: number;
  readonly behavior: "stop" | "prepaid";
  readonly periodEnd: number;
  // On the monotonic clock: when the request for this lease was sent, and when it's retired locally
  readonly requestedAt: number;
  readonly expiresAt: number;
  available: number;
  inFlight: number;
  // Settled usage not yet put into a report batch
  unreported: number;
  // Number of batches carrying its usage that the server hasn't confirmed yet
  unacked: number;
  retired: boolean;
  closeSent: boolean;
};

// One reservation can come from several leases
export type Reservation = { readonly parts: { lease: LocalLease; units: number }[]; readonly units: number; readonly owner: ConsumerState; settled: boolean };

// A debt: usage beyond the reservation. Once the batch reporting it is confirmed by the server (ackedAt), leases requested afterwards no longer pay it off
type Debt = { amount: number; lease: LocalLease; periodEnd: number; reported: boolean; ackedAt: number | null };

// The lease part of a report batch. Handed back to finishReport once the batch is confirmed or given up on
export type LeaseReport = {
  entries: { lease_id: string; used: number; closed?: boolean }[];
  used: LocalLease[];
  closed: LocalLease[];
  // Debts whose usage this batch carries
  debts: Debt[];
};

type Denial = {
  reason: "exhausted" | "suspended";
  version: number;
  behavior: "stop" | "prepaid" | null;
  // "Exhausted" was judged for a reservation this large: smaller requests might still get a lease, so the result doesn't apply to them
  reserve: number;
  // Hard limits: when the period ends (wall clock), derived from the server's retry_after
  periodEnd: number | null;
  checkedAt: number;
};

type ConsumerState = {
  // Can be renamed (rename): every lease held points at this object, so the name changes in place
  consumer: string;
  kind: ConsumerKind;
  version: number;
  behavior: QuotaBehavior | null | undefined;
  unlimitedVersion: number | null;
  denial: Denial | null;
  // Leases that can still admit requests, oldest first
  leases: LocalLease[];
  debts: Debt[];
  // Requests that reserved and haven't settled, and requests waiting for a lease: a consumer with either nonzero isn't evicted (evictIdle)
  inFlight: number;
  waiting: number;
  fetching: Promise<void> | null;
  // When the lease request currently in flight was sent (monotonic clock)
  fetchingSince: number;
  queued: boolean;
  // Reservation to ask for on the next lease request: that of the most recent request that needed quota
  wantedReserve: number;
  lastMinuteEnd: number;
  // Usage over the last 60 seconds: estimated from two adjacent one-minute windows, weighted, so each consumer takes only three numbers
  windowStart: number;
  usageCurrent: number;
  usagePrevious: number;
};

const UNMETERED: QuotaDecision = Object.freeze({ allowed: true, reservation: null });
const QUOTA_UNAVAILABLE: QuotaDecision = Object.freeze({ allowed: false, error: "quota_unavailable" });
const INSUFFICIENT_CREDITS: QuotaDecision = Object.freeze({ allowed: false, error: "insufficient_credits" });
const SUSPENDED: QuotaDecision = Object.freeze({ allowed: false, error: "consumer_suspended" });

export function minuteEndOf(wallTime: number) {
  return Math.floor(wallTime / MINUTE_MS) * MINUTE_MS + MINUTE_MS;
}

export class QuotaManager {
  private readonly clock: Clock;
  private readonly transport: Transport;
  private readonly warn: (message: string) => void;
  private readonly onUnavailable: () => "allow" | "deny";
  private readonly consumers = new Map<string, ConsumerState>();
  // Every lease not yet reported as closed, including retired ones still waiting on in-flight requests
  private readonly leases = new Map<string, LocalLease>();
  private readonly renewQueue = new Set<ConsumerState>();
  private cancelDrain: (() => void) | null = null;
  private lastDrainAt = -Infinity;
  private outageUntil = -Infinity;
  private stopped = false;
  // false: no timers for renewal (serverless); queued renewals go out when renewNow() runs after a request
  private readonly background: boolean;
  // Lease requests in flight: renewNow() waits for these too, or the platform would cut them off when waitUntil ends
  private readonly inFlight = new Set<Promise<void>>();
  // Serverless batched renewal: the run currently going out. Renewals queued while it's open ride along, and it only finishes once the queue is empty
  private renewal: { open: boolean; done: Promise<void> } | null = null;

  constructor(options: { clock: Clock; transport: Transport; warn: (message: string) => void; onUnavailable: () => "allow" | "deny"; background?: boolean }) {
    this.background = options.background ?? true;
    this.clock = options.clock;
    this.transport = options.transport;
    this.warn = options.warn;
    this.onUnavailable = options.onUnavailable;
  }

  // Called when a request starts. Returns a Promise only when it really has to wait for the server (first lease, next period's lease, leases used up concurrently)
  reserve(consumer: string, kind: ConsumerKind, info: QuotaInfo, units: number, startedAt: number): QuotaDecision | Promise<QuotaDecision> {
    // Consumers billed for overage, or with no quota, don't get leases and don't need state
    if ((info.behavior === null || info.behavior === "overage") && !this.consumers.has(consumer)) return UNMETERED;
    const state = this.stateFor(consumer, kind);
    this.observe(state, info.version, info.behavior);
    const minuteEnd = minuteEndOf(startedAt);
    if (minuteEnd > state.lastMinuteEnd) state.lastMinuteEnd = minuteEnd;
    state.wantedReserve = units;
    const decision = this.decideLocally(state, units, minuteEnd);
    if (decision) return decision;
    if (this.clock.monotonic() < this.outageUntil) return this.whenUnavailable();
    return this.waitForLease(state, units, minuteEnd);
  }

  // When the control plane is unavailable, or the consumer ID can't be worked out (your own auth, hashing key not here yet): apply onQuotaUnavailable
  whenUnavailable(): QuotaDecision {
    return this.onUnavailable() === "deny" ? QUOTA_UNAVAILABLE : UNMETERED;
  }

  // Called when a request ends: refund what was over-reserved, charge any excess; for non-billable outcomes actualUnits is 0 and everything is refunded.
  // Actual usage is spread over the leases in reservation order, with anything beyond the reservation charged to the last one
  settle(reservation: Reservation, actualUnits: number) {
    if (reservation.settled) return;
    reservation.settled = true;
    const state = reservation.owner;
    state.inFlight -= 1;
    let remaining = actualUnits;
    reservation.parts.forEach((part, index) => {
      const used = index === reservation.parts.length - 1 ? remaining : Math.min(part.units, remaining);
      remaining -= used;
      const lease = part.lease;
      lease.inFlight -= 1;
      lease.unreported += used;
      lease.available += part.units - used;
      if (lease.available < 0) {
        lease.owner.debts.push({ amount: -lease.available, lease, periodEnd: lease.periodEnd, reported: false, ackedAt: null });
        lease.available = 0;
      }
    });
    if (state.debts.length > 0) {
      this.absorbDebts(state);
      if (state.debts.length > 0) this.queueRenewal(state);
    }
  }

  // Consumer details from sync: a version change retires old leases; status back to active lifts a suspension
  observeRecord(consumer: string, version: number, behavior: QuotaBehavior | null, status: "active" | "suspended") {
    const state = this.consumers.get(consumer);
    if (!state) return;
    this.observe(state, version, behavior);
    if (status === "active" && state.denial?.reason === "suspended") state.denial = null;
  }

  // ---- Reporting (called by the meter when it builds a batch) ------------------------

  collectReports(): LeaseReport {
    const now = this.clock.monotonic();
    const minuteEnd = minuteEndOf(this.clock.now());
    const report: LeaseReport = { entries: [], used: [], closed: [], debts: [] };
    for (const lease of this.leases.values()) {
      if (!lease.retired && (now >= lease.expiresAt || lease.periodEnd < minuteEnd || lease.version < lease.owner.version)) {
        this.retire(lease);
      }
      if (lease.unreported !== 0) {
        // Each entry's usage has to stay under the protocol cap (the server merges entries for the same lease ID within a batch)
        for (let left = lease.unreported; left > 0; left -= MAX_ROW_UNITS) {
          report.entries.push({ lease_id: lease.id, used: Math.min(left, MAX_ROW_UNITS) });
        }
        lease.unreported = 0;
        lease.unacked += 1;
        report.used.push(lease);
        for (const debt of lease.owner.debts) {
          if (debt.lease === lease && !debt.reported) {
            debt.reported = true;
            report.debts.push(debt);
          }
        }
      } else if (lease.retired && !lease.closeSent && lease.inFlight === 0 && lease.unacked === 0) {
        // Retired, every admitted request finished, all usage reported: this batch carries "lease closed", which the server settles on
        report.entries.push({ lease_id: lease.id, used: 0, closed: true });
        lease.closeSent = true;
        report.closed.push(lease);
      }
    }
    return report;
  }

  // The batch was confirmed, or given up on (recorded as a known loss). If given up, the usage is lost but we carry on:
  // reporting "closed" and waiting for the server's 30-minute forced settlement release the same amount, just sooner or later
  finishReport(report: LeaseReport) {
    for (const lease of report.used) lease.unacked -= 1;
    for (const lease of report.closed) this.leases.delete(lease.id);
    // The batch with the debt usage was confirmed (or given up on): from now on the server counts it in what's available
    const now = this.clock.monotonic();
    for (const debt of report.debts) debt.ackedAt = now;
    for (const state of new Set(report.debts.map((debt) => debt.lease.owner))) this.absorbDebts(state);
  }

  // Leases the server doesn't recognize (already force-settled, etc.): stop reporting them
  forgetLeases(leaseIds: string[]) {
    for (const leaseId of leaseIds) {
      const lease = this.leases.get(leaseId);
      if (!lease) continue;
      this.retire(lease);
      this.leases.delete(leaseId);
    }
  }

  // Before exit: retire everything, and the last two batches report usage and "closed"
  retireAll() {
    for (const lease of this.leases.values()) this.retire(lease);
  }

  // Serverless: no background timers, so queued renewals go out right after the request (inside waitUntil), and we wait for them along with any already in flight.
  // Same as background renewal, at most one batch a second and 500 per batch: less than 1 second since the last batch means waiting inside waitUntil until a full second has passed,
  // and beyond 500, the rest go out a batch a second. Renewals queued by other requests while this run is still going ride along with it
  renewNow(): Promise<void> {
    if (this.renewal?.open) return this.renewal.done;
    const renewal = { open: true, done: Promise.resolve() };
    // If the queue empties immediately, the run finishes before renewInBatches returns (open is false), and anything queued later starts a new run
    renewal.done = this.renewInBatches(renewal);
    this.renewal = renewal;
    return renewal.done;
  }

  private async renewInBatches(renewal: { open: boolean }) {
    const calls: Promise<void>[] = [];
    while (this.renewQueue.size > 0 && !this.stopped) {
      const wait = this.lastDrainAt + 1000 - this.clock.monotonic();
      if (wait > 0) {
        await new Promise<void>((resolve) => this.clock.setTimer(resolve, wait));
        continue;
      }
      this.cancelDrain?.();
      this.cancelDrain = null;
      const due = this.takeRenewals();
      if (due.length === 0) continue;
      this.lastDrainAt = this.clock.monotonic();
      calls.push(this.requestLeases(due, RENEW_TIMEOUT_MS));
    }
    // Renewals queued from this point on wait for the next renewNow()
    renewal.open = false;
    await Promise.all([...calls, ...this.inFlight]);
  }

  stop() {
    this.stopped = true;
    this.cancelDrain?.();
    this.cancelDrain = null;
  }

  // ---- Local decisions ---------------------------------------------------------------

  private decideLocally(state: ConsumerState, units: number, minuteEnd: number): QuotaDecision | null {
    const denial = state.denial;
    if (denial && denial.reason === "suspended" && denial.version === state.version) {
      this.recheckLater(state, denial);
      return SUSPENDED;
    }
    if (state.behavior === null || state.behavior === "overage" || state.unlimitedVersion === state.version) return UNMETERED;
    this.dropUnusableLeases(state, minuteEnd);
    if (this.canReserve(state, units)) return { allowed: true, reservation: this.take(state, units) };
    if (
      denial &&
      denial.reason === "exhausted" &&
      denial.version === state.version &&
      units >= denial.reserve &&
      !(denial.periodEnd !== null && minuteEnd > denial.periodEnd)
    ) {
      this.recheckLater(state, denial);
      if (denial.behavior === "prepaid") return INSUFFICIENT_CREDITS;
      const retryAfterSeconds = denial.periodEnd === null ? null : Math.max(1, Math.ceil((denial.periodEnd - this.clock.now()) / 1000));
      return { allowed: false, error: "quota_exceeded", retryAfterSeconds };
    }
    return null;
  }

  private recheckLater(state: ConsumerState, denial: Denial) {
    if (this.clock.monotonic() - denial.checkedAt >= DENIAL_RECHECK_MS) this.queueRenewal(state);
  }

  // Retire leases whose version is stale, whose local lifetime has run out, or whose period ends before the end of the request's minute (strictly)
  private dropUnusableLeases(state: ConsumerState, minuteEnd: number) {
    if (state.leases.length === 0) return;
    const now = this.clock.monotonic();
    let kept = 0;
    for (const lease of state.leases) {
      if (lease.version < state.version || now >= lease.expiresAt || minuteEnd > lease.periodEnd) {
        lease.retired = true;
      } else {
        state.leases[kept++] = lease;
      }
    }
    state.leases.length = kept;
  }

  // Debts paid off, and the leases held add up to enough for this reservation
  private canReserve(state: ConsumerState, units: number) {
    this.absorbDebts(state);
    if (state.debts.length > 0) return false;
    let available = 0;
    for (const lease of state.leases) available += lease.available;
    return available >= units;
  }

  // Pay off debts with the leases held. A debt is only paid by leases requested while the server didn't know about it;
  // once the server has confirmed it, with no earlier lease left to pay it and no earlier lease request in flight, it needn't be paid anymore
  private absorbDebts(state: ConsumerState) {
    if (state.debts.length === 0) return;
    const remaining: Debt[] = [];
    for (const debt of state.debts) {
      for (const lease of state.leases) {
        if (debt.amount === 0) break;
        // Hard limits reset each period: excess in one period was counted in that period and doesn't take quota from later ones
        if (lease.behavior === "stop" && lease.periodEnd > debt.periodEnd) continue;
        if (debt.ackedAt !== null && lease.requestedAt >= debt.ackedAt) continue;
        const absorbed = Math.min(debt.amount, lease.available);
        lease.available -= absorbed;
        debt.amount -= absorbed;
      }
      if (debt.amount === 0) continue;
      const nextPeriodHeld = state.leases.some((lease) => lease.behavior === "stop" && lease.periodEnd > debt.periodEnd);
      if (nextPeriodHeld) continue;
      if (debt.ackedAt !== null) {
        const earlierLease = state.leases.some((lease) => lease.requestedAt < debt.ackedAt! && lease.available > 0);
        const earlierRequest = state.fetching !== null && state.fetchingSince < debt.ackedAt;
        if (!earlierLease && !earlierRequest) continue;
      }
      remaining.push(debt);
    }
    state.debts = remaining;
  }

  // Gather from the oldest lease first; one request can span several
  private take(state: ConsumerState, units: number): Reservation {
    const parts: Reservation["parts"] = [];
    let left = units;
    for (const lease of state.leases) {
      if (left === 0) break;
      const taken = Math.min(lease.available, left);
      if (taken <= 0) continue;
      lease.available -= taken;
      lease.inFlight += 1;
      parts.push({ lease, units: taken });
      left -= taken;
    }
    this.countUsage(state, units);
    state.inFlight += 1;
    const newest = state.leases[state.leases.length - 1];
    let available = 0;
    for (const held of state.leases) available += held.available;
    const aging = this.clock.monotonic() >= newest.expiresAt - (LEASE_TTL_MS - RENEW_AFTER_MS);
    if (available < newest.granted * RENEW_BELOW_FRACTION || aging) this.queueRenewal(state);
    return { parts, units, owner: state, settled: false };
  }

  // No usable lease locally: get one synchronously (or wait for the renewal already in flight), 300 ms at most in total.
  // Only a timeout or failed request means the control plane is unavailable and onQuotaUnavailable applies; if the server answered and it's still not enough, the quota is exhausted
  private async waitForLease(state: ConsumerState, units: number, minuteEnd: number): Promise<QuotaDecision> {
    // This customer can't be evicted while waiting: the lease would land on discarded state, this request would reserve from it, and any excess debt would be lost too
    state.waiting += 1;
    try {
      const deadline = this.clock.monotonic() + SYNC_WAIT_MS;
      // A new lease may be used up by concurrent requests arriving at the same moment, so get one at most twice more within the same 300 ms
      for (let attempt = 0; attempt < 3; attempt++) {
        const remaining = deadline - this.clock.monotonic();
        if (remaining <= 0 || this.clock.monotonic() < this.outageUntil) return this.whenUnavailable();
        state.wantedReserve = units;
        const fetching = state.fetching ?? this.requestLeases([state], remaining);
        if (!(await this.settlesWithin(fetching, remaining)) || this.clock.monotonic() < this.outageUntil) return this.whenUnavailable();
        const decision = this.decideLocally(state, units, minuteEnd);
        if (decision) return decision;
      }
      return this.outOfQuota(state);
    } finally {
      state.waiting -= 1;
    }
  }

  // The server issued a lease every time, but this request still doesn't fit locally: debt isn't paid off, or concurrent requests used up the new lease.
  // That means not enough quota, so onQuotaUnavailable=allow must not let it through. A renewal will cover it soon, so tell the client to retry in 1 second
  private outOfQuota(state: ConsumerState): QuotaDecision {
    if (state.behavior === "prepaid") return INSUFFICIENT_CREDITS;
    return { allowed: false, error: "quota_exceeded", retryAfterSeconds: 1 };
  }

  private settlesWithin(promise: Promise<void>, timeoutMs: number): Promise<boolean> {
    return new Promise((resolve) => {
      const cancel = this.clock.setTimer(() => resolve(false), timeoutMs);
      void promise.then(() => {
        cancel();
        resolve(true);
      });
    });
  }

  // ---- Requesting leases -------------------------------------------------------------

  private requestLeases(states: ConsumerState[], timeoutMs: number): Promise<void> {
    const oldestAt = this.clock.now() - MAX_AT_AGE_MS;
    const requests: LeaseRequestItem[] = states.map((state) => ({
      consumer: state.consumer,
      consumer_kind: state.kind,
      reserve: Math.max(1, state.wantedReserve),
      recent_usage_60s: this.recentUsage(state),
      quota_version: state.version,
      // Include the end of the request's minute, so near a period boundary the lease is for the period this request belongs to
      ...(state.lastMinuteEnd > oldestAt ? { at: new Date(state.lastMinuteEnd).toISOString() } : {}),
    }));
    const sentAt = this.clock.monotonic();
    const call: Promise<void> = this.transport
      .leases({ requests }, { timeoutMs })
      .then(
        (response) => this.applyResults(states, requests, response.results, sentAt),
        (error: unknown) => {
          this.outageUntil = this.clock.monotonic() + LEASE_OUTAGE_MS;
          this.warn(`UsageRail: lease request failed (${describeError(error)}), applying onQuotaUnavailable`);
        },
      )
      .finally(() => {
        for (const state of states) if (state.fetching === call) state.fetching = null;
        this.inFlight.delete(call);
      });
    this.inFlight.add(call);
    for (const state of states) {
      state.fetching = call;
      state.fetchingSince = sentAt;
    }
    return call;
  }

  private applyResults(states: ConsumerState[], requests: LeaseRequestItem[], results: LeaseResult[], sentAt: number) {
    // Match by the name used when sending: the state may have been renamed while the request was in flight, and the response uses the old name
    const byConsumer = new Map(states.map((state, index) => [requests[index].consumer, state]));
    const reserveOf = new Map(requests.map((request) => [request.consumer, request.reserve]));
    const now = this.clock.monotonic();
    for (const result of results) {
      const state = byConsumer.get(result.consumer);
      if (!state) continue;
      if (result.status === "granted") {
        this.observe(state, result.lease.quota_version, result.lease.behavior);
        this.addLease(state, result.lease, sentAt);
        continue;
      }
      // When the server's version is newer, the server wins (protocol.ts); results older than what we hold are ignored
      this.observe(state, result.quota_version, result.status === "exhausted" ? result.behavior : undefined);
      if (result.quota_version < state.version) continue;
      if (result.status === "unlimited") {
        state.unlimitedVersion = result.quota_version;
        state.denial = null;
      } else if (result.status === "exhausted") {
        const periodEnd = result.retry_after === null ? null : this.clock.now() + result.retry_after * 1000;
        state.denial = {
          reason: "exhausted",
          version: result.quota_version,
          behavior: result.behavior,
          reserve: reserveOf.get(result.consumer) ?? 1,
          periodEnd,
          checkedAt: now,
        };
      } else {
        // Suspended: block every request and retire the leases held, until a sync says it's active again (protocol.ts)
        for (const lease of state.leases) lease.retired = true;
        state.leases = [];
        state.denial = { reason: "suspended", version: result.quota_version, behavior: null, reserve: 1, periodEnd: null, checkedAt: now };
      }
    }
  }

  private addLease(state: ConsumerState, granted: Lease, requestedAt: number) {
    const periodEnd = Date.parse(granted.period_end);
    const lease: LocalLease = {
      id: granted.lease_id,
      owner: state,
      granted: granted.granted,
      version: granted.quota_version,
      behavior: granted.behavior,
      periodEnd,
      requestedAt,
      expiresAt: this.clock.monotonic() + LEASE_TTL_MS,
      available: granted.granted,
      inFlight: 0,
      unreported: 0,
      unacked: 0,
      retired: false,
      closeSent: false,
    };
    this.leases.set(lease.id, lease);
    // An older version than we hold, or an unreadable period: don't use it, go straight to closing so the server gets it back quickly
    if (lease.version < state.version || !Number.isFinite(periodEnd)) {
      lease.retired = true;
      return;
    }
    state.leases.push(lease);
    state.behavior = lease.behavior;
    state.denial = null;
    state.unlimitedVersion = null;
    this.absorbDebts(state);
  }

  private observe(state: ConsumerState, version: number, behavior: QuotaBehavior | null | undefined) {
    if (version > state.version) {
      state.version = version;
      const wasActive = state.leases.length > 0;
      // The quota version changed (plan change, new period, top-up, refund, grant expiry, manual adjustment): retire old leases at once and drop cached results
      for (const lease of state.leases) lease.retired = true;
      state.leases = [];
      state.denial = null;
      state.unlimitedVersion = null;
      if (behavior !== undefined) state.behavior = behavior;
      // It had leases, so this consumer is active: get a new one in the background so the next request doesn't wait
      if (wasActive && (state.behavior === "stop" || state.behavior === "prepaid")) this.queueRenewal(state);
    } else if (behavior !== undefined && state.behavior === undefined) {
      state.behavior = behavior;
    }
  }

  private retire(lease: LocalLease) {
    lease.retired = true;
    const held = lease.owner.leases;
    const index = held.indexOf(lease);
    if (index >= 0) held.splice(index, 1);
  }

  // ---- Background renewal: renewals within the same second are batched into one request ----

  private queueRenewal(state: ConsumerState) {
    if (this.stopped || state.queued || state.fetching || this.clock.monotonic() < this.outageUntil) return;
    state.queued = true;
    this.renewQueue.add(state);
    this.scheduleDrain();
  }

  private scheduleDrain() {
    if (!this.background || this.cancelDrain || this.renewQueue.size === 0 || this.stopped) return;
    // Less than 1 second since the last batched renewal: wait until a full second has passed, sending everything queued meanwhile together, at most once a second
    const delay = Math.max(0, this.lastDrainAt + 1000 - this.clock.monotonic());
    this.cancelDrain = this.clock.setTimer(() => this.drain(), delay);
  }

  private drain() {
    this.cancelDrain = null;
    this.lastDrainAt = this.clock.monotonic();
    const due = this.takeRenewals();
    if (due.length > 0) void this.requestLeases(due, RENEW_TIMEOUT_MS);
    this.scheduleDrain();
  }

  // Take at most one batch (500) from the renewal queue. Consumers with a lease request in flight don't need renewing; what comes back is new
  private takeRenewals() {
    const due: ConsumerState[] = [];
    for (const state of this.renewQueue) {
      this.renewQueue.delete(state);
      state.queued = false;
      if (!state.fetching) due.push(state);
      if (due.length === MAX_LEASE_REQUESTS_PER_CALL) break;
    }
    return due;
  }

  // ---- Consumer state ----------------------------------------------------------------

  private stateFor(consumer: string, kind: ConsumerKind): ConsumerState {
    const existing = this.consumers.get(consumer);
    if (existing) {
      // Move to the end: Map iteration order is then least to most recently used
      this.consumers.delete(consumer);
      this.consumers.set(consumer, existing);
      return existing;
    }
    const state: ConsumerState = {
      consumer,
      kind,
      version: 0,
      behavior: undefined,
      unlimitedVersion: null,
      denial: null,
      leases: [],
      debts: [],
      inFlight: 0,
      waiting: 0,
      fetching: null,
      fetchingSince: 0,
      queued: false,
      wantedReserve: 1,
      lastMinuteEnd: 0,
      windowStart: this.clock.monotonic(),
      usageCurrent: 0,
      usagePrevious: 0,
    };
    if (this.consumers.size >= MAX_QUOTA_CONSUMERS) this.evictIdle();
    this.consumers.set(consumer, state);
    return state;
  }

  // Drop a consumer's local state: its leases are retired as usual, the next batch reports them closed, and the next request asks the server again.
  // Used in two places: evicted from memory (the eviction rule), and a sync says it's gone (missing from a full sync, e.g. deleted)
  // The same consumer under a new local name (HTTP check service: a newly created external ID switches to the consumer ID once synced): quota state and leases move with it.
  // Otherwise the leases under the old name sit unused, holding the server's available quota so the new name can't get any. If the new name already has state, the old is retired (the next batch reports it closed and the server takes back what's unused)
  rename(from: string, to: string, kind: ConsumerKind) {
    const state = this.consumers.get(from);
    if (!state || from === to) return;
    if (this.consumers.has(to)) {
      this.forget(from);
      return;
    }
    this.consumers.delete(from);
    state.consumer = to;
    state.kind = kind;
    this.consumers.set(to, state);
  }

  forget(consumer: string) {
    const state = this.consumers.get(consumer);
    if (!state) return;
    this.consumers.delete(consumer);
    this.renewQueue.delete(state);
    for (const lease of state.leases) lease.retired = true;
    state.leases = [];
  }

  // Evict from the least recently used until under the cap. Skip consumers with requests waiting for a lease or unsettled, or with unpaid debt: once evicted, leases coming back,
  // usage beyond the reservation from requests that end later, and unpaid debt would all land on discarded state, and a new lease next time wouldn't know about them and would admit too much
  // (breaking "once negative, admit nothing"). Look at no more than 64 per call; busy ones seen are moved to the end so the next call continues with unseen ones. If all are busy, evict nothing for now:
  // the excess is evicted in batches by later new customers once those consumers go idle
  private evictIdle() {
    const busy: ConsumerState[] = [];
    let scanned = 0;
    for (const [consumer, state] of this.consumers) {
      if (this.consumers.size < MAX_QUOTA_CONSUMERS || scanned === MAX_EVICTION_SCAN) break;
      scanned += 1;
      if (state.inFlight > 0 || state.waiting > 0 || state.debts.length > 0) busy.push(state);
      else this.forget(consumer);
    }
    for (const state of busy) {
      this.consumers.delete(state.consumer);
      this.consumers.set(state.consumer, state);
    }
  }

  private rollUsageWindow(state: ConsumerState) {
    const elapsed = Math.floor((this.clock.monotonic() - state.windowStart) / MINUTE_MS);
    if (elapsed < 1) return;
    state.usagePrevious = elapsed === 1 ? state.usageCurrent : 0;
    state.usageCurrent = 0;
    state.windowStart += elapsed * MINUTE_MS;
  }

  private countUsage(state: ConsumerState, units: number) {
    this.rollUsageWindow(state);
    state.usageCurrent += units;
  }

  private recentUsage(state: ConsumerState) {
    this.rollUsageWindow(state);
    const previousWeight = 1 - (this.clock.monotonic() - state.windowStart) / MINUTE_MS;
    return Math.round(state.usageCurrent + state.usagePrevious * previousWeight);
  }
}
