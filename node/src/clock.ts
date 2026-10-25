// Clock. Every "now" in the SDK comes from here, so tests can inject a fake clock instead of actually waiting.
export type Clock = {
  // Wall-clock time (ms): minute buckets, key expiry, billing period boundaries. These have to line up with the server and Stripe
  now(): number;
  // Monotonic time (ms): local lease lifetime, token buckets, backoff, cache durations. Unaffected by someone changing the system clock or NTP adjustments
  monotonic(): number;
  // Timer that returns a cancel function. Background sync, reporting and lease renewal all go through it, so tests can advance a fake clock
  setTimer(callback: () => void, delayMs: number): () => void;
};

export const systemClock: Clock = {
  now: () => Date.now(),
  monotonic: () => performance.now(),
  setTimer(callback, delayMs) {
    const timer = setTimeout(callback, delayMs);
    // unref in Node so background timers don't keep the process alive. Skipped on runtimes without unref (Workers and others)
    (timer as { unref?: () => void }).unref?.();
    return () => clearTimeout(timer);
  },
};

// For serverless mode: timers are not unref'd. This mode has no background loop; its timers are short waits inside a request or a waitUntil task
// (the 0.5 s between commits, the 1 s for batching lease renewals, waiting for the first sync). When a self-hosted Next.js gets SIGTERM and closes its HTTP server,
// these timers are all that's left in the process. If they were unref'd, Node would think there's nothing left to do and exit, losing usage not yet committed in after()
export const waitingClock: Clock = {
  now: systemClock.now,
  monotonic: systemClock.monotonic,
  setTimer(callback, delayMs) {
    const timer = setTimeout(callback, delayMs);
    return () => clearTimeout(timer);
  },
};
