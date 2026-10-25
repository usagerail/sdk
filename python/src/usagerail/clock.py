"""Clock. Every "now" in the SDK comes from here, so tests can inject a fake clock instead of actually waiting."""

from __future__ import annotations

import heapq
import itertools
import os
import threading
import time
from typing import Callable, Protocol


class Clock(Protocol):
    def now(self) -> float:
        """Wall-clock time (ms): minute buckets, key expiry, billing period boundaries. These have to line up with the server and Stripe"""
        ...

    def monotonic(self) -> float:
        """Monotonic time (ms): local lease lifetime, token buckets, backoff, cache durations. Unaffected by someone changing the system clock or NTP adjustments"""
        ...

    def set_timer(self, callback: Callable[[], None], delay_ms: float) -> Callable[[], None]:
        """Timer that returns a cancel function. Background sync, reporting and lease renewal all go through it, so tests can advance a fake clock"""
        ...


class _Timer:
    __slots__ = ("callback", "cancelled")

    def __init__(self, callback: Callable[[], None]) -> None:
        self.callback = callback
        self.cancelled = False

    def cancel(self) -> None:
        self.cancelled = True


class SystemClock:
    """All timers share one background thread and run in order of due time, rather than one thread per timer: every request waiting for a lease has a timer.
    The thread is a daemon, so it doesn't keep the process alive (like unref in Node). Callbacks must be quick; network work goes to the transport's threads."""

    def __init__(self) -> None:
        self._reset()
        # With gunicorn --preload and the like, the app is built before forking: the child has none of the parent's threads, and a lock may be stuck held.
        # The child starts over; timers queued in the parent belong to the parent's engine, and the engine reschedules its own in the child (engine.py)
        # Windows has no fork and no such function
        if hasattr(os, "register_at_fork"):
            os.register_at_fork(after_in_child=self._reset)

    def _reset(self) -> None:
        self._condition = threading.Condition()
        self._queue: list[tuple[float, int, _Timer]] = []
        self._order = itertools.count()
        self._thread: threading.Thread | None = None

    def now(self) -> float:
        return time.time() * 1000

    def monotonic(self) -> float:
        return time.monotonic() * 1000

    def set_timer(self, callback: Callable[[], None], delay_ms: float) -> Callable[[], None]:
        timer = _Timer(callback)
        due = time.monotonic() + max(0.0, delay_ms) / 1000
        with self._condition:
            heapq.heappush(self._queue, (due, next(self._order), timer))
            if self._thread is None or not self._thread.is_alive():
                self._thread = threading.Thread(target=self._run, name="usagerail-timers", daemon=True)
                self._thread.start()
            self._condition.notify()
        return timer.cancel

    def _run(self) -> None:
        while True:
            with self._condition:
                while True:
                    while self._queue and self._queue[0][2].cancelled:
                        heapq.heappop(self._queue)
                    if not self._queue:
                        self._condition.wait()
                        continue
                    wait = self._queue[0][0] - time.monotonic()
                    if wait <= 0:
                        timer = heapq.heappop(self._queue)[2]
                        break
                    self._condition.wait(wait)
            if timer.cancelled:
                continue
            try:
                timer.callback()
            except Exception:  # noqa: BLE001 - a failing callback must not kill the timer thread; the engine logs its own warnings
                pass


system_clock = SystemClock()
