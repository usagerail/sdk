"""Usage aggregation and reporting, same as meter.ts in the Node SDK.

When each request ends, add 1 in memory to (consumer, method + route template, outcome, minute), with units tracked separately;
every 60 seconds, or when the table reaches 5,000 keys, send it all as a batch. Batches go out one at a time, in order; failed ones stay queued for retry.
"""

from __future__ import annotations

import copy
import functools
import math
import threading
from concurrent.futures import Future
from dataclasses import dataclass, replace
from typing import Any, Callable, Protocol

from ._futures import error_of, settle
from ._time import iso_from_ms
from .clock import Clock
from .protocol import BILLABLE_OUTCOMES, MAX_ROW_COUNT, MAX_ROW_UNITS
from .transport import SDK_VERSION, Transport, TransportError, describe_error, is_pausing_status

FLUSH_INTERVAL_MS = 60_000
# While the app drains requests during shutdown, report once a second: that uses exactly the ingest limit of 60 batches per instance per minute
DRAIN_FLUSH_INTERVAL_MS = 1_000
# Report as soon as the table reaches 5,000 keys; at most 5,000 rows per batch
MAX_TABLE_KEYS = 5_000
# Past 500 distinct routes within one minute, new ones are folded into _other
MAX_ROUTES_PER_MINUTE = 500
# The retry queue holds at most 100,000 rows or 10 minutes, whichever comes first
MAX_QUEUED_ROWS = 100_000
MAX_QUEUE_AGE_MS = 10 * 60_000
# dropped lists at most 1,000 consumers; the rest are combined into unattributed losses
MAX_DROPPED_CONSUMERS = 1_000
# Per-entry loss caps (validated by the server): 1 billion billable requests, and 10 billion units like a usage row. Exceeding them gets the whole batch a 400, losing the valid usage in it too
MAX_DROPPED_REQUESTS = MAX_ROW_COUNT * 100
# Errors like a revoked app key or a deleted app: stop reporting and try again every 10 minutes
PAUSED_RETRY_MS = 10 * 60_000
MAX_BACKOFF_MS = 60_000
# With your own auth, before the hashing key arrives, counts by raw external ID stay in memory for at most 10 minutes
PENDING_MAX_AGE_MS = 10 * 60_000

OTHER_ROUTE = "_other"
UNMATCHED_ROUTE = "_unmatched"
# Anonymous requests (consumer() returned None with your own auth, or an invalid key so we don't know who): analytics only, never quotas or invoices.
# Reported with consumer as an empty string and consumer_kind "anonymous"
ANONYMOUS_CONSUMER = ""


@dataclass(frozen=True, slots=True)
class Usage:
    consumer: str
    kind: str
    stripe_customer_id: str | None
    method: str
    route: str
    outcome: str
    # Start of the minute the request started in (wall clock, ms)
    minute_start: float
    units: int


class LeaseReporter(Protocol):
    """The meter collects lease usage from the quota module and hands it back once the batch is confirmed or given up on"""

    def collect_reports(self) -> Any: ...
    def finish_report(self, report: Any) -> None: ...
    def forget_leases(self, lease_ids: list[str]) -> None: ...


@dataclass(slots=True)
class _Aggregate:
    usage: Usage
    count: int
    units: int
    first_seen: float


@dataclass(slots=True)
class _QueuedBatch:
    batch: dict[str, Any]
    created_at: float
    report: Any
    sending: bool = False


@dataclass(slots=True)
class _Loss:
    billable_requests: int = 0
    units: int = 0


def _no_metadata() -> dict[str, Any]:
    """A fresh object each time, with no shared lists: batches must not affect each other"""
    return {"dropped": [], "unattributed_dropped": {"billable_requests": 0, "units": 0}, "key_usage": [], "lease_used": []}


def classify_status(status: int) -> str:
    """Outcomes. 101 means a long-lived connection opened; anything uncertain counts as 5xx, which isn't billed"""
    if status == 101:
        return "ws_open"
    if 200 <= status < 300:
        return "2xx"
    if 300 <= status < 400:
        return "3xx"
    if 400 <= status < 500:
        return "4xx"
    return "5xx"


class Meter:
    def __init__(
        self,
        clock: Clock,
        transport: Transport,
        instance_id: str,
        warn: Callable[[str], None],
        leases: LeaseReporter,
        resolve_pending: Callable[[], Callable[[str], str] | None],
        lock: threading.RLock,
    ) -> None:
        self._clock = clock
        self._transport = transport
        self._instance_id = instance_id
        self._warn = warn
        self._leases = leases
        self._resolve_pending = resolve_pending
        # The engine's lock: report results come back on network threads and timers run on the timer thread, and both must hold it before changing state
        self.lock = lock
        self._table: dict[tuple[Any, ...], _Aggregate] = {}
        # Rows sealed when their count or units neared the per-row cap, reported along with the table
        self._sealed: list[_Aggregate] = []
        # With your own auth, raw external IDs from before the hashing settings arrived
        self._pending: dict[tuple[Any, ...], _Aggregate] = {}
        self._sealed_pending: list[_Aggregate] = []
        self._key_uses: dict[str, int] = {}
        self._dropped: dict[str, _Loss] = {}
        self._unattributed = _Loss()
        self._queue: list[_QueuedBatch] = []
        self._queued_rows = 0
        self._sequence = 0
        self._failures = 0
        self._next_attempt_at = -math.inf
        self._pumping: Future[None] | None = None
        self._cancel_retry: Callable[[], None] | None = None
        self._cancel_flush: Callable[[], None] | None = None
        self._closed = False
        self._routes: set[str] = set()
        self._routes_minute = -math.inf

    def start(self, interval_ms: float = FLUSH_INTERVAL_MS) -> None:
        if self._cancel_flush is not None:
            self._cancel_flush()

        def tick() -> None:
            with self.lock:
                if self._closed:
                    return
                self._cancel_flush = self._clock.set_timer(tick, interval_ms)
                self.flush()

        self._cancel_flush = self._clock.set_timer(tick, interval_ms)

    def drain(self) -> None:
        """The app is shutting down and draining requests: report once now, then once a second until close()"""
        if self._closed:
            return
        self.start(DRAIN_FLUSH_INTERVAL_MS)
        self.flush()

    def record(self, usage: Usage, pending: bool, count: int = 1) -> None:
        """pending: consumer is still the raw external ID, converted once the hashing settings arrive. count 0 is a record that only adds units (when an SSE stream ends)"""
        route = self._limit_routes(usage.route, usage.minute_start)
        if route != usage.route:
            usage = replace(usage, route=route)
        key = _aggregate_key(usage)
        table = self._pending if pending else self._table
        entry = table.get(key)
        if entry is not None:
            if _fits(entry, count, usage.units):
                entry.count += count
                entry.units += usage.units
                return
            # Adding more would exceed the per-row cap and the server would reject the whole row: seal this row and start a new one below
            (self._sealed_pending if pending else self._sealed).append(entry)
            del table[key]
        if pending and len(self._pending) + len(self._sealed_pending) + self._queued_rows >= MAX_QUEUED_ROWS:
            # Shares the 100,000-row cap with the retry queue: when full we can't tell who it was, so it goes to unattributed losses
            if usage.outcome in BILLABLE_OUTCOMES:
                self._add_unattributed(1, usage.units)
            return
        table[key] = _Aggregate(usage, count, usage.units, self._clock.monotonic())
        if not pending and len(self._table) + len(self._sealed) >= MAX_TABLE_KEYS:
            self.flush()

    def record_key_use(self, key_id: str) -> None:
        self._key_uses[key_id] = self._key_uses.get(key_id, 0) + 1

    def flush(self) -> Future[None]:
        self._enqueue_batches()
        return self._pump()

    def close(self) -> Future[None]:
        """One last attempt to report before exit, ignoring backoff. Retired leases can only be reported "closed" after the previous batch is confirmed, so send one more batch"""
        self._closed = True
        if self._cancel_flush is not None:
            self._cancel_flush()
        if self._cancel_retry is not None:
            self._cancel_retry()
            self._cancel_retry = None
        self._next_attempt_at = -math.inf
        done: Future[None] = Future()

        def second(_: Future[None]) -> None:
            with self.lock:
                if self._queue:
                    settle(done, None)
                else:
                    self.flush().add_done_callback(lambda __: settle(done, None))

        self.flush().add_done_callback(second)
        return done

    def _limit_routes(self, route: str, minute_start: float) -> str:
        if minute_start > self._routes_minute:
            self._routes_minute = minute_start
            self._routes.clear()
        if route in self._routes:
            return route
        if len(self._routes) >= MAX_ROUTES_PER_MINUTE:
            return OTHER_ROUTE
        self._routes.add(route)
        return route

    # ---- Building batches ------------------------------------------------------------

    def _enqueue_batches(self) -> None:
        self._merge_pending()
        rows = [_to_row(entry) for entry in self._sealed]
        rows.extend(_to_row(entry) for entry in self._table.values())
        self._table = {}
        self._sealed = []
        key_usage = [{"key_id": key_id, "count": count} for key_id, count in self._key_uses.items()]
        self._key_uses = {}
        report = self._leases.collect_reports()
        dropped = self._dropped_batches()
        unattributed = self._unattributed
        self._unattributed = _Loss()
        metadata = {
            "dropped": dropped[0],
            "unattributed_dropped": {"billable_requests": unattributed.billable_requests, "units": unattributed.units},
            "key_usage": key_usage,
            "lease_used": report.entries,
        }
        if not rows and _is_empty_metadata(metadata):
            return
        now = self._clock.monotonic()
        # Merged raw-ID rows can push the total past 5,000: split first, with leases, keys and losses in the last batch
        start = 0
        while True:
            last = start + MAX_TABLE_KEYS >= len(rows)
            chunk = rows[start : start + MAX_TABLE_KEYS]
            self._queue.append(_QueuedBatch(self._new_batch(chunk, metadata if last else _no_metadata()), now, report if last else None))
            self._queued_rows += len(chunk)
            if last:
                break
            start += MAX_TABLE_KEYS
        # Losses that don't fit in one batch after splitting go out in batches of their own, carrying only losses
        for extra in dropped[1:]:
            self._queue.append(_QueuedBatch(self._new_batch([], {**_no_metadata(), "dropped": extra}), now, None))
        self._enforce_limits()

    def _dropped_batches(self) -> list[list[dict[str, Any]]]:
        """Known losses are split into entries within the protocol's per-entry caps (the server adds up entries for the same consumer within a batch), 1,000 entries per batch"""
        entries: list[dict[str, Any]] = []
        for consumer, loss in self._dropped.items():
            requests, units = loss.billable_requests, loss.units
            while True:
                part_requests, part_units = min(requests, MAX_DROPPED_REQUESTS), min(units, MAX_ROW_UNITS)
                entries.append({"consumer": consumer, "billable_requests": part_requests, "units": part_units})
                requests -= part_requests
                units -= part_units
                if requests == 0 and units == 0:
                    break
        self._dropped = {}
        return [entries[start : start + MAX_DROPPED_CONSUMERS] for start in range(0, len(entries), MAX_DROPPED_CONSUMERS)] or [[]]

    def _merge_pending(self) -> None:
        """Hashing settings arrived: convert raw external IDs to reported IDs and merge them into the table; rows still waiting after 10 minutes can't be attributed and go to unattributed losses"""
        if not self._pending and not self._sealed_pending:
            return
        resolve = self._resolve_pending()
        now = self._clock.monotonic()
        if resolve is not None:
            for entry in self._sealed_pending:
                self._sealed.append(_Aggregate(replace(entry.usage, consumer=resolve(entry.usage.consumer)), entry.count, entry.units, entry.first_seen))
            self._sealed_pending = []
        else:
            self._sealed_pending = [entry for entry in self._sealed_pending if not self._expire_pending(entry, now)]
        for key, entry in list(self._pending.items()):
            if resolve is not None:
                usage = replace(entry.usage, consumer=resolve(entry.usage.consumer))
                resolved_key = _aggregate_key(usage)
                existing = self._table.get(resolved_key)
                if existing is not None and _fits(existing, entry.count, entry.units):
                    existing.count += entry.count
                    existing.units += entry.units
                elif existing is not None:
                    self._sealed.append(_Aggregate(usage, entry.count, entry.units, entry.first_seen))
                else:
                    self._table[resolved_key] = _Aggregate(usage, entry.count, entry.units, entry.first_seen)
                del self._pending[key]
            elif self._expire_pending(entry, now):
                del self._pending[key]

    def _expire_pending(self, entry: _Aggregate, now: float) -> bool:
        """Raw-ID rows that waited 10 minutes without hashing settings: can't tell who they were, so record them as unattributed losses"""
        if now - entry.first_seen <= PENDING_MAX_AGE_MS:
            return False
        if entry.usage.outcome in BILLABLE_OUTCOMES:
            self._add_unattributed(entry.count, entry.units)
        return True

    def _new_batch(self, rows: list[dict[str, Any]], metadata: dict[str, Any]) -> dict[str, Any]:
        self._sequence += 1
        return {
            # Instance ID + increasing sequence number: unchanged on retry, a new number for each sub-batch when split
            "batch_id": f"{self._instance_id}:{self._sequence}",
            "sdk_version": SDK_VERSION,
            "instance_id": self._instance_id,
            "rows": rows,
            **metadata,
        }

    # ---- Sending ---------------------------------------------------------------------

    def _pump(self) -> Future[None]:
        if self._pumping is None:
            self._pumping = Future()
            pumping = self._pumping
            self._send_queue()
            return pumping
        return self._pumping

    def _send_queue(self) -> None:
        """Send batches one at a time, in order. When the transport answers on the spot, keep sending in this loop instead of recursing"""
        while True:
            self._enforce_limits()
            if not self._queue:
                self._finish_pump()
                return
            item = self._queue[0]
            if self._clock.monotonic() < self._next_attempt_at:
                self._schedule_retry()
                self._finish_pump()
                return
            item.sending = True
            call = self._transport.ingest(item.batch)
            if not call.done():
                call.add_done_callback(functools.partial(self._sent_later, item))
                return
            if not self._after_send(item, call):
                self._schedule_retry()
                self._finish_pump()
                return

    def _sent_later(self, item: _QueuedBatch, completed: Future[dict[str, Any]]) -> None:
        with self.lock:
            if self._after_send(item, completed):
                self._send_queue()
            else:
                self._schedule_retry()
                self._finish_pump()

    def _after_send(self, item: _QueuedBatch, completed: Future[dict[str, Any]]) -> bool:
        """True: move on to the next batch; False: wait and retry later"""
        item.sending = False
        error = error_of(completed)
        if error is not None:
            return self._handle_failure(item, error)
        self._failures = 0
        self._remove_head(item)
        self._handle_accepted(item, completed.result())
        return True

    def _finish_pump(self) -> None:
        pumping, self._pumping = self._pumping, None
        if pumping is not None:
            settle(pumping, None)

    def _handle_accepted(self, item: _QueuedBatch, response: dict[str, Any]) -> None:
        if item.report is not None:
            self._leases.finish_report(item.report)
        unknown_leases = response.get("unknown_leases") or []
        if unknown_leases:
            self._leases.forget_leases(unknown_leases)
        rejected = response.get("rejected_rows") or []
        if not rejected:
            return
        # Rows the server rejected won't be sent again; record them as known losses in the next batch's dropped
        rows = item.batch["rows"]
        for rejection in rejected:
            index = rejection.get("index")
            row = rows[index] if isinstance(index, int) and 0 <= index < len(rows) else None
            if row is not None and row["result_type"] in BILLABLE_OUTCOMES:
                self._add_dropped(row["consumer"], row["count"], row["units"])
        self._warn(f"UsageRail: the server rejected {len(rejected)} usage rows ({rejected[0].get('reason')}); they won't be billed")

    def _handle_failure(self, item: _QueuedBatch, error: BaseException) -> bool:
        """True: this batch is dealt with (split or dropped), move on to the next; False: wait and retry later"""
        status = error.status if isinstance(error, TransportError) else None
        if status == 413:
            self._remove_head(item)
            parts = self._split(item)
            if parts is not None:
                self._queue[0:0] = parts
                self._queued_rows += sum(len(part.batch["rows"]) for part in parts)
            else:
                self._drop_as_known_loss(item, True)
            return True
        if status == 400:
            # The whole batch is malformed: drop it and record a known loss, no retry. A consumer ID itself may be what's invalid,
            # so don't record by consumer; fold everything into unattributed, or the next batch would carry the same ID and be rejected again
            self._remove_head(item)
            self._drop_as_known_loss(item, False)
            self._warn("UsageRail: the server could not parse a usage batch (400); it was dropped")
            return True
        if is_pausing_status(status):
            self._next_attempt_at = self._clock.monotonic() + PAUSED_RETRY_MS
            self._warn(f"UsageRail: usage reporting returned {status} (check the app key); retrying every 10 minutes")
            return False
        # 429, 5xx, network errors: exponential backoff, waiting at least as long as any Retry-After from the server
        self._failures += 1
        backoff = min(MAX_BACKOFF_MS, 1000 * 2 ** (self._failures - 1))
        retry_after = (error.retry_after_ms or 0) if isinstance(error, TransportError) else 0
        self._next_attempt_at = self._clock.monotonic() + max(backoff, retry_after)
        self._warn(f"UsageRail: usage reporting failed ({describe_error(error)}); retrying")
        return False

    def _split(self, item: _QueuedBatch) -> list[_QueuedBatch] | None:
        """Splitting on 413: send sub-batches with only billable rows first, and the one with only lease_used, key_usage and dropped last.
        Billable rows draw down balances before lease usage is recorded, so in between the server can only undercount what's available, never overcount"""
        rows = item.batch["rows"]
        metadata = _metadata_of(item.batch)
        parts: list[tuple[list[dict[str, Any]], dict[str, Any]]] = []
        if len(rows) > 1:
            half = math.ceil(len(rows) / 2)
            parts = [(rows[:half], _no_metadata()), (rows[half:], _no_metadata())]
            if not _is_empty_metadata(metadata):
                parts.append(([], metadata))
        elif len(rows) == 1 and not _is_empty_metadata(metadata):
            parts = [(rows, _no_metadata()), ([], metadata)]
        elif not rows and _metadata_size(metadata) > 1:
            first, second = _halve_metadata(metadata)
            parts = [([], first), ([], second)]
        else:
            return None
        # The lease usage receipt rides on the last sub-batch: once it's confirmed, so are all earlier ones (they're sent in order)
        return [
            _QueuedBatch(self._new_batch(part_rows, part_metadata), item.created_at, item.report if index == len(parts) - 1 else None)
            for index, (part_rows, part_metadata) in enumerate(parts)
        ]

    def _remove_head(self, item: _QueuedBatch) -> None:
        if not self._queue or self._queue[0] is not item:
            return
        self._queue.pop(0)
        self._queued_rows -= len(item.batch["rows"])

    def _enforce_limits(self) -> None:
        now = self._clock.monotonic()
        # Kept for at most 10 minutes: older batches aren't retried and are recorded per consumer in the next batch's dropped
        while self._queue and not self._queue[0].sending and now - self._queue[0].created_at > MAX_QUEUE_AGE_MS:
            item = self._queue.pop(0)
            self._queued_rows -= len(item.batch["rows"])
            self._drop_as_known_loss(item, True)
        # At most 100,000 rows (shared with raw-ID rows not yet converted): beyond that, drop the oldest
        while self._queued_rows + len(self._pending) + len(self._sealed_pending) > MAX_QUEUED_ROWS:
            index = next((position for position, queued in enumerate(self._queue) if not queued.sending), None)
            if index is None:
                break
            item = self._queue.pop(index)
            self._queued_rows -= len(item.batch["rows"])
            self._drop_as_known_loss(item, True)

    def _drop_as_known_loss(self, item: _QueuedBatch, attribute: bool) -> None:
        batch = item.batch
        for row in batch["rows"]:
            if row["result_type"] not in BILLABLE_OUTCOMES:
                continue
            if attribute:
                self._add_dropped(row["consumer"], row["count"], row["units"])
            else:
                self._add_unattributed(row["count"], row["units"])
        # Losses this batch was meant to carry move to the next batch too, or the losses themselves would be lost
        for entry in batch["dropped"]:
            if attribute:
                self._add_dropped(entry["consumer"], entry["billable_requests"], entry["units"])
            else:
                self._add_unattributed(entry["billable_requests"], entry["units"])
        self._add_unattributed(batch["unattributed_dropped"]["billable_requests"], batch["unattributed_dropped"]["units"])
        if item.report is not None:
            self._leases.finish_report(item.report)

    def _add_dropped(self, consumer: str, requests: int, units: int) -> None:
        # Anonymous usage is never billed, so losing it isn't a billing loss
        if consumer == ANONYMOUS_CONSUMER:
            return
        loss = self._dropped.get(consumer)
        if loss is not None:
            loss.billable_requests += requests
            loss.units += units
        elif len(self._dropped) < MAX_DROPPED_CONSUMERS:
            self._dropped[consumer] = _Loss(requests, units)
        else:
            self._add_unattributed(requests, units)

    def _add_unattributed(self, requests: int, units: int) -> None:
        self._unattributed.billable_requests += requests
        self._unattributed.units += units

    def _schedule_retry(self) -> None:
        if self._cancel_retry is not None or self._closed:
            return
        delay = max(0.0, self._next_attempt_at - self._clock.monotonic())

        def retry() -> None:
            with self.lock:
                self._cancel_retry = None
                self._pump()

        self._cancel_retry = self._clock.set_timer(retry, delay)


def _fits(entry: _Aggregate, count: int, units: int) -> bool:
    return entry.count + count <= MAX_ROW_COUNT and entry.units + units <= MAX_ROW_UNITS


def _aggregate_key(usage: Usage) -> tuple[Any, ...]:
    return (usage.kind, usage.consumer, usage.stripe_customer_id or "", usage.method, usage.route, usage.outcome, usage.minute_start)


def _to_row(entry: _Aggregate) -> dict[str, Any]:
    usage = entry.usage
    row: dict[str, Any] = {
        "consumer": usage.consumer,
        "method": usage.method,
        "route": usage.route,
        "result_type": usage.outcome,
        "minute": _minute_iso(usage.minute_start),
        "count": entry.count,
        "units": entry.units,
        "consumer_kind": usage.kind,
    }
    if usage.stripe_customer_id:
        row["stripe_customer_id"] = usage.stripe_customer_id
    return row


@functools.lru_cache(maxsize=64)
def _minute_iso(minute_start: float) -> str:
    """Rows in a batch mostly share a few minutes, so each is formatted once"""
    return iso_from_ms(minute_start)


def _metadata_of(batch: dict[str, Any]) -> dict[str, Any]:
    return {key: batch[key] for key in ("dropped", "unattributed_dropped", "key_usage", "lease_used")}


def _metadata_size(metadata: dict[str, Any]) -> int:
    return len(metadata["dropped"]) + len(metadata["key_usage"]) + len(metadata["lease_used"])


def _is_empty_metadata(metadata: dict[str, Any]) -> bool:
    unattributed = metadata["unattributed_dropped"]
    return _metadata_size(metadata) == 0 and unattributed["billable_requests"] == 0 and unattributed["units"] == 0


def _halve_metadata(metadata: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Combine the entries of all three lists and cut in the middle: halving each list separately would leave something like one key usage plus one lease usage
    unchanged in the first half, getting 413 and resending the same batch forever"""
    entries = [(name, item) for name in ("dropped", "key_usage", "lease_used") for item in metadata[name]]
    middle = math.ceil(len(entries) / 2)

    def part(chosen: list[tuple[str, Any]]) -> dict[str, list[Any]]:
        return {name: [item for kind, item in chosen if kind == name] for name in ("dropped", "key_usage", "lease_used")}

    first, second = part(entries[:middle]), part(entries[middle:])
    return (
        {**first, "unattributed_dropped": {"billable_requests": 0, "units": 0}},
        {**second, "unattributed_dropped": copy.copy(metadata["unattributed_dropped"])},
    )
