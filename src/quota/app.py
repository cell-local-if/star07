"""Multi-tenant rate limiting: the baseline service.

Public contract is README.md. Time is injected everywhere so the refill maths is testable and deterministic.
"""
from __future__ import annotations

import json
import math
import threading
import uuid
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable


class QuotaError(Exception):
    code = "internal_error"
    status = 500


class InvalidRequest(QuotaError):
    code, status = "invalid_request", 400


class LimitNotFound(QuotaError):
    code, status = "not_found", 404


class OverQuota(QuotaError):
    code, status = "over_quota", 429

    def __init__(self, message: str, retry_after: float) -> None:
        super().__init__(message)
        self.retry_after = retry_after


@dataclass
class Limit:
    capacity: int
    refill_per_second: float

    def as_json(self) -> dict[str, Any]:
        return {"capacity": self.capacity, "refill_per_second": self.refill_per_second}


@dataclass
class Bucket:
    tokens: float
    updated_at: float
    cost_history: list[float] = field(default_factory=list)


@dataclass(frozen=True)
class LedgerEvent:
    """One accepted spend in a key's billing ledger: instant checks, hierarchy checks and
    confirmed reservations only."""

    seq: int
    source: str  # "check" | "hierarchy_check" | "reservation_consume" | "hierarchy_reservation_consume"
    reservation_id: str | None
    cost: int
    remaining: int
    capacity: int
    effective_at: float

    def as_json(self) -> dict[str, Any]:
        return {"seq": self.seq, "source": self.source, "reservation_id": self.reservation_id,
                "cost": self.cost, "remaining": self.remaining, "capacity": self.capacity,
                "effective_at": self.effective_at}


@dataclass
class Reservation:
    reservation_id: str
    key: str
    cost: int
    created_at: float
    ttl_seconds: int
    rolled_back: bool = False

    def expires_at(self) -> float:
        return self.created_at + self.ttl_seconds


@dataclass
class HierarchyReservation:
    """One cross-layer hold: `cost` tokens held on every layer of `keys` simultaneously.

    Lives in its own registry, shares the single-key holds' lock, clock watermark and lazy
    expiry rule (created_at + ttl_seconds, boundary inclusive). It is all-or-nothing in both
    directions: created only when every layer can pay, and settled — by expiry, rollback or
    consume — as one unit, so no layer is ever left half-held or half-refunded.
    """

    reservation_id: str
    keys: list[str]
    cost: int
    created_at: float
    ttl_seconds: int

    def expires_at(self) -> float:
        return self.created_at + self.ttl_seconds


@dataclass(frozen=True)
class WindowConfig:
    """One sliding-window limit: at most max_events successful admissions per window_seconds."""

    window_seconds: int
    max_events: int

    def as_json(self) -> dict[str, Any]:
        return {"window_seconds": self.window_seconds, "max_events": self.max_events}


@dataclass
class SlidingWindow:
    """Accepted-admission timestamps for one window key, oldest first; nothing else ever lands here."""

    config: WindowConfig
    events: list[float] = field(default_factory=list)


def validate_key(key: Any) -> str:
    if not isinstance(key, str) or not key or len(key) > 200:
        raise InvalidRequest("key must be a non-empty string of at most 200 characters")
    return key


def validate_cost(cost: Any) -> int:
    if not isinstance(cost, int) or isinstance(cost, bool) or cost < 1 or cost > 1_000_000:
        raise InvalidRequest("cost must be an integer between 1 and 1000000")
    return cost


def validate_keys(keys: Any) -> list[str]:
    """The hierarchy endpoint's ordered parent-to-leaf key list: 2..20 distinct keys, each
    passing the same key rule as configure/check. Anything else is invalid_request and the
    request never reaches the lock."""
    if not isinstance(keys, list) or not 2 <= len(keys) <= 20:
        raise InvalidRequest("keys must be a list of 2 to 20 distinct key strings")
    seen: set[str] = set()
    for key in keys:
        validate_key(key)
        if key in seen:
            raise InvalidRequest("keys must not contain duplicates")
        seen.add(key)
    return list(keys)


DEFAULT_TTL_SECONDS = 60


def retry_after_header(retry_after: float) -> str:
    """The single Retry-After rule for every 429: raw seconds needed to refill this rejection's
    cost at the current rate, rounded UP to whole milliseconds and rendered with three decimals.

    Ceiling — never plain rounding or text truncation — keeps the hinted wait at or above the
    exact deficit/rate, so a sub-millisecond shortfall still hints 0.001 rather than 0.000, and
    every concurrent path that reaches the same rejection emits the identical value.
    """
    return f"{math.ceil(retry_after * 1000) / 1000:.3f}"


def validate_ttl(ttl: Any) -> int:
    if not isinstance(ttl, int) or isinstance(ttl, bool) or ttl < 1 or ttl > 86_400:
        raise InvalidRequest("ttl_seconds must be an integer between 1 and 86400")
    return ttl


def validate_limit(payload: Any) -> Limit:
    if not isinstance(payload, dict):
        raise InvalidRequest("body must be a JSON object")
    extra = set(payload) - {"capacity", "refill_per_second"}
    if extra:
        raise InvalidRequest(f"unknown fields: {sorted(extra)}")
    capacity, rate = payload.get("capacity"), payload.get("refill_per_second")
    if not isinstance(capacity, int) or isinstance(capacity, bool) or capacity < 1 or capacity > 1_000_000:
        raise InvalidRequest("capacity must be an integer between 1 and 1000000")
    if not isinstance(rate, (int, float)) or isinstance(rate, bool) or rate <= 0 or rate > 1_000_000:
        raise InvalidRequest("refill_per_second must be a positive number")
    return Limit(int(capacity), float(rate))


def validate_window(payload: Any) -> WindowConfig:
    if not isinstance(payload, dict):
        raise InvalidRequest("body must be a JSON object")
    extra = set(payload) - {"window_seconds", "max_events"}
    if extra:
        raise InvalidRequest(f"unknown fields: {sorted(extra)}")
    window_seconds, max_events = payload.get("window_seconds"), payload.get("max_events")
    if (not isinstance(window_seconds, int) or isinstance(window_seconds, bool)
            or window_seconds < 1 or window_seconds > 3_600):
        raise InvalidRequest("window_seconds must be an integer between 1 and 3600")
    if (not isinstance(max_events, int) or isinstance(max_events, bool)
            or max_events < 1 or max_events > 1_000_000):
        raise InvalidRequest("max_events must be an integer between 1 and 1000000")
    return WindowConfig(window_seconds, max_events)


class Limiter:
    """One token bucket per tenant key. `now` is a seconds callable, injected for tests.

    The clock may stall, jump backwards, or drift between readings inside a single call. Every
    public operation therefore samples it exactly once, after taking the lock, and clamps the
    reading to the highest value ever observed (the watermark). Refill, expiry settling,
    reservation creation and reconfiguration within that operation all use this single effective
    moment: a regressed reading is treated as "time stayed at the watermark", so no tokens are
    conjured, no reservation is refunded early or late, and the regressed interval is never
    counted again once the clock recovers.
    """

    def __init__(self, now: Callable[[], float]) -> None:
        self._now = now
        self._lock = threading.RLock()
        self._limits: dict[str, Limit] = {}
        self._buckets: dict[str, Bucket] = {}
        self._reservations: dict[str, Reservation] = {}
        # Confirmed holds leave the active registry for good; the value is the exact first consume
        # response, replayed verbatim for idempotent retries (used is never booked a second time).
        self._consumed: dict[str, dict[str, Any]] = {}
        # Cross-layer holds and their confirmed-consume replays, in namespaces of their own: a
        # reservation_id created here never resolves through the single-key endpoints and vice
        # versa (cross-resource identifiers are 404, not confusion).
        self._hierarchy_reservations: dict[str, HierarchyReservation] = {}
        self._hierarchy_consumed: dict[str, dict[str, Any]] = {}
        # Append-only billing ledger per key: one entry per accepted check and per first consume.
        # Entries are appended inside the lock at booking time, so seq is dense and gap-free even
        # under concurrent settling; the list is never trimmed (read-only views take a tail slice).
        self._ledgers: dict[str, list[LedgerEvent]] = {}
        # Independent sliding windows, keyed in their own namespace: a window named like a bucket
        # shares neither history nor accounting with it, and window admissions never touch a ledger.
        self._windows: dict[str, SlidingWindow] = {}
        # Highest clock reading ever observed; the effective moment never moves below it.
        self._watermark = float("-inf")

    def _tick(self) -> float:
        """Sample the clock once and clamp to the high-water mark. Caller holds the lock."""
        reading = self._now()
        if reading > self._watermark:
            self._watermark = reading
        return self._watermark

    def configure(self, key: Any, payload: Any) -> Limit:
        key = validate_key(key)
        limit = validate_limit(payload)
        with self._lock:
            now = self._tick()
            if key not in self._limits:
                self._limits[key] = limit
                self._buckets[key] = Bucket(limit.capacity, now)
                return limit
            # Hot reconfiguration of a live bucket. The wait since the bucket's last effective
            # moment is first refilled at the OLD rate (capped at the OLD capacity); only then is
            # the new configuration installed, so the new rate governs nothing before this moment
            # and a capacity increase never conjures a full bucket.
            self._refill(key, now)
            self._limits[key] = limit
            # Due holds settle after the old-rate refill and are credited against the NEW capacity
            # (their _refill calls are no-ops: updated_at is already this effective moment).
            self._expire_due(key, now)
            bucket = self._buckets[key]
            bucket.tokens = min(float(limit.capacity), bucket.tokens)
            bucket.updated_at = now
        return limit

    def limit(self, key: str) -> Limit:
        with self._lock:
            if key not in self._limits:
                raise LimitNotFound(f"no limit configured for {key!r}")
            return self._limits[key]

    def _refill(self, key: str, now: float) -> Bucket:
        limit = self._limits[key]
        bucket = self._buckets.get(key) or Bucket(limit.capacity, now)
        elapsed = max(0.0, now - bucket.updated_at)
        bucket.tokens = min(float(limit.capacity), bucket.tokens + elapsed * limit.refill_per_second)
        bucket.updated_at = now
        self._buckets[key] = bucket
        return bucket

    def _expire_due(self, key: str, now: float) -> int:
        """Lazy, deterministic expiry settle: release every reservation of `key` whose TTL has elapsed.

        Covers both single-key holds and every cross-layer hold spanning `key`. Each reservation's
        cost is returned at most once: it is removed from its registry before any bucket is
        credited. A cross-layer hold lapses as one unit — all of its layers are refilled and
        credited in this same critical section, each capped at its own current capacity — so no
        layer ever observes a half-expired hold or is refunded twice for it. The bucket refills by
        elapsed time first, then the returned cost is capped at the key's current capacity.
        Boundary is inclusive (created_at + ttl <= now). Caller holds the lock and supplies the
        operation's single effective moment; a no-op when the key has no due reservations (and
        never touches an unconfigured key).
        """
        due = [rid for rid, reservation in self._reservations.items()
               if reservation.key == key and not reservation.rolled_back
               and reservation.expires_at() <= now]
        hierarchy_due = [rid for rid, reservation in self._hierarchy_reservations.items()
                         if key in reservation.keys and reservation.expires_at() <= now]
        if not due and not hierarchy_due:
            return 0
        returned = sum(self._reservations[rid].cost for rid in due)
        for rid in due:
            del self._reservations[rid]
        own = 0
        for rid in hierarchy_due:
            reservation = self._hierarchy_reservations.pop(rid)
            for layer_key in reservation.keys:
                layer_bucket = self._refill(layer_key, now)
                layer_bucket.tokens = min(float(self._limits[layer_key].capacity),
                                          layer_bucket.tokens + reservation.cost)
            own += reservation.cost
        bucket = self._refill(key, now)
        bucket.tokens = min(float(self._limits[key].capacity), bucket.tokens + returned)
        return returned + own

    def _record_event(self, key: str, source: str, cost: int, reservation_id: str | None,
                      remaining: float, capacity: int, effective_at: float) -> None:
        """Append one accepted spend to the key's ledger. Caller holds the lock, booking just happened.

        The new seq is len+1 computed inside the same critical section as the booking, so concurrent
        bookings can neither skip nor reuse a number; totals derived from these entries therefore
        never under- or over-count.
        """
        events = self._ledgers.setdefault(key, [])
        events.append(LedgerEvent(len(events) + 1, source, reservation_id, cost,
                                  int(remaining), capacity, effective_at))

    def check(self, key: Any, cost: Any) -> dict[str, Any]:
        # Same key rule as configure/reserve, enforced before the lock: a rejected check
        # never samples the clock, refills, deducts, or posts a ledger event.
        key = validate_key(key)
        cost = validate_cost(cost)
        with self._lock:
            now = self._tick()
            limit = self.limit(key)
            self._expire_due(key, now)
            bucket = self._refill(key, now)
            if bucket.tokens >= cost:
                bucket.tokens -= cost
                bucket.cost_history.append(cost)
                self._record_event(key, "check", cost, None, bucket.tokens, limit.capacity, now)
                return {"allowed": True, "remaining": int(bucket.tokens), "capacity": limit.capacity}
            deficit = cost - bucket.tokens
            retry_after = deficit / limit.refill_per_second
            raise OverQuota(f"key {key!r} has {bucket.tokens:.3f} tokens, needs {cost}", retry_after)

    def hierarchy_check(self, keys: Any, cost: Any) -> dict[str, Any]:
        """Atomically deduct `cost` from every layer of an ordered parent-to-leaf hierarchy.

        Same critical section, same single effective moment as check(): validation happens before
        the lock, then every layer's due reservations are settled and its bucket refilled at this
        one moment, and only then is affordability judged. The decision is all-or-nothing — either
        every layer can pay and all are deducted and booked together, or none is touched (beyond
        the lazy expiry settle every entry point performs) and the rejection hints the longest
        wait any insufficient layer needs to cover its own deficit.
        """
        keys = validate_keys(keys)
        cost = validate_cost(cost)
        with self._lock:
            now = self._tick()
            # Configuration is validated for every layer, in input order, before any settle or
            # deduction: the 404 names the first unconfigured key and no layer's state changes.
            for key in keys:
                if key not in self._limits:
                    raise LimitNotFound(f"no limit configured for {key!r}")
            buckets = []
            for key in keys:
                self._expire_due(key, now)
                buckets.append(self._refill(key, now))
            shortfalls = [(key, bucket) for key, bucket in zip(keys, buckets) if bucket.tokens < cost]
            if shortfalls:
                retry_after = max((cost - bucket.tokens) / self._limits[key].refill_per_second
                                  for key, bucket in shortfalls)
                raise OverQuota(
                    f"hierarchy layers short of cost {cost}: "
                    + ", ".join(f"{key!r} has {bucket.tokens:.3f}" for key, bucket in shortfalls),
                    retry_after)
            layers = []
            for key, bucket in zip(keys, buckets):
                bucket.tokens -= cost
                bucket.cost_history.append(cost)
                limit = self._limits[key]
                self._record_event(key, "hierarchy_check", cost, None, bucket.tokens, limit.capacity, now)
                layers.append({"key": key, "remaining": int(bucket.tokens), "capacity": limit.capacity})
            return {"allowed": True, "cost": cost, "layers": layers}

    def hierarchy_reserve(self, keys: Any, cost: Any,
                          ttl_seconds: Any = DEFAULT_TTL_SECONDS) -> dict[str, Any]:
        """Atomically hold `cost` tokens on every layer of a hierarchy, without booking them as used.

        Same critical section, same single effective moment and same all-or-nothing judgement as
        hierarchy_check: validation happens before the lock, configuration is confirmed for every
        layer in input order (the 404 names the first unconfigured key), then each layer's due
        holds — single-key and cross-layer alike — are settled and its bucket refilled at this one
        moment. Either every layer can pay and all are deducted together, or none is touched and
        the rejection hints the longest wait any insufficient layer needs to cover its own deficit.
        The hold books no used and no ledger event; that happens only at consume time. It lapses
        `ttl_seconds` of monotonic time after creation; reconfiguring a layer never extends it.
        """
        keys = validate_keys(keys)
        cost = validate_cost(cost)
        ttl_seconds = validate_ttl(ttl_seconds)
        with self._lock:
            now = self._tick()
            for key in keys:
                if key not in self._limits:
                    raise LimitNotFound(f"no limit configured for {key!r}")
            buckets = []
            for key in keys:
                self._expire_due(key, now)
                buckets.append(self._refill(key, now))
            shortfalls = [(key, bucket) for key, bucket in zip(keys, buckets) if bucket.tokens < cost]
            if shortfalls:
                retry_after = max((cost - bucket.tokens) / self._limits[key].refill_per_second
                                  for key, bucket in shortfalls)
                raise OverQuota(
                    f"hierarchy layers short of cost {cost}: "
                    + ", ".join(f"{key!r} has {bucket.tokens:.3f}" for key, bucket in shortfalls),
                    retry_after)
            for bucket in buckets:
                bucket.tokens -= cost
            reservation = HierarchyReservation(uuid.uuid4().hex, list(keys), cost, now, ttl_seconds)
            self._hierarchy_reservations[reservation.reservation_id] = reservation
            layers = [{"key": key, "remaining": int(bucket.tokens), "capacity": self._limits[key].capacity}
                      for key, bucket in zip(keys, buckets)]
            return {"reservation_id": reservation.reservation_id, "keys": list(keys), "cost": cost,
                    "ttl_seconds": ttl_seconds, "layers": layers}

    def reserve(self, key: Any, cost: Any, ttl_seconds: Any = DEFAULT_TTL_SECONDS) -> dict[str, Any]:
        """Atomically hold `cost` tokens, exactly as check() judges them, without booking them as used.

        The hold lapses `ttl_seconds` of monotonic time after creation; reconfiguring the key never
        extends it. Expired holds are settled lazily at the start of this call (see _expire_due).
        """
        validate_key(key)
        cost = validate_cost(cost)
        ttl_seconds = validate_ttl(ttl_seconds)
        with self._lock:
            now = self._tick()
            limit = self.limit(key)
            self._expire_due(key, now)
            bucket = self._refill(key, now)
            if bucket.tokens < cost:
                deficit = cost - bucket.tokens
                raise OverQuota(f"key {key!r} has {bucket.tokens:.3f} tokens, needs {cost}",
                                deficit / limit.refill_per_second)
            bucket.tokens -= cost
            reservation = Reservation(uuid.uuid4().hex, key, cost, now, ttl_seconds)
            self._reservations[reservation.reservation_id] = reservation
            return {"reservation_id": reservation.reservation_id, "key": key, "cost": cost,
                    "remaining": int(bucket.tokens), "capacity": limit.capacity,
                    "ttl_seconds": ttl_seconds}

    def rollback(self, reservation_id: str) -> dict[str, Any]:
        with self._lock:
            now = self._tick()
            reservation = self._reservations.get(reservation_id)
            if reservation is None:
                raise LimitNotFound(f"no rollbackable reservation {reservation_id!r}")
            key = reservation.key
            # Rollback starts with the same lazy settle: a reservation whose TTL has elapsed has
            # already been refunded, so it is unknown to this endpoint and returns 404.
            self._expire_due(key, now)
            reservation = self._reservations.get(reservation_id)
            if reservation is None or reservation.rolled_back:
                raise LimitNotFound(f"no rollbackable reservation {reservation_id!r}")
            reservation.rolled_back = True
            del self._reservations[reservation_id]
            limit = self._limits[key]
            bucket = self._refill(key, now)
            bucket.tokens = min(float(limit.capacity), bucket.tokens + reservation.cost)
            return {"reservation_id": reservation_id, "rolled_back": True,
                    "remaining": int(bucket.tokens), "capacity": limit.capacity}

    def consume(self, reservation_id: str) -> dict[str, Any]:
        """Confirm a live hold as real usage.

        Due reservations of the key are refunded first, exactly as every other entry does. The target
        hold's cost was already taken from the bucket at reserve() time, so here it is booked once into
        used with no extra deduction and no refund. Repeating the call replays the first response byte
        for byte and never books used again.
        """
        with self._lock:
            now = self._tick()
            snapshot = self._consumed.get(reservation_id)
            if snapshot is not None:
                return dict(snapshot)
            reservation = self._reservations.get(reservation_id)
            if reservation is None:
                raise LimitNotFound(f"no consumable reservation {reservation_id!r}")
            key = reservation.key
            # A reservation whose TTL has elapsed is refunded by the existing settle first; the target
            # is then unknown, consumes nothing and is never booked later.
            self._expire_due(key, now)
            reservation = self._reservations.pop(reservation_id, None)
            if reservation is None:
                raise LimitNotFound(f"no consumable reservation {reservation_id!r}")
            limit = self._limits[key]
            bucket = self._refill(key, now)
            bucket.cost_history.append(reservation.cost)
            self._record_event(key, "reservation_consume", reservation.cost, reservation_id,
                               bucket.tokens, limit.capacity, now)
            snapshot = {"reservation_id": reservation_id, "consumed": True,
                        "remaining": int(bucket.tokens), "capacity": limit.capacity,
                        "used": sum(bucket.cost_history)}
            self._consumed[reservation_id] = snapshot
            return dict(snapshot)

    def hierarchy_rollback(self, reservation_id: str) -> dict[str, Any]:
        """Cancel a live cross-layer hold exactly once: every layer gets the held cost back.

        The same lazy settle runs first on each layer, so a hold whose TTL has elapsed has already
        been refunded as one unit and is unknown to this endpoint (404) — as are repeated
        rollbacks, unknown identifiers, single-key reservation identifiers and confirmed holds
        (a confirmed hold left the registry at consume time and is never refunded). Each layer is
        refilled by elapsed time first, then credited the creation-time cost capped at its current
        capacity, all inside the one critical section: no partial refunds.
        """
        with self._lock:
            now = self._tick()
            reservation = self._hierarchy_reservations.get(reservation_id)
            if reservation is None:
                raise LimitNotFound(f"no rollbackable reservation {reservation_id!r}")
            for key in reservation.keys:
                self._expire_due(key, now)
            reservation = self._hierarchy_reservations.pop(reservation_id, None)
            if reservation is None:
                raise LimitNotFound(f"no rollbackable reservation {reservation_id!r}")
            layers = []
            for key in reservation.keys:
                limit = self._limits[key]
                bucket = self._refill(key, now)
                bucket.tokens = min(float(limit.capacity), bucket.tokens + reservation.cost)
                layers.append({"key": key, "remaining": int(bucket.tokens), "capacity": limit.capacity})
            return {"reservation_id": reservation_id, "rolled_back": True, "layers": layers}

    def hierarchy_consume(self, reservation_id: str) -> dict[str, Any]:
        """Confirm a live cross-layer hold as real usage on every layer.

        Every layer's due holds are settled first, exactly as the single-key consume settles its
        key. The target hold's cost was already taken from each bucket at reserve time, so here it
        is booked once per layer into used — no extra deduction, no refund — and each layer's
        ledger gains one event with source "hierarchy_reservation_consume" carrying the shared
        reservation_id. Repeating the call replays the first response byte for byte and never
        books used again; an expired, rolled-back or unknown hold is 404 and is never booked later.
        """
        with self._lock:
            now = self._tick()
            snapshot = self._hierarchy_consumed.get(reservation_id)
            if snapshot is not None:
                return dict(snapshot)
            reservation = self._hierarchy_reservations.get(reservation_id)
            if reservation is None:
                raise LimitNotFound(f"no consumable reservation {reservation_id!r}")
            for key in reservation.keys:
                self._expire_due(key, now)
            reservation = self._hierarchy_reservations.pop(reservation_id, None)
            if reservation is None:
                raise LimitNotFound(f"no consumable reservation {reservation_id!r}")
            layers = []
            for key in reservation.keys:
                limit = self._limits[key]
                bucket = self._refill(key, now)
                bucket.cost_history.append(reservation.cost)
                self._record_event(key, "hierarchy_reservation_consume", reservation.cost,
                                   reservation_id, bucket.tokens, limit.capacity, now)
                layers.append({"key": key, "remaining": int(bucket.tokens), "capacity": limit.capacity})
            snapshot = {"reservation_id": reservation_id, "consumed": True, "layers": layers}
            self._hierarchy_consumed[reservation_id] = snapshot
            return dict(snapshot)

    def state(self, key: str) -> dict[str, Any]:
        with self._lock:
            now = self._tick()
            limit = self.limit(key)
            self._expire_due(key, now)
            bucket = self._refill(key, now)
            return {"limit": limit.as_json(), "remaining": int(bucket.tokens), "used": sum(bucket.cost_history)}

    def ledger(self, key: str, event_limit: int = 100) -> dict[str, Any]:
        """Read-only billing ledger: the accepted spends behind GET /v1/limits/{key}'s `used`.

        Deliberately samples no clock and runs no settle/refill: a read never advances the
        watermark, releases a hold, conjures tokens or books usage, so clock stalls or regressions
        cannot cause early refunds or late postings through this entry point. Only the lock is taken
        so the copy is consistent with concurrent check/reserve/consume/rollback/expiry work.
        `event_limit` is assumed pre-validated as an int in 1..1000; the returned tail is ordered by
        seq ascending while totals always cover the full append-only history.
        """
        with self._lock:
            if key not in self._limits:
                raise LimitNotFound(f"no limit configured for {key!r}")
            events = self._ledgers.get(key, [])
            return {
                "key": key,
                "totals": {"accepted_count": len(events),
                           "accepted_cost": sum(event.cost for event in events)},
                "events": [event.as_json() for event in events[-event_limit:]],
            }

    def _expire_window(self, window: SlidingWindow, now: float) -> None:
        """Drop every window event whose age has reached window_seconds at this effective moment.

        An event admitted at effective_at leaves the window at effective_at + window_seconds, i.e.
        it is stale once effective_at <= now - window_seconds: the boundary is inclusive, so an old
        event stamped exactly on the edge is settled before admission is judged. Admissions are
        stamped with the non-decreasing watermark, so the surviving timestamps are an ordered tail
        and one prefix drop settles them all. A stalled clock produces an identical cutoff and drops
        nothing; a regressed reading is clamped to the watermark before the cutoff is ever computed.
        Caller holds the lock and supplies the operation's single effective moment.
        """
        cutoff = now - window.config.window_seconds
        events = window.events
        index = 0
        while index < len(events) and events[index] <= cutoff:
            index += 1
        if index:
            del events[:index]

    def configure_window(self, key: Any, payload: Any) -> WindowConfig:
        """Create or replace a window's configuration while keeping its admitted history.

        The new config governs the response and every later admission. Shortening window_seconds
        settles the (now out-of-window) prefix immediately at this effective moment; lowering
        max_events never revokes past admissions — the surviving history stands and only subsequent
        checks are rejected. Invalid input is rejected before the lock and changes nothing.
        """
        key = validate_key(key)
        config = validate_window(payload)
        with self._lock:
            now = self._tick()
            window = self._windows.get(key)
            if window is None:
                self._windows[key] = SlidingWindow(config)
            else:
                window.config = config
                self._expire_window(window, now)
        return config

    def window_state(self, key: Any) -> dict[str, Any]:
        """The same {window, used, remaining} snapshot a check at this effective moment would see."""
        key = validate_key(key)
        with self._lock:
            now = self._tick()
            window = self._windows.get(key)
            if window is None:
                raise LimitNotFound(f"no window configured for {key!r}")
            self._expire_window(window, now)
            used = len(window.events)
            return {"window": window.config.as_json(), "used": used,
                    "remaining": window.config.max_events - used}

    def window_check(self, key: Any) -> dict[str, Any]:
        """Atomically admit one request into the sliding window, counting successful admissions only.

        Stale events leave first (boundary inclusive). With fewer than max_events live events the
        request is counted at this single effective moment and the returned used already includes
        it; a full window rejects without appending, and Retry-After is the exact wait until the
        earliest live event leaves, which every concurrent reject at this moment computes alike.
        """
        key = validate_key(key)
        with self._lock:
            now = self._tick()
            window = self._windows.get(key)
            if window is None:
                raise LimitNotFound(f"no window configured for {key!r}")
            self._expire_window(window, now)
            config = window.config
            used = len(window.events)
            if used >= config.max_events:
                retry_after = window.events[0] + config.window_seconds - now
                raise OverQuota(f"window for {key!r} is full: {used}/{config.max_events} events",
                                retry_after)
            window.events.append(now)
            used += 1
            return {"allowed": True, "used": used, "remaining": config.max_events - used,
                    "limit": config.max_events, "window_seconds": config.window_seconds}


def make_handler(limiter: Limiter) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "quota/0.1"
        protocol_version = "HTTP/1.1"

        def log_message(self, *args: Any) -> None:
            return

        def _not_found(self) -> None:
            self._send(404, {"error": {"code": "not_found"}})

        def __getattr__(self, name: str) -> Any:
            # Any HTTP method without an explicit do_<METHOD> below (PATCH, HEAD, OPTIONS,
            # ...) is unsupported on every route: the same 404 as a path mismatch, resolved
            # before the request body is even looked at.
            if name.startswith("do_"):
                return self._not_found
            raise AttributeError(name)

        def _send(self, status: int, body: dict[str, Any], headers: dict[str, str] | None = None) -> None:
            raw = json.dumps(body).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            for name, value in (headers or {}).items():
                self.send_header(name, value)
            self.end_headers()
            self.wfile.write(raw)

        def _read_json(self) -> Any:
            length = self.headers.get("Content-Length")
            if length is None:
                raise InvalidRequest("Content-Length is required")
            try:
                size = int(length)
            except ValueError as error:
                raise InvalidRequest("Content-Length must be an integer") from error
            if size < 0 or size > 1_048_576:
                raise InvalidRequest("Content-Length must be between 0 and 1 MiB")
            if size == 0:
                return None
            try:
                return json.loads(self.rfile.read(size).decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise InvalidRequest("body must be valid UTF-8 JSON") from error

        def _route_segments(self) -> list[str]:
            """Exact public path segments: the non-empty slash-separated parts of the raw
            request target's path.

            Routing reads the target from the request line, not `self.path`: parse_request
            collapses a leading "//" (an open-redirect guard), and routing must still see
            that as the mismatch it is. Anything with an empty segment — doubled slashes,
            a leading or trailing extra slash, the bare root — yields no segments at all,
            so a malformed path can never collapse onto a real route by filtering empties.
            The query string is stripped first; only the ledger route inspects it.
            """
            words = self.requestline.split(" ")
            target = words[1] if len(words) >= 2 else ""
            parts = target.split("?")[0].split("/")
            segments = parts[1:] if parts[0] == "" else []
            if any(segment == "" for segment in segments):
                return []
            return segments

        def _ledger_event_limit(self) -> int:
            """Parse the ledger endpoint's sole, optional `events` query parameter.

            Anything but exactly one integer literal in 1..1000 named `events` — an unknown
            parameter, a repeat, a blank/garbled value, a stray or empty pair — is invalid_request.
            Only called once the /v1/ledgers/{key} route itself matches.
            """
            query = self.path.split("?", 1)[1] if "?" in self.path else ""
            if query == "":
                return 100
            event_limit: int | None = None
            for pair in query.split("&"):
                name, separator, value = pair.partition("=")
                if not separator or name != "events" or not value \
                        or not all("0" <= char <= "9" for char in value):
                    raise InvalidRequest("query string must be events=<integer 1..1000> and nothing else")
                if event_limit is not None:
                    raise InvalidRequest("events may be given at most once")
                event_limit = int(value)
                if not 1 <= event_limit <= 1000:
                    raise InvalidRequest("events must be an integer between 1 and 1000")
            assert event_limit is not None
            return event_limit

        def do_GET(self) -> None:  # noqa: N802
            try:
                parts = self._route_segments()
                if parts == ["health"]:
                    return self._send(200, {"status": "ok"})
                if len(parts) == 3 and parts[:2] == ["v1", "limits"]:
                    return self._send(200, limiter.state(parts[2]))
                if len(parts) == 3 and parts[:2] == ["v1", "ledgers"]:
                    # Route matched first: query validation now beats the key's 404, just as body
                    # validation precedes quota classification everywhere else.
                    event_limit = self._ledger_event_limit()
                    return self._send(200, limiter.ledger(parts[2], event_limit))
                if len(parts) == 3 and parts[:2] == ["v1", "windows"]:
                    return self._send(200, limiter.window_state(parts[2]))
                return self._send(404, {"error": {"code": "not_found"}})
            except QuotaError as error:
                return self._send(error.status, {"error": {"code": error.code, "message": str(error)}})
            except Exception:
                return self._send(500, {"error": {"code": "internal_error"}})

        def do_PUT(self) -> None:  # noqa: N802
            try:
                parts = self._route_segments()
                if len(parts) != 3 or parts[:2] != ["v1", "limits"]:
                    if len(parts) == 3 and parts[:2] == ["v1", "windows"]:
                        window = limiter.configure_window(parts[2], self._read_json())
                        return self._send(200, {"key": parts[2], "window": window.as_json()})
                    return self._send(404, {"error": {"code": "not_found"}})
                limit = limiter.configure(parts[2], self._read_json())
                return self._send(200, {"key": parts[2], "limit": limit.as_json()})
            except QuotaError as error:
                return self._send(error.status, {"error": {"code": error.code, "message": str(error)}})
            except Exception:
                return self._send(500, {"error": {"code": "internal_error"}})

        def do_POST(self) -> None:  # noqa: N802
            try:
                parts = self._route_segments()
                if parts == ["v1", "check"]:
                    body = self._read_json()
                    if not isinstance(body, dict) or set(body) - {"key", "cost"}:
                        raise InvalidRequest("body must be {\"key\": <string>, \"cost\": <integer>}")
                    result = limiter.check(body.get("key"), body.get("cost", 1))
                    return self._send(200, result)
                if parts == ["v1", "hierarchies", "check"]:
                    body = self._read_json()
                    if not isinstance(body, dict) or "keys" not in body or set(body) - {"keys", "cost"}:
                        raise InvalidRequest('body must be {"keys": [<string>, ...], "cost": <integer>}')
                    result = limiter.hierarchy_check(body["keys"], body.get("cost", 1))
                    return self._send(200, result)
                if parts == ["v1", "hierarchies", "reservations"]:
                    body = self._read_json()
                    if not isinstance(body, dict) or "keys" not in body \
                            or set(body) - {"keys", "cost", "ttl_seconds"}:
                        raise InvalidRequest(
                            'body must be {"keys": [<string>, ...], "cost": <integer>, '
                            '"ttl_seconds": <integer 1..86400>}')
                    ttl = body.get("ttl_seconds", DEFAULT_TTL_SECONDS)
                    result = limiter.hierarchy_reserve(body["keys"], body.get("cost", 1), ttl)
                    return self._send(200, result)
                if len(parts) == 5 and parts[:3] == ["v1", "hierarchies", "reservations"] \
                        and parts[4] == "consume":
                    body = self._read_json()
                    if not isinstance(body, dict) or body:
                        raise InvalidRequest('body must be an empty JSON object {}')
                    result = limiter.hierarchy_consume(parts[3])
                    return self._send(200, result)
                if parts == ["v1", "reservations"]:
                    body = self._read_json()
                    if not isinstance(body, dict) or set(body) - {"key", "cost", "ttl_seconds"}:
                        raise InvalidRequest(
                            'body must be {"key": <string>, "cost": <integer>, "ttl_seconds": <integer 1..86400>}')
                    ttl = body.get("ttl_seconds", DEFAULT_TTL_SECONDS)
                    result = limiter.reserve(body.get("key"), body.get("cost", 1), ttl)
                    return self._send(200, result)
                if len(parts) == 4 and parts[:2] == ["v1", "reservations"] and parts[3] == "consume":
                    body = self._read_json()
                    if not isinstance(body, dict) or body:
                        raise InvalidRequest('body must be an empty JSON object {}')
                    result = limiter.consume(parts[2])
                    return self._send(200, result)
                if len(parts) == 4 and parts[:2] == ["v1", "windows"] and parts[3] == "check":
                    body = self._read_json()
                    if not isinstance(body, dict) or body:
                        raise InvalidRequest('body must be an empty JSON object {}')
                    result = limiter.window_check(parts[2])
                    return self._send(200, result)
                return self._send(404, {"error": {"code": "not_found"}})
            except OverQuota as error:
                # check and reservations share one deterministic ceiling-to-millisecond rule.
                return self._send(error.status, {"error": {"code": error.code, "message": str(error)}},
                                  {"Retry-After": retry_after_header(error.retry_after)})
            except QuotaError as error:
                return self._send(error.status, {"error": {"code": error.code, "message": str(error)}})
            except Exception:
                return self._send(500, {"error": {"code": "internal_error"}})

        def do_DELETE(self) -> None:  # noqa: N802
            try:
                parts = self._route_segments()
                if len(parts) == 3 and parts[:2] == ["v1", "reservations"]:
                    result = limiter.rollback(parts[2])
                    return self._send(200, result)
                if len(parts) == 4 and parts[:3] == ["v1", "hierarchies", "reservations"]:
                    result = limiter.hierarchy_rollback(parts[3])
                    return self._send(200, result)
                return self._send(404, {"error": {"code": "not_found"}})
            except QuotaError as error:
                return self._send(error.status, {"error": {"code": error.code, "message": str(error)}})
            except Exception:
                return self._send(500, {"error": {"code": "internal_error"}})

    return Handler


def serve(host: str = "127.0.0.1", port: int = 18893, now: Callable[[], float] | None = None) -> ThreadingHTTPServer:
    import time

    limiter = Limiter(now or time.monotonic)
    httpd = ThreadingHTTPServer((host, port), make_handler(limiter))
    httpd.limiter = limiter  # type: ignore[attr-defined]
    return httpd


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="multi-tenant rate limiter")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18893)
    args = parser.parse_args()
    server = serve(args.host, args.port)
    print(f"quota service listening on http://{args.host}:{args.port}", flush=True)
    server.serve_forever()
