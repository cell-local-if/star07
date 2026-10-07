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


class RevisionConflict(QuotaError):
    """An If-Match precondition named a revision other than the key's current one."""

    code, status = "revision_conflict", 409


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


@dataclass(frozen=True)
class ConfigureResult:
    """A successful PUT /v1/limits/{key}: the installed limit plus the revision it now carries.

    Revision starts at 1 on a key's first successful creation and gains exactly 1 on every later
    successful PUT — even when the new configuration equals the old one — so it counts successful
    configuration writes, never configuration-value changes.
    """

    limit: Limit
    revision: int


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


@dataclass(frozen=True)
class LeakyBucketConfig:
    """One leaky bucket: at most `capacity` accumulated water, leaking `leak_per_second`."""

    capacity: int
    leak_per_second: float

    def as_json(self) -> dict[str, Any]:
        return {"capacity": self.capacity, "leak_per_second": self.leak_per_second}


@dataclass
class LeakyBucket:
    """One independent leaky bucket's live water level and its last effective moment.

    Lives in its own namespace, shares only the lock, clock and high-water mark with the token
    buckets: a same-named token bucket, window, reservation, ledger and revision neither observe
    nor are touched by anything here, and leaky-bucket traffic is never booked or counted.
    """

    config: LeakyBucketConfig
    level: float
    updated_at: float


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

# The five decision kinds counted by GET /v1/metrics, in response order: single-key instant
# check, hierarchy instant check, single-key reservation creation, cross-layer reservation
# creation, sliding-window check.
DECISION_KINDS = ("check", "hierarchy_check", "reservation", "hierarchy_reservation", "window_check")


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


def validate_leaky_bucket(payload: Any) -> LeakyBucketConfig:
    """The sole accepted shape of a leaky-bucket configuration: exactly capacity and
    leak_per_second, nothing optional or extra.

    capacity is a non-boolean integer in 1..1000000; leak_per_second is a non-boolean number
    strictly greater than 0 and at most 1000000 (integers accepted, booleans never — ``True``
    must not sneak in as 1). Like every body validator this runs before the lock, so a rejected
    PUT creates no bucket and never advances the clock watermark.
    """
    if not isinstance(payload, dict):
        raise InvalidRequest("body must be a JSON object")
    extra = set(payload) - {"capacity", "leak_per_second"}
    if extra:
        raise InvalidRequest(f"unknown fields: {sorted(extra)}")
    capacity = payload.get("capacity")
    if not isinstance(capacity, int) or isinstance(capacity, bool) or not 1 <= capacity <= 1_000_000:
        raise InvalidRequest("capacity must be an integer between 1 and 1000000")
    rate = payload.get("leak_per_second")
    if not isinstance(rate, (int, float)) or isinstance(rate, bool):
        raise InvalidRequest("leak_per_second must be a positive number no greater than 1000000")
    # Python's JSON parser accepts the non-standard NaN/Infinity literals; neither is a usable rate.
    if isinstance(rate, float) and (math.isnan(rate) or math.isinf(rate)):
        raise InvalidRequest("leak_per_second must be a finite number")
    if not 0 < rate <= 1_000_000:
        raise InvalidRequest("leak_per_second must be a positive number no greater than 1000000")
    return LeakyBucketConfig(int(capacity), float(rate))


def validate_if_match(header: Any) -> int:
    """The sole accepted shape of PUT's optional optimistic-concurrency precondition.

    Exactly one entity tag: a double-quoted decimal positive integer with no whitespace or other
    symbols inside or out — ``"3"``, never ``3``, ``*``, ``W/"3"`` or a list. A missing header is
    handled by the caller as "no precondition"; this function validates only a header that is
    present, and like body validation it runs before the lock so a malformed precondition can
    never partially configure a key or advance the clock watermark.
    """
    if not isinstance(header, str):
        raise InvalidRequest('If-Match must be a quoted positive integer, e.g. "3"')
    if len(header) < 3 or not header.startswith('"') or not header.endswith('"'):
        raise InvalidRequest('If-Match must be a quoted positive integer, e.g. "3"')
    digits = header[1:-1]
    # Canonical decimal only: no sign, whitespace, symbol, and no leading zero ("01" is not the
    # shape the server's own ETags ever take, so it names no revision rather than aliasing 1).
    if not digits or not all("0" <= char <= "9" for char in digits):
        raise InvalidRequest('If-Match must be a quoted positive integer, e.g. "3"')
    if len(digits) > 1 and digits[0] == "0":
        raise InvalidRequest('If-Match must be a quoted positive integer, e.g. "3"')
    revision = int(digits)
    if revision < 1:
        raise InvalidRequest('If-Match must be a quoted positive integer, e.g. "3"')
    return revision


def etag_header(revision: int) -> str:
    """The ETag for a configuration revision: the decimal revision wrapped in double quotes."""
    return f'"{revision}"'


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
        # Configuration revision per key: 1 at first successful creation, +1 per later successful
        # PUT (even when the configuration values are unchanged). It exists exactly while a limit
        # does and is the only state the ETag/If-Match optimistic-concurrency protocol observes.
        self._revisions: dict[str, int] = {}
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
        # Independent leaky buckets, likewise in their own namespace: a leaky bucket named like a
        # token bucket or window shares neither level nor configuration, and its traffic never
        # reaches a ledger, a revision or the five decision counters.
        self._leaky_buckets: dict[str, LeakyBucket] = {}
        # Highest clock reading ever observed; the effective moment never moves below it.
        self._watermark = float("-inf")
        # Cumulative per-kind decision counters, process-local and born with the Limiter: one
        # decision increments exactly one of allowed/over_quota, always inside the lock, so
        # concurrent decisions can neither lose nor double an increment. Nothing else (consume,
        # rollback, expiry settling, reads, validation failures, 404s) ever touches them, and a
        # restart resets them to zero — there is no persistence promise.
        self._decisions: dict[str, dict[str, int]] = {
            kind: {"allowed": 0, "over_quota": 0} for kind in DECISION_KINDS
        }

    def _record_decision(self, kind: str, outcome: str) -> None:
        """Count one quota decision. Caller holds the lock and the decision has just been made."""
        self._decisions[kind][outcome] += 1

    def metrics(self) -> dict[str, Any]:
        """Read-only cumulative decision counts since this Limiter was created.

        Only the lock is taken so the copy is consistent with concurrent decisions: no clock
        sample, no watermark advance, no reservation settling, no refill, no ledger or window
        mutation — a stalled or regressed clock cannot change what a read returns.
        """
        with self._lock:
            return {"metrics": {"decisions": {
                kind: dict(counts) for kind, counts in self._decisions.items()}}}

    def _tick(self) -> float:
        """Sample the clock once and clamp to the high-water mark. Caller holds the lock."""
        reading = self._now()
        if reading > self._watermark:
            self._watermark = reading
        return self._watermark

    def configure(self, key: Any, payload: Any,
                  expected_revision: int | None = None) -> ConfigureResult:
        """Create or hot-reconfigure one key's bucket; optionally under an If-Match precondition.

        `key`, the body and (when present) `expected_revision` are all validated before the lock,
        so either rejection is 400 with no partial state. Inside the one critical section the
        existence test, the revision comparison and the whole install run atomically: a legal
        If-Match on an unconfigured key is 404 and creates nothing; a revision that does not equal
        the key's current one is 409 *before* the clock is sampled or anything mutates, so tokens,
        used, reservations, ledgers, decision counts and the high-water mark are all untouched.
        A missing precondition keeps the baseline unconditional write. Every successful write —
        creation or update, even an identical re-PUT — advances the key's revision by exactly 1.
        """
        key = validate_key(key)
        limit = validate_limit(payload)
        with self._lock:
            if key not in self._limits:
                if expected_revision is not None:
                    # Not even the clock is sampled: a conditional write to an unconfigured key
                    # leaves the watermark and every other piece of state exactly as it was.
                    raise LimitNotFound(f"no limit configured for {key!r}")
                now = self._tick()
                self._limits[key] = limit
                self._buckets[key] = Bucket(limit.capacity, now)
                self._revisions[key] = 1
                return ConfigureResult(limit, 1)
            if expected_revision is not None and self._revisions[key] != expected_revision:
                raise RevisionConflict("If-Match revision does not match current configuration")
            # Hot reconfiguration of a live bucket. The wait since the bucket's last effective
            # moment is first refilled at the OLD rate (capped at the OLD capacity); only then is
            # the new configuration installed, so the new rate governs nothing before this moment
            # and a capacity increase never conjures a full bucket.
            now = self._tick()
            self._refill(key, now)
            self._limits[key] = limit
            # Due holds settle after the old-rate refill and are credited against the NEW capacity
            # (their _refill calls are no-ops: updated_at is already this effective moment).
            self._expire_due(key, now)
            bucket = self._buckets[key]
            bucket.tokens = min(float(limit.capacity), bucket.tokens)
            bucket.updated_at = now
            self._revisions[key] += 1
            return ConfigureResult(limit, self._revisions[key])

    def limit(self, key: str) -> Limit:
        with self._lock:
            if key not in self._limits:
                raise LimitNotFound(f"no limit configured for {key!r}")
            return self._limits[key]

    def state_snapshot(self, key: str) -> tuple[dict[str, Any], int]:
        """The GET /v1/limits/{key} body and the current revision, from one locked snapshot.

        Both values are produced in the same critical section, so the ETag a caller sees can
        never name a revision other than the one the returned state belongs to — a PUT landing
        between a separate state read and a separate revision read is impossible.
        """
        with self._lock:
            now = self._tick()
            limit = self.limit(key)
            self._expire_due(key, now)
            bucket = self._refill(key, now)
            body = {"limit": limit.as_json(), "remaining": int(bucket.tokens),
                    "used": sum(bucket.cost_history)}
            return body, self._revisions[key]

    def state(self, key: str) -> dict[str, Any]:
        return self.state_snapshot(key)[0]

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
                self._record_decision("check", "allowed")
                return {"allowed": True, "remaining": int(bucket.tokens), "capacity": limit.capacity}
            deficit = cost - bucket.tokens
            retry_after = deficit / limit.refill_per_second
            self._record_decision("check", "over_quota")
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
                self._record_decision("hierarchy_check", "over_quota")
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
            # The whole hierarchy request is one decision, however many layers it spans.
            self._record_decision("hierarchy_check", "allowed")
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
                self._record_decision("hierarchy_reservation", "over_quota")
                raise OverQuota(
                    f"hierarchy layers short of cost {cost}: "
                    + ", ".join(f"{key!r} has {bucket.tokens:.3f}" for key, bucket in shortfalls),
                    retry_after)
            for bucket in buckets:
                bucket.tokens -= cost
            reservation = HierarchyReservation(uuid.uuid4().hex, list(keys), cost, now, ttl_seconds)
            self._hierarchy_reservations[reservation.reservation_id] = reservation
            self._record_decision("hierarchy_reservation", "allowed")
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
                self._record_decision("reservation", "over_quota")
                raise OverQuota(f"key {key!r} has {bucket.tokens:.3f} tokens, needs {cost}",
                                deficit / limit.refill_per_second)
            bucket.tokens -= cost
            reservation = Reservation(uuid.uuid4().hex, key, cost, now, ttl_seconds)
            self._reservations[reservation.reservation_id] = reservation
            self._record_decision("reservation", "allowed")
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
                self._record_decision("window_check", "over_quota")
                raise OverQuota(f"window for {key!r} is full: {used}/{config.max_events} events",
                                retry_after)
            window.events.append(now)
            self._record_decision("window_check", "allowed")
            used += 1
            return {"allowed": True, "used": used, "remaining": config.max_events - used,
                    "limit": config.max_events, "window_seconds": config.window_seconds}

    def _leak(self, bucket: LeakyBucket, now: float) -> LeakyBucket:
        """Drain one leaky bucket at its CURRENT rate for the wait since its last effective moment.

        The wait is clamped at zero (the watermark already turns a regressed reading into a
        zero-length wait), the drained water is floored at zero, and the bucket is stamped with
        this effective moment so the interval is counted exactly once — a stalled clock leaks
        nothing and a recovered clock never recounts the regressed interval. Caller holds the
        lock and has sampled the clock once for the whole operation.
        """
        elapsed = max(0.0, now - bucket.updated_at)
        bucket.level = max(0.0, bucket.level - elapsed * bucket.config.leak_per_second)
        bucket.updated_at = now
        return bucket

    @staticmethod
    def _leaky_bucket_body(key: str, bucket: LeakyBucket) -> dict[str, Any]:
        """The fixed four-field view shared by PUT and GET: the level is the post-leak occupancy
        at this effective moment, rounded to three decimals; capacity and rate are configuration."""
        return {"key": key, "level": round(bucket.level, 3),
                "capacity": bucket.config.capacity,
                "leak_per_second": bucket.config.leak_per_second}

    def configure_leaky_bucket(self, key: Any, payload: Any) -> dict[str, Any]:
        """Create or hot-reconfigure one leaky bucket, isolated from every other subsystem.

        `key` and the body are validated before the lock, so a rejected PUT creates no bucket and
        never samples the clock. A new bucket is born empty (level 0) at this effective moment.
        On re-PUT the wait since the bucket's last effective moment first drains at the OLD rate,
        floored at zero; only then is the new configuration installed and the surviving water
        capped at the NEW capacity. The new rate never acts backwards on the pre-PUT wait. No
        ledger entry, no revision and no decision count results from this write.
        """
        key = validate_key(key)
        config = validate_leaky_bucket(payload)
        with self._lock:
            now = self._tick()
            bucket = self._leaky_buckets.get(key)
            if bucket is None:
                bucket = LeakyBucket(config, 0.0, now)
                self._leaky_buckets[key] = bucket
            else:
                self._leak(bucket, now)
                bucket.config = config
                bucket.level = min(float(config.capacity), bucket.level)
            return self._leaky_bucket_body(key, bucket)

    def leaky_bucket_state(self, key: Any) -> dict[str, Any]:
        """The GET view: leak first at the current rate, then report the four fields."""
        key = validate_key(key)
        with self._lock:
            now = self._tick()
            bucket = self._leaky_buckets.get(key)
            if bucket is None:
                raise LimitNotFound(f"no leaky bucket configured for {key!r}")
            self._leak(bucket, now)
            return self._leaky_bucket_body(key, bucket)

    def leaky_bucket_check(self, key: Any, cost: Any = 1) -> dict[str, Any]:
        """Admit one pour into the leaky bucket, or reject it without changing the level.

        Validation runs before the lock. Inside the one critical section the clock is sampled
        once and the bucket drains at its current leak_per_second for the wait since its last
        effective moment. When level + cost fits within capacity the cost is added and the
        post-admission occupancy (three decimals) is returned; otherwise nothing is added and the
        429's Retry-After is the exact wait to drain the level + cost - capacity gap, which the
        HTTP layer ceilings to whole milliseconds — every concurrent reject at this moment
        computes the same value. Like window checks this decision is NOT one of the five counted
        kinds and never reaches a ledger.
        """
        key = validate_key(key)
        cost = validate_cost(cost)
        with self._lock:
            now = self._tick()
            bucket = self._leaky_buckets.get(key)
            if bucket is None:
                raise LimitNotFound(f"no leaky bucket configured for {key!r}")
            self._leak(bucket, now)
            if bucket.level + cost <= bucket.config.capacity:
                bucket.level += cost
                return {"allowed": True, "cost": cost,
                        "level": round(bucket.level, 3),
                        "capacity": bucket.config.capacity}
            deficit = bucket.level + cost - bucket.config.capacity
            raise OverQuota(
                f"leaky bucket {key!r} holds {bucket.level:.3f}, cannot pour {cost}",
                deficit / bucket.config.leak_per_second)


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

        def _query_string(self) -> str:
            """The raw query string of the request target ("" when absent or bare), read from
            the request line exactly as routing does."""
            words = self.requestline.split(" ")
            target = words[1] if len(words) >= 2 else ""
            return target.split("?", 1)[1] if "?" in target else ""

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
                    state, revision = limiter.state_snapshot(parts[2])
                    return self._send(200, state, {"ETag": etag_header(revision)})
                if len(parts) == 3 and parts[:2] == ["v1", "ledgers"]:
                    # Route matched first: query validation now beats the key's 404, just as body
                    # validation precedes quota classification everywhere else.
                    event_limit = self._ledger_event_limit()
                    return self._send(200, limiter.ledger(parts[2], event_limit))
                if len(parts) == 3 and parts[:2] == ["v1", "windows"]:
                    return self._send(200, limiter.window_state(parts[2]))
                if len(parts) == 3 and parts[:2] == ["v1", "leaky-buckets"]:
                    # Like /v1/metrics this read route names no query parameters: any query string
                    # is invalid_request, decided before the limiter call (and hence before the
                    # clock is sampled or the watermark advances).
                    if self._query_string() != "":
                        raise InvalidRequest("GET /v1/leaky-buckets/{key} takes no query parameters")
                    return self._send(200, limiter.leaky_bucket_state(parts[2]))
                if parts == ["v1", "metrics"]:
                    # Read-only cumulative counters. The route takes no query parameters at all:
                    # any non-empty query string is invalid_request, decided before the read.
                    if self._query_string() != "":
                        raise InvalidRequest("GET /v1/metrics takes no query parameters")
                    return self._send(200, limiter.metrics())
                return self._send(404, {"error": {"code": "not_found"}})
            except QuotaError as error:
                return self._send(error.status, {"error": {"code": error.code, "message": str(error)}})
            except Exception:
                return self._send(500, {"error": {"code": "internal_error"}})

        def do_PUT(self) -> None:  # noqa: N802
            try:
                parts = self._route_segments()
                if len(parts) == 3 and parts[:2] == ["v1", "leaky-buckets"]:
                    # This route takes no query parameters: reject before the body is read so a
                    # garbled body can never mask the 400, and before any state could change.
                    if self._query_string() != "":
                        raise InvalidRequest("PUT /v1/leaky-buckets/{key} takes no query parameters")
                    return self._send(200, limiter.configure_leaky_bucket(
                        parts[2], self._read_json()))
                if len(parts) != 3 or parts[:2] != ["v1", "limits"]:
                    if len(parts) == 3 and parts[:2] == ["v1", "windows"]:
                        window = limiter.configure_window(parts[2], self._read_json())
                        return self._send(200, {"key": parts[2], "window": window.as_json()})
                    return self._send(404, {"error": {"code": "not_found"}})
                # The precondition belongs to this one route: header and body are both validated
                # before the lock (a repeated If-Match line is a list of tags and is never the one
                # accepted shape), so neither failure can partially configure anything.
                if_match_values = self.headers.get_all("If-Match")
                if if_match_values is not None:
                    if len(if_match_values) != 1:
                        raise InvalidRequest('If-Match must be a quoted positive integer, e.g. "3"')
                    expected_revision: int | None = validate_if_match(if_match_values[0])
                else:
                    expected_revision = None
                result = limiter.configure(parts[2], self._read_json(), expected_revision)
                return self._send(200, {"key": parts[2], "limit": result.limit.as_json()},
                                  {"ETag": etag_header(result.revision)})
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
                if len(parts) == 4 and parts[:2] == ["v1", "leaky-buckets"] and parts[3] == "check":
                    # No query parameters on this route either: reject before reading the body.
                    if self._query_string() != "":
                        raise InvalidRequest(
                            "POST /v1/leaky-buckets/{key}/check takes no query parameters")
                    # The key rides in the path, so the body is an object carrying at most a cost;
                    # {} (cost omitted, defaulting to 1) is the empty-object case. Anything else —
                    # arrays, scalars, unknown fields, malformed JSON — is invalid_request and the
                    # request never reaches the lock.
                    body = self._read_json()
                    if not isinstance(body, dict) or set(body) - {"cost"}:
                        raise InvalidRequest('body must be {} or {"cost": <integer>}')
                    result = limiter.leaky_bucket_check(parts[2], body.get("cost", 1))
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
