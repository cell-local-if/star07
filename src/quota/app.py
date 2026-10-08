"""Multi-tenant rate limiting: the baseline service.

Public contract is README.md. Time is injected everywhere so the refill maths is testable and deterministic.
"""
from __future__ import annotations

import json
import math
import threading
import uuid
from collections import deque
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


class IdempotencyConflict(QuotaError):
    """A stored Idempotency-Key was replayed with request params other than the first creation's."""

    code, status = "idempotency_conflict", 409


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
    # Cumulative accepted spend on this key since the Limiter was created: a single running
    # total, not a per-event list, so a hot key's memory stays constant no matter how long it
    # lives. Booked exactly where the ledger books (same lines, same critical section), so this
    # always equals the ledger's totals.accepted_cost.
    used: int = 0


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


# How many of a key's most recent ledger events are kept for the `events` tail: exactly the
# maximum the GET /v1/ledgers/{key} `events` parameter may ask for, so any legal query is
# answered in full while a hot key's memory stays bounded. Totals are cumulative counters and
# are NOT bounded by this — they cover every booking since the Limiter was created.
LEDGER_EVENT_KEEP = 1000


@dataclass
class Ledger:
    """One key's bounded billing ledger: the newest LEDGER_EVENT_KEEP events plus lifetime totals.

    The event detail is a bounded tail — appending past LEDGER_EVENT_KEEP drops the oldest entry
    — while accepted_count/accepted_cost are running totals over EVERY accepted booking since
    the Limiter was created, trimmed events included. seq equals the event's accepted_count
    ordinal (both gain exactly 1 per booking, in the same critical section), so seq stays dense
    from 1 with no gaps or duplicates, and the surviving tail is always the contiguous most
    recent events: the first retained seq is accepted_count - len(events) + 1.
    """

    events: deque[LedgerEvent] = field(default_factory=lambda: deque(maxlen=LEDGER_EVENT_KEEP))
    accepted_count: int = 0
    accepted_cost: int = 0


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
    """Accepted admissions for one window key, oldest first; nothing else ever lands here.

    Each admission is an (effective_at, cost) pair: a check may register any integer cost in
    1..max_events, so the live occupancy is the COST SUM of the surviving pairs rather than
    their count. Same-moment admissions keep their relative append order and expire together
    at effective_at + window_seconds.
    """

    config: WindowConfig
    events: list[tuple[float, int]] = field(default_factory=list)


@dataclass
class WindowReservation:
    """One capacity hold inside a sliding window: `cost` of the window's occupancy budget held
    until consumed, rolled back, or expired.

    Lives in its own registry (keyed by reservation_id, carrying its window key), shares the
    windows' lock, clock watermark and lazy expiry rule (created_at + ttl_seconds, boundary
    inclusive), and is never booked to a ledger, counted in metrics or tied to a revision. While
    live it counts toward the window's used exactly like an admitted event; unlike an event it
    leaves at created_at + ttl_seconds rather than effective_at + window_seconds, and expiring
    forms no event of any kind.
    """

    reservation_id: str
    key: str
    cost: int
    created_at: float
    ttl_seconds: int

    def expires_at(self) -> float:
        return self.created_at + self.ttl_seconds


@dataclass(frozen=True)
class LeakyBucketConfig:
    """One leaky bucket: at most `capacity` accumulated water, leaking `leak_per_second`."""

    capacity: int
    leak_per_second: float

    def as_json(self) -> dict[str, Any]:
        return {"capacity": self.capacity, "leak_per_second": self.leak_per_second}


@dataclass(frozen=True)
class IdempotentReservation:
    """One Idempotency-Key binding for POST /v1/reservations: the exact first-request params
    and the verbatim first 200 response.

    The binding belongs to creation only and lives in its own registry: it outlives the
    reservation it created (expiry, rollback and consume leave it untouched), is never booked
    to a ledger, counted in metrics or visible on any other route, and exists only in process
    memory. Nothing binds the key unless the first creation fully succeeded.
    """

    key: str
    cost: int
    ttl_seconds: int
    response: dict[str, Any]


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


IDEMPOTENCY_KEY_MAX_LENGTH = 128


def validate_idempotency_key(value: Any) -> str:
    """The sole accepted shape of POST /v1/reservations' optional Idempotency-Key header.

    A present header must be exactly one value of 1..128 non-whitespace ASCII characters; an
    empty value, a repeated header line (handled by the caller), a whitespace character or any
    non-ASCII byte is invalid_request. Like If-Match and body validation this runs before the
    lock (and, at the HTTP layer, before the body is read), so a malformed key can never deduct
    tokens, create a reservation, advance the clock watermark or bind anything.
    """
    if not isinstance(value, str) or not 1 <= len(value) <= IDEMPOTENCY_KEY_MAX_LENGTH:
        raise InvalidRequest(
            "Idempotency-Key must be 1 to 128 non-whitespace ASCII characters")
    for char in value:
        if ord(char) > 127 or char.isspace():
            raise InvalidRequest(
                "Idempotency-Key must be 1 to 128 non-whitespace ASCII characters")
    return value


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
        # Idempotency-Key bindings for single-key reservation creation only: header value ->
        # the exact first-request params and verbatim first 200 response. A binding is written
        # only by a fully successful creation and is never removed by rollback, consume or
        # expiry: a late retry still replays the original creation and can never hold tokens a
        # second time. Process-local only, like everything else here — a restart forgets all
        # keys, and nothing is shared across instances.
        self._idempotent_reservations: dict[str, IdempotentReservation] = {}
        # Confirmed holds leave the active registry for good; the value is the exact first consume
        # response, replayed verbatim for idempotent retries (used is never booked a second time).
        self._consumed: dict[str, dict[str, Any]] = {}
        # Cross-layer holds and their confirmed-consume replays, in namespaces of their own: a
        # reservation_id created here never resolves through the single-key endpoints and vice
        # versa (cross-resource identifiers are 404, not confusion).
        self._hierarchy_reservations: dict[str, HierarchyReservation] = {}
        self._hierarchy_consumed: dict[str, dict[str, Any]] = {}
        # Bounded billing ledger per key: the newest LEDGER_EVENT_KEEP event details plus
        # cumulative totals over the key's whole lifetime. Entries are appended inside the lock
        # at booking time, so seq is dense and gap-free even under concurrent settling; the tail
        # trims itself (read-only views take a further tail slice) while totals never shrink.
        self._ledgers: dict[str, Ledger] = {}
        # Independent sliding windows, keyed in their own namespace: a window named like a bucket
        # shares neither history nor accounting with it, and window admissions never touch a ledger.
        self._windows: dict[str, SlidingWindow] = {}
        # Window capacity holds and their confirmed-consume replays, likewise in namespaces of
        # their own: a window reservation id never resolves through the token-bucket or hierarchy
        # reservation endpoints (nor theirs through the window ones), a hold is never booked to a
        # ledger, counted in the five decision kinds or tied to a revision, and a confirmed hold
        # leaves the active registry for good (the replay snapshot alone survives).
        self._window_reservations: dict[str, WindowReservation] = {}
        self._window_consumed: dict[str, dict[str, Any]] = {}
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
                    "used": bucket.used}
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

        Totals and seq are computed inside the same critical section as the booking, so concurrent
        bookings can neither skip nor reuse a number and the running totals never under- or
        over-count. The deque's maxlen drops the oldest detail entry past LEDGER_EVENT_KEEP; the
        totals are untouched by trimming, so they still cover every booking since creation.
        """
        ledger = self._ledgers.setdefault(key, Ledger())
        ledger.accepted_count += 1
        ledger.accepted_cost += cost
        ledger.events.append(LedgerEvent(ledger.accepted_count, source, reservation_id, cost,
                                         int(remaining), capacity, effective_at))

    def check(self, key: Any, cost: Any) -> dict[str, Any]:
        # Same key rule as configure/reserve, enforced before the lock: a rejected check
        # never samples the clock, refills, deducts, or posts a ledger event.
        key = validate_key(key)
        cost = validate_cost(cost)
        with self._lock:
            # A legal cost that exceeds the key's CURRENT capacity can never be admitted, however
            # long the caller waits: reject it as invalid_request instead of hinting a finite
            # Retry-After for an unsatisfiable request. The comparison reads the same locked
            # configuration a concurrent PUT installs, so each request sees exactly one capacity —
            # before or after the reconfigure, never a torn intermediate. This branch runs before
            # the clock is sampled: no watermark advance, no expiry settle, no refill, no ledger
            # event, no decision count. An unconfigured key has no capacity to compare against,
            # so it keeps the baseline 404 (clock sampled exactly as before).
            limit = self._limits.get(key)
            if limit is not None and cost > limit.capacity:
                raise InvalidRequest(
                    f"cost {cost} exceeds capacity {limit.capacity} for key {key!r}")
            now = self._tick()
            limit = self.limit(key)
            self._expire_due(key, now)
            bucket = self._refill(key, now)
            if bucket.tokens >= cost:
                bucket.tokens -= cost
                bucket.used += cost
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
        the lock, then configuration is confirmed for every layer in input order and the
        unsatisfiable-cost boundary is checked against the locked configuration, all before the
        clock is sampled. Only then is every layer's due reservations settled and its bucket
        refilled at this one moment, and only then is affordability judged. The decision is
        all-or-nothing — either every layer can pay and all are deducted and booked together, or
        none is touched (beyond the lazy expiry settle every entry point performs) and the
        rejection hints the longest wait any insufficient layer needs to cover its own deficit.
        """
        keys = validate_keys(keys)
        cost = validate_cost(cost)
        with self._lock:
            # Configuration is confirmed for every layer, in input order, before the clock is
            # sampled or anything settles or changes: the 404 names the first unconfigured key
            # and no layer's state changes.
            for key in keys:
                if key not in self._limits:
                    raise LimitNotFound(f"no limit configured for {key!r}")
            # A legal cost above any layer's CURRENT capacity is unsatisfiable on that layer for
            # however long the caller waits: the request is invalid_request, naming the first
            # layer in input order whose capacity is below the cost and that capacity, rather
            # than a 429 hinting a finite wait. Like check()'s boundary this reads the same
            # locked configuration a concurrent PUT installs (every request sees exactly one
            # capacity set, never a torn mixture) and runs before the clock is sampled: no
            # watermark advance, no expiry settle, no refill, no deduction, no ledger event and
            # no decision count. cost exactly equal to every capacity stays a legal request and
            # proceeds to the ordinary availability judgement below.
            for key in keys:
                capacity = self._limits[key].capacity
                if cost > capacity:
                    raise InvalidRequest(
                        f"cost {cost} exceeds capacity {capacity} for key {key!r}")
            now = self._tick()
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
                bucket.used += cost
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
        hierarchy_check: validation happens before the lock; inside the one critical section
        configuration is first confirmed for every layer in input order (the 404 names the first
        unconfigured key) and the unsatisfiable-cost boundary is checked against the locked
        configuration, all before the clock is sampled. Only then are each layer's due holds —
        single-key and cross-layer alike — settled and its bucket refilled at this one moment.
        Either every layer can pay and all are deducted together, or none is touched and the
        rejection hints the longest wait any insufficient layer needs to cover its own deficit.
        The hold books no used and no ledger event; that happens only at consume time. It lapses
        `ttl_seconds` of monotonic time after creation; reconfiguring a layer never extends it.
        """
        keys = validate_keys(keys)
        cost = validate_cost(cost)
        ttl_seconds = validate_ttl(ttl_seconds)
        with self._lock:
            # Configuration is confirmed in input order before the clock is sampled: the 404
            # names the first unconfigured key and no layer's state changes.
            for key in keys:
                if key not in self._limits:
                    raise LimitNotFound(f"no limit configured for {key!r}")
            # Same unsatisfiable-cost boundary as the other public entries: a legal cost above
            # any layer's CURRENT capacity can never be held on that layer, so the request is
            # invalid_request naming the first such layer (and its capacity) instead of a 429
            # with a finite Retry-After. The whole check reads one locked configuration set —
            # before or after a concurrent PUT, never a mixture — and runs before the clock is
            # sampled, so no watermark, settle, refill, deduction, ledger event, reservation or
            # decision count results from the rejection. cost equal to every capacity proceeds
            # to the ordinary availability judgement.
            for key in keys:
                capacity = self._limits[key].capacity
                if cost > capacity:
                    raise InvalidRequest(
                        f"cost {cost} exceeds capacity {capacity} for key {key!r}")
            now = self._tick()
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

    def reserve(self, key: Any, cost: Any, ttl_seconds: Any = DEFAULT_TTL_SECONDS,
                idempotency_key: Any = None) -> dict[str, Any]:
        """Atomically hold `cost` tokens, exactly as check() judges them, without booking them as used.

        The hold lapses `ttl_seconds` of monotonic time after creation; reconfiguring the key never
        extends it. Expired holds are settled lazily at the start of this call (see _expire_due).

        A non-None `idempotency_key` turns this call into a creation-idempotent one: the key is
        validated first (mirroring the HTTP rule that the header is rejected before the body is
        read), and inside the one critical section the lookup runs FIRST — before the clock is
        sampled, before anything settles, refills or deducts. A stored binding whose params
        match replays the exact first 200 response (no second hold, no decision count); a stored
        binding whose key/cost/ttl differs is 409 idempotency_conflict and changes nothing (not
        even the watermark). Only a fully successful creation binds the key: a 400/404/429
        leaves the key free so a later legal retry is a genuine first creation.
        """
        if idempotency_key is not None:
            idempotency_key = validate_idempotency_key(idempotency_key)
        key = validate_key(key)
        cost = validate_cost(cost)
        ttl_seconds = validate_ttl(ttl_seconds)
        with self._lock:
            if idempotency_key is not None:
                binding = self._idempotent_reservations.get(idempotency_key)
                if binding is not None:
                    # Same header value: the whole request fingerprint must match. This branch —
                    # replay or conflict — never samples the clock, so it can neither settle a
                    # due hold nor advance the watermark; it also deducts nothing and counts no
                    # decision. The binding outlives its reservation, so a retry after consume,
                    # rollback or expiry still answers with the frozen first creation.
                    if (binding.key, binding.cost, binding.ttl_seconds) != (key, cost, ttl_seconds):
                        raise IdempotencyConflict(
                            "Idempotency-Key was already used with different request parameters")
                    return dict(binding.response)
            # Same unsatisfiable-cost rule as check(): a legal cost above the key's CURRENT
            # capacity can never be held, however long the caller waits, so it is 400
            # invalid_request instead of a 429 hinting a finite wait, and no Retry-After is
            # sent. The comparison reads the same locked configuration a concurrent PUT
            # installs, so each request sees exactly one capacity — before or after the
            # reconfigure, never a torn intermediate — and the branch runs before the clock is
            # sampled: no watermark advance, no expiry settle, no refill, no deduction, no
            # reservation and no decision count. An unconfigured key has no capacity to compare
            # against and keeps the baseline 404 (clock sampled exactly as before).
            limit = self._limits.get(key)
            if limit is not None and cost > limit.capacity:
                raise InvalidRequest(
                    f"cost {cost} exceeds capacity {limit.capacity} for key {key!r}")
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
            result = {"reservation_id": reservation.reservation_id, "key": key, "cost": cost,
                      "remaining": int(bucket.tokens), "capacity": limit.capacity,
                      "ttl_seconds": ttl_seconds}
            if idempotency_key is not None:
                # Bind only on a fully successful creation: every rejection above skipped this
                # line, so the same header value can still drive a later legal first creation.
                self._idempotent_reservations[idempotency_key] = IdempotentReservation(
                    key, cost, ttl_seconds, dict(result))
            return result

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
            bucket.used += reservation.cost
            self._record_event(key, "reservation_consume", reservation.cost, reservation_id,
                               bucket.tokens, limit.capacity, now)
            snapshot = {"reservation_id": reservation_id, "consumed": True,
                        "remaining": int(bucket.tokens), "capacity": limit.capacity,
                        "used": bucket.used}
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
                bucket.used += reservation.cost
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
        seq ascending and is always the contiguous most recent events (once the history outgrows
        LEDGER_EVENT_KEEP the first returned seq is greater than 1), while totals always cover the
        full lifetime of bookings, trimmed events included.
        """
        with self._lock:
            if key not in self._limits:
                raise LimitNotFound(f"no limit configured for {key!r}")
            ledger = self._ledgers.get(key)
            if ledger is None:
                return {"key": key,
                        "totals": {"accepted_count": 0, "accepted_cost": 0},
                        "events": []}
            events = list(ledger.events)
            return {
                "key": key,
                "totals": {"accepted_count": ledger.accepted_count,
                           "accepted_cost": ledger.accepted_cost},
                "events": [event.as_json() for event in events[-event_limit:]],
            }

    def reconciliation(self, key: Any) -> dict[str, Any]:
        """Cross-check one key's booked usage against its ledger, event detail and live holds.

        Unlike ledger() this entry DOES sample the clock: inside the one critical section the
        clock is sampled exactly once (clamped to the watermark, so a stalled or regressed clock
        settles nothing early or late), every due reservation touching the key — single-key and
        cross-layer alike — is settled at that effective moment with the inclusive boundary, and
        the whole snapshot is then read from that same settled state. The settle only releases
        tokens: it books no used, no ledger event and no decision count, exactly as at every
        other entry point. Nothing here ever writes history back: a mismatch is reported, not
        repaired.

        `used` is the bucket's cumulative accepted spend; the ledger totals are the lifetime
        accepted_count/accepted_cost. The retained detail is the bounded readable tail, the
        trimmed detail is the cumulative difference (accepted minus retained), and together they
        cover the accepted totals by construction. `reconciled` is true exactly when used equals
        accepted_cost, retained plus trimmed match the accepted totals in count and in cost, and
        the retained seqs are one contiguous run ending at accepted_count (an empty ledger with
        no bookings at all is trivially consistent). Any drift — used vs ledger cost, a trimmed
        surplus or deficit, a seq gap, a hierarchy hold counted twice — still returns 200 with
        reconciled false and the raw figures; the caller decides what to do about it.
        """
        key = validate_key(key)
        with self._lock:
            if key not in self._limits:
                # The 404 creates nothing and does not even sample the clock: an unknown key
                # leaves the watermark and every other piece of state exactly as it was.
                raise LimitNotFound(f"no limit configured for {key!r}")
            now = self._tick()
            self._expire_due(key, now)
            used = self._buckets[key].used
            ledger = self._ledgers.get(key)
            accepted_count = ledger.accepted_count if ledger is not None else 0
            accepted_cost = ledger.accepted_cost if ledger is not None else 0
            events = list(ledger.events) if ledger is not None else []
            retained_count = len(events)
            retained_cost = sum(event.cost for event in events)
            trimmed_count = accepted_count - retained_count
            trimmed_cost = accepted_cost - retained_cost
            first_seq = events[0].seq if events else None
            last_seq = events[-1].seq if events else None
            # Live holds touching this key, after the settle above: each cross-layer hold counts
            # its cost exactly once however many layers it spans.
            single_key_holds = [reservation for reservation in self._reservations.values()
                                if reservation.key == key and not reservation.rolled_back]
            hierarchy_holds = [reservation for reservation in self._hierarchy_reservations.values()
                               if key in reservation.keys]
            active_cost = (sum(reservation.cost for reservation in single_key_holds)
                           + sum(reservation.cost for reservation in hierarchy_holds))
            if events:
                seq_ok = last_seq == accepted_count and all(
                    event.seq == first_seq + index for index, event in enumerate(events))
            else:
                # No retained detail: consistent only when nothing was ever booked (the trimmed
                # side then covers nothing either). A booked-but-fully-unreadable ledger is a
                # broken chain, not a clean one.
                seq_ok = accepted_count == 0
            reconciled = (used == accepted_cost
                          and trimmed_count >= 0 and trimmed_cost >= 0
                          and retained_count + trimmed_count == accepted_count
                          and retained_cost + trimmed_cost == accepted_cost
                          and seq_ok)
            return {
                "key": key,
                "reconciled": reconciled,
                "usage": {"used": used,
                          "ledger_accepted_count": accepted_count,
                          "ledger_accepted_cost": accepted_cost,
                          "used_minus_ledger_cost": used - accepted_cost},
                "holds": {"active_count": len(single_key_holds) + len(hierarchy_holds),
                          "active_cost": active_cost,
                          "single_key_count": len(single_key_holds),
                          "hierarchy_count": len(hierarchy_holds)},
                "events": {"retained_count": retained_count,
                           "retained_cost": retained_cost,
                           "trimmed_count": trimmed_count,
                           "trimmed_cost": trimmed_cost,
                           "first_seq": first_seq,
                           "last_seq": last_seq},
            }

    def _expire_window(self, window: SlidingWindow, now: float) -> None:
        """Drop every window admission whose age has reached window_seconds at this effective moment.

        An admission stamped at effective_at leaves the window at effective_at + window_seconds,
        i.e. it is stale once effective_at <= now - window_seconds: the boundary is inclusive, so
        an old admission stamped exactly on the edge (including every other admission sharing
        that same moment) settles before admission is judged. Admissions are stamped with the
        non-decreasing watermark, so the surviving timestamps are an ordered tail and one prefix
        drop settles them all. A stalled clock produces an identical cutoff and drops nothing; a
        regressed reading is clamped to the watermark before the cutoff is ever computed.
        Caller holds the lock and supplies the operation's single effective moment.
        """
        cutoff = now - window.config.window_seconds
        events = window.events
        index = 0
        while index < len(events) and events[index][0] <= cutoff:
            index += 1
        if index:
            del events[:index]

    @staticmethod
    def _window_used(window: SlidingWindow) -> int:
        """The live event occupancy: the cost sum of every surviving admission."""
        return sum(cost for _, cost in window.events)

    def _expire_window_reservations(self, key: str, now: float) -> None:
        """Lazy, deterministic expiry settle for one window key's capacity holds.

        A hold lapses at created_at + ttl_seconds (boundary inclusive, the same >= rule the
        window events and the token-bucket holds use) and is simply forgotten: it forms no
        window event, no ledger entry and no metric, and each hold is released at most once
        because it leaves the registry here. Caller holds the lock and supplies the operation's
        single effective moment; a stalled or regressed clock produces the same cutoff as every
        other entry (the watermark), so a due hold is never released early or late.
        """
        due = [rid for rid, reservation in self._window_reservations.items()
               if reservation.key == key and reservation.expires_at() <= now]
        for rid in due:
            del self._window_reservations[rid]

    def _window_live_reservation_cost(self, key: str) -> int:
        """The cost sum of one window key's live (not yet settled) capacity holds.

        Caller holds the lock and has already settled the key's due holds at this effective
        moment, so everything still in the registry for the key is genuinely live.
        """
        return sum(reservation.cost for reservation in self._window_reservations.values()
                   if reservation.key == key)

    def _window_release_wait(self, window: SlidingWindow, key: str, cost: int,
                             now: float) -> float:
        """The wait until the earliest releasing batches free enough room for `cost`.

        Surviving admissions leave at effective_at + window_seconds and live reservations at
        created_at + ttl_seconds; both kinds are merged into one release timeline and walked
        oldest release first, batching same-moment releases (they free together) and
        accumulating released cost until dropping this batch first makes used - released + cost
        fit max_events. The wait ends at that batch's release moment, which every concurrent
        reject at this moment computes identically. Caller holds the lock, has already settled
        every due admission and hold at this effective moment, and has established
        cost <= max_events, so the walk always reaches a sufficient batch (everything live
        eventually releases).
        """
        releases = [(effective_at + window.config.window_seconds, admitted)
                    for effective_at, admitted in window.events]
        releases += [(reservation.expires_at(), reservation.cost)
                     for reservation in self._window_reservations.values()
                     if reservation.key == key]
        releases.sort(key=lambda release: release[0])
        used = sum(released_cost for _, released_cost in releases)
        released = 0
        index = 0
        moment = now
        while index < len(releases):
            moment = releases[index][0]
            while index < len(releases) and releases[index][0] == moment:
                released += releases[index][1]
                index += 1
            if used - released + cost <= window.config.max_events:
                break
        return moment - now

    def configure_window(self, key: Any, payload: Any) -> WindowConfig:
        """Create or replace a window's configuration while keeping its admitted history.

        The new config governs the response and every later admission. Shortening window_seconds
        settles the (now out-of-window) prefix immediately at this effective moment; lowering
        max_events never revokes past admissions — the surviving history stands and only subsequent
        checks are rejected. Live capacity holds are occupancy of exactly this kind: their TTL is
        never extended by a reconfigure, due holds settle here like at every other window entry,
        and a lowered max_events never revokes them. Invalid input is rejected before the lock
        and changes nothing.
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
                self._expire_window_reservations(key, now)
        return config

    def window_state(self, key: Any) -> dict[str, Any]:
        """The same {window, used, remaining} snapshot a cost-1 check at this effective moment would see.

        used is the surviving admissions' cost SUM plus every live capacity hold's cost, not
        their count; remaining is max_events minus that sum (and may be negative when max_events
        was lowered below the live occupancy).
        """
        key = validate_key(key)
        with self._lock:
            now = self._tick()
            window = self._windows.get(key)
            if window is None:
                raise LimitNotFound(f"no window configured for {key!r}")
            self._expire_window(window, now)
            self._expire_window_reservations(key, now)
            used = self._window_used(window) + self._window_live_reservation_cost(key)
            return {"window": window.config.as_json(), "used": used,
                    "remaining": window.config.max_events - used}

    def window_check(self, key: Any, cost: Any = 1) -> dict[str, Any]:
        """Atomically admit one weighted request into the sliding window.

        `cost` defaults to 1 and follows the shared cost rule (a non-boolean integer in
        1..1000000), validated with the key before the lock. Inside the one critical section a
        legal cost above a CONFIGURED window's CURRENT max_events is rejected as invalid_request
        before the clock is sampled: such a request can never fit however long the caller waits,
        so the rejection samples no clock (no watermark advance), evicts no admissions, changes no
        used, sends no Retry-After and counts no window_check decision. An unknown window has no
        max_events to compare against and keeps the baseline 404 (the clock is sampled exactly as
        before and the lookup never creates one).

        After that single sample, every admission whose effective_at plus window_seconds has
        reached this moment (the boundary is inclusive, and same-moment admissions leave together
        as one batch) is dropped, and every capacity hold whose TTL has elapsed is released. The
        live used is the surviving cost sum plus the live holds' cost. When used + cost <=
        max_events the cost is admitted at this single effective moment and the returned used
        already includes it; otherwise the cost enters no history, one over_quota decision is
        counted, and Retry-After is the wait until the oldest same-moment batch of expiring
        admissions or holds whose cumulative released cost first makes room for this request
        frees enough capacity — i.e. until that batch's release moment, which every concurrent
        reject at this moment computes identically. cost exactly equal to max_events fits an
        otherwise empty window.
        """
        key = validate_key(key)
        cost = validate_cost(cost)
        with self._lock:
            # The unsatisfiable boundary is decided before the clock is sampled, exactly as on
            # the token-bucket entries: a configured window whose CURRENT max_events can never
            # hold this legal cost is 400 invalid_request with no eviction, no occupancy change,
            # no metric, no Retry-After and no watermark advance. The comparison reads the same
            # locked configuration a concurrent PUT installs, so each request sees exactly one
            # max_events — before or after the reconfigure, never a torn value. An unknown window
            # has no max_events to compare against and keeps the baseline 404 (clock sampled
            # exactly as before, and the lookup never creates a window).
            window = self._windows.get(key)
            if window is not None and cost > window.config.max_events:
                raise InvalidRequest(
                    f"cost {cost} exceeds max_events {window.config.max_events} for key {key!r}")
            now = self._tick()
            if window is None:
                raise LimitNotFound(f"no window configured for {key!r}")
            self._expire_window(window, now)
            self._expire_window_reservations(key, now)
            config = window.config
            used = self._window_used(window) + self._window_live_reservation_cost(key)
            if used + cost <= config.max_events:
                window.events.append((now, cost))
                self._record_decision("window_check", "allowed")
                used += cost
                return {"allowed": True, "used": used, "remaining": config.max_events - used,
                        "limit": config.max_events, "window_seconds": config.window_seconds}
            # Over quota: the wait until the earliest same-moment release batch (admissions and
            # live holds on one merged timeline) whose cumulative freed cost first makes room.
            retry_after = self._window_release_wait(window, key, cost, now)
            self._record_decision("window_check", "over_quota")
            raise OverQuota(
                f"window for {key!r} is full: {used}/{config.max_events} used, needs {cost}",
                retry_after)

    def window_reserve(self, key: Any, cost: Any = 1,
                       ttl_seconds: Any = DEFAULT_TTL_SECONDS) -> dict[str, Any]:
        """Atomically hold `cost` of a window's occupancy budget, without forming an event.

        Same critical section, same single effective moment and same judgement as window_check:
        key/cost/ttl are validated before the lock; inside it a legal cost above a CONFIGURED
        window's CURRENT max_events is 400 invalid_request before the clock is sampled (no
        watermark advance, no settle, no occupancy change), and an unknown window keeps the
        baseline 404. After the single sample, due admissions and due holds settle first; when
        used + cost fits, the hold is created at this effective moment and counts toward used
        and remaining immediately, so every later check and state read sees it as capacity in
        use. Otherwise nothing is held and the 429's Retry-After is the wait until the earliest
        same-moment release batch — expiring admissions and live holds on one merged timeline —
        first frees enough room for this cost, computed identically by every concurrent reject.

        The hold lapses at created_at + ttl_seconds (boundary inclusive) and is then simply
        released, forming no event; reconfiguring the window never extends the TTL. A window
        reservation is never booked to a ledger, never counted in the five decision kinds and
        never tied to a revision or ETag.
        """
        key = validate_key(key)
        cost = validate_cost(cost)
        ttl_seconds = validate_ttl(ttl_seconds)
        with self._lock:
            # Same unsatisfiable-cost boundary as window_check, decided before the clock is
            # sampled: a legal cost above the CURRENT max_events can never be held, so it is
            # 400 invalid_request with no watermark advance, no settle, no occupancy change, no
            # reservation and no Retry-After. An unknown window has no max_events to compare
            # against and keeps the baseline 404 (clock sampled exactly as before).
            window = self._windows.get(key)
            if window is not None and cost > window.config.max_events:
                raise InvalidRequest(
                    f"cost {cost} exceeds max_events {window.config.max_events} for key {key!r}")
            now = self._tick()
            if window is None:
                raise LimitNotFound(f"no window configured for {key!r}")
            self._expire_window(window, now)
            self._expire_window_reservations(key, now)
            config = window.config
            used = self._window_used(window) + self._window_live_reservation_cost(key)
            if used + cost > config.max_events:
                retry_after = self._window_release_wait(window, key, cost, now)
                raise OverQuota(
                    f"window for {key!r} is full: {used}/{config.max_events} used, needs {cost}",
                    retry_after)
            reservation = WindowReservation(uuid.uuid4().hex, key, cost, now, ttl_seconds)
            self._window_reservations[reservation.reservation_id] = reservation
            used += cost
            return {"reservation_id": reservation.reservation_id, "key": key, "cost": cost,
                    "ttl_seconds": ttl_seconds, "used": used,
                    "remaining": config.max_events - used,
                    "limit": config.max_events, "window_seconds": config.window_seconds}

    def window_rollback(self, key: Any, reservation_id: str) -> dict[str, Any]:
        """Cancel a live window capacity hold exactly once: its cost stops counting immediately.

        The same lazy settle runs first at this single effective moment, so a hold whose TTL has
        elapsed has already been released and is unknown to this endpoint (404) — as are repeated
        rollbacks, unknown identifiers, holds belonging to a different window key (cross-key
        access is 404, not confusion), holds from the token-bucket or hierarchy namespaces, and
        confirmed holds (a consumed hold left the registry at consume time). An unknown window
        is 404 as well. The release forms no event: used and remaining simply move back.
        """
        key = validate_key(key)
        with self._lock:
            now = self._tick()
            window = self._windows.get(key)
            if window is None:
                raise LimitNotFound(f"no window configured for {key!r}")
            self._expire_window(window, now)
            self._expire_window_reservations(key, now)
            reservation = self._window_reservations.get(reservation_id)
            if reservation is None or reservation.key != key:
                raise LimitNotFound(f"no rollbackable window reservation {reservation_id!r}")
            del self._window_reservations[reservation_id]
            config = window.config
            used = self._window_used(window) + self._window_live_reservation_cost(key)
            return {"key": key, "cost": reservation.cost, "rolled_back": True, "used": used,
                    "remaining": config.max_events - used,
                    "limit": config.max_events, "window_seconds": config.window_seconds}

    def window_consume(self, key: Any, reservation_id: str) -> dict[str, Any]:
        """Confirm a live window capacity hold as ordinary occupancy.

        The same lazy settle runs first at this single effective moment, so a hold whose TTL has
        elapsed has already been released and is unknown to this endpoint (404) — as are unknown
        identifiers, cross-key holds, holds from the other reservation namespaces and rolled-back
        holds. The confirmed hold leaves the registry and becomes an ordinary admission stamped
        at this effective moment: it slides out of the window at now + window_seconds exactly
        like a checked admission. The conversion forms no ledger event, no metric and no
        revision change, and it never re-judges capacity — the cost was already occupying the
        window, so used and remaining are unchanged by the conversion itself. Repeating the call
        replays the first response byte for byte and never occupies a second time.
        """
        key = validate_key(key)
        with self._lock:
            now = self._tick()
            snapshot = self._window_consumed.get(reservation_id)
            if snapshot is not None and snapshot["key"] == key:
                return dict(snapshot)
            window = self._windows.get(key)
            if window is None:
                raise LimitNotFound(f"no window configured for {key!r}")
            self._expire_window(window, now)
            self._expire_window_reservations(key, now)
            reservation = self._window_reservations.get(reservation_id)
            if reservation is None or reservation.key != key:
                raise LimitNotFound(f"no consumable window reservation {reservation_id!r}")
            del self._window_reservations[reservation_id]
            window.events.append((now, reservation.cost))
            config = window.config
            used = self._window_used(window) + self._window_live_reservation_cost(key)
            snapshot = {"key": key, "cost": reservation.cost, "consumed": True, "used": used,
                        "remaining": config.max_events - used,
                        "limit": config.max_events, "window_seconds": config.window_seconds}
            self._window_consumed[reservation_id] = snapshot
            return dict(snapshot)

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
                if len(parts) == 4 and parts[:2] == ["v1", "ledgers"] and parts[3] == "reconciliation":
                    # Like /v1/metrics this route names no query parameters and reads no body:
                    # any query string is invalid_request, decided before the limiter call (and
                    # hence before the clock is sampled or any state could change). The response
                    # carries no ETag — reconciliation observes no configuration revision.
                    if self._query_string() != "":
                        raise InvalidRequest(
                            "GET /v1/ledgers/{key}/reconciliation takes no query parameters")
                    return self._send(200, limiter.reconciliation(parts[2]))
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
                    # The creation Idempotency-Key belongs to this one route and is validated
                    # before the body is read (the same place If-Match holds on PUT): an empty,
                    # repeated, over-long, whitespace-bearing or non-ASCII value is 400
                    # invalid_request without adopting any body state, deducting any token or
                    # sending Retry-After. A missing header keeps the baseline flow byte for byte.
                    idempotency_values = self.headers.get_all("Idempotency-Key")
                    if idempotency_values is not None:
                        if len(idempotency_values) != 1:
                            raise InvalidRequest(
                                "Idempotency-Key must be 1 to 128 non-whitespace ASCII characters")
                        idempotency_key: str | None = validate_idempotency_key(idempotency_values[0])
                    else:
                        idempotency_key = None
                    body = self._read_json()
                    if not isinstance(body, dict) or set(body) - {"key", "cost", "ttl_seconds"}:
                        raise InvalidRequest(
                            'body must be {"key": <string>, "cost": <integer>, "ttl_seconds": <integer 1..86400>}')
                    ttl = body.get("ttl_seconds", DEFAULT_TTL_SECONDS)
                    result = limiter.reserve(body.get("key"), body.get("cost", 1), ttl,
                                             idempotency_key)
                    return self._send(200, result)
                if len(parts) == 4 and parts[:2] == ["v1", "reservations"] and parts[3] == "consume":
                    body = self._read_json()
                    if not isinstance(body, dict) or body:
                        raise InvalidRequest('body must be an empty JSON object {}')
                    result = limiter.consume(parts[2])
                    return self._send(200, result)
                if len(parts) == 4 and parts[:2] == ["v1", "windows"] and parts[3] == "check":
                    # The key rides in the path, so the body is an object carrying at most a cost:
                    # {} (cost omitted, defaulting to 1) is the empty-object case. Anything else —
                    # arrays, scalars, unknown fields, malformed JSON — is invalid_request and the
                    # request never reaches the lock, mirroring the leaky-bucket check exactly.
                    body = self._read_json()
                    if not isinstance(body, dict) or set(body) - {"cost"}:
                        raise InvalidRequest('body must be {} or {"cost": <integer>}')
                    result = limiter.window_check(parts[2], body.get("cost", 1))
                    return self._send(200, result)
                if len(parts) == 4 and parts[:2] == ["v1", "windows"] and parts[3] == "reservations":
                    # Window capacity hold creation: the key rides in the path, so the body is an
                    # object carrying at most cost and ttl_seconds (each optional, defaulting to 1
                    # and 60). Anything else is invalid_request before the lock, exactly as on the
                    # window check route.
                    body = self._read_json()
                    if not isinstance(body, dict) or set(body) - {"cost", "ttl_seconds"}:
                        raise InvalidRequest(
                            'body must be {} or {"cost": <integer>, "ttl_seconds": <integer 1..86400>}')
                    ttl = body.get("ttl_seconds", DEFAULT_TTL_SECONDS)
                    result = limiter.window_reserve(parts[2], body.get("cost", 1), ttl)
                    return self._send(200, result)
                if len(parts) == 6 and parts[:2] == ["v1", "windows"] \
                        and parts[3] == "reservations" and parts[5] == "consume":
                    body = self._read_json()
                    if not isinstance(body, dict) or body:
                        raise InvalidRequest('body must be an empty JSON object {}')
                    result = limiter.window_consume(parts[2], parts[4])
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
                if len(parts) == 5 and parts[:2] == ["v1", "windows"] and parts[3] == "reservations":
                    result = limiter.window_rollback(parts[2], parts[4])
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
