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
from urllib.parse import parse_qsl, urlsplit


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


def validate_key(key: Any) -> str:
    if not isinstance(key, str) or not key or len(key) > 200:
        raise InvalidRequest("key must be a non-empty string of at most 200 characters")
    return key


def validate_cost(cost: Any) -> int:
    if not isinstance(cost, int) or isinstance(cost, bool) or cost < 1 or cost > 1_000_000:
        raise InvalidRequest("cost must be an integer between 1 and 1000000")
    return cost


DEFAULT_TTL_SECONDS = 60

# Ledger pagination: a single GET never returns more events than this.
LEDGER_PAGE_SIZE = 100
# `after` is a 32-bit signed cursor: strictly decimal, no sign, no fraction, no exponent.
MAX_AFTER = 2_147_483_647


def validate_after(raw: Any) -> int:
    """Validate the ledger `after` cursor: a non-negative ASCII decimal integer, at most 2**31 - 1.

    Signs, decimal points, exponents, blanks and non-ASCII digits are all rejected; int() alone
    would silently accept several of those (and unicode digits), so the character set is checked
    explicitly before converting.
    """
    if not isinstance(raw, str) or not raw or any(char not in "0123456789" for char in raw):
        raise InvalidRequest("after must be a non-negative decimal integer")
    after = int(raw)
    if after > MAX_AFTER:
        raise InvalidRequest(f"after must be at most {MAX_AFTER}")
    return after


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


class Limiter:
    """One token bucket per tenant key. `now` is a monotonic-seconds callable, injected for tests."""

    def __init__(self, now: Callable[[], float]) -> None:
        self._now = now
        self._lock = threading.RLock()
        self._limits: dict[str, Limit] = {}
        self._buckets: dict[str, Bucket] = {}
        self._reservations: dict[str, Reservation] = {}
        # Confirmed holds leave the active registry for good; the value is the exact first consume
        # response, replayed verbatim for idempotent retries (used is never booked a second time).
        self._consumed: dict[str, dict[str, Any]] = {}
        # Per-key billing ledger: one immutable event per accepted usage (successful check or first
        # consume of a reservation), in occurrence order, sequenced from 1 with no gaps. Rejected
        # checks, bare holds, refunds, rollbacks and replayed consumes never append. Reconfiguring
        # the key keeps the ledger and its sequence running; a process restart starts it empty.
        self._ledgers: dict[str, list[dict[str, Any]]] = {}

    def configure(self, key: Any, payload: Any) -> Limit:
        key = validate_key(key)
        limit = validate_limit(payload)
        with self._lock:
            # Reconfiguration starts with the same lazy expiry settle every other operation does;
            # the release is credited against the bucket running under the old configuration.
            self._expire_due(key)
            self._limits[key] = limit
            existing = self._buckets.get(key)
            self._buckets[key] = Bucket(limit.capacity if existing is None else min(existing.tokens, limit.capacity),
                                        self._now(), existing.cost_history if existing else [])
        return limit

    def limit(self, key: str) -> Limit:
        with self._lock:
            if key not in self._limits:
                raise LimitNotFound(f"no limit configured for {key!r}")
            return self._limits[key]

    def _refill(self, key: str) -> Bucket:
        limit = self._limits[key]
        bucket = self._buckets.get(key) or Bucket(limit.capacity, self._now())
        elapsed = max(0.0, self._now() - bucket.updated_at)
        bucket.tokens = min(float(limit.capacity), bucket.tokens + elapsed * limit.refill_per_second)
        bucket.updated_at = self._now()
        self._buckets[key] = bucket
        return bucket

    def _expire_due(self, key: str) -> int:
        """Lazy, deterministic expiry settle: release every reservation of `key` whose TTL has elapsed.

        Each reservation's cost is returned at most once: it is removed from the registry before the
        bucket is credited. The bucket refills by elapsed time first, then the returned cost is capped
        at the key's current capacity. Boundary is inclusive (created_at + ttl <= now). Caller holds
        the lock; a no-op when the key has no due reservations (and never touches an unconfigured key).
        """
        now = self._now()
        due = [rid for rid, reservation in self._reservations.items()
               if reservation.key == key and not reservation.rolled_back
               and reservation.expires_at() <= now]
        if not due:
            return 0
        returned = sum(self._reservations[rid].cost for rid in due)
        for rid in due:
            del self._reservations[rid]
        bucket = self._refill(key)
        bucket.tokens = min(float(self._limits[key].capacity), bucket.tokens + returned)
        return returned

    def _record_event(self, key: str, source: str, cost: int, bucket: Bucket) -> None:
        """Append one ledger event for a usage that has just been booked. Caller holds the lock and
        has already pushed `cost` onto the bucket's cost history, so used_after reads off the same
        accounting the state endpoint reports. The stored event is never mutated afterwards."""
        events = self._ledgers.setdefault(key, [])
        events.append({"sequence": len(events) + 1, "source": source, "cost": cost,
                       "used_after": sum(bucket.cost_history), "event_id": uuid.uuid4().hex,
                       "occurred_at": self._now()})

    def check(self, key: Any, cost: Any) -> dict[str, Any]:
        cost = validate_cost(cost)
        with self._lock:
            limit = self.limit(key)
            self._expire_due(key)
            bucket = self._refill(key)
            if bucket.tokens >= cost:
                bucket.tokens -= cost
                bucket.cost_history.append(cost)
                self._record_event(key, "check", cost, bucket)
                return {"allowed": True, "remaining": int(bucket.tokens), "capacity": limit.capacity}
            deficit = cost - bucket.tokens
            retry_after = deficit / limit.refill_per_second
            raise OverQuota(f"key {key!r} has {bucket.tokens:.3f} tokens, needs {cost}", retry_after)

    def reserve(self, key: Any, cost: Any, ttl_seconds: Any = DEFAULT_TTL_SECONDS) -> dict[str, Any]:
        """Atomically hold `cost` tokens, exactly as check() judges them, without booking them as used.

        The hold lapses `ttl_seconds` of monotonic time after creation; reconfiguring the key never
        extends it. Expired holds are settled lazily at the start of this call (see _expire_due).
        """
        validate_key(key)
        cost = validate_cost(cost)
        ttl_seconds = validate_ttl(ttl_seconds)
        with self._lock:
            limit = self.limit(key)
            self._expire_due(key)
            bucket = self._refill(key)
            if bucket.tokens < cost:
                deficit = cost - bucket.tokens
                raise OverQuota(f"key {key!r} has {bucket.tokens:.3f} tokens, needs {cost}",
                                deficit / limit.refill_per_second)
            bucket.tokens -= cost
            reservation = Reservation(uuid.uuid4().hex, key, cost, self._now(), ttl_seconds)
            self._reservations[reservation.reservation_id] = reservation
            return {"reservation_id": reservation.reservation_id, "key": key, "cost": cost,
                    "remaining": int(bucket.tokens), "capacity": limit.capacity,
                    "ttl_seconds": ttl_seconds}

    def rollback(self, reservation_id: str) -> dict[str, Any]:
        with self._lock:
            reservation = self._reservations.get(reservation_id)
            if reservation is None:
                raise LimitNotFound(f"no rollbackable reservation {reservation_id!r}")
            key = reservation.key
            # Rollback starts with the same lazy settle: a reservation whose TTL has elapsed has
            # already been refunded, so it is unknown to this endpoint and returns 404.
            self._expire_due(key)
            reservation = self._reservations.get(reservation_id)
            if reservation is None or reservation.rolled_back:
                raise LimitNotFound(f"no rollbackable reservation {reservation_id!r}")
            reservation.rolled_back = True
            del self._reservations[reservation_id]
            limit = self._limits[key]
            bucket = self._refill(key)
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
            snapshot = self._consumed.get(reservation_id)
            if snapshot is not None:
                return dict(snapshot)
            reservation = self._reservations.get(reservation_id)
            if reservation is None:
                raise LimitNotFound(f"no consumable reservation {reservation_id!r}")
            key = reservation.key
            # A reservation whose TTL has elapsed is refunded by the existing settle first; the target
            # is then unknown, consumes nothing and is never booked later.
            self._expire_due(key)
            reservation = self._reservations.pop(reservation_id, None)
            if reservation is None:
                raise LimitNotFound(f"no consumable reservation {reservation_id!r}")
            limit = self._limits[key]
            bucket = self._refill(key)
            bucket.cost_history.append(reservation.cost)
            self._record_event(key, "reservation_consume", reservation.cost, bucket)
            snapshot = {"reservation_id": reservation_id, "consumed": True,
                        "remaining": int(bucket.tokens), "capacity": limit.capacity,
                        "used": sum(bucket.cost_history)}
            self._consumed[reservation_id] = snapshot
            return dict(snapshot)

    def state(self, key: str) -> dict[str, Any]:
        with self._lock:
            limit = self.limit(key)
            self._expire_due(key)
            bucket = self._refill(key)
            return {"limit": limit.as_json(), "remaining": int(bucket.tokens), "used": sum(bucket.cost_history)}

    def ledger(self, key: Any, after: int = 0) -> dict[str, Any]:
        """Read-only page of the key's billing ledger: events with sequence strictly above `after`.

        The read settles due reservations of the key first, exactly like the state endpoint — the
        refunds themselves are not usage and never appear as events. At most LEDGER_PAGE_SIZE events
        are returned, oldest first; next_after is the last returned sequence (the cursor for the
        next page), or the caller's `after` unchanged when the page is empty. Returned events are
        copies: the stored ledger entries stay frozen once appended.
        """
        key = validate_key(key)
        with self._lock:
            self.limit(key)
            self._expire_due(key)
            events = self._ledgers.get(key, [])
            page = [dict(event) for event in events if event["sequence"] > after][:LEDGER_PAGE_SIZE]
            return {"key": key, "events": page,
                    "next_after": page[-1]["sequence"] if page else after}


def make_handler(limiter: Limiter) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "quota/0.1"
        protocol_version = "HTTP/1.1"

        def log_message(self, *args: Any) -> None:
            return

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

        def _keys(self) -> list[str]:
            return [p for p in self.path.split("?")[0].split("/") if p]

        def _ledger_after(self) -> int:
            """Parse the ledger query string: `after` is the only accepted parameter.

            Anything else — an unknown name, a repeated cursor, a sign, a fraction, an exponent,
            a blank or a non-ASCII digit — is invalid_request and changes no state. Evaluated before
            the limiter call, so a malformed query answers 400 even when the key itself is unknown.
            """
            pairs = parse_qsl(urlsplit(self.path).query, keep_blank_values=True)
            after: int | None = None
            for name, value in pairs:
                if name != "after":
                    raise InvalidRequest(f"unknown query parameter: {name!r}")
                if after is not None:
                    raise InvalidRequest("after must appear at most once")
                after = validate_after(value)
            return 0 if after is None else after

        def do_GET(self) -> None:  # noqa: N802
            try:
                parts = self._keys()
                if parts == ["health"]:
                    return self._send(200, {"status": "ok"})
                if len(parts) == 3 and parts[:2] == ["v1", "limits"]:
                    return self._send(200, limiter.state(parts[2]))
                if len(parts) == 4 and parts[:2] == ["v1", "limits"] and parts[3] == "ledger":
                    return self._send(200, limiter.ledger(parts[2], self._ledger_after()))
                return self._send(404, {"error": {"code": "not_found"}})
            except QuotaError as error:
                return self._send(error.status, {"error": {"code": error.code, "message": str(error)}})
            except Exception:
                return self._send(500, {"error": {"code": "internal_error"}})

        def do_PUT(self) -> None:  # noqa: N802
            try:
                parts = self._keys()
                if len(parts) != 3 or parts[:2] != ["v1", "limits"]:
                    return self._send(404, {"error": {"code": "not_found"}})
                limit = limiter.configure(parts[2], self._read_json())
                return self._send(200, {"key": parts[2], "limit": limit.as_json()})
            except QuotaError as error:
                return self._send(error.status, {"error": {"code": error.code, "message": str(error)}})
            except Exception:
                return self._send(500, {"error": {"code": "internal_error"}})

        def do_POST(self) -> None:  # noqa: N802
            try:
                parts = self._keys()
                if parts == ["v1", "check"]:
                    body = self._read_json()
                    if not isinstance(body, dict) or set(body) - {"key", "cost"}:
                        raise InvalidRequest("body must be {\"key\": <string>, \"cost\": <integer>}")
                    result = limiter.check(body.get("key"), body.get("cost", 1))
                    return self._send(200, result)
                if parts == ["v1", "reservations"]:
                    body = self._read_json()
                    if not isinstance(body, dict) or set(body) - {"key", "cost", "ttl_seconds"}:
                        raise InvalidRequest(
                            'body must be {"key": <string>, "cost": <integer>, "ttl_seconds": <integer 1..86400>}')
                    ttl = body.get("ttl_seconds", DEFAULT_TTL_SECONDS)
                    try:
                        result = limiter.reserve(body.get("key"), body.get("cost", 1), ttl)
                    except OverQuota as error:
                        # Round up to the millisecond so the value is always long enough to refill `cost`.
                        retry_after = math.ceil(error.retry_after * 1000) / 1000
                        return self._send(error.status, {"error": {"code": error.code, "message": str(error)}},
                                          {"Retry-After": f"{retry_after:.3f}"})
                    return self._send(200, result)
                if len(parts) == 4 and parts[:2] == ["v1", "reservations"] and parts[3] == "consume":
                    body = self._read_json()
                    if not isinstance(body, dict) or body:
                        raise InvalidRequest('body must be an empty JSON object {}')
                    result = limiter.consume(parts[2])
                    return self._send(200, result)
                return self._send(404, {"error": {"code": "not_found"}})
            except OverQuota as error:
                return self._send(error.status, {"error": {"code": error.code, "message": str(error)}},
                                  {"Retry-After": f"{error.retry_after:.3f}"})
            except QuotaError as error:
                return self._send(error.status, {"error": {"code": error.code, "message": str(error)}})
            except Exception:
                return self._send(500, {"error": {"code": "internal_error"}})

        def do_DELETE(self) -> None:  # noqa: N802
            try:
                parts = self._keys()
                if len(parts) != 3 or parts[:2] != ["v1", "reservations"]:
                    return self._send(404, {"error": {"code": "not_found"}})
                result = limiter.rollback(parts[2])
                return self._send(200, result)
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
