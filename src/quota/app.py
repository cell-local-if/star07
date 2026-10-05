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


@dataclass
class Reservation:
    reservation_id: str
    key: str
    cost: int
    created_at: float
    ttl_seconds: int
    settled: bool = False

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
MAX_TTL_SECONDS = 86400


def validate_ttl_seconds(ttl_seconds: Any) -> int:
    if (not isinstance(ttl_seconds, int) or isinstance(ttl_seconds, bool)
            or ttl_seconds < 1 or ttl_seconds > MAX_TTL_SECONDS):
        raise InvalidRequest(f"ttl_seconds must be an integer between 1 and {MAX_TTL_SECONDS}")
    return ttl_seconds


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

    def configure(self, key: Any, payload: Any) -> Limit:
        key = validate_key(key)
        limit = validate_limit(payload)
        with self._lock:
            if key in self._limits:
                # Reconfigure is a settlement point: release this key's expired reservations
                # under the current limit before the new one takes over. Live reservations
                # stay valid and keep their original expiry.
                self._settle_expired(key)
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

    def _settle_expired(self, key: str) -> None:
        """Lazily release this key's expired reservations. Lock must be held and `key` configured.

        Refills by elapsed time first, then returns each expired reservation's cost exactly
        once, capped at the key's current capacity. A reservation expires when the monotonic
        clock reaches created_at + ttl_seconds (boundary inclusive).
        """
        now = self._now()
        expired = [reservation for reservation in self._reservations.values()
                   if reservation.key == key and not reservation.settled and now >= reservation.expires_at()]
        if not expired:
            return
        limit = self._limits[key]
        bucket = self._refill(key)
        for reservation in expired:
            reservation.settled = True
            bucket.tokens = min(float(limit.capacity), bucket.tokens + reservation.cost)

    def check(self, key: Any, cost: Any) -> dict[str, Any]:
        cost = validate_cost(cost)
        with self._lock:
            limit = self.limit(key)
            self._settle_expired(key)
            bucket = self._refill(key)
            if bucket.tokens >= cost:
                bucket.tokens -= cost
                bucket.cost_history.append(cost)
                return {"allowed": True, "remaining": int(bucket.tokens), "capacity": limit.capacity}
            deficit = cost - bucket.tokens
            retry_after = deficit / limit.refill_per_second
            raise OverQuota(f"key {key!r} has {bucket.tokens:.3f} tokens, needs {cost}", retry_after)

    def reserve(self, key: Any, cost: Any, ttl_seconds: Any = DEFAULT_TTL_SECONDS) -> dict[str, Any]:
        """Atomically hold `cost` tokens, exactly as check() judges them, without booking them as used.

        The hold expires `ttl_seconds` after creation and is then released lazily.
        """
        validate_key(key)
        cost = validate_cost(cost)
        ttl_seconds = validate_ttl_seconds(ttl_seconds)
        with self._lock:
            limit = self.limit(key)
            self._settle_expired(key)
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
            # Rolling back is a settlement point for the reservation's key: an already
            # expired reservation is released here and can no longer be rolled back.
            self._settle_expired(reservation.key)
            if reservation.settled:
                raise LimitNotFound(f"no rollbackable reservation {reservation_id!r}")
            reservation.settled = True
            key = reservation.key
            limit = self._limits[key]
            bucket = self._refill(key)
            bucket.tokens = min(float(limit.capacity), bucket.tokens + reservation.cost)
            return {"reservation_id": reservation_id, "rolled_back": True,
                    "remaining": int(bucket.tokens), "capacity": limit.capacity}

    def state(self, key: str) -> dict[str, Any]:
        with self._lock:
            limit = self.limit(key)
            self._settle_expired(key)
            bucket = self._refill(key)
            return {"limit": limit.as_json(), "remaining": int(bucket.tokens), "used": sum(bucket.cost_history)}


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

        def do_GET(self) -> None:  # noqa: N802
            try:
                parts = self._keys()
                if parts == ["health"]:
                    return self._send(200, {"status": "ok"})
                if len(parts) == 3 and parts[:2] == ["v1", "limits"]:
                    return self._send(200, limiter.state(parts[2]))
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
                        raise InvalidRequest("body must be {\"key\": <string>, \"cost\": <integer>, "
                                             "\"ttl_seconds\": <integer 1..86400>?}")
                    try:
                        result = limiter.reserve(body.get("key"), body.get("cost", 1),
                                                 body.get("ttl_seconds", DEFAULT_TTL_SECONDS))
                    except OverQuota as error:
                        # Round up to the millisecond so the value is always long enough to refill `cost`.
                        retry_after = math.ceil(error.retry_after * 1000) / 1000
                        return self._send(error.status, {"error": {"code": error.code, "message": str(error)}},
                                          {"Retry-After": f"{retry_after:.3f}"})
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
