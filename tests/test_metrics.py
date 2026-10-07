"""Cumulative decision metrics: GET /v1/metrics counts decisions, nothing else."""
from __future__ import annotations

import json
import threading
import unittest
import urllib.error
import urllib.request

from quota import InvalidRequest, LimitNotFound, Limiter, OverQuota


class Clock:
    def __init__(self) -> None:
        self.t = 1000.0
        self.calls = 0

    def __call__(self) -> float:
        self.calls += 1
        return self.t


def zero_metrics() -> dict:
    return {"metrics": {"decisions": {
        "check": {"allowed": 0, "over_quota": 0},
        "hierarchy_check": {"allowed": 0, "over_quota": 0},
        "reservation": {"allowed": 0, "over_quota": 0},
        "hierarchy_reservation": {"allowed": 0, "over_quota": 0},
        "window_check": {"allowed": 0, "over_quota": 0},
    }}}


class MetricsUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.limiter = Limiter(self.clock)

    def decisions(self) -> dict:
        return self.limiter.metrics()["metrics"]["decisions"]

    def test_initial_shape_is_exact_and_all_zero(self) -> None:
        self.assertEqual(self.limiter.metrics(), zero_metrics())

    def test_check_allowed_and_over_quota_each_count_once(self) -> None:
        self.limiter.configure("tenant-a", {"capacity": 1, "refill_per_second": 1.0})
        self.limiter.check("tenant-a", 1)
        with self.assertRaises(OverQuota):
            self.limiter.check("tenant-a", 1)
        self.assertEqual(self.decisions()["check"], {"allowed": 1, "over_quota": 1})

    def test_hierarchy_check_counts_the_request_not_the_layers(self) -> None:
        for key in ("org", "team", "user"):
            self.limiter.configure(key, {"capacity": 1, "refill_per_second": 1.0})
        self.limiter.hierarchy_check(["org", "team", "user"], 1)
        with self.assertRaises(OverQuota):
            self.limiter.hierarchy_check(["org", "team", "user"], 1)
        self.assertEqual(self.decisions()["hierarchy_check"], {"allowed": 1, "over_quota": 1})

    def test_reservation_create_counts_but_consume_rollback_and_expiry_do_not(self) -> None:
        self.limiter.configure("tenant-a", {"capacity": 2, "refill_per_second": 1.0})
        consumed = self.limiter.reserve("tenant-a", 1, ttl_seconds=10)["reservation_id"]
        expired = self.limiter.reserve("tenant-a", 1, ttl_seconds=10)["reservation_id"]
        with self.assertRaises(OverQuota):
            self.limiter.reserve("tenant-a", 1)
        self.limiter.consume(consumed)
        self.limiter.consume(consumed)  # idempotent replay: still nothing
        self.clock.t += 11.0            # `expired` lapses; the settle is lazy
        self.limiter.check("tenant-a", 1)  # forces the expiry settle — not a reservation decision
        rolled = self.limiter.reserve("tenant-a", 1)["reservation_id"]
        self.limiter.rollback(rolled)
        self.assertEqual(self.decisions()["reservation"], {"allowed": 3, "over_quota": 1})
        self.assertEqual(self.decisions()["check"], {"allowed": 1, "over_quota": 0})

    def test_hierarchy_reservation_counts_once_per_request(self) -> None:
        for key in ("org", "team"):
            self.limiter.configure(key, {"capacity": 1, "refill_per_second": 1.0})
        reservation_id = self.limiter.hierarchy_reserve(["org", "team"], 1)["reservation_id"]
        with self.assertRaises(OverQuota):
            self.limiter.hierarchy_reserve(["org", "team"], 1)
        self.limiter.hierarchy_consume(reservation_id)
        self.assertEqual(self.decisions()["hierarchy_reservation"], {"allowed": 1, "over_quota": 1})

    def test_window_check_counts_admission_and_rejection(self) -> None:
        self.limiter.configure_window("w", {"window_seconds": 60, "max_events": 1})
        self.limiter.window_check("w")
        with self.assertRaises(OverQuota):
            self.limiter.window_check("w")
        self.limiter.window_state("w")  # reads never count
        self.assertEqual(self.decisions()["window_check"], {"allowed": 1, "over_quota": 1})

    def test_validation_failures_and_unknown_keys_count_nothing(self) -> None:
        self.limiter.configure("tenant-a", {"capacity": 1, "refill_per_second": 1.0})
        for action in (
            lambda: self.limiter.check("tenant-a", 0),            # invalid cost
            lambda: self.limiter.check("", 1),                    # invalid key
            lambda: self.limiter.check("ghost", 1),               # unconfigured key
            lambda: self.limiter.reserve("ghost", 1),             # unconfigured key
            lambda: self.limiter.reserve("tenant-a", 1, ttl_seconds=0),  # invalid ttl
            lambda: self.limiter.hierarchy_check(["tenant-a"], 1),       # too few keys
            lambda: self.limiter.hierarchy_check(["tenant-a", "ghost"], 1),
            lambda: self.limiter.hierarchy_reserve(["tenant-a", "ghost"], 1),
            lambda: self.limiter.window_check("ghost"),           # unconfigured window
            lambda: self.limiter.window_check(""),                # invalid key
        ):
            with self.assertRaises((InvalidRequest, LimitNotFound)):
                action()
        self.assertEqual(self.limiter.metrics(), zero_metrics())

    def test_reads_never_sample_the_clock_or_mutate_state(self) -> None:
        self.limiter.configure("tenant-a", {"capacity": 1, "refill_per_second": 1.0})
        self.limiter.reserve("tenant-a", 1, ttl_seconds=5)
        calls_before = self.clock.calls
        self.clock.t += 100.0  # past the reservation's expiry: a mutating entry would settle it
        first = self.limiter.metrics()
        self.clock.t -= 200.0  # regression: must not change the read either
        second = self.limiter.metrics()
        self.assertEqual(first, second)
        self.assertEqual(self.clock.calls, calls_before)  # no clock sample at all
        # The reads never moved the watermark: effective time is still the reserve moment,
        # so the lapsed-in-real-time hold is still live and unsettled.
        self.assertEqual(self.limiter.state("tenant-a")["remaining"], 0)
        # Only a real clock reading past the TTL settles it, exactly once.
        self.clock.t = 1200.0
        self.assertEqual(self.limiter.state("tenant-a")["remaining"], 1)

    def test_concurrent_decisions_are_counted_exactly(self) -> None:
        self.limiter.configure("tenant-a", {"capacity": 40, "refill_per_second": 0.001})
        threads = [threading.Thread(target=lambda: self._try_check()) for _ in range(100)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        counts = self.decisions()["check"]
        self.assertEqual(counts["allowed"], 40)
        self.assertEqual(counts["over_quota"], 60)

    def _try_check(self) -> None:
        try:
            self.limiter.check("tenant-a", 1)
        except OverQuota:
            pass


class MetricsHttpTests(unittest.TestCase):
    def setUp(self) -> None:
        from quota import serve

        self.clock = Clock()
        self.server = serve(port=0, now=self.clock)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()

    def request(self, method: str, path: str, body: dict | None = None) -> tuple[int, dict, dict]:
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
                                         headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read() or b"{}"), dict(response.headers)
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read() or b"{}"), dict(error.headers)

    def test_empty_limiter_reports_the_exact_zero_shape(self) -> None:
        status, body, _ = self.request("GET", "/v1/metrics")
        self.assertEqual(status, 200)
        self.assertEqual(body, zero_metrics())

    def test_decisions_flow_through_the_http_surface(self) -> None:
        self.request("PUT", "/v1/limits/k", {"capacity": 1, "refill_per_second": 1.0})
        self.request("PUT", "/v1/windows/w", {"window_seconds": 60, "max_events": 1})
        self.assertEqual(self.request("POST", "/v1/check", {"key": "k", "cost": 1})[0], 200)
        status, _, headers = self.request("POST", "/v1/check", {"key": "k", "cost": 1})
        self.assertEqual(status, 429)
        self.assertEqual(headers["Retry-After"], "1.000")
        self.assertEqual(self.request("POST", "/v1/windows/w/check", {})[0], 200)
        self.assertEqual(self.request("POST", "/v1/windows/w/check", {})[0], 429)
        # Reads, 404s and validation failures move no counter.
        self.request("GET", "/v1/limits/k")
        self.request("GET", "/v1/limits/ghost")
        self.request("POST", "/v1/check", {"key": "k", "cost": 0})
        status, body, _ = self.request("GET", "/v1/metrics")
        self.assertEqual(status, 200)
        self.assertEqual(body["metrics"]["decisions"]["check"], {"allowed": 1, "over_quota": 1})
        self.assertEqual(body["metrics"]["decisions"]["window_check"], {"allowed": 1, "over_quota": 1})
        self.assertEqual(body["metrics"]["decisions"]["reservation"], {"allowed": 0, "over_quota": 0})

    def test_any_query_parameter_is_invalid_request(self) -> None:
        for path in ("/v1/metrics?events=1", "/v1/metrics?foo", "/v1/metrics?foo=1&bar=2"):
            status, body, _ = self.request("GET", path)
            self.assertEqual(status, 400, path)
            self.assertEqual(body["error"]["code"], "invalid_request", path)

    def test_wrong_path_shape_and_wrong_method_are_not_found(self) -> None:
        for method, path in (("GET", "/v1/metrics/extra"), ("GET", "/v1"),
                             ("POST", "/v1/metrics"), ("PUT", "/v1/metrics"),
                             ("DELETE", "/v1/metrics"), ("PATCH", "/v1/metrics")):
            status, body, _ = self.request(method, path, {} if method in ("POST", "PUT") else None)
            self.assertEqual(status, 404, (method, path))
            self.assertEqual(body["error"]["code"], "not_found", (method, path))

    def test_metrics_read_does_not_advance_time_or_settle_state(self) -> None:
        self.request("PUT", "/v1/limits/k", {"capacity": 1, "refill_per_second": 1.0})
        self.request("POST", "/v1/reservations", {"key": "k", "cost": 1, "ttl_seconds": 5})
        self.clock.t += 100.0
        before = self.request("GET", "/v1/metrics")[1]
        self.clock.t -= 50.0
        after = self.request("GET", "/v1/metrics")[1]
        self.assertEqual(before, after)
        # The read settled nothing: the next state read refunds the lapsed hold exactly once.
        _, state, _ = self.request("GET", "/v1/limits/k")
        self.assertEqual(state["remaining"], 1)


if __name__ == "__main__":
    unittest.main()
