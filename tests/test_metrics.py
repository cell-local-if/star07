"""GET /v1/metrics: cumulative, in-memory, read-only counts of the five decision streams."""
from __future__ import annotations

import json
import threading
import unittest
import urllib.error
import urllib.request

from quota import Limiter, OverQuota


class Clock:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t


ZERO_DECISIONS = {
    "check": {"allowed": 0, "over_quota": 0},
    "hierarchy_check": {"allowed": 0, "over_quota": 0},
    "reservation": {"allowed": 0, "over_quota": 0},
    "hierarchy_reservation": {"allowed": 0, "over_quota": 0},
    "window_check": {"allowed": 0, "over_quota": 0},
}


class MetricsUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.limiter = Limiter(self.clock)
        self.limiter.configure("tenant-a", {"capacity": 2, "refill_per_second": 1.0})

    def decisions(self) -> dict:
        return self.limiter.metrics()["decisions"]

    def test_fresh_limiter_reports_only_zeros(self) -> None:
        self.assertEqual(self.limiter.metrics(), {"decisions": ZERO_DECISIONS})

    def test_check_allow_and_over_quota_are_counted(self) -> None:
        self.limiter.check("tenant-a", 1)
        self.limiter.check("tenant-a", 1)
        with self.assertRaises(OverQuota):
            self.limiter.check("tenant-a", 1)
        self.assertEqual(self.decisions()["check"], {"allowed": 2, "over_quota": 1})

    def test_hierarchy_check_counts_once_for_the_whole_request(self) -> None:
        self.limiter.configure("p", {"capacity": 1, "refill_per_second": 1.0})
        self.limiter.configure("l", {"capacity": 100, "refill_per_second": 1.0})
        self.limiter.hierarchy_check(["p", "l"], 1)                 # one allow, not two (one per layer)
        with self.assertRaises(OverQuota):
            self.limiter.hierarchy_check(["p", "l"], 1)
        self.assertEqual(self.decisions()["hierarchy_check"], {"allowed": 1, "over_quota": 1})

    def test_reservation_create_is_counted_but_consume_rollback_and_expiry_are_not(self) -> None:
        rid = self.limiter.reserve("tenant-a", 1, ttl_seconds=10)["reservation_id"]
        self.limiter.consume(rid)
        self.limiter.consume(rid)                                   # idempotent replay: no count
        self.assertEqual(self.decisions()["reservation"], {"allowed": 1, "over_quota": 0})

        rolling = self.limiter.reserve("tenant-a", 1, ttl_seconds=10)["reservation_id"]
        self.limiter.rollback(rolling)
        with self.assertRaises(Exception):
            self.limiter.rollback(rolling)                          # repeated rollback: no count
        expiring = self.limiter.reserve("tenant-a", 1, ttl_seconds=10)["reservation_id"]
        self.clock.t += 10
        self.limiter.state("tenant-a")                             # settles the due hold: no count
        with self.assertRaises(Exception):
            self.limiter.consume(expiring)
        self.assertEqual(self.decisions()["reservation"], {"allowed": 3, "over_quota": 0})

        with self.assertRaises(OverQuota):
            self.limiter.reserve("tenant-a", 100, ttl_seconds=10)
        self.assertEqual(self.decisions()["reservation"], {"allowed": 3, "over_quota": 1})

    def test_hierarchy_reservation_counts_once_and_not_on_consume_or_rollback(self) -> None:
        self.limiter.configure("p", {"capacity": 5, "refill_per_second": 1.0})
        self.limiter.configure("l", {"capacity": 50, "refill_per_second": 1.0})
        rid = self.limiter.hierarchy_reserve(["p", "l"], 1, ttl_seconds=60)["reservation_id"]
        self.limiter.hierarchy_consume(rid)
        self.limiter.hierarchy_consume(rid)
        other = self.limiter.hierarchy_reserve(["p", "l"], 1, ttl_seconds=60)["reservation_id"]
        self.limiter.hierarchy_rollback(other)
        self.assertEqual(self.decisions()["hierarchy_reservation"], {"allowed": 2, "over_quota": 0})
        with self.assertRaises(OverQuota):
            self.limiter.hierarchy_reserve(["p", "l"], 100, ttl_seconds=60)
        self.assertEqual(self.decisions()["hierarchy_reservation"], {"allowed": 2, "over_quota": 1})

    def test_window_check_allow_and_full_window_are_counted(self) -> None:
        self.limiter.configure_window("w", {"window_seconds": 60, "max_events": 1})
        self.limiter.window_check("w")
        with self.assertRaises(OverQuota):
            self.limiter.window_check("w")
        self.assertEqual(self.decisions()["window_check"], {"allowed": 1, "over_quota": 1})

    def test_validation_failures_and_missing_keys_are_not_counted(self) -> None:
        for bad_cost in (0, True, "x", 1.5):
            with self.assertRaises(Exception):
                self.limiter.check("tenant-a", bad_cost)
        with self.assertRaises(Exception):
            self.limiter.check("missing", 1)
        with self.assertRaises(Exception):
            self.limiter.reserve("tenant-a", 0)
        with self.assertRaises(Exception):
            self.limiter.reserve("tenant-a", 1, 0)
        with self.assertRaises(Exception):
            self.limiter.hierarchy_check(["tenant-a"], 1)   # too few keys
        with self.assertRaises(Exception):
            self.limiter.window_check("no-window")
        self.assertEqual(self.decisions(), ZERO_DECISIONS)

    def test_read_only_entries_and_configuration_are_not_counted(self) -> None:
        self.limiter.check("tenant-a", 1)
        self.limiter.state("tenant-a")
        self.limiter.ledger("tenant-a")
        self.limiter.metrics()
        self.limiter.configure("tenant-a", {"capacity": 9, "refill_per_second": 2.0})
        self.limiter.configure_window("w", {"window_seconds": 60, "max_events": 3})
        self.limiter.window_state("w")
        self.assertEqual(self.decisions(), {**ZERO_DECISIONS, "check": {"allowed": 1, "over_quota": 0}})

    def test_metrics_read_samples_no_clock_and_settles_nothing(self) -> None:
        rid = self.limiter.reserve("tenant-a", 1, ttl_seconds=10)["reservation_id"]
        self.clock.t += 10                                          # hold is now due
        self.limiter.metrics()                                      # read must neither tick nor settle
        self.assertIn(rid, self.limiter._reservations)
        # A read that had ticked would have pinned the watermark at 1010 and refunded early.
        self.clock.t = 1005.0
        self.assertEqual(self.limiter.state("tenant-a")["remaining"], 2)  # still held, no advance

    def test_concurrent_decisions_count_exactly_without_loss_or_duplication(self) -> None:
        limiter = Limiter(self.clock)
        limiter.configure("hot", {"capacity": 100, "refill_per_second": 0.0001})
        limiter.configure_window("hot", {"window_seconds": 3600, "max_events": 100})
        errors: list[BaseException] = []

        def attempt() -> None:
            try:
                limiter.check("hot", 1)
            except OverQuota:
                pass
            except BaseException as error:  # noqa: BLE001
                errors.append(error)

        threads = [threading.Thread(target=attempt) for _ in range(400)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        counts = limiter.metrics()["decisions"]["check"]
        self.assertEqual(counts, {"allowed": 100, "over_quota": 300})
        self.assertEqual(counts["allowed"] + counts["over_quota"], 400)


class MetricsHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        import http.client

        cls.http_client = http.client
        from quota import serve

        cls.clock = Clock()
        cls.server = serve(port=0, now=cls.clock)
        cls.port = cls.server.server_address[1]
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()

    def request(self, method: str, path: str, body: dict | None = None) -> tuple[int, dict, dict]:
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
                                         headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read() or b"{}"), dict(response.headers)
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read() or b"{}"), dict(error.headers)

    def raw_request(self, method: str, path: str, payload: bytes | None) -> tuple[int, dict]:
        connection = self.http_client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        connection.putrequest(method, path)
        connection.endheaders(payload if payload is not None else b"")
        response = connection.getresponse()
        body = json.loads(response.read() or b"{}")
        connection.close()
        return response.status, body

    def test_shape_and_accumulation_across_all_five_streams(self) -> None:
        status, body, _ = self.request("GET", "/v1/metrics")
        self.assertEqual(status, 200)
        self.assertEqual(body, {"metrics": {"decisions": ZERO_DECISIONS}})

        self.request("PUT", "/v1/limits/m-1", {"capacity": 1, "refill_per_second": 1})
        self.request("POST", "/v1/check", {"key": "m-1"})
        self.request("POST", "/v1/check", {"key": "m-1"})          # over_quota
        self.request("PUT", "/v1/limits/m-2", {"capacity": 1, "refill_per_second": 1})
        self.request("POST", "/v1/hierarchies/check", {"keys": ["m-1", "m-2"], "cost": 1})  # over_quota
        self.request("POST", "/v1/reservations", {"key": "m-2", "cost": 1})
        self.request("POST", "/v1/hierarchies/reservations",
                     {"keys": ["m-1", "m-2"], "cost": 1, "ttl_seconds": 10})  # over_quota
        self.request("PUT", "/v1/windows/mw", {"window_seconds": 60, "max_events": 1})
        self.request("POST", "/v1/windows/mw/check", {})
        self.request("POST", "/v1/windows/mw/check", {})           # over_quota

        # Errors and reads that must not move the counters.
        self.request("POST", "/v1/check", {"key": "m-1", "cost": "bad"})   # 400
        self.request("POST", "/v1/check", {"key": "absent"})               # 404
        self.request("GET", "/v1/limits/m-1")
        self.request("GET", "/v1/ledgers/m-1")

        status, body, _ = self.request("GET", "/v1/metrics")
        self.assertEqual(status, 200)
        self.assertEqual(body, {"metrics": {"decisions": {
            "check": {"allowed": 1, "over_quota": 1},
            "hierarchy_check": {"allowed": 0, "over_quota": 1},
            "reservation": {"allowed": 1, "over_quota": 0},
            "hierarchy_reservation": {"allowed": 0, "over_quota": 1},
            "window_check": {"allowed": 1, "over_quota": 1},
        }}})

    def test_query_parameters_are_400_but_bare_question_mark_is_allowed(self) -> None:
        status, body, _ = self.request("GET", "/v1/metrics?x=1")
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        status, body, _ = self.request("GET", "/v1/metrics?x=1&y=2")
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        status, body, _ = self.request("GET", "/v1/metrics?=")
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        self.assertEqual(self.request("GET", "/v1/metrics?")[0], 200)

    def test_wrong_segments_and_methods_are_404(self) -> None:
        self.assertEqual(self.request("GET", "/v1/metrics/x")[0], 404)
        self.assertEqual(self.request("GET", "/v1")[0], 404)
        self.assertEqual(self.request("GET", "/metrics")[0], 404)
        self.assertEqual(self.request("POST", "/v1/metrics", {})[0], 404)
        self.assertEqual(self.request("PUT", "/v1/metrics", {})[0], 404)
        self.assertEqual(self.request("DELETE", "/v1/metrics")[0], 404)

    def test_bad_path_and_method_do_not_read_the_body(self) -> None:
        # Garbage/absent body with no Content-Length on a mismatched route still resolves to 404,
        # never a 400 from body handling.
        self.assertEqual(self.raw_request("POST", "/v1/metrics", b'{"x": 1}')[0], 404)
        self.assertEqual(self.raw_request("PUT", "/v1/metrics/x", b'[]')[0], 404)


if __name__ == "__main__":
    unittest.main()
