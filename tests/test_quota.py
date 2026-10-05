"""Baseline tests for the rate limiter: deterministic because time is injected."""
from __future__ import annotations

import json
import threading
import unittest
import urllib.error
import urllib.request

from quota import InvalidRequest, LimitNotFound, Limiter, OverQuota, ReservationNotFound


class Clock:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t


class LimiterUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.limiter = Limiter(self.clock)
        self.limiter.configure("tenant-a", {"capacity": 3, "refill_per_second": 1.0})

    def test_burst_then_over_quota_with_retry_after(self) -> None:
        for _ in range(3):
            self.assertTrue(self.limiter.check("tenant-a", 1)["allowed"])
        with self.assertRaises(OverQuota) as raised:
            self.limiter.check("tenant-a", 1)
        self.assertAlmostEqual(raised.exception.retry_after, 1.0, places=6)

    def test_refill_is_proportional_to_elapsed_time(self) -> None:
        self.limiter.check("tenant-a", 3)
        self.clock.t += 2.0           # 2 seconds of refill at 1 token/s
        result = self.limiter.check("tenant-a", 2)
        self.assertEqual(result["remaining"], 0)

    def test_keys_are_isolated_and_unknown_key_is_404(self) -> None:
        self.limiter.configure("tenant-b", {"capacity": 1, "refill_per_second": 0.5})
        self.limiter.check("tenant-b", 1)
        self.assertTrue(self.limiter.check("tenant-a", 1)["allowed"])
        with self.assertRaises(LimitNotFound):
            self.limiter.check("tenant-c", 1)

    def test_invalid_configuration_and_cost_are_rejected(self) -> None:
        for bad in [{"capacity": 0, "refill_per_second": 1}, {"capacity": 1}, {"capacity": 1, "refill_per_second": -1},
                    "nope", {"capacity": 1, "refill_per_second": 1, "extra": 1}]:
            with self.assertRaises(InvalidRequest):
                self.limiter.configure("tenant-d", bad)
        with self.assertRaises(InvalidRequest):
            self.limiter.check("tenant-a", 0)


class ReservationUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.limiter = Limiter(self.clock)
        self.limiter.configure("tenant-a", {"capacity": 5, "refill_per_second": 1.0})

    def test_reserve_deducts_immediately_but_does_not_count_as_used(self) -> None:
        result = self.limiter.reserve("tenant-a", 2)
        self.assertEqual((result["key"], result["cost"], result["remaining"], result["capacity"]),
                         ("tenant-a", 2, 3, 5))
        self.assertTrue(result["reservation_id"])
        state = self.limiter.state("tenant-a")
        self.assertEqual((state["remaining"], state["used"]), (3, 0))

    def test_rollback_returns_tokens_and_is_one_shot(self) -> None:
        reservation_id = self.limiter.reserve("tenant-a", 2)["reservation_id"]
        self.clock.t += 1.0  # 1 token refills while reserved
        result = self.limiter.rollback(reservation_id)
        self.assertEqual((result["rolled_back"], result["remaining"]), (True, 5))
        with self.assertRaises(ReservationNotFound):
            self.limiter.rollback(reservation_id)
        with self.assertRaises(ReservationNotFound):
            self.limiter.rollback("never-existed")

    def test_rollback_is_capped_at_current_capacity(self) -> None:
        reservation_id = self.limiter.reserve("tenant-a", 2)["reservation_id"]
        self.clock.t += 10.0  # bucket refills to capacity 5 while 2 are reserved
        result = self.limiter.rollback(reservation_id)
        self.assertEqual(result["remaining"], 5)  # 3 + 2 capped at 5, not 7

    def test_rollback_uses_capacity_from_reconfiguration(self) -> None:
        reservation_id = self.limiter.reserve("tenant-a", 4)["reservation_id"]
        self.limiter.configure("tenant-a", {"capacity": 2, "refill_per_second": 1.0})
        result = self.limiter.rollback(reservation_id)
        self.assertEqual((result["remaining"], result["capacity"]), (2, 2))

    def test_reserve_validation_and_unknown_key(self) -> None:
        for bad_key in [None, 1, "", "x" * 201]:
            with self.assertRaises(InvalidRequest):
                self.limiter.reserve(bad_key, 1)
        for bad_cost in [0, -1, 1_000_001, 1.5, True, "1"]:
            with self.assertRaises(InvalidRequest):
                self.limiter.reserve("tenant-a", bad_cost)
        with self.assertRaises(LimitNotFound):
            self.limiter.reserve("ghost", 1)

    def test_reserve_over_quota_carries_retry_after(self) -> None:
        self.limiter.reserve("tenant-a", 4)
        with self.assertRaises(OverQuota) as raised:
            self.limiter.reserve("tenant-a", 2)
        self.assertGreaterEqual(raised.exception.retry_after, 1.0)  # deficit 1 at 1 token/s

    def test_reserve_and_check_share_one_atomic_budget(self) -> None:
        self.limiter.configure("hot", {"capacity": 10, "refill_per_second": 1e-9})  # ~no refill during the test
        successes: list[str] = []
        lock = threading.Lock()

        def attempt(i: int) -> None:
            try:
                if i % 2:
                    self.limiter.reserve("hot", 1)
                else:
                    self.limiter.check("hot", 1)
                with lock:
                    successes.append(str(i))
            except OverQuota:
                pass

        threads = [threading.Thread(target=attempt, args=(i,)) for i in range(40)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(len(successes), 10)  # never oversold, however the threads interleave


class HttpSurfaceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
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

    def test_health_and_configure_then_check(self) -> None:
        self.assertEqual(self.request("GET", "/health")[0], 200)
        status, body, _ = self.request("PUT", "/v1/limits/t-1", {"capacity": 2, "refill_per_second": 2})
        self.assertEqual((status, body["limit"]["capacity"]), (200, 2))
        self.assertEqual(self.request("POST", "/v1/check", {"key": "t-1"})[0], 200)

    def test_over_quota_returns_429_with_retry_after(self) -> None:
        self.request("PUT", "/v1/limits/t-2", {"capacity": 1, "refill_per_second": 1})
        self.request("POST", "/v1/check", {"key": "t-2"})
        status, body, headers = self.request("POST", "/v1/check", {"key": "t-2"})
        self.assertEqual((status, body["error"]["code"]), (429, "over_quota"))
        self.assertIn("Retry-After", headers)

    def test_unknown_key_and_route(self) -> None:
        self.assertEqual(self.request("GET", "/v1/limits/absent")[0], 404)
        self.assertEqual(self.request("POST", "/v1/nope", {})[0], 404)

    def test_reserve_then_rollback_round_trip(self) -> None:
        self.request("PUT", "/v1/limits/t-3", {"capacity": 3, "refill_per_second": 1})
        status, body, _ = self.request("POST", "/v1/reservations", {"key": "t-3", "cost": 2})
        self.assertEqual(status, 200)
        self.assertEqual((body["key"], body["cost"], body["remaining"], body["capacity"]), ("t-3", 2, 1, 3))
        reservation_id = body["reservation_id"]
        state = self.request("GET", "/v1/limits/t-3")[1]
        self.assertEqual((state["remaining"], state["used"]), (1, 0))
        status, body, _ = self.request("DELETE", f"/v1/reservations/{reservation_id}")
        self.assertEqual((status, body["rolled_back"], body["remaining"]), (200, True, 3))
        self.assertEqual(self.request("GET", "/v1/limits/t-3")[1]["remaining"], 3)
        # one-shot: a second delete and unknown ids are 404
        self.assertEqual(self.request("DELETE", f"/v1/reservations/{reservation_id}")[0], 404)
        self.assertEqual(self.request("DELETE", "/v1/reservations/nope")[0], 404)
        self.assertEqual(self.request("DELETE", "/v1/reservations")[0], 404)

    def test_reserve_error_semantics(self) -> None:
        self.request("PUT", "/v1/limits/t-4", {"capacity": 1, "refill_per_second": 2})
        self.assertEqual(self.request("POST", "/v1/reservations", {"key": "ghost"})[0], 404)
        self.assertEqual(self.request("POST", "/v1/reservations", {"key": "t-4", "cost": True})[0], 400)
        self.assertEqual(self.request("POST", "/v1/reservations", {"key": ""})[0], 400)
        self.assertEqual(self.request("POST", "/v1/reservations", {"key": "t-4", "extra": 1})[0], 400)
        self.request("POST", "/v1/reservations", {"key": "t-4"})  # cost defaults to 1, bucket now empty
        status, body, headers = self.request("POST", "/v1/reservations", {"key": "t-4", "cost": 1})
        self.assertEqual((status, body["error"]["code"]), (429, "over_quota"))
        self.assertGreaterEqual(float(headers["Retry-After"]), 0.5)  # 1 token at 2 tokens/s


if __name__ == "__main__":
    unittest.main()
