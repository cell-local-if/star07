"""POST /v1/reservations: a legal cost above the key's current capacity is unsatisfiable.

Mirrors POST /v1/check's cost-exceeds-capacity rule for single-key holds: such a request can
never be reserved no matter how long the caller waits, so the entry rejects it with 400
invalid_request (never 200/429, never a Retry-After) inside the same critical section that
concurrent PUTs serialize on, before the clock is sampled — no watermark advance, no expiry
settle, no refill, no deduction, no reservation, no decision count. Cost exactly AT the capacity
stays a legal request and keeps the ordinary availability judgement.
"""
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

    def __call__(self) -> float:
        return self.t


class ReserveCostExceedsCapacityUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.limiter = Limiter(self.clock)
        self.limiter.configure("tenant-a", {"capacity": 5, "refill_per_second": 1.0})

    def test_cost_equal_to_capacity_is_still_held(self) -> None:
        result = self.limiter.reserve("tenant-a", 5)
        self.assertEqual((result["cost"], result["remaining"], result["capacity"]), (5, 0, 5))
        self.assertIn(result["reservation_id"], self.limiter._reservations)

    def test_cost_above_capacity_is_invalid_request_and_stable(self) -> None:
        for cost in (6, 1_000_000):
            with self.assertRaises(InvalidRequest) as raised:
                self.limiter.reserve("tenant-a", cost)
            self.assertIn("exceeds capacity 5", str(raised.exception))
        with self.assertRaises(InvalidRequest):
            self.limiter.reserve("tenant-a", 6)

    def test_unknown_key_with_large_cost_is_still_not_found(self) -> None:
        with self.assertRaises(LimitNotFound):
            self.limiter.reserve("tenant-x", 1_000_000)

    def test_format_validation_still_precedes_the_capacity_check(self) -> None:
        # Illegal cost/ttl are invalid_request before the lock, even against an unknown key.
        for bad_cost in (0, True, 1.5, "6"):
            with self.assertRaises(InvalidRequest):
                self.limiter.reserve("tenant-a", bad_cost)
            with self.assertRaises(InvalidRequest):
                self.limiter.reserve("tenant-x", bad_cost)
        with self.assertRaises(InvalidRequest):
            self.limiter.reserve("tenant-a", 6, ttl_seconds=0)
        # A LEGAL oversized cost on an unconfigured key keeps the 404, exactly as for check.
        with self.assertRaises(LimitNotFound):
            self.limiter.reserve("tenant-x", 6)

    def test_rejection_creates_no_hold_and_leaves_state_ledger_metrics_untouched(self) -> None:
        self.assertTrue(self.limiter.check("tenant-a", 2)["allowed"])   # tokens 3, used 2
        before_metrics = self.limiter.metrics()
        with self.assertRaises(InvalidRequest):
            self.limiter.reserve("tenant-a", 6)
        self.assertEqual(self.limiter._reservations, {})                # no hold registered
        state = self.limiter.state("tenant-a")
        self.assertEqual((state["remaining"], state["used"]), (3, 2))
        self.assertEqual(self.limiter.ledger("tenant-a")["totals"],
                         {"accepted_count": 1, "accepted_cost": 2})
        self.assertEqual(self.limiter.metrics(), before_metrics)

    def test_rejection_never_samples_the_clock_or_advances_the_watermark(self) -> None:
        self.limiter.reserve("tenant-a", 5, ttl_seconds=10)             # empty at t=1000
        self.clock.t = 1005.0
        with self.assertRaises(InvalidRequest):
            self.limiter.reserve("tenant-a", 6)                         # must not tick
        self.clock.t = 1004.0
        # Had the rejection sampled 1005, the watermark would pin there and this read would
        # show 5 tokens; untouched, the effective moment is still 1000 and 1004 refills 4.
        self.assertEqual(self.limiter.state("tenant-a")["remaining"], 4)

    def test_rejection_settles_no_due_reservation(self) -> None:
        reservation = self.limiter.reserve("tenant-a", 3, ttl_seconds=10)   # tokens 2
        self.clock.t += 10                                                  # hold is now due
        with self.assertRaises(InvalidRequest):
            self.limiter.reserve("tenant-a", 6)
        self.assertIn(reservation["reservation_id"], self.limiter._reservations)
        state = self.limiter.state("tenant-a")                              # this read settles it
        self.assertEqual((state["remaining"], state["used"]), (5, 0))

    def test_cost_at_capacity_but_above_live_tokens_is_still_429(self) -> None:
        self.limiter.check("tenant-a", 3)                              # 2 tokens live
        # cost 5 == capacity: unsatisfiable-never, so it keeps the temporary-shortfall 429.
        with self.assertRaises(OverQuota) as raised:
            self.limiter.reserve("tenant-a", 5)
        self.assertEqual(raised.exception.retry_after, 3.0)
        self.assertEqual(self.limiter._reservations, {})

    def test_capacity_change_flips_the_verdict_atomically(self) -> None:
        with self.assertRaises(InvalidRequest):
            self.limiter.reserve("tenant-a", 6)
        # Capacity raised above the cost leaves invalid_request territory; a capacity increase
        # never conjures tokens, so the untouched 5-token bucket makes this a temporary 429.
        self.limiter.configure("tenant-a", {"capacity": 10, "refill_per_second": 1.0})
        with self.assertRaises(OverQuota):
            self.limiter.reserve("tenant-a", 6)
        self.limiter.configure("tenant-a", {"capacity": 5, "refill_per_second": 1.0})
        with self.assertRaises(InvalidRequest):
            self.limiter.reserve("tenant-a", 6)

    def test_concurrent_reconfigure_and_reserve_never_see_a_torn_capacity(self) -> None:
        limiter = Limiter(self.clock)                   # clock frozen for the whole test
        limiter.configure("hot", {"capacity": 10, "refill_per_second": 0.0001})
        stop = threading.Event()
        outcomes: list[str] = []
        errors: list[BaseException] = []
        list_lock = threading.Lock()

        def reconfigure() -> None:
            try:
                while not stop.is_set():
                    limiter.configure("hot", {"capacity": 5, "refill_per_second": 0.0001})
                    limiter.configure("hot", {"capacity": 10, "refill_per_second": 0.0001})
            except BaseException as error:  # noqa: BLE001 - surface thread failures on main thread
                with list_lock:
                    errors.append(error)

        def reserve() -> None:
            try:
                for _ in range(50):
                    try:
                        result = limiter.reserve("hot", 6, ttl_seconds=3600)
                        if result["capacity"] != 10:
                            raise AssertionError(f"held against capacity {result['capacity']}")
                        outcome = "allowed"
                    except InvalidRequest:
                        outcome = "invalid"             # saw capacity 5: one coherent config
                    except OverQuota:
                        outcome = "over_quota"          # saw capacity 10, bucket too empty
                    with list_lock:
                        outcomes.append(outcome)
            except BaseException as error:  # noqa: BLE001
                with list_lock:
                    errors.append(error)

        threads = [threading.Thread(target=reconfigure) for _ in range(2)]
        threads += [threading.Thread(target=reserve) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads[2:]:
            thread.join()
        stop.set()
        for thread in threads[:2]:
            thread.join()
        self.assertEqual(errors, [])
        self.assertEqual(len(outcomes), 400)
        self.assertLessEqual(set(outcomes), {"allowed", "invalid", "over_quota"})
        self.assertIn("invalid", outcomes)
        limiter.configure("hot", {"capacity": 5, "refill_per_second": 0.0001})
        with self.assertRaises(InvalidRequest):
            limiter.reserve("hot", 6)


class ReserveCostExceedsCapacityHttpTests(unittest.TestCase):
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

    def test_cost_above_capacity_is_400_without_retry_after(self) -> None:
        self.request("PUT", "/v1/limits/rc-1", {"capacity": 5, "refill_per_second": 1})
        status, body, headers = self.request("POST", "/v1/reservations", {"key": "rc-1", "cost": 6})
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        self.assertIn("exceeds capacity 5", body["error"]["message"])
        self.assertNotIn("Retry-After", headers)
        self.assertEqual(self.request("POST", "/v1/reservations", {"key": "rc-1", "cost": 6})[0], 400)
        # The boundary itself still creates a hold.
        status, body, _ = self.request("POST", "/v1/reservations", {"key": "rc-1", "cost": 5})
        self.assertEqual((status, body["remaining"], body["capacity"]), (200, 0, 5))

    def test_unknown_key_with_large_cost_is_404(self) -> None:
        status, body, headers = self.request("POST", "/v1/reservations",
                                             {"key": "rc-absent", "cost": 1_000_000})
        self.assertEqual((status, body["error"]["code"]), (404, "not_found"))
        self.assertNotIn("Retry-After", headers)

    def test_rejection_changes_no_state_revision_or_metrics(self) -> None:
        self.request("PUT", "/v1/limits/rc-2", {"capacity": 4, "refill_per_second": 1})
        self.request("POST", "/v1/check", {"key": "rc-2", "cost": 1})   # tokens 3, used 1
        _, _, before_state_headers = self.request("GET", "/v1/limits/rc-2")
        etag_before = before_state_headers.get("ETag")
        _, before_metrics, _ = self.request("GET", "/v1/metrics")
        status, _, headers = self.request("POST", "/v1/reservations", {"key": "rc-2", "cost": 5})
        self.assertEqual(status, 400)
        self.assertNotIn("Retry-After", headers)
        _, state, state_headers = self.request("GET", "/v1/limits/rc-2")
        self.assertEqual((state["remaining"], state["used"]), (3, 1))
        self.assertEqual(state_headers.get("ETag"), etag_before)
        _, ledger, _ = self.request("GET", "/v1/ledgers/rc-2")
        self.assertEqual(ledger["totals"], {"accepted_count": 1, "accepted_cost": 1})
        _, after_metrics, _ = self.request("GET", "/v1/metrics")
        self.assertEqual(after_metrics, before_metrics)


if __name__ == "__main__":
    unittest.main()
