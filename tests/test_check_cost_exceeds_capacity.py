"""POST /v1/check: a legal cost above the key's current capacity is unsatisfiable.

Such a request can never be admitted no matter how long the caller waits, so the check
entry rejects it with 400 invalid_request (never 200/429, never a Retry-After) instead of
hinting a finite wait. The rejection is decided inside the same critical section that
concurrent PUTs serialize on, before the clock is sampled: no watermark advance, no expiry
settle, no refill, no deduction, no ledger event, no decision count.
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


class CostExceedsCapacityUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.limiter = Limiter(self.clock)
        self.limiter.configure("tenant-a", {"capacity": 5, "refill_per_second": 1.0})

    def test_cost_equal_to_capacity_is_still_admitted(self) -> None:
        result = self.limiter.check("tenant-a", 5)
        self.assertTrue(result["allowed"])
        self.assertEqual((result["remaining"], result["capacity"]), (0, 5))

    def test_cost_above_capacity_is_invalid_request_and_stable(self) -> None:
        for cost in (6, 1_000_000):
            with self.assertRaises(InvalidRequest) as raised:
                self.limiter.check("tenant-a", cost)
            self.assertIn("exceeds capacity 5", str(raised.exception))
        # Stable across repeats: still rejected, never downgraded to over_quota.
        with self.assertRaises(InvalidRequest):
            self.limiter.check("tenant-a", 6)

    def test_unknown_key_with_large_cost_is_still_not_found(self) -> None:
        with self.assertRaises(LimitNotFound):
            self.limiter.check("tenant-x", 1_000_000)

    def test_rejection_leaves_state_ledger_and_metrics_untouched(self) -> None:
        self.assertTrue(self.limiter.check("tenant-a", 2)["allowed"])   # tokens 3, used 2
        before_metrics = self.limiter.metrics()
        with self.assertRaises(InvalidRequest):
            self.limiter.check("tenant-a", 6)
        state = self.limiter.state("tenant-a")
        self.assertEqual((state["remaining"], state["used"]), (3, 2))
        self.assertEqual(self.limiter.ledger("tenant-a")["totals"],
                         {"accepted_count": 1, "accepted_cost": 2})
        # The rejected check counted neither an allowed nor an over_quota decision.
        self.assertEqual(self.limiter.metrics(), before_metrics)

    def test_rejection_never_samples_the_clock_or_advances_the_watermark(self) -> None:
        self.limiter.check("tenant-a", 5)                               # empty at t=1000
        self.clock.t = 1005.0
        with self.assertRaises(InvalidRequest):
            self.limiter.check("tenant-a", 6)                           # must not tick
        self.clock.t = 1004.0
        # Had the rejection sampled 1005, the watermark would pin there and this read would
        # show 5 tokens; untouched, the effective moment is still 1000 and 1004 refills 4.
        self.assertEqual(self.limiter.state("tenant-a")["remaining"], 4)

    def test_rejection_settles_no_due_reservation(self) -> None:
        reservation = self.limiter.reserve("tenant-a", 3, ttl_seconds=10)   # tokens 2
        self.clock.t += 10                                                  # hold is now due
        with self.assertRaises(InvalidRequest):
            self.limiter.check("tenant-a", 6)
        self.assertIn(reservation["reservation_id"], self.limiter._reservations)
        state = self.limiter.state("tenant-a")                              # this read settles it
        self.assertEqual((state["remaining"], state["used"]), (5, 0))

    def test_capacity_change_flips_the_verdict_atomically(self) -> None:
        with self.assertRaises(InvalidRequest):
            self.limiter.check("tenant-a", 6)
        # Reconfigure up: cost 6 is now satisfiable, so the verdict leaves invalid_request
        # territory (the 5 surviving tokens make this one over_quota, never a capacity 400).
        self.limiter.configure("tenant-a", {"capacity": 10, "refill_per_second": 1.0})
        with self.assertRaises(OverQuota):
            self.limiter.check("tenant-a", 6)
        self.limiter.configure("tenant-a", {"capacity": 5, "refill_per_second": 1.0})
        with self.assertRaises(InvalidRequest):
            self.limiter.check("tenant-a", 6)

    def test_concurrent_reconfigure_and_check_never_see_a_torn_capacity(self) -> None:
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
            except BaseException as error:  # noqa: BLE001 - surface thread failures on the main thread
                with list_lock:
                    errors.append(error)

        def check() -> None:
            try:
                for _ in range(50):
                    try:
                        result = limiter.check("hot", 6)
                        # Only the capacity-10 configuration can ever admit cost 6.
                        if result["capacity"] != 10:
                            raise AssertionError(f"admitted against capacity {result['capacity']}")
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
        threads += [threading.Thread(target=check) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads[2:]:
            thread.join()
        stop.set()
        for thread in threads[:2]:
            thread.join()
        self.assertEqual(errors, [])
        self.assertEqual(len(outcomes), 400)
        # Every request observed exactly one whole configuration: no intermediate capacities,
        # no unexpected error kinds, and the limiter is still coherent afterwards.
        self.assertLessEqual(set(outcomes), {"allowed", "invalid", "over_quota"})
        self.assertIn("invalid", outcomes)
        limiter.configure("hot", {"capacity": 5, "refill_per_second": 0.0001})
        with self.assertRaises(InvalidRequest):
            limiter.check("hot", 6)


class CostExceedsCapacityHttpTests(unittest.TestCase):
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
        self.request("PUT", "/v1/limits/xc-1", {"capacity": 5, "refill_per_second": 1})
        status, body, headers = self.request("POST", "/v1/check", {"key": "xc-1", "cost": 6})
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        self.assertIn("exceeds capacity 5", body["error"]["message"])
        self.assertNotIn("Retry-After", headers)
        # Stable on repetition, and the boundary itself still admits.
        self.assertEqual(self.request("POST", "/v1/check", {"key": "xc-1", "cost": 6})[0], 400)
        status, body, _ = self.request("POST", "/v1/check", {"key": "xc-1", "cost": 5})
        self.assertEqual((status, body["allowed"], body["remaining"]), (200, True, 0))

    def test_unknown_key_with_large_cost_is_404(self) -> None:
        status, body, headers = self.request("POST", "/v1/check",
                                             {"key": "xc-absent", "cost": 1_000_000})
        self.assertEqual((status, body["error"]["code"]), (404, "not_found"))
        self.assertNotIn("Retry-After", headers)

    def test_format_validation_still_precedes_the_key_and_capacity_checks(self) -> None:
        # An illegal cost is 400 even for an unknown key: format beats 404, as before.
        status, body, _ = self.request("POST", "/v1/check", {"key": "xc-absent", "cost": 0})
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        status, body, _ = self.request("POST", "/v1/check", {"key": "xc-absent", "cost": True})
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))

    def test_rejection_changes_no_state_or_metrics(self) -> None:
        self.request("PUT", "/v1/limits/xc-2", {"capacity": 4, "refill_per_second": 1})
        self.request("POST", "/v1/check", {"key": "xc-2", "cost": 1})   # tokens 3, used 1
        _, before, _ = self.request("GET", "/v1/metrics")
        status, _, _ = self.request("POST", "/v1/check", {"key": "xc-2", "cost": 5})
        self.assertEqual(status, 400)
        _, state, _ = self.request("GET", "/v1/limits/xc-2")
        self.assertEqual((state["remaining"], state["used"]), (3, 1))
        _, ledger, _ = self.request("GET", "/v1/ledgers/xc-2")
        self.assertEqual(ledger["totals"], {"accepted_count": 1, "accepted_cost": 1})
        _, after, _ = self.request("GET", "/v1/metrics")
        self.assertEqual(after, before)

    def test_other_entry_points_are_unaffected(self) -> None:
        # The same oversized cost through reservations keeps the baseline 429 + Retry-After.
        self.request("PUT", "/v1/limits/xc-3", {"capacity": 2, "refill_per_second": 1})
        status, body, headers = self.request("POST", "/v1/reservations", {"key": "xc-3", "cost": 3})
        self.assertEqual((status, body["error"]["code"]), (429, "over_quota"))
        self.assertIn("Retry-After", headers)
        # And a same-cost hierarchy check keeps its own semantics too.
        self.request("PUT", "/v1/limits/xc-4", {"capacity": 2, "refill_per_second": 1})
        status, _, headers = self.request("POST", "/v1/hierarchies/check",
                                          {"keys": ["xc-3", "xc-4"], "cost": 3})
        self.assertEqual(status, 429)
        self.assertIn("Retry-After", headers)


if __name__ == "__main__":
    unittest.main()
