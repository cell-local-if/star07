"""POST /v1/check: a legal cost above the key's current capacity is unsatisfiable.

Such a request can never enter the bucket (tokens refill only up to capacity), so instead of a
finite Retry-After it gets 400 invalid_request before the clock is sampled, with every piece of
state left untouched. cost == capacity keeps its ordinary 200/429 judgement and an unknown key
keeps its 404, since neither has a configuration to compare against.
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


class CostVsCapacityUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.limiter = Limiter(self.clock)
        self.limiter.configure("tenant-a", {"capacity": 5, "refill_per_second": 1.0})

    def test_cost_equal_to_capacity_is_judged_normally(self) -> None:
        # Full bucket: cost == capacity is admitted, not rejected as over-capacity.
        result = self.limiter.check("tenant-a", 5)
        self.assertEqual((result["allowed"], result["remaining"], result["capacity"]), (True, 0, 5))
        # Empty bucket: the same boundary cost is an ordinary 429 with a finite hint, never 400.
        with self.assertRaises(OverQuota) as raised:
            self.limiter.check("tenant-a", 5)
        self.assertEqual(raised.exception.retry_after, 5.0)
        self.assertEqual(self.limiter.metrics()["metrics"]["decisions"]["check"],
                         {"allowed": 1, "over_quota": 1})

    def test_cost_just_above_capacity_is_invalid_request(self) -> None:
        with self.assertRaises(InvalidRequest) as raised:
            self.limiter.check("tenant-a", 6)
        message = str(raised.exception)
        self.assertIn("6", message)
        self.assertIn("5", message)
        self.assertIn("capacity", message)
        self.assertFalse(hasattr(raised.exception, "retry_after"))

    def test_rejection_is_stable_however_long_the_clock_advances(self) -> None:
        for _ in range(3):
            with self.assertRaises(InvalidRequest):
                self.limiter.check("tenant-a", 6)
        self.clock.t += 10_000.0                     # waiting forever still cannot fit cost 6
        with self.assertRaises(InvalidRequest):
            self.limiter.check("tenant-a", 6)

    def test_unknown_key_with_a_large_legal_cost_is_not_found(self) -> None:
        # No configuration to compare against: 404, not the new capacity 400.
        with self.assertRaises(LimitNotFound):
            self.limiter.check("tenant-missing", 1_000_000)

    def test_format_errors_still_beat_the_capacity_rule_and_the_404(self) -> None:
        for bad_cost in (0, 1_000_001, True, 1.5, "6", None):
            with self.assertRaises(InvalidRequest):
                self.limiter.check("tenant-a", bad_cost)
        with self.assertRaises(InvalidRequest):      # bad cost on unknown key: 400 before 404
            self.limiter.check("tenant-missing", 0)
        for bad_key in (1, "", "x" * 201, True, None):
            with self.assertRaises(InvalidRequest):
                self.limiter.check(bad_key, 6)

    def test_rejection_samples_no_clock_and_leaves_no_trace(self) -> None:
        self.limiter.check("tenant-a", 3)            # tokens 2, used 3, watermark anchored at t=1000
        self.clock.t = 1010.0                        # a normal call would now refill to the cap
        watermark_before = self.limiter._watermark
        for _ in range(3):
            with self.assertRaises(InvalidRequest):
                self.limiter.check("tenant-a", 6)
        # ...the rejected calls never sampled the clock: watermark and bucket stamp stay put.
        self.assertEqual(self.limiter._watermark, watermark_before)
        self.clock.t = 995.0                         # below the skipped reading: effective stays 1000
        state = self.limiter.state("tenant-a")      # had the rejection ticked, this would refill to 5
        self.assertEqual((state["remaining"], state["used"]), (2, 3))
        self.assertEqual(self.limiter.ledger("tenant-a")["totals"],
                         {"accepted_count": 1, "accepted_cost": 3})
        self.assertEqual(self.limiter.metrics()["metrics"]["decisions"]["check"],
                         {"allowed": 1, "over_quota": 0})

    def test_rejection_settles_no_due_reservation(self) -> None:
        self.clock.t = 1000.0
        limiter = Limiter(self.clock)
        limiter.configure("held", {"capacity": 5, "refill_per_second": 0.0001})
        reservation = limiter.reserve("held", 1, ttl_seconds=10)  # tokens 4, due at 1010
        self.clock.t = 1010.0                        # the hold is now due
        with self.assertRaises(InvalidRequest):
            limiter.check("held", 6)                 # over-capacity: no settle may run
        # Still held: the rejected check neither refunded it nor unlinked it (a read of the ledger
        # deliberately samples no clock and settles nothing, so it cannot mask this observation).
        self.assertIn(reservation["reservation_id"], limiter._reservations)
        self.assertEqual(limiter.ledger("held")["totals"],
                         {"accepted_count": 0, "accepted_cost": 0})
        # A legal check at the same effective moment settles the refund (capped at 5) and then pays:
        # 4 + negligible refill + 1 refund, capped to 5, minus cost 1 -> remaining 4, used 1.
        result = limiter.check("held", 1)
        self.assertTrue(result["allowed"])
        self.assertEqual(result["remaining"], 4)
        self.assertNotIn(reservation["reservation_id"], limiter._reservations)
        self.assertEqual(limiter.state("held")["used"], 1)

    def test_hot_reconfigure_switches_the_classification(self) -> None:
        with self.assertRaises(InvalidRequest):      # capacity 5: cost 6 unsatisfiable
            self.limiter.check("tenant-a", 6)
        self.limiter.configure("tenant-a", {"capacity": 10, "refill_per_second": 1.0})
        # Capacity 10 with only 5 tokens: the request is now satisfiable, so it is an ordinary
        # finite 429 — never 400 — judged against exactly the installed configuration.
        with self.assertRaises(OverQuota) as raised:
            self.limiter.check("tenant-a", 6)
        self.assertEqual(raised.exception.retry_after, 1.0)
        self.limiter.configure("tenant-a", {"capacity": 5, "refill_per_second": 1.0})
        with self.assertRaises(InvalidRequest):
            self.limiter.check("tenant-a", 6)

    def test_concurrent_reconfigure_and_check_never_observe_a_torn_capacity(self) -> None:
        limiter = Limiter(self.clock)                # frozen clock: tokens never leave 5
        limiter.configure("hot", {"capacity": 5, "refill_per_second": 1.0})
        outcomes: list[str] = []
        errors: list[BaseException] = []
        outcomes_lock = threading.Lock()
        barrier = threading.Barrier(10)

        def reconfigure() -> None:
            try:
                barrier.wait()
                for index in range(200):
                    capacity = 5 if index % 2 == 0 else 10
                    limiter.configure("hot", {"capacity": capacity, "refill_per_second": 1.0})
            except BaseException as error:  # noqa: BLE001 - surface thread failures on the main thread
                with outcomes_lock:
                    errors.append(error)

        def spend() -> None:
            try:
                barrier.wait()
                for _ in range(200):
                    try:
                        limiter.check("hot", 6)
                        outcome = "allowed"         # impossible with 5 tokens under a frozen clock
                    except InvalidRequest:
                        outcome = "unsatisfiable"   # proves the request saw capacity 5
                    except OverQuota:
                        outcome = "over_quota"      # proves the request saw capacity 10
                    with outcomes_lock:
                        outcomes.append(outcome)
            except BaseException as error:  # noqa: BLE001
                with outcomes_lock:
                    errors.append(error)

        threads = [threading.Thread(target=reconfigure) for _ in range(2)]
        threads += [threading.Thread(target=spend) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        self.assertTrue(outcomes)
        self.assertTrue(set(outcomes) <= {"unsatisfiable", "over_quota"})
        # Every classification witnesses one single installed configuration, and both were in force.
        self.assertEqual(set(outcomes), {"unsatisfiable", "over_quota"})
        # The unsatisfiable 400s booked nothing; the 429s booked nothing either; no 200 happened.
        state = limiter.state("hot")
        self.assertEqual((state["remaining"], state["used"]), (5, 0))
        self.assertEqual(limiter.ledger("hot")["totals"],
                         {"accepted_count": 0, "accepted_cost": 0})
        decisions = limiter.metrics()["metrics"]["decisions"]["check"]
        self.assertEqual(decisions["allowed"], 0)
        self.assertEqual(decisions["over_quota"], outcomes.count("over_quota"))


class CostVsCapacityHttpTests(unittest.TestCase):
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

    def test_cost_equal_to_capacity_keeps_200_then_429(self) -> None:
        self.request("PUT", "/v1/limits/cc-1", {"capacity": 4, "refill_per_second": 1})
        status, body, _ = self.request("POST", "/v1/check", {"key": "cc-1", "cost": 4})
        self.assertEqual((status, body["allowed"], body["remaining"]), (200, True, 0))
        status, body, headers = self.request("POST", "/v1/check", {"key": "cc-1", "cost": 4})
        self.assertEqual((status, body["error"]["code"]), (429, "over_quota"))
        self.assertIn("Retry-After", headers)

    def test_cost_above_capacity_is_400_without_retry_after(self) -> None:
        self.request("PUT", "/v1/limits/cc-2", {"capacity": 4, "refill_per_second": 1})
        for _ in range(3):
            status, body, headers = self.request("POST", "/v1/check", {"key": "cc-2", "cost": 5})
            self.assertEqual(status, 400)
            self.assertEqual(body["error"]["code"], "invalid_request")
            self.assertIn("capacity", body["error"]["message"])
            self.assertNotIn("Retry-After", headers)

    def test_unknown_key_and_shape_errors_keep_their_priority(self) -> None:
        # A legal large cost against an unknown key is still 404.
        status, body, _ = self.request("POST", "/v1/check", {"key": "cc-missing", "cost": 1_000_000})
        self.assertEqual((status, body["error"]["code"]), (404, "not_found"))
        # Malformed cost on an unknown key stays 400, ahead of the 404.
        status, body, _ = self.request("POST", "/v1/check", {"key": "cc-missing", "cost": 0})
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        # Unknown fields are 400 ahead of the capacity comparison.
        status, body, _ = self.request("POST", "/v1/check",
                                       {"key": "cc-2", "cost": 5, "extra": 1})
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))

    def test_rejection_changes_neither_state_ledger_nor_metrics(self) -> None:
        self.request("PUT", "/v1/limits/cc-3", {"capacity": 3, "refill_per_second": 1})
        self.request("POST", "/v1/check", {"key": "cc-3", "cost": 3})
        _, state_before, _ = self.request("GET", "/v1/limits/cc-3")
        _, metrics_before, _ = self.request("GET", "/v1/metrics")
        for _ in range(3):
            status, _, headers = self.request("POST", "/v1/check", {"key": "cc-3", "cost": 4})
            self.assertEqual((status, "Retry-After" in headers), (400, False))
        _, state_after, _ = self.request("GET", "/v1/limits/cc-3")
        _, metrics_after, _ = self.request("GET", "/v1/metrics")
        _, ledger, _ = self.request("GET", "/v1/ledgers/cc-3")
        self.assertEqual(state_after, state_before)
        self.assertEqual((state_after["remaining"], state_after["used"]), (0, 3))
        self.assertEqual(metrics_after, metrics_before)
        self.assertEqual(ledger["totals"], {"accepted_count": 1, "accepted_cost": 3})

    def test_concurrent_reconfigure_and_check_show_no_intermediate_result(self) -> None:
        import http.client

        self.request("PUT", "/v1/limits/cc-hot", {"capacity": 5, "refill_per_second": 1})
        _, metrics_before, _ = self.request("GET", "/v1/metrics")
        statuses: list[int] = []
        errors: list[BaseException] = []
        list_lock = threading.Lock()
        barrier = threading.Barrier(8)

        def raw_post(path: str, payload: dict) -> int:
            connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
            connection.request("POST", path, json.dumps(payload).encode(),
                               {"Content-Type": "application/json"})
            response = connection.getresponse()
            response.read()
            status = response.status
            connection.close()
            return status

        def reconfigure() -> None:
            try:
                barrier.wait()
                for index in range(60):
                    capacity = 5 if index % 2 == 0 else 10
                    status, _, _ = self.request(
                        "PUT", "/v1/limits/cc-hot",
                        {"capacity": capacity, "refill_per_second": 1})
                    if status != 200:
                        raise AssertionError(f"reconfigure status {status}")
            except BaseException as error:  # noqa: BLE001
                with list_lock:
                    errors.append(error)

        def spend() -> None:
            try:
                barrier.wait()
                for _ in range(60):
                    status = raw_post("/v1/check", {"key": "cc-hot", "cost": 6})
                    with list_lock:
                        statuses.append(status)
            except BaseException as error:  # noqa: BLE001
                with list_lock:
                    errors.append(error)

        threads = [threading.Thread(target=reconfigure) for _ in range(2)]
        threads += [threading.Thread(target=spend) for _ in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        self.assertTrue(statuses)
        self.assertEqual(set(statuses), {400, 429})     # never 200, never anything torn
        _, metrics_after, _ = self.request("GET", "/v1/metrics")
        check_before = metrics_before["metrics"]["decisions"]["check"]
        check_after = metrics_after["metrics"]["decisions"]["check"]
        self.assertEqual(check_after["allowed"] - check_before["allowed"], 0)
        self.assertEqual(check_after["over_quota"] - check_before["over_quota"],
                         statuses.count(429))
        _, ledger, _ = self.request("GET", "/v1/ledgers/cc-hot")
        self.assertEqual(ledger["totals"], {"accepted_count": 0, "accepted_cost": 0})


if __name__ == "__main__":
    unittest.main()
