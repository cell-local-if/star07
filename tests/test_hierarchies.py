"""Hierarchy check tests: atomic multi-layer deduction over the existing per-key buckets."""
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


class HierarchyUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.limiter = Limiter(self.clock)
        self.limiter.configure("global", {"capacity": 10, "refill_per_second": 1.0})
        self.limiter.configure("tenant-a", {"capacity": 4, "refill_per_second": 0.5})

    def test_success_deducts_every_layer_and_reports_in_order(self) -> None:
        result = self.limiter.hierarchy_check(["global", "tenant-a"], 3)
        self.assertEqual(result, {"allowed": True, "cost": 3, "layers": [
            {"key": "global", "remaining": 7, "capacity": 10},
            {"key": "tenant-a", "remaining": 1, "capacity": 4},
        ]})
        self.assertEqual(self.limiter.state("global")["used"], 3)
        self.assertEqual(self.limiter.state("tenant-a")["used"], 3)

    def test_cost_defaults_to_one(self) -> None:
        result = self.limiter.hierarchy_check(["global", "tenant-a"], 1)
        self.assertEqual(result["cost"], 1)
        self.assertEqual([layer["remaining"] for layer in result["layers"]], [9, 3])

    def test_ledger_events_carry_hierarchy_source_and_null_reservation(self) -> None:
        self.limiter.hierarchy_check(["global", "tenant-a"], 2)
        for key in ("global", "tenant-a"):
            events = self.limiter.ledger(key)["events"]
            self.assertEqual(len(events), 1)
            event = events[0]
            self.assertEqual(event["source"], "hierarchy_check")
            self.assertIsNone(event["reservation_id"])
            self.assertEqual(event["cost"], 2)
            self.assertEqual(event["effective_at"], self.clock.t)
        self.assertEqual(self.limiter.ledger("global")["events"][0]["remaining"], 8)
        self.assertEqual(self.limiter.ledger("tenant-a")["events"][0]["remaining"], 2)

    def test_any_short_layer_rejects_all_without_deducting_or_booking(self) -> None:
        self.limiter.check("tenant-a", 3)  # tenant-a now holds 1 token
        with self.assertRaises(OverQuota):
            self.limiter.hierarchy_check(["global", "tenant-a"], 2)
        self.assertEqual(self.limiter.state("global")["remaining"], 10)
        self.assertEqual(self.limiter.state("tenant-a")["remaining"], 1)
        self.assertEqual(self.limiter.state("global")["used"], 0)
        self.assertEqual(self.limiter.ledger("global")["totals"]["accepted_count"], 0)
        self.assertEqual(self.limiter.ledger("tenant-a")["totals"]["accepted_count"], 1)

    def test_retry_after_is_the_slowest_short_layer(self) -> None:
        self.limiter.check("global", 9)     # deficit 2 at 1.0/s -> 2s
        self.limiter.check("tenant-a", 4)   # deficit 3 at 0.5/s -> 6s
        with self.assertRaises(OverQuota) as raised:
            self.limiter.hierarchy_check(["global", "tenant-a"], 3)
        self.assertAlmostEqual(raised.exception.retry_after, 6.0, places=6)

    def test_unconfigured_layer_is_404_naming_the_first_in_input_order(self) -> None:
        with self.assertRaises(LimitNotFound) as raised:
            self.limiter.hierarchy_check(["global", "missing-1", "missing-2"], 1)
        self.assertIn("missing-1", str(raised.exception))
        self.assertNotIn("missing-2", str(raised.exception))

    def test_invalid_keys_and_cost_are_400_and_change_nothing(self) -> None:
        for bad_keys in ([], ["global"], ["global"] * 2, ["global", 5], ["global", ""],
                         ["global", None], [f"k{i}" for i in range(21)], "global"):
            with self.assertRaises(InvalidRequest):
                self.limiter.hierarchy_check(bad_keys, 1)
        for bad_cost in (0, -1, 1_000_001, 1.5, True, "2"):
            with self.assertRaises(InvalidRequest):
                self.limiter.hierarchy_check(["global", "tenant-a"], bad_cost)
        self.assertEqual(self.limiter.state("global")["remaining"], 10)
        self.assertEqual(self.limiter.state("tenant-a")["remaining"], 4)

    def test_expired_reservations_settle_before_layers_are_judged(self) -> None:
        reservation = self.limiter.reserve("tenant-a", 4, ttl_seconds=30)
        self.assertIsNotNone(reservation)
        self.clock.t += 30.0  # hold lapses at the inclusive boundary
        result = self.limiter.hierarchy_check(["global", "tenant-a"], 4)
        self.assertTrue(result["allowed"])
        self.assertEqual(result["layers"][1]["remaining"], 0)

    def test_stalled_and_regressed_clock_follow_the_watermark(self) -> None:
        self.limiter.hierarchy_check(["global", "tenant-a"], 2)  # global 8, tenant-a 2
        self.clock.t -= 500.0  # regression: no refill is conjured
        # Cost 3 fits both capacities (10 and 4) but exceeds tenant-a's 2 live tokens: a genuine
        # temporary shortfall (never the cost>capacity 400), still 429 under the pinned clock.
        with self.assertRaises(OverQuota):
            self.limiter.hierarchy_check(["global", "tenant-a"], 3)
        self.clock.t += 500.0  # recovery: the regressed interval is not counted twice
        result = self.limiter.hierarchy_check(["global", "tenant-a"], 2)
        self.assertTrue(result["allowed"])
        self.assertEqual([layer["remaining"] for layer in result["layers"]], [6, 0])

    def test_concurrent_hierarchy_checks_never_oversell_any_layer(self) -> None:
        limiter = Limiter(Clock())
        limiter.configure("global", {"capacity": 40, "refill_per_second": 1.0})
        limiter.configure("tenant-a", {"capacity": 25, "refill_per_second": 1.0})
        outcomes: list[str] = []
        lock = threading.Lock()

        def attempt() -> None:
            try:
                limiter.hierarchy_check(["global", "tenant-a"], 2)
                outcome = "allowed"
            except OverQuota:
                outcome = "rejected"
            with lock:
                outcomes.append(outcome)

        threads = [threading.Thread(target=attempt) for _ in range(30)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        allowed = outcomes.count("allowed")
        self.assertEqual(allowed, 12)  # tenant-a caps at floor(25/2)
        self.assertEqual(limiter.state("global")["remaining"], 40 - 2 * allowed)
        self.assertEqual(limiter.state("tenant-a")["remaining"], 25 - 2 * allowed)
        for key in ("global", "tenant-a"):
            totals = limiter.ledger(key)["totals"]
            self.assertEqual((totals["accepted_count"], totals["accepted_cost"]), (allowed, 2 * allowed))


class HierarchyHttpTests(unittest.TestCase):
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

    def test_happy_path_and_ledger_visibility(self) -> None:
        self.request("PUT", "/v1/limits/h-org", {"capacity": 5, "refill_per_second": 1})
        self.request("PUT", "/v1/limits/h-team", {"capacity": 3, "refill_per_second": 1})
        status, body, _ = self.request("POST", "/v1/hierarchies/check",
                                       {"keys": ["h-org", "h-team"], "cost": 2})
        self.assertEqual(status, 200)
        self.assertEqual(body["layers"], [{"key": "h-org", "remaining": 3, "capacity": 5},
                                          {"key": "h-team", "remaining": 1, "capacity": 3}])
        _, ledger, _ = self.request("GET", "/v1/ledgers/h-org")
        self.assertEqual(ledger["events"][0]["source"], "hierarchy_check")
        self.assertEqual(ledger["totals"]["accepted_cost"], 2)
        status, body, _ = self.request("POST", "/v1/hierarchies/check", {"keys": ["h-org", "h-team"]})
        self.assertEqual((status, body["cost"]), (200, 1))  # cost defaults to 1
        self.assertEqual([layer["remaining"] for layer in body["layers"]], [2, 0])

    def test_over_quota_is_429_with_ceiled_retry_after_and_no_partial_spend(self) -> None:
        self.request("PUT", "/v1/limits/h-top", {"capacity": 10, "refill_per_second": 1})
        self.request("PUT", "/v1/limits/h-leaf", {"capacity": 1, "refill_per_second": 0.5})
        self.request("POST", "/v1/check", {"key": "h-leaf", "cost": 1})
        status, body, headers = self.request("POST", "/v1/hierarchies/check",
                                             {"keys": ["h-top", "h-leaf"], "cost": 1})
        self.assertEqual((status, body["error"]["code"]), (429, "over_quota"))
        self.assertEqual(headers["Retry-After"], "2.000")
        _, state, _ = self.request("GET", "/v1/limits/h-top")
        self.assertEqual(state["remaining"], 10)

    def test_body_shape_errors_are_400(self) -> None:
        for bad in ({"keys": ["h-org", "h-team"], "extra": 1}, {"cost": 1},
                    {"keys": ["h-org", "h-org"]}, {"keys": ["h-org"]},
                    {"keys": ["h-org", "h-team"], "cost": True}, {"keys": "h-org"}, []):
            status, body, _ = self.request("POST", "/v1/hierarchies/check", bad)
            self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"), bad)

    def test_unconfigured_layer_is_404_naming_first_missing_key(self) -> None:
        self.request("PUT", "/v1/limits/h-real", {"capacity": 5, "refill_per_second": 1})
        status, body, _ = self.request("POST", "/v1/hierarchies/check",
                                       {"keys": ["h-real", "h-ghost-1", "h-ghost-2"]})
        self.assertEqual((status, body["error"]["code"]), (404, "not_found"))
        self.assertIn("h-ghost-1", body["error"]["message"])
        self.assertNotIn("h-ghost-2", body["error"]["message"])

    def test_invalid_body_beats_unconfigured_layer(self) -> None:
        status, body, _ = self.request("POST", "/v1/hierarchies/check",
                                       {"keys": ["h-ghost", "h-ghost"], "cost": 1})
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))

    def test_route_mismatches_are_404(self) -> None:
        self.assertEqual(self.request("POST", "/v1/hierarchies", {"keys": ["a", "b"]})[0], 404)
        self.assertEqual(self.request("GET", "/v1/hierarchies/check")[0], 404)
        self.assertEqual(self.request("PUT", "/v1/hierarchies/check", {"keys": ["a", "b"]})[0], 404)


if __name__ == "__main__":
    unittest.main()
