"""Hierarchy check tests: atomic multi-layer instant deduction over the existing buckets."""
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
        self.limiter.configure("tenant-a", {"capacity": 4, "refill_per_second": 2.0})

    def test_success_deducts_every_layer_in_input_order(self) -> None:
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
        self.assertEqual(result["layers"][0]["remaining"], 9)

    def test_success_posts_one_ledger_event_per_layer(self) -> None:
        self.limiter.hierarchy_check(["global", "tenant-a"], 2)
        for key, remaining in (("global", 8), ("tenant-a", 2)):
            ledger = self.limiter.ledger(key)
            self.assertEqual(ledger["totals"], {"accepted_count": 1, "accepted_cost": 2})
            (event,) = ledger["events"]
            self.assertEqual(event["seq"], 1)
            self.assertEqual(event["source"], "hierarchy_check")
            self.assertIsNone(event["reservation_id"])
            self.assertEqual(event["cost"], 2)
            self.assertEqual(event["remaining"], remaining)
            self.assertEqual(event["effective_at"], self.clock.t)

    def test_insufficient_layer_rejects_all_and_posts_nothing(self) -> None:
        with self.assertRaises(OverQuota) as raised:
            self.limiter.hierarchy_check(["global", "tenant-a"], 5)
        # tenant-a deficit: 5 - 4 = 1 at 2/s -> 0.5s; global is sufficient.
        self.assertAlmostEqual(raised.exception.retry_after, 0.5, places=6)
        self.assertEqual(self.limiter.state("global")["remaining"], 10)
        self.assertEqual(self.limiter.state("tenant-a")["remaining"], 4)
        self.assertEqual(self.limiter.ledger("global")["totals"]["accepted_count"], 0)
        self.assertEqual(self.limiter.ledger("tenant-a")["totals"]["accepted_count"], 0)

    def test_retry_after_is_the_max_over_all_short_layers(self) -> None:
        self.limiter.check("global", 9)       # global: 1 left
        self.limiter.check("tenant-a", 3)     # tenant-a: 1 left
        with self.assertRaises(OverQuota) as raised:
            self.limiter.hierarchy_check(["global", "tenant-a"], 5)
        # global: 4 / 1.0 = 4s; tenant-a: 4 / 2.0 = 2s -> max wins.
        self.assertAlmostEqual(raised.exception.retry_after, 4.0, places=6)

    def test_expired_reservations_settle_before_judgement(self) -> None:
        self.limiter.reserve("tenant-a", 3, ttl_seconds=10)
        self.clock.t += 10.0
        result = self.limiter.hierarchy_check(["global", "tenant-a"], 4)
        self.assertTrue(result["allowed"])
        self.assertEqual(result["layers"][1]["remaining"], 0)

    def test_invalid_keys_and_cost_are_400(self) -> None:
        for bad_keys in ("global", ["only-one"], ["a"] * 21, ["global", "global"],
                         ["global", ""], ["global", 5], [], None):
            with self.assertRaises(InvalidRequest, msg=f"keys={bad_keys!r}"):
                self.limiter.hierarchy_check(bad_keys, 1)
        for bad_cost in (0, 1_000_001, True, 1.5, "2"):
            with self.assertRaises(InvalidRequest, msg=f"cost={bad_cost!r}"):
                self.limiter.hierarchy_check(["global", "tenant-a"], bad_cost)
        # Rejected requests change nothing.
        self.assertEqual(self.limiter.state("global")["remaining"], 10)
        self.assertEqual(self.limiter.state("tenant-a")["remaining"], 4)

    def test_first_unconfigured_layer_in_input_order_is_named(self) -> None:
        with self.assertRaises(LimitNotFound) as raised:
            self.limiter.hierarchy_check(["missing-1", "global", "missing-2"], 1)
        self.assertIn("missing-1", str(raised.exception))
        self.assertNotIn("missing-2", str(raised.exception))

    def test_clock_regression_neither_conjures_nor_double_counts(self) -> None:
        self.limiter.hierarchy_check(["global", "tenant-a"], 4)
        self.clock.t -= 500.0  # regressed reading clamps to the watermark: zero refill
        with self.assertRaises(OverQuota):
            self.limiter.hierarchy_check(["global", "tenant-a"], 7)
        self.assertEqual(self.limiter.state("global")["remaining"], 6)

    def test_concurrent_hierarchy_checks_never_oversell(self) -> None:
        outcomes: list[str] = []

        def attempt() -> None:
            try:
                self.limiter.hierarchy_check(["global", "tenant-a"], 1)
                outcomes.append("ok")
            except OverQuota:
                outcomes.append("rejected")

        threads = [threading.Thread(target=attempt) for _ in range(12)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        # tenant-a (capacity 4) is the binding layer: exactly 4 succeed, none partially.
        self.assertEqual(outcomes.count("ok"), 4)
        self.assertEqual(self.limiter.state("global")["remaining"], 6)
        self.assertEqual(self.limiter.state("tenant-a")["remaining"], 0)
        self.assertEqual(self.limiter.ledger("global")["totals"]["accepted_count"], 4)
        self.assertEqual(self.limiter.ledger("tenant-a")["totals"]["accepted_count"], 4)


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

    def test_happy_path_and_ledger_source(self) -> None:
        self.request("PUT", "/v1/limits/org", {"capacity": 5, "refill_per_second": 1})
        self.request("PUT", "/v1/limits/team", {"capacity": 3, "refill_per_second": 1})
        status, body, _ = self.request("POST", "/v1/hierarchies/check",
                                       {"keys": ["org", "team"], "cost": 2})
        self.assertEqual(status, 200)
        self.assertEqual(body["layers"][0], {"key": "org", "remaining": 3, "capacity": 5})
        _, ledger, _ = self.request("GET", "/v1/ledgers/team")
        self.assertEqual(ledger["events"][0]["source"], "hierarchy_check")

    def test_429_carries_ceiled_millisecond_retry_after(self) -> None:
        self.request("PUT", "/v1/limits/slow", {"capacity": 1, "refill_per_second": 3})
        self.request("PUT", "/v1/limits/fast", {"capacity": 10, "refill_per_second": 1})
        status, body, headers = self.request("POST", "/v1/hierarchies/check",
                                             {"keys": ["slow", "fast"], "cost": 2})
        self.assertEqual((status, body["error"]["code"]), (429, "over_quota"))
        # slow deficit 1 at 3/s -> 0.333...s, ceiled to 0.334.
        self.assertEqual(headers["Retry-After"], "0.334")

    def test_validation_and_not_found_classification(self) -> None:
        self.request("PUT", "/v1/limits/known", {"capacity": 5, "refill_per_second": 1})
        for bad_body in ({"keys": ["known", "known"]},
                         {"keys": ["known"]},
                         {"keys": ["known", "other"], "cost": True},
                         {"keys": ["known", "other"], "extra": 1},
                         {"cost": 1}):
            status, body, _ = self.request("POST", "/v1/hierarchies/check", bad_body)
            self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"), msg=bad_body)
        status, body, _ = self.request("POST", "/v1/hierarchies/check",
                                       {"keys": ["known", "ghost"]})
        self.assertEqual((status, body["error"]["code"]), (404, "not_found"))
        self.assertIn("ghost", body["error"]["message"])

    def test_existing_endpoints_unaffected(self) -> None:
        self.assertEqual(self.request("POST", "/v1/hierarchies", {})[0], 404)
        self.request("PUT", "/v1/limits/plain", {"capacity": 2, "refill_per_second": 1})
        status, body, _ = self.request("POST", "/v1/check", {"key": "plain"})
        self.assertEqual((status, body["remaining"]), (200, 1))


if __name__ == "__main__":
    unittest.main()
