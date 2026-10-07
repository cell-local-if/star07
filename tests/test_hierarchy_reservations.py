"""Cross-layer reservation tests: atomic multi-layer holds with lazy expiry, consume and rollback."""
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


class HierarchyReservationUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.limiter = Limiter(self.clock)
        self.limiter.configure("global", {"capacity": 10, "refill_per_second": 1.0})
        self.limiter.configure("tenant-a", {"capacity": 4, "refill_per_second": 0.5})

    def test_reserve_deducts_every_layer_without_used_or_ledger(self) -> None:
        result = self.limiter.hierarchy_reserve(["global", "tenant-a"], 3, 60)
        self.assertEqual(result["keys"], ["global", "tenant-a"])
        self.assertEqual(result["cost"], 3)
        self.assertEqual(result["ttl_seconds"], 60)
        self.assertEqual(result["layers"], [
            {"key": "global", "remaining": 7, "capacity": 10},
            {"key": "tenant-a", "remaining": 1, "capacity": 4},
        ])
        self.assertTrue(result["reservation_id"])
        for key in ("global", "tenant-a"):
            state = self.limiter.state(key)
            self.assertEqual(state["used"], 0)
            self.assertEqual(self.limiter.ledger(key)["totals"]["accepted_count"], 0)
        self.assertEqual(self.limiter.state("global")["remaining"], 7)
        self.assertEqual(self.limiter.state("tenant-a")["remaining"], 1)

    def test_cost_and_ttl_default(self) -> None:
        result = self.limiter.hierarchy_reserve(["global", "tenant-a"], 1)
        self.assertEqual((result["cost"], result["ttl_seconds"]), (1, 60))

    def test_short_layer_rejects_all_with_max_retry_after(self) -> None:
        self.limiter.check("global", 9)     # deficit 2 at 1.0/s -> 2s
        self.limiter.check("tenant-a", 4)   # deficit 3 at 0.5/s -> 6s
        with self.assertRaises(OverQuota) as raised:
            self.limiter.hierarchy_reserve(["global", "tenant-a"], 3, 60)
        self.assertAlmostEqual(raised.exception.retry_after, 6.0, places=6)
        self.assertEqual(self.limiter.state("global")["remaining"], 1)
        self.assertEqual(self.limiter.state("tenant-a")["remaining"], 0)
        for key in ("global", "tenant-a"):
            self.assertEqual(self.limiter.ledger(key)["totals"]["accepted_count"], 1)

    def test_invalid_input_is_400_and_unconfigured_layer_is_404(self) -> None:
        for bad_keys in ([], ["global"], ["global", "global"], "global", ["global", 5]):
            with self.assertRaises(InvalidRequest):
                self.limiter.hierarchy_reserve(bad_keys, 1, 60)
        for bad_cost in (0, 1.5, True, "2", 1_000_001):
            with self.assertRaises(InvalidRequest):
                self.limiter.hierarchy_reserve(["global", "tenant-a"], bad_cost, 60)
        for bad_ttl in (0, 86401, 1.5, True, "60"):
            with self.assertRaises(InvalidRequest):
                self.limiter.hierarchy_reserve(["global", "tenant-a"], 1, bad_ttl)
        with self.assertRaises(LimitNotFound) as raised:
            self.limiter.hierarchy_reserve(["global", "ghost-1", "ghost-2"], 1, 60)
        self.assertIn("ghost-1", str(raised.exception))
        self.assertNotIn("ghost-2", str(raised.exception))
        self.assertEqual(self.limiter.state("global")["remaining"], 10)
        self.assertEqual(self.limiter.state("tenant-a")["remaining"], 4)

    def test_consume_books_used_and_ledger_on_every_layer_without_deducting(self) -> None:
        reservation = self.limiter.hierarchy_reserve(["global", "tenant-a"], 3, 60)
        rid = reservation["reservation_id"]
        result = self.limiter.hierarchy_consume(rid)
        self.assertTrue(result["consumed"])
        self.assertEqual(result["reservation_id"], rid)
        self.assertEqual(result["layers"], [
            {"key": "global", "remaining": 7, "capacity": 10},
            {"key": "tenant-a", "remaining": 1, "capacity": 4},
        ])
        for key in ("global", "tenant-a"):
            self.assertEqual(self.limiter.state(key)["used"], 3)
            events = self.limiter.ledger(key)["events"]
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0]["source"], "hierarchy_reservation_consume")
            self.assertEqual(events[0]["reservation_id"], rid)
            self.assertEqual(events[0]["cost"], 3)
            self.assertEqual(events[0]["effective_at"], self.clock.t)

    def test_consume_replays_first_response_without_rebooking(self) -> None:
        rid = self.limiter.hierarchy_reserve(["global", "tenant-a"], 2, 60)["reservation_id"]
        first = self.limiter.hierarchy_consume(rid)
        self.clock.t += 5.0
        replay = self.limiter.hierarchy_consume(rid)
        self.assertEqual(replay, first)
        for key in ("global", "tenant-a"):
            self.assertEqual(self.limiter.state(key)["used"], 2)
            self.assertEqual(self.limiter.ledger(key)["totals"]["accepted_count"], 1)

    def test_rollback_refunds_every_layer_capped_at_capacity(self) -> None:
        rid = self.limiter.hierarchy_reserve(["global", "tenant-a"], 3, 60)["reservation_id"]
        self.limiter.check("global", 2)  # global now at 5
        result = self.limiter.hierarchy_rollback(rid)
        self.assertTrue(result["rolled_back"])
        self.assertEqual(result["layers"], [
            {"key": "global", "remaining": 8, "capacity": 10},
            {"key": "tenant-a", "remaining": 4, "capacity": 4},  # capped at capacity
        ])
        for key in ("global", "tenant-a"):
            self.assertEqual(self.limiter.state(key)["used"], 2 if key == "global" else 0)
        with self.assertRaises(LimitNotFound):
            self.limiter.hierarchy_rollback(rid)  # repeat rollback is 404
        self.assertEqual(self.limiter.state("global")["remaining"], 8)

    def test_expired_hold_is_settled_by_quota_entries_and_gone_for_consume_and_rollback(self) -> None:
        rid = self.limiter.hierarchy_reserve(["global", "tenant-a"], 3, 30)["reservation_id"]
        self.clock.t += 30.0  # inclusive boundary: the hold has lapsed
        # Any quota entry on one layer settles the whole cross-layer hold exactly once.
        self.assertEqual(self.limiter.state("tenant-a")["remaining"], 4)
        self.assertEqual(self.limiter.state("global")["remaining"], 10)
        with self.assertRaises(LimitNotFound):
            self.limiter.hierarchy_consume(rid)
        with self.assertRaises(LimitNotFound):
            self.limiter.hierarchy_rollback(rid)
        for key in ("global", "tenant-a"):
            self.assertEqual(self.limiter.state(key)["used"], 0)
            self.assertEqual(self.limiter.ledger(key)["totals"]["accepted_count"], 0)

    def test_consumed_hold_is_404_for_rollback_and_never_refunded(self) -> None:
        rid = self.limiter.hierarchy_reserve(["global", "tenant-a"], 3, 60)["reservation_id"]
        self.limiter.hierarchy_consume(rid)
        with self.assertRaises(LimitNotFound):
            self.limiter.hierarchy_rollback(rid)
        self.clock.t += 3600.0
        self.assertEqual(self.limiter.state("global")["used"], 3)
        self.assertEqual(self.limiter.state("tenant-a")["used"], 3)

    def test_cross_resource_identifiers_are_404(self) -> None:
        single = self.limiter.reserve("global", 1, 60)["reservation_id"]
        cross = self.limiter.hierarchy_reserve(["global", "tenant-a"], 1, 60)["reservation_id"]
        with self.assertRaises(LimitNotFound):
            self.limiter.hierarchy_consume(single)
        with self.assertRaises(LimitNotFound):
            self.limiter.hierarchy_rollback(single)
        with self.assertRaises(LimitNotFound):
            self.limiter.consume(cross)
        with self.assertRaises(LimitNotFound):
            self.limiter.rollback(cross)

    def test_reconfigure_keeps_ttl_and_creation_cost_but_new_capacity(self) -> None:
        rid = self.limiter.hierarchy_reserve(["global", "tenant-a"], 3, 30)["reservation_id"]
        self.limiter.configure("tenant-a", {"capacity": 2, "refill_per_second": 1.0})
        result = self.limiter.hierarchy_consume(rid)
        self.assertEqual(result["layers"][1], {"key": "tenant-a", "remaining": 1, "capacity": 2})
        self.assertEqual(self.limiter.state("tenant-a")["used"], 3)  # creation-time cost booked
        # Reconfigure did not extend the TTL: a second hold still lapses on its own schedule.
        rid2 = self.limiter.hierarchy_reserve(["global", "tenant-a"], 1, 10)["reservation_id"]
        self.limiter.configure("global", {"capacity": 20, "refill_per_second": 1.0})
        self.clock.t += 10.0
        with self.assertRaises(LimitNotFound):
            self.limiter.hierarchy_consume(rid2)

    def test_stalled_and_regressed_clock_never_refunds_early(self) -> None:
        rid = self.limiter.hierarchy_reserve(["global", "tenant-a"], 3, 30)["reservation_id"]
        self.clock.t -= 500.0  # regression: watermark holds, nothing expires early
        result = self.limiter.hierarchy_consume(rid)
        self.assertTrue(result["consumed"])
        self.assertEqual(self.limiter.state("global")["remaining"], 7)

    def test_concurrent_reserve_and_consume_never_oversell_or_double_book(self) -> None:
        limiter = Limiter(Clock())
        limiter.configure("global", {"capacity": 40, "refill_per_second": 1.0})
        limiter.configure("tenant-a", {"capacity": 24, "refill_per_second": 1.0})
        reserved: list[str] = []
        lock = threading.Lock()

        def attempt() -> None:
            try:
                result = limiter.hierarchy_reserve(["global", "tenant-a"], 2, 60)
            except OverQuota:
                return
            with lock:
                reserved.append(result["reservation_id"])

        threads = [threading.Thread(target=attempt) for _ in range(30)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(len(reserved), 12)  # tenant-a caps at floor(24/2)
        for rid in reserved:
            limiter.hierarchy_consume(rid)
        for key, capacity in (("global", 40), ("tenant-a", 24)):
            self.assertEqual(limiter.state(key)["used"], 24)
            self.assertEqual(limiter.state(key)["remaining"], capacity - 24)
            totals = limiter.ledger(key)["totals"]
            self.assertEqual((totals["accepted_count"], totals["accepted_cost"]), (12, 24))


class HierarchyReservationHttpTests(unittest.TestCase):
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

    def test_full_lifecycle_over_http(self) -> None:
        self.request("PUT", "/v1/limits/hr-org", {"capacity": 5, "refill_per_second": 1})
        self.request("PUT", "/v1/limits/hr-team", {"capacity": 3, "refill_per_second": 1})
        status, body, _ = self.request("POST", "/v1/hierarchies/reservations",
                                       {"keys": ["hr-org", "hr-team"], "cost": 2, "ttl_seconds": 60})
        self.assertEqual(status, 200)
        self.assertEqual(body["layers"], [{"key": "hr-org", "remaining": 3, "capacity": 5},
                                          {"key": "hr-team", "remaining": 1, "capacity": 3}])
        rid = body["reservation_id"]
        _, state, _ = self.request("GET", "/v1/limits/hr-org")
        self.assertEqual((state["remaining"], state["used"]), (3, 0))
        status, body, _ = self.request("POST", f"/v1/hierarchies/reservations/{rid}/consume", {})
        self.assertEqual((status, body["consumed"]), (200, True))
        _, ledger, _ = self.request("GET", "/v1/ledgers/hr-team")
        self.assertEqual(ledger["events"][0]["source"], "hierarchy_reservation_consume")
        self.assertEqual(ledger["events"][0]["reservation_id"], rid)
        status, body, _ = self.request("POST", f"/v1/hierarchies/reservations/{rid}/consume", {})
        self.assertEqual((status, body["consumed"]), (200, True))  # idempotent replay
        _, ledger, _ = self.request("GET", "/v1/ledgers/hr-team")
        self.assertEqual(ledger["totals"]["accepted_count"], 1)
        self.assertEqual(self.request("DELETE", f"/v1/hierarchies/reservations/{rid}")[0], 404)

    def test_rollback_over_http(self) -> None:
        self.request("PUT", "/v1/limits/hr-top", {"capacity": 4, "refill_per_second": 1})
        self.request("PUT", "/v1/limits/hr-leaf", {"capacity": 4, "refill_per_second": 1})
        _, body, _ = self.request("POST", "/v1/hierarchies/reservations",
                                  {"keys": ["hr-top", "hr-leaf"], "cost": 2})
        rid = body["reservation_id"]
        status, body, _ = self.request("DELETE", f"/v1/hierarchies/reservations/{rid}")
        self.assertEqual((status, body["rolled_back"]), (200, True))
        self.assertEqual([layer["remaining"] for layer in body["layers"]], [4, 4])
        self.assertEqual(self.request("DELETE", f"/v1/hierarchies/reservations/{rid}")[0], 404)

    def test_over_quota_is_429_with_ceiled_retry_after(self) -> None:
        self.request("PUT", "/v1/limits/hr-wide", {"capacity": 10, "refill_per_second": 1})
        self.request("PUT", "/v1/limits/hr-thin", {"capacity": 1, "refill_per_second": 0.5})
        self.request("POST", "/v1/check", {"key": "hr-thin", "cost": 1})
        status, body, headers = self.request("POST", "/v1/hierarchies/reservations",
                                             {"keys": ["hr-wide", "hr-thin"], "cost": 1})
        self.assertEqual((status, body["error"]["code"]), (429, "over_quota"))
        self.assertEqual(headers["Retry-After"], "2.000")
        _, state, _ = self.request("GET", "/v1/limits/hr-wide")
        self.assertEqual(state["remaining"], 10)

    def test_body_shape_errors_are_400(self) -> None:
        self.request("PUT", "/v1/limits/hr-a", {"capacity": 5, "refill_per_second": 1})
        self.request("PUT", "/v1/limits/hr-b", {"capacity": 5, "refill_per_second": 1})
        for bad in ({"keys": ["hr-a", "hr-b"], "extra": 1}, {"cost": 1},
                    {"keys": ["hr-a", "hr-a"]}, {"keys": ["hr-a"]},
                    {"keys": ["hr-a", "hr-b"], "ttl_seconds": 0},
                    {"keys": ["hr-a", "hr-b"], "ttl_seconds": True}, []):
            status, body, _ = self.request("POST", "/v1/hierarchies/reservations", bad)
            self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"), bad)
        _, created, _ = self.request("POST", "/v1/hierarchies/reservations",
                                     {"keys": ["hr-a", "hr-b"], "cost": 1})
        rid = created["reservation_id"]
        status, body, _ = self.request("POST", f"/v1/hierarchies/reservations/{rid}/consume",
                                       {"extra": 1})
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        self.request("POST", f"/v1/hierarchies/reservations/{rid}/consume", {})

    def test_route_and_identifier_mismatches_are_404(self) -> None:
        self.request("PUT", "/v1/limits/hr-x", {"capacity": 5, "refill_per_second": 1})
        self.request("PUT", "/v1/limits/hr-y", {"capacity": 5, "refill_per_second": 1})
        _, created, _ = self.request("POST", "/v1/hierarchies/reservations",
                                     {"keys": ["hr-x", "hr-y"], "cost": 1})
        rid = created["reservation_id"]
        self.assertEqual(self.request("GET", "/v1/hierarchies/reservations")[0], 404)
        self.assertEqual(self.request("PUT", "/v1/hierarchies/reservations", {"keys": ["a", "b"]})[0], 404)
        self.assertEqual(self.request("POST", f"/v1/hierarchies/reservations/{rid}")[0], 404)
        self.assertEqual(self.request("DELETE", f"/v1/hierarchies/reservations/{rid}/extra")[0], 404)
        self.assertEqual(self.request("POST", f"/v1/hierarchies/reservations/{rid}/rollback", {})[0], 404)
        # Cross-resource: the hierarchy id is unknown to the single-key endpoints and back.
        self.assertEqual(self.request("POST", f"/v1/reservations/{rid}/consume", {})[0], 404)
        self.assertEqual(self.request("DELETE", f"/v1/reservations/{rid}")[0], 404)
        _, single, _ = self.request("POST", "/v1/reservations", {"key": "hr-x", "cost": 1})
        self.assertEqual(
            self.request("POST", f"/v1/hierarchies/reservations/{single['reservation_id']}/consume", {})[0], 404)
        self.assertEqual(self.request("DELETE", f"/v1/hierarchies/reservations/{single['reservation_id']}")[0], 404)


if __name__ == "__main__":
    unittest.main()
