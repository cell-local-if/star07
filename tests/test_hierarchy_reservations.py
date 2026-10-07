"""Cross-layer hierarchy reservation tests: atomic multi-layer holds, consume and rollback."""
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
        self.assertTrue(result["reservation_id"])
        self.assertEqual(result["layers"], [
            {"key": "global", "remaining": 7, "capacity": 10},
            {"key": "tenant-a", "remaining": 1, "capacity": 4},
        ])
        for key in ("global", "tenant-a"):
            state = self.limiter.state(key)
            self.assertEqual(state["used"], 0)
            self.assertEqual(self.limiter.ledger(key)["totals"]["accepted_count"], 0)
        self.assertEqual(self.limiter.state("global")["remaining"], 7)
        self.assertEqual(self.limiter.state("tenant-a")["remaining"], 1)

    def test_cost_and_ttl_default(self) -> None:
        result = self.limiter.hierarchy_reserve(["global", "tenant-a"], 1)
        self.assertEqual((result["cost"], result["ttl_seconds"]), (1, 60))

    def test_ids_share_no_namespace_with_single_key_reservations(self) -> None:
        single = self.limiter.reserve("global", 1)["reservation_id"]
        cross = self.limiter.hierarchy_reserve(["global", "tenant-a"], 1)["reservation_id"]
        self.assertNotEqual(single, cross)
        with self.assertRaises(LimitNotFound):
            self.limiter.hierarchy_consume(single)
        with self.assertRaises(LimitNotFound):
            self.limiter.hierarchy_rollback(single)
        with self.assertRaises(LimitNotFound):
            self.limiter.consume(cross)
        with self.assertRaises(LimitNotFound):
            self.limiter.rollback(cross)

    def test_short_layer_rejects_all_without_deducting(self) -> None:
        self.limiter.check("tenant-a", 3)  # tenant-a now holds 1 token
        with self.assertRaises(OverQuota) as raised:
            self.limiter.hierarchy_reserve(["global", "tenant-a"], 2)
        self.assertAlmostEqual(raised.exception.retry_after, 2.0, places=6)
        self.assertEqual(self.limiter.state("global")["remaining"], 10)
        self.assertEqual(self.limiter.state("tenant-a")["remaining"], 1)
        self.assertEqual(self.limiter.ledger("global")["totals"]["accepted_count"], 0)

    def test_retry_after_is_the_slowest_short_layer(self) -> None:
        self.limiter.check("global", 9)     # deficit 2 at 1.0/s -> 2s
        self.limiter.check("tenant-a", 4)   # deficit 3 at 0.5/s -> 6s
        with self.assertRaises(OverQuota) as raised:
            self.limiter.hierarchy_reserve(["global", "tenant-a"], 3)
        self.assertAlmostEqual(raised.exception.retry_after, 6.0, places=6)

    def test_unconfigured_layer_is_404_naming_the_first_in_input_order(self) -> None:
        with self.assertRaises(LimitNotFound) as raised:
            self.limiter.hierarchy_reserve(["global", "missing-1", "missing-2"], 1)
        self.assertIn("missing-1", str(raised.exception))
        self.assertNotIn("missing-2", str(raised.exception))

    def test_invalid_input_is_400_and_changes_nothing(self) -> None:
        for bad_keys in ([], ["global"], ["global", "global"], ["global", 5], "global"):
            with self.assertRaises(InvalidRequest):
                self.limiter.hierarchy_reserve(bad_keys, 1)
        for bad_cost in (0, -1, 1_000_001, 1.5, True, "2"):
            with self.assertRaises(InvalidRequest):
                self.limiter.hierarchy_reserve(["global", "tenant-a"], bad_cost)
        for bad_ttl in (0, 86_401, 1.5, True, "60"):
            with self.assertRaises(InvalidRequest):
                self.limiter.hierarchy_reserve(["global", "tenant-a"], 1, bad_ttl)
        self.assertEqual(self.limiter.state("global")["remaining"], 10)
        self.assertEqual(self.limiter.state("tenant-a")["remaining"], 4)

    def test_consume_books_every_layer_once_without_deducting(self) -> None:
        reservation = self.limiter.hierarchy_reserve(["global", "tenant-a"], 3, 60)
        result = self.limiter.hierarchy_consume(reservation["reservation_id"])
        self.assertEqual(result["consumed"], True)
        self.assertEqual(result["reservation_id"], reservation["reservation_id"])
        self.assertEqual(result["layers"], [
            {"key": "global", "remaining": 7, "capacity": 10, "used": 3},
            {"key": "tenant-a", "remaining": 1, "capacity": 4, "used": 3},
        ])
        for key in ("global", "tenant-a"):
            events = self.limiter.ledger(key)["events"]
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0]["source"], "hierarchy_reservation_consume")
            self.assertEqual(events[0]["reservation_id"], reservation["reservation_id"])
            self.assertEqual(events[0]["cost"], 3)
            self.assertEqual(events[0]["effective_at"], self.clock.t)

    def test_consume_replay_is_idempotent(self) -> None:
        reservation = self.limiter.hierarchy_reserve(["global", "tenant-a"], 2, 60)
        first = self.limiter.hierarchy_consume(reservation["reservation_id"])
        self.clock.t += 5.0
        second = self.limiter.hierarchy_consume(reservation["reservation_id"])
        self.assertEqual(first, second)
        self.assertEqual(self.limiter.state("global")["used"], 2)
        self.assertEqual(self.limiter.ledger("global")["totals"]["accepted_count"], 1)
        self.assertEqual(self.limiter.ledger("tenant-a")["totals"]["accepted_count"], 1)

    def test_rollback_refunds_every_layer_capped_at_capacity(self) -> None:
        reservation = self.limiter.hierarchy_reserve(["global", "tenant-a"], 3, 60)
        result = self.limiter.hierarchy_rollback(reservation["reservation_id"])
        self.assertEqual(result["rolled_back"], True)
        self.assertEqual(result["layers"], [
            {"key": "global", "remaining": 10, "capacity": 10},
            {"key": "tenant-a", "remaining": 4, "capacity": 4},
        ])
        self.assertEqual(self.limiter.state("global")["used"], 0)
        with self.assertRaises(LimitNotFound):
            self.limiter.hierarchy_rollback(reservation["reservation_id"])

    def test_expiry_settles_at_inclusive_boundary_and_refunds_each_layer_once(self) -> None:
        reservation = self.limiter.hierarchy_reserve(["global", "tenant-a"], 3, 10)
        self.clock.t += 10.0  # hold lapses at the inclusive boundary
        self.assertEqual(self.limiter.state("global")["remaining"], 10)
        self.assertEqual(self.limiter.state("tenant-a")["remaining"], 4)
        # Settled holds are unknown to consume and rollback, and never refund again.
        with self.assertRaises(LimitNotFound):
            self.limiter.hierarchy_consume(reservation["reservation_id"])
        with self.assertRaises(LimitNotFound):
            self.limiter.hierarchy_rollback(reservation["reservation_id"])
        self.assertEqual(self.limiter.state("global")["remaining"], 10)
        self.assertEqual(self.limiter.state("tenant-a")["remaining"], 4)

    def test_expiry_settles_per_layer_as_keys_are_touched(self) -> None:
        reservation = self.limiter.hierarchy_reserve(["global", "tenant-a"], 3, 10)
        self.clock.t += 10.0
        self.limiter.check("global", 1)  # touches only "global"
        self.assertEqual(self.limiter.state("global")["remaining"], 9)
        self.assertEqual(self.limiter.state("tenant-a")["remaining"], 4)
        # The consume attempt afterwards still finds the hold fully expired: 404, no booking.
        with self.assertRaises(LimitNotFound):
            self.limiter.hierarchy_consume(reservation["reservation_id"])
        self.assertEqual(self.limiter.state("global")["used"], 1)
        self.assertEqual(self.limiter.state("tenant-a")["used"], 0)

    def test_reconfigure_does_not_extend_ttl_and_consume_uses_creation_cost(self) -> None:
        reservation = self.limiter.hierarchy_reserve(["global", "tenant-a"], 3, 10)
        self.limiter.configure("tenant-a", {"capacity": 20, "refill_per_second": 2.0})
        result = self.limiter.hierarchy_consume(reservation["reservation_id"])
        tenant_layer = result["layers"][1]
        self.assertEqual(tenant_layer["used"], 3)          # cost booked as reserved
        self.assertEqual(tenant_layer["capacity"], 20)     # remaining/capacity from new config
        self.clock.t += 10.0
        # The reconfigured hold still lapsed at its original expiry; being consumed, no refund
        # ever lands and nothing is booked twice.
        self.assertEqual(self.limiter.state("global")["used"], 3)
        self.assertEqual(self.limiter.state("tenant-a")["used"], 3)
        self.assertEqual(self.limiter.ledger("global")["totals"]["accepted_count"], 1)
        self.assertEqual(self.limiter.ledger("tenant-a")["totals"]["accepted_count"], 1)

    def test_stalled_and_regressed_clock_never_refunds_early(self) -> None:
        reservation = self.limiter.hierarchy_reserve(["global", "tenant-a"], 3, 10)
        self.clock.t -= 500.0  # regression: time stands at the watermark, nothing expires
        result = self.limiter.hierarchy_consume(reservation["reservation_id"])
        self.assertTrue(result["consumed"])
        self.assertEqual(self.limiter.state("global")["remaining"], 7)
        with self.assertRaises(LimitNotFound):
            self.limiter.hierarchy_rollback(reservation["reservation_id"])

    def test_consumed_hold_is_never_refunded_by_later_expiry(self) -> None:
        reservation = self.limiter.hierarchy_reserve(["global", "tenant-a"], 3, 5)
        self.limiter.hierarchy_consume(reservation["reservation_id"])
        self.clock.t += 100.0
        self.assertEqual(self.limiter.state("global")["used"], 3)
        self.assertEqual(self.limiter.state("tenant-a")["used"], 3)
        self.assertEqual(self.limiter.ledger("global")["totals"]["accepted_cost"], 3)

    def test_concurrent_reservations_never_oversell_any_layer(self) -> None:
        limiter = Limiter(Clock())
        limiter.configure("global", {"capacity": 40, "refill_per_second": 1.0})
        limiter.configure("tenant-a", {"capacity": 25, "refill_per_second": 1.0})
        outcomes: list[str] = []
        lock = threading.Lock()

        def attempt() -> None:
            try:
                limiter.hierarchy_reserve(["global", "tenant-a"], 2, 600)
                outcome = "reserved"
            except OverQuota:
                outcome = "rejected"
            with lock:
                outcomes.append(outcome)

        threads = [threading.Thread(target=attempt) for _ in range(30)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        reserved = outcomes.count("reserved")
        self.assertEqual(reserved, 12)  # tenant-a caps at floor(25/2)
        self.assertEqual(limiter.state("global")["remaining"], 40 - 2 * reserved)
        self.assertEqual(limiter.state("tenant-a")["remaining"], 25 - 2 * reserved)
        for key in ("global", "tenant-a"):
            self.assertEqual(limiter.ledger(key)["totals"]["accepted_count"], 0)


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

    def configure(self, key: str, capacity: int, rate: float = 1) -> None:
        status, _, _ = self.request("PUT", f"/v1/limits/{key}",
                                    {"capacity": capacity, "refill_per_second": rate})
        self.assertEqual(status, 200)

    def test_full_lifecycle_over_http(self) -> None:
        self.configure("hr-org", 5)
        self.configure("hr-team", 3)
        status, body, _ = self.request("POST", "/v1/hierarchies/reservations",
                                       {"keys": ["hr-org", "hr-team"], "cost": 2, "ttl_seconds": 30})
        self.assertEqual(status, 200)
        self.assertEqual(body["layers"], [{"key": "hr-org", "remaining": 3, "capacity": 5},
                                          {"key": "hr-team", "remaining": 1, "capacity": 3}])
        rid = body["reservation_id"]
        status, body, _ = self.request("POST", f"/v1/hierarchies/reservations/{rid}/consume", {})
        self.assertEqual(status, 200)
        self.assertTrue(body["consumed"])
        self.assertEqual([layer["used"] for layer in body["layers"]], [2, 2])
        _, ledger, _ = self.request("GET", "/v1/ledgers/hr-org")
        self.assertEqual(ledger["events"][0]["source"], "hierarchy_reservation_consume")
        self.assertEqual(ledger["events"][0]["reservation_id"], rid)
        # Replay is byte-identical; a consumed hold cannot be rolled back.
        self.assertEqual(self.request("POST", f"/v1/hierarchies/reservations/{rid}/consume", {})[1], body)
        self.assertEqual(self.request("DELETE", f"/v1/hierarchies/reservations/{rid}")[0], 404)

    def test_rollback_over_http(self) -> None:
        self.configure("hb-org", 5)
        self.configure("hb-team", 3)
        _, body, _ = self.request("POST", "/v1/hierarchies/reservations",
                                  {"keys": ["hb-org", "hb-team"], "cost": 2})
        rid = body["reservation_id"]
        status, body, _ = self.request("DELETE", f"/v1/hierarchies/reservations/{rid}")
        self.assertEqual(status, 200)
        self.assertTrue(body["rolled_back"])
        self.assertEqual([layer["remaining"] for layer in body["layers"]], [5, 3])
        self.assertEqual(self.request("DELETE", f"/v1/hierarchies/reservations/{rid}")[0], 404)
        self.assertEqual(self.request("POST", f"/v1/hierarchies/reservations/{rid}/consume", {})[0], 404)

    def test_over_quota_is_429_with_ceiled_retry_after_and_no_partial_hold(self) -> None:
        self.configure("hq-top", 10)
        self.configure("hq-leaf", 1, 0.5)
        self.request("POST", "/v1/check", {"key": "hq-leaf", "cost": 1})
        status, body, headers = self.request("POST", "/v1/hierarchies/reservations",
                                             {"keys": ["hq-top", "hq-leaf"], "cost": 1})
        self.assertEqual((status, body["error"]["code"]), (429, "over_quota"))
        self.assertEqual(headers["Retry-After"], "2.000")
        _, state, _ = self.request("GET", "/v1/limits/hq-top")
        self.assertEqual(state["remaining"], 10)

    def test_body_shape_errors_are_400(self) -> None:
        for bad in ({"keys": ["a", "b"], "extra": 1}, {"cost": 1},
                    {"keys": ["a", "a"]}, {"keys": ["a"]},
                    {"keys": ["a", "b"], "cost": True},
                    {"keys": ["a", "b"], "ttl_seconds": 0},
                    {"keys": ["a", "b"], "ttl_seconds": 86401}, []):
            status, body, _ = self.request("POST", "/v1/hierarchies/reservations", bad)
            self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"), bad)

    def test_consume_requires_empty_object_body(self) -> None:
        self.configure("hc-org", 5)
        self.configure("hc-team", 3)
        _, body, _ = self.request("POST", "/v1/hierarchies/reservations",
                                  {"keys": ["hc-org", "hc-team"]})
        rid = body["reservation_id"]
        for bad in ({"extra": 1}, [], None):
            status, body, _ = self.request("POST", f"/v1/hierarchies/reservations/{rid}/consume", bad)
            self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"), bad)

    def test_cross_resource_ids_and_unknown_ids_are_404(self) -> None:
        self.configure("hx-key", 5)
        self.configure("hx-org", 5)
        self.configure("hx-team", 3)
        _, single, _ = self.request("POST", "/v1/reservations", {"key": "hx-key"})
        _, cross, _ = self.request("POST", "/v1/hierarchies/reservations",
                                   {"keys": ["hx-org", "hx-team"]})
        single_id, cross_id = single["reservation_id"], cross["reservation_id"]
        self.assertEqual(self.request("POST", f"/v1/hierarchies/reservations/{single_id}/consume", {})[0], 404)
        self.assertEqual(self.request("DELETE", f"/v1/hierarchies/reservations/{single_id}")[0], 404)
        self.assertEqual(self.request("POST", f"/v1/reservations/{cross_id}/consume", {})[0], 404)
        self.assertEqual(self.request("DELETE", f"/v1/reservations/{cross_id}")[0], 404)
        self.assertEqual(self.request("DELETE", "/v1/hierarchies/reservations/nope")[0], 404)

    def test_route_mismatches_are_404(self) -> None:
        self.assertEqual(self.request("GET", "/v1/hierarchies/reservations")[0], 404)
        self.assertEqual(self.request("PUT", "/v1/hierarchies/reservations", {"keys": ["a", "b"]})[0], 404)
        self.assertEqual(self.request("POST", "/v1/hierarchies/reservations/x", {})[0], 404)
        self.assertEqual(self.request("GET", "/v1/hierarchies/reservations/x/consume")[0], 404)
        self.assertEqual(self.request("DELETE", "/v1/hierarchies/reservations/x/consume")[0], 404)
        self.assertEqual(self.request("DELETE", "/v1/hierarchies/reservations")[0], 404)


if __name__ == "__main__":
    unittest.main()
