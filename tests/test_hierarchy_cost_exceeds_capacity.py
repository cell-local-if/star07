"""Cost-above-capacity rejection for the two hierarchy entry points.

POST /v1/hierarchies/check and POST /v1/hierarchies/reservations judge the cost boundary
consistently with POST /v1/check and POST /v1/reservations: keys/cost/ttl are validated first,
then inside the one critical section every layer is confirmed configured in input order (the
404 names the first missing key), and only then — still before the clock is sampled — a legal
cost above any layer's current capacity is rejected as invalid_request, naming the first layer
in input order whose capacity the cost exceeds. No watermark advance, no hold settlement, no
refill, no deduction, no used/ledger write, no reservation, no decision count. Cost exactly AT
every capacity stays legal and proceeds to the ordinary availability judgement.
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


# Each case runs against both public hierarchy operations so their boundary semantics stay in
# lockstep: (method name, decision kind counted in metrics on a genuine 429).
ENTRY_POINTS = (("hierarchy_check", "hierarchy_check"),
                ("hierarchy_reserve", "hierarchy_reservation"))


class HierarchyCostExceedsCapacityUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.limiter = self.fresh_limiter()

    def call(self, limiter: Limiter, method: str, keys: list[str], cost: int, ttl: int = 60):
        if method == "hierarchy_check":
            return limiter.hierarchy_check(keys, cost)
        return limiter.hierarchy_reserve(keys, cost, ttl)

    def fresh_limiter(self) -> Limiter:
        limiter = Limiter(self.clock)
        limiter.configure("global", {"capacity": 10, "refill_per_second": 1.0})
        limiter.configure("tenant-a", {"capacity": 4, "refill_per_second": 0.5})
        return limiter

    def test_cost_equal_to_every_capacity_is_still_legal(self) -> None:
        # tenant-a has the smallest capacity (4): cost 4 equals it, so both entries proceed.
        result = self.limiter.hierarchy_check(["global", "tenant-a"], 4)
        self.assertEqual(result["layers"], [{"key": "global", "remaining": 6, "capacity": 10},
                                            {"key": "tenant-a", "remaining": 0, "capacity": 4}])
        # A fresh limiter for the hold, since the check above spent tenant-a down to zero.
        other = self.fresh_limiter()
        hold = other.hierarchy_reserve(["global", "tenant-a"], 4)
        self.assertEqual(hold["layers"], [{"key": "global", "remaining": 6, "capacity": 10},
                                          {"key": "tenant-a", "remaining": 0, "capacity": 4}])
        self.assertIn(hold["reservation_id"], other._hierarchy_reservations)

    def test_cost_above_a_capacity_is_invalid_request_naming_first_layer(self) -> None:
        for method, _ in ENTRY_POINTS:
            limiter = self.fresh_limiter()
            # Both layers are under cost 11; the message names the first layer in input order.
            with self.assertRaises(InvalidRequest) as raised:
                self.call(limiter, method, ["global", "tenant-a"], 11)
            message = str(raised.exception)
            self.assertIn("exceeds capacity 10", message)
            self.assertIn("'global'", message)
            self.assertNotIn("tenant-a", message)
            # Only the second layer is over: the first offending (and only named) layer is it.
            with self.assertRaises(InvalidRequest) as raised:
                self.call(limiter, method, ["global", "tenant-a"], 5)
            message = str(raised.exception)
            self.assertIn("exceeds capacity 4", message)
            self.assertIn("'tenant-a'", message)
            self.assertNotIn("global", message)

    def test_reordering_changes_which_first_offender_is_named(self) -> None:
        self.limiter.configure("middle", {"capacity": 2, "refill_per_second": 1.0})
        for method, _ in ENTRY_POINTS:
            with self.assertRaises(InvalidRequest) as raised:
                self.call(self.limiter, method, ["global", "middle", "tenant-a"], 3)
            self.assertIn("'middle'", str(raised.exception))     # capacity 2, first over cost 3
            with self.assertRaises(InvalidRequest) as raised:
                self.call(self.limiter, method, ["tenant-a", "middle", "global"], 3)
            self.assertIn("'middle'", str(raised.exception))     # tenant-a(4) is fine, middle first

    def test_unconfigured_layer_is_404_even_when_a_configured_layer_is_over_capacity(self) -> None:
        # Presence is fully confirmed before any capacity comparison: with a missing layer the
        # answer is 404 even though cost 6 exceeds the present tenant-a's capacity 4.
        for method, _ in ENTRY_POINTS:
            with self.assertRaises(LimitNotFound) as raised:
                self.call(self.limiter, method, ["global", "ghost-1", "ghost-2"], 6)
            message = str(raised.exception)
            self.assertIn("ghost-1", message)
            self.assertNotIn("ghost-2", message)
            self.assertNotIn("exceeds", message)

    def test_format_validation_still_precedes_everything(self) -> None:
        for method, _ in ENTRY_POINTS:
            for bad_keys in (["global"], ["global", "global"], ["global", 5], "global"):
                with self.assertRaises(InvalidRequest):
                    self.call(self.limiter, method, bad_keys, 6)
            for bad_cost in (0, True, 1.5, 1_000_001):
                with self.assertRaises(InvalidRequest):
                    self.call(self.limiter, method, ["global", "tenant-a"], bad_cost)
        for bad_ttl in (0, True, 86_401):
            with self.assertRaises(InvalidRequest):
                self.limiter.hierarchy_reserve(["global", "tenant-a"], 6, bad_ttl)

    def test_rejection_touches_no_state_ledger_or_metric(self) -> None:
        self.limiter.hierarchy_check(["global", "tenant-a"], 2)   # g 8/used2, t 2/used2
        before_metrics = self.limiter.metrics()
        for method, _ in ENTRY_POINTS:
            with self.assertRaises(InvalidRequest):
                self.call(self.limiter, method, ["global", "tenant-a"], 5)
        # Nothing held, nothing deducted further, nothing booked, nothing counted.
        self.assertEqual(self.limiter._hierarchy_reservations, {})
        self.assertEqual(self.limiter.state("global")["remaining"], 8)
        self.assertEqual(self.limiter.state("tenant-a")["remaining"], 2)
        for key in ("global", "tenant-a"):
            totals = self.limiter.ledger(key)["totals"]
            self.assertEqual(totals, {"accepted_count": 1, "accepted_cost": 2})
        self.assertEqual(self.limiter.metrics(), before_metrics)

    def test_rejection_never_samples_the_clock_or_advances_the_watermark(self) -> None:
        limiter = self.fresh_limiter()
        limiter.hierarchy_reserve(["global", "tenant-a"], 4, ttl_seconds=10)  # g 6, t 0
        self.clock.t = 1005.0
        for method, _ in ENTRY_POINTS:
            with self.assertRaises(InvalidRequest):
                self.call(limiter, method, ["global", "tenant-a"], 5)   # over tenant-a cap 4
        self.clock.t = 1002.0
        # Had a rejection sampled 1005, the watermark would pin at 1005: this read would then
        # refill global by 5s (capped to 10). Untouched, the effective moment is still 1000, so
        # only 2s are refilled and global holds exactly 8.
        self.assertEqual(limiter.state("global")["remaining"], 8)

    def test_rejection_settles_no_due_hold(self) -> None:
        limiter = self.fresh_limiter()
        rid = limiter.hierarchy_reserve(["global", "tenant-a"], 4, ttl_seconds=10)["reservation_id"]
        self.clock.t += 10                                                  # hold now due
        for method, _ in ENTRY_POINTS:
            with self.assertRaises(InvalidRequest):
                self.call(limiter, method, ["global", "tenant-a"], 5)
        self.assertIn(rid, limiter._hierarchy_reservations)                 # not settled
        state = limiter.state("tenant-a")                                   # this read settles it
        self.assertEqual((state["remaining"], state["used"]), (4, 0))

    def test_cost_at_capacity_but_above_live_tokens_is_still_429(self) -> None:
        limiter = self.fresh_limiter()
        limiter.hierarchy_check(["global", "tenant-a"], 2)                 # g 8, t 2
        # cost 4 equals tenant-a's capacity but exceeds its 2 live tokens: temporary, hence 429
        # with a Retry-After hint (deficit 2 at 0.5/s = 4s), never the capacity 400.
        with self.assertRaises(OverQuota) as raised:
            limiter.hierarchy_check(["global", "tenant-a"], 4)
        self.assertAlmostEqual(raised.exception.retry_after, 4.0, places=6)
        with self.assertRaises(OverQuota) as raised:
            limiter.hierarchy_reserve(["global", "tenant-a"], 4)
        self.assertAlmostEqual(raised.exception.retry_after, 4.0, places=6)
        decisions = limiter.metrics()["metrics"]["decisions"]
        self.assertEqual(decisions["hierarchy_check"]["over_quota"], 1)
        self.assertEqual(decisions["hierarchy_reservation"]["over_quota"], 1)
        # Both temporary rejections left state exactly as after the one accepted hierarchy check.
        self.assertEqual(limiter.state("global")["remaining"], 8)
        self.assertEqual(limiter.state("tenant-a")["remaining"], 2)

    def test_capacity_change_flips_the_verdict_atomically(self) -> None:
        for method, _ in ENTRY_POINTS:
            limiter = self.fresh_limiter()
            with self.assertRaises(InvalidRequest):
                self.call(limiter, method, ["global", "tenant-a"], 5)
            # Capacity raised above the cost leaves invalid_request territory; a capacity increase
            # never conjures tokens, so the untouched 4-token leaf makes this a temporary 429.
            limiter.configure("tenant-a", {"capacity": 8, "refill_per_second": 1.0})
            with self.assertRaises(OverQuota):
                self.call(limiter, method, ["global", "tenant-a"], 5)
            limiter.configure("tenant-a", {"capacity": 4, "refill_per_second": 0.5})
            with self.assertRaises(InvalidRequest):
                self.call(limiter, method, ["global", "tenant-a"], 5)

    def test_concurrent_reconfigure_never_mixes_two_capacity_versions(self) -> None:
        # Layer "hot" flips between capacities 10 and 5; requests carry cost 6 and a second
        # always-big layer. Every request must observe exactly one whole configuration.
        limiter = self.fresh_limiter()
        limiter.configure("hot", {"capacity": 10, "refill_per_second": 0.0001})
        limiter.configure("big", {"capacity": 100, "refill_per_second": 1.0})
        stop = threading.Event()
        outcomes: list[str] = []
        errors: list[BaseException] = []
        list_lock = threading.Lock()

        def reconfigure() -> None:
            try:
                while not stop.is_set():
                    limiter.configure("hot", {"capacity": 5, "refill_per_second": 0.0001})
                    limiter.configure("hot", {"capacity": 10, "refill_per_second": 0.0001})
            except BaseException as error:  # noqa: BLE001
                with list_lock:
                    errors.append(error)

        def attempt(method: str) -> None:
            try:
                for _ in range(40):
                    try:
                        result = limiter.hierarchy_check(["big", "hot"], 6) \
                            if method == "check" else \
                            limiter.hierarchy_reserve(["big", "hot"], 6, 3600)
                        layer = next(layer for layer in result["layers"] if layer["key"] == "hot")
                        if layer["capacity"] != 10:
                            raise AssertionError("admitted against a capacity below 6")
                        outcome = "allowed"
                        if method == "reserve":
                            limiter.hierarchy_rollback(result["reservation_id"])
                    except InvalidRequest:
                        outcome = "invalid"
                    except OverQuota:
                        outcome = "over_quota"
                    with list_lock:
                        outcomes.append(outcome)
            except BaseException as error:  # noqa: BLE001
                with list_lock:
                    errors.append(error)

        threads = [threading.Thread(target=reconfigure) for _ in range(2)]
        threads += [threading.Thread(target=attempt, args=("check",)) for _ in range(4)]
        threads += [threading.Thread(target=attempt, args=("reserve",)) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads[2:]:
            thread.join()
        stop.set()
        for thread in threads[:2]:
            thread.join()
        self.assertEqual(errors, [])
        self.assertEqual(len(outcomes), 320)
        self.assertLessEqual(set(outcomes), {"allowed", "invalid", "over_quota"})
        self.assertIn("invalid", outcomes)
        self.assertEqual(limiter._hierarchy_reservations, {})


class HierarchyCostExceedsCapacityHttpTests(unittest.TestCase):
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

    def test_check_cost_above_layer_capacity_is_400_without_retry_after(self) -> None:
        self.request("PUT", "/v1/limits/hc-a", {"capacity": 5, "refill_per_second": 1})
        self.request("PUT", "/v1/limits/hc-b", {"capacity": 3, "refill_per_second": 1})
        status, body, headers = self.request("POST", "/v1/hierarchies/check",
                                             {"keys": ["hc-a", "hc-b"], "cost": 6})
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        self.assertIn("exceeds capacity 5", body["error"]["message"])
        self.assertIn("hc-a", body["error"]["message"])
        self.assertNotIn("Retry-After", headers)
        # cost 4 only exceeds the second layer: that layer is named.
        status, body, _ = self.request("POST", "/v1/hierarchies/check",
                                       {"keys": ["hc-a", "hc-b"], "cost": 4})
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        self.assertIn("exceeds capacity 3", body["error"]["message"])
        self.assertIn("hc-b", body["error"]["message"])
        # The boundary itself still deducts all layers.
        status, body, _ = self.request("POST", "/v1/hierarchies/check",
                                       {"keys": ["hc-a", "hc-b"], "cost": 3})
        self.assertEqual(status, 200)
        self.assertEqual([layer["remaining"] for layer in body["layers"]], [2, 0])

    def test_reserve_cost_above_layer_capacity_is_400_and_creates_no_hold(self) -> None:
        self.request("PUT", "/v1/limits/hrx-a", {"capacity": 5, "refill_per_second": 1})
        self.request("PUT", "/v1/limits/hrx-b", {"capacity": 5, "refill_per_second": 1})
        _, before_metrics, _ = self.request("GET", "/v1/metrics")
        status, body, headers = self.request("POST", "/v1/hierarchies/reservations",
                                             {"keys": ["hrx-a", "hrx-b"], "cost": 6,
                                              "ttl_seconds": 30})
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        self.assertIn("exceeds capacity 5", body["error"]["message"])
        self.assertNotIn("Retry-After", headers)
        _, state, _ = self.request("GET", "/v1/limits/hrx-a")
        self.assertEqual((state["remaining"], state["used"]), (5, 0))
        _, after_metrics, _ = self.request("GET", "/v1/metrics")
        self.assertEqual(after_metrics, before_metrics)

    def test_missing_layer_is_404_even_when_cost_exceeds_a_present_capacity(self) -> None:
        self.request("PUT", "/v1/limits/hc-real", {"capacity": 2, "refill_per_second": 1})
        for path in ("/v1/hierarchies/check", "/v1/hierarchies/reservations"):
            status, body, headers = self.request("POST", path,
                                                 {"keys": ["hc-real", "hcx-ghost"], "cost": 5})
            self.assertEqual((status, body["error"]["code"]), (404, "not_found"), path)
            self.assertIn("hcx-ghost", body["error"]["message"])
            self.assertNotIn("Retry-After", headers)

    def test_temporary_shortfall_at_equal_capacity_still_429s(self) -> None:
        self.request("PUT", "/v1/limits/hc-c", {"capacity": 3, "refill_per_second": 1})
        self.request("PUT", "/v1/limits/hc-d", {"capacity": 3, "refill_per_second": 1})
        self.request("POST", "/v1/hierarchies/check", {"keys": ["hc-c", "hc-d"], "cost": 3})
        for path in ("/v1/hierarchies/check", "/v1/hierarchies/reservations"):
            status, body, headers = self.request("POST", path,
                                                 {"keys": ["hc-c", "hc-d"], "cost": 3})
            self.assertEqual((status, body["error"]["code"]), (429, "over_quota"), path)
            self.assertEqual(headers["Retry-After"], "3.000")


if __name__ == "__main__":
    unittest.main()
