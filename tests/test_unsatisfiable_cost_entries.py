"""Never-satisfiable cost on reservations and the two hierarchy entries.

A legal ``cost`` greater than a key's CURRENT capacity can never fit that bucket, however long
the caller waits. ``POST /v1/check`` already rejects that as ``400 invalid_request``; these tests
pin the same boundary on ``POST /v1/reservations``, ``POST /v1/hierarchies/check`` and
``POST /v1/hierarchies/reservations`` so instant deduction, single-key holds and cross-layer
holds judge the cost boundary identically:

* single-key reservation on a configured key: ``400 invalid_request`` naming the capacity,
  never ``429`` and never a ``Retry-After``;
* hierarchy entries: keys/cost/ttl keep their existing validation, then inside the one critical
  section every layer is first confirmed configured in input order (a missing layer is still
  ``404 not_found`` naming the FIRST missing key — configuration confirmation beats the
  capacity 400); only then is cost compared with every layer's CURRENT capacity and the FIRST
  layer in input order whose capacity is below the cost is named on the ``400``;
* cost EQUAL to every capacity stays legal and proceeds to the ordinary availability judgement;
* the rejection is decided before the clock is sampled: no watermark advance, no due-reservation
  settle, no refill, no deduction, no used/ledger write, no reservation, no decision count, no
  revision/ETag change;
* a concurrent PUT installs one whole configuration per request — a request sees the capacity
  set entirely before or entirely after the PUT, never a torn mixture.
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


class ReservationCostBoundaryUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.limiter = Limiter(self.clock)
        self.limiter.configure("tenant-a", {"capacity": 5, "refill_per_second": 1.0})

    def test_cost_equal_to_capacity_is_still_held(self) -> None:
        result = self.limiter.reserve("tenant-a", 5)
        self.assertEqual((result["remaining"], result["capacity"]), (0, 5))

    def test_cost_above_capacity_is_invalid_request_without_retry_after_semantics(self) -> None:
        for cost in (6, 1_000_000):
            with self.assertRaises(InvalidRequest) as raised:
                self.limiter.reserve("tenant-a", cost)
            self.assertIn("exceeds capacity 5", str(raised.exception))
            self.assertNotIsInstance(raised.exception, OverQuota)
        with self.assertRaises(InvalidRequest):
            self.limiter.reserve("tenant-a", 6)

    def test_unknown_key_with_large_cost_is_still_not_found(self) -> None:
        with self.assertRaises(LimitNotFound):
            self.limiter.reserve("tenant-x", 1_000_000)

    def test_rejection_leaves_state_registry_ledger_and_metrics_untouched(self) -> None:
        self.assertTrue(self.limiter.check("tenant-a", 2)["allowed"])   # tokens 3, used 2
        before_metrics = self.limiter.metrics()
        with self.assertRaises(InvalidRequest):
            self.limiter.reserve("tenant-a", 6)
        state = self.limiter.state("tenant-a")
        self.assertEqual((state["remaining"], state["used"]), (3, 2))
        self.assertEqual(self.limiter._reservations, {})
        self.assertEqual(self.limiter.ledger("tenant-a")["totals"],
                         {"accepted_count": 1, "accepted_cost": 2})
        self.assertEqual(self.limiter.metrics(), before_metrics)

    def test_rejection_never_samples_the_clock_or_advances_the_watermark(self) -> None:
        self.limiter.reserve("tenant-a", 5)                             # empty at t=1000
        self.clock.t = 1005.0
        with self.assertRaises(InvalidRequest):
            self.limiter.reserve("tenant-a", 6)                        # must not tick
        self.clock.t = 1003.0
        # Had the rejection sampled 1005, the watermark would pin there and this read would
        # show 5 tokens; untouched, the effective moment is still 1000 and 1003 refills 3.
        self.assertEqual(self.limiter.state("tenant-a")["remaining"], 3)

    def test_rejection_settles_no_due_reservation(self) -> None:
        reservation = self.limiter.reserve("tenant-a", 3, ttl_seconds=10)   # tokens 2
        self.clock.t += 10                                                  # hold is now due
        with self.assertRaises(InvalidRequest):
            self.limiter.reserve("tenant-a", 6)
        self.assertIn(reservation["reservation_id"], self.limiter._reservations)
        state = self.limiter.state("tenant-a")                              # this read settles it
        self.assertEqual((state["remaining"], state["used"]), (5, 0))

    def test_capacity_change_flips_the_verdict_atomically(self) -> None:
        with self.assertRaises(InvalidRequest):
            self.limiter.reserve("tenant-a", 6)
        # Capacity 10 makes cost 6 satisfiable in principle; the 5 surviving tokens make this
        # one a temporary over_quota, never the capacity-boundary invalid_request.
        self.limiter.configure("tenant-a", {"capacity": 10, "refill_per_second": 1.0})
        with self.assertRaises(OverQuota):
            self.limiter.reserve("tenant-a", 6)
        self.limiter.configure("tenant-a", {"capacity": 5, "refill_per_second": 1.0})
        with self.assertRaises(InvalidRequest):
            self.limiter.reserve("tenant-a", 6)


class HierarchyCostBoundaryUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.limiter = Limiter(self.clock)
        self.limiter.configure("global", {"capacity": 10, "refill_per_second": 1.0})
        self.limiter.configure("tenant-a", {"capacity": 4, "refill_per_second": 0.5})
        self.entries = (self.limiter.hierarchy_check,
                        lambda keys, cost: self.limiter.hierarchy_reserve(keys, cost, 60))

    def test_cost_equal_to_every_capacity_is_legal(self) -> None:
        # Fresh state per entry: each entry alone must admit/hold cost exactly equal to the
        # smallest layer's capacity (equality is not an early rejection).
        limiter = Limiter(self.clock)
        limiter.configure("global", {"capacity": 10, "refill_per_second": 1.0})
        limiter.configure("tenant-a", {"capacity": 4, "refill_per_second": 0.5})
        result = limiter.hierarchy_check(["global", "tenant-a"], 4)
        self.assertEqual([layer["remaining"] for layer in result["layers"]], [6, 0])

        limiter = Limiter(self.clock)
        limiter.configure("global", {"capacity": 10, "refill_per_second": 1.0})
        limiter.configure("tenant-a", {"capacity": 4, "refill_per_second": 0.5})
        result = limiter.hierarchy_reserve(["global", "tenant-a"], 4, 60)
        self.assertEqual([layer["remaining"] for layer in result["layers"]], [6, 0])

    def test_cost_equal_to_capacity_but_bucket_empty_still_reaches_over_quota(self) -> None:
        # Equality must not be rejected early: an empty bucket makes the same request a
        # temporary 429 with a finite Retry-After, exactly as before.
        self.limiter.check("tenant-a", 4)
        for entry in self.entries:
            with self.assertRaises(OverQuota):
                entry(["global", "tenant-a"], 4)

    def test_cost_above_a_capacity_names_the_first_offending_layer_in_input_order(self) -> None:
        # global (10) fits cost 5, tenant-a (4) does not: tenant-a is named.
        for entry in self.entries:
            with self.assertRaises(InvalidRequest) as raised:
                entry(["global", "tenant-a"], 5)
            message = str(raised.exception)
            self.assertIn("tenant-a", message)
            self.assertIn("exceeds capacity 4", message)
            self.assertNotIn("global", message)
        # Reversed order: the first layer in input order (capacity 4) is named even though the
        # later layer would fit.
        for entry in self.entries:
            with self.assertRaises(InvalidRequest) as raised:
                entry(["tenant-a", "global"], 5)
            self.assertIn("tenant-a", str(raised.exception))
            self.assertIn("exceeds capacity 4", str(raised.exception))

    def test_unconfigured_layer_is_404_even_when_a_configured_layer_cannot_fit_cost(self) -> None:
        # Configuration is confirmed for EVERY layer first: cost 11 beats global's capacity 10,
        # yet the missing layer still wins as the first unconfigured key, 404 over 400.
        for entry in self.entries:
            with self.assertRaises(LimitNotFound) as raised:
                entry(["global", "ghost-1", "ghost-2"], 11)
            message = str(raised.exception)
            self.assertIn("ghost-1", message)
            self.assertNotIn("ghost-2", message)

    def test_format_validation_still_precedes_the_lock_boundary(self) -> None:
        for entry in self.entries:
            with self.assertRaises(InvalidRequest):
                entry(["global"], 6)                    # too few keys
            with self.assertRaises(InvalidRequest):
                entry(["global", "tenant-a"], 0)        # illegal cost

    def test_rejection_changes_no_state_ledger_or_metrics(self) -> None:
        self.limiter.hierarchy_check(["global", "tenant-a"], 2)   # global 8, tenant-a 2, used 2
        before_metrics = self.limiter.metrics()
        for entry in self.entries:
            with self.assertRaises(InvalidRequest):
                entry(["global", "tenant-a"], 5)
        for key, remaining in (("global", 8), ("tenant-a", 2)):
            state = self.limiter.state(key)
            self.assertEqual((state["remaining"], state["used"]), (remaining, 2))
            self.assertEqual(self.limiter.ledger(key)["totals"],
                             {"accepted_count": 1, "accepted_cost": 2})
        self.assertEqual(self.limiter._hierarchy_reservations, {})
        self.assertEqual(self.limiter.metrics(), before_metrics)

    def test_rejection_never_samples_the_clock_or_advances_the_watermark(self) -> None:
        self.limiter.hierarchy_check(["global", "tenant-a"], 4)   # global 6, tenant-a 0
        self.clock.t = 1005.0
        for entry in self.entries:
            with self.assertRaises(InvalidRequest):
                entry(["global", "tenant-a"], 5)                  # must not tick
        self.clock.t = 1003.0
        # Without the forbidden sample, refill runs from 1000 for 3 seconds: 6 + 3 = 9.
        self.assertEqual(self.limiter.state("global")["remaining"], 9)

    def test_rejection_settles_no_due_reservation_on_any_layer(self) -> None:
        reservation = self.limiter.reserve("tenant-a", 3, ttl_seconds=10)  # tenant-a holds 1
        cross = self.limiter.hierarchy_reserve(["global", "tenant-a"], 1, ttl_seconds=10)
        self.clock.t += 10                                                  # both are due
        for entry in self.entries:
            with self.assertRaises(InvalidRequest):
                entry(["global", "tenant-a"], 11)
        self.assertIn(reservation["reservation_id"], self.limiter._reservations)
        self.assertIn(cross["reservation_id"], self.limiter._hierarchy_reservations)
        # A later read settles them: the rejected requests did not.
        self.assertEqual(self.limiter.state("tenant-a")["remaining"], 4)
        self.assertEqual(self.limiter.state("global")["remaining"], 10)

    def test_concurrent_reconfigure_and_requests_never_see_a_torn_capacity(self) -> None:
        limiter = Limiter(self.clock)                   # clock frozen for the whole test
        limiter.configure("hot", {"capacity": 10, "refill_per_second": 0.0001})
        limiter.configure("stable", {"capacity": 1_000_000, "refill_per_second": 0.0001})
        stop = threading.Event()
        outcomes: list[str] = []
        errors: list[BaseException] = []
        list_lock = threading.Lock()

        def reconfigure() -> None:
            try:
                while not stop.is_set():
                    limiter.configure("hot", {"capacity": 5, "refill_per_second": 0.0001})
                    limiter.configure("hot", {"capacity": 10, "refill_per_second": 0.0001})
            except BaseException as error:  # noqa: BLE001 - surface thread failures
                with list_lock:
                    errors.append(error)

        def request(entry_name: str) -> None:
            try:
                for _ in range(50):
                    try:
                        if entry_name == "check":
                            result = limiter.hierarchy_check(["hot", "stable"], 6)
                        else:
                            result = limiter.hierarchy_reserve(["hot", "stable"], 6, 3600)
                        hot_layer = next(layer for layer in result["layers"] if layer["key"] == "hot")
                        if hot_layer["capacity"] != 10:
                            raise AssertionError("admitted against capacity 5")
                        outcome = "allowed"
                    except InvalidRequest:
                        outcome = "invalid"             # saw hot at capacity 5
                    except OverQuota:
                        outcome = "over_quota"          # saw capacity 10, bucket too empty
                    with list_lock:
                        outcomes.append(outcome)
            except BaseException as error:  # noqa: BLE001
                with list_lock:
                    errors.append(error)

        threads = [threading.Thread(target=reconfigure) for _ in range(2)]
        threads += [threading.Thread(target=request, args=("check",)) for _ in range(4)]
        threads += [threading.Thread(target=request, args=("reserve",)) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads[2:]:
            thread.join()
        stop.set()
        for thread in threads[:2]:
            thread.join()
        self.assertEqual(errors, [])
        self.assertEqual(len(outcomes), 400)
        # Every request observed exactly one whole configuration: no admission against the
        # capacity-5 version, no unexpected error kinds.
        self.assertLessEqual(set(outcomes), {"allowed", "invalid", "over_quota"})
        self.assertIn("invalid", outcomes)
        limiter.configure("hot", {"capacity": 5, "refill_per_second": 0.0001})
        for entry in (lambda: limiter.hierarchy_check(["hot", "stable"], 6),
                      lambda: limiter.hierarchy_reserve(["hot", "stable"], 6, 3600)):
            with self.assertRaises(InvalidRequest):
                entry()


class CostBoundaryHttpTests(unittest.TestCase):
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

    def test_reservation_above_capacity_is_400_without_retry_after(self) -> None:
        self.request("PUT", "/v1/limits/ub-1", {"capacity": 5, "refill_per_second": 1})
        status, body, headers = self.request("POST", "/v1/reservations", {"key": "ub-1", "cost": 6})
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        self.assertIn("exceeds capacity 5", body["error"]["message"])
        self.assertNotIn("Retry-After", headers)
        self.assertEqual(self.request("POST", "/v1/reservations", {"key": "ub-1", "cost": 6})[0], 400)
        # The boundary itself still creates the hold.
        status, body, _ = self.request("POST", "/v1/reservations", {"key": "ub-1", "cost": 5})
        self.assertEqual((status, body["remaining"]), (200, 0))

    def test_hierarchy_entries_above_capacity_are_400_naming_the_layer(self) -> None:
        self.request("PUT", "/v1/limits/ub-org", {"capacity": 10, "refill_per_second": 1})
        self.request("PUT", "/v1/limits/ub-leaf", {"capacity": 4, "refill_per_second": 1})
        for path in ("/v1/hierarchies/check", "/v1/hierarchies/reservations"):
            status, body, headers = self.request("POST", path,
                                                 {"keys": ["ub-org", "ub-leaf"], "cost": 5})
            self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"), path)
            self.assertIn("ub-leaf", body["error"]["message"])
            self.assertIn("exceeds capacity 4", body["error"]["message"])
            self.assertNotIn("Retry-After", headers)

    def test_cost_equal_to_capacity_still_flows_through_the_hierarchy_entries(self) -> None:
        self.request("PUT", "/v1/limits/ub-eq-1", {"capacity": 4, "refill_per_second": 1})
        self.request("PUT", "/v1/limits/ub-eq-2", {"capacity": 4, "refill_per_second": 1})
        status, body, _ = self.request("POST", "/v1/hierarchies/check",
                                       {"keys": ["ub-eq-1", "ub-eq-2"], "cost": 4})
        self.assertEqual(status, 200)
        self.assertEqual([layer["remaining"] for layer in body["layers"]], [0, 0])
        # Same cost on the now-empty layers is a temporary 429 — equality was not pre-rejected.
        status, body, headers = self.request("POST", "/v1/hierarchies/reservations",
                                             {"keys": ["ub-eq-1", "ub-eq-2"], "cost": 4})
        self.assertEqual((status, body["error"]["code"]), (429, "over_quota"))
        self.assertIn("Retry-After", headers)

    def test_missing_layer_is_404_before_the_capacity_boundary(self) -> None:
        self.request("PUT", "/v1/limits/ub-real", {"capacity": 10, "refill_per_second": 1})
        for path in ("/v1/hierarchies/check", "/v1/hierarchies/reservations"):
            status, body, headers = self.request("POST", path,
                                                 {"keys": ["ub-real", "ub-ghost"], "cost": 11})
            self.assertEqual((status, body["error"]["code"]), (404, "not_found"), path)
            self.assertIn("ub-ghost", body["error"]["message"])
            self.assertNotIn("Retry-After", headers)

    def test_rejections_change_no_state_metrics_or_revision(self) -> None:
        self.request("PUT", "/v1/limits/ub-s1", {"capacity": 4, "refill_per_second": 1})
        self.request("PUT", "/v1/limits/ub-s2", {"capacity": 4, "refill_per_second": 1})
        _, _, before_headers1 = self.request("GET", "/v1/limits/ub-s1")
        _, _, before_headers2 = self.request("GET", "/v1/limits/ub-s2")
        _, before_metrics, _ = self.request("GET", "/v1/metrics")
        self.request("POST", "/v1/reservations", {"key": "ub-s1", "cost": 5})
        for path in ("/v1/hierarchies/check", "/v1/hierarchies/reservations"):
            self.request("POST", path, {"keys": ["ub-s1", "ub-s2"], "cost": 5})
        _, state1, after_headers1 = self.request("GET", "/v1/limits/ub-s1")
        _, state2, after_headers2 = self.request("GET", "/v1/limits/ub-s2")
        self.assertEqual((state1["remaining"], state1["used"]), (4, 0))
        self.assertEqual((state2["remaining"], state2["used"]), (4, 0))
        self.assertEqual(after_headers1["ETag"], before_headers1["ETag"])
        self.assertEqual(after_headers2["ETag"], before_headers2["ETag"])
        _, after_metrics, _ = self.request("GET", "/v1/metrics")
        self.assertEqual(after_metrics, before_metrics)


if __name__ == "__main__":
    unittest.main()
