"""POST /v1/windows/{key}/check with a weighted ``cost``.

The check body extends from the empty object to ``{}`` (unchanged: one unit-weight admission)
or ``{"cost": <integer 1..1000000>}``. Each accepted check books one *occupancy* carrying its
cost; ``used`` is the sum of the live occupancies' costs and admission requires
``used + cost <= max_events``. These tests pin:

* ``{}`` and the omitted/default cost are exactly the old unit-weight behaviour and the response
  gains no fields;
* a legal cost above the window's CURRENT max_events is unsatisfiable: 400 invalid_request with
  no Retry-After, decided inside the lock but before the clock is sampled — no eviction, no used
  change, no window_check decision count;
* every other malformed body/value stays 400 and precedes the unknown-window 404, while a
  well-formed request to an unknown window is 404 and creates nothing;
* an over-quota rejection books nothing and its Retry-After waits for the earliest expiry
  boundary at which the occupancies released so far (same-moment occupancies release together)
  accumulate enough cost to cover this request, ceiled to milliseconds with three decimals;
* concurrent checks can never push live cost above max_events.
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


class WeightedWindowUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.limiter = Limiter(self.clock)
        self.limiter.configure_window("w", {"window_seconds": 10, "max_events": 10})

    def test_empty_object_and_default_cost_keep_unit_weight_semantics(self) -> None:
        defaulted = self.limiter.window_check("w")
        explicit = self.limiter.window_check("w", 1)
        self.assertEqual(defaulted, {"allowed": True, "used": 1, "remaining": 9,
                                     "limit": 10, "window_seconds": 10})
        self.assertEqual(explicit, {"allowed": True, "used": 2, "remaining": 8,
                                    "limit": 10, "window_seconds": 10})

    def test_weighted_admission_counts_cost_sum_not_event_count(self) -> None:
        result = self.limiter.window_check("w", 6)
        self.assertEqual(result, {"allowed": True, "used": 6, "remaining": 4,
                                  "limit": 10, "window_seconds": 10})
        # Two more occupancies worth 4 total: used is a cost sum, so three occupancies read 10.
        self.assertEqual(self.limiter.window_check("w", 3)["used"], 9)
        self.assertEqual(self.limiter.window_check("w", 1)["used"], 10)
        state = self.limiter.window_state("w")
        self.assertEqual(state, {"window": {"window_seconds": 10, "max_events": 10},
                                 "used": 10, "remaining": 0})
        self.assertEqual(len(self.limiter._windows["w"].events), 3)

    def test_cost_equal_to_max_events_fills_the_window_in_one_occupancy(self) -> None:
        result = self.limiter.window_check("w", 10)
        self.assertTrue(result["allowed"])
        self.assertEqual((result["used"], result["remaining"]), (10, 0))
        with self.assertRaises(OverQuota):
            self.limiter.window_check("w", 1)

    def test_over_quota_books_nothing_and_keeps_used(self) -> None:
        self.assertTrue(self.limiter.window_check("w", 6)["allowed"])
        with self.assertRaises(OverQuota):
            self.limiter.window_check("w", 5)            # 6 + 5 > 10
        state = self.limiter.window_state("w")
        self.assertEqual((state["used"], state["remaining"]), (6, 4))
        # The rejected cost is not history: the smaller request that does fit still goes in.
        self.assertEqual(self.limiter.window_check("w", 4)["used"], 10)

    def test_retry_after_waits_for_enough_cumulatively_released_cost(self) -> None:
        # cost 1 leaves at 1010, cost 8 leaves at 1015; used is 9 at t=1005.
        self.clock.t = 1000.0
        self.limiter.window_check("w", 1)
        self.clock.t = 1005.0
        self.limiter.window_check("w", 8)
        # deficit 1: the first boundary (1010) already releases enough -> wait 5.
        with self.assertRaises(OverQuota) as raised:
            self.limiter.window_check("w", 2)
        self.assertAlmostEqual(raised.exception.retry_after, 5.0, places=6)
        # deficit 2: the 1010 batch frees only 1, so the hint waits for the 1015 batch -> 10.
        with self.assertRaises(OverQuota) as raised:
            self.limiter.window_check("w", 3)
        self.assertAlmostEqual(raised.exception.retry_after, 10.0, places=6)

    def test_same_moment_occupancies_release_as_one_batch(self) -> None:
        # Two occupancies admitted at 1000 share one boundary and release 4 together at 1010.
        self.limiter.window_check("w", 2)
        self.limiter.window_check("w", 2)
        self.clock.t = 1006.0
        self.limiter.window_check("w", 6)                # used 10
        # deficit 4: neither 2 alone would suffice, but the 1010 batch frees both -> wait 4.
        with self.assertRaises(OverQuota) as raised:
            self.limiter.window_check("w", 4)
        self.assertAlmostEqual(raised.exception.retry_after, 4.0, places=6)
        # deficit 5: the whole 1010 batch (4) is still short, so the hint waits for 1016 (6 more).
        with self.assertRaises(OverQuota) as raised:
            self.limiter.window_check("w", 5)
        self.assertAlmostEqual(raised.exception.retry_after, 10.0, places=6)

    def test_retry_after_recomputes_after_partial_expiry(self) -> None:
        self.clock.t = 1000.0
        self.limiter.window_check("w", 1)
        self.clock.t = 1005.0
        self.limiter.window_check("w", 8)
        self.clock.t = 1010.0                            # the 1-cost occupancy leaves
        # used 8, available 2: cost 3 needs one more, and the 8-cost batch leaves at 1015.
        with self.assertRaises(OverQuota) as raised:
            self.limiter.window_check("w", 3)
        self.assertAlmostEqual(raised.exception.retry_after, 5.0, places=6)
        # At 1015 everything is gone: the full cost fits with no rejection.
        self.clock.t = 1015.0
        self.assertEqual(self.limiter.window_check("w", 10)["used"], 10)

    def test_inclusive_expiry_boundary_frees_weighted_occupancy_before_judging(self) -> None:
        self.limiter.window_check("w", 10)               # occupancy at 1000
        self.clock.t = 1009.0
        with self.assertRaises(OverQuota):
            self.limiter.window_check("w", 10)
        self.clock.t = 1010.0                            # exactly at the leave boundary
        result = self.limiter.window_check("w", 10)
        self.assertTrue(result["allowed"])
        self.assertEqual(result["used"], 10)

    def test_sub_millisecond_weighted_wait_ceilings_like_the_unit_case(self) -> None:
        self.limiter.configure_window("fast", {"window_seconds": 10, "max_events": 2})
        self.limiter.window_check("fast", 2)             # at 1000
        self.clock.t = 1009.9999
        with self.assertRaises(OverQuota) as raised:
            self.limiter.window_check("fast", 1)
        self.assertAlmostEqual(raised.exception.retry_after, 0.0001, places=9)

    def test_cost_above_max_events_is_invalid_request(self) -> None:
        for cost in (11, 1_000_000):
            with self.assertRaises(InvalidRequest) as raised:
                self.limiter.window_check("w", cost)
            self.assertIn("exceeds max_events 10", str(raised.exception))
            self.assertNotIsInstance(raised.exception, OverQuota)
        # Stable across repeats and never downgraded to over_quota.
        with self.assertRaises(InvalidRequest):
            self.limiter.window_check("w", 11)

    def test_oversized_cost_leaves_history_used_watermark_and_metrics_untouched(self) -> None:
        self.assertTrue(self.limiter.window_check("w", 4)["allowed"])
        before = self.limiter.metrics()
        self.clock.t = 1005.0
        with self.assertRaises(InvalidRequest):
            self.limiter.window_check("w", 11)           # must not tick or evict
        self.clock.t = 1003.0
        # Had the rejection sampled 1005, the watermark would pin there; untouched, the moment
        # is still 1000, so at 1003 nothing has expired and used stays 4.
        state = self.limiter.window_state("w")
        self.assertEqual((state["used"], state["remaining"]), (4, 6))
        self.assertEqual(self.limiter.metrics(), before)

    def test_oversized_cost_runs_before_eviction_so_history_is_not_settled(self) -> None:
        self.limiter.window_check("w", 4)                # at 1000
        self.clock.t = 1010.0                            # it would be stale at a real tick
        with self.assertRaises(InvalidRequest):
            self.limiter.window_check("w", 11)
        # The rejected request neither ticked nor settled: the occupancy is still on record.
        self.assertEqual([cost for _, cost in self.limiter._windows["w"].events], [4])

    def test_capacity_change_flips_the_verdict_atomically(self) -> None:
        self.limiter.window_check("w", 6)
        with self.assertRaises(InvalidRequest):
            self.limiter.window_check("w", 11)
        self.limiter.configure_window("w", {"window_seconds": 10, "max_events": 11})
        with self.assertRaises(OverQuota):               # 6 live; cost 11 is legal but won't fit
            self.limiter.window_check("w", 11)
        self.limiter.configure_window("w", {"window_seconds": 10, "max_events": 10})
        with self.assertRaises(InvalidRequest):
            self.limiter.window_check("w", 11)

    def test_illegal_cost_shape_is_invalid_request_before_the_lock(self) -> None:
        for bad in (0, -1, 1_000_001, True, False, 1.5, "5", None, [3], {"n": 1}):
            with self.assertRaises(InvalidRequest, msg=repr(bad)):
                self.limiter.window_check("w", bad)
        self.assertEqual(self.limiter.window_state("w")["used"], 0)

    def test_format_validation_precedes_unknown_window_404(self) -> None:
        with self.assertRaises(InvalidRequest):
            self.limiter.window_check("ghost", 0)
        with self.assertRaises(InvalidRequest):
            self.limiter.window_check("ghost", True)
        # A well-formed request to an unknown window is 404 and creates nothing.
        with self.assertRaises(LimitNotFound):
            self.limiter.window_check("ghost", 1)
        with self.assertRaises(LimitNotFound):
            self.limiter.window_check("ghost", 1_000_000)
        with self.assertRaises(LimitNotFound):
            self.limiter.window_state("ghost")

    def test_shrinking_window_evicts_weighted_history_and_lowering_keeps_costs(self) -> None:
        self.limiter.window_check("w", 7)                # at 1000
        self.clock.t = 1006.0
        self.limiter.configure_window("w", {"window_seconds": 5, "max_events": 10})
        self.assertEqual(self.limiter.window_state("w")["used"], 0)
        # Lowering max_events never rewrites surviving history.
        self.clock.t = 1007.0
        self.limiter.window_check("w", 6)
        self.limiter.configure_window("w", {"window_seconds": 5, "max_events": 5})
        state = self.limiter.window_state("w")
        self.assertEqual((state["used"], state["remaining"]), (6, -1))

    def test_metrics_count_format_legal_decisions_only(self) -> None:
        self.limiter.window_check("w", 6)                # allowed
        with self.assertRaises(OverQuota):
            self.limiter.window_check("w", 5)            # over_quota
        with self.assertRaises(InvalidRequest):
            self.limiter.window_check("w", 11)           # counts nothing
        with self.assertRaises(InvalidRequest):
            self.limiter.window_check("w", 0)            # counts nothing
        with self.assertRaises(LimitNotFound):
            self.limiter.window_check("ghost", 1)        # counts nothing
        self.assertEqual(self.limiter.metrics()["metrics"]["decisions"]["window_check"],
                         {"allowed": 1, "over_quota": 1})

    def test_concurrent_weighted_checks_never_exceed_max_events(self) -> None:
        limiter = Limiter(self.clock)                    # frozen clock for the whole test
        limiter.configure_window("hot", {"window_seconds": 10, "max_events": 100})
        allowed: list[int] = []
        list_lock = threading.Lock()

        def attempt() -> None:
            try:
                limiter.window_check("hot", 3)
                with list_lock:
                    allowed.append(3)
            except OverQuota:
                pass

        threads = [threading.Thread(target=attempt) for _ in range(200)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        window = limiter._windows["hot"]
        self.assertEqual(sum(cost for _, cost in window.events), 99)  # 33 whole costs fit
        self.assertEqual(len(allowed), 33)
        self.assertEqual(limiter.window_state("hot")["used"], 99)
        # One unit request still fits the leftover, a 2-cost one does not.
        self.assertTrue(limiter.window_check("hot", 1)["allowed"])
        with self.assertRaises(OverQuota):
            limiter.window_check("hot", 2)


class WeightedWindowHttpTests(unittest.TestCase):
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

    def raw_request(self, method: str, path: str, payload: bytes) -> tuple[int, dict, dict]:
        import http.client

        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        connection.putrequest(method, path)
        connection.putheader("Content-Length", str(len(payload)))
        connection.endheaders(payload)
        response = connection.getresponse()
        headers = dict(response.headers)
        body = json.loads(response.read() or b"{}")
        connection.close()
        return response.status, body, headers

    def test_empty_object_and_cost_share_the_unit_weight_path(self) -> None:
        clock = type(self).clock
        clock.t = 5000.0
        self.request("PUT", "/v1/windows/cw-1", {"window_seconds": 10, "max_events": 10})
        status, body, _ = self.request("POST", "/v1/windows/cw-1/check", {})
        self.assertEqual((status, body), (200, {"allowed": True, "used": 1, "remaining": 9,
                                               "limit": 10, "window_seconds": 10}))
        status, body, _ = self.request("POST", "/v1/windows/cw-1/check", {"cost": 1})
        self.assertEqual((body["used"], body["remaining"]), (2, 8))

    def test_weighted_lifecycle_get_and_over_quota_header(self) -> None:
        clock = type(self).clock
        clock.t = 5300.0  # past every other HTTP test's watermark (one shared clock, run alphabetically)
        self.request("PUT", "/v1/windows/cw-2", {"window_seconds": 10, "max_events": 10})
        status, body, _ = self.request("POST", "/v1/windows/cw-2/check", {"cost": 6})
        self.assertEqual((status, body["used"], body["remaining"]), (200, 6, 4))
        status, body, _ = self.request("GET", "/v1/windows/cw-2")
        self.assertEqual(body, {"window": {"window_seconds": 10, "max_events": 10},
                                "used": 6, "remaining": 4})
        # The cost-6 occupancy leaves at 5310: at 5305 used 6 leaves room 4, deficit 1, so the
        # cumulative release at the earliest boundary (6 >= 1) hints "5.000".
        clock.t = 5305.0
        status, body, headers = self.request("POST", "/v1/windows/cw-2/check", {"cost": 5})
        self.assertEqual((status, body["error"]["code"]), (429, "over_quota"))
        self.assertEqual(headers["Retry-After"], "5.000")
        _, state, _ = self.request("GET", "/v1/windows/cw-2")
        self.assertEqual(state["used"], 6)                          # rejection booked nothing

    def test_sub_millisecond_weighted_hint_is_001(self) -> None:
        clock = type(self).clock
        clock.t = 5200.0
        self.request("PUT", "/v1/windows/cw-3", {"window_seconds": 10, "max_events": 3})
        self.request("POST", "/v1/windows/cw-3/check", {"cost": 3})
        clock.t = 5209.9999
        status, _, headers = self.request("POST", "/v1/windows/cw-3/check", {"cost": 1})
        self.assertEqual(status, 429)
        self.assertEqual(headers["Retry-After"], "0.001")

    def test_cost_above_max_events_is_400_without_retry_after(self) -> None:
        self.request("PUT", "/v1/windows/cw-4", {"window_seconds": 10, "max_events": 5})
        status, body, headers = self.request("POST", "/v1/windows/cw-4/check", {"cost": 6})
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        self.assertIn("exceeds max_events 5", body["error"]["message"])
        self.assertNotIn("Retry-After", headers)
        self.assertEqual(self.request("POST", "/v1/windows/cw-4/check", {"cost": 6})[0], 400)
        # The boundary itself admits, and nothing rejected above disturbed used.
        status, body, _ = self.request("POST", "/v1/windows/cw-4/check", {"cost": 5})
        self.assertEqual((status, body["used"], body["remaining"]), (200, 5, 0))

    def test_bad_bodies_are_400_and_unknown_window_well_formed_body_is_404(self) -> None:
        self.request("PUT", "/v1/windows/cw-5", {"window_seconds": 10, "max_events": 3})
        path = "/v1/windows/cw-5/check"
        for payload in (b"[]", b"null", b"5", b'"x"', b'{"x": 1}', b'{"allowed": true}',
                        b'{"cost": 0}', b'{"cost": 1000001}', b'{"cost": true}',
                        b'{"cost": 1.5}', b'{"cost": "1"}', b'{"cost": null}',
                        b'{"cost": 1, "x": 2}'):
            status, parsed, _ = self.raw_request("POST", path, payload)
            self.assertEqual((status, parsed["error"]["code"]), (400, "invalid_request"), payload)
        # Malformed JSON and a missing Content-Length stay 400 as on every JSON route.
        status, parsed, _ = self.raw_request("POST", path, b"{bad json")
        self.assertEqual((status, parsed["error"]["code"]), (400, "invalid_request"))
        # A well-formed body against an unknown window is 404 and never creates it.
        for body in ({}, {"cost": 1}, {"cost": 1_000_000}):
            status, parsed, _ = self.request("POST", "/v1/windows/cw-ghost/check", body)
            self.assertEqual((status, parsed["error"]["code"]), (404, "not_found"), body)
        self.assertEqual(self.request("GET", "/v1/windows/cw-ghost")[0], 404)
        # Rejected requests admitted nothing.
        _, state, _ = self.request("GET", "/v1/windows/cw-5")
        self.assertEqual((state["used"], state["remaining"]), (0, 3))

    def test_decisions_increment_only_for_well_formed_posts(self) -> None:
        self.request("PUT", "/v1/windows/cw-6", {"window_seconds": 10, "max_events": 2})
        _, before, _ = self.request("GET", "/v1/metrics")
        self.request("POST", "/v1/windows/cw-6/check", {"cost": 2})       # allowed
        self.request("POST", "/v1/windows/cw-6/check", {})               # over_quota
        self.request("POST", "/v1/windows/cw-6/check", {"cost": 3})      # 400: not counted
        self.request("POST", "/v1/windows/cw-6/check", {"cost": True})   # 400: not counted
        self.request("POST", "/v1/windows/cw-ghost-2/check", {"cost": 1})  # 404: not counted
        _, after, _ = self.request("GET", "/v1/metrics")
        delta_before = before["metrics"]["decisions"]["window_check"]
        delta_after = after["metrics"]["decisions"]["window_check"]
        self.assertEqual(delta_after["allowed"] - delta_before["allowed"], 1)
        self.assertEqual(delta_after["over_quota"] - delta_before["over_quota"], 1)


if __name__ == "__main__":
    unittest.main()
