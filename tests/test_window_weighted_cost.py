"""Weighted sliding-window checks: POST /v1/windows/{key}/check accepts a cost.

The check body is now ``{}`` (cost defaulting to 1, byte-for-byte the baseline behaviour) or
``{"cost": <int 1..1000000>}``. Every admitted check registers one weighted occupancy
``(effective_at, cost)`` and the live ``used`` is the COST SUM of surviving occupancies, so these
tests pin, beyond the baseline count-based behaviour already covered in test_windows.py:

* weighted admission sums cost and remaining = max_events - used; equality used + cost ==
  max_events still fits;
* a legal cost above a CONFIGURED window's current max_events is 400 invalid_request (never
  200/429, never a Retry-After), decided before the clock is sampled: no watermark advance, no
  eviction, no used change, no window_check metric — and reads the same locked configuration a
  concurrent PUT installs;
* an unknown window keeps 404 not_found for every structurally legal cost and is never created,
  while structurally illegal cost is 400 before the lookup (format beats not_found, as on every
  other entry);
* on over_quota the Retry-After waits until the oldest same-moment batch whose cumulative
  released cost first makes room leaves (all occupancies sharing a moment free together),
  ceiled to whole milliseconds with three decimals;
* every structurally legal check — allowed or over_quota — increments exactly one window_check
  counter; 400 and 404 count nothing;
* weighted concurrency never lets the surviving cost sum exceed max_events.
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

    def test_empty_object_and_missing_arg_still_mean_cost_one(self) -> None:
        first = self.limiter.window_check("w")
        self.assertEqual(first, {"allowed": True, "used": 1, "remaining": 9,
                                "limit": 10, "window_seconds": 10})
        second = self.limiter.window_check("w", 1)
        self.assertEqual((second["used"], second["remaining"]), (2, 8))

    def test_weighted_admission_sums_cost(self) -> None:
        result = self.limiter.window_check("w", 4)
        self.assertEqual((result["used"], result["remaining"], result["limit"]), (4, 6, 10))
        result = self.limiter.window_check("w", 3)
        self.assertEqual((result["used"], result["remaining"]), (7, 3))
        self.assertEqual(self.limiter.window_state("w")["used"], 7)

    def test_cost_filling_the_window_exactly_is_admitted(self) -> None:
        result = self.limiter.window_check("w", 10)
        self.assertTrue(result["allowed"])
        self.assertEqual((result["used"], result["remaining"]), (10, 0))
        # One more unit no longer fits: ordinary 429, not the capacity 400.
        with self.assertRaises(OverQuota):
            self.limiter.window_check("w", 1)

    def test_cost_equal_to_max_events_fits_an_empty_window_only(self) -> None:
        self.limiter.window_check("w", 1)
        with self.assertRaises(OverQuota):
            self.limiter.window_check("w", 10)               # legal cost, just no room
        self.clock.t = 1010.0                                # the cost-1 leaves
        result = self.limiter.window_check("w", 10)
        self.assertTrue(result["allowed"])
        self.assertEqual((result["used"], result["remaining"]), (10, 0))

    def test_cost_above_max_events_is_invalid_request_and_stable(self) -> None:
        for cost in (11, 1_000_000):
            with self.assertRaises(InvalidRequest) as raised:
                self.limiter.window_check("w", cost)
            self.assertIn("exceeds max_events 10", str(raised.exception))
            self.assertNotIsInstance(raised.exception, OverQuota)
        with self.assertRaises(InvalidRequest):
            self.limiter.window_check("w", 11)

    def test_invalid_cost_shapes_are_rejected_before_the_lock(self) -> None:
        for bad in (0, -1, 1_000_001, True, False, 1.5, "3", None, [3], {"cost": 3}):
            with self.assertRaises(InvalidRequest, msg=repr(bad)):
                self.limiter.window_check("w", bad)
        # Format validation beats the window lookup: an illegal cost on an unknown key is 400,
        # exactly as on POST /v1/check.
        for bad in (0, True, 1_000_001):
            with self.assertRaises(InvalidRequest, msg=repr(bad)):
                self.limiter.window_check("ghost", bad)

    def test_unknown_window_with_a_legal_cost_is_404_and_is_not_created(self) -> None:
        for cost in (1, 11, 1_000_000):
            with self.assertRaises(LimitNotFound, msg=repr(cost)):
                self.limiter.window_check("ghost", cost)
        with self.assertRaises(LimitNotFound):
            self.limiter.window_state("ghost")                # nothing was created

    def test_oversized_cost_rejection_leaves_used_and_history_untouched(self) -> None:
        self.limiter.window_check("w", 4)                     # occupancy 4 at 1000
        self.clock.t = 1005.0
        before = self.limiter.metrics()
        with self.assertRaises(InvalidRequest):
            self.limiter.window_check("w", 11)
        # No eviction happened as part of the rejection: the 1000 occupancy is still recorded
        # even though 1005-1000 = 5 < 10 would keep it anyway; force a would-be-stale case below.
        self.assertEqual(self.limiter._windows["w"].events, [(1000.0, 4)])
        state = self.limiter.window_state("w")                # this read ticks normally
        self.assertEqual((state["used"], state["remaining"]), (4, 6))
        self.assertEqual(self.limiter.metrics(), before)      # neither allowed nor over_quota

    def test_oversized_cost_rejection_neither_evicts_nor_samples_the_clock(self) -> None:
        self.limiter.configure_window("stale", {"window_seconds": 10, "max_events": 5})
        self.limiter.window_check("stale", 2)                 # occupancy at 1000
        self.clock.t = 1020.0                                 # it would be stale now
        with self.assertRaises(InvalidRequest):
            self.limiter.window_check("stale", 6)
        # The rejection neither evicted ...
        self.assertEqual(self.limiter._windows["stale"].events, [(1000.0, 2)])
        self.clock.t = 1005.0                                 # clock jumps backwards
        # ... nor pinned the watermark at 1020: at this read the 1000 occupancy still survives
        # (cutoff 995), whereas a forbidden sample at 1020 would already have evicted it.
        self.assertEqual(self.limiter.window_state("stale")["used"], 2)

    def test_max_events_change_flips_the_verdict_atomically(self) -> None:
        with self.assertRaises(InvalidRequest):
            self.limiter.window_check("w", 11)
        self.limiter.configure_window("w", {"window_seconds": 10, "max_events": 20})
        result = self.limiter.window_check("w", 11)           # now satisfiable and admitted
        self.assertTrue(result["allowed"])
        self.assertEqual(result["used"], 11)
        self.limiter.configure_window("w", {"window_seconds": 10, "max_events": 10})
        with self.assertRaises(InvalidRequest):
            self.limiter.window_check("w", 11)

    def test_rejection_runs_no_eviction_even_when_history_could_leave(self) -> None:
        self.limiter.window_check("w", 6)                    # at 1000
        self.clock.t = 1010.0                                 # exactly on the leave boundary
        with self.assertRaises(InvalidRequest):
            self.limiter.window_check("w", 11)               # must not evict first
        self.assertEqual(self.limiter._windows["w"].events, [(1000.0, 6)])
        # A legal check at this same moment DOES settle the boundary occupancy first.
        result = self.limiter.window_check("w", 10)
        self.assertTrue(result["allowed"])
        self.assertEqual(result["used"], 10)

    def test_over_quota_appends_nothing_and_reports_weighted_used(self) -> None:
        self.limiter.window_check("w", 6)
        self.limiter.window_check("w", 3)
        with self.assertRaises(OverQuota) as raised:
            self.limiter.window_check("w", 2)                 # 9 + 2 > 10
        self.assertAlmostEqual(raised.exception.retry_after, 10.0, places=6)
        self.assertEqual(self.limiter.window_state("w")["used"], 9)

    def test_retry_after_waits_for_cumulative_batch_release(self) -> None:
        # Occupancies 4@1000, 4@1001, 2@1002: full at 10.
        self.clock.t = 1000.0
        self.limiter.window_check("w", 4)
        self.clock.t = 1001.0
        self.limiter.window_check("w", 4)
        self.clock.t = 1002.0
        self.limiter.window_check("w", 2)
        # Releasing just the 1000 batch leaves used 6: cost 3 fits (6+3<=10) -> wait to 1010.
        with self.assertRaises(OverQuota) as raised:
            self.limiter.window_check("w", 3)
        self.assertAlmostEqual(raised.exception.retry_after, 8.0, places=6)
        # cost 5: after the 1000 batch 6+5=11 > 10; the 1001 batch must also leave (used 2) -> 1011.
        with self.assertRaises(OverQuota) as raised:
            self.limiter.window_check("w", 5)
        self.assertAlmostEqual(raised.exception.retry_after, 9.0, places=6)
        # cost 9: neither the 1000 (4) nor 1001 (4) cumulative release suffices (2+9>10); the
        # current 1002 batch must leave too, emptying the window -> 1012.
        with self.assertRaises(OverQuota) as raised:
            self.limiter.window_check("w", 9)
        self.assertAlmostEqual(raised.exception.retry_after, 10.0, places=6)

    def test_same_moment_occupancies_are_released_as_one_batch(self) -> None:
        # Two cost-4 occupancies share moment 1000 (8), then cost 2 at 1002 fills the window.
        self.limiter.window_check("w", 4)
        self.limiter.window_check("w", 4)
        self.clock.t = 1002.0
        self.limiter.window_check("w", 2)
        # cost 5 cannot fit on the first cost-4 occupancy alone but the whole 1000 batch (8)
        # frees room at once: the hinted boundary is the shared 1000+10, never an interior
        # split of one same-moment batch.
        with self.assertRaises(OverQuota) as raised:
            self.limiter.window_check("w", 5)
        self.assertAlmostEqual(raised.exception.retry_after, 8.0, places=6)
        # At 1010 both 1000 occupancies leave together; only the cost-2 survives.
        self.clock.t = 1010.0
        with self.assertRaises(OverQuota) as raised:
            self.limiter.window_check("w", 9)             # 2 + 9 > 10: wait for the 1002 batch
        self.assertAlmostEqual(raised.exception.retry_after, 2.0, places=6)

    def test_sub_millisecond_weighted_wait_ceilings_up(self) -> None:
        self.limiter.configure_window("fast", {"window_seconds": 10, "max_events": 4})
        self.limiter.window_check("fast", 4)                  # at 1000
        self.clock.t = 1009.9999
        with self.assertRaises(OverQuota) as raised:
            self.limiter.window_check("fast", 1)
        self.assertAlmostEqual(raised.exception.retry_after, 0.0001, places=9)

    def test_weighted_eviction_uses_inclusive_boundary(self) -> None:
        self.limiter.window_check("w", 4)                    # at 1000
        self.clock.t = 1009.0
        self.assertEqual(self.limiter.window_state("w")["used"], 4)
        self.clock.t = 1010.0                                 # cutoff exactly 1000
        self.assertEqual(self.limiter.window_state("w")["used"], 0)
        result = self.limiter.window_check("w", 10)
        self.assertTrue(result["allowed"])
        self.assertEqual(result["used"], 10)

    def test_reconfigure_shorten_evicts_weighted_history(self) -> None:
        self.limiter.window_check("w", 6)                    # at 1000
        self.clock.t = 1006.0
        self.limiter.configure_window("w", {"window_seconds": 5, "max_events": 10})
        self.assertEqual(self.limiter.window_state("w")["used"], 0)

    def test_lowering_max_events_keeps_weighted_history_and_rejects_later_checks(self) -> None:
        self.limiter.window_check("w", 6)                    # 6 at 1000
        self.limiter.configure_window("w", {"window_seconds": 10, "max_events": 5})
        state = self.limiter.window_state("w")
        self.assertEqual((state["used"], state["remaining"]), (6, -1))
        # Even cost 1 is an ordinary 429 against the surviving occupancy ...
        with self.assertRaises(OverQuota):
            self.limiter.window_check("w", 1)
        # ... and cost 6 exceeds the new max_events outright: 400 invalid_request.
        with self.assertRaises(InvalidRequest):
            self.limiter.window_check("w", 6)

    def test_metrics_count_legal_allowed_and_over_quota_only(self) -> None:
        before = self.limiter.metrics()["metrics"]["decisions"]["window_check"]
        self.limiter.window_check("w", 4)                     # allowed
        with self.assertRaises(OverQuota):
            self.limiter.window_check("w", 8)                 # 4 + 8 > 10: over_quota
        with self.assertRaises(InvalidRequest):
            self.limiter.window_check("w", 11)                # 400: not counted
        with self.assertRaises(InvalidRequest):
            self.limiter.window_check("w", 0)                 # 400: not counted
        with self.assertRaises(LimitNotFound):
            self.limiter.window_check("ghost", 1)             # 404: not counted
        self.limiter.window_state("w")                        # reads never count
        after = self.limiter.metrics()["metrics"]["decisions"]["window_check"]
        self.assertEqual(after["allowed"] - before["allowed"], 1)
        self.assertEqual(after["over_quota"] - before["over_quota"], 1)

    def test_window_isolation_from_buckets_holds_with_weights(self) -> None:
        self.limiter.configure("shared", {"capacity": 3, "refill_per_second": 1.0})
        self.limiter.configure_window("shared", {"window_seconds": 10, "max_events": 2})
        self.assertTrue(self.limiter.check("shared", 3)["allowed"])
        self.assertTrue(self.limiter.window_check("shared", 2)["allowed"])
        with self.assertRaises(OverQuota):
            self.limiter.window_check("shared", 1)
        self.assertEqual(self.limiter.state("shared")["used"], 3)
        self.assertEqual(self.limiter.window_state("shared")["used"], 2)
        self.assertEqual(self.limiter.ledger("shared")["totals"],
                         {"accepted_count": 1, "accepted_cost": 3})

    def test_concurrent_weighted_checks_never_exceed_max_events(self) -> None:
        limiter = Limiter(self.clock)                        # frozen clock for the whole test
        limiter.configure_window("hot", {"window_seconds": 10, "max_events": 100})
        outcomes: list[str] = []
        outcomes_lock = threading.Lock()

        def attempt(cost: int) -> None:
            try:
                limiter.window_check("hot", cost)
                outcome = "allowed"
            except OverQuota:
                outcome = "over_quota"
            except BaseException as error:  # noqa: BLE001 - surface thread failures
                outcome = f"error:{error!r}"
            with outcomes_lock:
                outcomes.append(outcome)

        threads = ([threading.Thread(target=attempt, args=(3,)) for _ in range(300)]
                   + [threading.Thread(target=attempt, args=(7,)) for _ in range(100)])
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertTrue(all(o in ("allowed", "over_quota") for o in outcomes), outcomes)
        admitted = [cost for _, cost in limiter._windows["hot"].events]
        used = sum(admitted)
        self.assertLessEqual(used, 100)                       # never over the limit
        self.assertTrue(set(admitted) <= {3, 7})
        # After all 400 checks completed on a frozen clock the occupancy must be stable against
        # the smallest cost offered (3): a later cost-3 thread would otherwise have been admitted.
        self.assertGreaterEqual(used, 98)
        self.assertEqual(limiter.window_state("hot")["used"], used)
        self.assertEqual(outcomes.count("allowed"), len(admitted))
        self.assertEqual(outcomes.count("allowed") + outcomes.count("over_quota"), 400)


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

    def request(self, method: str, path: str, body: object = None) -> tuple[int, dict, dict]:
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
                                         headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read() or b"{}"), dict(response.headers)
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read() or b"{}"), dict(error.headers)

    def raw_request(self, method: str, path: str, payload: bytes | None,
                    content_length: str | object = "auto") -> tuple[int, dict, dict]:
        import http.client

        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        connection.putrequest(method, path)
        if content_length != "omit":
            connection.putheader("Content-Length",
                                 str(len(payload)) if content_length == "auto" else content_length)
        connection.endheaders(payload if payload is not None else b"")
        response = connection.getresponse()
        headers = dict(response.headers)
        body = json.loads(response.read() or b"{}")
        connection.close()
        return response.status, body, headers

    def test_empty_object_and_cost_share_the_admission_shape(self) -> None:
        clock = type(self).clock
        clock.t = 6100.0
        self.request("PUT", "/v1/windows/wh-1", {"window_seconds": 10, "max_events": 10})
        status, body, _ = self.request("POST", "/v1/windows/wh-1/check", {})
        self.assertEqual((status, body), (200, {"allowed": True, "used": 1, "remaining": 9,
                                               "limit": 10, "window_seconds": 10}))
        status, body, _ = self.request("POST", "/v1/windows/wh-1/check", {"cost": 4})
        self.assertEqual((status, body["used"], body["remaining"]), (200, 5, 5))
        status, body, _ = self.request("GET", "/v1/windows/wh-1")
        self.assertEqual(body, {"window": {"window_seconds": 10, "max_events": 10},
                                "used": 5, "remaining": 5})

    def test_weighted_over_quota_retry_after_uses_cumulative_release(self) -> None:
        clock = type(self).clock
        clock.t = 6400.0
        self.request("PUT", "/v1/windows/wh-2", {"window_seconds": 10, "max_events": 10})
        self.request("POST", "/v1/windows/wh-2/check", {"cost": 4})
        clock.t = 6401.0
        self.request("POST", "/v1/windows/wh-2/check", {"cost": 4})
        clock.t = 6402.0
        self.request("POST", "/v1/windows/wh-2/check", {"cost": 2})
        status, _, headers = self.request("POST", "/v1/windows/wh-2/check", {"cost": 3})
        self.assertEqual(status, 429)
        self.assertEqual(headers["Retry-After"], "8.000")
        status, _, headers = self.request("POST", "/v1/windows/wh-2/check", {"cost": 5})
        self.assertEqual(status, 429)
        self.assertEqual(headers["Retry-After"], "9.000")

    def test_sub_millisecond_weighted_wait_is_001(self) -> None:
        clock = type(self).clock
        clock.t = 6300.0
        self.request("PUT", "/v1/windows/wh-3", {"window_seconds": 10, "max_events": 4})
        self.request("POST", "/v1/windows/wh-3/check", {"cost": 4})
        clock.t = 6309.9999
        status, _, headers = self.request("POST", "/v1/windows/wh-3/check", {"cost": 1})
        self.assertEqual(status, 429)
        self.assertEqual(headers["Retry-After"], "0.001")

    def test_cost_above_max_events_is_400_without_retry_after(self) -> None:
        type(self).clock.t = 6000.0
        self.request("PUT", "/v1/windows/wh-4", {"window_seconds": 10, "max_events": 10})
        status, body, headers = self.request("POST", "/v1/windows/wh-4/check", {"cost": 11})
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        self.assertIn("exceeds max_events 10", body["error"]["message"])
        self.assertNotIn("Retry-After", headers)
        # Stable on repetition; the boundary itself still admits.
        self.assertEqual(self.request("POST", "/v1/windows/wh-4/check", {"cost": 11})[0], 400)
        status, body, _ = self.request("POST", "/v1/windows/wh-4/check", {"cost": 10})
        self.assertEqual((status, body["allowed"], body["remaining"]), (200, True, 0))

    def test_illegal_cost_shapes_are_400(self) -> None:
        self.request("PUT", "/v1/windows/wh-5", {"window_seconds": 10, "max_events": 10})
        path = "/v1/windows/wh-5/check"
        for payload in (b'{"cost": 0}', b'{"cost": -1}', b'{"cost": 1000001}',
                        b'{"cost": true}', b'{"cost": 1.5}', b'{"cost": "3"}',
                        b'{"cost": null}', b'{"cost": [3]}',
                        b'{"x": 1}', b'{"cost": 1, "x": 2}',
                        b"[]", b"null", b"5", b'"x"'):
            status, parsed, _ = self.raw_request("POST", path, payload)
            self.assertEqual((status, parsed["error"]["code"]), (400, "invalid_request"), payload)
        status, parsed, _ = self.raw_request("POST", path, b'{bad json')
        self.assertEqual((status, parsed["error"]["code"]), (400, "invalid_request"))
        status, parsed, _ = self.raw_request("POST", path, b"{}", content_length="omit")
        self.assertEqual((status, parsed["error"]["code"]), (400, "invalid_request"))
        # Nothing was admitted by any rejected request.
        _, state, _ = self.request("GET", "/v1/windows/wh-5")
        self.assertEqual((state["used"], state["remaining"]), (0, 10))

    def test_unknown_window_legal_cost_is_404_illegal_cost_is_400(self) -> None:
        status, body, headers = self.request("POST", "/v1/windows/wh-ghost/check", {"cost": 11})
        self.assertEqual((status, body["error"]["code"]), (404, "not_found"))
        self.assertNotIn("Retry-After", headers)
        status, body, _ = self.request("POST", "/v1/windows/wh-ghost/check", {})
        self.assertEqual((status, body["error"]["code"]), (404, "not_found"))
        self.assertEqual(self.request("GET", "/v1/windows/wh-ghost")[0], 404)
        status, body, _ = self.request("POST", "/v1/windows/wh-ghost/check", {"cost": 0})
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))

    def test_metrics_count_only_legal_200_and_429(self) -> None:
        clock = type(self).clock
        clock.t = 6200.0
        self.request("PUT", "/v1/windows/wh-6", {"window_seconds": 10, "max_events": 4})
        _, before, _ = self.request("GET", "/v1/metrics")
        self.request("POST", "/v1/windows/wh-6/check", {"cost": 2})      # allowed
        self.request("POST", "/v1/windows/wh-6/check", {"cost": 3})      # 429
        self.request("POST", "/v1/windows/wh-6/check", {"cost": 5})      # 400
        self.request("POST", "/v1/windows/wh-6/check", {"cost": 0})      # 400
        self.request("POST", "/v1/windows/wh-absent/check", {"cost": 1})  # 404
        _, after, _ = self.request("GET", "/v1/metrics")
        self.assertEqual(after["metrics"]["decisions"]["window_check"],
                         {"allowed": before["metrics"]["decisions"]["window_check"]["allowed"] + 1,
                          "over_quota": before["metrics"]["decisions"]["window_check"]["over_quota"] + 1})


if __name__ == "__main__":
    unittest.main()
