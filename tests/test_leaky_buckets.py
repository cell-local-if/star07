"""Leaky-bucket limiter tests: deterministic because time is injected.

The leaky bucket is an independent subsystem: same lock, clock and high-water mark as the token
buckets and windows, but its own namespace and no ledger, revision or decision-counter contact.
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


class LeakyBucketUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.limiter = Limiter(self.clock)

    def create(self, key: str = "b", capacity: int = 10, rate: float = 2.0) -> dict:
        return self.limiter.configure_leaky_bucket(
            key, {"capacity": capacity, "leak_per_second": rate})

    def test_first_configuration_is_born_empty_with_four_fields(self) -> None:
        body = self.create("b", 10, 2)
        self.assertEqual(body, {"key": "b", "level": 0, "capacity": 10, "leak_per_second": 2.0})
        self.assertEqual(self.limiter.leaky_bucket_state("b"),
                         {"key": "b", "level": 0, "capacity": 10, "leak_per_second": 2.0})

    def test_pour_admits_up_to_capacity_and_reports_post_pour_level(self) -> None:
        self.create("b", 5, 1)
        self.assertEqual(self.limiter.leaky_bucket_check("b", 4),
                         {"allowed": True, "cost": 4, "level": 4, "capacity": 5})
        # Exactly filling the bucket is still admissible (<=).
        self.assertEqual(self.limiter.leaky_bucket_check("b")["level"], 5)
        with self.assertRaises(OverQuota) as raised:
            self.limiter.leaky_bucket_check("b", 1)
        self.assertAlmostEqual(raised.exception.retry_after, 1.0, places=6)
        # The rejection poured nothing: the level is still exactly capacity.
        self.assertEqual(self.limiter.leaky_bucket_state("b")["level"], 5)

    def test_level_leaks_at_the_current_rate_before_judging(self) -> None:
        self.create("b", 10, 2)
        self.limiter.leaky_bucket_check("b", 10)                    # level 10 at 1000
        self.clock.t = 1001.0
        result = self.limiter.leaky_bucket_check("b", 2)            # leaks 2 -> 8, then +2
        self.assertEqual((result["allowed"], result["level"]), (True, 10))
        self.clock.t = 1005.0
        result = self.limiter.leaky_bucket_check("b", 1)            # leaks since 1001: 4*2 = 8
        self.assertEqual(result["level"], 3)                        # 10-8, then +1

    def test_level_is_rounded_to_three_decimals(self) -> None:
        self.create("b", 100, 6)
        self.limiter.leaky_bucket_check("b", 5)                     # level 5 at 1000
        self.clock.t = 1000.001
        body = self.limiter.leaky_bucket_state("b")
        self.assertEqual(body["level"], 4.994)                      # 5 - 0.001*6

    def test_rejection_hint_is_the_gap_drained_at_the_current_rate(self) -> None:
        self.create("b", 5, 2)
        self.limiter.leaky_bucket_check("b", 4)                     # level 4
        with self.assertRaises(OverQuota) as raised:
            self.limiter.leaky_bucket_check("b", 2)                 # 4+2-5 = 1 gap
        self.assertAlmostEqual(raised.exception.retry_after, 0.5, places=6)
        self.assertEqual(self.limiter.leaky_bucket_state("b")["level"], 4)

    def test_reput_drains_at_old_rate_first_floored_at_zero(self) -> None:
        self.create("b", 10, 2)
        self.limiter.leaky_bucket_check("b", 10)                    # level 10 at 1000
        self.clock.t = 1002.0                                       # 2s * 2/s = 4 drains
        body = self.limiter.configure_leaky_bucket(
            "b", {"capacity": 100, "leak_per_second": 10})
        self.assertEqual(body["level"], 6)                          # old-rate drain, not 0 or -10
        self.clock.t = 1003.0                                       # one second at the NEW rate
        self.assertEqual(self.limiter.leaky_bucket_state("b")["level"], 0)  # 6-10 floored

    def test_new_rate_never_acts_backwards_on_the_old_wait(self) -> None:
        self.create("b", 10, 1)
        self.limiter.leaky_bucket_check("b", 10)
        self.clock.t = 1002.0                                       # 2 seconds at 1/s
        body = self.limiter.configure_leaky_bucket(
            "b", {"capacity": 100, "leak_per_second": 100})
        # Had the new rate applied retroactively the bucket would already be empty; it is not.
        self.assertEqual(body["level"], 8)

    def test_long_wait_floors_at_zero_even_within_reput(self) -> None:
        self.create("b", 10, 1)
        self.limiter.leaky_bucket_check("b", 10)
        self.clock.t = 2000.0
        body = self.limiter.configure_leaky_bucket(
            "b", {"capacity": 10, "leak_per_second": 1})
        self.assertEqual(body["level"], 0)

    def test_reput_caps_surviving_water_at_the_new_capacity(self) -> None:
        self.create("b", 10, 1)
        self.limiter.leaky_bucket_check("b", 10)                    # level 10 at 1000
        body = self.limiter.configure_leaky_bucket(
            "b", {"capacity": 3, "leak_per_second": 1})             # same moment: no drain
        self.assertEqual(body["level"], 3)
        self.assertEqual(self.limiter.leaky_bucket_state("b")["capacity"], 3)
        with self.assertRaises(OverQuota) as raised:
            self.limiter.leaky_bucket_check("b", 1)                 # 3+1 > 3
        self.assertAlmostEqual(raised.exception.retry_after, 1.0)

    def test_stalled_clock_leaks_nothing(self) -> None:
        self.create("b", 10, 2)
        self.limiter.leaky_bucket_check("b", 5)
        for _ in range(3):
            self.assertEqual(self.limiter.leaky_bucket_state("b")["level"], 5)
        with self.assertRaises(OverQuota) as raised:
            self.limiter.leaky_bucket_check("b", 6)                 # 5+6 > 10, gap 1
        self.assertAlmostEqual(raised.exception.retry_after, 0.5)

    def test_regression_holds_the_level_and_recovery_never_recounts(self) -> None:
        self.create("b", 10, 1)
        self.limiter.leaky_bucket_check("b", 5)                     # level 5 at 1000
        self.clock.t = 900.0                                        # clock jumps backwards
        self.assertEqual(self.limiter.leaky_bucket_state("b")["level"], 5)
        self.limiter.leaky_bucket_check("b", 5)                     # fills to 10 at the watermark
        with self.assertRaises(OverQuota):
            self.limiter.leaky_bucket_check("b", 1)
        self.clock.t = 1002.0                                       # recovered: 1000..1002 only
        self.assertEqual(self.limiter.leaky_bucket_state("b")["level"], 8)

    def test_invalid_configuration_is_rejected_and_creates_nothing(self) -> None:
        bad_payloads = [
            {"capacity": 0, "leak_per_second": 1},
            {"capacity": 1_000_001, "leak_per_second": 1},
            {"capacity": 1.5, "leak_per_second": 1},
            {"capacity": True, "leak_per_second": 1},
            {"capacity": "10", "leak_per_second": 1},
            {"capacity": None, "leak_per_second": 1},
            {"capacity": 10},
            {"capacity": 10, "leak_per_second": 0},
            {"capacity": 10, "leak_per_second": -1},
            {"capacity": 10, "leak_per_second": 1_000_001},
            {"capacity": 10, "leak_per_second": True},
            {"capacity": 10, "leak_per_second": False},
            {"capacity": 10, "leak_per_second": "1"},
            {"capacity": 10, "leak_per_second": None},
            {"leak_per_second": 1},
            {"capacity": 10, "leak_per_second": 1, "extra": 1},
            {}, "nope", [], None, 5,
        ]
        for bad in bad_payloads:
            with self.assertRaises(InvalidRequest, msg=repr(bad)):
                self.limiter.configure_leaky_bucket("fresh", bad)
        with self.assertRaises(LimitNotFound):
            self.limiter.leaky_bucket_state("fresh")                # nothing was created
        for bad_key in [1, True, None, "", "x" * 201, ["b"]]:
            with self.assertRaises(InvalidRequest):
                self.limiter.configure_leaky_bucket(
                    bad_key, {"capacity": 10, "leak_per_second": 1})

    def test_invalid_cost_is_rejected_before_the_lock(self) -> None:
        self.create("b", 10, 1)
        for bad_cost in [0, -1, 1_000_001, True, False, 1.0, "1", None, [], {}]:
            with self.assertRaises(InvalidRequest, msg=repr(bad_cost)):
                self.limiter.leaky_bucket_check("b", bad_cost)
        self.assertEqual(self.limiter.leaky_bucket_state("b")["level"], 0)
        with self.assertRaises(InvalidRequest):
            self.limiter.leaky_bucket_check("", 1)

    def test_unknown_bucket_is_404(self) -> None:
        with self.assertRaises(LimitNotFound):
            self.limiter.leaky_bucket_state("ghost")
        with self.assertRaises(LimitNotFound):
            self.limiter.leaky_bucket_check("ghost", 1)

    def test_validation_failures_never_advance_the_watermark(self) -> None:
        self.create("a", 10, 1)
        self.limiter.leaky_bucket_check("a", 5)                     # level 5 at 1000
        self.clock.t = 5000.0                                       # far future reading
        for bad in [{"capacity": 0, "leak_per_second": 1}, {}, "x"]:
            with self.assertRaises(InvalidRequest):
                self.limiter.configure_leaky_bucket("a", bad)
        with self.assertRaises(InvalidRequest):
            self.limiter.leaky_bucket_check("a", True)
        with self.assertRaises(InvalidRequest):                      # bad key never reaches the lock
            self.limiter.leaky_bucket_check("", 1)
        self.clock.t = 1000.0                                       # back to the real moment
        # If any rejected 400 had ticked, the watermark 5000 would have leaked all five away.
        self.assertEqual(self.limiter.leaky_bucket_state("a")["level"], 5)

    def test_isolated_from_token_buckets_windows_ledger_revision_and_metrics(self) -> None:
        self.limiter.configure("shared", {"capacity": 2, "refill_per_second": 1.0})
        self.limiter.configure_window("shared", {"window_seconds": 10, "max_events": 1})
        self.create("shared", 10, 2)
        self.assertTrue(self.limiter.check("shared", 2)["allowed"])
        self.assertTrue(self.limiter.window_check("shared")["allowed"])
        self.assertEqual(self.limiter.leaky_bucket_check("shared", 8)["level"], 8)
        # Each subsystem keeps its own occupancy.
        token_state = self.limiter.state("shared")
        self.assertEqual((token_state["remaining"], token_state["used"]), (0, 2))
        self.assertEqual(self.limiter.window_state("shared")["used"], 1)
        self.assertEqual(self.limiter.leaky_bucket_state("shared")["level"], 8)
        # The ledger sees only the token-bucket spend.
        self.assertEqual(self.limiter.ledger("shared", 1000)["totals"],
                         {"accepted_count": 1, "accepted_cost": 2})
        # Leaky traffic adds no sixth decision kind and moves none of the five.
        decisions = self.limiter.metrics()["metrics"]["decisions"]
        self.assertEqual(set(decisions),
                         {"check", "hierarchy_check", "reservation",
                          "hierarchy_reservation", "window_check"})
        self.assertEqual(decisions["check"], {"allowed": 1, "over_quota": 0})
        self.assertEqual(decisions["window_check"], {"allowed": 1, "over_quota": 0})
        # Namespaces do not alias in either direction.
        self.create("only-leaky", 1, 1)
        with self.assertRaises(LimitNotFound):
            self.limiter.state("only-leaky")
        with self.assertRaises(LimitNotFound):
            self.limiter.window_state("only-leaky")
        self.limiter.configure("only-bucket", {"capacity": 1, "refill_per_second": 1.0})
        with self.assertRaises(LimitNotFound):
            self.limiter.leaky_bucket_state("only-bucket")
        # No ETag/revision machinery exists for leaky buckets.
        self.assertEqual(self.limiter._revisions.get("only-leaky"), None)

    def test_concurrent_pours_never_exceed_capacity_and_rejects_add_nothing(self) -> None:
        limiter = Limiter(self.clock)                               # frozen clock
        limiter.configure_leaky_bucket("hot", {"capacity": 100, "leak_per_second": 1})
        outcomes: list[bool] = []
        outcomes_lock = threading.Lock()

        def attempt() -> None:
            try:
                limiter.leaky_bucket_check("hot", 1)
                ok = True
            except OverQuota:
                ok = False
            with outcomes_lock:
                outcomes.append(ok)

        threads = [threading.Thread(target=attempt) for _ in range(400)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(sum(outcomes), 100)
        bucket = limiter._leaky_buckets["hot"]
        self.assertEqual(bucket.level, 100)
        self.assertLessEqual(bucket.level, bucket.config.capacity)

    def test_concurrent_pours_and_reconfigures_keep_level_within_capacity(self) -> None:
        limiter = Limiter(self.clock)                               # frozen clock
        limiter.configure_leaky_bucket("hot", {"capacity": 100, "leak_per_second": 1})
        errors: list[BaseException] = []

        def pour() -> None:
            try:
                limiter.leaky_bucket_check("hot", 1)
            except OverQuota:
                pass
            except BaseException as error:  # noqa: BLE001 - surface thread failures
                errors.append(error)

        def shrink() -> None:
            try:
                limiter.configure_leaky_bucket("hot", {"capacity": 50, "leak_per_second": 1})
                limiter.configure_leaky_bucket("hot", {"capacity": 100, "leak_per_second": 1})
            except BaseException as error:  # noqa: BLE001
                errors.append(error)

        threads = [threading.Thread(target=pour) for _ in range(300)]
        threads += [threading.Thread(target=shrink) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        bucket = limiter._leaky_buckets["hot"]
        self.assertGreaterEqual(bucket.level, 0)
        self.assertLessEqual(bucket.level, bucket.config.capacity)


class LeakyBucketHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        import http.client

        cls.http_client = http.client
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

    def raw_request(self, method: str, path: str, payload: bytes | None,
                    content_length: str | object = "auto") -> tuple[int, dict]:
        connection = self.http_client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        connection.putrequest(method, path)
        if content_length != "omit":
            connection.putheader("Content-Length",
                                 str(len(payload)) if content_length == "auto" else content_length)
        connection.endheaders(payload if payload is not None else b"")
        response = connection.getresponse()
        body = json.loads(response.read() or b"{}")
        connection.close()
        return response.status, body

    def test_configure_check_and_get_lifecycle(self) -> None:
        clock = type(self).clock
        clock.t = 4000.0
        status, body, headers = self.request(
            "PUT", "/v1/leaky-buckets/h-1", {"capacity": 5, "leak_per_second": 1})
        self.assertEqual(status, 200)
        self.assertEqual(body, {"key": "h-1", "level": 0, "capacity": 5, "leak_per_second": 1.0})
        self.assertNotIn("ETag", headers)                            # no revision protocol here
        status, body, _ = self.request("POST", "/v1/leaky-buckets/h-1/check", {})
        self.assertEqual((status, body),
                         (200, {"allowed": True, "cost": 1, "level": 1.0, "capacity": 5}))
        status, body, _ = self.request("POST", "/v1/leaky-buckets/h-1/check", {"cost": 4})
        self.assertEqual((status, body["level"]), (200, 5.0))
        status, body, headers = self.request("POST", "/v1/leaky-buckets/h-1/check", {})
        self.assertEqual((status, body["error"]["code"]), (429, "over_quota"))
        self.assertEqual(headers["Retry-After"], "1.000")
        clock.t = 4000.5
        status, body, _ = self.request("GET", "/v1/leaky-buckets/h-1")
        self.assertEqual(status, 200)
        self.assertEqual(body, {"key": "h-1", "level": 4.5, "capacity": 5, "leak_per_second": 1.0})

    def test_sub_millisecond_wait_ceilings_to_001(self) -> None:
        clock = type(self).clock
        # All HTTP tests share one clock and one high-water mark, so anchor strictly later than
        # any earlier test's moments.
        clock.t = 5200.0
        self.request("PUT", "/v1/leaky-buckets/h-2", {"capacity": 2, "leak_per_second": 1})
        self.request("POST", "/v1/leaky-buckets/h-2/check", {"cost": 2})
        clock.t = 5200.9999                                          # 0.9999 leaks away
        status, _, headers = self.request("POST", "/v1/leaky-buckets/h-2/check", {})
        self.assertEqual(status, 429)
        self.assertEqual(headers["Retry-After"], "0.001")

    def test_hot_reput_drains_at_old_rate_then_caps_at_new_capacity(self) -> None:
        clock = type(self).clock
        clock.t = 4300.0
        self.request("PUT", "/v1/leaky-buckets/h-r", {"capacity": 10, "leak_per_second": 2})
        self.request("POST", "/v1/leaky-buckets/h-r/check", {"cost": 10})
        clock.t = 4302.0                                            # drains 4 at 2/s
        status, body, _ = self.request(
            "PUT", "/v1/leaky-buckets/h-r", {"capacity": 6, "leak_per_second": 10})
        self.assertEqual(status, 200)
        self.assertEqual(body["level"], 6)                          # 6 surviving, capped to 6
        clock.t = 4302.5
        _, body, _ = self.request("GET", "/v1/leaky-buckets/h-r")
        self.assertEqual(body["level"], 1.0)                        # 0.5s at the NEW rate: 6-5

    def test_invalid_configuration_is_400_and_creates_nothing(self) -> None:
        for bad in [{"capacity": 0, "leak_per_second": 1},
                    {"capacity": 10, "leak_per_second": True},
                    {"capacity": 10, "leak_per_second": 0},
                    {"capacity": 10},
                    {"capacity": 10, "leak_per_second": 1, "x": 1},
                    {}, "nope", []]:
            status, body, _ = self.request("PUT", "/v1/leaky-buckets/h-bad", bad)
            self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"), bad)
        self.assertEqual(self.request("GET", "/v1/leaky-buckets/h-bad")[0], 404)
        status, body, _ = self.request("PUT", "/v1/leaky-buckets/" + "k" * 201,
                                       {"capacity": 10, "leak_per_second": 1})
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))

    def test_check_body_rules(self) -> None:
        self.request("PUT", "/v1/leaky-buckets/h-3", {"capacity": 3, "leak_per_second": 1})
        path = "/v1/leaky-buckets/h-3/check"
        for payload in (b'{"cost": 0}', b'{"cost": true}', b'{"cost": 1.0}',
                        b'{"cost": "1"}', b'{"x": 1}', b'{"cost": 1, "x": 2}',
                        b"[]", b"null", b"5", b'"x"'):
            status, parsed = self.raw_request("POST", path, payload)
            self.assertEqual((status, parsed["error"]["code"]), (400, "invalid_request"), payload)
        status, parsed = self.raw_request("POST", path, b"")
        self.assertEqual((status, parsed["error"]["code"]), (400, "invalid_request"))
        status, parsed = self.raw_request("POST", path, b'{bad json')
        self.assertEqual((status, parsed["error"]["code"]), (400, "invalid_request"))
        status, parsed = self.raw_request("POST", path, b"{}", content_length="omit")
        self.assertEqual((status, parsed["error"]["code"]), (400, "invalid_request"))
        # Nothing was poured by the rejected requests.
        _, state, _ = self.request("GET", "/v1/leaky-buckets/h-3")
        self.assertEqual(state["level"], 0)

    def test_unknown_bucket_is_404(self) -> None:
        self.assertEqual(self.request("GET", "/v1/leaky-buckets/ghost")[0], 404)
        status, body, _ = self.request("POST", "/v1/leaky-buckets/ghost/check", {})
        self.assertEqual((status, body["error"]["code"]), (404, "not_found"))

    def test_query_parameters_are_400_on_all_three_routes(self) -> None:
        self.request("PUT", "/v1/leaky-buckets/h-4", {"capacity": 1, "leak_per_second": 1})
        status, body, _ = self.request(
            "PUT", "/v1/leaky-buckets/h-4?x=1", {"capacity": 2, "leak_per_second": 1})
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        status, body, _ = self.request("POST", "/v1/leaky-buckets/h-4/check?x=1", {})
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        status, body, _ = self.request("GET", "/v1/leaky-buckets/h-4?x=1")
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        # The rejected PUT changed neither level nor configuration.
        _, state, _ = self.request("GET", "/v1/leaky-buckets/h-4")
        self.assertEqual((state["level"], state["capacity"]), (0, 1))
        # Query validation beats the key's 404.
        self.assertEqual(self.request("GET", "/v1/leaky-buckets/ghost?x=1")[0], 400)
        status, _, _ = self.request("POST", "/v1/leaky-buckets/ghost/check?x=1", {})
        self.assertEqual(status, 400)

    def test_route_and_method_mismatches_are_404_before_the_body_is_read(self) -> None:
        for method, path, payload in [
            ("POST", "/v1/leaky-buckets/h-5", b'{}'),               # missing /check
            ("POST", "/v1/leaky-buckets", b'{}'),                   # missing key
            ("POST", "/v1/leaky-buckets/h-5/check/extra", b'{}'),
            ("GET", "/v1/leaky-buckets/h-5/check", None),
            ("PUT", "/v1/leaky-buckets/h-5/check", b'{}'),
            ("DELETE", "/v1/leaky-buckets/h-5", None),
            ("PATCH", "/v1/leaky-buckets/h-5", b'{}'),
            ("POST", "//v1/leaky-buckets/h-5/check", b'{}'),
            ("POST", "/v1/leaky-buckets//check", b'{}'),
        ]:
            status, parsed = self.raw_request(
                method, path, payload,
                content_length="auto" if payload is not None else "omit")
            self.assertEqual((status, parsed["error"]["code"]), (404, "not_found"), (method, path))
        # Garbage bodies are never inspected off-route.
        status, parsed = self.raw_request("POST", "/v1/nope", b"\xff not json",
                                          content_length="nine")
        self.assertEqual(status, 404)

    def test_namespaces_stay_isolated_via_http(self) -> None:
        self.request("PUT", "/v1/leaky-buckets/h-6", {"capacity": 1, "leak_per_second": 1})
        self.assertEqual(self.request("GET", "/v1/limits/h-6")[0], 404)
        self.assertEqual(self.request("GET", "/v1/windows/h-6")[0], 404)
        self.request("PUT", "/v1/limits/h-7", {"capacity": 1, "refill_per_second": 1})
        self.assertEqual(self.request("GET", "/v1/leaky-buckets/h-7")[0], 404)
        # Leaky pours leave no ledger trace and never move the five metric counters' shape.
        self.request("POST", "/v1/leaky-buckets/h-6/check", {})
        self.assertEqual(self.request("GET", "/v1/ledgers/h-6")[0], 404)
        status, metrics, _ = self.request("GET", "/v1/metrics")
        self.assertEqual(status, 200)
        self.assertEqual(set(metrics["metrics"]["decisions"]),
                         {"check", "hierarchy_check", "reservation",
                          "hierarchy_reservation", "window_check"})


if __name__ == "__main__":
    unittest.main()
