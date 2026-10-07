"""Leaky-bucket limiter tests: deterministic because time is injected."""
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


class LeakyUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.limiter = Limiter(self.clock)
        self.limiter.configure_leaky("lb", {"capacity": 10, "leak_per_second": 2.0})

    def test_first_configure_starts_empty(self) -> None:
        state = self.limiter.leaky_state("lb")
        self.assertEqual(state, {"key": "lb", "level": 0.0, "capacity": 10,
                                 "leak_per_second": 2.0})

    def test_check_pours_and_reports_level_after_this_cost(self) -> None:
        result = self.limiter.leaky_check("lb", 4)
        self.assertEqual(result, {"allowed": True, "cost": 4, "level": 4.0, "capacity": 10})
        second = self.limiter.leaky_check("lb", 1)          # default-shaped explicit cost
        self.assertEqual((second["level"], second["cost"]), (5.0, 1))

    def test_bucket_drains_at_the_current_rate(self) -> None:
        self.limiter.leaky_check("lb", 6)                   # level 6 at 1000
        self.clock.t = 1002.0
        state = self.limiter.leaky_state("lb")              # 2 s * 2/s drained
        self.assertEqual(state["level"], 2.0)
        self.clock.t = 1003.0
        self.assertEqual(self.limiter.leaky_state("lb")["level"], 0.0)   # floored at 0

    def test_level_rounds_to_three_decimals(self) -> None:
        self.limiter.configure_leaky("frac", {"capacity": 10, "leak_per_second": 0.3})
        self.limiter.leaky_check("frac", 5)                 # level 5 at 1000
        self.clock.t = 1000.3333
        level = self.limiter.leaky_state("frac")["level"]   # 5 - 0.09999
        self.assertEqual(level, round(5 - 0.3 * 0.3333, 3))
        self.assertEqual(level, 4.9)

    def test_full_bucket_rejects_without_raising_level(self) -> None:
        self.limiter.leaky_check("lb", 8)                   # level 8 at 1000
        with self.assertRaises(OverQuota) as raised:
            self.limiter.leaky_check("lb", 5)               # 8 + 5 > 10
        # deficit = 8 + 5 - 10 = 3, at 2/s -> 1.5 s
        self.assertAlmostEqual(raised.exception.retry_after, 1.5, places=6)
        self.assertEqual(self.limiter.leaky_state("lb")["level"], 8.0)   # nothing poured

    def test_rejection_wait_accounts_for_draining(self) -> None:
        self.limiter.leaky_check("lb", 10)                  # full at 1000
        self.clock.t = 1001.0                               # level 8 now
        with self.assertRaises(OverQuota) as raised:
            self.limiter.leaky_check("lb", 4)               # deficit 8 + 4 - 10 = 2
        self.assertAlmostEqual(raised.exception.retry_after, 1.0, places=6)
        self.clock.t = 1002.0                               # level 6: 6 + 4 fits
        self.assertTrue(self.limiter.leaky_check("lb", 4)["allowed"])

    def test_reconfigure_drains_at_old_rate_then_caps_at_new_capacity(self) -> None:
        self.limiter.leaky_check("lb", 10)                  # full at 1000, leaking 2/s
        self.clock.t = 1002.0
        config = self.limiter.configure_leaky("lb", {"capacity": 5, "leak_per_second": 0.5})
        self.assertEqual(config.as_json(), {"capacity": 5, "leak_per_second": 0.5})
        # Old rate drained 4 (10 -> 6), then the new capacity caps it at 5.
        self.assertEqual(self.limiter.leaky_state("lb")["level"], 5.0)

    def test_new_rate_is_not_retroactive(self) -> None:
        self.limiter.leaky_check("lb", 10)                  # full at 1000
        self.clock.t = 1002.0
        self.limiter.configure_leaky("lb", {"capacity": 10, "leak_per_second": 100.0})
        # The 2 s wait drained at the OLD 2/s, not the new 100/s: level is 6, not 0.
        self.assertEqual(self.limiter.leaky_state("lb")["level"], 6.0)
        self.clock.t = 1002.06                              # 0.06 s at 100/s drains the rest
        self.assertEqual(self.limiter.leaky_state("lb")["level"], 0.0)

    def test_reconfigure_with_larger_capacity_keeps_level(self) -> None:
        self.limiter.leaky_check("lb", 9)
        self.limiter.configure_leaky("lb", {"capacity": 100, "leak_per_second": 2.0})
        self.assertEqual(self.limiter.leaky_state("lb")["level"], 9.0)   # same moment: no drain

    def test_stalled_clock_drains_nothing(self) -> None:
        self.limiter.leaky_check("lb", 7)
        for _ in range(3):
            self.assertEqual(self.limiter.leaky_state("lb")["level"], 7.0)
            with self.assertRaises(OverQuota) as raised:
                self.limiter.leaky_check("lb", 4)
            self.assertAlmostEqual(raised.exception.retry_after, 0.5, places=6)

    def test_regression_holds_level_and_recovery_never_double_counts(self) -> None:
        self.limiter.leaky_check("lb", 8)                   # level 8 at 1000
        self.clock.t = 900.0                                # clock jumps backwards
        self.assertEqual(self.limiter.leaky_state("lb")["level"], 8.0)   # stays at the watermark
        self.clock.t = 1001.0                               # recovered: only 1 real second drains
        self.assertEqual(self.limiter.leaky_state("lb")["level"], 6.0)
        self.clock.t = 950.0                                # regressed again: no further drain
        self.assertEqual(self.limiter.leaky_state("lb")["level"], 6.0)

    def test_invalid_configuration_is_rejected_and_creates_nothing(self) -> None:
        bad_payloads = [
            {"capacity": 0, "leak_per_second": 1},
            {"capacity": 1_000_001, "leak_per_second": 1},
            {"capacity": 1.5, "leak_per_second": 1},
            {"capacity": True, "leak_per_second": 1},
            {"capacity": "10", "leak_per_second": 1},
            {"capacity": None, "leak_per_second": 1},
            {"leak_per_second": 1},
            {"capacity": 10},
            {"capacity": 10, "leak_per_second": 0},
            {"capacity": 10, "leak_per_second": -1},
            {"capacity": 10, "leak_per_second": 1_000_001},
            {"capacity": 10, "leak_per_second": False},
            {"capacity": 10, "leak_per_second": "2"},
            {"capacity": 10, "leak_per_second": None},
            {"capacity": 10, "leak_per_second": 1, "extra": 1},
            "nope", [], None, 5,
        ]
        watermark = self.limiter._watermark
        for bad in bad_payloads:
            with self.assertRaises(InvalidRequest, msg=repr(bad)):
                self.limiter.configure_leaky("fresh", bad)
        with self.assertRaises(LimitNotFound):
            self.limiter.leaky_state("fresh")               # nothing was created
        self.assertEqual(self.limiter._watermark, watermark)   # and the clock was never sampled
        for bad_key in [1, True, None, "", "x" * 201, ["lb"]]:
            with self.assertRaises(InvalidRequest):
                self.limiter.configure_leaky(bad_key, {"capacity": 1, "leak_per_second": 1})

    def test_invalid_cost_is_rejected_without_pouring(self) -> None:
        for bad in [0, -1, 1_000_001, 1.5, True, False, "1", None]:
            with self.assertRaises(InvalidRequest, msg=repr(bad)):
                self.limiter.leaky_check("lb", bad)
        self.assertEqual(self.limiter.leaky_state("lb")["level"], 0.0)

    def test_unknown_bucket_is_not_found_but_invalid_key_is_invalid_request(self) -> None:
        with self.assertRaises(LimitNotFound):
            self.limiter.leaky_state("ghost")
        with self.assertRaises(LimitNotFound):
            self.limiter.leaky_check("ghost", 1)
        with self.assertRaises(InvalidRequest):
            self.limiter.leaky_state("")

    def test_leaky_bucket_and_same_named_bucket_and_window_are_isolated(self) -> None:
        self.limiter.configure("shared", {"capacity": 2, "refill_per_second": 1.0})
        self.limiter.configure_window("shared", {"window_seconds": 10, "max_events": 1})
        self.limiter.configure_leaky("shared", {"capacity": 3, "leak_per_second": 1.0})
        self.assertTrue(self.limiter.check("shared", 2)["allowed"])
        self.assertTrue(self.limiter.window_check("shared")["allowed"])
        self.assertTrue(self.limiter.leaky_check("shared", 3)["allowed"])
        with self.assertRaises(OverQuota):                  # leaky bucket full independently
            self.limiter.leaky_check("shared", 1)
        # Neither the token bucket's used nor its ledger carries leaky traffic.
        self.assertEqual(self.limiter.state("shared")["used"], 2)
        ledger = self.limiter.ledger("shared", 1000)
        self.assertEqual(ledger["totals"], {"accepted_count": 1, "accepted_cost": 2})
        # A leaky-only key is unknown to the other namespaces and vice versa.
        with self.assertRaises(LimitNotFound):
            self.limiter.state("lb")
        with self.assertRaises(LimitNotFound):
            self.limiter.window_state("lb")
        self.limiter.configure("only-bucket", {"capacity": 1, "refill_per_second": 1.0})
        with self.assertRaises(LimitNotFound):
            self.limiter.leaky_state("only-bucket")

    def test_leaky_decisions_never_touch_the_five_metric_kinds(self) -> None:
        self.limiter.leaky_check("lb", 10)
        with self.assertRaises(OverQuota):
            self.limiter.leaky_check("lb", 1)
        self.limiter.configure_leaky("lb", {"capacity": 1, "leak_per_second": 1})
        decisions = self.limiter.metrics()["metrics"]["decisions"]
        self.assertEqual(set(decisions), {"check", "hierarchy_check", "reservation",
                                          "hierarchy_reservation", "window_check"})
        for counts in decisions.values():
            self.assertEqual(counts, {"allowed": 0, "over_quota": 0})

    def test_concurrent_checks_never_exceed_capacity(self) -> None:
        limiter = Limiter(self.clock)                       # frozen clock for the whole test
        limiter.configure_leaky("hot", {"capacity": 100, "leak_per_second": 1.0})
        outcomes: list[bool] = []
        outcomes_lock = threading.Lock()

        def attempt() -> None:
            try:
                limiter.leaky_check("hot", 1)
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
        self.assertEqual(limiter.leaky_state("hot")["level"], 100.0)

    def test_concurrent_checks_and_reconfigures_serialize_safely(self) -> None:
        limiter = Limiter(self.clock)
        limiter.configure_leaky("hot", {"capacity": 100, "leak_per_second": 1.0})
        errors: list[BaseException] = []

        def check() -> None:
            try:
                limiter.leaky_check("hot", 1)
            except OverQuota:
                pass
            except BaseException as error:  # noqa: BLE001 - surface thread failures
                errors.append(error)

        def reconfigure() -> None:
            try:
                limiter.configure_leaky("hot", {"capacity": 100, "leak_per_second": 1.0})
            except BaseException as error:  # noqa: BLE001
                errors.append(error)

        threads = [threading.Thread(target=check) for _ in range(200)]
        threads += [threading.Thread(target=reconfigure) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        level = limiter.leaky_state("hot")["level"]         # clock frozen: nothing drained
        self.assertLessEqual(level, 100.0)


class LeakyHttpTests(unittest.TestCase):
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
        status, body, _ = self.request("PUT", "/v1/leaky-buckets/h-1",
                                       {"capacity": 4, "leak_per_second": 1})
        self.assertEqual(status, 200)
        self.assertEqual(body, {"key": "h-1", "leaky_bucket": {"capacity": 4, "leak_per_second": 1.0}})
        status, body, _ = self.request("POST", "/v1/leaky-buckets/h-1/check", {})
        self.assertEqual((status, body), (200, {"allowed": True, "cost": 1, "level": 1.0,
                                                "capacity": 4}))
        status, body, _ = self.request("POST", "/v1/leaky-buckets/h-1/check", {"cost": 3})
        self.assertEqual((status, body["level"]), (200, 4.0))
        status, body, headers = self.request("POST", "/v1/leaky-buckets/h-1/check", {"cost": 2})
        self.assertEqual((status, body["error"]["code"]), (429, "over_quota"))
        self.assertEqual(headers["Retry-After"], "2.000")   # deficit 4 + 2 - 4 = 2 at 1/s
        status, body, _ = self.request("GET", "/v1/leaky-buckets/h-1")
        self.assertEqual(body, {"key": "h-1", "level": 4.0, "capacity": 4, "leak_per_second": 1.0})
        clock.t = 4002.0                                    # 2 s at 1/s drains 2
        status, body, _ = self.request("POST", "/v1/leaky-buckets/h-1/check", {"cost": 2})
        self.assertEqual((status, body["level"]), (200, 4.0))

    def test_sub_millisecond_wait_ceilings_to_001(self) -> None:
        clock = type(self).clock
        clock.t = 4100.0
        self.request("PUT", "/v1/leaky-buckets/h-2", {"capacity": 1, "leak_per_second": 10000})
        self.assertEqual(self.request("POST", "/v1/leaky-buckets/h-2/check", {})[0], 200)
        status, _, headers = self.request("POST", "/v1/leaky-buckets/h-2/check", {})
        self.assertEqual(status, 429)
        self.assertEqual(headers["Retry-After"], "0.001")   # 1/10000 s ceilings up to 0.001

    def test_invalid_configuration_is_400_and_creates_nothing(self) -> None:
        for bad in [{"capacity": 0, "leak_per_second": 1},
                    {"capacity": 10, "leak_per_second": True},
                    {"capacity": 10, "leak_per_second": 0},
                    {"capacity": 10, "leak_per_second": 1, "x": 1},
                    {"capacity": 10}, {}, "nope", []]:
            status, body, _ = self.request("PUT", "/v1/leaky-buckets/h-bad", bad)
            self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"), bad)
        self.assertEqual(self.request("GET", "/v1/leaky-buckets/h-bad")[0], 404)
        status, body, _ = self.request("PUT", "/v1/leaky-buckets/" + "k" * 201,
                                       {"capacity": 1, "leak_per_second": 1})
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))

    def test_check_body_accepts_only_an_optional_cost(self) -> None:
        self.request("PUT", "/v1/leaky-buckets/h-3", {"capacity": 10, "leak_per_second": 1})
        path = "/v1/leaky-buckets/h-3/check"
        for payload in (b"", b"[]", b"null", b"5", b'"x"', b'{"x": 1}', b'{"cost": 1, "y": 2}',
                        b'{"cost": 0}', b'{"cost": 1.5}', b'{"cost": true}', b'{"cost": "1"}'):
            status, parsed = self.raw_request("POST", path, payload)
            self.assertEqual((status, parsed["error"]["code"]), (400, "invalid_request"), payload)
        status, parsed = self.raw_request("POST", path, b'{bad json')
        self.assertEqual((status, parsed["error"]["code"]), (400, "invalid_request"))
        status, parsed = self.raw_request("POST", path, b"{}", content_length="omit")
        self.assertEqual((status, parsed["error"]["code"]), (400, "invalid_request"))
        # Nothing was poured by the rejected requests.
        _, state, _ = self.request("GET", "/v1/leaky-buckets/h-3")
        self.assertEqual(state["level"], 0.0)
        # Empty object and explicit cost both pour.
        self.assertEqual(self.request("POST", path, {})[1]["level"], 1.0)
        self.assertEqual(self.request("POST", path, {"cost": 2})[1]["level"], 3.0)

    def test_unknown_bucket_is_404(self) -> None:
        self.assertEqual(self.request("GET", "/v1/leaky-buckets/ghost")[0], 404)
        status, body, _ = self.request("POST", "/v1/leaky-buckets/ghost/check", {})
        self.assertEqual((status, body["error"]["code"]), (404, "not_found"))

    def test_query_parameters_are_400_on_all_three_routes(self) -> None:
        self.request("PUT", "/v1/leaky-buckets/h-4", {"capacity": 2, "leak_per_second": 1})
        status, body, _ = self.request("PUT", "/v1/leaky-buckets/h-4?x=1",
                                       {"capacity": 2, "leak_per_second": 1})
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        status, body, _ = self.request("POST", "/v1/leaky-buckets/h-4/check?x=1", {})
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        status, body, _ = self.request("GET", "/v1/leaky-buckets/h-4?x=1")
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        # The rejected writes poured and changed nothing.
        _, state, _ = self.request("GET", "/v1/leaky-buckets/h-4")
        self.assertEqual(state["level"], 0.0)

    def test_route_and_method_mismatches_are_404_before_the_body_is_read(self) -> None:
        for method, path, payload in [
            ("POST", "/v1/leaky-buckets/h-5", b'{}'),                    # missing /check
            ("POST", "/v1/leaky-buckets", b'{}'),                        # missing key
            ("POST", "/v1/leaky-buckets/h-5/check/extra", b'{}'),
            ("GET", "/v1/leaky-buckets/h-5/check", None),
            ("PUT", "/v1/leaky-buckets/h-5/check", b'{"capacity": 1, "leak_per_second": 1}'),
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

    def test_leaky_bucket_leaves_other_surfaces_untouched(self) -> None:
        self.request("PUT", "/v1/leaky-buckets/h-6", {"capacity": 2, "leak_per_second": 1})
        self.assertEqual(self.request("GET", "/v1/limits/h-6")[0], 404)
        self.assertEqual(self.request("GET", "/v1/windows/h-6")[0], 404)
        self.request("POST", "/v1/leaky-buckets/h-6/check", {"cost": 2})
        # Leaky traffic writes no ledger and no revision/ETag surface exists for it.
        self.assertEqual(self.request("GET", "/v1/ledgers/h-6")[0], 404)
        _, _, headers = self.request("GET", "/v1/leaky-buckets/h-6")
        self.assertNotIn("ETag", headers)
        # The metrics shape still carries exactly the five baseline kinds.
        _, metrics, _ = self.request("GET", "/v1/metrics")
        self.assertEqual(set(metrics["metrics"]["decisions"]),
                         {"check", "hierarchy_check", "reservation",
                          "hierarchy_reservation", "window_check"})


if __name__ == "__main__":
    unittest.main()
