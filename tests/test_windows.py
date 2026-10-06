"""Sliding-window limiter tests: deterministic because time is injected."""
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


class WindowUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.limiter = Limiter(self.clock)
        self.limiter.configure_window("w", {"window_seconds": 10, "max_events": 3})

    def test_admission_shape_includes_this_event(self) -> None:
        first = self.limiter.window_check("w")
        self.assertEqual(first, {"allowed": True, "used": 1, "remaining": 2,
                                "limit": 3, "window_seconds": 10})
        second = self.limiter.window_check("w")
        self.assertEqual((second["used"], second["remaining"]), (2, 1))
        third = self.limiter.window_check("w")
        self.assertEqual((third["used"], third["remaining"]), (3, 0))

    def test_full_window_rejects_with_leave_wait(self) -> None:
        for _ in range(3):
            self.assertTrue(self.limiter.window_check("w")["allowed"])
        with self.assertRaises(OverQuota) as raised:
            self.limiter.window_check("w")                       # effective moment 1000
        self.assertAlmostEqual(raised.exception.retry_after, 10.0, places=6)
        # The rejection booked nothing.
        self.assertEqual(self.limiter.window_state("w")["used"], 3)

    def test_get_snapshot_matches_a_check_at_the_same_moment(self) -> None:
        self.limiter.window_check("w")
        self.assertEqual(self.limiter.window_state("w"),
                         {"window": {"window_seconds": 10, "max_events": 3},
                          "used": 1, "remaining": 2})

    def test_boundary_event_is_evicted_first_then_admission_wins(self) -> None:
        self.assertTrue(self.limiter.window_check("w")["allowed"])   # event at 1000
        self.clock.t = 1009.0
        self.assertEqual(self.limiter.window_state("w")["used"], 1)  # cutoff 999: still live
        self.clock.t = 1010.0
        # cutoff is exactly 1000: the boundary-equal old event settles before judging.
        self.assertEqual(self.limiter.window_state("w")["used"], 0)
        result = self.limiter.window_check("w")
        self.assertTrue(result["allowed"])
        self.assertEqual(result["used"], 1)

    def test_events_leave_in_admission_order(self) -> None:
        self.clock.t = 1000.0
        self.limiter.window_check("w")
        self.clock.t = 1002.0
        self.limiter.window_check("w")
        self.clock.t = 1003.0
        self.limiter.window_check("w")                             # full
        with self.assertRaises(OverQuota):
            self.limiter.window_check("w")
        self.clock.t = 1010.0                                      # first event leaves
        result = self.limiter.window_check("w")
        self.assertTrue(result["allowed"])
        self.assertEqual(result["used"], 3)
        with self.assertRaises(OverQuota) as raised:
            self.limiter.window_check("w")                         # 1002 event is now earliest
        self.assertAlmostEqual(raised.exception.retry_after, 2.0, places=6)

    def test_retry_after_raw_wait_can_be_sub_millisecond(self) -> None:
        self.limiter.configure_window("fast", {"window_seconds": 10, "max_events": 1})
        self.limiter.window_check("fast")                          # event at 1000
        self.clock.t = 1009.9999
        with self.assertRaises(OverQuota) as raised:
            self.limiter.window_check("fast")
        self.assertAlmostEqual(raised.exception.retry_after, 0.0001, places=9)

    def test_stalled_clock_evicts_nothing_and_keeps_the_hint_stable(self) -> None:
        for _ in range(3):
            self.limiter.window_check("w")
        for _ in range(3):
            self.assertEqual(self.limiter.window_state("w")["used"], 3)
            with self.assertRaises(OverQuota) as raised:
                self.limiter.window_check("w")
            self.assertAlmostEqual(raised.exception.retry_after, 10.0, places=6)

    def test_regression_never_evicts_early_nor_recounts_on_recovery(self) -> None:
        self.limiter.configure_window("tight", {"window_seconds": 10, "max_events": 1})
        self.limiter.window_check("tight")                          # event at 1000
        self.clock.t = 900.0                                        # clock jumps backwards
        self.assertEqual(self.limiter.window_state("tight")["used"], 1)  # stays at the watermark
        with self.assertRaises(OverQuota):
            self.limiter.window_check("tight")
        self.clock.t = 1005.0                                       # recovered; 1000 still live
        self.assertEqual(self.limiter.window_state("tight")["used"], 1)
        self.clock.t = 1009.0
        self.assertEqual(self.limiter.window_state("tight")["used"], 1)
        self.clock.t = 1010.0                                       # effective expiry moment
        self.assertEqual(self.limiter.window_state("tight")["used"], 0)
        self.assertEqual(self.limiter.window_state("tight")["used"], 0)  # same moment: stays empty

    def test_reconfigure_keeps_history_and_takes_effect_in_the_response(self) -> None:
        for _ in range(3):
            self.limiter.window_check("w")                         # 3 events at 1000
        config = self.limiter.configure_window("w", {"window_seconds": 10, "max_events": 5})
        self.assertEqual(config.as_json(), {"window_seconds": 10, "max_events": 5})
        state = self.limiter.window_state("w")
        self.assertEqual((state["used"], state["remaining"]), (3, 2))  # history kept, room added
        self.assertTrue(self.limiter.window_check("w")["allowed"])
        self.assertTrue(self.limiter.window_check("w")["allowed"])
        with self.assertRaises(OverQuota):
            self.limiter.window_check("w")                         # 5/5 now

    def test_shrinking_window_evicts_out_of_window_events_immediately(self) -> None:
        self.limiter.window_check("w")                             # event at 1000
        self.clock.t = 1006.0
        self.limiter.configure_window("w", {"window_seconds": 5, "max_events": 3})
        # New cutoff is 1006-5=1001 and the 1000 event is beyond it.
        self.assertEqual(self.limiter.window_state("w")["used"], 0)
        self.assertTrue(self.limiter.window_check("w")["allowed"])

    def test_lowering_max_events_does_not_revoke_past_admissions(self) -> None:
        for _ in range(3):
            self.limiter.window_check("w")                         # 3 admitted at 1000
        self.limiter.configure_window("w", {"window_seconds": 10, "max_events": 2})
        state = self.limiter.window_state("w")
        self.assertEqual(state["used"], 3)                         # nothing revoked
        self.assertEqual(state["remaining"], -1)                   # max_events - used, exact
        self.assertEqual(state["window"]["max_events"], 2)
        with self.assertRaises(OverQuota):
            self.limiter.window_check("w")                         # only later checks rejected
        self.clock.t = 1010.0                                      # the old events leave together
        state = self.limiter.window_state("w")
        self.assertEqual((state["used"], state["remaining"]), (0, 2))
        self.assertTrue(self.limiter.window_check("w")["allowed"])

    def test_invalid_configuration_is_rejected_and_creates_no_window(self) -> None:
        bad_payloads = [
            {"window_seconds": 0, "max_events": 1},
            {"window_seconds": 3601, "max_events": 1},
            {"window_seconds": 1.5, "max_events": 1},
            {"window_seconds": True, "max_events": 1},
            {"window_seconds": "10", "max_events": 1},
            {"window_seconds": None, "max_events": 1},
            {"window_seconds": 10},
            {"window_seconds": 10, "max_events": 0},
            {"window_seconds": 10, "max_events": 1_000_001},
            {"window_seconds": 10, "max_events": 1.0},
            {"window_seconds": 10, "max_events": False},
            {"window_seconds": 10, "max_events": "3"},
            {"max_events": 1},
            {"window_seconds": 10, "max_events": 3, "extra": 1},
            "nope", [], None, 5,
        ]
        for bad in bad_payloads:
            with self.assertRaises(InvalidRequest, msg=repr(bad)):
                self.limiter.configure_window("fresh", bad)
        with self.assertRaises(LimitNotFound):
            self.limiter.window_state("fresh")                     # nothing was created
        for bad_key in [1, True, None, "", "x" * 201, ["w"]]:
            with self.assertRaises(InvalidRequest):
                self.limiter.configure_window(bad_key, {"window_seconds": 10, "max_events": 1})

    def test_unknown_window_is_not_found_but_invalid_key_is_invalid_request(self) -> None:
        with self.assertRaises(LimitNotFound):
            self.limiter.window_state("ghost")
        with self.assertRaises(LimitNotFound):
            self.limiter.window_check("ghost")
        with self.assertRaises(InvalidRequest):
            self.limiter.window_state("")

    def test_window_and_same_named_bucket_are_fully_isolated(self) -> None:
        self.limiter.configure("shared", {"capacity": 2, "refill_per_second": 1.0})
        self.limiter.configure_window("shared", {"window_seconds": 10, "max_events": 1})
        self.assertTrue(self.limiter.check("shared", 2)["allowed"])
        self.assertTrue(self.limiter.window_check("shared")["allowed"])
        with self.assertRaises(OverQuota):                         # window full independently
            self.limiter.window_check("shared")
        # Neither subsystem's counters nor the ledger carry the other's traffic.
        self.assertEqual(self.limiter.state("shared")["used"], 2)
        self.assertEqual(self.limiter.window_state("shared")["used"], 1)
        ledger = self.limiter.ledger("shared", 1000)
        self.assertEqual(ledger["totals"], {"accepted_count": 1, "accepted_cost": 2})
        # A window-only key is still unknown to the bucket side and vice versa.
        self.limiter.configure_window("only-window", {"window_seconds": 10, "max_events": 1})
        with self.assertRaises(LimitNotFound):
            self.limiter.state("only-window")
        self.limiter.configure("only-bucket", {"capacity": 1, "refill_per_second": 1.0})
        with self.assertRaises(LimitNotFound):
            self.limiter.window_state("only-bucket")

    def test_concurrent_checks_never_oversell_or_double_count(self) -> None:
        limiter = Limiter(self.clock)                              # frozen clock for the whole test
        limiter.configure_window("hot", {"window_seconds": 10, "max_events": 100})
        outcomes: list[bool] = []
        outcomes_lock = threading.Lock()

        def attempt() -> None:
            try:
                limiter.window_check("hot")
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
        self.assertEqual(len(limiter._windows["hot"].events), 100)
        self.assertEqual(limiter.window_state("hot")["used"], 100)

    def test_concurrent_checks_and_reconfigures_serialize_safely(self) -> None:
        limiter = Limiter(self.clock)
        limiter.configure_window("hot", {"window_seconds": 10, "max_events": 100})
        errors: list[BaseException] = []

        def check() -> None:
            try:
                limiter.window_check("hot")
            except OverQuota:
                pass
            except BaseException as error:  # noqa: BLE001 - surface thread failures
                errors.append(error)

        def reconfigure() -> None:
            try:
                limiter.configure_window("hot", {"window_seconds": 10, "max_events": 100})
            except BaseException as error:  # noqa: BLE001
                errors.append(error)

        threads = [threading.Thread(target=check) for _ in range(200)]
        threads += [threading.Thread(target=reconfigure) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        admitted = len(limiter._windows["hot"].events)
        self.assertLessEqual(admitted, 100)                        # clock frozen: nothing expired
        self.assertEqual(limiter.window_state("hot")["used"], admitted)


class WindowHttpTests(unittest.TestCase):
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
        status, body, _ = self.request("PUT", "/v1/windows/h-1",
                                       {"window_seconds": 10, "max_events": 2})
        self.assertEqual(status, 200)
        self.assertEqual(body, {"key": "h-1", "window": {"window_seconds": 10, "max_events": 2}})
        status, body, _ = self.request("POST", "/v1/windows/h-1/check", {})
        self.assertEqual((status, body), (200, {"allowed": True, "used": 1, "remaining": 1,
                                               "limit": 2, "window_seconds": 10}))
        self.request("POST", "/v1/windows/h-1/check", {})
        status, body, headers = self.request("POST", "/v1/windows/h-1/check", {})
        self.assertEqual((status, body["error"]["code"]), (429, "over_quota"))
        self.assertEqual(headers["Retry-After"], "10.000")
        status, body, _ = self.request("GET", "/v1/windows/h-1")
        self.assertEqual(status, 200)
        self.assertEqual(body, {"window": {"window_seconds": 10, "max_events": 2},
                                "used": 2, "remaining": 0})
        clock.t = 4010.0                                            # inclusive boundary
        status, body, _ = self.request("POST", "/v1/windows/h-1/check", {})
        self.assertEqual(status, 200)
        self.assertEqual((body["used"], body["remaining"]), (1, 1))

    def test_sub_millisecond_wait_ceilings_to_001(self) -> None:
        clock = type(self).clock
        clock.t = 4100.0
        self.request("PUT", "/v1/windows/h-2", {"window_seconds": 10, "max_events": 1})
        self.assertEqual(self.request("POST", "/v1/windows/h-2/check", {})[0], 200)
        clock.t = 4109.9999
        status, _, headers = self.request("POST", "/v1/windows/h-2/check", {})
        self.assertEqual(status, 429)
        self.assertEqual(headers["Retry-After"], "0.001")

    def test_invalid_configuration_is_400_and_creates_nothing(self) -> None:
        for bad in [{"window_seconds": 0, "max_events": 1},
                    {"window_seconds": 10, "max_events": True},
                    {"window_seconds": 10, "max_events": 1, "x": 1},
                    {"window_seconds": 10}, {}, "nope", []]:
            status, body, _ = self.request("PUT", "/v1/windows/h-bad", bad)
            self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"), bad)
        self.assertEqual(self.request("GET", "/v1/windows/h-bad")[0], 404)
        status, body, _ = self.request("PUT", "/v1/windows/" + "k" * 201,
                                       {"window_seconds": 10, "max_events": 1})
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))

    def test_check_body_must_be_empty_object(self) -> None:
        self.request("PUT", "/v1/windows/h-3", {"window_seconds": 10, "max_events": 3})
        path = "/v1/windows/h-3/check"
        for payload in (b"", b"[]", b"null", b"5", b'"x"', b'{"x": 1}', b'{"allowed": true}'):
            status, parsed = self.raw_request("POST", path, payload)
            self.assertEqual((status, parsed["error"]["code"]), (400, "invalid_request"), payload)
        status, parsed = self.raw_request("POST", path, b'{bad json')
        self.assertEqual((status, parsed["error"]["code"]), (400, "invalid_request"))
        status, parsed = self.raw_request("POST", path, b"{}", content_length="omit")
        self.assertEqual((status, parsed["error"]["code"]), (400, "invalid_request"))
        # Nothing was admitted by the rejected requests.
        _, state, _ = self.request("GET", "/v1/windows/h-3")
        self.assertEqual((state["used"], state["remaining"]), (0, 3))

    def test_unknown_window_is_404(self) -> None:
        self.assertEqual(self.request("GET", "/v1/windows/ghost")[0], 404)
        status, body, _ = self.request("POST", "/v1/windows/ghost/check", {})
        self.assertEqual((status, body["error"]["code"]), (404, "not_found"))

    def test_route_and_method_mismatches_are_404_before_the_body_is_read(self) -> None:
        for method, path, payload in [
            ("POST", "/v1/windows/h-4", b'{}'),                     # missing /check
            ("POST", "/v1/windows", b'{}'),                        # missing key
            ("POST", "/v1/windows/h-4/check/extra", b'{}'),
            ("GET", "/v1/windows/h-4/check", None),
            ("PUT", "/v1/windows/h-4/check", b'{}'),
            ("DELETE", "/v1/windows/h-4", None),
            ("PATCH", "/v1/windows/h-4", b'{}'),
            ("POST", "//v1/windows/h-4/check", b'{}'),
            ("POST", "/v1/windows//check", b'{}'),
        ]:
            status, parsed = self.raw_request(
                method, path, payload,
                content_length="auto" if payload is not None else "omit")
            self.assertEqual((status, parsed["error"]["code"]), (404, "not_found"), (method, path))
        # Garbage bodies are never inspected off-route.
        status, parsed = self.raw_request("POST", "/v1/nope", b"\xff not json",
                                          content_length="nine")
        self.assertEqual(status, 404)

    def test_window_and_bucket_namespaces_are_isolated_via_http(self) -> None:
        self.request("PUT", "/v1/windows/h-5", {"window_seconds": 10, "max_events": 1})
        self.assertEqual(self.request("GET", "/v1/limits/h-5")[0], 404)
        self.request("PUT", "/v1/limits/h-6", {"capacity": 1, "refill_per_second": 1})
        self.assertEqual(self.request("GET", "/v1/windows/h-6")[0], 404)
        # Window traffic leaves no ledger trace.
        self.request("POST", "/v1/windows/h-5/check", {})
        self.assertEqual(self.request("GET", "/v1/ledgers/h-5")[0], 404)


if __name__ == "__main__":
    unittest.main()
