"""Window capacity reservations: POST/DELETE/consume under /v1/windows/{key}/reservations.

A window reservation holds `cost` of the window's occupancy budget without forming an event:
it counts toward used/remaining immediately, lapses at created_at + ttl_seconds (boundary
inclusive, lazy, forming no event), rolls back for an immediate release, and consumes into an
ordinary admission stamped at the consume moment (sliding out at + window_seconds). These tests
pin, beyond the token-bucket reservation semantics already covered elsewhere:

* the three success shapes share key/cost/used/remaining/limit/window_seconds; creation adds
  reservation_id and ttl_seconds, rollback adds rolled_back, consume adds consumed;
* live holds are occupancy for later checks and state reads; expiry, rollback and consume move
  used exactly as specified and each exactly once;
* over_quota Retry-After waits for the earliest same-moment release batch over the merged
  timeline of expiring admissions AND live holds, ceiled to whole milliseconds;
* cost above the current max_events is 400 invalid_request before the clock is sampled; unknown
  window, unknown/cross-key/expired holds are 404; repeated consume replays, repeated rollback
  is 404;
* no ledger event, no revision/ETag change, no metrics count, and reconfiguration never extends
  a hold's TTL nor revokes occupancy when max_events drops.
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


class WindowReservationUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.limiter = Limiter(self.clock)
        self.limiter.configure_window("w", {"window_seconds": 10, "max_events": 5})

    def test_create_defaults_and_shape(self) -> None:
        result = self.limiter.window_reserve("w")
        self.assertEqual(result, {"reservation_id": result["reservation_id"], "key": "w",
                                  "cost": 1, "ttl_seconds": 60, "used": 1, "remaining": 4,
                                  "limit": 5, "window_seconds": 10})
        self.assertIsInstance(result["reservation_id"], str)
        self.assertTrue(result["reservation_id"])

    def test_hold_counts_as_occupancy_immediately(self) -> None:
        self.limiter.window_reserve("w", 3, 30)
        state = self.limiter.window_state("w")
        self.assertEqual((state["used"], state["remaining"]), (3, 2))
        result = self.limiter.window_check("w", 2)
        self.assertEqual((result["used"], result["remaining"]), (5, 0))
        with self.assertRaises(OverQuota):
            self.limiter.window_check("w")

    def test_expiry_releases_at_ttl_boundary_and_forms_no_event(self) -> None:
        self.limiter.window_reserve("w", 2, 30)                       # created at 1000
        self.clock.t = 1029.0
        self.assertEqual(self.limiter.window_state("w")["used"], 2)   # not yet due
        self.clock.t = 1030.0                                         # created_at + ttl exactly
        self.assertEqual(self.limiter.window_state("w")["used"], 0)
        # Nothing slid into the window's event history: the release formed no event.
        self.assertEqual(self.limiter._windows["w"].events, [])

    def test_rollback_releases_immediately_and_only_once(self) -> None:
        created = self.limiter.window_reserve("w", 2, 30)
        result = self.limiter.window_rollback("w", created["reservation_id"])
        self.assertEqual(result, {"key": "w", "cost": 2, "rolled_back": True,
                                  "used": 0, "remaining": 5, "limit": 5, "window_seconds": 10})
        self.assertEqual(self.limiter.window_state("w")["used"], 0)
        with self.assertRaises(LimitNotFound):                        # repeated rollback
            self.limiter.window_rollback("w", created["reservation_id"])

    def test_rollback_after_expiry_is_not_found(self) -> None:
        created = self.limiter.window_reserve("w", 2, 30)
        self.clock.t = 1030.0
        with self.assertRaises(LimitNotFound):
            self.limiter.window_rollback("w", created["reservation_id"])

    def test_consume_converts_to_ordinary_occupancy_at_the_same_moment(self) -> None:
        created = self.limiter.window_reserve("w", 2, 30)             # hold at 1000
        self.clock.t = 1005.0
        result = self.limiter.window_consume("w", created["reservation_id"])
        self.assertEqual(result, {"key": "w", "cost": 2, "consumed": True,
                                  "used": 2, "remaining": 3, "limit": 5, "window_seconds": 10})
        # The converted admission slides out at the consume moment + window_seconds, not the
        # creation moment and not the TTL.
        self.clock.t = 1014.0
        self.assertEqual(self.limiter.window_state("w")["used"], 2)
        self.clock.t = 1015.0
        self.assertEqual(self.limiter.window_state("w")["used"], 0)

    def test_consume_replays_without_double_occupancy(self) -> None:
        created = self.limiter.window_reserve("w", 2, 30)
        first = self.limiter.window_consume("w", created["reservation_id"])
        second = self.limiter.window_consume("w", created["reservation_id"])
        self.assertEqual(first, second)
        self.assertEqual(self.limiter.window_state("w")["used"], 2)
        self.assertEqual(len(self.limiter._windows["w"].events), 1)

    def test_consume_after_expiry_is_not_found_and_never_occupies(self) -> None:
        created = self.limiter.window_reserve("w", 2, 30)
        self.clock.t = 1030.0
        with self.assertRaises(LimitNotFound):
            self.limiter.window_consume("w", created["reservation_id"])
        self.assertEqual(self.limiter.window_state("w")["used"], 0)
        self.assertEqual(self.limiter._windows["w"].events, [])

    def test_unknown_window_unknown_and_cross_key_ids_are_not_found(self) -> None:
        created = self.limiter.window_reserve("w", 1, 30)
        self.limiter.configure_window("other", {"window_seconds": 10, "max_events": 5})
        with self.assertRaises(LimitNotFound):                        # unknown window
            self.limiter.window_rollback("ghost", created["reservation_id"])
        with self.assertRaises(LimitNotFound):
            self.limiter.window_consume("ghost", created["reservation_id"])
        with self.assertRaises(LimitNotFound):                        # cross-key hold
            self.limiter.window_rollback("other", created["reservation_id"])
        with self.assertRaises(LimitNotFound):
            self.limiter.window_consume("other", created["reservation_id"])
        with self.assertRaises(LimitNotFound):                        # unknown id
            self.limiter.window_rollback("w", "nope")
        with self.assertRaises(LimitNotFound):
            self.limiter.window_consume("w", "nope")
        # The hold itself is untouched by all of the above.
        self.assertEqual(self.limiter.window_state("w")["used"], 1)

    def test_token_bucket_reservation_ids_do_not_resolve_here(self) -> None:
        self.limiter.configure("w", {"capacity": 5, "refill_per_second": 1.0})
        token_hold = self.limiter.reserve("w", 1)
        with self.assertRaises(LimitNotFound):
            self.limiter.window_rollback("w", token_hold["reservation_id"])
        with self.assertRaises(LimitNotFound):
            self.limiter.window_consume("w", token_hold["reservation_id"])
        window_hold = self.limiter.window_reserve("w", 1)
        with self.assertRaises(LimitNotFound):
            self.limiter.rollback(window_hold["reservation_id"])
        with self.assertRaises(LimitNotFound):
            self.limiter.consume(window_hold["reservation_id"])

    def test_full_window_rejects_creation_with_merged_release_wait(self) -> None:
        self.limiter.window_check("w", 2)                             # event at 1000, leaves 1010
        self.clock.t = 1002.0
        self.limiter.window_reserve("w", 2, 20)                       # hold, releases 1022
        self.clock.t = 1004.0
        self.limiter.window_check("w")                                # event at 1004, leaves 1014
        # used is 5/5; a cost-2 hold first fits once the 1010 batch frees its 2.
        with self.assertRaises(OverQuota) as raised:
            self.limiter.window_reserve("w", 2)
        self.assertAlmostEqual(raised.exception.retry_after, 6.0, places=6)
        # A cost-3 hold needs the 1010 and 1014 releases: 2+1 freed, then used 5-3+3 fits.
        with self.assertRaises(OverQuota) as raised:
            self.limiter.window_reserve("w", 3)
        self.assertAlmostEqual(raised.exception.retry_after, 10.0, places=6)
        # A cost-4 hold also needs the hold's own release at 1022.
        with self.assertRaises(OverQuota) as raised:
            self.limiter.window_reserve("w", 4)
        self.assertAlmostEqual(raised.exception.retry_after, 18.0, places=6)
        # Rejections held nothing.
        self.assertEqual(self.limiter.window_state("w")["used"], 5)

    def test_check_retry_after_also_sees_live_holds(self) -> None:
        self.limiter.window_reserve("w", 5, 20)                       # whole budget held to 1020
        with self.assertRaises(OverQuota) as raised:
            self.limiter.window_check("w")
        self.assertAlmostEqual(raised.exception.retry_after, 20.0, places=6)

    def test_cost_above_max_events_is_invalid_request_before_the_clock(self) -> None:
        before = self.limiter.metrics()
        with self.assertRaises(InvalidRequest) as raised:
            self.limiter.window_reserve("w", 6)
        self.assertIn("exceeds max_events 5", str(raised.exception))
        with self.assertRaises(InvalidRequest):
            self.limiter.window_reserve("w", 6, 0)                    # ttl 0 is invalid regardless
        self.assertEqual(self.limiter.metrics(), before)              # no decision counted
        self.assertEqual(self.limiter._window_reservations, {})
        # cost equal to max_events is legal and fits an empty window.
        result = self.limiter.window_reserve("w", 5)
        self.assertEqual((result["used"], result["remaining"]), (5, 0))

    def test_unknown_window_is_not_found_and_never_created(self) -> None:
        with self.assertRaises(LimitNotFound):
            self.limiter.window_reserve("ghost", 1_000_000)           # no max_events to compare
        with self.assertRaises(LimitNotFound):
            self.limiter.window_state("ghost")
        with self.assertRaises(InvalidRequest):                       # format beats not_found
            self.limiter.window_reserve("ghost", 0)

    def test_invalid_cost_and_ttl_are_rejected(self) -> None:
        for bad_cost in (0, -1, 1.5, True, "2", None, 1_000_001):
            with self.assertRaises(InvalidRequest, msg=repr(bad_cost)):
                self.limiter.window_reserve("w", bad_cost)
        for bad_ttl in (0, -1, 1.5, True, "60", None, 86_401):
            with self.assertRaises(InvalidRequest, msg=repr(bad_ttl)):
                self.limiter.window_reserve("w", 1, bad_ttl)
        self.assertEqual(self.limiter.window_state("w")["used"], 0)

    def test_no_ledger_revision_or_metrics_footprint(self) -> None:
        self.limiter.configure("w", {"capacity": 5, "refill_per_second": 1.0})
        _, revision_before = self.limiter.state_snapshot("w")
        before_metrics = self.limiter.metrics()
        created = self.limiter.window_reserve("w", 2, 30)
        self.limiter.window_consume("w", created["reservation_id"])
        other = self.limiter.window_reserve("w", 1, 30)
        self.limiter.window_rollback("w", other["reservation_id"])
        self.limiter.window_reserve("w", 1, 1)
        self.clock.t = 1001.0                                         # let the last hold lapse
        self.limiter.window_state("w")
        # The same-named token bucket saw nothing: no ledger events, no used, same revision.
        state, revision_after = self.limiter.state_snapshot("w")
        self.assertEqual(revision_after, revision_before)
        self.assertEqual(state["used"], 0)
        self.assertEqual(self.limiter.ledger("w")["totals"],
                         {"accepted_count": 0, "accepted_cost": 0})
        self.assertEqual(self.limiter.metrics(), before_metrics)

    def test_reconfigure_never_extends_ttl_and_never_revokes_holds(self) -> None:
        self.limiter.window_reserve("w", 4, 30)                       # created at 1000
        self.clock.t = 1005.0
        # Shortening the window evicts nothing (no events) and leaves the hold alone.
        self.limiter.configure_window("w", {"window_seconds": 3, "max_events": 2})
        state = self.limiter.window_state("w")
        self.assertEqual((state["used"], state["remaining"]), (4, -2))
        self.clock.t = 1030.0                                         # original TTL still governs
        self.assertEqual(self.limiter.window_state("w")["used"], 0)

    def test_stalled_and_regressed_clock_follow_the_watermark(self) -> None:
        created = self.limiter.window_reserve("w", 2, 10)             # created at 1000
        self.clock.t = 900.0                                          # regression: stays at 1000
        self.assertEqual(self.limiter.window_state("w")["used"], 2)
        self.clock.t = 1005.0                                         # recovered; not yet due
        self.assertEqual(self.limiter.window_state("w")["used"], 2)
        self.clock.t = 1010.0
        with self.assertRaises(LimitNotFound):                        # due exactly at the boundary
            self.limiter.window_consume("w", created["reservation_id"])
        self.assertEqual(self.limiter.window_state("w")["used"], 0)

    def test_concurrent_creation_never_oversells(self) -> None:
        limiter = Limiter(self.clock)                                 # frozen clock
        limiter.configure_window("hot", {"window_seconds": 10, "max_events": 100})
        outcomes: list[bool] = []
        outcomes_lock = threading.Lock()

        def attempt() -> None:
            try:
                limiter.window_reserve("hot", 1, 60)
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
        self.assertEqual(limiter.window_state("hot")["used"], 100)


class WindowReservationHttpTests(unittest.TestCase):
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

    def test_full_lifecycle_over_http(self) -> None:
        clock = type(self).clock
        clock.t = 5000.0
        self.request("PUT", "/v1/windows/hr-1", {"window_seconds": 10, "max_events": 3})
        status, body, headers = self.request("POST", "/v1/windows/hr-1/reservations",
                                             {"cost": 2, "ttl_seconds": 30})
        self.assertEqual(status, 200)
        self.assertEqual(body, {"reservation_id": body["reservation_id"], "key": "hr-1",
                                "cost": 2, "ttl_seconds": 30, "used": 2, "remaining": 1,
                                "limit": 3, "window_seconds": 10})
        self.assertNotIn("ETag", headers)
        reservation_id = body["reservation_id"]
        # The hold is occupancy for the plain check and the state read.
        status, body, _ = self.request("POST", "/v1/windows/hr-1/check", {})
        self.assertEqual((status, body["used"], body["remaining"]), (200, 3, 0))
        status, body, _ = self.request("GET", "/v1/windows/hr-1")
        self.assertEqual((body["used"], body["remaining"]), (3, 0))
        # Consume converts the hold; a repeat replays byte for byte.
        status, first, _ = self.request(
            "POST", f"/v1/windows/hr-1/reservations/{reservation_id}/consume", {})
        self.assertEqual(status, 200)
        self.assertEqual(first, {"key": "hr-1", "cost": 2, "consumed": True,
                                 "used": 3, "remaining": 0, "limit": 3, "window_seconds": 10})
        status, second, _ = self.request(
            "POST", f"/v1/windows/hr-1/reservations/{reservation_id}/consume", {})
        self.assertEqual((status, second), (200, first))
        # The converted occupancy slides out at the consume moment + window_seconds.
        clock.t = 5010.0
        status, body, _ = self.request("GET", "/v1/windows/hr-1")
        self.assertEqual((body["used"], body["remaining"]), (0, 3))

    def test_rollback_over_http(self) -> None:
        clock = type(self).clock
        clock.t = 5500.0
        self.request("PUT", "/v1/windows/hr-2", {"window_seconds": 10, "max_events": 2})
        _, created, _ = self.request("POST", "/v1/windows/hr-2/reservations", {"cost": 2})
        status, body, _ = self.request(
            "DELETE", f"/v1/windows/hr-2/reservations/{created['reservation_id']}")
        self.assertEqual(status, 200)
        self.assertEqual(body, {"key": "hr-2", "cost": 2, "rolled_back": True,
                                "used": 0, "remaining": 2, "limit": 2, "window_seconds": 10})
        status, body, _ = self.request(
            "DELETE", f"/v1/windows/hr-2/reservations/{created['reservation_id']}")
        self.assertEqual((status, body["error"]["code"]), (404, "not_found"))

    def test_over_quota_retry_after_is_ceiled_to_milliseconds(self) -> None:
        clock = type(self).clock
        clock.t = 5400.0
        self.request("PUT", "/v1/windows/hr-3", {"window_seconds": 10, "max_events": 1})
        self.request("POST", "/v1/windows/hr-3/reservations", {"ttl_seconds": 10})
        clock.t = 5409.9999
        status, body, headers = self.request("POST", "/v1/windows/hr-3/reservations", {})
        self.assertEqual((status, body["error"]["code"]), (429, "over_quota"))
        self.assertEqual(headers["Retry-After"], "0.001")

    def test_error_classification_over_http(self) -> None:
        self.request("PUT", "/v1/windows/hr-4", {"window_seconds": 10, "max_events": 2})
        # cost above max_events is 400 without Retry-After.
        status, body, headers = self.request("POST", "/v1/windows/hr-4/reservations", {"cost": 3})
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        self.assertNotIn("Retry-After", headers)
        # Unknown window is 404; unknown and cross-key ids are 404.
        self.assertEqual(self.request("POST", "/v1/windows/ghost/reservations", {})[0], 404)
        self.assertEqual(self.request("DELETE", "/v1/windows/hr-4/reservations/nope")[0], 404)
        self.assertEqual(
            self.request("POST", "/v1/windows/hr-4/reservations/nope/consume", {})[0], 404)
        _, created, _ = self.request("POST", "/v1/windows/hr-4/reservations", {})
        self.request("PUT", "/v1/windows/hr-5", {"window_seconds": 10, "max_events": 2})
        self.assertEqual(
            self.request("DELETE", f"/v1/windows/hr-5/reservations/{created['reservation_id']}")[0],
            404)
        self.assertEqual(
            self.request(
                "POST", f"/v1/windows/hr-5/reservations/{created['reservation_id']}/consume", {})[0],
            404)

    def test_body_validation_over_http(self) -> None:
        self.request("PUT", "/v1/windows/hr-6", {"window_seconds": 10, "max_events": 3})
        path = "/v1/windows/hr-6/reservations"
        for payload in (b"", b"[]", b"null", b"5", b'"x"', b'{"key": "hr-6"}', b'{"x": 1}',
                        b'{"cost": 0}', b'{"cost": true}', b'{"cost": 1.5}',
                        b'{"ttl_seconds": 0}', b'{"ttl_seconds": 86401}', b'{"ttl_seconds": 1.5}',
                        b'{"cost": 1, "ttl_seconds": 60, "x": 1}'):
            status, parsed = self.raw_request("POST", path, payload)
            self.assertEqual((status, parsed["error"]["code"]), (400, "invalid_request"), payload)
        status, parsed = self.raw_request("POST", path, b'{bad json')
        self.assertEqual((status, parsed["error"]["code"]), (400, "invalid_request"))
        status, parsed = self.raw_request("POST", path, b"{}", content_length="omit")
        self.assertEqual((status, parsed["error"]["code"]), (400, "invalid_request"))
        # The consume body must be the empty object.
        _, created, _ = self.request("POST", path, {})
        consume_path = f"/v1/windows/hr-6/reservations/{created['reservation_id']}/consume"
        for payload in (b'{"x": 1}', b"[]", b"null"):
            status, parsed = self.raw_request("POST", consume_path, payload)
            self.assertEqual((status, parsed["error"]["code"]), (400, "invalid_request"), payload)
        # Nothing was held or consumed by the rejected requests.
        _, state, _ = self.request("GET", "/v1/windows/hr-6")
        self.assertEqual((state["used"], state["remaining"]), (1, 2))

    def test_route_and_method_mismatches_are_404(self) -> None:
        for method, path, payload in [
            ("GET", "/v1/windows/hr-7/reservations", None),
            ("PUT", "/v1/windows/hr-7/reservations", b'{}'),
            ("POST", "/v1/windows/hr-7/reservations/x", b'{}'),       # missing /consume
            ("DELETE", "/v1/windows/hr-7/reservations/x/consume", None),
            ("POST", "/v1/windows/hr-7/reservations/x/consume/extra", b'{}'),
            ("GET", "/v1/windows/hr-7/reservations/x/consume", None),
            ("POST", "/v1/reservations/x/consume", b'{}'),            # token-bucket shape stays put
        ]:
            status, parsed = self.raw_request(
                method, path, payload,
                content_length="auto" if payload is not None else "omit")
            self.assertEqual((status, parsed["error"]["code"]), (404, "not_found"), (method, path))

    def test_metrics_ledger_and_etag_are_untouched_over_http(self) -> None:
        clock = type(self).clock
        clock.t = 5300.0
        self.request("PUT", "/v1/windows/hr-8", {"window_seconds": 10, "max_events": 2})
        _, metrics_before, _ = self.request("GET", "/v1/metrics")
        _, created, _ = self.request("POST", "/v1/windows/hr-8/reservations", {"cost": 2})
        self.request("POST", f"/v1/windows/hr-8/reservations/{created['reservation_id']}/consume", {})
        _, metrics_after, _ = self.request("GET", "/v1/metrics")
        self.assertEqual(metrics_after, metrics_before)
        # A same-named bucket carries no ledger events and an unchanged ETag from all this.
        self.request("PUT", "/v1/limits/hr-8", {"capacity": 5, "refill_per_second": 1})
        _, _, headers_before = self.request("GET", "/v1/limits/hr-8")
        clock.t = 5311.0                                              # converted occupancy slid out
        _, created, _ = self.request("POST", "/v1/windows/hr-8/reservations", {})
        self.request("DELETE", f"/v1/windows/hr-8/reservations/{created['reservation_id']}")
        _, ledger, _ = self.request("GET", "/v1/ledgers/hr-8")
        self.assertEqual(ledger["totals"], {"accepted_count": 0, "accepted_cost": 0})
        _, _, headers_after = self.request("GET", "/v1/limits/hr-8")
        self.assertEqual(headers_after["ETag"], headers_before["ETag"])


if __name__ == "__main__":
    unittest.main()
