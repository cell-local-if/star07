"""Window capacity reservations: holds against a sliding window that settle without an event.

Covers the three new routes' limiter-level and HTTP behaviour:

* POST   /v1/windows/{key}/reservations                          — hold cost, counts in used now
* DELETE /v1/windows/{key}/reservations/{rid}                    — release a live hold once
* POST   /v1/windows/{key}/reservations/{rid}/consume            — convert a hold into an event

A live hold contributes its cost to used/remaining immediately, is invisible to the ledger,
revision/ETag and the five decision counters, lapses at created_at + ttl_seconds WITHOUT an event,
and on consume becomes one ordinary (effective_at, cost) event stamped at the consume moment that
then slides out by window_seconds. The 429 Retry-After walks one merged timeline of event leave
moments and hold release moments.
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
        self.limiter.configure_window("w", {"window_seconds": 10, "max_events": 10})

    # ----- creation -----------------------------------------------------

    def test_create_shape_and_defaults(self) -> None:
        result = self.limiter.window_reserve("w")
        self.assertEqual(set(result),
                         {"reservation_id", "key", "cost", "used", "remaining",
                          "limit", "window_seconds", "ttl_seconds"})
        self.assertEqual((result["key"], result["cost"], result["used"], result["remaining"],
                          result["limit"], result["window_seconds"], result["ttl_seconds"]),
                         ("w", 1, 1, 9, 10, 10, 60))
        self.assertIsInstance(result["reservation_id"], str)
        self.assertTrue(result["reservation_id"])

    def test_create_with_explicit_cost_and_ttl_sums_into_used(self) -> None:
        first = self.limiter.window_reserve("w", 4, ttl_seconds=30)
        self.assertEqual((first["used"], first["remaining"], first["ttl_seconds"]), (4, 6, 30))
        second = self.limiter.window_reserve("w", 3, ttl_seconds=15)
        self.assertEqual((second["used"], second["remaining"]), (7, 3))
        # Equality fills the window exactly.
        third = self.limiter.window_reserve("w", 3)
        self.assertEqual((third["used"], third["remaining"]), (10, 0))

    def test_hold_counts_in_state_and_check_immediately(self) -> None:
        self.limiter.window_reserve("w", 4)
        state = self.limiter.window_state("w")
        self.assertEqual((state["used"], state["remaining"]), (4, 6))
        # A later check judges against the held capacity.
        with self.assertRaises(OverQuota):
            self.limiter.window_check("w", 7)                 # 4 + 7 > 10
        result = self.limiter.window_check("w", 6)
        self.assertTrue(result["allowed"])
        self.assertEqual((result["used"], result["remaining"]), (10, 0))

    def test_create_holds_no_event_in_history(self) -> None:
        self.limiter.window_reserve("w", 4)
        # The hold occupies capacity but the window's event history stays empty.
        self.assertEqual(self.limiter._windows["w"].events, [])

    def test_insufficient_capacity_is_429_without_creating_the_hold(self) -> None:
        self.limiter.window_reserve("w", 8)
        with self.assertRaises(OverQuota) as raised:
            self.limiter.window_reserve("w", 3)               # 8 + 3 > 10
        self.assertAlmostEqual(raised.exception.retry_after, 60.0, places=6)  # default ttl
        self.assertEqual(len(self.limiter._window_reservations), 1)
        self.assertEqual(self.limiter.window_state("w")["used"], 8)

    def test_cost_above_max_events_is_invalid_request(self) -> None:
        for bad in (11, 1_000_000):
            with self.assertRaises(InvalidRequest) as raised:
                self.limiter.window_reserve("w", bad)
            self.assertIn("exceeds max_events 10", str(raised.exception))
            self.assertNotIsInstance(raised.exception, OverQuota)

    def test_oversized_cost_rejection_neither_settles_nor_samples_clock(self) -> None:
        self.limiter.configure_window("stale", {"window_seconds": 10, "max_events": 5})
        self.limiter.window_check("stale", 2)                 # event at 1000
        self.clock.t = 1020.0                                 # it would be stale now
        with self.assertRaises(InvalidRequest):
            self.limiter.window_reserve("stale", 6)
        self.assertEqual(self.limiter._windows["stale"].events, [(1000.0, 2)])
        self.clock.t = 1005.0                                 # clock jumps backwards
        # No sample pinned the watermark at 1020: the 1000 event still survives at this read.
        self.assertEqual(self.limiter.window_state("stale")["used"], 2)

    def test_invalid_cost_and_ttl_shapes_are_rejected(self) -> None:
        for bad in (0, -1, 1_000_001, True, False, 1.5, "3", None, [3]):
            with self.assertRaises(InvalidRequest, msg=repr(bad)):
                self.limiter.window_reserve("w", bad)
        for bad in (0, 86_401, True, False, 1.5, "60", None, [60]):
            with self.assertRaises(InvalidRequest, msg=repr(bad)):
                self.limiter.window_reserve("w", 1, bad)
        # Format validation beats the window lookup, exactly as on window check.
        with self.assertRaises(InvalidRequest):
            self.limiter.window_reserve("ghost", 0)
        for legal in (1, 11, 1_000_000):
            with self.assertRaises(LimitNotFound, msg=repr(legal)):
                self.limiter.window_reserve("ghost", legal)

    # ----- TTL release --------------------------------------------------

    def test_hold_releases_at_ttl_boundary_without_an_event(self) -> None:
        self.limiter.window_reserve("w", 4, ttl_seconds=5)   # release at 1005
        self.clock.t = 1004.0
        self.assertEqual(self.limiter.window_state("w")["used"], 4)
        self.clock.t = 1005.0                                 # inclusive boundary
        self.assertEqual(self.limiter.window_state("w")["used"], 0)
        self.assertEqual(self.limiter._windows["w"].events, [])   # no event was ever formed
        self.assertEqual(self.limiter._window_reservations, {})

    def test_release_frees_capacity_seen_by_later_check_and_reserve(self) -> None:
        self.limiter.window_reserve("w", 10, ttl_seconds=5)
        with self.assertRaises(OverQuota):
            self.limiter.window_check("w")
        self.clock.t = 1005.0
        result = self.limiter.window_check("w")              # freed at the boundary
        self.assertTrue(result["allowed"])
        self.assertEqual(result["used"], 1)
        self.clock.t = 1006.0
        again = self.limiter.window_reserve("w", 9)          # the check's event holds 1
        self.assertEqual((again["used"], again["remaining"]), (10, 0))

    def test_settle_runs_on_every_window_entry(self) -> None:
        rid = self.limiter.window_reserve("w", 4, ttl_seconds=5)["reservation_id"]
        self.clock.t = 1005.0
        # Each entry settles the due hold itself: state, check and a fresh reserve all see it gone.
        self.assertEqual(self.limiter.window_state("w")["used"], 0)
        self.limiter.window_reserve("w", 1, ttl_seconds=5)
        self.clock.t = 1011.0                                 # new hold also due
        self.assertTrue(self.limiter.window_check("w")["allowed"])
        with self.assertRaises(LimitNotFound):
            self.limiter.window_consume("w", rid)            # an expired hold is unknown

    def test_stalled_clock_keeps_the_hold_and_regressed_clock_never_releases_early(self) -> None:
        self.limiter.window_reserve("w", 4, ttl_seconds=5)
        for _ in range(3):
            self.assertEqual(self.limiter.window_state("w")["used"], 4)
        self.clock.t = 900.0                                  # jump backwards
        self.assertEqual(self.limiter.window_state("w")["used"], 4)
        with self.assertRaises(OverQuota):
            self.limiter.window_check("w", 7)
        self.clock.t = 1004.0
        self.assertEqual(self.limiter.window_state("w")["used"], 4)
        self.clock.t = 1005.0
        self.assertEqual(self.limiter.window_state("w")["used"], 0)

    # ----- rollback -----------------------------------------------------

    def test_rollback_frees_immediately_once(self) -> None:
        rid = self.limiter.window_reserve("w", 4)["reservation_id"]
        result = self.limiter.window_rollback("w", rid)
        self.assertEqual(result, {"key": "w", "cost": 4,
                                  "used": 0, "remaining": 10, "limit": 10,
                                  "window_seconds": 10, "rolled_back": True})
        with self.assertRaises(LimitNotFound):
            self.limiter.window_rollback("w", rid)

    def test_rollback_reopens_capacity_for_a_waiting_check(self) -> None:
        rid = self.limiter.window_reserve("w", 10)["reservation_id"]
        with self.assertRaises(OverQuota):
            self.limiter.window_check("w")
        self.limiter.window_rollback("w", rid)
        result = self.limiter.window_check("w")
        self.assertTrue(result["allowed"])
        self.assertEqual(result["used"], 1)

    def test_rollback_after_expiry_or_consume_is_404(self) -> None:
        expired = self.limiter.window_reserve("w", 1, ttl_seconds=5)["reservation_id"]
        consumed = self.limiter.window_reserve("w", 1, ttl_seconds=50)["reservation_id"]
        self.limiter.window_consume("w", consumed)
        self.clock.t = 1005.0
        with self.assertRaises(LimitNotFound):
            self.limiter.window_rollback("w", expired)
        with self.assertRaises(LimitNotFound):
            self.limiter.window_rollback("w", consumed)       # confirmed holds are never refunded

    # ----- consume ------------------------------------------------------

    def test_consume_converts_hold_into_event_with_same_occupancy(self) -> None:
        rid = self.limiter.window_reserve("w", 4, ttl_seconds=60)["reservation_id"]
        self.clock.t = 1005.0
        result = self.limiter.window_consume("w", rid)
        self.assertEqual(result, {"key": "w", "cost": 4,
                                  "used": 4, "remaining": 6, "limit": 10,
                                  "window_seconds": 10, "consumed": True})
        # The hold left the registry and the same cost is now an ordinary event stamped at 1005.
        self.assertEqual(self.limiter._windows["w"].events, [(1005.0, 4)])
        self.assertEqual(self.limiter._window_reservations, {})
        self.assertEqual(self.limiter.window_state("w")["used"], 4)

    def test_consumed_event_slides_out_by_window_seconds_not_creation_ttl(self) -> None:
        # The creation TTL (60 -> release 1060) no longer governs: consumed at 1005, the ordinary
        # event leaves at 1005 + window_seconds = 1015, long before the TTL would have lapsed.
        rid = self.limiter.window_reserve("w", 4, ttl_seconds=60)["reservation_id"]
        self.clock.t = 1005.0
        self.limiter.window_consume("w", rid)
        self.clock.t = 1014.0
        self.assertEqual(self.limiter.window_state("w")["used"], 4)
        self.clock.t = 1015.0
        self.assertEqual(self.limiter.window_state("w")["used"], 0)

    def test_consume_is_idempotent_and_replays_verbatim(self) -> None:
        rid = self.limiter.window_reserve("w", 4, ttl_seconds=60)["reservation_id"]
        self.clock.t = 1005.0
        first = self.limiter.window_consume("w", rid)
        self.clock.t = 1020.0                                 # the event has since left
        self.assertEqual(self.limiter.window_state("w")["used"], 0)
        second = self.limiter.window_consume("w", rid)
        self.assertEqual(second, first)                       # frozen first response
        # Only one event was ever produced.
        self.assertEqual(len(self.limiter._windows["w"].events), 0)  # it has expired by now
        with self.assertRaises(LimitNotFound):
            self.limiter.window_rollback("w", rid)

    def test_consume_after_expiry_is_404_and_never_books(self) -> None:
        rid = self.limiter.window_reserve("w", 4, ttl_seconds=5)["reservation_id"]
        self.clock.t = 1005.0
        with self.assertRaises(LimitNotFound):
            self.limiter.window_consume("w", rid)
        self.assertEqual(self.limiter._windows["w"].events, [])
        self.assertEqual(self.limiter.window_state("w")["used"], 0)

    # ----- unknown / cross-key / cross-namespace ------------------------

    def test_unknown_window_is_404_on_every_route(self) -> None:
        with self.assertRaises(LimitNotFound):
            self.limiter.window_reserve("ghost", 1)
        with self.assertRaises(LimitNotFound):
            self.limiter.window_rollback("ghost", "rid")
        with self.assertRaises(LimitNotFound):
            self.limiter.window_consume("ghost", "rid")

    def test_cross_key_hold_is_404_and_stays_intact(self) -> None:
        self.limiter.configure_window("other", {"window_seconds": 10, "max_events": 10})
        rid = self.limiter.window_reserve("other", 4)["reservation_id"]
        # Accessed through the wrong window: never resolved, never deleted.
        with self.assertRaises(LimitNotFound):
            self.limiter.window_rollback("w", rid)
        with self.assertRaises(LimitNotFound):
            self.limiter.window_consume("w", rid)
        self.assertEqual(self.limiter.window_state("other")["used"], 4)
        # The original window can still settle it.
        self.assertEqual(self.limiter.window_rollback("other", rid)["rolled_back"], True)

    def test_consumed_cross_key_replay_does_not_resolve_elsewhere(self) -> None:
        self.limiter.configure_window("other", {"window_seconds": 10, "max_events": 10})
        rid = self.limiter.window_reserve("other", 4)["reservation_id"]
        self.limiter.window_consume("other", rid)
        with self.assertRaises(LimitNotFound):
            self.limiter.window_consume("w", rid)
        # The frozen replay still resolves on the owning window.
        replay = self.limiter.window_consume("other", rid)
        self.assertTrue(replay["consumed"])

    def test_window_hold_ids_never_resolve_on_token_bucket_routes_and_vice_versa(self) -> None:
        self.limiter.configure("w", {"capacity": 10, "refill_per_second": 1.0})
        token_rid = self.limiter.reserve("w", 4)["reservation_id"]
        window_rid = self.limiter.window_reserve("w", 4)["reservation_id"]
        with self.assertRaises(LimitNotFound):
            self.limiter.window_rollback("w", token_rid)
        with self.assertRaises(LimitNotFound):
            self.limiter.window_consume("w", token_rid)
        with self.assertRaises(LimitNotFound):
            self.limiter.rollback(window_rid)
        with self.assertRaises(LimitNotFound):
            self.limiter.consume(window_rid)
        # Both holds are still live in their own registries.
        self.assertEqual(self.limiter.window_state("w")["used"], 4)

    # ----- merged retry-after timeline ----------------------------------

    def test_retry_after_merges_hold_release_and_event_leave_moments(self) -> None:
        # Event 4 at 1000 (leaves 1010); hold 4 created 1000 with ttl 3 (releases 1003): used 8.
        self.limiter.window_check("w", 4)
        self.limiter.window_reserve("w", 4, ttl_seconds=3)
        with self.assertRaises(OverQuota) as raised:
            self.limiter.window_reserve("w", 3)               # hold release at 1003 frees room
        self.assertAlmostEqual(raised.exception.retry_after, 3.0, places=6)
        with self.assertRaises(OverQuota) as raised:
            self.limiter.window_reserve("w", 7)               # need both releases: wait to 1010
        self.assertAlmostEqual(raised.exception.retry_after, 10.0, places=6)

    def test_window_check_retry_after_waits_for_hold_release(self) -> None:
        self.limiter.window_reserve("w", 6, ttl_seconds=2)    # releases 1002
        with self.assertRaises(OverQuota) as raised:
            self.limiter.window_check("w", 5)                 # 6 + 5 > 10
        self.assertAlmostEqual(raised.exception.retry_after, 2.0, places=6)

    def test_same_moment_event_and_hold_release_as_one_batch(self) -> None:
        self.limiter.window_check("w", 4)                     # leaves 1010
        self.limiter.window_reserve("w", 4, ttl_seconds=10)   # also releases 1010: batch of 8
        with self.assertRaises(OverQuota) as raised:
            self.limiter.window_reserve("w", 9)               # 8 + 9 > 10; one batch frees all
        self.assertAlmostEqual(raised.exception.retry_after, 10.0, places=6)

    def test_sub_millisecond_wait_is_preserved_for_the_http_ceiling(self) -> None:
        self.limiter.window_reserve("w", 10, ttl_seconds=1)
        self.clock.t = 1000.9999
        with self.assertRaises(OverQuota) as raised:
            self.limiter.window_reserve("w", 1)
        self.assertAlmostEqual(raised.exception.retry_after, 0.0001, places=9)

    # ----- reconfigure interactions -------------------------------------

    def test_shortening_window_evicts_events_but_never_extends_or_shortens_hold_ttl(self) -> None:
        self.limiter.window_check("w", 4)                     # event at 1000
        self.limiter.window_reserve("w", 4, ttl_seconds=20)   # releases 1020
        self.clock.t = 1006.0
        self.limiter.configure_window("w", {"window_seconds": 5, "max_events": 10})
        self.assertEqual(self.limiter.window_state("w")["used"], 4)   # event evicted, hold kept
        self.clock.t = 1015.0
        self.assertEqual(self.limiter.window_state("w")["used"], 4)   # hold still bound to 1020
        self.clock.t = 1020.0
        self.assertEqual(self.limiter.window_state("w")["used"], 0)

    def test_lowering_max_events_keeps_existing_holds(self) -> None:
        self.limiter.window_reserve("w", 6, ttl_seconds=20)
        self.limiter.configure_window("w", {"window_seconds": 10, "max_events": 5})
        state = self.limiter.window_state("w")
        self.assertEqual((state["used"], state["remaining"]), (6, -1))
        with self.assertRaises(OverQuota):
            self.limiter.window_reserve("w", 1)
        with self.assertRaises(InvalidRequest):
            self.limiter.window_reserve("w", 6)

    # ----- isolation: metrics, ledger, buckets --------------------------

    def test_no_decision_counter_moves_for_any_hold_operation(self) -> None:
        before = self.limiter.metrics()
        self.limiter.window_reserve("w", 2)
        with self.assertRaises(OverQuota):
            self.limiter.window_reserve("w", 9)                # legal cost, 2 + 9 > 10: a 429
        rid = self.limiter.window_reserve("w", 3, ttl_seconds=50)["reservation_id"]
        self.limiter.window_consume("w", rid)
        self.limiter.window_consume("w", rid)
        rolled = self.limiter.window_reserve("w", 1)["reservation_id"]
        self.limiter.window_rollback("w", rolled)
        with self.assertRaises(LimitNotFound):
            self.limiter.window_rollback("w", rolled)
        self.assertEqual(self.limiter.metrics(), before)      # shape and all five kinds unchanged

    def test_hold_operations_never_write_the_ledger(self) -> None:
        self.limiter.configure("w", {"capacity": 10, "refill_per_second": 1.0})
        rid = self.limiter.window_reserve("w", 4)["reservation_id"]
        self.limiter.window_consume("w", rid)
        rolled = self.limiter.window_reserve("w", 4)["reservation_id"]
        self.limiter.window_rollback("w", rolled)
        self.assertEqual(self.limiter.ledger("w")["totals"],
                         {"accepted_count": 0, "accepted_cost": 0})

    def test_concurrent_holds_never_oversell(self) -> None:
        limiter = Limiter(self.clock)                         # frozen clock
        limiter.configure_window("hot", {"window_seconds": 10, "max_events": 100})
        outcomes: list[str] = []
        outcomes_lock = threading.Lock()

        def attempt() -> None:
            try:
                limiter.window_reserve("hot", 1)
                outcome = "allowed"
            except OverQuota:
                outcome = "over_quota"
            except BaseException as error:  # noqa: BLE001 - surface thread failures
                outcome = f"error:{error!r}"
            with outcomes_lock:
                outcomes.append(outcome)

        threads = [threading.Thread(target=attempt) for _ in range(400)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertTrue(all(o in ("allowed", "over_quota") for o in outcomes), outcomes)
        self.assertEqual(outcomes.count("allowed"), 100)
        self.assertEqual(len(limiter._window_reservations), 100)
        self.assertEqual(limiter.window_state("hot")["used"], 100)
        # Holds are not window_check decisions.
        counts = limiter.metrics()["metrics"]["decisions"]
        self.assertEqual(counts["window_check"], {"allowed": 0, "over_quota": 0})


class WindowReservationHttpTests(unittest.TestCase):
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

    def setUp(self) -> None:
        # One server (and hence one Limiter/watermark) is shared by every method; reset both the
        # injected clock and the high-water mark so each test starts from a clean 5000.0 timeline.
        clock = type(self).clock
        clock.t = 5000.0
        self.server.limiter._watermark = float("-inf")

    # ----- lifecycle ----------------------------------------------------

    def test_create_holds_then_check_and_state_see_it(self) -> None:
        clock = type(self).clock
        clock.t = 7000.0
        self.request("PUT", "/v1/windows/hr-1", {"window_seconds": 10, "max_events": 10})
        status, body, headers = self.request("POST", "/v1/windows/hr-1/reservations", {})
        self.assertEqual(status, 200)
        self.assertEqual(set(body), {"reservation_id", "key", "cost", "used", "remaining",
                                     "limit", "window_seconds", "ttl_seconds"})
        self.assertEqual((body["key"], body["cost"], body["used"], body["remaining"],
                          body["limit"], body["window_seconds"], body["ttl_seconds"]),
                         ("hr-1", 1, 1, 9, 10, 10, 60))
        self.assertNotIn("ETag", headers)
        rid = body["reservation_id"]
        status, body, _ = self.request("GET", "/v1/windows/hr-1")
        self.assertEqual((status, body["used"], body["remaining"]), (200, 1, 9))
        # The held capacity is enforced on the ordinary check route.
        status, body, _ = self.request("POST", "/v1/windows/hr-1/check", {"cost": 10})
        self.assertEqual((status, body["error"]["code"]), (429, "over_quota"))

    def test_weighted_create_rollback_and_consume_shapes(self) -> None:
        clock = type(self).clock
        clock.t = 7100.0
        self.request("PUT", "/v1/windows/hr-2", {"window_seconds": 10, "max_events": 10})
        status, created, headers = self.request(
            "POST", "/v1/windows/hr-2/reservations", {"cost": 4, "ttl_seconds": 30})
        self.assertEqual(status, 200)
        self.assertEqual(created, {"reservation_id": created["reservation_id"], "key": "hr-2",
                                   "cost": 4, "used": 4, "remaining": 6, "limit": 10,
                                   "window_seconds": 10, "ttl_seconds": 30})
        self.assertNotIn("ETag", headers)
        rid = created["reservation_id"]
        # Consume converts the hold to an event with unchanged net occupancy.
        status, consumed, headers = self.request(
            "POST", f"/v1/windows/hr-2/reservations/{rid}/consume", {})
        self.assertEqual(status, 200)
        self.assertEqual(consumed, {"key": "hr-2", "cost": 4,
                                    "used": 4, "remaining": 6, "limit": 10,
                                    "window_seconds": 10, "consumed": True})
        self.assertNotIn("ETag", headers)
        # Replay is byte-for-byte.
        status, replay, _ = self.request(
            "POST", f"/v1/windows/hr-2/reservations/{rid}/consume", {})
        self.assertEqual((status, replay), (200, consumed))
        # A confirmed hold cannot be rolled back.
        status, body, _ = self.request("DELETE", f"/v1/windows/hr-2/reservations/{rid}")
        self.assertEqual((status, body["error"]["code"]), (404, "not_found"))

    def test_rollback_frees_capacity_and_is_404_on_repeat(self) -> None:
        self.request("PUT", "/v1/windows/hr-3", {"window_seconds": 10, "max_events": 4})
        rid = self.request("POST", "/v1/windows/hr-3/reservations", {"cost": 4})[1]["reservation_id"]
        status, body, headers = self.request("DELETE", f"/v1/windows/hr-3/reservations/{rid}")
        self.assertEqual(status, 200)
        self.assertEqual(body, {"key": "hr-3", "cost": 4,
                                "used": 0, "remaining": 4, "limit": 4,
                                "window_seconds": 10, "rolled_back": True})
        self.assertNotIn("ETag", headers)
        status, body, _ = self.request("DELETE", f"/v1/windows/hr-3/reservations/{rid}")
        self.assertEqual((status, body["error"]["code"]), (404, "not_found"))
        # Capacity is back: a full-size check now fits.
        status, body, _ = self.request("POST", "/v1/windows/hr-3/check", {"cost": 4})
        self.assertEqual(status, 200)
        self.assertEqual(body["used"], 4)

    def test_consumed_event_slides_out_by_window_seconds(self) -> None:
        clock = type(self).clock
        clock.t = 7200.0
        self.request("PUT", "/v1/windows/hr-4", {"window_seconds": 10, "max_events": 10})
        rid = self.request("POST", "/v1/windows/hr-4/reservations",
                           {"cost": 4, "ttl_seconds": 60})[1]["reservation_id"]
        clock.t = 7205.0
        self.assertEqual(self.request(
            "POST", f"/v1/windows/hr-4/reservations/{rid}/consume", {})[0], 200)
        clock.t = 7214.0
        self.assertEqual(self.request("GET", "/v1/windows/hr-4")[1]["used"], 4)
        clock.t = 7215.0                                        # 7205 + window_seconds
        self.assertEqual(self.request("GET", "/v1/windows/hr-4")[1]["used"], 0)

    def test_ttl_release_then_rollback_and_consume_are_404(self) -> None:
        clock = type(self).clock
        clock.t = 7300.0
        self.request("PUT", "/v1/windows/hr-5", {"window_seconds": 10, "max_events": 10})
        rid = self.request("POST", "/v1/windows/hr-5/reservations",
                           {"cost": 4, "ttl_seconds": 5})[1]["reservation_id"]
        clock.t = 7305.0                                        # inclusive TTL boundary
        self.assertEqual(self.request("GET", "/v1/windows/hr-5")[1]["used"], 0)
        status, body, _ = self.request("DELETE", f"/v1/windows/hr-5/reservations/{rid}")
        self.assertEqual((status, body["error"]["code"]), (404, "not_found"))
        status, body, _ = self.request(
            "POST", f"/v1/windows/hr-5/reservations/{rid}/consume", {})
        self.assertEqual((status, body["error"]["code"]), (404, "not_found"))

    # ----- error classification -----------------------------------------

    def test_create_body_must_be_cost_ttl_object(self) -> None:
        self.request("PUT", "/v1/windows/hr-6", {"window_seconds": 10, "max_events": 10})
        path = "/v1/windows/hr-6/reservations"
        for payload in (b"", b"[]", b"null", b"5", b'"x"', b'{"x": 1}',
                        b'{"cost": 0}', b'{"cost": true}', b'{"cost": 1.5}',
                        b'{"cost": 11}', b'{"ttl_seconds": 0}', b'{"ttl_seconds": true}',
                        b'{"cost": 1, "x": 1}'):
            status, parsed, _ = self.raw_request("POST", path, payload)
            self.assertEqual((status, parsed["error"]["code"]), (400, "invalid_request"), payload)
        status, parsed, _ = self.raw_request("POST", path, b'{bad json')
        self.assertEqual((status, parsed["error"]["code"]), (400, "invalid_request"))
        status, parsed, _ = self.raw_request("POST", path, b"{}", content_length="omit")
        self.assertEqual((status, parsed["error"]["code"]), (400, "invalid_request"))
        # Nothing was held by the rejected requests.
        self.assertEqual(self.request("GET", "/v1/windows/hr-6")[1]["used"], 0)

    def test_consume_body_must_be_empty_object(self) -> None:
        self.request("PUT", "/v1/windows/hr-7", {"window_seconds": 10, "max_events": 10})
        rid = self.request("POST", "/v1/windows/hr-7/reservations", {"cost": 1})[1]["reservation_id"]
        path = f"/v1/windows/hr-7/reservations/{rid}/consume"
        for payload in (b"[]", b"null", b"5", b'{"x": 1}', b'{"consumed": true}'):
            status, parsed, _ = self.raw_request("POST", path, payload)
            self.assertEqual((status, parsed["error"]["code"]), (400, "invalid_request"), payload)
        # The rejected consumes did not confirm the hold: rollback still works.
        status, _, _ = self.request("DELETE", f"/v1/windows/hr-7/reservations/{rid}")
        self.assertEqual(status, 200)

    def test_cost_above_max_events_is_400_without_retry_after(self) -> None:
        self.request("PUT", "/v1/windows/hr-8", {"window_seconds": 10, "max_events": 10})
        status, body, headers = self.request(
            "POST", "/v1/windows/hr-8/reservations", {"cost": 11})
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        self.assertIn("exceeds max_events 10", body["error"]["message"])
        self.assertNotIn("Retry-After", headers)
        self.assertEqual(self.request("GET", "/v1/windows/hr-8")[1]["used"], 0)

    def test_merged_retry_after_header(self) -> None:
        clock = type(self).clock
        clock.t = 7400.0
        self.request("PUT", "/v1/windows/hr-9", {"window_seconds": 10, "max_events": 10})
        self.request("POST", "/v1/windows/hr-9/check", {"cost": 4})          # leaves 7410
        self.request("POST", "/v1/windows/hr-9/reservations",
                     {"cost": 4, "ttl_seconds": 3})                          # releases 7403
        status, _, headers = self.request(
            "POST", "/v1/windows/hr-9/reservations", {"cost": 3})
        self.assertEqual(status, 429)
        self.assertEqual(headers["Retry-After"], "3.000")
        status, _, headers = self.request(
            "POST", "/v1/windows/hr-9/reservations", {"cost": 7})
        self.assertEqual(status, 429)
        self.assertEqual(headers["Retry-After"], "10.000")

    def test_unknown_and_cross_key_reservations_are_404(self) -> None:
        self.request("PUT", "/v1/windows/hr-a", {"window_seconds": 10, "max_events": 10})
        self.request("PUT", "/v1/windows/hr-b", {"window_seconds": 10, "max_events": 10})
        # Valid body, unknown window -> 404; invalid body, unknown window -> 400 (format first).
        status, body, _ = self.request("POST", "/v1/windows/hr-ghost/reservations", {"cost": 1})
        self.assertEqual((status, body["error"]["code"]), (404, "not_found"))
        status, body, _ = self.request("POST", "/v1/windows/hr-ghost/reservations", {"cost": 0})
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        rid = self.request("POST", "/v1/windows/hr-a/reservations", {"cost": 2})[1]["reservation_id"]
        for method, path in [
            ("DELETE", f"/v1/windows/hr-b/reservations/{rid}"),
            ("POST", f"/v1/windows/hr-b/reservations/{rid}/consume"),
        ]:
            body = {} if method == "POST" else None
            status, parsed, _ = self.request(method, path, body)
            self.assertEqual((status, parsed["error"]["code"]), (404, "not_found"), (method, path))
        # The cross-key access left the hold live on its owning window.
        self.assertEqual(self.request("GET", "/v1/windows/hr-a")[1]["used"], 2)

    def test_metrics_and_ledger_are_untouched(self) -> None:
        self.request("PUT", "/v1/windows/hr-c", {"window_seconds": 10, "max_events": 4})
        _, before, _ = self.request("GET", "/v1/metrics")
        rid = self.request("POST", "/v1/windows/hr-c/reservations", {"cost": 2})[1]["reservation_id"]
        self.request("POST", f"/v1/windows/hr-c/reservations/{rid}/consume", {})
        self.request("POST", f"/v1/windows/hr-c/reservations/{rid}/consume", {})
        rid2 = self.request("POST", "/v1/windows/hr-c/reservations", {"cost": 1})[1]["reservation_id"]
        self.request("DELETE", f"/v1/windows/hr-c/reservations/{rid2}")
        _, after, _ = self.request("GET", "/v1/metrics")
        self.assertEqual(after, before)
        # Window-only name: no bucket, hence no ledger.
        self.assertEqual(self.request("GET", "/v1/ledgers/hr-c")[0], 404)

    def test_route_and_method_mismatches_are_404(self) -> None:
        for method, path, payload in [
            ("POST", "/v1/windows/hr-d/reservations/rid", b"{}"),       # len5, no /consume
            ("POST", "/v1/windows/reservations", b"{}"),                # missing key
            ("POST", "/v1/windows/hr-d/reservations/rid/consume/extra", b"{}"),
            ("GET", "/v1/windows/hr-d/reservations", None),
            ("PUT", "/v1/windows/hr-d/reservations", b"{}"),
            ("DELETE", "/v1/windows/hr-d/reservations", None),          # len4
            ("DELETE", "/v1/windows/hr-d", None),
            ("PATCH", "/v1/windows/hr-d/reservations", b"{}"),
            ("POST", "//v1/windows/hr-d/reservations", b"{}"),
        ]:
            status, parsed, _ = self.raw_request(
                method, path, payload,
                content_length="auto" if payload is not None else "omit")
            self.assertEqual((status, parsed["error"]["code"]), (404, "not_found"), (method, path))
        # A garbage body off-route is never inspected.
        status, parsed, _ = self.raw_request("POST", "/v1/nope", b"\xff not json",
                                             content_length="nine")
        self.assertEqual(status, 404)


if __name__ == "__main__":
    unittest.main()
