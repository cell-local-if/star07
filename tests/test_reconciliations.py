"""Tests for GET /v1/reconciliations/{key}: one locked snapshot of bucket view and ledger totals.

Covers same-instant consistency with /v1/limits and /v1/ledgers, the one lazy settle of due
holds, the no-side-effects contract (no ledger append, revision, decision count or double
settle), stalled/regressed clock watermark semantics, concurrent readers mixed with every
mutating entry point, and the 400/404 error surface.
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


class JitterClock:
    """Thread-safe clock whose readings oscillate slightly below a fixed moment."""

    def __init__(self, t: float) -> None:
        self.t = t
        self.calls = 0
        self.lock = threading.Lock()

    def __call__(self) -> float:
        with self.lock:
            self.calls += 1
            return self.t - (self.calls % 3)


class ReconcileUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.limiter = Limiter(self.clock)
        self.limiter.configure("tenant-a", {"capacity": 10, "refill_per_second": 1.0})

    def test_reconcile_shape_matches_the_contract_example(self) -> None:
        # Four accepted spends totalling 7, clock frozen: 3 tokens remain.
        for cost in (1, 2, 1, 3):
            self.assertTrue(self.limiter.check("tenant-a", cost)["allowed"])
        result = self.limiter.reconcile("tenant-a")
        self.assertEqual(result, {"key": "tenant-a", "remaining": 3, "used": 7,
                                  "accepted_count": 4, "accepted_cost": 7, "balanced": True})
        self.assertEqual(set(result),
                         {"key", "remaining", "used", "accepted_count", "accepted_cost", "balanced"})

    def test_reconcile_matches_limits_state_and_ledger_totals_at_same_instant(self) -> None:
        self.limiter.check("tenant-a", 2)                          # tokens 8, used 2
        self.clock.t += 3.0                                       # 3 tokens back at 1/s: tokens 10 (capped)
        reservation = self.limiter.reserve("tenant-a", 4, ttl_seconds=60)  # tokens 6, still not used
        self.limiter.consume(reservation["reservation_id"])       # tokens 6, used 6, one more event
        result = self.limiter.reconcile("tenant-a")
        state = self.limiter.state("tenant-a")                    # same reading: identical view
        ledger = self.limiter.ledger("tenant-a", 1000)
        self.assertEqual((result["remaining"], result["used"]),
                         (state["remaining"], state["used"]))
        self.assertEqual((result["accepted_count"], result["accepted_cost"]),
                         (ledger["totals"]["accepted_count"], ledger["totals"]["accepted_cost"]))
        self.assertEqual(result, {"key": "tenant-a", "remaining": 6, "used": 6,
                                  "accepted_count": 2, "accepted_cost": 6, "balanced": True})

    def test_balanced_is_the_used_vs_accepted_cost_comparison(self) -> None:
        self.limiter.check("tenant-a", 2)
        # The service keeps used == accepted_cost itself; tamper with one side to prove the
        # field is computed rather than hard-coded to true.
        self.limiter._buckets["tenant-a"].cost_history.append(9)
        result = self.limiter.reconcile("tenant-a")
        self.assertEqual(result["used"], 11)
        self.assertEqual(result["accepted_cost"], 2)
        self.assertFalse(result["balanced"])

    def test_reconcile_refills_by_the_watermark_rules_like_limits_read(self) -> None:
        self.limiter.check("tenant-a", 10)                        # empty
        self.clock.t += 4.0
        result = self.limiter.reconcile("tenant-a")
        self.assertEqual((result["remaining"], result["used"]), (4, 10))
        state = self.limiter.state("tenant-a")                    # same instant, zero extra wait
        self.assertEqual(state["remaining"], 4)

    def test_reconcile_settles_due_single_key_reservation_once(self) -> None:
        limiter = Limiter(self.clock)
        limiter.configure("frozen", {"capacity": 5, "refill_per_second": 0.0001})
        self.assertTrue(limiter.check("frozen", 2)["allowed"])   # tokens 3, used 2
        reservation = limiter.reserve("frozen", 2, ttl_seconds=10)  # tokens 1, due at 1010
        self.clock.t += 10
        result = limiter.reconcile("frozen")                     # the one lazy settle runs here
        self.assertEqual(result, {"key": "frozen", "remaining": 3, "used": 2,
                                  "accepted_count": 1, "accepted_cost": 2, "balanced": True})
        self.assertNotIn(reservation["reservation_id"], limiter._reservations)
        # Same reading: no second refund, no conjured tokens, identical answer.
        self.assertEqual(limiter.reconcile("frozen"), result)
        with self.assertRaises(LimitNotFound):
            limiter.rollback(reservation["reservation_id"])
        # The settle booked nothing.
        self.assertEqual(limiter.ledger("frozen")["totals"],
                         {"accepted_count": 1, "accepted_cost": 2})

    def test_reconcile_settles_due_cross_layer_hold_on_every_layer(self) -> None:
        limiter = Limiter(self.clock)
        limiter.configure("global", {"capacity": 5, "refill_per_second": 0.0001})
        limiter.configure("tenant-b", {"capacity": 5, "refill_per_second": 0.0001})
        hold = limiter.hierarchy_reserve(["global", "tenant-b"], 2, ttl_seconds=5)
        single = limiter.reserve("tenant-b", 1, ttl_seconds=5)   # tenant-b holds 3 total
        self.clock.t += 5                                        # both holds are due
        result_b = limiter.reconcile("tenant-b")                 # settles the cross hold AND the single one
        self.assertEqual((result_b["remaining"], result_b["used"]), (5, 0))
        self.assertEqual(result_b["accepted_cost"], 0)
        # A cross-layer settle is one unit: the global layer was refunded in the same critical section.
        self.assertNotIn(hold["reservation_id"], limiter._hierarchy_reservations)
        self.assertNotIn(single["reservation_id"], limiter._reservations)
        result_g = limiter.reconcile("global")
        self.assertEqual((result_g["remaining"], result_g["used"], result_g["balanced"]), (5, 0, True))
        self.assertEqual(limiter.ledger("global")["events"], [])
        self.assertEqual(limiter.ledger("tenant-b")["events"], [])

    def test_reconcile_leaves_live_holds_and_their_refund_path_intact(self) -> None:
        reservation = self.limiter.reserve("tenant-a", 4, ttl_seconds=60)  # tokens 6
        result = self.limiter.reconcile("tenant-a")
        self.assertEqual((result["remaining"], result["used"]), (6, 0))
        self.assertIn(reservation["reservation_id"], self.limiter._reservations)
        rolled_back = self.limiter.rollback(reservation["reservation_id"])
        self.assertEqual(rolled_back["remaining"], 10)

    def test_reconcile_appends_no_ledger_event(self) -> None:
        self.limiter.check("tenant-a", 3)
        reservation = self.limiter.reserve("tenant-a", 2, ttl_seconds=5)
        before = self.limiter.ledger("tenant-a", 1000)
        self.clock.t += 5                                        # the hold becomes due
        for _ in range(4):
            self.limiter.reconcile("tenant-a")                   # settling readers never book
        after = self.limiter.ledger("tenant-a", 1000)
        self.assertEqual(after, before)
        self.assertEqual(after["totals"], {"accepted_count": 1, "accepted_cost": 3})
        with self.assertRaises(LimitNotFound):
            self.limiter.consume(reservation["reservation_id"])

    def test_reconcile_advances_no_revision_or_etag_state(self) -> None:
        self.assertEqual(self.limiter.state_snapshot("tenant-a")[1], 1)
        self.limiter.reserve("tenant-a", 2, ttl_seconds=5)
        self.clock.t += 5
        for _ in range(3):
            self.limiter.reconcile("tenant-a")
        self.assertEqual(self.limiter.state_snapshot("tenant-a")[1], 1)
        # A later real configuration write still advances exactly once.
        self.limiter.configure("tenant-a", {"capacity": 11, "refill_per_second": 1.0})
        self.assertEqual(self.limiter.state_snapshot("tenant-a")[1], 2)

    def test_reconcile_counts_no_decision(self) -> None:
        self.limiter.check("tenant-a", 1)
        self.limiter.reserve("tenant-a", 2, ttl_seconds=5)
        before = self.limiter.metrics()
        self.clock.t += 5
        for _ in range(10):
            self.limiter.reconcile("tenant-a")                   # even settling due holds
        self.assertEqual(self.limiter.metrics(), before)

    def test_reconcile_is_stable_under_a_stalled_clock(self) -> None:
        self.limiter.check("tenant-a", 4)
        first = self.limiter.reconcile("tenant-a")
        for _ in range(4):
            self.assertEqual(self.limiter.reconcile("tenant-a"), first)
        state = self.limiter.state("tenant-a")
        self.assertEqual((state["remaining"], state["used"]),
                         (first["remaining"], first["used"]))

    def test_reconcile_honours_watermark_on_clock_regression(self) -> None:
        self.clock.t = 100.0
        limiter = Limiter(self.clock)
        limiter.configure("k", {"capacity": 10, "refill_per_second": 1.0})
        self.assertTrue(limiter.check("k", 10)["allowed"])      # empty at t=100
        self.clock.t = 90.0                                      # clock jumps backwards
        self.assertEqual(limiter.reconcile("k")["remaining"], 0)
        self.clock.t = 101.0                                     # only 100→101 counts
        self.assertEqual(limiter.reconcile("k")["remaining"], 1)

    def test_due_hold_is_not_settled_early_under_regression_and_once_at_expiry(self) -> None:
        self.clock.t = 100.0
        limiter = Limiter(self.clock)
        limiter.configure("k", {"capacity": 10, "refill_per_second": 0.0001})
        reservation = limiter.reserve("k", 4, ttl_seconds=10)   # due at effective 110
        self.clock.t = 95.0                                      # regressed: time stays at 100
        self.assertEqual(limiter.reconcile("k")["remaining"], 6)
        self.assertIn(reservation["reservation_id"], limiter._reservations)
        self.clock.t = 110.0                                     # effective expiry moment
        result = limiter.reconcile("k")
        self.assertEqual((result["remaining"], result["used"], result["accepted_cost"]), (10, 0, 0))
        self.assertEqual(limiter.reconcile("k"), result)        # never refunded twice
        with self.assertRaises(LimitNotFound):
            limiter.rollback(reservation["reservation_id"])

    def test_regressed_interval_is_not_recounted_after_recovery(self) -> None:
        self.clock.t = 100.0
        limiter = Limiter(self.clock)
        limiter.configure("k", {"capacity": 20, "refill_per_second": 1.0})
        self.assertTrue(limiter.check("k", 10)["allowed"])      # 10 tokens left
        self.clock.t = 80.0                                      # big regression
        self.assertEqual(limiter.reconcile("k")["remaining"], 10)
        self.clock.t = 103.0                                     # just 100→103 elapsed
        self.assertEqual(limiter.reconcile("k")["remaining"], 13)

    def test_invalid_key_is_invalid_request_before_the_lock(self) -> None:
        for bad_key in [1, True, False, None, "", "x" * 201, ["tenant-a"], {"k": 1}, 1.5]:
            with self.assertRaises(InvalidRequest, msg=repr(bad_key)):
                self.limiter.reconcile(bad_key)
        # A rejected reconcile never sampled the clock: drain the bucket, offer a dominant
        # reading during a rejected call, then regress. Had the rejection ticked, the watermark
        # would be pinned to 200 and this read would show a full bucket instead of 5 tokens.
        self.assertTrue(self.limiter.check("tenant-a", 10)["allowed"])  # empty at t=1000
        self.clock.t = 200.0
        with self.assertRaises(InvalidRequest):
            self.limiter.reconcile("")
        self.clock.t = 1005.0                                     # only 1000→1005 may count
        self.assertEqual(self.limiter.reconcile("tenant-a")["remaining"], 5)

    def test_valid_but_unconfigured_key_is_not_found(self) -> None:
        with self.assertRaises(LimitNotFound):
            self.limiter.reconcile("ghost")

    def test_window_and_leaky_bucket_namespace_is_isolated(self) -> None:
        self.limiter.configure_window("only-window", {"window_seconds": 60, "max_events": 3})
        self.limiter.window_check("only-window")
        self.limiter.configure_leaky_bucket("only-leaky",
                                            {"capacity": 3, "leak_per_second": 1.0})
        self.limiter.leaky_bucket_check("only-leaky")
        with self.assertRaises(LimitNotFound):
            self.limiter.reconcile("only-window")
        with self.assertRaises(LimitNotFound):
            self.limiter.reconcile("only-leaky")


class ReconcileConcurrencyUnitTests(unittest.TestCase):
    def test_concurrent_reconciles_settle_due_holds_exactly_once(self) -> None:
        clock = Clock()
        limiter = Limiter(clock)
        limiter.configure("hot", {"capacity": 10, "refill_per_second": 0.0001})
        ids = [limiter.reserve("hot", 1, ttl_seconds=10)["reservation_id"] for _ in range(10)]
        clock.t += 10                                            # every hold is due
        results: list[dict] = []
        errors: list[BaseException] = []
        lock = threading.Lock()

        def read_many() -> None:
            try:
                local = []
                for _ in range(25):
                    view = limiter.reconcile("hot")
                    if not view["balanced"] or view["used"] != 0 or view["accepted_cost"] != 0:
                        raise AssertionError(f"unexpected reconcile view: {view}")
                    local.append(view)
                with lock:
                    results.extend(local)
            except BaseException as error:  # noqa: BLE001
                with lock:
                    errors.append(error)

        threads = [threading.Thread(target=read_many) for _ in range(16)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        self.assertEqual(len(results), 16 * 25)
        self.assertTrue(all(view["remaining"] == 10 for view in results))
        self.assertEqual([r for r in limiter._reservations.values() if r.key == "hot"], [])
        for rid in ids:
            with self.assertRaises(LimitNotFound):
                limiter.rollback(rid)
        state = limiter.state("hot")
        self.assertEqual((state["remaining"], state["used"]), (10, 0))
        self.assertEqual(limiter.ledger("hot")["events"], [])

    def test_concurrent_reads_against_every_mutating_entry_stay_self_consistent(self) -> None:
        clock = Clock()
        limiter = Limiter(clock)                                # frozen for the whole storm
        limiter.configure("hot", {"capacity": 200, "refill_per_second": 0.0001})
        rids = [limiter.reserve("hot", 1, ttl_seconds=3600)["reservation_id"] for _ in range(100)]
        consumed: list[str] = []
        rolled: list[str] = []
        errors: list[BaseException] = []
        list_lock = threading.Lock()

        def spend_check() -> None:
            try:
                limiter.check("hot", 1)                         # 100 free tokens: every check wins
            except BaseException as error:  # noqa: BLE001
                with list_lock:
                    errors.append(error)

        def settle(do_consume: bool, rid: str) -> None:
            try:
                if do_consume:
                    limiter.consume(rid)
                    with list_lock:
                        consumed.append(rid)
                else:
                    limiter.rollback(rid)
                    with list_lock:
                        rolled.append(rid)
            except LimitNotFound:
                pass
            except BaseException as error:  # noqa: BLE001
                with list_lock:
                    errors.append(error)

        def read_reconcile(reader_index: int) -> None:
            last_count = -1
            try:
                for round_index in range(50):
                    view = limiter.reconcile("hot")
                    # Every snapshot must be internally self-consistent:
                    if set(view) != {"key", "remaining", "used", "accepted_count",
                                     "accepted_cost", "balanced"}:
                        raise AssertionError(f"bad shape: {view}")
                    if not view["balanced"] or view["used"] != view["accepted_cost"]:
                        raise AssertionError(f"unbalanced snapshot: {view}")
                    if not (0 <= view["remaining"] <= 200 and 0 <= view["used"] <= 200):
                        raise AssertionError(f"out-of-range snapshot: {view}")
                    if view["accepted_count"] < last_count:     # the append-only ledger never shrinks
                        raise AssertionError(f"accepted_count moved backwards: {view}")
                    if view["accepted_count"] != view["accepted_cost"]:
                        raise AssertionError(f"every booking in this storm costs 1: {view}")
                    last_count = view["accepted_count"]
            except BaseException as error:  # noqa: BLE001
                with list_lock:
                    errors.append(error)

        threads = [threading.Thread(target=spend_check) for _ in range(100)]
        for index, rid in enumerate(rids):
            threads.append(threading.Thread(target=settle, args=(True, rid)))
            threads.append(threading.Thread(target=settle, args=(False, rid)))
        threads += [threading.Thread(target=read_reconcile, args=(index,)) for index in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])

        final = limiter.reconcile("hot")
        ledger = limiter.ledger("hot", 1000)
        state = limiter.state("hot")
        live = sum(r.cost for r in limiter._reservations.values() if r.key == "hot")
        self.assertEqual(len(consumed) + len(rolled), 100)
        self.assertEqual(set(consumed) & set(rolled), set())
        self.assertEqual(final["used"], state["used"])
        self.assertEqual(final["remaining"], state["remaining"])
        self.assertEqual((ledger["totals"]["accepted_count"], ledger["totals"]["accepted_cost"]),
                         (final["accepted_count"], final["accepted_cost"]))
        self.assertEqual(final["used"], 100 + len(consumed))
        self.assertEqual(final["remaining"] + final["used"] + live, 200)

    def test_concurrent_reads_under_a_jittering_clock_stay_balanced(self) -> None:
        limiter = Limiter(JitterClock(1000.0))
        limiter.configure("hot", {"capacity": 100, "refill_per_second": 0.0001})
        errors: list[BaseException] = []
        lock = threading.Lock()

        def spend() -> None:
            try:
                limiter.check("hot", 1)
            except OverQuota:
                pass
            except BaseException as error:  # noqa: BLE001
                with lock:
                    errors.append(error)

        def read_many() -> None:
            try:
                for _ in range(40):
                    view = limiter.reconcile("hot")
                    if not view["balanced"] or view["used"] != view["accepted_cost"]:
                        raise AssertionError(f"unbalanced snapshot under jitter: {view}")
            except BaseException as error:  # noqa: BLE001
                with lock:
                    errors.append(error)

        threads = [threading.Thread(target=spend) for _ in range(200)]
        threads += [threading.Thread(target=read_many) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        final = limiter.reconcile("hot")
        ledger = limiter.ledger("hot")
        self.assertEqual(final["used"], ledger["totals"]["accepted_cost"])
        self.assertEqual(final["accepted_count"], ledger["totals"]["accepted_count"])
        self.assertEqual(final["used"], 100)                     # never oversold, never over-counted


class ReconcileHttpTests(unittest.TestCase):
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

    def request(self, method: str, path: str,
                body: dict | None = None) -> tuple[int, dict, dict]:
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
                                         headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read() or b"{}"), dict(response.headers)
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read() or b"{}"), dict(error.headers)

    def raw_request(self, method: str, path: str, payload: bytes | None = None,
                    headers: dict[str, str] | None = None) -> tuple[int, dict, dict]:
        connection = self.http_client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        connection.putrequest(method, path)
        for name, value in (headers or {}).items():
            connection.putheader(name, value)
        if payload is not None:
            connection.putheader("Content-Length", str(len(payload)))
        connection.endheaders(payload if payload is not None else b"")
        response = connection.getresponse()
        raw = response.read()
        parsed = json.loads(raw or b"{}")
        response_headers = dict(response.getheaders())
        connection.close()
        return response.status, parsed, response_headers

    def request_retry(self, method: str, path: str,
                      body: dict | None = None) -> tuple[int, dict, dict]:
        # A 230-connection storm against the stdlib ThreadingHTTPServer can occasionally reset a
        # fresh connection; retry the idempotent request a few times instead of flaking.
        for attempt in range(5):
            try:
                return self.request(method, path, body)
            except (ConnectionError, OSError, urllib.error.URLError):
                if attempt == 4:
                    raise

    def test_success_body_matches_the_spec_example(self) -> None:
        clock = type(self).clock
        clock.t = 17000.0
        self.request("PUT", "/v1/limits/tenant-a", {"capacity": 10, "refill_per_second": 1})
        for cost in (1, 2, 1, 3):
            status, _, _ = self.request("POST", "/v1/check", {"key": "tenant-a", "cost": cost})
            self.assertEqual(status, 200)
        status, body, headers = self.request("GET", "/v1/reconciliations/tenant-a")
        self.assertEqual(status, 200)
        self.assertEqual(body, {"key": "tenant-a", "remaining": 3, "used": 7,
                                "accepted_count": 4, "accepted_cost": 7, "balanced": True})
        self.assertNotIn("ETag", headers)
        self.assertNotIn("Retry-After", headers)

    def test_same_instant_consistency_with_limits_and_ledger(self) -> None:
        clock = type(self).clock
        clock.t = 13000.0
        self.request("PUT", "/v1/limits/rec-1", {"capacity": 8, "refill_per_second": 1})
        self.request("POST", "/v1/check", {"key": "rec-1", "cost": 2})
        _, body_reservation, _ = self.request("POST", "/v1/reservations",
                                              {"key": "rec-1", "cost": 3, "ttl_seconds": 60})
        rid = body_reservation["reservation_id"]
        self.request("POST", f"/v1/reservations/{rid}/consume", {})  # used 5, tokens 3
        clock.t = 13003.0                                         # 3 tokens back: tokens 6
        status, reconciliation, rec_headers = self.request("GET", "/v1/reconciliations/rec-1")
        _, limits, limit_headers = self.request("GET", "/v1/limits/rec-1")
        _, ledger, _ = self.request("GET", "/v1/ledgers/rec-1")
        self.assertEqual(status, 200)
        self.assertEqual((reconciliation["remaining"], reconciliation["used"]),
                         (limits["remaining"], limits["used"]))
        self.assertEqual((reconciliation["accepted_count"], reconciliation["accepted_cost"]),
                         (ledger["totals"]["accepted_count"], ledger["totals"]["accepted_cost"]))
        self.assertEqual(reconciliation, {"key": "rec-1", "remaining": 6, "used": 5,
                                          "accepted_count": 2, "accepted_cost": 5, "balanced": True})
        self.assertNotIn("ETag", rec_headers)
        self.assertIn("ETag", limit_headers)                     # limits keeps its ETag

    def test_reconcile_settles_due_hold_without_booking_or_revision_change(self) -> None:
        clock = type(self).clock
        clock.t = 12000.0
        self.request("PUT", "/v1/limits/rec-2", {"capacity": 5, "refill_per_second": 0.0001})
        _, body, _ = self.request("POST", "/v1/reservations",
                                  {"key": "rec-2", "cost": 3, "ttl_seconds": 10})
        rid = body["reservation_id"]
        _, before, etag_before = self.request("GET", "/v1/limits/rec-2")
        self.assertEqual((before["remaining"], before["used"]), (2, 0))
        clock.t = 12010.0
        for _ in range(3):
            status, reconciliation, headers = self.request("GET", "/v1/reconciliations/rec-2")
            self.assertEqual(status, 200)
            self.assertEqual((reconciliation["remaining"], reconciliation["used"],
                              reconciliation["accepted_cost"], reconciliation["balanced"]),
                             (5, 0, 0, True))
            self.assertNotIn("ETag", headers)
        _, ledger, _ = self.request("GET", "/v1/ledgers/rec-2")
        self.assertEqual(ledger["totals"], {"accepted_count": 0, "accepted_cost": 0})
        _, after, etag_after = self.request("GET", "/v1/limits/rec-2")
        self.assertEqual(etag_before["ETag"], etag_after["ETag"])   # revision never advanced
        self.assertEqual((after["remaining"], after["used"]), (5, 0))
        self.assertEqual(self.request("DELETE", f"/v1/reservations/{rid}")[0], 404)

    def test_if_match_and_idempotency_key_headers_are_ignored(self) -> None:
        self.request("PUT", "/v1/limits/rec-3", {"capacity": 3, "refill_per_second": 1})
        for headers in ({"If-Match": '"999"'}, {"If-Match": '"1"'},
                        {"Idempotency-Key": "any-value"},
                        {"If-Match": '"1"', "Idempotency-Key": "any-value"}):
            status, body, response_headers = self.raw_request(
                "GET", "/v1/reconciliations/rec-3", headers=headers)
            self.assertEqual(status, 200, headers)
            self.assertEqual(body["balanced"], True)
            self.assertNotIn("ETag", response_headers)

    def test_query_string_is_400_even_for_an_unconfigured_key(self) -> None:
        for path in ["/v1/reconciliations/rec-4?x=1",
                     "/v1/reconciliations/rec-4?events=1",
                     "/v1/reconciliations/rec-4?=",
                     "/v1/reconciliations/rec-4?a=b&c=d",
                     "/v1/reconciliations/rec-4?%20"]:
            status, body, _ = self.request("GET", path)
            self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"), path)
        # A bare separator is an empty query string, exactly as on metrics/leaky-buckets.
        self.request("PUT", "/v1/limits/rec-4", {"capacity": 1, "refill_per_second": 1})
        self.assertEqual(self.request("GET", "/v1/reconciliations/rec-4?")[0], 200)
        # Query validation beats the key's 404.
        status, body, _ = self.request("GET", "/v1/reconciliations/missing?bogus=1")
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))

    def test_invalid_key_is_400_and_unknown_key_is_404(self) -> None:
        status, body, _ = self.request("GET", f"/v1/reconciliations/{'k' * 201}")
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        self.assertIn("message", body["error"])
        status, body, _ = self.request("GET", "/v1/reconciliations/no-such-key")
        self.assertEqual(status, 404)
        self.assertEqual(body["error"]["code"], "not_found")
        self.assertIn("message", body["error"])

    def test_bad_routes_extra_segments_and_methods_are_404(self) -> None:
        self.request("PUT", "/v1/limits/rec-5", {"capacity": 2, "refill_per_second": 1})
        for path in ["/v1/reconciliations", "/v1/reconciliations/rec-5/extra",
                     "//v1/reconciliations/rec-5", "/v1//reconciliations/rec-5",
                     "/v1/reconciliations//rec-5", "/v1/reconciliations/rec-5/",
                     "/v1/reconciliation/rec-5"]:
            status, body, _ = self.raw_request("GET", path)
            self.assertEqual((status, body["error"]["code"]), (404, "not_found"), path)
        # A query string never rescues a path mismatch.
        status, _, _ = self.request("GET", "/v1/reconciliations?x=1")
        self.assertEqual(status, 404)
        status, _, _ = self.request("GET", "/v1/reconciliations/rec-5/extra?x=1")
        self.assertEqual(status, 404)
        # Every non-GET verb is 404 on the route, even with a body.
        for method, payload in [("POST", b"{}"), ("PUT", b"{}"), ("DELETE", None),
                                ("PATCH", b"{}")]:
            status, body, _ = self.raw_request(method, "/v1/reconciliations/rec-5", payload)
            self.assertEqual((status, body["error"]["code"]), (404, "not_found"), method)

    def test_stalled_and_regressed_clock_over_http(self) -> None:
        clock = type(self).clock
        clock.t = 16000.0
        self.request("PUT", "/v1/limits/rec-6", {"capacity": 10, "refill_per_second": 1})
        self.request("POST", "/v1/check", {"key": "rec-6", "cost": 10})
        first_status, first, _ = self.request("GET", "/v1/reconciliations/rec-6")
        self.assertEqual((first_status, first["remaining"]), (200, 0))
        for _ in range(3):                                       # clock stalls
            status, body, _ = self.request("GET", "/v1/reconciliations/rec-6")
            self.assertEqual((status, body), (200, first))
        clock.t = 15990.0                                        # regression: no refill
        status, body, _ = self.request("GET", "/v1/reconciliations/rec-6")
        self.assertEqual(body["remaining"], 0)
        clock.t = 16002.0                                        # only 16000→16002 counts
        status, body, _ = self.request("GET", "/v1/reconciliations/rec-6")
        self.assertEqual((status, body["remaining"], body["used"]), (200, 2, 10))
        self.assertEqual(body["balanced"], True)

    def test_concurrent_http_reads_remain_consistent_with_mutations(self) -> None:
        clock = type(self).clock
        clock.t = 11000.0
        self.request("PUT", "/v1/limits/rec-hot", {"capacity": 100, "refill_per_second": 0.0001})
        errors: list[BaseException] = []
        lock = threading.Lock()

        def spend() -> None:
            # POST /v1/check is not idempotent, so a connect-level reset must NOT be retried
            # (a retried spend could book twice): count only responses the server clearly gave.
            try:
                status, _, _ = self.request("POST", "/v1/check", {"key": "rec-hot", "cost": 1})
                if status not in (200, 429):
                    raise AssertionError(f"unexpected check status {status}")
            except (ConnectionError, OSError, urllib.error.URLError):
                pass
            except BaseException as error:  # noqa: BLE001
                with lock:
                    errors.append(error)

        def read_many() -> None:
            try:
                for _ in range(40):
                    status, body, headers = self.request_retry("GET", "/v1/reconciliations/rec-hot")
                    if status != 200:
                        raise AssertionError(f"reconcile failed: {status} {body}")
                    if not body["balanced"] or body["used"] != body["accepted_cost"]:
                        raise AssertionError(f"unbalanced HTTP snapshot: {body}")
                    if body["remaining"] + body["used"] > 100:
                        raise AssertionError(f"tokens beyond capacity: {body}")
                    if "ETag" in headers:
                        raise AssertionError("reconciliation must not carry an ETag")
            except BaseException as error:  # noqa: BLE001
                with lock:
                    errors.append(error)

        threads = [threading.Thread(target=spend) for _ in range(150)]
        threads += [threading.Thread(target=read_many) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])

        _, reconciliation, _ = self.request("GET", "/v1/reconciliations/rec-hot")
        _, limits, _ = self.request("GET", "/v1/limits/rec-hot")
        _, ledger, _ = self.request("GET", "/v1/ledgers/rec-hot")
        self.assertEqual((reconciliation["remaining"], reconciliation["used"]),
                         (limits["remaining"], limits["used"]))
        self.assertEqual((reconciliation["accepted_count"], reconciliation["accepted_cost"]),
                         (ledger["totals"]["accepted_count"], ledger["totals"]["accepted_cost"]))
        # Every accepted spend costs 1 and takes 1 token from a 100-capacity bucket that never
        # refills: remaining + used must be exactly 100, never more (no oversell, no over-count).
        self.assertEqual(reconciliation["remaining"] + reconciliation["used"], 100)
        self.assertTrue(reconciliation["balanced"])


if __name__ == "__main__":
    unittest.main()
