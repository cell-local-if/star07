"""Reconciliation endpoint tests: GET /v1/reconciliations/{key}.

The read joins the token bucket's live state with the ledger's full-history totals in one
locked snapshot at one effective moment, and books nothing itself.
"""
from __future__ import annotations

import json
import threading
import unittest
import urllib.error
import urllib.request

from quota import InvalidRequest, LimitNotFound, Limiter


class Clock:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t


class ReconciliationUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.limiter = Limiter(self.clock)
        self.limiter.configure("tenant-a", {"capacity": 10, "refill_per_second": 1.0})

    def test_shape_and_same_moment_consistency_with_state_and_ledger(self) -> None:
        self.limiter.check("tenant-a", 3)
        self.limiter.check("tenant-a", 4)
        result = self.limiter.reconciliation("tenant-a")
        self.assertEqual(result, {"key": "tenant-a", "remaining": 3, "used": 7,
                                  "accepted_count": 2, "accepted_cost": 7, "balanced": True})
        state = self.limiter.state("tenant-a")            # same frozen moment
        self.assertEqual((result["remaining"], result["used"]),
                         (state["remaining"], state["used"]))
        totals = self.limiter.ledger("tenant-a")["totals"]
        self.assertEqual((result["accepted_count"], result["accepted_cost"]),
                         (totals["accepted_count"], totals["accepted_cost"]))

    def test_spec_example_scenario(self) -> None:
        limiter = Limiter(self.clock)
        limiter.configure("tenant-a", {"capacity": 10, "refill_per_second": 0.0001})
        for cost in (1, 2, 1, 3):                          # four accepted spends, cost 7
            limiter.check("tenant-a", cost)
        self.assertEqual(limiter.reconciliation("tenant-a"),
                         {"key": "tenant-a", "remaining": 3, "used": 7,
                          "accepted_count": 4, "accepted_cost": 7, "balanced": True})

    def test_holds_and_all_booking_sources_are_reconciled(self) -> None:
        self.limiter.configure("tenant-b", {"capacity": 10, "refill_per_second": 0.0001})
        self.limiter.check("tenant-b", 1)                                # check
        self.limiter.hierarchy_check(["tenant-a", "tenant-b"], 2)        # hierarchy_check
        reservation = self.limiter.reserve("tenant-b", 3, ttl_seconds=60)
        mid = self.limiter.reconciliation("tenant-b")
        self.assertEqual(mid["used"], 3)            # the live hold is not booked yet
        self.assertEqual(mid["accepted_count"], 2)
        self.assertEqual(mid["remaining"], 4)       # 10 - 1 - 2 - 3 held
        self.assertTrue(mid["balanced"])
        self.limiter.consume(reservation["reservation_id"])              # reservation_consume
        result = self.limiter.reconciliation("tenant-b")
        self.assertEqual((result["used"], result["accepted_count"], result["accepted_cost"]),
                         (6, 3, 6))
        self.assertEqual(result["remaining"], 4)    # consume moves no tokens
        self.assertTrue(result["balanced"])

    def test_hierarchy_spend_is_counted_on_every_layer(self) -> None:
        self.limiter.configure("tenant-c", {"capacity": 5, "refill_per_second": 0.0001})
        self.limiter.hierarchy_check(["tenant-a", "tenant-c"], 2)
        for key, capacity in (("tenant-a", 10), ("tenant-c", 5)):
            result = self.limiter.reconciliation(key)
            self.assertEqual((result["used"], result["accepted_count"],
                              result["accepted_cost"]), (2, 1, 2))
            self.assertEqual(result["remaining"], capacity - 2)
            self.assertTrue(result["balanced"])

    def test_due_reservation_is_settled_once_by_the_read(self) -> None:
        limiter = Limiter(self.clock)
        limiter.configure("frozen", {"capacity": 5, "refill_per_second": 0.0001})
        limiter.check("frozen", 2)                                # tokens 3, used 2
        limiter.reserve("frozen", 2, ttl_seconds=5)               # tokens 1
        self.clock.t += 5                                         # hold is now due
        first = limiter.reconciliation("frozen")                  # the read performs the refund
        self.assertEqual((first["remaining"], first["used"]), (3, 2))
        second = limiter.reconciliation("frozen")                 # same moment: no second refund
        self.assertEqual(second, first)
        self.assertEqual(limiter.state("frozen")["remaining"], 3)

    def test_read_books_nothing_and_counts_nothing(self) -> None:
        self.limiter.check("tenant-a", 2)
        _, revision_before = self.limiter.state_snapshot("tenant-a")
        ledger_before = self.limiter.ledger("tenant-a", 1000)
        metrics_before = self.limiter.metrics()
        reservations_before = dict(self.limiter._reservations)
        self.limiter.reconciliation("tenant-a")
        _, revision_after = self.limiter.state_snapshot("tenant-a")
        self.assertEqual(revision_after, revision_before)         # no revision/ETag drift
        self.assertEqual(self.limiter.ledger("tenant-a", 1000), ledger_before)  # no new event
        self.assertEqual(self.limiter.metrics(), metrics_before)  # no decision counted
        self.assertEqual(self.limiter._reservations, reservations_before)

    def test_stalled_clock_keeps_the_result_stable(self) -> None:
        self.limiter.check("tenant-a", 4)
        first = self.limiter.reconciliation("tenant-a")
        for _ in range(3):                                        # clock never advances
            self.assertEqual(self.limiter.reconciliation("tenant-a"), first)

    def test_regression_is_clamped_and_recovery_never_recounts(self) -> None:
        self.clock.t = 100.0
        limiter = Limiter(self.clock)
        limiter.configure("k", {"capacity": 10, "refill_per_second": 1.0})
        limiter.check("k", 10)                                    # empty at t=100
        self.clock.t = 90.0                                       # clock jumps backwards
        result = limiter.reconciliation("k")                      # treated as still t=100
        self.assertEqual((result["remaining"], result["used"]), (0, 10))
        self.clock.t = 101.0                                      # only 100→101 may count
        result = limiter.reconciliation("k")
        self.assertEqual(result["remaining"], 1)
        self.assertTrue(result["balanced"])

    def test_unconfigured_key_is_not_found_and_invalid_key_is_400(self) -> None:
        with self.assertRaises(LimitNotFound):
            self.limiter.reconciliation("absent")
        for bad_key in ["", "x" * 201, None, 5, True]:
            with self.assertRaises(InvalidRequest):
                self.limiter.reconciliation(bad_key)

    def test_concurrent_reconciliations_and_mutations_stay_internally_consistent(self) -> None:
        limiter = Limiter(self.clock)                             # clock frozen for the test
        limiter.configure("hot", {"capacity": 400, "refill_per_second": 0.0001})
        rids = [limiter.reserve("hot", 1, ttl_seconds=3600)["reservation_id"] for _ in range(100)]
        errors: list[BaseException] = []
        reports: list[dict] = []
        list_lock = threading.Lock()

        def mutate(index: int) -> None:
            try:
                if index % 3 == 0:
                    limiter.check("hot", 1)
                elif index % 3 == 1:
                    limiter.consume(rids[index % len(rids)])
                else:
                    limiter.rollback(rids[index % len(rids)])
            except LimitNotFound:
                pass
            except BaseException as error:  # noqa: BLE001 - surface thread failures here
                with list_lock:
                    errors.append(error)

        def reconcile() -> None:
            try:
                for _ in range(50):
                    report = limiter.reconciliation("hot")
                    if report["used"] != report["accepted_cost"] or not report["balanced"]:
                        raise AssertionError(f"unbalanced snapshot: {report}")
                    if report["accepted_count"] < 0 or report["remaining"] < 0:
                        raise AssertionError(f"impossible snapshot: {report}")
                    with list_lock:
                        reports.append(report)
            except BaseException as error:  # noqa: BLE001
                with list_lock:
                    errors.append(error)

        threads = ([threading.Thread(target=mutate, args=(index,)) for index in range(300)]
                   + [threading.Thread(target=reconcile) for _ in range(8)])
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        self.assertTrue(reports)
        # accepted_count never goes backwards within any single reader's stream.
        final = limiter.reconciliation("hot")
        state = limiter.state("hot")
        totals = limiter.ledger("hot", 1000)["totals"]
        self.assertEqual((final["remaining"], final["used"]), (state["remaining"], state["used"]))
        self.assertEqual((final["accepted_count"], final["accepted_cost"]),
                         (totals["accepted_count"], totals["accepted_cost"]))


class ReconciliationHttpTests(unittest.TestCase):
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
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data,
                                         method=method,
                                         headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read() or b"{}"), dict(response.headers)
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read() or b"{}"), dict(error.headers)

    def test_success_matches_limits_and_ledgers_at_the_same_moment(self) -> None:
        type(self).clock.t = 3000.0
        self.request("PUT", "/v1/limits/rec-1", {"capacity": 10, "refill_per_second": 1})
        self.request("POST", "/v1/check", {"key": "rec-1", "cost": 4})
        _, body, _ = self.request("POST", "/v1/reservations", {"key": "rec-1", "cost": 3})
        self.request("POST", f"/v1/reservations/{body['reservation_id']}/consume", {})
        status, report, headers = self.request("GET", "/v1/reconciliations/rec-1")
        self.assertEqual(status, 200)
        self.assertEqual(report, {"key": "rec-1", "remaining": 3, "used": 7,
                                  "accepted_count": 2, "accepted_cost": 7, "balanced": True})
        self.assertNotIn("ETag", headers)                          # no configuration is named
        _, state, _ = self.request("GET", "/v1/limits/rec-1")      # same frozen moment
        self.assertEqual((report["remaining"], report["used"]),
                         (state["remaining"], state["used"]))
        _, ledger, _ = self.request("GET", "/v1/ledgers/rec-1")
        self.assertEqual((report["accepted_count"], report["accepted_cost"]),
                         (ledger["totals"]["accepted_count"], ledger["totals"]["accepted_cost"]))

    def test_read_changes_no_revision_etag_or_decision_counts(self) -> None:
        type(self).clock.t = 3100.0
        self.request("PUT", "/v1/limits/rec-2", {"capacity": 5, "refill_per_second": 1})
        self.request("POST", "/v1/check", {"key": "rec-2", "cost": 2})
        _, _, before_headers = self.request("GET", "/v1/limits/rec-2")
        _, metrics_before, _ = self.request("GET", "/v1/metrics")
        _, ledger_before, _ = self.request("GET", "/v1/ledgers/rec-2")
        self.assertEqual(self.request("GET", "/v1/reconciliations/rec-2")[0], 200)
        _, _, after_headers = self.request("GET", "/v1/limits/rec-2")
        self.assertEqual(after_headers["ETag"], before_headers["ETag"])
        _, metrics_after, _ = self.request("GET", "/v1/metrics")
        self.assertEqual(metrics_after, metrics_before)
        _, ledger_after, _ = self.request("GET", "/v1/ledgers/rec-2")
        self.assertEqual(ledger_after, ledger_before)

    def test_query_string_is_400_with_error_shape(self) -> None:
        self.request("PUT", "/v1/limits/rec-3", {"capacity": 5, "refill_per_second": 1})
        for path in ["/v1/reconciliations/rec-3?events=1", "/v1/reconciliations/rec-3?x",
                     "/v1/reconciliations/rec-3?events="]:
            status, body, _ = self.request("GET", path)
            self.assertEqual(status, 400, path)
            self.assertEqual(body["error"]["code"], "invalid_request")
            self.assertIn("message", body["error"])
        # A bare "?" carries an empty query string and, as on /v1/metrics, is no parameter at all.
        self.assertEqual(self.request("GET", "/v1/reconciliations/rec-3?")[0], 200)

    def test_invalid_key_is_400_and_unconfigured_key_is_404(self) -> None:
        status, body, _ = self.request("GET", f"/v1/reconciliations/{'x' * 201}")
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        status, body, _ = self.request("GET", "/v1/reconciliations/never-configured")
        self.assertEqual((status, body["error"]["code"]), (404, "not_found"))
        self.assertIn("message", body["error"])

    def test_bad_paths_and_methods_are_404(self) -> None:
        self.request("PUT", "/v1/limits/rec-4", {"capacity": 5, "refill_per_second": 1})
        for path in ["/v1/reconciliations", "/v1/reconciliations/rec-4/extra",
                     "/v1/reconciliations/rec-4/", "/v1/reconciliations//",
                     "//v1/reconciliations/rec-4"]:
            self.assertEqual(self.request("GET", path)[0], 404, path)
        for method in ["POST", "PUT", "DELETE", "PATCH"]:
            self.assertEqual(self.request(method, "/v1/reconciliations/rec-4", {})[0], 404,
                             method)

    def test_clock_regression_via_http_keeps_watermark_semantics(self) -> None:
        clock = type(self).clock
        clock.t = 4000.0
        self.request("PUT", "/v1/limits/rec-5", {"capacity": 10, "refill_per_second": 1})
        self.request("POST", "/v1/check", {"key": "rec-5", "cost": 10})
        clock.t = 3990.0                                            # regressed: still t=4000
        _, report, _ = self.request("GET", "/v1/reconciliations/rec-5")
        self.assertEqual((report["remaining"], report["used"]), (0, 10))
        clock.t = 4002.0                                            # only 4000→4002 counts
        _, report, _ = self.request("GET", "/v1/reconciliations/rec-5")
        self.assertEqual(report["remaining"], 2)
        self.assertTrue(report["balanced"])

    def test_expiry_settles_through_the_endpoint_exactly_once(self) -> None:
        clock = type(self).clock
        clock.t = 5000.0
        self.request("PUT", "/v1/limits/rec-6", {"capacity": 5, "refill_per_second": 0.0001})
        self.request("POST", "/v1/check", {"key": "rec-6", "cost": 2})
        self.request("POST", "/v1/reservations", {"key": "rec-6", "cost": 2, "ttl_seconds": 5})
        clock.t += 5                                                # hold is now due
        _, first, _ = self.request("GET", "/v1/reconciliations/rec-6")
        self.assertEqual((first["remaining"], first["used"]), (3, 2))
        _, second, _ = self.request("GET", "/v1/reconciliations/rec-6")
        self.assertEqual(second, first)                             # no double refund
        _, state, _ = self.request("GET", "/v1/limits/rec-6")
        self.assertEqual((state["remaining"], state["used"]), (3, 2))


if __name__ == "__main__":
    unittest.main()
