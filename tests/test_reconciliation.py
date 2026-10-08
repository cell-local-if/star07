"""GET /v1/ledgers/{key}/reconciliation: cross-check booked usage, ledger totals, retained vs
trimmed event detail and unconfirmed holds for one token-bucket key."""
from __future__ import annotations

import json
import threading
import unittest
import urllib.error
import urllib.request

from quota import LEDGER_EVENT_KEEP, InvalidRequest, Limiter, LimitNotFound


class Clock:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t


class ReconciliationUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.limiter = Limiter(self.clock)
        self.limiter.configure("k", {"capacity": 1_000_000, "refill_per_second": 0.0001})

    def test_fresh_key_is_trivially_reconciled(self) -> None:
        report = self.limiter.reconciliation("k")
        self.assertEqual(set(report), {"key", "reconciled", "usage", "holds", "events"})
        self.assertEqual(report["key"], "k")
        self.assertTrue(report["reconciled"])
        self.assertEqual(report["usage"], {"used": 0, "ledger_accepted_count": 0,
                                           "ledger_accepted_cost": 0,
                                           "used_minus_ledger_cost": 0})
        self.assertEqual(report["holds"], {"active_count": 0, "active_cost": 0,
                                           "single_key_count": 0, "hierarchy_count": 0})
        # Empty detail: both seqs are null.
        self.assertEqual(report["events"], {"retained_count": 0, "retained_cost": 0,
                                            "trimmed_count": 0, "trimmed_cost": 0,
                                            "first_seq": None, "last_seq": None})

    def test_mixed_sources_reconcile(self) -> None:
        self.limiter.check("k", 3)
        reservation = self.limiter.reserve("k", 2)
        self.limiter.consume(reservation["reservation_id"])
        self.limiter.configure("parent", {"capacity": 1_000_000, "refill_per_second": 0.0001})
        self.limiter.hierarchy_check(["parent", "k"], 4)
        hierarchy_hold = self.limiter.hierarchy_reserve(["parent", "k"], 5)
        self.limiter.hierarchy_consume(hierarchy_hold["reservation_id"])
        report = self.limiter.reconciliation("k")
        self.assertTrue(report["reconciled"])
        self.assertEqual(report["usage"], {"used": 14, "ledger_accepted_count": 4,
                                           "ledger_accepted_cost": 14,
                                           "used_minus_ledger_cost": 0})
        self.assertEqual(report["events"]["retained_count"], 4)
        self.assertEqual(report["events"]["retained_cost"], 14)
        self.assertEqual(report["events"]["trimmed_count"], 0)
        self.assertEqual(report["events"]["trimmed_cost"], 0)
        self.assertEqual(report["events"]["first_seq"], 1)
        self.assertEqual(report["events"]["last_seq"], 4)

    def test_trimming_is_reported_and_still_reconciles(self) -> None:
        for _ in range(LEDGER_EVENT_KEEP + 25):
            self.limiter.check("k", 1)
        report = self.limiter.reconciliation("k")
        self.assertTrue(report["reconciled"])
        events = report["events"]
        self.assertEqual(events["retained_count"], LEDGER_EVENT_KEEP)
        self.assertEqual(events["retained_cost"], LEDGER_EVENT_KEEP)
        self.assertEqual(events["trimmed_count"], 25)
        self.assertEqual(events["trimmed_cost"], 25)
        self.assertEqual(events["first_seq"], 26)
        self.assertEqual(events["last_seq"], LEDGER_EVENT_KEEP + 25)
        self.assertEqual(report["usage"]["ledger_accepted_count"], LEDGER_EVENT_KEEP + 25)
        self.assertEqual(report["usage"]["used"], LEDGER_EVENT_KEEP + 25)

    def test_live_holds_are_counted_once_each(self) -> None:
        self.limiter.reserve("k", 3, ttl_seconds=60)
        self.limiter.reserve("k", 4, ttl_seconds=60)
        self.limiter.configure("parent", {"capacity": 1_000_000, "refill_per_second": 0.0001})
        self.limiter.hierarchy_reserve(["parent", "k"], 5)
        report = self.limiter.reconciliation("k")
        self.assertEqual(report["holds"], {"active_count": 3, "active_cost": 12,
                                           "single_key_count": 2, "hierarchy_count": 1})
        # The cross-layer hold touches two layers but its cost is counted once per key.
        parent_report = self.limiter.reconciliation("parent")
        self.assertEqual(parent_report["holds"], {"active_count": 1, "active_cost": 5,
                                                  "single_key_count": 0, "hierarchy_count": 1})
        # Holds are not usage: the ledger side is untouched and the key still reconciles.
        self.assertTrue(report["reconciled"])
        self.assertEqual(report["usage"]["used"], 0)

    def test_due_holds_settle_at_the_inclusive_boundary(self) -> None:
        reservation = self.limiter.reserve("k", 7, ttl_seconds=10)
        self.clock.t += 10  # exactly created_at + ttl: the boundary is inclusive
        report = self.limiter.reconciliation("k")
        self.assertEqual(report["holds"], {"active_count": 0, "active_cost": 0,
                                           "single_key_count": 0, "hierarchy_count": 0})
        # The settle released the tokens...
        self.assertEqual(self.limiter.state("k")["remaining"], 1_000_000)
        # ...but booked nothing: no used, no ledger event, no decision count.
        self.assertEqual(report["usage"]["used"], 0)
        self.assertEqual(report["usage"]["ledger_accepted_count"], 0)
        self.assertTrue(report["reconciled"])
        self.assertEqual(self.limiter.metrics()["metrics"]["decisions"]["reservation"],
                         {"allowed": 1, "over_quota": 0})
        # The settled hold is gone for good.
        with self.assertRaises(LimitNotFound):
            self.limiter.rollback(reservation["reservation_id"])

    def test_reconciliation_books_nothing_and_counts_nothing(self) -> None:
        self.limiter.check("k", 5)
        before_ledger = self.limiter.ledger("k", LEDGER_EVENT_KEEP)
        before_metrics = self.limiter.metrics()
        before_used = self.limiter.state("k")["used"]
        self.limiter.reconciliation("k")
        self.assertEqual(self.limiter.ledger("k", LEDGER_EVENT_KEEP), before_ledger)
        self.assertEqual(self.limiter.metrics(), before_metrics)
        self.assertEqual(self.limiter.state("k")["used"], before_used)

    def test_used_ledger_divergence_is_reported_not_repaired(self) -> None:
        self.limiter.check("k", 5)
        self.limiter._buckets["k"].used += 2  # drift between the bucket and the ledger
        report = self.limiter.reconciliation("k")
        self.assertFalse(report["reconciled"])
        self.assertEqual(report["usage"]["used"], 7)
        self.assertEqual(report["usage"]["ledger_accepted_cost"], 5)
        self.assertEqual(report["usage"]["used_minus_ledger_cost"], 2)
        # No back-writing: the drift survives the read.
        self.assertEqual(self.limiter.state("k")["used"], 7)
        self.assertEqual(self.limiter.ledger("k")["totals"]["accepted_cost"], 5)

    def test_seq_gap_breaks_reconciliation(self) -> None:
        for _ in range(5):
            self.limiter.check("k", 1)
        events = self.limiter._ledgers["k"].events
        del events[2]  # seqs now 1, 2, 4, 5: a broken chain
        report = self.limiter.reconciliation("k")
        self.assertFalse(report["reconciled"])
        self.assertEqual(report["events"]["retained_count"], 4)
        self.assertEqual(report["events"]["first_seq"], 1)
        self.assertEqual(report["events"]["last_seq"], 5)

    def test_fully_unreadable_booked_ledger_breaks_reconciliation(self) -> None:
        for _ in range(3):
            self.limiter.check("k", 1)
        self.limiter._ledgers["k"].events.clear()
        report = self.limiter.reconciliation("k")
        self.assertFalse(report["reconciled"])
        self.assertEqual(report["events"]["retained_count"], 0)
        self.assertIsNone(report["events"]["first_seq"])
        self.assertIsNone(report["events"]["last_seq"])
        self.assertEqual(report["events"]["trimmed_count"], 3)

    def test_windows_and_leaky_buckets_do_not_enter_reconciliation(self) -> None:
        self.limiter.configure_window("k", {"window_seconds": 60, "max_events": 10})
        self.limiter.window_check("k", 3)
        self.limiter.window_reserve("k", 2)
        self.limiter.configure_leaky_bucket("k", {"capacity": 10, "leak_per_second": 1.0})
        self.limiter.leaky_bucket_check("k", 4)
        report = self.limiter.reconciliation("k")
        self.assertTrue(report["reconciled"])
        self.assertEqual(report["usage"]["used"], 0)
        self.assertEqual(report["usage"]["ledger_accepted_count"], 0)
        self.assertEqual(report["holds"]["active_count"], 0)

    def test_unknown_key_is_404_and_creates_nothing(self) -> None:
        with self.assertRaises(LimitNotFound):
            self.limiter.reconciliation("unknown")
        with self.assertRaises(LimitNotFound):
            self.limiter.ledger("unknown")
        self.assertNotIn("unknown", self.limiter._ledgers)
        self.assertNotIn("unknown", self.limiter._buckets)

    def test_unknown_key_404_does_not_advance_the_watermark(self) -> None:
        limiter = Limiter(self.clock)
        limiter.configure("a", {"capacity": 10, "refill_per_second": 1.0})
        limiter.check("a", 10)
        self.clock.t = 1005.0
        with self.assertRaises(LimitNotFound):
            limiter.reconciliation("unknown")  # must not sample the clock
        self.clock.t = 1002.0  # regression below the 404-time reading
        # The watermark never left 1000.0: only 2 seconds of refill are visible.
        self.assertEqual(limiter.state("a")["remaining"], 2)

    def test_invalid_key_is_rejected_before_any_state(self) -> None:
        for bad in ("", "x" * 201, None, 7):
            with self.assertRaises(InvalidRequest):
                self.limiter.reconciliation(bad)

    def test_stalled_and_regressed_clock_settle_nothing_early(self) -> None:
        self.limiter.reserve("k", 5, ttl_seconds=10)
        self.clock.t -= 50  # regression: effective moment stays at the watermark
        report = self.limiter.reconciliation("k")
        self.assertEqual(report["holds"]["active_count"], 1)
        self.assertEqual(report["holds"]["active_cost"], 5)


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
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
                                         headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read() or b"{}"), dict(response.headers)
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read() or b"{}"), dict(error.headers)

    def test_http_happy_path_shape_and_no_etag(self) -> None:
        self.assertEqual(self.request("PUT", "/v1/limits/http-rec",
                                      {"capacity": 100, "refill_per_second": 1.0})[0], 200)
        self.assertEqual(self.request("POST", "/v1/check", {"key": "http-rec", "cost": 4})[0], 200)
        status, body, headers = self.request("GET", "/v1/ledgers/http-rec/reconciliation")
        self.assertEqual(status, 200)
        self.assertNotIn("ETag", headers)
        self.assertEqual(set(body), {"key", "reconciled", "usage", "holds", "events"})
        self.assertTrue(body["reconciled"])
        self.assertEqual(set(body["usage"]),
                         {"used", "ledger_accepted_count", "ledger_accepted_cost",
                          "used_minus_ledger_cost"})
        self.assertEqual(set(body["holds"]),
                         {"active_count", "active_cost", "single_key_count", "hierarchy_count"})
        self.assertEqual(set(body["events"]),
                         {"retained_count", "retained_cost", "trimmed_count", "trimmed_cost",
                          "first_seq", "last_seq"})
        self.assertEqual(body["usage"]["used"], 4)
        self.assertEqual(body["usage"]["ledger_accepted_cost"], 4)
        self.assertEqual(body["events"]["first_seq"], 1)
        self.assertEqual(body["events"]["last_seq"], 1)

    def test_http_any_query_parameter_is_400(self) -> None:
        self.assertEqual(self.request("PUT", "/v1/limits/http-q",
                                      {"capacity": 100, "refill_per_second": 1.0})[0], 200)
        for query in ("events=10", "x=1", "events=1&events=2", "events="):
            status, body, _ = self.request(
                "GET", f"/v1/ledgers/http-q/reconciliation?{query}")
            self.assertEqual(status, 400, query)
            self.assertEqual(body["error"]["code"], "invalid_request")
        # State untouched: the plain route still reconciles.
        status, body, _ = self.request("GET", "/v1/ledgers/http-q/reconciliation")
        self.assertEqual(status, 200)
        self.assertTrue(body["reconciled"])

    def test_http_invalid_key_is_400(self) -> None:
        status, body, _ = self.request("GET", f"/v1/ledgers/{'x' * 201}/reconciliation")
        self.assertEqual(status, 400)
        self.assertEqual(body["error"]["code"], "invalid_request")

    def test_http_unknown_key_is_404_and_creates_nothing(self) -> None:
        status, body, _ = self.request("GET", "/v1/ledgers/http-unknown/reconciliation")
        self.assertEqual(status, 404)
        self.assertEqual(body["error"]["code"], "not_found")
        # No object was created: the ledger route still 404s.
        self.assertEqual(self.request("GET", "/v1/ledgers/http-unknown")[0], 404)
        self.assertEqual(self.request("GET", "/v1/limits/http-unknown")[0], 404)

    def test_http_path_and_method_mismatches_are_404(self) -> None:
        self.assertEqual(self.request("PUT", "/v1/limits/http-mm",
                                      {"capacity": 100, "refill_per_second": 1.0})[0], 200)
        self.assertEqual(
            self.request("GET", "/v1/ledgers/http-mm/reconciliation/extra")[0], 404)
        self.assertEqual(self.request("GET", "/v1/ledgers/http-mm/reconciliatio")[0], 404)
        for method in ("POST", "PUT", "DELETE", "PATCH"):
            self.assertEqual(
                self.request(method, "/v1/ledgers/http-mm/reconciliation")[0], 404, method)

    def test_http_settles_due_holds_without_booking(self) -> None:
        self.assertEqual(self.request("PUT", "/v1/limits/http-exp",
                                      {"capacity": 50, "refill_per_second": 0.0001})[0], 200)
        status, reservation, _ = self.request(
            "POST", "/v1/reservations", {"key": "http-exp", "cost": 20, "ttl_seconds": 10})
        self.assertEqual(status, 200)
        self.clock.t += 10
        status, report, _ = self.request("GET", "/v1/ledgers/http-exp/reconciliation")
        self.assertEqual(status, 200)
        self.assertEqual(report["holds"]["active_count"], 0)
        self.assertEqual(report["usage"]["used"], 0)
        self.assertTrue(report["reconciled"])
        # Tokens were released...
        self.assertEqual(self.request("GET", "/v1/limits/http-exp")[1]["remaining"], 50)
        # ...and the settled hold is unknown to the rollback route.
        self.assertEqual(
            self.request("DELETE", f"/v1/reservations/{reservation['reservation_id']}")[0], 404)
        # Metrics were not touched by the settle.
        _, metrics, _ = self.request("GET", "/v1/metrics")
        self.assertEqual(metrics["metrics"]["decisions"]["reservation"],
                         {"allowed": 1, "over_quota": 0})

    def test_http_ledger_events_behavior_unchanged(self) -> None:
        self.assertEqual(self.request("PUT", "/v1/limits/http-led",
                                      {"capacity": 100, "refill_per_second": 1.0})[0], 200)
        for _ in range(3):
            self.assertEqual(
                self.request("POST", "/v1/check", {"key": "http-led", "cost": 1})[0], 200)
        status, ledger, _ = self.request("GET", "/v1/ledgers/http-led?events=2")
        self.assertEqual(status, 200)
        self.assertEqual([event["seq"] for event in ledger["events"]], [2, 3])
        self.assertEqual(ledger["totals"], {"accepted_count": 3, "accepted_cost": 3})
        # The reconciliation read did not disturb the ledger view.
        self.assertEqual(self.request("GET", "/v1/ledgers/http-led/reconciliation")[0], 200)
        status, again, _ = self.request("GET", "/v1/ledgers/http-led?events=2")
        self.assertEqual(again, ledger)


if __name__ == "__main__":
    unittest.main()
