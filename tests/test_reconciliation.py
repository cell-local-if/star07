"""GET /v1/ledgers/{key}/reconciliation: the read-only audit cross-checking booked usage,
ledger totals, retained/trimmed detail coverage and live unconfirmed holds from one snapshot."""
from __future__ import annotations

import json
import threading
import unittest
import urllib.error
import urllib.request

from quota import LEDGER_EVENT_KEEP, InvalidRequest, LimitNotFound, Limiter


class Clock:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t


class ReconciliationUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.limiter = Limiter(self.clock)

    def test_empty_configured_key_reconciles_with_null_seqs(self) -> None:
        self.limiter.configure("fresh", {"capacity": 10, "refill_per_second": 1.0})
        report = self.limiter.reconciliation("fresh")
        self.assertEqual(set(report), {"key", "reconciled", "usage", "holds", "events"})
        self.assertEqual(report["key"], "fresh")
        self.assertTrue(report["reconciled"])
        self.assertEqual(report["usage"], {
            "used": 0, "ledger_accepted_count": 0, "ledger_accepted_cost": 0,
            "used_minus_ledger_cost": 0})
        self.assertEqual(report["holds"], {
            "active_count": 0, "active_cost": 0, "single_key_count": 0, "hierarchy_count": 0})
        self.assertEqual(report["events"], {
            "retained_count": 0, "retained_cost": 0, "trimmed_count": 0, "trimmed_cost": 0,
            "first_seq": None, "last_seq": None})

    def test_booked_history_reconciles_and_reports_retained_tail(self) -> None:
        self.limiter.configure("k", {"capacity": 1_000_000, "refill_per_second": 0.0001})
        for _ in range(5):
            self.limiter.check("k", 2)
        hold = self.limiter.reserve("k", 3)
        self.limiter.consume(hold["reservation_id"])
        report = self.limiter.reconciliation("k")
        self.assertTrue(report["reconciled"])
        self.assertEqual(report["usage"], {
            "used": 13, "ledger_accepted_count": 6, "ledger_accepted_cost": 13,
            "used_minus_ledger_cost": 0})
        # The unconfirmed hold was consumed before the audit: nothing stays active.
        self.assertEqual(report["holds"]["active_count"], 0)
        self.assertEqual(report["events"], {
            "retained_count": 6, "retained_cost": 13, "trimmed_count": 0, "trimmed_cost": 0,
            "first_seq": 1, "last_seq": 6})

    def test_trimmed_prefix_is_derived_and_full_tail_is_audited(self) -> None:
        # Unlike GET /v1/ledgers/{key}'s default 100-event view, the audit counts the ENTIRE
        # readable tail (all LEDGER_EVENT_KEEP details), not just the default window.
        self.limiter.configure("hot", {"capacity": 1_000_000, "refill_per_second": 0.0001})
        for _ in range(LEDGER_EVENT_KEEP + 37):
            self.limiter.check("hot", 1)
        report = self.limiter.reconciliation("hot")
        self.assertTrue(report["reconciled"])
        self.assertEqual(report["events"]["retained_count"], LEDGER_EVENT_KEEP)
        self.assertEqual(report["events"]["retained_cost"], LEDGER_EVENT_KEEP)
        self.assertEqual(report["events"]["trimmed_count"], 37)
        self.assertEqual(report["events"]["trimmed_cost"], 37)
        self.assertEqual(report["events"]["first_seq"], 38)
        self.assertEqual(report["events"]["last_seq"], LEDGER_EVENT_KEEP + 37)
        # Retained plus trimmed rebuilds the accepted totals exactly.
        events = report["events"]
        self.assertEqual(events["retained_count"] + events["trimmed_count"],
                         report["usage"]["ledger_accepted_count"])
        self.assertEqual(events["retained_cost"] + events["trimmed_cost"],
                         report["usage"]["ledger_accepted_cost"])
        self.assertEqual(report["usage"]["used"], LEDGER_EVENT_KEEP + 37)

    def test_live_single_and_hierarchy_holds_count_once_per_touched_key(self) -> None:
        self.limiter.configure("parent", {"capacity": 1_000_000, "refill_per_second": 1.0})
        self.limiter.configure("leaf", {"capacity": 1_000_000, "refill_per_second": 1.0})
        self.limiter.configure("other", {"capacity": 1_000_000, "refill_per_second": 1.0})
        self.limiter.reserve("leaf", 5)
        self.limiter.reserve("leaf", 2)
        cross1 = self.limiter.hierarchy_reserve(["parent", "leaf"], 4)
        cross2 = self.limiter.hierarchy_reserve(["parent", "other"], 7)  # does not touch leaf
        report = self.limiter.reconciliation("leaf")
        self.assertTrue(report["reconciled"])
        self.assertEqual(report["holds"], {
            "active_count": 3, "active_cost": 5 + 2 + 4,
            "single_key_count": 2, "hierarchy_count": 1})
        # Each layer audit counts the one cross-layer hold exactly once, never per layer.
        parent_report = self.limiter.reconciliation("parent")
        self.assertEqual(parent_report["holds"], {
            "active_count": 2, "active_cost": 4 + 7,
            "single_key_count": 0, "hierarchy_count": 2})
        other_report = self.limiter.reconciliation("other")
        self.assertEqual(other_report["holds"]["hierarchy_count"], 1)
        self.assertEqual(other_report["holds"]["active_cost"], 7)
        # The holds hold no usage: used and the ledger stay at zero and still reconcile.
        self.assertEqual(report["usage"]["used"], 0)
        self.assertTrue(cross1["reservation_id"] != cross2["reservation_id"])

    def test_due_holds_settle_at_the_inclusive_boundary_before_the_snapshot(self) -> None:
        self.limiter.configure("k", {"capacity": 10, "refill_per_second": 0.0001})
        live = self.limiter.reserve("k", 6, ttl_seconds=10)
        due_at_boundary = self.limiter.reserve("k", 3, ttl_seconds=5)
        self.clock.t += 5  # exactly the boundary: the ttl=5 hold is due (<=)
        report = self.limiter.reconciliation("k")
        # The due hold is gone from the snapshot; only the ttl=10 hold survives. Settlement only
        # released the expired hold's tokens: 10 - 6 - 3 = 1, then +3 returned = 4.
        self.assertEqual(report["holds"], {
            "active_count": 1, "active_cost": 6,
            "single_key_count": 1, "hierarchy_count": 0})
        self.assertEqual(report["usage"]["used"], 0)
        self.assertEqual(report["usage"]["ledger_accepted_cost"], 0)
        self.assertTrue(report["reconciled"])
        self.assertEqual(self.limiter.state("k")["remaining"], 4)
        with self.assertRaises(LimitNotFound):
            self.limiter.consume(due_at_boundary["reservation_id"])
        # The live hold is still consumable after the audit settled its peer (created at 1000
        # with ttl 10 -> due only at 1010, so 1009 is still inside the hold).
        self.clock.t += 4
        consumed = self.limiter.consume(live["reservation_id"])
        self.assertTrue(consumed["consumed"])

    def test_due_hierarchy_hold_settles_on_every_layer_from_the_audit(self) -> None:
        self.limiter.configure("parent", {"capacity": 10, "refill_per_second": 0.0001})
        self.limiter.configure("leaf", {"capacity": 10, "refill_per_second": 0.0001})
        self.limiter.hierarchy_reserve(["parent", "leaf"], 4, ttl_seconds=5)
        self.clock.t += 5
        leaf_report = self.limiter.reconciliation("leaf")
        self.assertEqual(leaf_report["holds"]["hierarchy_count"], 0)
        self.assertEqual(leaf_report["holds"]["active_count"], 0)
        # Auditing one layer settled the whole cross-layer unit; the other layer sees it gone too.
        parent_report = self.limiter.reconciliation("parent")
        self.assertEqual(parent_report["holds"]["hierarchy_count"], 0)
        self.assertEqual(self.limiter.state("parent")["remaining"], 10)
        self.assertEqual(self.limiter.state("leaf")["remaining"], 10)

    def test_settlement_books_nothing_and_counts_no_metrics(self) -> None:
        self.limiter.configure("k", {"capacity": 100, "refill_per_second": 0.0001})
        self.limiter.check("k", 7)
        self.limiter.reserve("k", 5, ttl_seconds=10)
        decisions_before = json.loads(json.dumps(self.limiter.metrics()))
        self.clock.t += 10
        report1 = self.limiter.reconciliation("k")
        report2 = self.limiter.reconciliation("k")  # stalled clock: identical, nothing settles twice
        self.assertEqual(report1, report2)
        self.assertEqual(report1["usage"]["used"], 7)
        self.assertEqual(report1["usage"]["ledger_accepted_cost"], 7)
        self.assertEqual(report1["holds"]["active_count"], 0)
        self.assertEqual(self.limiter.metrics(), decisions_before)

    def test_window_and_window_holds_never_enter_the_audit(self) -> None:
        self.limiter.configure("k", {"capacity": 100, "refill_per_second": 1.0})
        self.limiter.configure_window("k", {"window_seconds": 60, "max_events": 10})
        self.limiter.window_check("k", 3)
        self.limiter.window_reserve("k", 2, ttl_seconds=60)
        report = self.limiter.reconciliation("k")
        self.assertTrue(report["reconciled"])
        self.assertEqual(report["holds"], {
            "active_count": 0, "active_cost": 0,
            "single_key_count": 0, "hierarchy_count": 0})
        self.assertEqual(report["usage"]["used"], 0)

    def test_clock_regression_uses_the_watermark_and_settles_nothing_early(self) -> None:
        self.limiter.configure("k", {"capacity": 10, "refill_per_second": 1.0})
        self.limiter.configure("other", {"capacity": 10, "refill_per_second": 1.0})
        self.limiter.reserve("k", 4, ttl_seconds=10)   # due at 1010
        self.clock.t += 10
        # Advance the shared watermark to the due moment WITHOUT touching k: an operation on
        # another key ticks the clock but settles only that key's reservations.
        self.assertTrue(self.limiter.check("other", 1)["allowed"])
        self.clock.t -= 999  # raw reading (11) is far before the hold's creation
        report = self.limiter.reconciliation("k")
        # The audit clamps to the watermark (1010), so the boundary holds and the hold settles;
        # a naive use of the regressed reading would wrongly keep it active and refund it late.
        self.assertEqual(report["holds"]["active_count"], 0)
        self.assertEqual(report["usage"]["used"], 0)
        self.assertTrue(report["reconciled"])

    def test_concurrent_audits_and_bookings_stay_consistent(self) -> None:
        limiter = Limiter(self.clock)                  # frozen clock, effectively no expiry
        limiter.configure("hot", {"capacity": 1_000_000, "refill_per_second": 0.0001})
        errors: list[BaseException] = []
        list_lock = threading.Lock()

        def spend() -> None:
            try:
                for _ in range(300):
                    limiter.check("hot", 1)
            except BaseException as error:  # noqa: BLE001
                with list_lock:
                    errors.append(error)

        def audit() -> None:
            try:
                for _ in range(300):
                    report = limiter.reconciliation("hot")
                    usage, events = report["usage"], report["events"]
                    if events["retained_count"] + events["trimmed_count"] \
                            != usage["ledger_accepted_count"]:
                        raise AssertionError("count coverage broken under concurrency")
                    if events["retained_cost"] + events["trimmed_cost"] \
                            != usage["ledger_accepted_cost"]:
                        raise AssertionError("cost coverage broken under concurrency")
                    if usage["used"] != usage["ledger_accepted_cost"]:
                        raise AssertionError("used drifted from the ledger under concurrency")
                    if events["last_seq"] is not None \
                            and events["last_seq"] != usage["ledger_accepted_count"]:
                        raise AssertionError("tail last_seq disagrees with accepted_count")
                    if not report["reconciled"]:
                        raise AssertionError(f"healthy ledger reported unreconciled: {report}")
            except BaseException as error:  # noqa: BLE001
                with list_lock:
                    errors.append(error)

        threads = [threading.Thread(target=spend) for _ in range(6)]
        threads += [threading.Thread(target=audit) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        report = limiter.reconciliation("hot")
        self.assertTrue(report["reconciled"])
        self.assertEqual(report["usage"]["ledger_accepted_count"], 1800)

    def test_used_ledger_mismatch_is_reported_not_repaired(self) -> None:
        self.limiter.configure("k", {"capacity": 1_000_000, "refill_per_second": 0.0001})
        for _ in range(3):
            self.limiter.check("k", 1)
        self.limiter._buckets["k"].used += 7  # damage: used drifts above the ledger total
        report = self.limiter.reconciliation("k")
        self.assertFalse(report["reconciled"])
        self.assertEqual(report["usage"]["used"], 10)
        self.assertEqual(report["usage"]["ledger_accepted_cost"], 3)
        self.assertEqual(report["usage"]["used_minus_ledger_cost"], 7)
        # Still 200-shaped data and no repair: the ledger is untouched.
        self.assertEqual(self.limiter.ledger("k", 1000)["totals"]["accepted_cost"], 3)

    def test_broken_seq_tail_fails_reconciliation_while_totals_stand(self) -> None:
        self.limiter.configure("k", {"capacity": 1_000_000, "refill_per_second": 0.0001})
        for _ in range(4):
            self.limiter.check("k", 1)
        # Punch a gap into the middle of the retained detail: totals still agree with used, but
        # the audit must detect the broken sequence rather than trusting counts alone.
        events = self.limiter._ledgers["k"].events
        events[2].__dict__["seq"] = 9
        report = self.limiter.reconciliation("k")
        self.assertFalse(report["reconciled"])
        self.assertEqual(report["usage"]["used_minus_ledger_cost"], 0)
        self.assertEqual(report["events"]["first_seq"], 1)
        self.assertEqual(report["events"]["last_seq"], 4)

    def test_last_seq_short_of_count_fails_reconciliation(self) -> None:
        self.limiter.configure("k", {"capacity": 1_000_000, "refill_per_second": 0.0001})
        for _ in range(4):
            self.limiter.check("k", 1)
        events = self.limiter._ledgers["k"].events
        events[-1].__dict__["seq"] = 2  # dense 1,2,3,2 would repeat; make it a short final seq
        report = self.limiter.reconciliation("k")
        self.assertFalse(report["reconciled"])

    def test_empty_detail_with_lifetime_totals_fails_with_null_seqs(self) -> None:
        self.limiter.configure("k", {"capacity": 1_000_000, "refill_per_second": 0.0001})
        for _ in range(3):
            self.limiter.check("k", 1)
        self.limiter._ledgers["k"].events.clear()  # damage: totals booked, detail gone
        report = self.limiter.reconciliation("k")
        self.assertFalse(report["reconciled"])
        self.assertEqual(report["events"]["retained_count"], 0)
        self.assertEqual(report["events"]["trimmed_count"], 3)
        self.assertIsNone(report["events"]["first_seq"])
        self.assertIsNone(report["events"]["last_seq"])

    def test_detail_cost_past_total_fails_coverage(self) -> None:
        self.limiter.configure("k", {"capacity": 1_000_000, "refill_per_second": 0.0001})
        self.limiter.check("k", 1)
        ledger = self.limiter._ledgers["k"]
        ledger.accepted_cost = 0  # damage: retained cost 1 can no longer be covered by totals
        report = self.limiter.reconciliation("k")
        self.assertFalse(report["reconciled"])
        self.assertEqual(report["events"]["retained_cost"], 1)
        self.assertEqual(report["events"]["trimmed_cost"], -1)

    def test_invalid_key_is_rejected_before_the_lock_and_changes_nothing(self) -> None:
        with self.assertRaises(InvalidRequest):
            self.limiter.reconciliation("")
        with self.assertRaises(InvalidRequest):
            self.limiter.reconciliation("x" * 201)
        with self.assertRaises(InvalidRequest):
            self.limiter.reconciliation(7)
        self.assertEqual(self.limiter._ledgers, {})

    def test_unconfigured_key_is_not_found_and_creates_nothing(self) -> None:
        with self.assertRaises(LimitNotFound):
            self.limiter.reconciliation("ghost")
        self.assertNotIn("ghost", self.limiter._buckets)
        self.assertNotIn("ghost", self.limiter._ledgers)
        self.assertNotIn("ghost", self.limiter._limits)


class ReconciliationHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from quota import serve

        cls.clock = Clock()
        cls.server = serve(port=0, now=cls.clock)
        cls.port = cls.server.server_address[1]
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()
        # Create every key the cases below use up front, so no case depends on execution order.
        def put_limit(key: str, capacity: int = 1_000_000, rate: float = 0.0001) -> None:
            request = urllib.request.Request(
                f"http://127.0.0.1:{cls.port}/v1/limits/{key}",
                data=json.dumps({"capacity": capacity, "refill_per_second": rate}).encode(),
                method="PUT", headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(request, timeout=5):
                pass

        put_limit("rec-k")
        put_limit("rec-ledger")
        put_limit("rec-empty", capacity=5, rate=1.0)
        put_limit("rec-q", capacity=5, rate=1.0)

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

    def test_http_report_shape_and_no_etag(self) -> None:
        self.request("POST", "/v1/check", {"key": "rec-k", "cost": 4})
        self.request("POST", "/v1/check", {"key": "rec-k", "cost": 6})
        status, body, headers = self.request("GET", "/v1/ledgers/rec-k/reconciliation")
        self.assertEqual(status, 200)
        self.assertEqual(set(body), {"key", "reconciled", "usage", "holds", "events"})
        self.assertEqual(body["key"], "rec-k")
        self.assertTrue(body["reconciled"])
        self.assertEqual(body["usage"], {
            "used": 10, "ledger_accepted_count": 2, "ledger_accepted_cost": 10,
            "used_minus_ledger_cost": 0})
        self.assertEqual(body["events"]["first_seq"], 1)
        self.assertEqual(body["events"]["last_seq"], 2)
        self.assertNotIn("ETag", headers)

    def test_http_empty_history_seqs_are_null(self) -> None:
        status, body, _ = self.request("GET", "/v1/ledgers/rec-empty/reconciliation")
        self.assertEqual(status, 200)
        self.assertTrue(body["reconciled"])
        self.assertIsNone(body["events"]["first_seq"])
        self.assertIsNone(body["events"]["last_seq"])

    def test_http_query_parameters_are_invalid_and_change_nothing(self) -> None:
        # A bare "?" carries an empty query string and is the same as no parameters here (the
        # exact rule GET /v1/metrics and the leaky routes already follow); only a real pair is a
        # rejected query. Both rejected audits must leave state untouched.
        status_empty, _, _ = self.request("GET", "/v1/ledgers/rec-q/reconciliation?")
        self.assertEqual(status_empty, 200)
        for path in ("/v1/ledgers/rec-q/reconciliation?x=1",
                     "/v1/ledgers/rec-q/reconciliation?events=10"):
            status, body, _ = self.request("GET", path)
            self.assertEqual(status, 400, path)
            self.assertEqual(body["error"]["code"], "invalid_request")
        status, body, _ = self.request("GET", "/v1/ledgers/rec-q/reconciliation")
        self.assertEqual(status, 200)
        self.assertTrue(body["reconciled"])

    def test_http_unknown_key_is_404_and_creates_no_objects(self) -> None:
        status, body, _ = self.request("GET", "/v1/ledgers/no-such-key/reconciliation")
        self.assertEqual(status, 404)
        self.assertEqual(body["error"]["code"], "not_found")
        limiter = self.server.limiter
        self.assertNotIn("no-such-key", limiter._limits)
        self.assertNotIn("no-such-key", limiter._buckets)
        self.assertNotIn("no-such-key", limiter._ledgers)

    def test_http_oversized_key_is_400(self) -> None:
        status, body, _ = self.request("GET", f"/v1/ledgers/{'k' * 201}/reconciliation")
        self.assertEqual(status, 400)
        self.assertEqual(body["error"]["code"], "invalid_request")

    def test_http_bad_path_and_wrong_method_are_404(self) -> None:
        for method, path in (
                ("GET", "/v1/ledgers/rec-k/reconciliation/extra"),
                ("GET", "/v1/ledgers//reconciliation"),
                ("POST", "/v1/ledgers/rec-k/reconciliation"),
                ("PUT", "/v1/ledgers/rec-k/reconciliation"),
                ("DELETE", "/v1/ledgers/rec-k/reconciliation")):
            status, _, _ = self.request(method, path, {} if method in ("POST", "PUT") else None)
            self.assertEqual(status, 404, path)
        # The plain ledger route on the same key is unaffected and stays a 200 read.
        self.assertEqual(self.request("GET", "/v1/ledgers/rec-k")[0], 200)

    def test_http_existing_ledger_route_behavior_unchanged(self) -> None:
        self.request("POST", "/v1/check", {"key": "rec-ledger", "cost": 1})
        status, body, headers = self.request("GET", "/v1/ledgers/rec-ledger?events=1")
        self.assertEqual(status, 200)
        self.assertEqual(set(body), {"key", "totals", "events"})
        self.assertEqual(len(body["events"]), 1)
        self.assertNotIn("ETag", headers)
        status, body, _ = self.request("GET", "/v1/ledgers/rec-ledger?bogus=1")
        self.assertEqual(status, 400)


if __name__ == "__main__":
    unittest.main()
