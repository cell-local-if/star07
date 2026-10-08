"""Bounded usage/ledger storage: a hot key keeps at most LEDGER_EVENT_KEEP event details while
totals and `used` still cover every booking since the Limiter was created."""
from __future__ import annotations

import json
import threading
import unittest
import urllib.error
import urllib.request

from quota import LEDGER_EVENT_KEEP, Limiter, LimitNotFound, OverQuota


class Clock:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t


class BoundedLedgerUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.limiter = Limiter(self.clock)
        # A practically inexhaustible bucket: the clock is frozen and the refill rate negligible,
        # so thousands of cost-1 checks all succeed without any refill interference.
        self.limiter.configure("hot", {"capacity": 1_000_000, "refill_per_second": 0.0001})

    def book_checks(self, count: int) -> None:
        for _ in range(count):
            self.assertTrue(self.limiter.check("hot", 1)["allowed"])

    def test_detail_storage_is_bounded_while_totals_cover_lifetime(self) -> None:
        self.book_checks(LEDGER_EVENT_KEEP + 500)
        ledger = self.limiter.ledger("hot", LEDGER_EVENT_KEEP)
        # Exactly the newest 1000 details survive: seq starts past 1, is contiguous, and ends at
        # the lifetime count — no gaps, no duplicates, no old events mixed in.
        events = ledger["events"]
        self.assertEqual(len(events), LEDGER_EVENT_KEEP)
        self.assertEqual([event["seq"] for event in events],
                         list(range(501, LEDGER_EVENT_KEEP + 501)))
        self.assertEqual(ledger["totals"], {"accepted_count": LEDGER_EVENT_KEEP + 500,
                                            "accepted_cost": LEDGER_EVENT_KEEP + 500})
        # The retained detail is really the tail: the oldest surviving event is seq 501.
        self.assertEqual(events[0]["seq"], 501)
        self.assertEqual(events[-1]["seq"], LEDGER_EVENT_KEEP + 500)
        # used and accepted_cost stay strictly equal, both covering the trimmed-away events.
        self.assertEqual(self.limiter.state("hot")["used"], LEDGER_EVENT_KEEP + 500)

    def test_internal_event_buffer_never_exceeds_the_keep_bound(self) -> None:
        self.book_checks(LEDGER_EVENT_KEEP + 2500)
        self.assertEqual(len(self.limiter._ledgers["hot"].events), LEDGER_EVENT_KEEP)
        ledger = self.limiter.ledger("hot", LEDGER_EVENT_KEEP)
        self.assertEqual(ledger["totals"]["accepted_count"], LEDGER_EVENT_KEEP + 2500)
        self.assertEqual([event["seq"] for event in ledger["events"]],
                         list(range(2501, LEDGER_EVENT_KEEP + 2501)))

    def test_tail_windows_after_trimming(self) -> None:
        self.book_checks(LEDGER_EVENT_KEEP + 50)
        default = self.limiter.ledger("hot")
        self.assertEqual(len(default["events"]), 100)
        self.assertEqual([event["seq"] for event in default["events"]],
                         list(range(LEDGER_EVENT_KEEP + 50 - 99, LEDGER_EVENT_KEEP + 51)))
        one = self.limiter.ledger("hot", 1)
        self.assertEqual([event["seq"] for event in one["events"]], [LEDGER_EVENT_KEEP + 50])
        full = self.limiter.ledger("hot", LEDGER_EVENT_KEEP)
        self.assertEqual(len(full["events"]), LEDGER_EVENT_KEEP)
        self.assertEqual(full["events"][0]["seq"], 51)
        for view in (default, one, full):
            self.assertEqual(view["totals"], {"accepted_count": LEDGER_EVENT_KEEP + 50,
                                              "accepted_cost": LEDGER_EVENT_KEEP + 50})

    def test_below_the_bound_nothing_is_trimmed(self) -> None:
        self.book_checks(LEDGER_EVENT_KEEP)
        ledger = self.limiter.ledger("hot", LEDGER_EVENT_KEEP)
        self.assertEqual([event["seq"] for event in ledger["events"]],
                         list(range(1, LEDGER_EVENT_KEEP + 1)))
        self.assertEqual(ledger["totals"], {"accepted_count": LEDGER_EVENT_KEEP,
                                            "accepted_cost": LEDGER_EVENT_KEEP})

    def test_mixed_sources_survive_trimming_with_fields_intact(self) -> None:
        limiter = Limiter(self.clock)
        limiter.configure("parent", {"capacity": 1_000_000, "refill_per_second": 0.0001})
        limiter.configure("leaf", {"capacity": 1_000_000, "refill_per_second": 0.0001})
        for _ in range(LEDGER_EVENT_KEEP):
            limiter.check("leaf", 1)                       # seqs 1..1000, all trimmed eventually
        reservation = limiter.reserve("leaf", 2)
        limiter.consume(reservation["reservation_id"])     # seq 1001
        limiter.hierarchy_check(["parent", "leaf"], 3)     # leaf seq 1002
        hierarchy_hold = limiter.hierarchy_reserve(["parent", "leaf"], 4)
        limiter.hierarchy_consume(hierarchy_hold["reservation_id"])  # leaf seq 1003
        ledger = limiter.ledger("leaf", LEDGER_EVENT_KEEP)
        events = ledger["events"]
        self.assertEqual(len(events), LEDGER_EVENT_KEEP)
        self.assertEqual([event["seq"] for event in events], list(range(4, 1004)))
        # The three newest events are the mixed-source ones, in booking order, fields intact.
        last = events[-3:]
        self.assertEqual([event["source"] for event in last],
                         ["reservation_consume", "hierarchy_check", "hierarchy_reservation_consume"])
        self.assertEqual([event["cost"] for event in last], [2, 3, 4])
        self.assertEqual([event["reservation_id"] for event in last],
                         [reservation["reservation_id"], None, hierarchy_hold["reservation_id"]])
        self.assertTrue(all(event["capacity"] == 1_000_000 for event in last))
        self.assertTrue(all(event["effective_at"] == self.clock.t for event in last))
        # Totals and used cover the trimmed 1000 checks too: 1000*1 + 2 + 3 + 4.
        self.assertEqual(ledger["totals"], {"accepted_count": 1003, "accepted_cost": 1009})
        self.assertEqual(limiter.state("leaf")["used"], 1009)

    def test_failures_after_trimming_add_neither_totals_nor_used(self) -> None:
        self.book_checks(LEDGER_EVENT_KEEP + 10)
        before = self.limiter.ledger("hot", LEDGER_EVENT_KEEP)
        limiter2 = Limiter(self.clock)                     # separate small key for the 429
        limiter2.configure("tiny", {"capacity": 1, "refill_per_second": 0.0001})
        limiter2.check("tiny", 1)
        with self.assertRaises(OverQuota):
            limiter2.check("tiny", 1)                      # rejected: nothing booked
        reservation = self.limiter.reserve("hot", 5, ttl_seconds=10)
        self.limiter.rollback(reservation["reservation_id"])  # undone: nothing booked
        expiring = self.limiter.reserve("hot", 5, ttl_seconds=5)
        self.clock.t += 5
        self.limiter.state("hot")                          # settles the due hold
        with self.assertRaises(LimitNotFound):
            self.limiter.consume(expiring["reservation_id"])  # expired hold never books
        duplicate = self.limiter.reserve("hot", 5)
        first = self.limiter.consume(duplicate["reservation_id"])
        self.assertEqual(self.limiter.consume(duplicate["reservation_id"]), first)  # replay only
        after = self.limiter.ledger("hot", LEDGER_EVENT_KEEP)
        # Only the one successful consume booked since `before`: +1 count, +5 cost.
        self.assertEqual(after["totals"]["accepted_count"],
                         before["totals"]["accepted_count"] + 1)
        self.assertEqual(after["totals"]["accepted_cost"], before["totals"]["accepted_cost"] + 5)
        self.assertEqual(self.limiter.state("hot")["used"], after["totals"]["accepted_cost"])

    def test_reads_after_trimming_sample_no_clock_and_advance_no_watermark(self) -> None:
        clock = Clock()
        limiter = Limiter(clock)
        limiter.configure("k", {"capacity": 1_000_000, "refill_per_second": 1.0})
        for _ in range(LEDGER_EVENT_KEEP + 100):
            limiter.check("k", 1)
        first = limiter.ledger("k", LEDGER_EVENT_KEEP)
        clock.t += 100                                     # a read never samples this...
        second = limiter.ledger("k", LEDGER_EVENT_KEEP)
        clock.t -= 200                                     # ...nor a regression
        third = limiter.ledger("k", LEDGER_EVENT_KEEP)
        self.assertEqual(second, first)
        self.assertEqual(third, first)
        # The watermark never moved off 1000.0 through those reads: state() at the regressed
        # clock refills nothing beyond the watermark moment.
        self.assertEqual(limiter.state("k")["remaining"], 1_000_000 - (LEDGER_EVENT_KEEP + 100))

    def test_concurrent_hot_key_stays_bounded_and_consistent(self) -> None:
        limiter = Limiter(self.clock)                      # clock frozen for the whole test
        limiter.configure("hot", {"capacity": 1_000_000, "refill_per_second": 0.0001})
        spenders = 8
        spends_each = 400                                  # 3200 bookings: well past the bound
        errors: list[BaseException] = []
        list_lock = threading.Lock()

        def spend() -> None:
            try:
                for _ in range(spends_each):
                    limiter.check("hot", 1)
            except BaseException as error:  # noqa: BLE001
                with list_lock:
                    errors.append(error)

        def read_ledger() -> None:
            try:
                for tail in (1, 7, 100, 999, 1000) * 20:
                    view = limiter.ledger("hot", tail)
                    events = view["events"]
                    count = view["totals"]["accepted_count"]
                    seqs = [event["seq"] for event in events]
                    # The tail is always the contiguous newest min(tail, count, KEEP) events.
                    expected_len = min(tail, count, LEDGER_EVENT_KEEP)
                    if len(events) != expected_len \
                            or seqs != list(range(count - expected_len + 1, count + 1)):
                        raise AssertionError(f"inconsistent ledger view: {view['totals']}, {seqs}")
                    if view["totals"]["accepted_cost"] != count:  # every booking was cost 1
                        raise AssertionError("accepted_cost diverged from accepted_count")
            except BaseException as error:  # noqa: BLE001
                with list_lock:
                    errors.append(error)

        threads = [threading.Thread(target=spend) for _ in range(spenders)]
        threads += [threading.Thread(target=read_ledger) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])

        booked = spenders * spends_each
        self.assertEqual(len(limiter._ledgers["hot"].events), LEDGER_EVENT_KEEP)
        ledger = limiter.ledger("hot", LEDGER_EVENT_KEEP)
        self.assertEqual(ledger["totals"], {"accepted_count": booked, "accepted_cost": booked})
        self.assertEqual([event["seq"] for event in ledger["events"]],
                         list(range(booked - LEDGER_EVENT_KEEP + 1, booked + 1)))
        self.assertEqual(limiter.state("hot")["used"], booked)


class BoundedLedgerHttpTests(unittest.TestCase):
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

    def test_http_ledger_tail_and_totals_after_trimming(self) -> None:
        self.assertEqual(self.request("PUT", "/v1/limits/http-hot",
                                      {"capacity": 1_000_000, "refill_per_second": 0.0001})[0], 200)
        for _ in range(LEDGER_EVENT_KEEP + 25):
            status, body, _ = self.request("POST", "/v1/check", {"key": "http-hot", "cost": 1})
            self.assertEqual(status, 200)
            self.assertTrue(body["allowed"])
        status, ledger, _ = self.request("GET", f"/v1/ledgers/http-hot?events={LEDGER_EVENT_KEEP}")
        self.assertEqual(status, 200)
        self.assertEqual(len(ledger["events"]), LEDGER_EVENT_KEEP)
        seqs = [event["seq"] for event in ledger["events"]]
        self.assertEqual(seqs, list(range(26, LEDGER_EVENT_KEEP + 26)))
        self.assertEqual(ledger["totals"], {"accepted_count": LEDGER_EVENT_KEEP + 25,
                                            "accepted_cost": LEDGER_EVENT_KEEP + 25})
        # The default window still returns the newest 100, and `used` matches accepted_cost.
        status, default, _ = self.request("GET", "/v1/ledgers/http-hot")
        self.assertEqual(status, 200)
        self.assertEqual([event["seq"] for event in default["events"]],
                         list(range(LEDGER_EVENT_KEEP + 25 - 99, LEDGER_EVENT_KEEP + 26)))
        status, state, _ = self.request("GET", "/v1/limits/http-hot")
        self.assertEqual(status, 200)
        self.assertEqual(state["used"], ledger["totals"]["accepted_cost"])

    def test_events_parameter_validation_unchanged(self) -> None:
        self.assertEqual(self.request("PUT", "/v1/limits/http-val",
                                      {"capacity": 10, "refill_per_second": 1.0})[0], 200)
        for query in ("events=0", "events=1001", "events=abc", "events=", "events=1&events=2",
                      "unknown=1", "events=1&unknown=2"):
            status, body, _ = self.request("GET", f"/v1/ledgers/http-val?{query}")
            self.assertEqual(status, 400, query)
            self.assertEqual(body["error"]["code"], "invalid_request")
        self.assertEqual(self.request("GET", "/v1/ledgers/http-val?events=1")[0], 200)
        self.assertEqual(self.request("GET", "/v1/ledgers/http-val?events=1000")[0], 200)


if __name__ == "__main__":
    unittest.main()
