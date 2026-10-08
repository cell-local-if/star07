"""Bounded ledger/usage storage: hot keys keep O(1) memory while totals and `used`
still cover every accepted booking since the Limiter was created."""
from __future__ import annotations

import json
import threading
import unittest
import urllib.error
import urllib.request

from quota import Limiter
from quota.app import LEDGER_EVENT_RETENTION


class Clock:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t


class BoundedLedgerUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.limiter = Limiter(self.clock)
        self.limiter.configure("hot", {"capacity": 10_000, "refill_per_second": 0.0001})

    def book_checks(self, count: int) -> None:
        for _ in range(count):
            self.limiter.check("hot", 1)                      # clock frozen: every check books

    def test_event_details_are_capped_but_totals_cover_all_history(self) -> None:
        total = LEDGER_EVENT_RETENTION + 200
        self.book_checks(total)
        ledger = self.limiter.ledger("hot", 1000)
        # Internal detail storage is bounded: exactly the newest 1000 events survive.
        self.assertEqual(len(self.limiter._ledgers["hot"].events), LEDGER_EVENT_RETENTION)
        self.assertEqual(ledger["totals"], {"accepted_count": total, "accepted_cost": total})
        # GET /v1/limits/{key}'s used still matches totals.accepted_cost exactly.
        self.assertEqual(self.limiter.state("hot")["used"], total)

    def test_tail_past_retention_starts_above_one_and_stays_dense(self) -> None:
        total = LEDGER_EVENT_RETENTION + 200
        self.book_checks(total)
        full = self.limiter.ledger("hot", 1000)
        seqs = [event["seq"] for event in full["events"]]
        self.assertEqual(seqs, list(range(total - 999, total + 1)))   # 201..1200, no gap/dup
        default = self.limiter.ledger("hot")
        self.assertEqual([event["seq"] for event in default["events"]],
                         list(range(total - 99, total + 1)))
        one = self.limiter.ledger("hot", 1)
        self.assertEqual([event["seq"] for event in one["events"]], [total])
        for view in (full, default, one):
            self.assertEqual(view["totals"], {"accepted_count": total, "accepted_cost": total})

    def test_usage_memory_is_a_running_total_not_a_history(self) -> None:
        self.book_checks(LEDGER_EVENT_RETENTION + 500)
        bucket = self.limiter._buckets["hot"]
        self.assertIsInstance(bucket.used, int)
        self.assertEqual(bucket.used, LEDGER_EVENT_RETENTION + 500)
        self.assertFalse(hasattr(bucket, "cost_history"))

    def test_mixed_sources_share_one_bounded_sequence(self) -> None:
        limiter = self.limiter
        limiter.configure("parent", {"capacity": 10_000, "refill_per_second": 0.0001})
        limiter.configure("leaf", {"capacity": 10_000, "refill_per_second": 0.0001})
        self.book_checks(LEDGER_EVENT_RETENTION)                      # hot: 1000 check events
        reservation = limiter.reserve("hot", 2, ttl_seconds=3600)
        limiter.consume(reservation["reservation_id"])                # seq 1001
        limiter.hierarchy_check(["parent", "leaf"], 3)                # one event per layer
        limiter.check("hot", 1)                                       # hot's seq 1002
        # hot's ledger: 1000 checks + 1 consume + 1 check = 1002 events, seq 3..1002 retained.
        ledger = self.limiter.ledger("hot", 1000)
        events = ledger["events"]
        self.assertEqual([event["seq"] for event in events], list(range(3, 1003)))
        self.assertEqual(ledger["totals"], {"accepted_count": 1002, "accepted_cost": 1003})
        self.assertEqual(limiter.state("hot")["used"], 1003)
        # The retained consume event still carries its reservation_id and fields verbatim.
        consume_events = [event for event in events
                          if event["source"] == "reservation_consume"]
        self.assertEqual(len(consume_events), 1)
        self.assertEqual(consume_events[0]["reservation_id"],
                         reservation["reservation_id"])
        self.assertEqual(consume_events[0]["cost"], 2)
        # Each hierarchy layer got its own event; both layers' totals include it.
        for key in ("parent", "leaf"):
            view = limiter.ledger(key, 1000)
            self.assertEqual(view["totals"], {"accepted_count": 1, "accepted_cost": 3})
            self.assertEqual(view["events"][0]["source"], "hierarchy_check")
            self.assertEqual(view["events"][0]["seq"], 1)

    def test_trimming_one_key_leaves_other_keys_untouched(self) -> None:
        self.book_checks(LEDGER_EVENT_RETENTION + 100)
        self.limiter.configure("quiet", {"capacity": 10, "refill_per_second": 1.0})
        self.limiter.check("quiet", 2)
        view = self.limiter.ledger("quiet", 1000)
        self.assertEqual([event["seq"] for event in view["events"]], [1])
        self.assertEqual(view["totals"], {"accepted_count": 1, "accepted_cost": 2})
        self.assertEqual(self.limiter.state("quiet")["used"], 2)

    def test_reads_past_retention_sample_no_clock_and_are_stable(self) -> None:
        self.book_checks(LEDGER_EVENT_RETENTION + 50)
        first = self.limiter.ledger("hot", 1000)
        self.clock.t += 100                                            # a read never samples this
        second = self.limiter.ledger("hot", 1000)
        self.clock.t -= 200                                            # ...nor a regression
        third = self.limiter.ledger("hot", 1000)
        self.assertEqual(second, first)
        self.assertEqual(third, first)
        self.assertEqual(self.limiter._watermark, 1000.0)              # reads never moved it

    def test_concurrent_bookings_past_retention_keep_a_dense_tail(self) -> None:
        limiter = Limiter(self.clock)                                  # clock frozen throughout
        limiter.configure("burst", {"capacity": 10_000, "refill_per_second": 0.0001})
        errors: list[BaseException] = []
        total = LEDGER_EVENT_RETENTION + 500

        def spend() -> None:
            try:
                for _ in range(total // 10):
                    limiter.check("burst", 1)
            except BaseException as error:  # noqa: BLE001
                errors.append(error)

        def read_ledger() -> None:
            try:
                for _ in range(50):
                    view = limiter.ledger("burst", 1000)
                    seqs = [event["seq"] for event in view["events"]]
                    count = view["totals"]["accepted_count"]
                    expected = min(LEDGER_EVENT_RETENTION, count)
                    if seqs != list(range(count - expected + 1, count + 1)):
                        raise AssertionError(f"non-dense tail: totals={view['totals']}")
                    if view["totals"]["accepted_cost"] != count:
                        raise AssertionError("accepted_cost diverged from accepted_count")
            except BaseException as error:  # noqa: BLE001
                errors.append(error)

        threads = ([threading.Thread(target=spend) for _ in range(10)]
                   + [threading.Thread(target=read_ledger) for _ in range(4)])
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        ledger = limiter.ledger("burst", 1000)
        self.assertEqual(ledger["totals"], {"accepted_count": total, "accepted_cost": total})
        self.assertEqual([event["seq"] for event in ledger["events"]],
                         list(range(total - 999, total + 1)))
        self.assertEqual(limiter.state("burst")["used"], total)
        self.assertEqual(len(limiter._ledgers["burst"].events), LEDGER_EVENT_RETENTION)


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
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data,
                                         method=method,
                                         headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read() or b"{}"), dict(response.headers)
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read() or b"{}"), dict(error.headers)

    def test_http_views_past_retention_keep_used_and_totals_identical(self) -> None:
        total = LEDGER_EVENT_RETENTION + 100
        self.assertEqual(self.request(
            "PUT", "/v1/limits/http-hot",
            {"capacity": 10_000, "refill_per_second": 0.0001})[0], 200)
        for _ in range(total):
            status, body, _ = self.request("POST", "/v1/check", {"key": "http-hot", "cost": 1})
            self.assertEqual(status, 200)
        status, ledger, _ = self.request("GET", "/v1/ledgers/http-hot?events=1000")
        self.assertEqual(status, 200)
        self.assertEqual(ledger["totals"], {"accepted_count": total, "accepted_cost": total})
        self.assertEqual([event["seq"] for event in ledger["events"]],
                         list(range(total - 999, total + 1)))
        status, default, _ = self.request("GET", "/v1/ledgers/http-hot")
        self.assertEqual(status, 200)
        self.assertEqual(len(default["events"]), 100)
        self.assertEqual([event["seq"] for event in default["events"]],
                         list(range(total - 99, total + 1)))
        status, state, _ = self.request("GET", "/v1/limits/http-hot")
        self.assertEqual(status, 200)
        self.assertEqual(state["used"], ledger["totals"]["accepted_cost"])
        # The events parameter boundary is unchanged by bounded storage.
        self.assertEqual(self.request("GET", "/v1/ledgers/http-hot?events=0")[0], 400)
        self.assertEqual(self.request("GET", "/v1/ledgers/http-hot?events=1001")[0], 400)


if __name__ == "__main__":
    unittest.main()
