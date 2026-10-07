"""Optimistic concurrency for hot reconfiguration: per-key revisions, ETag and If-Match."""
from __future__ import annotations

import json
import threading
import unittest
import urllib.error
import urllib.request

from quota import InvalidRequest, LimitNotFound, Limiter, RevisionConflict, validate_if_match


class Clock:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t


class IfMatchValidationTests(unittest.TestCase):
    def test_valid_quoted_positive_integer(self) -> None:
        self.assertEqual(validate_if_match('"1"'), 1)
        self.assertEqual(validate_if_match('"42"'), 42)
        self.assertEqual(validate_if_match('"0007"'), 7)

    def test_anything_else_is_invalid_request(self) -> None:
        for bad in [None, 3, 3.0, True, "3", '"', '""', '"0"', '"00"', '"+3"', '"-3"', '" 3"',
                    '"3 "', '"3.0"', '"abc"', '"*"', '"1","2"', '"1"2"', ""]:
            with self.assertRaises(InvalidRequest, msg=repr(bad)):
                validate_if_match(bad)


class RevisionUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.limiter = Limiter(self.clock)

    def test_first_create_is_revision_1_and_every_put_increments(self) -> None:
        _, revision = self.limiter.configure("k", {"capacity": 3, "refill_per_second": 1.0})
        self.assertEqual(revision, 1)
        _, revision = self.limiter.configure("k", {"capacity": 5, "refill_per_second": 2.0})
        self.assertEqual(revision, 2)
        # An identical configuration still increments.
        _, revision = self.limiter.configure("k", {"capacity": 5, "refill_per_second": 2.0})
        self.assertEqual(revision, 3)

    def test_revisions_are_tracked_per_key(self) -> None:
        self.limiter.configure("a", {"capacity": 1, "refill_per_second": 1.0})
        self.limiter.configure("a", {"capacity": 1, "refill_per_second": 1.0})
        _, revision = self.limiter.configure("b", {"capacity": 1, "refill_per_second": 1.0})
        self.assertEqual(revision, 1)

    def test_matching_if_match_updates_and_mismatch_is_409(self) -> None:
        self.limiter.configure("k", {"capacity": 3, "refill_per_second": 1.0})
        _, revision = self.limiter.configure("k", {"capacity": 4, "refill_per_second": 1.0},
                                             if_match=1)
        self.assertEqual(revision, 2)
        with self.assertRaises(RevisionConflict) as raised:
            self.limiter.configure("k", {"capacity": 9, "refill_per_second": 9.0}, if_match=1)
        self.assertEqual(str(raised.exception),
                         "If-Match revision does not match current configuration")
        self.assertEqual(raised.exception.status, 409)
        self.assertEqual(raised.exception.code, "revision_conflict")
        # The rejected write installed nothing and consumed no revision.
        self.assertEqual(self.limiter.state("k")["limit"],
                         {"capacity": 4, "refill_per_second": 1.0})
        _, revision = self.limiter.configure("k", {"capacity": 9, "refill_per_second": 9.0},
                                             if_match=2)
        self.assertEqual(revision, 3)

    def test_if_match_on_unconfigured_key_is_404_and_creates_nothing(self) -> None:
        with self.assertRaises(LimitNotFound):
            self.limiter.configure("ghost", {"capacity": 1, "refill_per_second": 1.0}, if_match=1)
        with self.assertRaises(LimitNotFound):
            self.limiter.state("ghost")
        # A later unconditional create still starts at revision 1.
        _, revision = self.limiter.configure("ghost", {"capacity": 1, "refill_per_second": 1.0})
        self.assertEqual(revision, 1)

    def test_conflict_changes_no_quota_state(self) -> None:
        limiter = self.limiter
        limiter.configure("k", {"capacity": 5, "refill_per_second": 0.0001})   # revision 1
        self.assertTrue(limiter.check("k", 2)["allowed"])                      # tokens 3, used 2
        reservation = limiter.reserve("k", 1, ttl_seconds=60)                  # tokens 2
        ledger_before = limiter.ledger("k", 1000)
        watermark_before = limiter._watermark
        with self.assertRaises(RevisionConflict):
            limiter.configure("k", {"capacity": 1, "refill_per_second": 1.0}, if_match=99)
        state = limiter.state("k")
        self.assertEqual((state["remaining"], state["used"]), (2, 2))
        self.assertEqual(state["limit"], {"capacity": 5, "refill_per_second": 0.0001})
        self.assertEqual(limiter.ledger("k", 1000), ledger_before)
        self.assertIn(reservation["reservation_id"], limiter._reservations)
        self.assertEqual(limiter._watermark, watermark_before)
        # The surviving reservation still rolls back normally.
        self.assertTrue(limiter.rollback(reservation["reservation_id"])["rolled_back"])

    def test_conflict_does_not_advance_the_watermark(self) -> None:
        limiter = self.limiter
        self.clock.t = 100.0
        limiter.configure("k", {"capacity": 10, "refill_per_second": 1.0})     # revision 1
        limiter.check("k", 10)                                                 # empty at t=100
        self.clock.t = 90.0                                                    # regressed reading
        with self.assertRaises(RevisionConflict):
            limiter.configure("k", {"capacity": 10, "refill_per_second": 1.0}, if_match=7)
        # The failed PUT sampled no clock: only the real 100->101 interval may refill.
        self.clock.t = 101.0
        self.assertEqual(limiter.state("k")["remaining"], 1)

    def test_unconditional_put_still_refills_at_old_rate_and_keeps_used(self) -> None:
        limiter = self.limiter
        limiter.configure("k", {"capacity": 10, "refill_per_second": 1.0})
        limiter.check("k", 10)
        self.clock.t += 2.0
        limit, revision = limiter.configure("k", {"capacity": 4, "refill_per_second": 5.0})
        self.assertEqual((limit.capacity, revision), (4, 2))
        state = limiter.state("k")
        self.assertEqual((state["remaining"], state["used"]), (2, 10))  # 2 refilled, capped at 4

    def test_concurrent_conditional_puts_have_exactly_one_winner(self) -> None:
        limiter = self.limiter
        limiter.configure("hot", {"capacity": 100, "refill_per_second": 1.0})  # revision 1
        outcomes: list[str] = []
        outcomes_lock = threading.Lock()

        def attempt() -> None:
            try:
                limiter.configure("hot", {"capacity": 100, "refill_per_second": 2.0}, if_match=1)
                outcome = "updated"
            except RevisionConflict:
                outcome = "conflict"
            with outcomes_lock:
                outcomes.append(outcome)

        threads = [threading.Thread(target=attempt) for _ in range(32)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(outcomes.count("updated"), 1)
        self.assertEqual(outcomes.count("conflict"), 31)
        _, revision = limiter.configure("hot", {"capacity": 100, "refill_per_second": 2.0},
                                        if_match=2)
        self.assertEqual(revision, 3)

    def test_concurrent_unconditional_puts_increment_once_each(self) -> None:
        limiter = self.limiter
        limiter.configure("hot", {"capacity": 100, "refill_per_second": 1.0})  # revision 1
        errors: list[BaseException] = []

        def attempt(index: int) -> None:
            try:
                limiter.configure("hot", {"capacity": 100, "refill_per_second": 1.0 + index})
            except BaseException as error:  # noqa: BLE001
                errors.append(error)

        threads = [threading.Thread(target=attempt, args=(index,)) for index in range(50)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        _, revision = limiter.configure("hot", {"capacity": 100, "refill_per_second": 1.0})
        self.assertEqual(revision, 52)                                   # 1 + 50 + this one


class RevisionHttpTests(unittest.TestCase):
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

    def request(self, method: str, path: str, body: dict | None = None,
                headers: dict[str, str] | None = None) -> tuple[int, dict, dict]:
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data,
                                         method=method,
                                         headers={"Content-Type": "application/json",
                                                  **(headers or {})})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read() or b"{}"), dict(response.headers)
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read() or b"{}"), dict(error.headers)

    def test_put_and_get_carry_etag_of_the_current_revision(self) -> None:
        status, body, headers = self.request("PUT", "/v1/limits/e-tag-1",
                                             {"capacity": 2, "refill_per_second": 2})
        self.assertEqual(status, 200)
        self.assertEqual(headers["ETag"], '"1"')
        self.assertEqual(set(body), {"key", "limit"})                 # JSON shape unchanged
        status, body, headers = self.request("GET", "/v1/limits/e-tag-1")
        self.assertEqual((status, headers["ETag"]), (200, '"1"'))
        self.assertEqual(set(body), {"limit", "remaining", "used"})   # JSON shape unchanged
        # A GET does not consume a revision: the next PUT still moves 1 -> 2.
        status, _, headers = self.request("PUT", "/v1/limits/e-tag-1",
                                          {"capacity": 2, "refill_per_second": 2})
        self.assertEqual((status, headers["ETag"]), (200, '"2"'))
        _, _, headers = self.request("GET", "/v1/limits/e-tag-1")
        self.assertEqual(headers["ETag"], '"2"')

    def test_if_match_roundtrip_through_etag(self) -> None:
        _, _, headers = self.request("PUT", "/v1/limits/e-tag-2",
                                     {"capacity": 3, "refill_per_second": 1})
        etag = headers["ETag"]
        status, _, headers = self.request("PUT", "/v1/limits/e-tag-2",
                                          {"capacity": 4, "refill_per_second": 1},
                                          {"If-Match": etag})
        self.assertEqual((status, headers["ETag"]), (200, '"2"'))
        # Replaying the stale ETag now conflicts.
        status, body, _ = self.request("PUT", "/v1/limits/e-tag-2",
                                       {"capacity": 5, "refill_per_second": 1},
                                       {"If-Match": etag})
        self.assertEqual(status, 409)
        self.assertEqual(body, {"error": {"code": "revision_conflict",
                                          "message": "If-Match revision does not match current configuration"}})
        _, state, headers = self.request("GET", "/v1/limits/e-tag-2")
        self.assertEqual((state["limit"]["capacity"], headers["ETag"]), (4, '"2"'))

    def test_if_match_on_unconfigured_key_is_404(self) -> None:
        status, body, _ = self.request("PUT", "/v1/limits/e-tag-ghost",
                                       {"capacity": 1, "refill_per_second": 1},
                                       {"If-Match": '"1"'})
        self.assertEqual((status, body["error"]["code"]), (404, "not_found"))
        self.assertEqual(self.request("GET", "/v1/limits/e-tag-ghost")[0], 404)

    def test_malformed_if_match_is_400_and_changes_nothing(self) -> None:
        for bad in ["3", '"0"', '"+3"', '" 3"', '"3 "', '"3.0"', '"*"', '""', '"a"']:
            status, body, _ = self.request("PUT", "/v1/limits/e-tag-3",
                                           {"capacity": 2, "refill_per_second": 1},
                                           {"If-Match": bad})
            self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"), bad)
        # No partial configuration was produced by any rejected request.
        self.assertEqual(self.request("GET", "/v1/limits/e-tag-3")[0], 404)
        # And on a configured key the revision is untouched by rejected requests.
        _, _, headers = self.request("PUT", "/v1/limits/e-tag-4",
                                     {"capacity": 2, "refill_per_second": 1})
        self.assertEqual(headers["ETag"], '"1"')
        status, _, _ = self.request("PUT", "/v1/limits/e-tag-4",
                                    {"capacity": 9, "refill_per_second": 1}, {"If-Match": '"x"'})
        self.assertEqual(status, 400)
        status, _, headers = self.request("PUT", "/v1/limits/e-tag-4",
                                          {"capacity": 9, "refill_per_second": 1},
                                          {"If-Match": '"1"'})
        self.assertEqual((status, headers["ETag"]), (200, '"2"'))

    def test_conflict_via_http_leaves_quota_state_untouched(self) -> None:
        self.request("PUT", "/v1/limits/e-tag-5", {"capacity": 4, "refill_per_second": 0.0001})
        self.request("POST", "/v1/check", {"key": "e-tag-5", "cost": 2})
        _, before, _ = self.request("GET", "/v1/limits/e-tag-5")
        _, ledger_before, _ = self.request("GET", "/v1/ledgers/e-tag-5")
        _, metrics_before, _ = self.request("GET", "/v1/metrics")
        status, _, _ = self.request("PUT", "/v1/limits/e-tag-5",
                                    {"capacity": 1, "refill_per_second": 1},
                                    {"If-Match": '"99"'})
        self.assertEqual(status, 409)
        _, after, headers = self.request("GET", "/v1/limits/e-tag-5")
        self.assertEqual(after, before)
        self.assertEqual(headers["ETag"], '"1"')
        _, ledger_after, _ = self.request("GET", "/v1/ledgers/e-tag-5")
        self.assertEqual(ledger_after, ledger_before)
        _, metrics_after, _ = self.request("GET", "/v1/metrics")
        self.assertEqual(metrics_after, metrics_before)

    def test_etag_is_scoped_to_limits_not_tokens_windows_or_ledgers(self) -> None:
        self.request("PUT", "/v1/limits/e-tag-6", {"capacity": 2, "refill_per_second": 1})
        # Spending tokens and configuring a same-named window move no revision.
        self.request("POST", "/v1/check", {"key": "e-tag-6", "cost": 1})
        _, _, headers = self.request("PUT", "/v1/windows/e-tag-6",
                                     {"window_seconds": 10, "max_events": 2})
        self.assertNotIn("ETag", headers)
        _, _, headers = self.request("POST", "/v1/check", {"key": "e-tag-6", "cost": 1})
        self.assertNotIn("ETag", headers)
        _, _, headers = self.request("GET", "/v1/ledgers/e-tag-6")
        self.assertNotIn("ETag", headers)
        _, _, headers = self.request("GET", "/v1/limits/e-tag-6")
        self.assertEqual(headers["ETag"], '"1"')


if __name__ == "__main__":
    unittest.main()
