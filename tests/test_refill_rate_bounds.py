"""refill_per_second value boundary for PUT /v1/limits/{key}.

The rate must be a non-boolean, FINITE number with 0 < rate <= 1000000. Python's JSON parser
accepts the non-standard NaN/Infinity literals; NaN in particular slipped past the old range
check (both ``NaN <= 0`` and ``NaN > 1000000`` are false) and a stored non-finite rate later
made an over-quota Retry-After non-finite. These tests pin the rule at both layers with the
same result: ``Limiter.configure`` raises InvalidRequest and HTTP answers 400 invalid_request,
the key is never created or replaced, and no other state moves.
"""
from __future__ import annotations

import http.client
import json
import math
import threading
import unittest
import urllib.error
import urllib.request

from quota import (
    InvalidRequest,
    Limiter,
    OverQuota,
    validate_limit,
)


class Clock:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t


MESSAGE = "refill_per_second must be a finite positive number no greater than 1000000"

# Every one of these must be refused by validate_limit and by configure, with one fixed message.
BAD_RATES = [
    float("nan"), float("inf"), float("-inf"),
    True, False, 0, -0, -1, -0.5, 1_000_001, 1_000_000.0001, 1e100,
    None, "1", [], {},
]

GOOD_RATES = [1, 1_000_000, 0.5, 0.0001, 1e-300, 3.14159, 999_999.999]


class RefillRateValidationUnitTests(unittest.TestCase):
    def test_validate_limit_rejects_non_finite_and_out_of_range_rates_with_one_message(self) -> None:
        for rate in BAD_RATES:
            with self.assertRaises(InvalidRequest, msg=repr(rate)) as raised:
                validate_limit({"capacity": 5, "refill_per_second": rate})
            self.assertEqual(str(raised.exception), MESSAGE, repr(rate))

    def test_validate_limit_accepts_integers_and_finite_floats_including_both_boundaries(self) -> None:
        for rate in GOOD_RATES:
            limit = validate_limit({"capacity": 5, "refill_per_second": rate})
            self.assertEqual(limit.capacity, 5)
            self.assertTrue(math.isfinite(limit.refill_per_second))
            self.assertAlmostEqual(limit.refill_per_second, float(rate), places=20)

    def test_configure_rejects_the_same_inputs_with_the_same_error(self) -> None:
        limiter = Limiter(Clock())
        limiter.configure("k", {"capacity": 5, "refill_per_second": 2.0})
        for rate in BAD_RATES:
            with self.assertRaises(InvalidRequest, msg=repr(rate)) as raised:
                limiter.configure("k", {"capacity": 5, "refill_per_second": rate})
            self.assertEqual(str(raised.exception), MESSAGE, repr(rate))


class RefillRateRejectionStateUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.limiter = Limiter(self.clock)
        self.limiter.configure("k", {"capacity": 5, "refill_per_second": 2.0})
        # tokens 3, used 2, one ledger event; a live hold keeps 1 token and is due at t=1010.
        self.assertTrue(self.limiter.check("k", 2)["allowed"])
        self.rid = self.limiter.reserve("k", 1, ttl_seconds=10)["reservation_id"]
        self.metrics_before = self.limiter.metrics()
        self.ledger_before = self.limiter.ledger("k", 1000)

    def _assert_everything_untouched(self, revision: int = 1) -> None:
        # Call with the clock at t=1001 (see the tests): 1s at the old rate 2 refills the
        # post-hold 2 tokens to 4, and the hold due at 1010 is still live. Had any rejected
        # write sampled t=1050, the watermark (and the due settle) would give 5 here and the
        # reservation would be gone.
        self.assertEqual(self.limiter._revisions["k"], revision)
        state = self.limiter.state("k")
        self.assertEqual(state["limit"], {"capacity": 5, "refill_per_second": 2.0})
        self.assertEqual((state["remaining"], state["used"]), (4, 2))
        self.assertIn(self.rid, self.limiter._reservations)
        self.assertEqual(self.limiter.ledger("k", 1000), self.ledger_before)
        self.assertEqual(self.limiter.metrics(), self.metrics_before)

    def test_rejected_reconfigure_changes_no_state_even_at_a_later_clock_reading(self) -> None:
        self.clock.t = 1050.0          # a naive implementation would settle and pin the watermark
        for rate in (float("nan"), float("inf"), float("-inf"), True, 0, -1.0, 1_000_001):
            with self.assertRaises(InvalidRequest, msg=repr(rate)):
                self.limiter.configure("k", {"capacity": 9, "refill_per_second": rate})
        self.clock.t = 1001.0          # below the rejected readings: they must not have pinned 1050
        self._assert_everything_untouched()

    def test_rejected_create_on_unconfigured_key_creates_nothing_and_does_not_tick(self) -> None:
        self.clock.t = 2000.0
        for rate in (float("nan"), float("inf"), float("-inf"), True, -3.0, 2_000_000):
            with self.assertRaises(InvalidRequest, msg=repr(rate)):
                self.limiter.configure("ghost", {"capacity": 5, "refill_per_second": rate})
        self.assertNotIn("ghost", self.limiter._limits)
        self.assertNotIn("ghost", self.limiter._buckets)
        self.assertNotIn("ghost", self.limiter._revisions)
        self.clock.t = 1009.0
        # The first real creation is revision 1 stamped at 1009, not revision 2 stamped at 2000.
        result = self.limiter.configure("ghost", {"capacity": 5, "refill_per_second": 1.0})
        self.assertEqual(result.revision, 1)
        self.assertEqual(self.limiter._buckets["ghost"].updated_at, 1009.0)

    def test_legal_if_match_with_non_finite_rate_is_invalid_request_not_revision_conflict(self) -> None:
        for rate in (float("nan"), float("inf"), float("-inf")):
            with self.assertRaises(InvalidRequest, msg=repr(rate)):
                self.limiter.configure("k", {"capacity": 5, "refill_per_second": rate}, 1)
        # A stale precondition with a non-finite value is ALSO invalid_request: body validation
        # runs before the lock, so the request can never be classified as a revision conflict.
        with self.assertRaises(InvalidRequest):
            self.limiter.configure("k", {"capacity": 5, "refill_per_second": float("nan")}, 999)
        self.assertEqual(self.limiter._revisions["k"], 1)
        self.clock.t = 1001.0
        self._assert_everything_untouched()

    def test_legal_put_still_old_rate_refills_then_applies_new_capacity_and_bumps_revision(self) -> None:
        self.clock.t = 1002.0          # the live hold (due at 1010) survives
        result = self.limiter.configure("k", {"capacity": 4, "refill_per_second": 9.0})
        self.assertEqual(result.revision, 2)
        # 2s at the OLD rate 2 adds 4 on top of the post-hold 2 -> 6, capped at the NEW cap 4.
        state = self.limiter.state("k")
        self.assertEqual((state["remaining"], state["used"]), (4, 2))
        # An identical re-PUT still advances the revision by exactly one.
        self.assertEqual(
            self.limiter.configure("k", {"capacity": 4, "refill_per_second": 9.0}).revision, 3)

    def test_stored_rates_stay_finite_so_over_quota_retry_after_is_finite(self) -> None:
        for rate in GOOD_RATES:
            limiter = Limiter(Clock())
            limiter.configure("b", {"capacity": 1, "refill_per_second": rate})
            self.assertTrue(math.isfinite(limiter.limit("b").refill_per_second))
            limiter.check("b", 1)
            with self.assertRaises(OverQuota) as raised:
                limiter.check("b", 1)
            self.assertTrue(math.isfinite(raised.exception.retry_after))


class RefillRateHttpTests(unittest.TestCase):
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
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
            headers={"Content-Type": "application/json", **(headers or {})})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read() or b"{}"), dict(response.headers)
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read() or b"{}"), dict(error.headers)

    def raw_put(self, path: str, payload: bytes,
                headers: list[tuple[str, str]] | None = None) -> tuple[int, dict, dict]:
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        connection.putrequest("PUT", path)
        connection.putheader("Content-Length", str(len(payload)))
        for name, value in headers or []:
            connection.putheader(name, value)
        connection.endheaders(payload)
        response = connection.getresponse()
        body = json.loads(response.read() or b"{}")
        result = response.status, body, dict(response.headers)
        connection.close()
        return result

    def test_non_finite_and_out_of_range_literals_are_400_with_the_exact_message(self) -> None:
        status, _, headers = self.request("PUT", "/v1/limits/fb",
                                          {"capacity": 5, "refill_per_second": 2})
        self.assertEqual((status, headers["ETag"]), (200, '"1"'))
        # 1e999 parses to Infinity with the stdlib parser; the bare NaN/Infinity literals are the
        # non-standard forms json.loads accepts by default.
        for literal in [b"NaN", b"Infinity", b"-Infinity", b"true", b"false", b"0",
                        b"-1", b"-0.5", b"1000001", b"1e999", b"null", b'"1"']:
            status, body, resp_headers = self.raw_put(
                "/v1/limits/fb", b'{"capacity": 5, "refill_per_second": ' + literal + b'}')
            self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"), literal)
            self.assertEqual(body["error"]["message"], MESSAGE, literal)
            self.assertNotIn("ETag", resp_headers)
        # Nothing was replaced: the GET still shows revision 1 and the original configuration.
        status, body, headers = self.request("GET", "/v1/limits/fb")
        self.assertEqual(status, 200)
        self.assertEqual(headers["ETag"], '"1"')
        self.assertEqual(body["limit"], {"capacity": 5, "refill_per_second": 2.0})

    def test_legal_if_match_with_non_finite_value_is_400_not_409_and_writes_nothing(self) -> None:
        self.request("PUT", "/v1/limits/fc", {"capacity": 5, "refill_per_second": 2})
        for literal in (b"NaN", b"Infinity", b"-Infinity"):
            status, body, _ = self.raw_put(
                "/v1/limits/fc", b'{"capacity": 5, "refill_per_second": ' + literal + b'}',
                [("If-Match", '"1"')])
            self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"), literal)
        self.assertEqual(self.request("GET", "/v1/limits/fc")[2]["ETag"], '"1"')

    def test_rejected_put_never_creates_an_unconfigured_key(self) -> None:
        for literal in (b"NaN", b"Infinity", b"-Infinity", b"1e999"):
            status, body, _ = self.raw_put(
                "/v1/limits/fc-ghost",
                b'{"capacity": 5, "refill_per_second": ' + literal + b'}')
            self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"), literal)
        self.assertEqual(self.request("GET", "/v1/limits/fc-ghost")[0], 404)
        # It can still be created afterwards as revision 1.
        status, _, headers = self.request("PUT", "/v1/limits/fc-ghost",
                                          {"capacity": 5, "refill_per_second": 1})
        self.assertEqual((status, headers["ETag"]), (200, '"1"'))

    def test_boundary_and_finite_float_rates_remain_accepted(self) -> None:
        for rate in (1, 1000000, 0.5, 0.0001, 999999.999):
            status, _, _ = self.request("PUT", "/v1/limits/fd",
                                        {"capacity": 7, "refill_per_second": rate})
            self.assertEqual(status, 200, rate)
        status, body, _ = self.request("GET", "/v1/limits/fd")
        self.assertEqual(status, 200)
        self.assertEqual(body["limit"]["capacity"], 7)
        self.assertTrue(math.isfinite(body["limit"]["refill_per_second"]))
        # Five loop writes (revisions 1..5) plus one identical re-PUT: revision still advances
        # to 6 even though the rate 0.5 repeats the third loop write's value.
        status, _, headers = self.request("PUT", "/v1/limits/fd",
                                          {"capacity": 7, "refill_per_second": 0.5})
        self.assertEqual((status, headers["ETag"]), (200, '"6"'))


if __name__ == "__main__":
    unittest.main()
