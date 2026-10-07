"""Optimistic concurrency for PUT /v1/limits/{key}: per-key revision, ETag and If-Match.

Time is injected, so the "a rejected precondition never samples the clock / never settles a
reservation" guarantees are testable deterministically, exactly like the baseline suites.
"""
from __future__ import annotations

import json
import threading
import unittest
import urllib.error
import urllib.request

from quota import (
    InvalidRequest,
    LimitNotFound,
    Limiter,
    RevisionConflict,
    etag_header,
    validate_if_match,
)


class Clock:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t


class RevisionUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.limiter = Limiter(self.clock)

    def test_first_creation_is_revision_1(self) -> None:
        result = self.limiter.configure("tenant-a", {"capacity": 3, "refill_per_second": 1.0})
        self.assertEqual(result.revision, 1)
        self.assertEqual((result.limit.capacity, result.limit.refill_per_second), (3, 1.0))
        _, revision = self.limiter.state_snapshot("tenant-a")
        self.assertEqual(revision, 1)
        # state() keeps its baseline dict shape; revision rides alongside only internally.
        self.assertEqual(set(self.limiter.state("tenant-a")), {"limit", "remaining", "used"})

    def test_every_successful_put_increments_revision_even_for_identical_config(self) -> None:
        payload = {"capacity": 3, "refill_per_second": 1.0}
        revisions = [self.limiter.configure("tenant-a", payload).revision for _ in range(5)]
        self.assertEqual(revisions, [1, 2, 3, 4, 5])

    def test_matching_if_match_updates_and_advances_revision(self) -> None:
        self.assertEqual(self.limiter.configure("k", {"capacity": 5, "refill_per_second": 1.0}).revision, 1)
        result = self.limiter.configure("k", {"capacity": 9, "refill_per_second": 2.0}, 1)
        self.assertEqual(result.revision, 2)
        self.assertEqual(result.limit.capacity, 9)
        _, revision = self.limiter.state_snapshot("k")
        self.assertEqual(revision, 2)
        # The new precondition is the installed revision; a stale one is already rejected.
        self.assertEqual(self.limiter.configure("k", {"capacity": 9, "refill_per_second": 2.0}, 2).revision, 3)
        with self.assertRaises(RevisionConflict):
            self.limiter.configure("k", {"capacity": 1, "refill_per_second": 1.0}, 2)

    def test_conflict_changes_neither_revision_nor_configuration(self) -> None:
        self.limiter.configure("k", {"capacity": 5, "refill_per_second": 1.0})
        with self.assertRaises(RevisionConflict):
            self.limiter.configure("k", {"capacity": 9, "refill_per_second": 2.0}, 7)
        self.assertEqual(self.limiter._revisions["k"], 1)
        state, revision = self.limiter.state_snapshot("k")
        self.assertEqual(revision, 1)
        self.assertEqual(state["limit"], {"capacity": 5, "refill_per_second": 1.0})

    def test_conflict_never_samples_the_clock_or_pins_the_watermark(self) -> None:
        self.clock.t = 100.0
        self.limiter.configure("k", {"capacity": 10, "refill_per_second": 1.0})
        self.assertTrue(self.limiter.check("k", 10)["allowed"])       # empty at t=100
        self.clock.t = 103.0
        with self.assertRaises(RevisionConflict):
            self.limiter.configure("k", {"capacity": 10, "refill_per_second": 1.0}, 999)
        self.clock.t = 102.0                                          # regressed below the rejected read
        # A ticking conflict would have pinned the watermark to 103 (3 refilled tokens); the
        # conflict never ticked, so only 100 -> 102 counts.
        self.assertEqual(self.limiter.state("k")["remaining"], 2)

    def test_conflict_settles_no_due_reservation_and_refunds_nothing(self) -> None:
        self.clock.t = 100.0
        self.limiter.configure("k", {"capacity": 5, "refill_per_second": 0.0001})
        rid = self.limiter.reserve("k", 3, ttl_seconds=10)["reservation_id"]  # tokens 2, due at 110
        # A same-instant unconditional PUT advances the revision without refilling or settling.
        self.assertEqual(
            self.limiter.configure("k", {"capacity": 5, "refill_per_second": 0.0001}).revision, 2)
        self.clock.t = 110.0
        with self.assertRaises(RevisionConflict):
            self.limiter.configure("k", {"capacity": 5, "refill_per_second": 0.0001}, 1)
        # The due hold is still live: the conflict neither settled it nor touched the bucket.
        self.assertIn(rid, self.limiter._reservations)
        self.assertAlmostEqual(self.limiter._buckets["k"].tokens, 2.0, places=6)
        # An unconditional PUT then performs the one settle the conflict skipped.
        self.limiter.configure("k", {"capacity": 5, "refill_per_second": 0.0001})
        self.assertNotIn(rid, self.limiter._reservations)
        self.assertEqual(self.limiter.state("k")["remaining"], 5)

    def test_conflict_touches_no_used_ledger_or_decision_counts(self) -> None:
        self.limiter.configure("k", {"capacity": 5, "refill_per_second": 1.0})
        self.assertTrue(self.limiter.check("k", 2)["allowed"])
        before_metrics = self.limiter.metrics()
        before_ledger = self.limiter.ledger("k", 1000)
        with self.assertRaises(RevisionConflict):
            self.limiter.configure("k", {"capacity": 9, "refill_per_second": 2.0}, 42)
        self.assertEqual(self.limiter.state("k")["used"], 2)
        self.assertEqual(self.limiter.ledger("k", 1000), before_ledger)
        self.assertEqual(self.limiter.metrics(), before_metrics)

    def test_legal_if_match_on_unconfigured_key_is_404_and_creates_nothing(self) -> None:
        self.clock.t = 100.0
        with self.assertRaises(LimitNotFound):
            self.limiter.configure("ghost", {"capacity": 5, "refill_per_second": 1.0}, 1)
        self.assertNotIn("ghost", self.limiter._limits)
        self.assertNotIn("ghost", self.limiter._revisions)
        self.clock.t = 99.0                                           # the 404 never ticked at 100
        result = self.limiter.configure("ghost", {"capacity": 5, "refill_per_second": 1.0})
        self.assertEqual(result.revision, 1)                          # first creation, never "2"
        self.assertEqual(self.limiter._buckets["ghost"].updated_at, 99.0)

    def test_concurrent_conditional_writers_have_exactly_one_winner(self) -> None:
        self.limiter.configure("hot", {"capacity": 10, "refill_per_second": 1.0})  # revision 1
        outcomes: list[bool] = []
        outcomes_lock = threading.Lock()

        def writer() -> None:
            try:
                self.limiter.configure("hot", {"capacity": 20, "refill_per_second": 2.0}, 1)
                won = True
            except RevisionConflict:
                won = False
            with outcomes_lock:
                outcomes.append(won)

        threads = [threading.Thread(target=writer) for _ in range(32)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(sum(outcomes), 1)
        self.assertEqual(self.limiter._revisions["hot"], 2)
        # The winner installed revision 2, which the losers can now use to retry successfully.
        self.assertEqual(
            self.limiter.configure("hot", {"capacity": 30, "refill_per_second": 3.0}, 2).revision, 3)

    def test_window_with_the_same_name_has_no_revision_and_is_isolated(self) -> None:
        self.limiter.configure("shared", {"capacity": 5, "refill_per_second": 1.0})
        self.limiter.configure_window("shared", {"window_seconds": 10, "max_events": 2})
        self.assertEqual(set(self.limiter._revisions), {"shared"})
        # Window reconfiguration never bumps the bucket key's revision.
        self.limiter.configure_window("shared", {"window_seconds": 20, "max_events": 4})
        self.assertEqual(self.limiter.state_snapshot("shared")[1], 1)


class IfMatchValidationUnitTests(unittest.TestCase):
    def test_accepted_shapes(self) -> None:
        for header, revision in [('"1"', 1), ('"2"', 2), ('"123456789"', 123456789)]:
            self.assertEqual(validate_if_match(header), revision, header)

    def test_rejected_shapes(self) -> None:
        bad = [
            None, "", '"', '"""', 3, '3', '"0"', '"-1"', '"+1"', '"1.5"', '" 1"', '"1 "',
            '"1\t2"', '"*"', '"1,2"', 'W/"1"', 'w/"1"', '"01"', '"007"', '"01 "', '"0x1"',
            '"1e2"', '" 1"', " '1'",
        ]
        for header in bad:
            with self.assertRaises(InvalidRequest, msg=repr(header)):
                validate_if_match(header)

    def test_etag_rendering(self) -> None:
        self.assertEqual(etag_header(1), '"1"')
        self.assertEqual(etag_header(42), '"42"')


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
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
                                         headers={"Content-Type": "application/json", **(headers or {})})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read() or b"{}"), dict(response.headers)
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read() or b"{}"), dict(error.headers)

    def raw_request(self, method: str, path: str, payload: bytes | None,
                    match_headers: list[tuple[str, str]] | None) -> tuple[int, dict, dict]:
        """Send a limit PUT with full control over the raw If-Match header line(s)."""
        import http.client

        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        connection.putrequest(method, path)
        connection.putheader("Content-Length", str(len(payload) if payload is not None else 0))
        for name, value in match_headers or []:
            connection.putheader(name, value)
        connection.endheaders(payload if payload is not None else b"")
        response = connection.getresponse()
        parsed = json.loads(response.read() or b"{}")
        headers = dict(response.headers)
        connection.close()
        return response.status, parsed, headers

    def test_put_and_get_carry_etag_and_revision_increments_every_write(self) -> None:
        status, body, headers = self.request("PUT", "/v1/limits/et-1",
                                             {"capacity": 4, "refill_per_second": 1})
        self.assertEqual(status, 200)
        self.assertEqual(body, {"key": "et-1", "limit": {"capacity": 4, "refill_per_second": 1.0}})
        self.assertEqual(headers["ETag"], '"1"')

        status, _, headers = self.request("PUT", "/v1/limits/et-1",
                                          {"capacity": 8, "refill_per_second": 2})
        self.assertEqual((status, headers["ETag"]), (200, '"2"'))

        # An identical re-PUT still advances the revision.
        status, _, headers = self.request("PUT", "/v1/limits/et-1",
                                          {"capacity": 8, "refill_per_second": 2})
        self.assertEqual((status, headers["ETag"]), (200, '"3"'))

        status, body, headers = self.request("GET", "/v1/limits/et-1")
        self.assertEqual(status, 200)
        self.assertEqual(headers["ETag"], '"3"')
        self.assertEqual(body["limit"], {"capacity": 8, "refill_per_second": 2.0})
        self.assertNotIn("revision", body)                        # JSON shape gains no field

        # A pure read never advances the revision.
        self.assertEqual(self.request("GET", "/v1/limits/et-1")[2]["ETag"], '"3"')

    def test_get_unknown_key_is_404_without_etag(self) -> None:
        status, body, headers = self.request("GET", "/v1/limits/et-missing")
        self.assertEqual((status, body["error"]["code"]), (404, "not_found"))
        self.assertNotIn("ETag", headers)

    def test_matching_if_match_is_accepted(self) -> None:
        self.request("PUT", "/v1/limits/et-2", {"capacity": 4, "refill_per_second": 1})
        status, _, headers = self.request("PUT", "/v1/limits/et-2",
                                          {"capacity": 9, "refill_per_second": 2},
                                          {"If-Match": '"1"'})
        self.assertEqual(status, 200)
        self.assertEqual(headers["ETag"], '"2"')
        self.assertEqual(self.request("GET", "/v1/limits/et-2")[2]["ETag"], '"2"')

    def test_stale_if_match_is_409_with_the_exact_error_body_and_changes_nothing(self) -> None:
        self.request("PUT", "/v1/limits/et-3", {"capacity": 5, "refill_per_second": 1})
        self.request("POST", "/v1/check", {"key": "et-3", "cost": 2})   # remaining 3, used 2
        status, body, headers = self.request("PUT", "/v1/limits/et-3",
                                             {"capacity": 9, "refill_per_second": 2},
                                             {"If-Match": '"7"'})
        self.assertEqual(status, 409)
        self.assertEqual(body, {"error": {"code": "revision_conflict",
                                          "message": "If-Match revision does not match current configuration"}})
        self.assertNotIn("ETag", headers)
        _, state, get_headers = self.request("GET", "/v1/limits/et-3")
        self.assertEqual(get_headers["ETag"], '"1"')                     # revision untouched
        self.assertEqual(state["limit"], {"capacity": 5, "refill_per_second": 1.0})
        self.assertEqual((state["remaining"], state["used"]), (3, 2))   # tokens and used untouched

    def test_legal_if_match_on_unconfigured_key_is_404_and_not_created(self) -> None:
        status, body, headers = self.request("PUT", "/v1/limits/et-ghost",
                                             {"capacity": 5, "refill_per_second": 1},
                                             {"If-Match": '"1"'})
        self.assertEqual((status, body["error"]["code"]), (404, "not_found"))
        self.assertNotIn("ETag", headers)
        self.assertEqual(self.request("GET", "/v1/limits/et-ghost")[0], 404)
        # It can still be created afterwards, and that creation is revision 1.
        status, _, headers = self.request("PUT", "/v1/limits/et-ghost",
                                          {"capacity": 5, "refill_per_second": 1})
        self.assertEqual((status, headers["ETag"]), (200, '"1"'))

    def test_malformed_if_match_is_400_before_lock_for_configured_and_unknown_keys(self) -> None:
        self.request("PUT", "/v1/limits/et-4", {"capacity": 5, "refill_per_second": 1})
        malformed = ['"0"', '"-1"', '" 1"', '"1 2"', '"*"', 'W/"1"', '"1.5"', '"x"',
                     '"\t"', '1', '"', '""', '"01"']
        for key in ("et-4", "et-never-configured"):
            for value in malformed:
                status, body, _ = self.raw_request(
                    "PUT", f"/v1/limits/{key}", b'{"capacity": 7, "refill_per_second": 1}',
                    [("If-Match", value)])
                self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"),
                                 (key, value))
        # The configured key is untouched and the unconfigured one was never created.
        _, state, headers = self.request("GET", "/v1/limits/et-4")
        self.assertEqual((state["limit"]["capacity"], headers["ETag"]), (5, '"1"'))
        self.assertEqual(self.request("GET", "/v1/limits/et-never-configured")[0], 404)

    def test_repeated_if_match_line_is_400(self) -> None:
        self.request("PUT", "/v1/limits/et-5", {"capacity": 5, "refill_per_second": 1})
        status, body, _ = self.raw_request(
            "PUT", "/v1/limits/et-5", b'{"capacity": 7, "refill_per_second": 1}',
            [("If-Match", '"1"'), ("If-Match", '"1"')])
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        self.assertEqual(self.request("GET", "/v1/limits/et-5")[2]["ETag"], '"1"')

    def test_malformed_if_match_with_malformed_body_is_still_400_before_404(self) -> None:
        status, body, _ = self.raw_request(
            "PUT", "/v1/limits/et-no-such-key", b"{not json", [("If-Match", '"garbage"')])
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        self.assertEqual(self.request("GET", "/v1/limits/et-no-such-key")[0], 404)

    def test_if_match_is_ignored_on_get_and_on_window_routes(self) -> None:
        self.request("PUT", "/v1/limits/et-6", {"capacity": 5, "refill_per_second": 1})
        # GET never implements conditional semantics: the header is simply ignored.
        status, _, headers = self.request("GET", "/v1/limits/et-6", None, {"If-Match": '"999"'})
        self.assertEqual((status, headers["ETag"]), (200, '"1"'))
        # The window protocol is separate: a garbage precondition neither validates nor ETags.
        status, body, headers = self.request("PUT", "/v1/windows/et-6",
                                             {"window_seconds": 10, "max_events": 2},
                                             {"If-Match": '"garbage"'})
        self.assertEqual(status, 200)
        self.assertEqual(body, {"key": "et-6", "window": {"window_seconds": 10, "max_events": 2}})
        self.assertNotIn("ETag", headers)
        self.assertEqual(self.request("GET", "/v1/windows/et-6")[2].get("ETag"), None)
        # The same-named bucket revision and existence are unaffected by the window's namespace.
        self.assertEqual(self.request("GET", "/v1/limits/et-6")[2]["ETag"], '"1"')

    def test_conflict_does_not_settle_a_due_reservation(self) -> None:
        clock = type(self).clock
        clock.t = 5000.0
        self.request("PUT", "/v1/limits/et-7", {"capacity": 5, "refill_per_second": 0.0001})
        _, body, _ = self.request("POST", "/v1/reservations",
                                  {"key": "et-7", "cost": 3, "ttl_seconds": 10})
        rid = body["reservation_id"]
        # A same-instant identical PUT takes the revision to 2 without refilling or settling.
        self.assertEqual(
            self.request("PUT", "/v1/limits/et-7",
                         {"capacity": 5, "refill_per_second": 0.0001})[2]["ETag"],
            '"2"')
        clock.t += 10                                                    # the hold is now due
        status, _, _ = self.request("PUT", "/v1/limits/et-7",
                                    {"capacity": 5, "refill_per_second": 0.0001},
                                    {"If-Match": '"1"'})
        self.assertEqual(status, 409)
        # The stale precondition fails before any clock sample: internally the due hold is still
        # registered and still holding its 3 tokens, the bucket was never refilled either.
        limiter = self.server.limiter
        self.assertIn(rid, limiter._reservations)
        self.assertAlmostEqual(limiter._buckets["et-7"].tokens, 2.0, places=3)
        # The next unconditional PUT performs the one settle the conflict skipped.
        status, _, headers = self.request("PUT", "/v1/limits/et-7",
                                          {"capacity": 5, "refill_per_second": 0.0001})
        self.assertEqual((status, headers["ETag"]), (200, '"3"'))
        self.assertNotIn(rid, limiter._reservations)
        _, state, _ = self.request("GET", "/v1/limits/et-7")
        self.assertEqual((state["remaining"], state["used"]), (5, 0))

    def test_concurrent_stale_writers_have_one_winner_and_a_consistent_get_snapshot(self) -> None:
        self.request("PUT", "/v1/limits/et-hot", {"capacity": 5, "refill_per_second": 1})
        statuses: list[int] = []
        statuses_lock = threading.Lock()

        def stale_put() -> None:
            status, _, _ = self.request("PUT", "/v1/limits/et-hot",
                                        {"capacity": 10, "refill_per_second": 1},
                                        {"If-Match": '"1"'})
            with statuses_lock:
                statuses.append(status)

        threads = [threading.Thread(target=stale_put) for _ in range(16)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(sorted(statuses), [200] + [409] * 15)
        self.assertEqual(self.request("GET", "/v1/limits/et-hot")[2]["ETag"], '"2"')

    def test_concurrent_gets_never_see_an_etag_split_from_its_snapshot(self) -> None:
        # Revision N is installed with exactly one capacity; a torn GET would pair an ETag with a
        # capacity from a different revision. Collect (etag -> capacity) and demand a strict mapping.
        self.request("PUT", "/v1/limits/et-tear", {"capacity": 5, "refill_per_second": 1})
        observed: dict[str, int] = {}
        errors: list[BaseException] = []
        observed_lock = threading.Lock()
        stop = threading.Event()

        def writer(step: int) -> None:
            capacity = 10 if step % 2 == 0 else 20
            status, _, headers = self.request("PUT", "/v1/limits/et-tear",
                                              {"capacity": capacity, "refill_per_second": 1})
            with observed_lock:
                etag = headers["ETag"]
                if status != 200 or etag in observed and observed[etag] != capacity:
                    errors.append(AssertionError(f"PUT torn: {etag} -> {capacity}"))
                observed.setdefault(etag, capacity)

        def reader() -> None:
            while not stop.is_set():
                try:
                    _, body, headers = self.request("GET", "/v1/limits/et-tear")
                    etag, capacity = headers["ETag"], body["limit"]["capacity"]
                    with observed_lock:
                        if etag == '"1"':
                            if capacity != 5:
                                raise AssertionError("revision 1 changed shape")
                        elif etag in observed and observed[etag] != capacity:
                            raise AssertionError(f"GET torn: {etag} paired with {capacity}")
                        observed.setdefault(etag, capacity)
                except BaseException as error:  # noqa: BLE001 - surface on the main thread
                    with observed_lock:
                        errors.append(error)

        readers = [threading.Thread(target=reader) for _ in range(6)]
        for thread in readers:
            thread.start()
        for step in range(40):
            writer(step)
        stop.set()
        for thread in readers:
            thread.join()
        self.assertEqual(errors, [])
        self.assertEqual(observed['"1"'], 5)


if __name__ == "__main__":
    unittest.main()
