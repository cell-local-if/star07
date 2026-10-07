"""Creation idempotency for POST /v1/reservations via the optional Idempotency-Key header.

The binding pins the first successful create's exact parameters (key, cost, ttl_seconds) and its
exact first 200 response: identical retries replay without occupying tokens a second time,
mismatched retries are 409 idempotency_conflict, failed creates bind nothing, and the binding
outlives the reservation's own lifecycle. Everything is process-local.
"""
from __future__ import annotations

import http.client
import json
import threading
import time
import unittest
import urllib.error
import urllib.request

from quota import (
    IdempotencyConflict,
    InvalidRequest,
    Limiter,
    LimitNotFound,
    OverQuota,
    serve,
    validate_idempotency_key,
)


class Clock:
    def __init__(self, t: float = 1000.0) -> None:
        self.t = t
        self.calls = 0

    def __call__(self) -> float:
        self.calls += 1
        return self.t


class IdempotencyKeyValidationTests(unittest.TestCase):
    def accepts(self, value: str) -> None:
        self.assertEqual(validate_idempotency_key(value), value)

    def rejects(self, value: object) -> None:
        with self.assertRaises(InvalidRequest):
            validate_idempotency_key(value)

    def test_bounds_are_1_to_128(self) -> None:
        self.accepts("a")
        self.accepts("a" * 128)
        self.rejects("")
        self.rejects("a" * 129)

    def test_printable_ascii_punctuation_is_accepted(self) -> None:
        self.accepts("Az09-._~+/=:;,.!?@#$%^&*()[]{}|")

    def test_whitespace_and_controls_are_rejected(self) -> None:
        for value in ("a b", "ab ", "a\tb", "a\nb", "a\rb", "a\vb", "a\x00b", "a\x1fb",
                      "a\x7fb", " ", "\t"):
            self.rejects(value)

    def test_non_ascii_is_rejected(self) -> None:
        for value in ("café", "键", "a🎉b"):
            self.rejects(value)

    def test_non_string_values_are_rejected(self) -> None:
        for value in (None, 1, b"abc", ["abc"], {"k": "v"}):
            self.rejects(value)


class ReservationIdempotencyUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.limiter = Limiter(self.clock)
        self.limiter.configure("tenant-a", {"capacity": 5, "refill_per_second": 1.0})

    def decisions(self) -> dict:
        return self.limiter.metrics()["metrics"]["decisions"]["reservation"]

    def test_first_create_binds_and_replay_is_identical_without_a_second_occupation(self) -> None:
        first = self.limiter.reserve_idempotent("idem-1", "tenant-a", 3)
        self.assertEqual(first["cost"], 3)
        state = self.limiter.state("tenant-a")
        self.assertEqual((state["remaining"], state["used"]), (2, 0))
        calls_after_create = self.clock.calls

        second = self.limiter.reserve_idempotent("idem-1", "tenant-a", 3)
        self.assertEqual(second, first)                    # frozen first response, field for field
        # Replay samples no clock, settles nothing, deducts nothing and counts no decision.
        self.assertEqual(self.clock.calls, calls_after_create)
        state = self.limiter.state("tenant-a")
        self.assertEqual((state["remaining"], state["used"]), (2, 0))
        self.assertEqual(self.decisions(), {"allowed": 1, "over_quota": 0})
        self.assertEqual(len(self.limiter._reservations), 1)
        self.assertEqual(self.limiter.ledger("tenant-a")["events"], [])

    def test_explicit_default_ttl_matches_an_omitted_one(self) -> None:
        first = self.limiter.reserve_idempotent("idem-ttl", "tenant-a", 1)
        self.assertEqual(self.limiter.reserve_idempotent("idem-ttl", "tenant-a", 1, 60), first)
        with self.assertRaises(IdempotencyConflict):
            self.limiter.reserve_idempotent("idem-ttl", "tenant-a", 1, 30)

    def test_different_cost_is_conflict_and_changes_nothing(self) -> None:
        first = self.limiter.reserve_idempotent("idem-2", "tenant-a", 2)
        calls = self.clock.calls
        with self.assertRaises(IdempotencyConflict):
            self.limiter.reserve_idempotent("idem-2", "tenant-a", 1)
        self.assertEqual(self.clock.calls, calls)           # conflict samples no clock
        state = self.limiter.state("tenant-a")
        self.assertEqual((state["remaining"], state["used"]), (3, 0))
        self.assertEqual(len(self.limiter._reservations), 1)
        self.assertEqual(self.decisions(), {"allowed": 1, "over_quota": 0})
        # The original response still replays, unchanged.
        self.assertEqual(self.limiter.reserve_idempotent("idem-2", "tenant-a", 2), first)

    def test_different_key_is_conflict_even_when_the_other_key_is_unconfigured(self) -> None:
        self.limiter.reserve_idempotent("idem-3", "tenant-a", 1)
        # 409 wins over what would otherwise be a 404 for the unconfigured key.
        with self.assertRaises(IdempotencyConflict):
            self.limiter.reserve_idempotent("idem-3", "tenant-other", 1)
        with self.assertRaises(IdempotencyConflict):
            self.limiter.reserve_idempotent("idem-3", "tenant-a", 1, 10)
        self.assertEqual(self.decisions(), {"allowed": 1, "over_quota": 0})

    def test_differing_but_legal_over_capacity_cost_is_conflict_not_unsatisfiable_400(self) -> None:
        # The differing-parameters rule is evaluated inside the lock against the stored binding,
        # ahead of the capacity boundary: a syntactically legal cost the key could never hold is
        # still 409 on a bound header (a free header with the same body stays a baseline 400).
        self.limiter.reserve_idempotent("idem-cap2", "tenant-a", 1)
        with self.assertRaises(IdempotencyConflict):
            self.limiter.reserve_idempotent("idem-cap2", "tenant-a", 6)
        with self.assertRaises(InvalidRequest):
            self.limiter.reserve_idempotent("free-cap2", "tenant-a", 6)
        self.assertEqual(self.decisions(), {"allowed": 1, "over_quota": 0})

    def test_conflict_settles_no_due_reservation(self) -> None:
        due = self.limiter.reserve("tenant-a", 1, ttl_seconds=5)
        self.limiter.reserve_idempotent("idem-due", "tenant-a", 1)
        self.clock.t += 10                                  # the sibling hold is now due
        with self.assertRaises(IdempotencyConflict):
            self.limiter.reserve_idempotent("idem-due", "tenant-a", 2)
        # Conflict returns before the lazy settle, so the due hold is still registered.
        self.assertIn(due["reservation_id"], self.limiter._reservations)

    def test_unknown_key_does_not_bind_and_a_later_legal_use_creates(self) -> None:
        with self.assertRaises(LimitNotFound):
            self.limiter.reserve_idempotent("idem-404", "missing", 1)
        with self.assertRaises(LimitNotFound):
            self.limiter.reserve_idempotent("idem-404", "missing", 1)  # still a fresh 404
        result = self.limiter.reserve_idempotent("idem-404", "tenant-a", 1)
        self.assertIn("reservation_id", result)
        self.assertEqual(self.decisions(), {"allowed": 1, "over_quota": 0})

    def test_unsatisfiable_cost_does_not_bind(self) -> None:
        with self.assertRaises(InvalidRequest):
            self.limiter.reserve_idempotent("idem-cap", "tenant-a", 6)  # over capacity 5
        result = self.limiter.reserve_idempotent("idem-cap", "tenant-a", 2)
        self.assertIn("reservation_id", result)
        self.assertEqual(self.decisions(), {"allowed": 1, "over_quota": 0})

    def test_over_quota_does_not_bind_and_retry_after_refill_creates(self) -> None:
        self.limiter.reserve("tenant-a", 5)                 # empty the bucket
        with self.assertRaises(OverQuota):
            self.limiter.reserve_idempotent("idem-429", "tenant-a", 1)
        self.assertEqual(self.decisions(), {"allowed": 1, "over_quota": 1})
        self.clock.t += 5                                   # full refill
        result = self.limiter.reserve_idempotent("idem-429", "tenant-a", 1)
        self.assertIn("reservation_id", result)
        self.assertEqual(self.decisions(), {"allowed": 2, "over_quota": 1})

    def test_invalid_parameters_reach_no_binding_table(self) -> None:
        for bad_cost in (0, True, 1.5, "1"):
            with self.assertRaises(InvalidRequest):
                self.limiter.reserve_idempotent("idem-bad", "tenant-a", bad_cost)
        for bad_ttl in (0, 86401, True, 1.5, "60"):
            with self.assertRaises(InvalidRequest):
                self.limiter.reserve_idempotent("idem-bad", "tenant-a", 1, bad_ttl)
        # A later legal request with the same header value is still the first use.
        result = self.limiter.reserve_idempotent("idem-bad", "tenant-a", 1)
        self.assertIn("reservation_id", result)
        self.assertEqual(self.limiter._reservation_idempotency_inflight, {})

    def test_binding_survives_consume_and_keeps_replaying_the_create_response(self) -> None:
        first = self.limiter.reserve_idempotent("idem-life", "tenant-a", 2)
        consumed = self.limiter.consume(first["reservation_id"])
        self.assertEqual(consumed["used"], 2)
        self.clock.t += 100                                 # far past the 60s TTL
        replay = self.limiter.reserve_idempotent("idem-life", "tenant-a", 2)
        self.assertEqual(replay, first)                    # create response frozen, used not re-booked
        state = self.limiter.state("tenant-a")
        self.assertEqual(state["used"], 2)
        self.assertEqual(self.limiter.ledger("tenant-a")["totals"],
                         {"accepted_count": 1, "accepted_cost": 2})

    def test_binding_survives_rollback(self) -> None:
        first = self.limiter.reserve_idempotent("idem-rb", "tenant-a", 2, ttl_seconds=3600)
        self.assertEqual(self.limiter.rollback(first["reservation_id"])["rolled_back"], True)
        replay = self.limiter.reserve_idempotent("idem-rb", "tenant-a", 2, ttl_seconds=3600)
        self.assertEqual(replay, first)                    # no new hold, tokens stay refunded
        state = self.limiter.state("tenant-a")
        self.assertEqual((state["remaining"], state["used"]), (5, 0))

    def test_binding_survives_expiry_settlement(self) -> None:
        first = self.limiter.reserve_idempotent("idem-exp", "tenant-a", 2, ttl_seconds=10)
        self.assertEqual(self.limiter.state("tenant-a")["remaining"], 3)
        self.clock.t += 10
        settled = self.limiter.state("tenant-a")           # the one lazy refund happens here
        self.assertEqual(settled["remaining"], 5)
        replay = self.limiter.reserve_idempotent("idem-exp", "tenant-a", 2, ttl_seconds=10)
        self.assertEqual(replay, first)
        self.assertEqual(self.limiter.state("tenant-a")["remaining"], 5)  # replay re-occupies nothing
        self.assertEqual(self.decisions(), {"allowed": 1, "over_quota": 0})

    def test_replay_does_not_settle_but_a_later_state_read_does(self) -> None:
        first = self.limiter.reserve_idempotent("idem-nosettle", "tenant-a", 2, ttl_seconds=10)
        self.clock.t += 10
        self.limiter.reserve_idempotent("idem-nosettle", "tenant-a", 2, ttl_seconds=10)
        # The replay sampled no clock and ran no settle: the expired hold is still registered.
        self.assertIn(first["reservation_id"], self.limiter._reservations)
        self.assertIn(first["reservation_id"], self.limiter._reservations)
        self.assertEqual(self.limiter.state("tenant-a")["remaining"], 5)

    def test_keyless_reserve_remains_server_deduplicated(self) -> None:
        first = self.limiter.reserve("tenant-a", 1)
        second = self.limiter.reserve("tenant-a", 1)
        self.assertNotEqual(first["reservation_id"], second["reservation_id"])
        self.assertEqual(self.decisions()["allowed"], 2)

    def test_concurrent_identical_requests_converge_to_one_occupation(self) -> None:
        limiter = Limiter(Clock())
        limiter.configure("hot", {"capacity": 10, "refill_per_second": 0.0001})
        responses: list[dict] = []
        errors: list[BaseException] = []
        list_lock = threading.Lock()

        def create() -> None:
            try:
                response = limiter.reserve_idempotent("same-header", "hot", 1)
                with list_lock:
                    responses.append(response)
            except BaseException as error:  # noqa: BLE001 - surface worker failures
                with list_lock:
                    errors.append(error)

        threads = [threading.Thread(target=create) for _ in range(64)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        self.assertEqual(len(responses), 64)
        self.assertEqual({response["reservation_id"] for response in responses},
                         {responses[0]["reservation_id"]})
        self.assertEqual(limiter.metrics()["metrics"]["decisions"]["reservation"],
                         {"allowed": 1, "over_quota": 0})
        self.assertEqual(limiter.state("hot")["remaining"], 9)
        self.assertEqual(limiter._reservation_idempotency_inflight, {})

    def test_concurrent_mixed_parameters_have_one_occupation_and_other_params_conflict(self) -> None:
        limiter = Limiter(Clock())
        limiter.configure("hot", {"capacity": 10, "refill_per_second": 1.0})
        replay_ids: list[str] = []
        conflicts = 0
        counters_lock = threading.Lock()

        def create(cost: int) -> None:
            nonlocal conflicts
            try:
                response = limiter.reserve_idempotent("mixed-header", "hot", cost)
                with counters_lock:
                    replay_ids.append(response["reservation_id"])
            except IdempotencyConflict:
                with counters_lock:
                    conflicts += 1

        threads = [threading.Thread(target=create, args=(1 if index % 2 else 2,))
                   for index in range(40)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        # Exactly one side is the first use: its 20 callers all share one id, the other 20 conflict,
        # and only the single first create ever occupied tokens or counted.
        self.assertEqual(len(set(replay_ids)), 1)
        self.assertEqual(len(replay_ids), 20)
        self.assertEqual(conflicts, 20)
        self.assertEqual(limiter.metrics()["metrics"]["decisions"]["reservation"],
                         {"allowed": 1, "over_quota": 0})
        # The one occupation cost either 1 or 2 depending solely on which thread won the election.
        self.assertIn(limiter.state("hot")["remaining"], (8, 9))

    def test_concurrent_failed_leader_releases_every_waiter_and_binds_nothing(self) -> None:
        clock = Clock()
        limiter = Limiter(clock)
        limiter.configure("hot", {"capacity": 1, "refill_per_second": 0.0001})
        limiter.reserve("hot", 1, ttl_seconds=3600)                # bucket is now full: every attempt 429s
        errors: list[str] = []
        errors_lock = threading.Lock()

        def create() -> None:
            try:
                limiter.reserve_idempotent("failing-header", "hot", 1)
            except OverQuota:
                with errors_lock:
                    errors.append("429")
            except BaseException as error:  # noqa: BLE001
                with errors_lock:
                    errors.append(repr(error))

        threads = [threading.Thread(target=create) for _ in range(32)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        # Each waiter gets elected in turn after the previous leader failed; all see over quota.
        self.assertEqual(errors, ["429"] * 32)
        self.assertEqual(limiter.metrics()["metrics"]["decisions"]["reservation"],
                         {"allowed": 1, "over_quota": 32})
        self.assertEqual(limiter._reservation_idempotency, {})
        self.assertEqual(limiter._reservation_idempotency_inflight, {})
        # Once quota returns, the header value is still free for a legal first create.
        clock.t += 3600
        self.assertIn("reservation_id",
                      limiter.reserve_idempotent("failing-header", "hot", 1))

    def test_concurrent_unknown_key_all_404_and_value_stays_free(self) -> None:
        limiter = Limiter(Clock())
        statuses: list[str] = []
        statuses_lock = threading.Lock()

        def create() -> None:
            try:
                limiter.reserve_idempotent("missing-header", "nope", 1)
            except LimitNotFound:
                with statuses_lock:
                    statuses.append("404")

        threads = [threading.Thread(target=create) for _ in range(32)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(statuses, ["404"] * 32)
        self.assertEqual(limiter._reservation_idempotency_inflight, {})
        limiter.configure("late", {"capacity": 1, "refill_per_second": 1.0})
        self.assertIn("reservation_id",
                      limiter.reserve_idempotent("missing-header", "late", 1))


class ReservationIdempotencyHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.clock = Clock()
        cls.server = serve(port=0, now=cls.clock)
        cls.port = cls.server.server_address[1]
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()

    def request(self, path: str, body: dict | None,
                idempotency_key: str | object = None) -> tuple[int, dict, dict]:
        headers = {"Content-Type": "application/json"}
        if idempotency_key is not None:
            headers["Idempotency-Key"] = idempotency_key
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}", data=data, method="POST", headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read() or b"{}"), dict(response.headers)
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read() or b"{}"), dict(error.headers)

    def raw_reservation(self, payload: bytes, raw_headers: list[tuple[str, str]] | None = None,
                        content_length: str | object = "auto") -> tuple[int, dict, dict]:
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        connection.putrequest("POST", "/v1/reservations")
        if content_length != "omit":
            connection.putheader("Content-Length",
                                 str(len(payload)) if content_length == "auto" else content_length)
        for name, value in raw_headers or []:
            connection.putheader(name, value)
        connection.endheaders(payload)
        response = connection.getresponse()
        body = json.loads(response.read() or b"{}")
        headers = dict(response.headers)
        connection.close()
        return response.status, body, headers

    def state(self, key: str) -> dict:
        with urllib.request.urlopen(f"http://127.0.0.1:{self.port}/v1/limits/{key}", timeout=5) as r:
            return json.loads(r.read())

    def reservation_allowed_metric(self) -> int:
        with urllib.request.urlopen(f"http://127.0.0.1:{self.port}/v1/metrics", timeout=5) as response:
            metrics = json.loads(response.read())
        return metrics["metrics"]["decisions"]["reservation"]["allowed"]

    def test_request_without_header_keeps_baseline_behaviour(self) -> None:
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        conn.request("PUT", "/v1/limits/h-base",
                     body=json.dumps({"capacity": 2, "refill_per_second": 1}),
                     headers={"Content-Type": "application/json"})
        self.assertEqual(conn.getresponse().status, 200)
        conn.close()
        first = self.request("/v1/reservations", {"key": "h-base", "cost": 1})
        second = self.request("/v1/reservations", {"key": "h-base", "cost": 1})
        self.assertEqual((first[0], second[0]), (200, 200))
        self.assertNotEqual(first[1]["reservation_id"], second[1]["reservation_id"])

    def test_identical_idempotent_retry_replays_with_one_occupation(self) -> None:
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        conn.request("PUT", "/v1/limits/h-1",
                     body=json.dumps({"capacity": 3, "refill_per_second": 1}),
                     headers={"Content-Type": "application/json"})
        conn.getresponse().read()
        conn.close()
        status, first, headers = self.request(
            "/v1/reservations", {"key": "h-1", "cost": 2}, idempotency_key="key-h-1")
        self.assertEqual(status, 200)
        self.assertNotIn("Retry-After", headers)
        self.assertEqual((self.state("h-1")["remaining"], self.state("h-1")["used"]), (1, 0))
        allowed_before = self.reservation_allowed_metric()

        status, second, _ = self.request(
            "/v1/reservations", {"key": "h-1", "cost": 2}, idempotency_key="key-h-1")
        self.assertEqual(status, 200)
        self.assertEqual(second, first)
        self.assertEqual((self.state("h-1")["remaining"], self.state("h-1")["used"]), (1, 0))
        self.assertEqual(self.reservation_allowed_metric() - allowed_before, 0)

    def test_different_parameters_return_409_and_change_nothing(self) -> None:
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        conn.request("PUT", "/v1/limits/h-2",
                     body=json.dumps({"capacity": 4, "refill_per_second": 1}),
                     headers={"Content-Type": "application/json"})
        conn.getresponse().read()
        conn.close()
        status, first, _ = self.request(
            "/v1/reservations", {"key": "h-2", "cost": 2}, idempotency_key="key-h-2")
        self.assertEqual(status, 200)
        for body in ({"key": "h-2", "cost": 1}, {"key": "h-2", "cost": 2, "ttl_seconds": 30},
                     {"key": "elsewhere", "cost": 2}):
            status, error, headers = self.request(
                "/v1/reservations", body, idempotency_key="key-h-2")
            self.assertEqual((status, error["error"]["code"]), (409, "idempotency_conflict"), body)
            self.assertNotIn("Retry-After", headers)
        state = self.state("h-2")
        self.assertEqual((state["remaining"], state["used"]), (2, 0))
        # Original still replays byte for byte.
        status, replay, _ = self.request(
            "/v1/reservations", {"key": "h-2", "cost": 2}, idempotency_key="key-h-2")
        self.assertEqual((status, replay), (200, first))

    def test_malformed_headers_are_400_before_the_body_is_adopted(self) -> None:
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        conn.request("PUT", "/v1/limits/h-3",
                     body=json.dumps({"capacity": 2, "refill_per_second": 1}),
                     headers={"Content-Type": "application/json"})
        conn.getresponse().read()
        conn.close()
        valid_body = b'{"key": "h-3", "cost": 1}'
        cases: list[tuple[str, list[tuple[str, str]], bytes]] = [
            ("empty", [("Idempotency-Key", "")], valid_body),
            ("duplicate", [("Idempotency-Key", "a"), ("Idempotency-Key", "b")], valid_body),
            ("too long", [("Idempotency-Key", "a" * 129)], valid_body),
            ("inner space", [("Idempotency-Key", "a b")], valid_body),
            ("trailing space", [("Idempotency-Key", "ab ")], valid_body),
            ("tab", [("Idempotency-Key", "a\tb")], valid_body),
            ("non-ascii", [("Idempotency-Key", "caf\xc3\xa9")], valid_body),
            ("control", [("Idempotency-Key", "a\x01b")], valid_body),
            ("del", [("Idempotency-Key", "a\x7fb")], valid_body),
            # The header error wins even over an unparseable body it is not allowed to adopt.
            ("bad header masks bad body", [("Idempotency-Key", "a b")], b'not json at all'),
        ]
        for label, headers, payload in cases:
            status, body, response_headers = self.raw_reservation(payload, headers)
            self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"), label)
            self.assertNotIn("Retry-After", response_headers, label)
        # Nothing was adopted: the bucket is full and no reservation exists.
        state = self.state("h-3")
        self.assertEqual((state["remaining"], state["used"]), (2, 0))

    def test_valid_header_with_bad_body_is_400_and_binds_nothing(self) -> None:
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        conn.request("PUT", "/v1/limits/h-4",
                     body=json.dumps({"capacity": 2, "refill_per_second": 1}),
                     headers={"Content-Type": "application/json"})
        conn.getresponse().read()
        conn.close()
        for payload in (b'{"key": "h-4", "cost": true}', b'{"key": "", "cost": 1}',
                        b'{"key": "h-4", "cost": 1, "extra": 2}', b'[]', b'null'):
            status, body, _ = self.raw_reservation(payload, [("Idempotency-Key", "bind-h-4")])
            self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"), payload)
        status, first, _ = self.request(
            "/v1/reservations", {"key": "h-4", "cost": 1}, idempotency_key="bind-h-4")
        self.assertEqual(status, 200)
        self.assertEqual(self.state("h-4")["remaining"], 1)

    def test_structurally_bad_body_is_400_even_when_the_header_is_already_bound(self) -> None:
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        conn.request("PUT", "/v1/limits/h-4b",
                     body=json.dumps({"capacity": 2, "refill_per_second": 1}),
                     headers={"Content-Type": "application/json"})
        conn.getresponse().read()
        conn.close()
        status, first, _ = self.request(
            "/v1/reservations", {"key": "h-4b", "cost": 1}, idempotency_key="bind-h-4b")
        self.assertEqual(status, 200)
        # Body shape validation runs in the handler before the limiter sees the bound header, so
        # a malformed replay is 400 — not a replay and not a 409 — and the binding is untouched.
        for payload in (b'{"key": "h-4b", "cost": 1, "extra": 2}', b'[]', b'null'):
            status, body, _ = self.raw_reservation(payload, [("Idempotency-Key", "bind-h-4b")])
            self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"), payload)
        status, replay, _ = self.request(
            "/v1/reservations", {"key": "h-4b", "cost": 1}, idempotency_key="bind-h-4b")
        self.assertEqual((status, replay), (200, first))
        self.assertEqual(self.state("h-4b")["remaining"], 1)         # only the one occupation

    def test_unknown_key_and_over_quota_do_not_bind(self) -> None:
        status, body, _ = self.request(
            "/v1/reservations", {"key": "h-missing", "cost": 1}, idempotency_key="bind-miss")
        self.assertEqual((status, body["error"]["code"]), (404, "not_found"))
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        conn.request("PUT", "/v1/limits/h-5",
                     body=json.dumps({"capacity": 1, "refill_per_second": 1}),
                     headers={"Content-Type": "application/json"})
        conn.getresponse().read()
        conn.close()
        status, _, headers = self.request(
            "/v1/reservations", {"key": "h-5", "cost": 1}, idempotency_key="bind-429")
        self.assertEqual(status, 200)
        status, body, headers = self.request(
            "/v1/reservations", {"key": "h-5", "cost": 1}, idempotency_key="bind-429b")
        self.assertEqual((status, body["error"]["code"]), (429, "over_quota"))
        self.assertIn("Retry-After", headers)
        type(self).clock.t += 1
        status, success, _ = self.request(
            "/v1/reservations", {"key": "h-5", "cost": 1}, idempotency_key="bind-429b")
        self.assertEqual(status, 200)
        self.assertIn("reservation_id", success)

    def test_binding_replays_after_rollback(self) -> None:
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        conn.request("PUT", "/v1/limits/h-6",
                     body=json.dumps({"capacity": 3, "refill_per_second": 1}),
                     headers={"Content-Type": "application/json"})
        conn.getresponse().read()
        conn.close()
        status, first, _ = self.request(
            "/v1/reservations", {"key": "h-6", "cost": 2, "ttl_seconds": 3600},
            idempotency_key="bind-h-6")
        self.assertEqual(status, 200)
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        connection.request("DELETE", f"/v1/reservations/{first['reservation_id']}")
        response = connection.getresponse()
        self.assertEqual(response.status, 200)
        response.read()
        connection.close()
        self.assertEqual(self.state("h-6")["remaining"], 3)
        status, replay, _ = self.request(
            "/v1/reservations", {"key": "h-6", "cost": 2, "ttl_seconds": 3600},
            idempotency_key="bind-h-6")
        self.assertEqual((status, replay), (200, first))
        self.assertEqual(self.state("h-6")["remaining"], 3)          # replay did not re-occupy

    def test_binding_replays_after_expiry(self) -> None:
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        conn.request("PUT", "/v1/limits/h-7",
                     body=json.dumps({"capacity": 3, "refill_per_second": 0.0001}),
                     headers={"Content-Type": "application/json"})
        conn.getresponse().read()
        conn.close()
        status, first, _ = self.request(
            "/v1/reservations", {"key": "h-7", "cost": 2, "ttl_seconds": 10},
            idempotency_key="bind-h-7")
        self.assertEqual(status, 200)
        type(self).clock.t += 10
        self.assertEqual(self.state("h-7")["remaining"], 3)         # expiry settles on the read
        status, replay, _ = self.request(
            "/v1/reservations", {"key": "h-7", "cost": 2, "ttl_seconds": 10},
            idempotency_key="bind-h-7")
        self.assertEqual((status, replay), (200, first))

    def test_header_is_ignored_on_other_routes(self) -> None:
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        conn.request("PUT", "/v1/limits/h-8",
                     body=json.dumps({"capacity": 2, "refill_per_second": 1}),
                     headers={"Content-Type": "application/json"})
        conn.getresponse().read()
        conn.close()
        # /v1/check accepts the same header name but treats it as irrelevant.
        status, body, _ = self.request(
            "/v1/check", {"key": "h-8", "cost": 1}, idempotency_key="ignored-1")
        self.assertEqual(status, 200)
        self.assertTrue(body["allowed"])
        status, body, _ = self.request(
            "/v1/check", {"key": "h-8", "cost": 1}, idempotency_key="ignored-1")
        self.assertEqual(status, 200)                             # not deduplicated: two spends
        self.assertEqual(self.state("h-8")["used"], 2)

    def test_concurrent_http_retries_share_one_reservation(self) -> None:
        key = "h-conc"
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        conn.request("PUT", f"/v1/limits/{key}",
                     body=json.dumps({"capacity": 8, "refill_per_second": 0.0001}),
                     headers={"Content-Type": "application/json"})
        conn.getresponse().read()
        conn.close()
        payload = json.dumps({"key": key, "cost": 1}).encode()
        responses: list[tuple[int, str]] = []
        responses_lock = threading.Lock()
        allowed_before = self.reservation_allowed_metric()

        def create() -> None:
            # A burst can outrun the listen backlog; a transport reset is exactly the timeout
            # the Idempotency-Key exists to retry safely, so retry with a small backoff.
            last_error: BaseException | None = None
            for attempt in range(10):
                try:
                    connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
                    connection.putrequest("POST", "/v1/reservations")
                    connection.putheader("Content-Length", str(len(payload)))
                    connection.putheader("Content-Type", "application/json")
                    connection.putheader("Idempotency-Key", "http-concurrent")
                    connection.endheaders(payload)
                    response = connection.getresponse()
                    body = json.loads(response.read())
                    connection.close()
                    with responses_lock:
                        responses.append((response.status, body.get("reservation_id")))
                    return
                except (OSError, http.client.HTTPException) as error:
                    last_error = error
                    time.sleep(0.02 * (attempt + 1))
            with responses_lock:
                responses.append((-1, repr(last_error)))

        threads = [threading.Thread(target=create) for _ in range(32)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(len(responses), 32)
        self.assertTrue(all(status == 200 for status, _ in responses), responses)
        self.assertEqual({rid for _, rid in responses}, {responses[0][1]})
        self.assertEqual(self.state(key)["remaining"], 7)
        self.assertEqual(self.reservation_allowed_metric() - allowed_before, 1)


if __name__ == "__main__":
    unittest.main()
