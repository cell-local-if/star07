"""Creation idempotency for POST /v1/reservations via the optional Idempotency-Key header.

Covers the Limiter unit surface and the HTTP surface: first-success binding, verbatim replay,
409 idempotency_conflict on any param mismatch, no bind on unsuccessful creation, survival past
the reservation's lifecycle, header validation before the body is read, and concurrent collapse
to exactly one creation.
"""
from __future__ import annotations

import http.client
import json
import threading
import unittest
import urllib.error
import urllib.request

from quota import IdempotencyConflict, InvalidRequest, LimitNotFound, Limiter, OverQuota


class Clock:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t


class IdempotentReservationUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.limiter = Limiter(self.clock)
        self.limiter.configure("tenant-a", {"capacity": 5, "refill_per_second": 1.0})

    def reserve(self, key="tenant-a", cost=1, ttl=60, idem="idem-1"):
        return self.limiter.reserve(key, cost, ttl, idempotency_key=idem)

    def test_first_creation_binds_and_replays_the_same_response(self) -> None:
        first = self.reserve(cost=2, ttl=30)
        self.assertEqual((first["key"], first["cost"], first["remaining"],
                          first["capacity"], first["ttl_seconds"]),
                         ("tenant-a", 2, 3, 5, 30))
        second = self.reserve(cost=2, ttl=30)
        self.assertEqual(second, first)
        self.assertEqual(second["reservation_id"], first["reservation_id"])

    def test_replay_deducts_no_second_hold_and_counts_one_decision(self) -> None:
        first = self.reserve(cost=3)
        self.assertEqual(self.limiter.state("tenant-a")["remaining"], 2)
        self.clock.t += 100.0                                       # past the hold's ttl
        for _ in range(5):
            # Replays never tick, so the due hold is NOT settled by any of these.
            self.assertEqual(self.reserve(cost=3), first)
        decisions = self.limiter.metrics()["metrics"]["decisions"]["reservation"]
        self.assertEqual(decisions, {"allowed": 1, "over_quota": 0})
        live = [r for r in self.limiter._reservations.values() if r.key == "tenant-a"]
        self.assertEqual(len(live), 1)                              # still the one original hold
        state = self.limiter.state("tenant-a")                      # this read ticks and settles
        self.assertEqual((state["remaining"], state["used"]), (5, 0))

    def test_replayed_response_is_an_independent_copy(self) -> None:
        first = self.reserve()
        first["tampered"] = True
        first["cost"] = 999
        second = self.reserve()
        self.assertNotIn("tampered", second)
        self.assertEqual(second["cost"], 1)

    def test_conflict_on_key_cost_or_ttl_mismatch(self) -> None:
        self.limiter.configure("tenant-b", {"capacity": 5, "refill_per_second": 1.0})
        self.reserve(cost=2, ttl=30)
        with self.assertRaises(IdempotencyConflict):
            self.reserve(key="tenant-b", cost=2, ttl=30)          # different key
        with self.assertRaises(IdempotencyConflict):
            self.reserve(cost=3, ttl=30)                          # different cost
        with self.assertRaises(IdempotencyConflict):
            self.reserve(cost=2, ttl=31)                          # different ttl
        # The conflicting attempts changed nothing: one hold, one decision, frozen snapshot intact.
        state = self.limiter.state("tenant-a")
        self.assertEqual((state["remaining"], state["used"]), (3, 0))
        decisions = self.limiter.metrics()["metrics"]["decisions"]["reservation"]
        self.assertEqual(decisions, {"allowed": 1, "over_quota": 0})
        self.assertEqual(self.reserve(cost=2, ttl=30)["cost"], 2)

    def test_conflict_takes_precedence_over_over_quota(self) -> None:
        # First creation holds 2 of 5 tokens; a mismatching retry asks for 4 against only 3 free
        # tokens — which would ordinarily be 429 — but the fingerprint mismatch wins as 409 and
        # carries no retry hint, before affordability is ever judged.
        self.reserve(cost=2, ttl=30)
        with self.assertRaises(IdempotencyConflict):
            self.reserve(cost=4, ttl=30)

    def test_replay_and_conflict_never_sample_the_clock(self) -> None:
        # capacity 10, hold 8 at t=1000 -> 2 tokens left, due at 1060.
        limiter = Limiter(self.clock)
        limiter.configure("frozen", {"capacity": 10, "refill_per_second": 1.0})
        first = limiter.reserve("frozen", 8, 60, idempotency_key="K")
        self.assertEqual(first["remaining"], 2)
        self.clock.t = 2000.0                                      # well past expiry
        # Replay must neither tick (pinning the watermark to 2000) nor settle the due hold.
        self.assertEqual(limiter.reserve("frozen", 8, 60, idempotency_key="K"), first)
        with self.assertRaises(IdempotencyConflict):
            limiter.reserve("frozen", 8, 61, idempotency_key="K")
        self.clock.t = 1005.0                                      # before the hold's 1060 expiry
        # Had the replay/conflict ticked, the watermark would be 2000: the hold would already be
        # settled and the bucket full. It did not, so at 1005 the hold is live and 5s refilled.
        state = limiter.state("frozen")
        self.assertEqual(state["remaining"], 7)                   # 2 held-left + 5 refill, hold still live
        self.assertIn(first["reservation_id"], limiter._reservations)

    def test_unsuccessful_creation_does_not_bind_the_key(self) -> None:
        # 429 over quota: key stays free and a later legal retry is a genuine first creation.
        limiter = Limiter(self.clock)
        limiter.configure("tight", {"capacity": 1, "refill_per_second": 1.0})
        self.assertTrue(limiter.check("tight", 1)["allowed"])
        with self.assertRaises(OverQuota):
            limiter.reserve("tight", 1, 60, idempotency_key="retry-429")
        self.clock.t += 1.0
        first = limiter.reserve("tight", 1, 60, idempotency_key="retry-429")
        self.assertEqual(first["remaining"], 0)
        self.assertEqual(limiter.reserve("tight", 1, 60, idempotency_key="retry-429"), first)

        # 404 unknown key: configure afterwards and the same header drives a first creation.
        with self.assertRaises(LimitNotFound):
            limiter.reserve("ghost", 1, 60, idempotency_key="retry-404")
        limiter.configure("ghost", {"capacity": 2, "refill_per_second": 1.0})
        ghost = limiter.reserve("ghost", 1, 60, idempotency_key="retry-404")
        self.assertEqual(ghost["remaining"], 1)

        # 400 cost above capacity: the rejected fingerprint is not remembered.
        with self.assertRaises(InvalidRequest):
            limiter.reserve("ghost", 5, 60, idempotency_key="retry-400")
        created = limiter.reserve("ghost", 1, 60, idempotency_key="retry-400")
        self.assertEqual(created["remaining"], 0)                # the cost-5 attempt took nothing

        # Only the three real creations count.
        decisions = limiter.metrics()["metrics"]["decisions"]["reservation"]
        self.assertEqual(decisions, {"allowed": 3, "over_quota": 1})

    def test_invalid_idempotency_key_is_invalid_request(self) -> None:
        for bad_key in ["", " " * 1, "a b", "a\tb", "a\nb", "x" * 129,
                        "k-café", 123, True, ["k"], {"k": 1}]:
            with self.assertRaises(InvalidRequest, msg=repr(bad_key)):
                self.limiter.reserve("tenant-a", 1, 60, idempotency_key=bad_key)
        # A malformed header is rejected before the body params are even validated, and changes
        # nothing — same header rule precedence the HTTP layer enforces before reading the body.
        with self.assertRaises(InvalidRequest):
            self.limiter.reserve("", 0, 0, idempotency_key="bad key")
        # None means "no header" at the unit surface: baseline behaviour stays intact.
        self.assertEqual(self.limiter.reserve("tenant-a", 1, 60,
                                              idempotency_key=None)["ttl_seconds"], 60)
        # Boundary lengths are accepted.
        self.assertEqual(self.limiter.reserve("tenant-a", 1, 60,
                                              idempotency_key="x")["ttl_seconds"], 60)
        self.assertEqual(self.limiter.reserve("tenant-a", 1, 60,
                                              idempotency_key="y" * 128)["ttl_seconds"], 60)
        state = self.limiter.state("tenant-a")
        self.assertEqual((state["remaining"], state["used"]), (2, 0))

    def test_omitting_the_key_keeps_baseline_behaviour(self) -> None:
        first = self.limiter.reserve("tenant-a", 1)
        second = self.limiter.reserve("tenant-a", 1)
        self.assertNotEqual(first["reservation_id"], second["reservation_id"])
        decisions = self.limiter.metrics()["metrics"]["decisions"]["reservation"]
        self.assertEqual(decisions, {"allowed": 2, "over_quota": 0})

    def test_distinct_header_values_are_independent_creations(self) -> None:
        one = self.reserve(cost=1, idem="key-one")
        two = self.reserve(cost=1, idem="key-two")
        self.assertNotEqual(one["reservation_id"], two["reservation_id"])
        self.assertEqual(self.limiter.state("tenant-a")["remaining"], 3)

    def test_binding_survives_consume_and_keeps_replaying_creation(self) -> None:
        first = self.reserve(cost=2)
        consumed = self.limiter.consume(first["reservation_id"])
        self.assertTrue(consumed["consumed"])
        self.clock.t += 50.0
        replayed = self.reserve(cost=2)
        self.assertEqual(replayed, first)                            # creation snapshot, no "consumed" field
        self.assertNotIn("consumed", replayed)
        # Ledger still holds exactly the one consume event; replays append nothing.
        ledger = self.limiter.ledger("tenant-a", 1000)
        self.assertEqual([event["source"] for event in ledger["events"]],
                         ["reservation_consume"])

    def test_binding_survives_rollback(self) -> None:
        first = self.reserve(cost=2)
        self.assertTrue(self.limiter.rollback(first["reservation_id"])["rolled_back"])
        with self.assertRaises(LimitNotFound):
            self.limiter.rollback(first["reservation_id"])
        # A retry cannot re-hold the tokens; it replays the original 200.
        self.assertEqual(self.reserve(cost=2), first)
        state = self.limiter.state("tenant-a")
        self.assertEqual((state["remaining"], state["used"]), (5, 0))

    def test_binding_survives_expiry_settlement(self) -> None:
        limiter = Limiter(self.clock)
        limiter.configure("frozen", {"capacity": 5, "refill_per_second": 0.0001})
        first = limiter.reserve("frozen", 3, 10, idempotency_key="K")  # due at 1010
        self.clock.t += 10
        self.assertEqual(limiter.reserve("frozen", 3, 10, idempotency_key="K"), first)
        # The replayed snapshot still names the hold, but that hold has in fact expired and refunded.
        with self.assertRaises(LimitNotFound):
            limiter.rollback(first["reservation_id"])
        self.assertEqual(limiter.state("frozen")["remaining"], 5)
        # Mismatched params after expiry are still a conflict, not a fresh creation.
        with self.assertRaises(IdempotencyConflict):
            limiter.reserve("frozen", 3, 11, idempotency_key="K")
        decisions = limiter.metrics()["metrics"]["decisions"]["reservation"]
        self.assertEqual(decisions, {"allowed": 1, "over_quota": 0})

    def test_concurrent_identical_requests_collapse_to_one_creation(self) -> None:
        limiter = Limiter(self.clock)
        limiter.configure("hot", {"capacity": 20, "refill_per_second": 0.0001})
        responses: list[dict] = []
        errors: list[BaseException] = []
        start = threading.Barrier(32)

        def create() -> None:
            start.wait()
            try:
                responses.append(limiter.reserve("hot", 1, 3600, idempotency_key="same-key"))
            except BaseException as error:  # noqa: BLE001 - surface failures on the main thread
                errors.append(error)

        threads = [threading.Thread(target=create) for _ in range(32)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        self.assertEqual(len(responses), 32)
        ids = {response["reservation_id"] for response in responses}
        self.assertEqual(ids, {responses[0]["reservation_id"]})
        self.assertTrue(all(response == responses[0] for response in responses))
        self.assertEqual(limiter.state("hot")["remaining"], 19)
        decisions = limiter.metrics()["metrics"]["decisions"]["reservation"]
        self.assertEqual(decisions, {"allowed": 1, "over_quota": 0})

    def test_concurrent_collapse_holds_under_real_contention(self) -> None:
        # 16 identical requests against capacity 1 with a negligible rate: exactly one creation,
        # zero over_quota decisions — the losers must wait for and replay the winner, not race it.
        limiter = Limiter(self.clock)
        limiter.configure("one", {"capacity": 1, "refill_per_second": 0.0001})
        responses: list[dict] = []
        statuses: list[str] = []
        errors: list[BaseException] = []
        start = threading.Barrier(16)
        lists_lock = threading.Lock()

        def create() -> None:
            start.wait()
            try:
                response = limiter.reserve("one", 1, 3600, idempotency_key="only-one")
                with lists_lock:
                    responses.append(response)
            except OverQuota:
                with lists_lock:
                    statuses.append("over_quota")
            except BaseException as error:  # noqa: BLE001
                errors.append(error)

        threads = [threading.Thread(target=create) for _ in range(16)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        self.assertEqual(statuses, [])
        self.assertEqual(len(responses), 16)
        self.assertEqual(len({r["reservation_id"] for r in responses}), 1)


class IdempotentReservationHttpTests(unittest.TestCase):
    """Every test gets a fresh server (and therefore fresh counters and clock)."""

    def setUp(self) -> None:
        from quota import serve

        self.clock = Clock()
        self.server = serve(port=0, now=self.clock)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    def request(self, method: str, path: str, body=None,
                headers: dict | None = None) -> tuple[int, dict, dict]:
        data = None if body is None else json.dumps(body).encode()
        merged = {"Content-Type": "application/json"}
        if headers:
            merged.update(headers)
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data,
                                         method=method, headers=merged)
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read() or b"{}"), dict(response.headers)
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read() or b"{}"), dict(error.headers)

    def raw_post(self, path: str, payload: bytes,
                 idem_headers: list[tuple[str, str]] | None = None) -> tuple[int, dict, dict]:
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        connection.putrequest("POST", path)
        connection.putheader("Content-Length", str(len(payload)))
        connection.putheader("Content-Type", "application/json")
        for name, value in idem_headers or []:
            connection.putheader(name, value)
        connection.endheaders(payload)
        response = connection.getresponse()
        body = json.loads(response.read() or b"{}")
        headers = {name.lower(): value for name, value in response.getheaders()}
        status = response.status
        connection.close()
        return status, body, headers

    def test_replay_returns_same_reservation_and_counts_one_decision(self) -> None:
        self.request("PUT", "/v1/limits/i-1", {"capacity": 4, "refill_per_second": 1})
        payload = {"key": "i-1", "cost": 2, "ttl_seconds": 30}
        status, first, _ = self.request("POST", "/v1/reservations", payload,
                                        headers={"Idempotency-Key": "order-1"})
        self.assertEqual(status, 200)
        self.clock.t += 100.0
        status, second, _ = self.request("POST", "/v1/reservations", payload,
                                         headers={"Idempotency-Key": "order-1"})
        self.assertEqual(status, 200)
        self.assertEqual(second, first)
        _, state, _ = self.request("GET", "/v1/limits/i-1")
        self.assertEqual((state["remaining"], state["used"]), (4, 0))   # one hold, long expired+refunded
        _, metrics, _ = self.request("GET", "/v1/metrics")
        self.assertEqual(metrics["metrics"]["decisions"]["reservation"],
                         {"allowed": 1, "over_quota": 0})

    def test_param_mismatch_is_409_conflict_without_retry_after(self) -> None:
        self.request("PUT", "/v1/limits/i-2", {"capacity": 5, "refill_per_second": 1})
        self.request("POST", "/v1/reservations", {"key": "i-2", "cost": 2, "ttl_seconds": 30},
                     headers={"Idempotency-Key": "order-2"})
        for mismatched in [{"key": "i-other", "cost": 2, "ttl_seconds": 30},
                           {"key": "i-2", "cost": 3, "ttl_seconds": 30},
                           {"key": "i-2", "cost": 2, "ttl_seconds": 31}]:
            status, body, headers = self.request("POST", "/v1/reservations", mismatched,
                                                 headers={"Idempotency-Key": "order-2"})
            self.assertEqual((status, body["error"]["code"]), (409, "idempotency_conflict"),
                             mismatched)
            self.assertNotIn("Retry-After", headers)
        _, state, _ = self.request("GET", "/v1/limits/i-2")
        self.assertEqual((state["remaining"], state["used"]), (3, 0))
        _, metrics, _ = self.request("GET", "/v1/metrics")
        self.assertEqual(metrics["metrics"]["decisions"]["reservation"],
                         {"allowed": 1, "over_quota": 0})

    def test_malformed_header_is_400_before_the_body_is_adopted(self) -> None:
        self.request("PUT", "/v1/limits/i-3", {"capacity": 3, "refill_per_second": 1})
        payload = json.dumps({"key": "i-3", "cost": 2}).encode()
        cases: list[tuple[str, list[tuple[str, str]]]] = [
            ("empty", [("Idempotency-Key", "")]),
            ("spaces", [("Idempotency-Key", "a b")]),
            ("tab", [("Idempotency-Key", "a\tb")]),
            ("newline-ish", [("Idempotency-Key", "x" * 128 + " ")]),
            ("too-long", [("Idempotency-Key", "x" * 129)]),
            ("non-ascii", [("Idempotency-Key", "k-café")]),
            ("repeated", [("Idempotency-Key", "a"), ("Idempotency-Key", "b")]),
        ]
        for label, idem_headers in cases:
            status, body, headers = self.raw_post("/v1/reservations", payload, idem_headers)
            self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"), label)
            self.assertNotIn("Retry-After", headers)
        # A malformed header next to an unparseable body is still rejected on the header rule,
        # and the body is never adopted.
        status, body, _ = self.raw_post("/v1/reservations", b"{not json",
                                        [("Idempotency-Key", "bad key")])
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        self.assertIn("Idempotency-Key", body["error"]["message"])
        _, state, _ = self.request("GET", "/v1/limits/i-3")
        self.assertEqual((state["remaining"], state["used"]), (3, 0))
        _, metrics, _ = self.request("GET", "/v1/metrics")
        self.assertEqual(metrics["metrics"]["decisions"]["reservation"],
                         {"allowed": 0, "over_quota": 0})

    def test_key_length_boundaries_are_accepted(self) -> None:
        self.request("PUT", "/v1/limits/i-4", {"capacity": 3, "refill_per_second": 1})
        for value in ["x", "y" * 128]:
            status, _, _ = self.request("POST", "/v1/reservations", {"key": "i-4", "cost": 1},
                                        headers={"Idempotency-Key": value})
            self.assertEqual(status, 200, len(value))

    def test_header_is_ignored_on_every_other_route(self) -> None:
        # Single-key check accepts the header without any idempotency behaviour.
        self.request("PUT", "/v1/limits/i-5", {"capacity": 10, "refill_per_second": 1})
        self.request("PUT", "/v1/limits/i-parent", {"capacity": 10, "refill_per_second": 1})
        for _ in range(2):
            status, _, _ = self.request("POST", "/v1/check", {"key": "i-5", "cost": 1},
                                        headers={"Idempotency-Key": "ignored-here"})
            self.assertEqual(status, 200)
        # Hierarchy reservation creation is not idempotent either: same header, two reservations.
        ids = set()
        for _ in range(2):
            status, body, _ = self.request(
                "POST", "/v1/hierarchies/reservations",
                {"keys": ["i-parent", "i-5"], "cost": 1},
                headers={"Idempotency-Key": "ignored-here"})
            self.assertEqual(status, 200)
            ids.add(body["reservation_id"])
        self.assertEqual(len(ids), 2)
        # The consume route accepts the header but its replay semantics stay reservation_id based.
        status, created, _ = self.request("POST", "/v1/reservations", {"key": "i-5", "cost": 1},
                                          headers={"Idempotency-Key": "creation-only"})
        self.assertEqual(status, 200)
        status, consumed, _ = self.request(
            "POST", f"/v1/reservations/{created['reservation_id']}/consume", {},
            headers={"Idempotency-Key": "whatever"})
        self.assertEqual(status, 200)
        self.assertTrue(consumed["consumed"])

    def test_rejected_creation_is_not_bound_across_error_classes(self) -> None:
        # Over-quota 429 leaves the key free for a later legal retry.
        self.request("PUT", "/v1/limits/i-6", {"capacity": 1, "refill_per_second": 1})
        self.request("POST", "/v1/check", {"key": "i-6", "cost": 1})
        status, body, headers = self.request("POST", "/v1/reservations", {"key": "i-6", "cost": 1},
                                             headers={"Idempotency-Key": "retry-429"})
        self.assertEqual((status, body["error"]["code"]), (429, "over_quota"))
        self.assertIn("Retry-After", headers)
        self.clock.t += 1.0
        status, first, _ = self.request("POST", "/v1/reservations", {"key": "i-6", "cost": 1},
                                        headers={"Idempotency-Key": "retry-429"})
        self.assertEqual(status, 200)

        # Unknown-key 404 leaves the key free as well.
        status, body, _ = self.request("POST", "/v1/reservations", {"key": "i-6-missing"},
                                       headers={"Idempotency-Key": "retry-404"})
        self.assertEqual((status, body["error"]["code"]), (404, "not_found"))
        self.request("PUT", "/v1/limits/i-6-missing", {"capacity": 3, "refill_per_second": 1})
        status, second, _ = self.request("POST", "/v1/reservations", {"key": "i-6-missing"},
                                         headers={"Idempotency-Key": "retry-404"})
        self.assertEqual(status, 200)

        # A malformed body next to a perfectly good header does not bind it.
        status, body, _ = self.raw_post(
            "/v1/reservations", b'{"key": "i-6", "cost": "bad"}',
            [("Idempotency-Key", "retry-bad-body")])
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        status, third, _ = self.request("POST", "/v1/reservations", {"key": "i-6-missing"},
                                        headers={"Idempotency-Key": "retry-bad-body"})
        self.assertEqual(status, 200)
        self.assertEqual(len({first["reservation_id"], second["reservation_id"],
                              third["reservation_id"]}), 3)
        _, metrics, _ = self.request("GET", "/v1/metrics")
        self.assertEqual(metrics["metrics"]["decisions"]["reservation"],
                         {"allowed": 3, "over_quota": 1})

    def test_replay_after_full_lifecycle_still_shows_creation(self) -> None:
        self.request("PUT", "/v1/limits/i-7", {"capacity": 4, "refill_per_second": 0.0001})
        payload = {"key": "i-7", "cost": 3, "ttl_seconds": 10}
        status, first, _ = self.request("POST", "/v1/reservations", payload,
                                        headers={"Idempotency-Key": "lifecycle"})
        self.assertEqual(status, 200)
        status, consumed, _ = self.request(
            "POST", f"/v1/reservations/{first['reservation_id']}/consume", {})
        self.assertEqual(status, 200)
        self.assertEqual(consumed["used"], 3)
        self.clock.t += 100.0
        status, replayed, _ = self.request("POST", "/v1/reservations", payload,
                                           headers={"Idempotency-Key": "lifecycle"})
        self.assertEqual(status, 200)
        self.assertEqual(replayed, first)
        status, body, _ = self.request("POST", "/v1/reservations",
                                       {"key": "i-7", "cost": 3, "ttl_seconds": 11},
                                       headers={"Idempotency-Key": "lifecycle"})
        self.assertEqual((status, body["error"]["code"]), (409, "idempotency_conflict"))
        _, metrics, _ = self.request("GET", "/v1/metrics")
        self.assertEqual(metrics["metrics"]["decisions"]["reservation"],
                         {"allowed": 1, "over_quota": 0})
        _, ledger, _ = self.request("GET", "/v1/ledgers/i-7")
        self.assertEqual(ledger["totals"], {"accepted_count": 1, "accepted_cost": 3})

    def test_replay_survives_rollback_and_expiry_over_http(self) -> None:
        self.request("PUT", "/v1/limits/i-8", {"capacity": 4, "refill_per_second": 0.0001})
        payload = {"key": "i-8", "cost": 3, "ttl_seconds": 10}
        status, first, _ = self.request("POST", "/v1/reservations", payload,
                                        headers={"Idempotency-Key": "rolled"})
        self.assertEqual(status, 200)
        self.assertEqual(self.request("DELETE", f"/v1/reservations/{first['reservation_id']}")[0], 200)
        status, replayed, _ = self.request("POST", "/v1/reservations", payload,
                                           headers={"Idempotency-Key": "rolled"})
        self.assertEqual(status, 200)
        self.assertEqual(replayed, first)
        _, state, _ = self.request("GET", "/v1/limits/i-8")
        self.assertEqual((state["remaining"], state["used"]), (4, 0))   # rollback refunded, replay held nothing

        # Expiry path: fresh key, hold lapses, retry still replays and re-holds nothing.
        self.request("PUT", "/v1/limits/i-9", {"capacity": 4, "refill_per_second": 0.0001})
        payload = {"key": "i-9", "cost": 3, "ttl_seconds": 10}
        status, first, _ = self.request("POST", "/v1/reservations", payload,
                                        headers={"Idempotency-Key": "expired"})
        self.assertEqual(status, 200)
        self.clock.t += 10
        status, replayed, _ = self.request("POST", "/v1/reservations", payload,
                                           headers={"Idempotency-Key": "expired"})
        self.assertEqual(status, 200)
        self.assertEqual(replayed, first)
        _, state, _ = self.request("GET", "/v1/limits/i-9")
        self.assertEqual((state["remaining"], state["used"]), (4, 0))

    def test_concurrent_http_requests_create_once(self) -> None:
        self.request("PUT", "/v1/limits/i-hot", {"capacity": 50, "refill_per_second": 0.0001})
        payload = json.dumps({"key": "i-hot", "cost": 1, "ttl_seconds": 3600}).encode()
        responses: list[dict] = []
        errors: list[BaseException] = []
        start = threading.Barrier(24)
        lock = threading.Lock()

        def create() -> None:
            start.wait()
            # Every worker fires at once and the stdlib server's accept backlog can reset a
            # brand-new connection under the thundering herd; a real client reacts by retrying
            # the SAME Idempotency-Key — which is exactly the case idempotency exists for, so the
            # retry must still answer with the single shared reservation.
            for attempt in range(10):
                try:
                    _, body, _ = self.raw_post(
                        "/v1/reservations", payload, [("Idempotency-Key", "same-http-key")])
                    with lock:
                        responses.append(body)
                    return
                except (ConnectionError, OSError) as error:
                    if attempt == 9:
                        with lock:
                            errors.append(error)
        threads = [threading.Thread(target=create) for _ in range(24)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        self.assertEqual(len(responses), 24)
        ids = {body["reservation_id"] for body in responses}
        self.assertEqual(len(ids), 1)
        self.assertTrue(all(body == responses[0] for body in responses))
        _, state, _ = self.request("GET", "/v1/limits/i-hot")
        self.assertEqual(state["remaining"], 49)
        _, metrics, _ = self.request("GET", "/v1/metrics")
        self.assertEqual(metrics["metrics"]["decisions"]["reservation"],
                         {"allowed": 1, "over_quota": 0})


if __name__ == "__main__":
    unittest.main()
