"""Baseline tests for the rate limiter: deterministic because time is injected."""
from __future__ import annotations

import json
import threading
import unittest
import urllib.error
import urllib.request

from quota import InvalidRequest, LimitNotFound, Limiter, OverQuota

class Clock:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t


class LimiterUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.limiter = Limiter(self.clock)
        self.limiter.configure("tenant-a", {"capacity": 3, "refill_per_second": 1.0})

    def test_burst_then_over_quota_with_retry_after(self) -> None:
        for _ in range(3):
            self.assertTrue(self.limiter.check("tenant-a", 1)["allowed"])
        with self.assertRaises(OverQuota) as raised:
            self.limiter.check("tenant-a", 1)
        self.assertAlmostEqual(raised.exception.retry_after, 1.0, places=6)

    def test_refill_is_proportional_to_elapsed_time(self) -> None:
        self.limiter.check("tenant-a", 3)
        self.clock.t += 2.0           # 2 seconds of refill at 1 token/s
        result = self.limiter.check("tenant-a", 2)
        self.assertEqual(result["remaining"], 0)

    def test_keys_are_isolated_and_unknown_key_is_404(self) -> None:
        self.limiter.configure("tenant-b", {"capacity": 1, "refill_per_second": 0.5})
        self.limiter.check("tenant-b", 1)
        self.assertTrue(self.limiter.check("tenant-a", 1)["allowed"])
        with self.assertRaises(LimitNotFound):
            self.limiter.check("tenant-c", 1)

    def test_invalid_configuration_and_cost_are_rejected(self) -> None:
        for bad in [{"capacity": 0, "refill_per_second": 1}, {"capacity": 1}, {"capacity": 1, "refill_per_second": -1},
                    "nope", {"capacity": 1, "refill_per_second": 1, "extra": 1}]:
            with self.assertRaises(InvalidRequest):
                self.limiter.configure("tenant-d", bad)
        with self.assertRaises(InvalidRequest):
            self.limiter.check("tenant-a", 0)


class HttpSurfaceTests(unittest.TestCase):
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

    def test_health_and_configure_then_check(self) -> None:
        self.assertEqual(self.request("GET", "/health")[0], 200)
        status, body, _ = self.request("PUT", "/v1/limits/t-1", {"capacity": 2, "refill_per_second": 2})
        self.assertEqual((status, body["limit"]["capacity"]), (200, 2))
        self.assertEqual(self.request("POST", "/v1/check", {"key": "t-1"})[0], 200)

    def test_over_quota_returns_429_with_retry_after(self) -> None:
        self.request("PUT", "/v1/limits/t-2", {"capacity": 1, "refill_per_second": 1})
        self.request("POST", "/v1/check", {"key": "t-2"})
        status, body, headers = self.request("POST", "/v1/check", {"key": "t-2"})
        self.assertEqual((status, body["error"]["code"]), (429, "over_quota"))
        self.assertIn("Retry-After", headers)

    def test_unknown_key_and_route(self) -> None:
        self.assertEqual(self.request("GET", "/v1/limits/absent")[0], 404)
        self.assertEqual(self.request("POST", "/v1/nope", {})[0], 404)


class ReservationUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.limiter = Limiter(self.clock)
        self.limiter.configure("tenant-a", {"capacity": 5, "refill_per_second": 1.0})

    def test_reserve_decrements_remaining_but_not_used(self) -> None:
        result = self.limiter.reserve("tenant-a", 3)
        self.assertEqual(result["key"], "tenant-a")
        self.assertEqual(result["cost"], 3)
        self.assertEqual(result["remaining"], 2)
        self.assertEqual(result["capacity"], 5)
        self.assertIsInstance(result["reservation_id"], str)
        self.assertTrue(result["reservation_id"])
        state = self.limiter.state("tenant-a")
        self.assertEqual(state["remaining"], 2)
        self.assertEqual(state["used"], 0)

    def test_cost_defaults_to_one(self) -> None:
        result = self.limiter.reserve("tenant-a", 1)
        self.assertEqual(result["cost"], 1)

    def test_reservation_ids_are_unique_opaque(self) -> None:
        ids = {self.limiter.reserve("tenant-a", 1)["reservation_id"] for _ in range(3)}
        self.assertEqual(len(ids), 3)

    def test_rollback_returns_tokens_capped_at_capacity(self) -> None:
        reservation = self.limiter.reserve("tenant-a", 3)
        self.clock.t += 1.0          # refill would add 1 token
        result = self.limiter.rollback(reservation["reservation_id"])
        self.assertEqual(result["rolled_back"], True)
        self.assertEqual(result["reservation_id"], reservation["reservation_id"])
        self.assertEqual(result["remaining"], 5)   # 2 held + 1 refill + 3 returned, capped at 5
        self.assertEqual(result["capacity"], 5)
        state = self.limiter.state("tenant-a")
        self.assertEqual(state["remaining"], 5)
        self.assertEqual(state["used"], 0)

    def test_rollback_is_idempotent_only_once(self) -> None:
        reservation = self.limiter.reserve("tenant-a", 2)
        self.limiter.rollback(reservation["reservation_id"])
        with self.assertRaises(LimitNotFound):
            self.limiter.rollback(reservation["reservation_id"])

    def test_unknown_reservation_is_not_found(self) -> None:
        with self.assertRaises(LimitNotFound):
            self.limiter.rollback("does-not-exist")

    def test_over_quota_retry_after_covers_cost(self) -> None:
        self.limiter.reserve("tenant-a", 5)
        with self.assertRaises(OverQuota) as raised:
            self.limiter.reserve("tenant-a", 2)
        # Empty bucket: 2 tokens at 1/s needs 2 seconds.
        self.assertGreaterEqual(raised.exception.retry_after, 2.0 - 1e-9)

    def test_unknown_key_reservation_is_not_found(self) -> None:
        with self.assertRaises(LimitNotFound):
            self.limiter.reserve("tenant-x", 1)

    def test_invalid_key_and_cost_are_invalid_request(self) -> None:
        for bad_key in [1, "", "x" * 201, True, None]:
            with self.assertRaises(InvalidRequest):
                self.limiter.reserve(bad_key, 1)
        for bad_cost in [0, 1_000_001, True, 1.5, "1", None]:
            with self.assertRaises(InvalidRequest):
                self.limiter.reserve("tenant-a", bad_cost)

    def test_ttl_defaults_to_60_and_is_echoed(self) -> None:
        result = self.limiter.reserve("tenant-a", 1)
        self.assertEqual(result["ttl_seconds"], 60)

    def test_custom_ttl_bounds_are_accepted(self) -> None:
        self.assertEqual(self.limiter.reserve("tenant-a", 1, 1)["ttl_seconds"], 1)
        self.assertEqual(self.limiter.reserve("tenant-a", 1, 86400)["ttl_seconds"], 86400)

    def test_invalid_ttl_is_invalid_request_and_takes_nothing(self) -> None:
        for bad_ttl in [0, -1, 86401, 1.5, 60.0, True, "60", None]:
            with self.assertRaises(InvalidRequest):
                self.limiter.reserve("tenant-a", 1, bad_ttl)
        # Every rejected ttl must leave both the bucket and the registry untouched.
        state = self.limiter.state("tenant-a")
        self.assertEqual((state["remaining"], state["used"]), (5, 0))

    def test_reservation_expires_at_inclusive_boundary_and_settles_on_read(self) -> None:
        limiter = Limiter(self.clock)
        limiter.configure("frozen", {"capacity": 5, "refill_per_second": 0.0001})  # refill negligible here
        reservation = limiter.reserve("frozen", 3, ttl_seconds=10)  # now 1000, due 1010
        self.assertEqual(limiter.state("frozen")["remaining"], 2)
        self.clock.t += 9
        self.assertEqual(limiter.state("frozen")["remaining"], 2)   # 1009: still held
        self.clock.t += 1
        state = limiter.state("frozen")                             # 1010: due (<=) settles
        self.assertEqual(state["remaining"], 5)
        self.assertEqual(state["used"], 0)
        with self.assertRaises(LimitNotFound):
            limiter.rollback(reservation["reservation_id"])

    def test_expiry_refunds_once_without_advancing_clock(self) -> None:
        limiter = Limiter(self.clock)
        limiter.configure("frozen", {"capacity": 5, "refill_per_second": 0.0001})
        # Prior instant consumption is what keeps the refund visible below capacity.
        self.assertTrue(limiter.check("frozen", 2)["allowed"])       # used 2, tokens 3
        limiter.reserve("frozen", 2, ttl_seconds=5)                  # tokens 1
        self.clock.t += 5
        self.assertEqual(limiter.state("frozen")["remaining"], 3)   # 1 + 2 refunded
        # Same clock reading: no extra refill and the already-settled reservation refunds nothing again.
        self.assertEqual(limiter.state("frozen")["remaining"], 3)
        self.assertEqual(limiter.state("frozen")["used"], 2)

    def test_expiry_refund_is_capped_at_current_capacity(self) -> None:
        limiter = Limiter(self.clock)
        limiter.configure("frozen", {"capacity": 5, "refill_per_second": 0.0001})
        limiter.reserve("frozen", 5, ttl_seconds=5)                  # tokens 0
        self.clock.t += 5
        self.assertEqual(limiter.state("frozen")["remaining"], 5)   # refund capped at 5, not 5+refill

    def test_expiry_refund_uses_current_capacity_after_reconfigure(self) -> None:
        limiter = Limiter(self.clock)
        limiter.configure("frozen", {"capacity": 10, "refill_per_second": 0.0001})
        limiter.reserve("frozen", 10, ttl_seconds=10)                # tokens 0
        limiter.configure("frozen", {"capacity": 3, "refill_per_second": 0.0001})  # hold survives, cap 3
        self.clock.t += 10
        self.assertEqual(limiter.state("frozen")["remaining"], 3)   # refund of 10 capped at current cap 3

    def test_expiry_settles_on_check_and_reserve(self) -> None:
        limiter = Limiter(self.clock)
        limiter.configure("via-check", {"capacity": 3, "refill_per_second": 0.0001})
        limiter.reserve("via-check", 3, ttl_seconds=5)               # tokens 0
        with self.assertRaises(OverQuota):
            limiter.check("via-check", 3)
        self.clock.t += 5
        result = limiter.check("via-check", 3)                       # expiry refunds first
        self.assertTrue(result["allowed"])
        self.assertEqual(limiter.state("via-check")["used"], 3)      # refund never counts as used

        limiter.configure("via-reserve", {"capacity": 3, "refill_per_second": 0.0001})
        limiter.reserve("via-reserve", 3, ttl_seconds=5)             # tokens 0
        self.clock.t += 5
        result = limiter.reserve("via-reserve", 3, ttl_seconds=5)    # expiry settles on reserve too
        self.assertEqual(result["remaining"], 0)
        self.assertEqual(limiter.state("via-reserve")["used"], 0)

    def test_rollback_after_expiry_is_404_and_adds_nothing(self) -> None:
        limiter = Limiter(self.clock)
        limiter.configure("frozen", {"capacity": 5, "refill_per_second": 0.0001})
        self.assertTrue(limiter.check("frozen", 2)["allowed"])       # tokens 3, used 2
        reservation = limiter.reserve("frozen", 2, ttl_seconds=5)    # tokens 1
        self.clock.t += 5
        with self.assertRaises(LimitNotFound):
            limiter.rollback(reservation["reservation_id"])
        state = limiter.state("frozen")
        self.assertEqual((state["remaining"], state["used"]), (3, 2))

    def test_reconfigure_triggers_expiry_but_never_extends_it(self) -> None:
        limiter = Limiter(self.clock)
        limiter.configure("frozen", {"capacity": 5, "refill_per_second": 0.0001})
        reservation = limiter.reserve("frozen", 3, ttl_seconds=10)   # tokens 2, due 1010
        self.clock.t += 5
        limiter.configure("frozen", {"capacity": 8, "refill_per_second": 0.0001})  # not due at 1005
        self.assertEqual(limiter.state("frozen")["remaining"], 2)    # hold survives reconfigure
        self.clock.t += 4
        self.assertEqual(limiter.state("frozen")["remaining"], 2)    # 1009: still held, TTL unextended
        self.clock.t += 1
        limiter.configure("frozen", {"capacity": 8, "refill_per_second": 0.0001})  # PUT settles at 1010
        self.assertEqual(limiter.state("frozen")["remaining"], 5)    # refund lands in the new bucket
        with self.assertRaises(LimitNotFound):
            limiter.rollback(reservation["reservation_id"])

    def test_concurrent_expiry_rollback_and_spending_never_oversell_or_double_refund(self) -> None:
        limiter = Limiter(self.clock)
        limiter.configure("hot", {"capacity": 100, "refill_per_second": 0.0001})  # refill negligible here
        ids: list[str] = []
        ids_lock = threading.Lock()

        def hold() -> None:
            try:
                rid = limiter.reserve("hot", 1, ttl_seconds=10)["reservation_id"]
            except OverQuota:
                return
            with ids_lock:
                ids.append(rid)

        threads = [threading.Thread(target=hold) for _ in range(200)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(len(ids), 100)

        self.clock.t += 10                                                  # every hold is now due
        errors: list[BaseException] = []

        def settle(check: bool, rid: str) -> None:
            # Each caller mixes a spending attempt and a rollback of its (now expired) hold;
            # the rollback must 404 and the single lazy refund must feed exactly 100 spenders.
            try:
                if check:
                    limiter.check("hot", 1)
                else:
                    limiter.reserve("hot", 1, ttl_seconds=3600)
                try:
                    limiter.rollback(rid)
                    errors.append(AssertionError(f"rollback of expired {rid} unexpectedly succeeded"))
                except LimitNotFound:
                    pass
            except BaseException as error:  # noqa: BLE001 - surface thread failures on the main thread
                errors.append(error)

        threads = [threading.Thread(target=settle, args=(i % 2 == 0, ids[i])) for i in range(100)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])

        state = limiter.state("hot")
        self.assertEqual(state["remaining"], 0)
        self.assertEqual(state["used"], 50)                                 # only the 50 checks
        live = [r for r in limiter._reservations.values() if r.key == "hot"]
        self.assertEqual(len(live), 50)                                     # only the 50 new reservations
        self.assertEqual(state["remaining"] + state["used"] + len(live), 100)

    def test_reconfigure_keeps_reserved_consumption_and_applies_new_capacity(self) -> None:
        self.limiter.reserve("tenant-a", 4)       # 1 token left
        self.limiter.configure("tenant-a", {"capacity": 10, "refill_per_second": 2.0})
        state = self.limiter.state("tenant-a")
        self.assertEqual(state["limit"]["capacity"], 10)
        self.assertEqual(state["remaining"], 1)  # reserved spend preserved
        with self.assertRaises(OverQuota):
            self.limiter.reserve("tenant-a", 2)
        self.clock.t += 1.0                       # 2 tokens refill at the new rate
        state = self.limiter.state("tenant-a")
        self.assertEqual(state["remaining"], 3)

    def test_concurrent_checks_and_reservations_never_oversell(self) -> None:
        limiter = Limiter(self.clock)
        limiter.configure("hot", {"capacity": 100, "refill_per_second": 0.0001})  # clock never advances here
        outcomes: list[bool] = []
        outcomes_lock = threading.Lock()

        def attempt(check: bool) -> None:
            try:
                if check:
                    limiter.check("hot", 1)
                else:
                    limiter.reserve("hot", 1)
                ok = True
            except OverQuota:
                ok = False
            with outcomes_lock:
                outcomes.append(ok)

        threads = [threading.Thread(target=attempt, args=(i % 2 == 0,)) for i in range(400)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(sum(outcomes), 100)
        self.assertEqual(limiter.state("hot")["remaining"], 0)


class ConsumeUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.limiter = Limiter(self.clock)
        self.limiter.configure("tenant-a", {"capacity": 5, "refill_per_second": 1.0})

    def test_consume_books_cost_once_without_touching_tokens(self) -> None:
        reservation = self.limiter.reserve("tenant-a", 3)   # tokens 2, used 0
        result = self.limiter.consume(reservation["reservation_id"])
        self.assertEqual(result, {"reservation_id": reservation["reservation_id"], "consumed": True,
                                  "remaining": 2, "capacity": 5, "used": 3})
        state = self.limiter.state("tenant-a")              # same clock reading: same remaining
        self.assertEqual((state["remaining"], state["used"]), (2, 3))

    def test_consume_is_idempotent_and_replays_the_first_response(self) -> None:
        reservation = self.limiter.reserve("tenant-a", 2)
        first = self.limiter.consume(reservation["reservation_id"])
        self.clock.t += 3.0                                 # refill would change a fresh answer
        self.assertTrue(self.limiter.check("tenant-a", 1)["allowed"])  # and later usage would change used
        second = self.limiter.consume(reservation["reservation_id"])
        self.assertEqual(second, first)                     # byte-for-byte field values, frozen snapshot
        state = self.limiter.state("tenant-a")
        self.assertEqual(state["used"], 3)                  # 2 booked once + 1 instant spend

    def test_consume_settles_due_sibling_reservations_first(self) -> None:
        limiter = Limiter(self.clock)
        limiter.configure("frozen", {"capacity": 5, "refill_per_second": 0.0001})
        due = limiter.reserve("frozen", 2, ttl_seconds=5)   # tokens 3
        target = limiter.reserve("frozen", 1, ttl_seconds=60)  # tokens 2
        self.clock.t += 5
        result = limiter.consume(target["reservation_id"])  # due hold refunds 2 before confirmation
        self.assertEqual((result["remaining"], result["used"], result["capacity"]), (4, 1, 5))
        with self.assertRaises(LimitNotFound):
            limiter.consume(due["reservation_id"])          # settled hold is unknown, never booked
        self.assertEqual(limiter.state("frozen")["used"], 1)

    def test_consume_at_or_after_expiry_is_404_and_never_books(self) -> None:
        limiter = Limiter(self.clock)
        limiter.configure("frozen", {"capacity": 5, "refill_per_second": 0.0001})
        self.assertTrue(limiter.check("frozen", 2)["allowed"])  # tokens 3, used 2
        reservation = limiter.reserve("frozen", 2, ttl_seconds=5)  # tokens 1
        self.clock.t += 5                                   # inclusive boundary
        with self.assertRaises(LimitNotFound):
            limiter.consume(reservation["reservation_id"])
        self.clock.t += 10                                  # never books later either
        with self.assertRaises(LimitNotFound):
            limiter.consume(reservation["reservation_id"])
        state = limiter.state("frozen")                     # 1 + 2 refunded, used stays 2
        self.assertEqual((state["remaining"], state["used"]), (3, 2))

    def test_consumed_reservation_cannot_roll_back_or_expire(self) -> None:
        limiter = Limiter(self.clock)
        limiter.configure("frozen", {"capacity": 4, "refill_per_second": 0.0001})
        reservation = limiter.reserve("frozen", 3, ttl_seconds=5)  # tokens 1
        self.assertEqual(limiter.consume(reservation["reservation_id"])["used"], 3)
        with self.assertRaises(LimitNotFound):
            limiter.rollback(reservation["reservation_id"])
        self.clock.t += 10                                  # well past expiry: no refund
        state = limiter.state("frozen")
        self.assertEqual((state["remaining"], state["used"]), (1, 3))

    def test_unknown_and_rolled_back_reservations_are_404(self) -> None:
        with self.assertRaises(LimitNotFound):
            self.limiter.consume("nope")
        reservation = self.limiter.reserve("tenant-a", 1)
        self.limiter.rollback(reservation["reservation_id"])
        with self.assertRaises(LimitNotFound):
            self.limiter.consume(reservation["reservation_id"])
        self.assertEqual(self.limiter.state("tenant-a")["used"], 0)

    def test_consume_after_reconfigure_uses_old_cost_and_new_capacity(self) -> None:
        reservation = self.limiter.reserve("tenant-a", 4)   # tokens 1
        self.limiter.configure("tenant-a", {"capacity": 10, "refill_per_second": 2.0})
        self.clock.t += 1.0                                 # 2 tokens refill at the new rate
        result = self.limiter.consume(reservation["reservation_id"])
        self.assertEqual((result["remaining"], result["capacity"], result["used"]), (3, 10, 4))
        state = self.limiter.state("tenant-a")
        self.assertEqual((state["remaining"], state["used"]), (3, 4))

    def test_concurrent_consume_and_rollback_have_one_winner_each(self) -> None:
        limiter = Limiter(self.clock)
        limiter.configure("hot", {"capacity": 200, "refill_per_second": 0.0001})
        ids = [limiter.reserve("hot", 1, ttl_seconds=3600)["reservation_id"] for _ in range(200)]
        consumed: list[str] = []
        rolled: list[str] = []
        errors: list[BaseException] = []
        list_lock = threading.Lock()

        def settle(consume: bool, rid: str) -> None:
            try:
                if consume:
                    result = limiter.consume(rid)
                    if result["used"] <= 0:
                        raise AssertionError("consume reported no usage")
                    winner = consumed
                else:
                    limiter.rollback(rid)
                    winner = rolled
                with list_lock:
                    winner.append(rid)
            except LimitNotFound:
                pass
            except BaseException as error:  # noqa: BLE001 - surface thread failures on the main thread
                errors.append(error)

        threads = []
        for index, rid in enumerate(ids):
            threads.append(threading.Thread(target=settle, args=(True, rid)))
            threads.append(threading.Thread(target=settle, args=(False, rid)))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        self.assertEqual(len(consumed) + len(rolled), 200)
        self.assertEqual(set(consumed) & set(rolled), set())
        state = limiter.state("hot")
        self.assertEqual(state["used"], len(consumed))
        self.assertEqual(state["remaining"], len(rolled))  # negligible refill, rollback capped at capacity
        self.assertEqual([r for r in limiter._reservations.values() if r.key == "hot"], [])

    def test_concurrent_duplicate_consumes_book_usage_once(self) -> None:
        limiter = Limiter(self.clock)
        limiter.configure("hot", {"capacity": 5, "refill_per_second": 0.0001})
        rid = limiter.reserve("hot", 3, ttl_seconds=3600)["reservation_id"]
        responses: list[dict] = []
        errors: list[BaseException] = []

        def confirm() -> None:
            try:
                responses.append(limiter.consume(rid))
            except BaseException as error:  # noqa: BLE001
                errors.append(error)

        threads = [threading.Thread(target=confirm) for _ in range(16)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        self.assertEqual(len(responses), 16)
        self.assertTrue(all(response == responses[0] for response in responses))
        state = limiter.state("hot")
        self.assertEqual((state["remaining"], state["used"]), (2, 3))

    def test_concurrent_consume_against_expiry_never_refunds_and_books(self) -> None:
        limiter = Limiter(self.clock)
        limiter.configure("hot", {"capacity": 10, "refill_per_second": 0.0001})
        ids = [limiter.reserve("hot", 1, ttl_seconds=10)["reservation_id"] for _ in range(10)]
        self.clock.t += 10
        statuses: list[str] = []
        errors: list[BaseException] = []

        def finish(consume: bool, rid: str) -> None:
            try:
                if consume:
                    limiter.consume(rid)
                    statuses.append("consumed")
                else:
                    limiter.rollback(rid)
                    statuses.append("rolled")
            except LimitNotFound:
                pass
            except BaseException as error:  # noqa: BLE001
                errors.append(error)

        threads = []
        for index, rid in enumerate(ids):
            threads.append(threading.Thread(target=finish, args=(True, rid)))
            threads.append(threading.Thread(target=finish, args=(False, rid)))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        self.assertEqual(statuses, [])                            # everything expired: neither endpoint wins
        state = limiter.state("hot")
        self.assertEqual((state["remaining"], state["used"]), (10, 0))  # refunded exactly once, never booked


class ReservationHttpTests(unittest.TestCase):
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

    def test_reserve_and_rollback_lifecycle(self) -> None:
        self.request("PUT", "/v1/limits/r-1", {"capacity": 3, "refill_per_second": 1})
        status, body, _ = self.request("POST", "/v1/reservations", {"key": "r-1", "cost": 2})
        self.assertEqual(status, 200)
        self.assertEqual((body["key"], body["cost"], body["remaining"], body["capacity"]), ("r-1", 2, 1, 3))
        rid = body["reservation_id"]

        _, state, _ = self.request("GET", "/v1/limits/r-1")
        self.assertEqual((state["remaining"], state["used"]), (1, 0))

        status, body, _ = self.request("DELETE", f"/v1/reservations/{rid}")
        self.assertEqual(status, 200)
        self.assertEqual((body["reservation_id"], body["rolled_back"], body["remaining"], body["capacity"]),
                         (rid, True, 3, 3))

    def test_rollback_twice_and_unknown_id_are_404(self) -> None:
        self.request("PUT", "/v1/limits/r-2", {"capacity": 2, "refill_per_second": 1})
        _, body, _ = self.request("POST", "/v1/reservations", {"key": "r-2"})
        rid = body["reservation_id"]
        self.assertEqual(self.request("DELETE", f"/v1/reservations/{rid}")[0], 200)
        self.assertEqual(self.request("DELETE", f"/v1/reservations/{rid}")[0], 404)
        self.assertEqual(self.request("DELETE", "/v1/reservations/nope")[0], 404)

    def test_bad_routes_and_methods_are_404(self) -> None:
        self.assertEqual(self.request("DELETE", "/v1/reservations")[0], 404)
        self.assertEqual(self.request("DELETE", "/v1/reservations/a/b")[0], 404)
        self.assertEqual(self.request("DELETE", "/v1/limits/r-2")[0], 404)

    def test_reservation_errors_via_http(self) -> None:
        self.request("PUT", "/v1/limits/r-3", {"capacity": 1, "refill_per_second": 1})
        self.request("POST", "/v1/reservations", {"key": "r-3"})
        status, body, headers = self.request("POST", "/v1/reservations", {"key": "r-3"})
        self.assertEqual((status, body["error"]["code"]), (429, "over_quota"))
        self.assertIn("Retry-After", headers)
        self.assertGreaterEqual(float(headers["Retry-After"]), 1.0)
        self.assertEqual(self.request("POST", "/v1/reservations", {"key": "missing"})[0], 404)
        status, body, _ = self.request("POST", "/v1/reservations", {"key": "r-3", "cost": True})
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        status, body, _ = self.request("POST", "/v1/reservations", {"key": "", "cost": 1})
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))

    def test_ttl_defaults_and_is_echoed_via_http(self) -> None:
        self.request("PUT", "/v1/limits/e-1", {"capacity": 3, "refill_per_second": 1})
        status, body, _ = self.request("POST", "/v1/reservations", {"key": "e-1", "cost": 1})
        self.assertEqual(status, 200)
        self.assertEqual(body["ttl_seconds"], 60)
        status, body, _ = self.request("POST", "/v1/reservations", {"key": "e-1", "cost": 1, "ttl_seconds": 10})
        self.assertEqual((status, body["ttl_seconds"]), (200, 10))

    def test_invalid_ttl_is_400_and_deducts_nothing(self) -> None:
        self.request("PUT", "/v1/limits/e-2", {"capacity": 2, "refill_per_second": 1})
        for bad_ttl in [0, 86401, 1.5, True, "60"]:
            status, body, _ = self.request("POST", "/v1/reservations",
                                           {"key": "e-2", "cost": 1, "ttl_seconds": bad_ttl})
            self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"), bad_ttl)
        status, body, _ = self.request("POST", "/v1/reservations", {"key": "e-2", "ttl_seconds": "x"})
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        # No tokens were taken by any rejected request.
        _, state, _ = self.request("GET", "/v1/limits/e-2")
        self.assertEqual((state["remaining"], state["used"]), (2, 0))

    def test_expired_reservation_is_refunded_on_read_and_rollback_is_404(self) -> None:
        self.request("PUT", "/v1/limits/e-3", {"capacity": 4, "refill_per_second": 0.0001})
        _, body, _ = self.request("POST", "/v1/reservations", {"key": "e-3", "cost": 3, "ttl_seconds": 10})
        rid = body["reservation_id"]
        _, state, _ = self.request("GET", "/v1/limits/e-3")
        self.assertEqual((state["remaining"], state["used"]), (1, 0))

        type(self).clock.t += 9
        _, state, _ = self.request("GET", "/v1/limits/e-3")
        self.assertEqual(state["remaining"], 1)                       # one second before due: still held

        type(self).clock.t += 1
        _, state, _ = self.request("GET", "/v1/limits/e-3")          # inclusive boundary triggers refund
        self.assertEqual((state["remaining"], state["used"]), (4, 0))

        status, body, _ = self.request("DELETE", f"/v1/reservations/{rid}")
        self.assertEqual((status, body["error"]["code"]), (404, "not_found"))
        _, state, _ = self.request("GET", "/v1/limits/e-3")
        self.assertEqual(state["remaining"], 4)                      # the failed rollback adds nothing

    def test_unexpired_reservation_with_ttl_rolls_back_normally(self) -> None:
        self.request("PUT", "/v1/limits/e-4", {"capacity": 2, "refill_per_second": 1})
        _, body, _ = self.request("POST", "/v1/reservations", {"key": "e-4", "cost": 2, "ttl_seconds": 30})
        rid = body["reservation_id"]
        type(self).clock.t += 29
        status, body, _ = self.request("DELETE", f"/v1/reservations/{rid}")
        self.assertEqual(status, 200)
        self.assertEqual((body["reservation_id"], body["rolled_back"], body["remaining"], body["capacity"]),
                         (rid, True, 2, 2))


class ConsumeHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        import http.client

        cls.http_client = http.client
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

    def raw_request(self, method: str, path: str, payload: bytes | None,
                    content_length: str | object = "auto") -> tuple[int, dict]:
        connection = self.http_client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        connection.putrequest(method, path)
        if content_length != "omit":
            connection.putheader("Content-Length",
                                 str(len(payload)) if content_length == "auto" else content_length)
        connection.endheaders(payload if payload is not None else b"")
        response = connection.getresponse()
        body = json.loads(response.read() or b"{}")
        connection.close()
        return response.status, body

    def test_consume_lifecycle_is_idempotent_and_blocks_rollback(self) -> None:
        self.request("PUT", "/v1/limits/c-1", {"capacity": 4, "refill_per_second": 1})
        _, body, _ = self.request("POST", "/v1/reservations", {"key": "c-1", "cost": 3})
        rid = body["reservation_id"]

        status, first, _ = self.request("POST", f"/v1/reservations/{rid}/consume", {})
        self.assertEqual(status, 200)
        self.assertEqual(first, {"reservation_id": rid, "consumed": True,
                                 "remaining": 1, "capacity": 4, "used": 3})
        _, state, _ = self.request("GET", "/v1/limits/c-1")
        self.assertEqual((state["remaining"], state["used"]), (1, 3))    # same instant, same accounting

        type(self).clock.t += 2
        status, second, _ = self.request("POST", f"/v1/reservations/{rid}/consume", {})
        self.assertEqual(status, 200)
        self.assertEqual(second, first)                                   # identical fields, no double used
        _, state, _ = self.request("GET", "/v1/limits/c-1")
        self.assertEqual(state["used"], 3)

        self.assertEqual(self.request("DELETE", f"/v1/reservations/{rid}")[0], 404)
        type(self).clock.t += 100                                         # past TTL: no late refund
        _, state, _ = self.request("GET", "/v1/limits/c-1")
        self.assertLessEqual(state["remaining"], 4)
        self.assertEqual(state["used"], 3)

    def test_consume_unknown_rolled_back_and_expired_are_404(self) -> None:
        self.request("PUT", "/v1/limits/c-2", {"capacity": 3, "refill_per_second": 0.0001})
        self.assertEqual(self.request("POST", "/v1/reservations/nope/consume", {})[0], 404)

        _, body, _ = self.request("POST", "/v1/reservations", {"key": "c-2", "cost": 1})
        rid = body["reservation_id"]
        self.assertEqual(self.request("DELETE", f"/v1/reservations/{rid}")[0], 200)
        status, body, _ = self.request("POST", f"/v1/reservations/{rid}/consume", {})
        self.assertEqual((status, body["error"]["code"]), (404, "not_found"))

        _, body, _ = self.request("POST", "/v1/reservations", {"key": "c-2", "cost": 2, "ttl_seconds": 10})
        rid = body["reservation_id"]
        type(self).clock.t += 10                                          # inclusive boundary
        status, body, _ = self.request("POST", f"/v1/reservations/{rid}/consume", {})
        self.assertEqual((status, body["error"]["code"]), (404, "not_found"))
        _, state, _ = self.request("GET", "/v1/limits/c-2")
        self.assertEqual((state["remaining"], state["used"]), (3, 0))     # refunded, never booked

    def test_consume_validation_errors_are_400_and_change_nothing(self) -> None:
        self.request("PUT", "/v1/limits/c-3", {"capacity": 3, "refill_per_second": 1})
        _, body, _ = self.request("POST", "/v1/reservations", {"key": "c-3", "cost": 2})
        rid = body["reservation_id"]
        path = f"/v1/reservations/{rid}/consume"
        for payload in (b"", b"[]", b"null", b"5", b'"x"', b'{"x": 1}', b'{"consumed": true}'):
            status, parsed = self.raw_request("POST", path, payload)
            self.assertEqual((status, parsed["error"]["code"]), (400, "invalid_request"), payload)
        status, parsed = self.raw_request("POST", path, b'{"a": \xff}')   # invalid UTF-8 JSON
        self.assertEqual((status, parsed["error"]["code"]), (400, "invalid_request"))
        status, parsed = self.raw_request("POST", path, b"{}", content_length="omit")  # missing
        self.assertEqual((status, parsed["error"]["code"]), (400, "invalid_request"))
        status, parsed = self.raw_request("POST", path, b"{}", content_length="nine")  # not an integer
        self.assertEqual((status, parsed["error"]["code"]), (400, "invalid_request"))

        _, state, _ = self.request("GET", "/v1/limits/c-3")
        self.assertEqual((state["remaining"], state["used"]), (1, 0))     # nothing was booked
        self.assertEqual(self.request("DELETE", f"/v1/reservations/{rid}")[0], 200)     # still live

    def test_route_and_method_mismatch_beats_body_validation(self) -> None:
        # Garbage bodies on non-matching routes still get 404, never 400.
        status, _ = self.raw_request("POST", "/v1/nope", b'{"x": 1}')
        self.assertEqual(status, 404)
        status, _ = self.raw_request("POST", "/v1/reservations/a/b", b'[]')
        self.assertEqual(status, 404)
        status, _ = self.raw_request("GET", "/v1/reservations/a/consume", None, content_length="omit")
        self.assertEqual(status, 404)
        status, _ = self.raw_request("PUT", "/v1/reservations/a/consume", b'{}')
        self.assertEqual(status, 404)
        # The reservation collection still creates reservations: {} there is invalid_request, not consume.
        self.assertEqual(self.request("POST", "/v1/reservations", {})[0], 400)

    def test_consume_after_reconfigure_books_old_cost_against_new_limit(self) -> None:
        self.request("PUT", "/v1/limits/c-4", {"capacity": 5, "refill_per_second": 1})
        _, body, _ = self.request("POST", "/v1/reservations", {"key": "c-4", "cost": 4})
        rid = body["reservation_id"]
        self.request("PUT", "/v1/limits/c-4", {"capacity": 10, "refill_per_second": 2})
        type(self).clock.t += 1.0                                          # 2 tokens at the new rate
        status, body, _ = self.request("POST", f"/v1/reservations/{rid}/consume", {})
        self.assertEqual(status, 200)
        self.assertEqual((body["remaining"], body["capacity"], body["used"]), (3, 10, 4))


class ScriptedClock:
    """Each call pops the next reading (the last one repeats): a clock that drifts within one call."""

    def __init__(self, readings: list[float]) -> None:
        self.readings = list(readings)

    def __call__(self) -> float:
        if len(self.readings) > 1:
            return self.readings.pop(0)
        return self.readings[0]


class JitterClock:
    """Thread-safe clock whose readings oscillate slightly below a fixed moment."""

    def __init__(self, t: float) -> None:
        self.t = t
        self.calls = 0
        self.lock = threading.Lock()

    def __call__(self) -> float:
        with self.lock:
            self.calls += 1
            return self.t - (self.calls % 3)


class ClockRegressionUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.limiter = Limiter(self.clock)

    def test_backwards_reading_never_refills_and_is_not_recounted_on_recovery(self) -> None:
        self.clock.t = 100.0
        self.limiter.configure("k", {"capacity": 10, "refill_per_second": 1.0})
        self.assertTrue(self.limiter.check("k", 10)["allowed"])      # bucket empty at t=100
        self.clock.t = 90.0                                          # clock jumps backwards
        self.assertEqual(self.limiter.state("k")["remaining"], 0)    # treated as still t=100
        self.clock.t = 101.0                                         # recovers just past the watermark
        # Only 100→101 counts as elapsed; the 90→101 regressed interval is never re-counted.
        self.assertEqual(self.limiter.state("k")["remaining"], 1)

    def test_stalled_clock_refills_nothing(self) -> None:
        self.clock.t = 100.0
        self.limiter.configure("k", {"capacity": 10, "refill_per_second": 1.0})
        self.limiter.check("k", 10)
        for _ in range(3):
            self.assertEqual(self.limiter.state("k")["remaining"], 0)

    def test_reservation_survives_regression_and_settles_once_at_effective_expiry(self) -> None:
        self.clock.t = 100.0
        self.limiter.configure("k", {"capacity": 10, "refill_per_second": 0.0001})
        reservation = self.limiter.reserve("k", 4, ttl_seconds=10)   # effective 100, due 110
        self.assertEqual(self.limiter.state("k")["remaining"], 6)
        self.clock.t = 95.0                                          # regressed: time stays at 100
        self.assertEqual(self.limiter.state("k")["remaining"], 6)    # still held, no early refund
        self.clock.t = 110.0                                         # effective expiry moment
        self.assertEqual(self.limiter.state("k")["remaining"], 10)   # refunded exactly once
        self.assertEqual(self.limiter.state("k")["remaining"], 10)   # same moment: no second refund
        with self.assertRaises(LimitNotFound):
            self.limiter.rollback(reservation["reservation_id"])
        with self.assertRaises(LimitNotFound):
            self.limiter.consume(reservation["reservation_id"])
        state = self.limiter.state("k")
        self.assertEqual((state["remaining"], state["used"]), (10, 0))

    def test_reconfigure_under_regression_anchors_at_the_watermark(self) -> None:
        self.clock.t = 100.0
        self.limiter.configure("k", {"capacity": 10, "refill_per_second": 1.0})
        self.limiter.check("k", 10)                                  # empty at t=100
        self.clock.t = 95.0
        # Reconfigure with a regressed reading: the new bucket anchors at the watermark, not 95.
        self.limiter.configure("k", {"capacity": 10, "refill_per_second": 1.0})
        self.clock.t = 101.0
        self.assertEqual(self.limiter.state("k")["remaining"], 1)    # only 100→101 counts

    def test_single_operation_reads_the_clock_exactly_once(self) -> None:
        clock = ScriptedClock([1000.0, 1000.0, 1010.0])
        limiter = Limiter(clock)
        limiter.configure("k", {"capacity": 5, "refill_per_second": 1.0})   # reads 1000
        reservation = limiter.reserve("k", 2, ttl_seconds=10)               # one read: 1000, due 1010
        state = limiter.state("k")                                          # reads 1010: due, refunded
        self.assertEqual(state["remaining"], 5)
        with self.assertRaises(LimitNotFound):
            limiter.rollback(reservation["reservation_id"])

    def test_mid_call_regression_is_clamped_to_the_watermark(self) -> None:
        clock = ScriptedClock([100.0, 90.0, 109.0, 110.0])
        limiter = Limiter(clock)
        limiter.configure("k", {"capacity": 5, "refill_per_second": 0.0001})  # effective 100
        limiter.reserve("k", 2, ttl_seconds=10)     # reads 90 mid-call, clamped to 100: due at 110
        self.assertEqual(limiter.state("k")["remaining"], 3)   # effective 109: still held
        self.assertEqual(limiter.state("k")["remaining"], 5)   # effective 110: refunded once

    def test_concurrent_operations_with_jittering_clock_never_oversell(self) -> None:
        limiter = Limiter(JitterClock(1000.0))
        limiter.configure("hot", {"capacity": 100, "refill_per_second": 0.0001})
        outcomes: list[bool] = []
        outcomes_lock = threading.Lock()

        def attempt(check: bool) -> None:
            try:
                if check:
                    limiter.check("hot", 1)
                else:
                    limiter.reserve("hot", 1)
                ok = True
            except OverQuota:
                ok = False
            with outcomes_lock:
                outcomes.append(ok)

        threads = [threading.Thread(target=attempt, args=(i % 2 == 0,)) for i in range(400)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(sum(outcomes), 100)
        self.assertEqual(limiter.state("hot")["remaining"], 0)


class ClockRegressionHttpTests(unittest.TestCase):
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

    def test_regression_is_not_recounted_via_http(self) -> None:
        clock = type(self).clock
        clock.t = 100.0
        self.request("PUT", "/v1/limits/reg-1", {"capacity": 10, "refill_per_second": 1})
        status, body, _ = self.request("POST", "/v1/check", {"key": "reg-1", "cost": 10})
        self.assertEqual((status, body["remaining"]), (200, 0))
        clock.t = 90.0                                                # clock jumps backwards
        _, state, _ = self.request("GET", "/v1/limits/reg-1")
        self.assertEqual(state["remaining"], 0)
        clock.t = 101.0                                               # only 100→101 may count
        _, state, _ = self.request("GET", "/v1/limits/reg-1")
        self.assertEqual(state["remaining"], 1)

    def test_reservation_outlives_regression_via_http(self) -> None:
        clock = type(self).clock
        clock.t = 200.0
        self.request("PUT", "/v1/limits/reg-2", {"capacity": 10, "refill_per_second": 0.0001})
        _, body, _ = self.request("POST", "/v1/reservations", {"key": "reg-2", "cost": 4, "ttl_seconds": 10})
        rid = body["reservation_id"]
        self.assertEqual(body["remaining"], 6)
        clock.t = 195.0                                               # regressed: still held
        _, state, _ = self.request("GET", "/v1/limits/reg-2")
        self.assertEqual((state["remaining"], state["used"]), (6, 0))
        clock.t = 210.0                                               # effective expiry moment
        _, state, _ = self.request("GET", "/v1/limits/reg-2")
        self.assertEqual((state["remaining"], state["used"]), (10, 0))
        _, state, _ = self.request("GET", "/v1/limits/reg-2")         # same moment: no second refund
        self.assertEqual(state["remaining"], 10)
        self.assertEqual(self.request("DELETE", f"/v1/reservations/{rid}")[0], 404)
        _, state, _ = self.request("GET", "/v1/limits/reg-2")
        self.assertEqual(state["remaining"], 10)


class RetryAfterHttpTests(unittest.TestCase):
    """429 Retry-After: one deterministic ceiling-to-millisecond rule for check and reservations."""

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

    def test_sub_millisecond_deficit_ceilings_up_not_to_zero(self) -> None:
        clock = type(self).clock
        clock.t = 8000.0
        self.request("PUT", "/v1/limits/ra-1", {"capacity": 1, "refill_per_second": 1})
        status, _, _ = self.request("POST", "/v1/check", {"key": "ra-1", "cost": 1})
        self.assertEqual(status, 200)
        clock.t += 0.9999                                          # deficit: 0.0001s at 1 token/s
        status, body, headers = self.request("POST", "/v1/check", {"key": "ra-1", "cost": 1})
        self.assertEqual((status, body["error"]["code"]), (429, "over_quota"))
        self.assertEqual(headers["Retry-After"], "0.001")          # never 0.000

    def test_check_and_reservation_share_the_same_hint(self) -> None:
        clock = type(self).clock
        clock.t = 5000.0
        self.request("PUT", "/v1/limits/ra-2", {"capacity": 1, "refill_per_second": 0.3})
        status, _, _ = self.request("POST", "/v1/reservations", {"key": "ra-2", "cost": 1})
        self.assertEqual(status, 200)                              # bucket empty, deficit 1
        # exact wait is 1/0.3 = 3.333...s: ceiling to milliseconds, identical on both routes.
        expected = "3.334"
        status, _, check_headers = self.request("POST", "/v1/check", {"key": "ra-2", "cost": 1})
        self.assertEqual(status, 429)
        status, _, reserve_headers = self.request("POST", "/v1/reservations", {"key": "ra-2", "cost": 1})
        self.assertEqual(status, 429)
        self.assertEqual(check_headers["Retry-After"], expected)
        self.assertEqual(reserve_headers["Retry-After"], expected)
        self.assertGreaterEqual(float(expected), 1 / 0.3)

    def test_hint_covers_exact_wait_and_rejections_leave_no_trace(self) -> None:
        clock = type(self).clock
        clock.t = 6000.0
        self.request("PUT", "/v1/limits/ra-3", {"capacity": 3, "refill_per_second": 0.7})
        status, _, _ = self.request("POST", "/v1/check", {"key": "ra-3", "cost": 3})
        self.assertEqual(status, 200)
        clock.t += 2.0                                             # 1.4 tokens back, deficit 1.6
        deficit, rate = 1.6, 0.7
        for path, body in [("/v1/check", {"key": "ra-3", "cost": 3}),
                           ("/v1/reservations", {"key": "ra-3", "cost": 3})]:
            status, _, headers = self.request("POST", path, body)
            self.assertEqual(status, 429)
            hinted = float(headers["Retry-After"])
            self.assertGreaterEqual(hinted, deficit / rate)
            self.assertLess(hinted, deficit / rate + 0.001)        # ceiling, not padding
        # Neither rejection deducted tokens, booked usage, or posted ledger events.
        _, state, _ = self.request("GET", "/v1/limits/ra-3")
        self.assertEqual((state["remaining"], state["used"]), (1, 3))
        _, ledger, _ = self.request("GET", "/v1/ledgers/ra-3")
        self.assertEqual(ledger["totals"], {"accepted_count": 1, "accepted_cost": 3})

    def test_stalled_clock_keeps_the_hint_stable(self) -> None:
        clock = type(self).clock
        clock.t = 7000.0
        self.request("PUT", "/v1/limits/ra-4", {"capacity": 1, "refill_per_second": 2})
        self.request("POST", "/v1/check", {"key": "ra-4", "cost": 1})
        first = self.request("POST", "/v1/check", {"key": "ra-4", "cost": 1})[2]["Retry-After"]
        self.assertEqual(first, "0.500")
        for _ in range(3):                                         # clock never advances
            self.assertEqual(self.request("POST", "/v1/check", {"key": "ra-4", "cost": 1})[2]["Retry-After"],
                             first)
        clock.t = 6990.0                                           # regression: hint must not shrink
        self.assertEqual(self.request("POST", "/v1/check", {"key": "ra-4", "cost": 1})[2]["Retry-After"],
                         first)


class LedgerUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.limiter = Limiter(self.clock)
        self.limiter.configure("tenant-a", {"capacity": 5, "refill_per_second": 1.0})

    def test_successful_check_records_one_event_matching_used(self) -> None:
        start = self.clock.t                                      # effective moment of the calls
        result = self.limiter.check("tenant-a", 2)
        ledger = self.limiter.ledger("tenant-a")
        self.assertEqual(set(ledger), {"key", "totals", "events"})
        self.assertEqual(ledger["key"], "tenant-a")
        self.assertEqual(ledger["totals"], {"accepted_count": 1, "accepted_cost": 2})
        event, = ledger["events"]
        self.assertEqual(set(event), {"seq", "source", "reservation_id", "cost",
                                      "remaining", "capacity", "effective_at"})
        self.assertEqual(event, {"seq": 1, "source": "check", "reservation_id": None, "cost": 2,
                                 "remaining": 3, "capacity": 5, "effective_at": start})
        self.assertEqual(event["remaining"], result["remaining"])
        self.assertEqual(ledger["totals"]["accepted_cost"], self.limiter.state("tenant-a")["used"])

    def test_successful_consume_records_one_event_with_reservation_id(self) -> None:
        limiter = Limiter(self.clock)
        limiter.configure("frozen", {"capacity": 5, "refill_per_second": 0.0001})
        reservation = limiter.reserve("frozen", 3, ttl_seconds=60)  # tokens 2, still no event
        self.assertEqual(limiter.ledger("frozen")["events"], [])
        self.clock.t += 5                                            # refill negligible here
        limiter.consume(reservation["reservation_id"])
        ledger = limiter.ledger("frozen")
        self.assertEqual(ledger["totals"], {"accepted_count": 1, "accepted_cost": 3})
        event, = ledger["events"]
        self.assertEqual(event["source"], "reservation_consume")
        self.assertEqual(event["reservation_id"], reservation["reservation_id"])
        self.assertEqual((event["cost"], event["remaining"], event["capacity"]), (3, 2, 5))
        self.assertEqual(event["effective_at"], self.clock.t)    # the consume's own effective moment

    def test_duplicate_consume_appends_no_second_event(self) -> None:
        reservation = self.limiter.reserve("tenant-a", 2)
        first = self.limiter.consume(reservation["reservation_id"])
        self.clock.t += 3
        self.assertTrue(self.limiter.check("tenant-a", 1)["allowed"])
        second = self.limiter.consume(reservation["reservation_id"])
        self.assertEqual(second, first)                          # baseline idempotency intact
        ledger = self.limiter.ledger("tenant-a", 1000)
        self.assertEqual([event["source"] for event in ledger["events"]],
                         ["reservation_consume", "check"])
        self.assertEqual(ledger["totals"], {"accepted_count": 2, "accepted_cost": 3})
        self.assertEqual(self.limiter.state("tenant-a")["used"], 3)

    def test_failures_rollbacks_and_expiries_generate_no_events(self) -> None:
        for _ in range(5):
            self.limiter.check("tenant-a", 1)
        with self.assertRaises(OverQuota):
            self.limiter.check("tenant-a", 1)                    # rejected: nothing booked
        self.clock.t += 5                                        # refill to a full bucket
        reservation = self.limiter.reserve("tenant-a", 2)        # hold: not booked
        self.limiter.rollback(reservation["reservation_id"])     # undone: not booked
        expiring = self.limiter.reserve("tenant-a", 2, ttl_seconds=5)
        self.clock.t += 5
        self.limiter.state("tenant-a")                           # settles the due hold
        with self.assertRaises(LimitNotFound):
            self.limiter.consume(expiring["reservation_id"])     # expired hold never books
        ledger = self.limiter.ledger("tenant-a", 1000)
        self.assertEqual(ledger["totals"], {"accepted_count": 5, "accepted_cost": 5})
        self.assertTrue(all(event["source"] == "check" for event in ledger["events"]))
        self.assertEqual(self.limiter.state("tenant-a")["used"], 5)

    def test_seq_is_dense_across_mixed_sources(self) -> None:
        self.limiter.check("tenant-a", 1)                        # tokens 4
        reservation = self.limiter.reserve("tenant-a", 2)        # tokens 2
        self.limiter.consume(reservation["reservation_id"])      # tokens stay 2, used 3
        self.limiter.check("tenant-a", 1)                        # tokens 1, used 4
        ledger = self.limiter.ledger("tenant-a", 1000)
        events = ledger["events"]
        self.assertEqual([event["seq"] for event in events], [1, 2, 3])
        self.assertEqual([event["source"] for event in events],
                         ["check", "reservation_consume", "check"])
        self.assertEqual([event["cost"] for event in events], [1, 2, 1])
        self.assertEqual([event["reservation_id"] for event in events],
                         [None, reservation["reservation_id"], None])
        self.assertEqual(ledger["totals"], {"accepted_count": 3, "accepted_cost": 4})
        self.assertEqual(self.limiter.state("tenant-a")["used"], 4)

    def test_tail_defaults_to_100_and_limits_select_the_newest_events(self) -> None:
        limiter = Limiter(self.clock)
        limiter.configure("big", {"capacity": 1000, "refill_per_second": 0.0001})
        for _ in range(150):
            limiter.check("big", 1)                              # clock frozen: all 150 book
        default = limiter.ledger("big")
        self.assertEqual(len(default["events"]), 100)
        self.assertEqual([event["seq"] for event in default["events"]], list(range(51, 151)))
        fifty = limiter.ledger("big", 50)
        self.assertEqual([event["seq"] for event in fifty["events"]], list(range(101, 151)))
        one = limiter.ledger("big", 1)
        self.assertEqual([event["seq"] for event in one["events"]], [150])
        full = limiter.ledger("big", 1000)
        self.assertEqual([event["seq"] for event in full["events"]], list(range(1, 151)))
        # Totals always describe the complete history, regardless of the tail window.
        for view in (default, fifty, one, full):
            self.assertEqual(view["totals"], {"accepted_count": 150, "accepted_cost": 150})

    def test_unknown_key_ledger_is_not_found(self) -> None:
        with self.assertRaises(LimitNotFound):
            self.limiter.ledger("tenant-x")

    def test_ledger_read_samples_no_clock_and_advances_no_watermark(self) -> None:
        clock = Clock()
        clock.t = 100.0
        limiter = Limiter(clock)
        limiter.configure("k", {"capacity": 10, "refill_per_second": 1.0})
        limiter.check("k", 10)                                   # empty bucket, watermark 100
        clock.t = 105.0
        limiter.ledger("k")                                      # must not sample the clock
        clock.t = 104.0                                          # below the reading the ledger skipped
        # state() ticks now: effective = max(104, watermark). A ledger that had ticked would have
        # pinned the watermark to 105 and this read would show 5 refilled tokens instead of 4.
        self.assertEqual(limiter.state("k")["remaining"], 4)

    def test_ledger_read_settles_no_due_reservation_and_conjures_no_tokens(self) -> None:
        limiter = Limiter(self.clock)
        limiter.configure("frozen", {"capacity": 5, "refill_per_second": 0.0001})
        reservation = limiter.reserve("frozen", 3, ttl_seconds=10)  # tokens 2
        self.clock.t += 10                                       # the hold is now due
        view = limiter.ledger("frozen")
        self.assertEqual(view["totals"], {"accepted_count": 0, "accepted_cost": 0})
        self.assertIn(reservation["reservation_id"], limiter._reservations)  # not settled by a read
        state = limiter.state("frozen")                          # this read performs the one refund
        self.assertEqual((state["remaining"], state["used"]), (5, 0))
        self.assertNotIn(reservation["reservation_id"], limiter._reservations)
        self.assertEqual(limiter.ledger("frozen")["events"], [])

    def test_stalled_or_regressed_clock_returns_identical_ledgers(self) -> None:
        self.limiter.check("tenant-a", 2)
        first = self.limiter.ledger("tenant-a", 1000)
        self.clock.t += 100                                      # a read never samples this...
        second = self.limiter.ledger("tenant-a", 1000)
        self.clock.t -= 200                                      # ...nor a regression
        third = self.limiter.ledger("tenant-a", 1000)
        self.assertEqual(second, first)
        self.assertEqual(third, first)

    def test_reconfigure_keeps_history_and_new_events_use_new_capacity(self) -> None:
        self.limiter.check("tenant-a", 2)                        # tokens 3
        self.limiter.configure("tenant-a", {"capacity": 10, "refill_per_second": 2.0})
        self.limiter.check("tenant-a", 1)                        # min(3,10) -> tokens 2
        ledger = self.limiter.ledger("tenant-a", 1000)
        self.assertEqual([event["capacity"] for event in ledger["events"]], [5, 10])
        self.assertEqual([event["remaining"] for event in ledger["events"]], [3, 2])
        self.assertEqual(ledger["totals"], {"accepted_count": 2, "accepted_cost": 3})
        self.assertEqual(self.limiter.state("tenant-a")["used"], 3)

    def test_concurrent_mixed_operations_keep_a_dense_consistent_ledger(self) -> None:
        limiter = Limiter(self.clock)                            # clock frozen for the whole test
        limiter.configure("hot", {"capacity": 300, "refill_per_second": 0.0001})
        rids = [limiter.reserve("hot", 1, ttl_seconds=3600)["reservation_id"] for _ in range(100)]
        consumed: list[str] = []
        rolled: list[str] = []
        errors: list[BaseException] = []
        reader_counts: list[list[int]] = []
        list_lock = threading.Lock()
        tail_choices = [1, 2, 7, 100, 999, 1000]

        def spend_check() -> None:
            try:
                limiter.check("hot", 1)                          # 200 free tokens: every check wins
            except BaseException as error:  # noqa: BLE001
                with list_lock:
                    errors.append(error)

        def settle(do_consume: bool, rid: str) -> None:
            try:
                if do_consume:
                    limiter.consume(rid)
                    with list_lock:
                        consumed.append(rid)
                else:
                    limiter.rollback(rid)
                    with list_lock:
                        rolled.append(rid)
            except LimitNotFound:
                pass
            except BaseException as error:  # noqa: BLE001
                with list_lock:
                    errors.append(error)

        def read_ledger(reader_index: int) -> None:
            counts: list[int] = []
            try:
                for round_index in range(50):
                    tail = tail_choices[(reader_index + round_index) % len(tail_choices)]
                    view = limiter.ledger("hot", tail)
                    events = view["events"]
                    count = view["totals"]["accepted_count"]
                    seqs = [event["seq"] for event in events]
                    if len(events) != min(tail, count) \
                            or seqs != list(range(count - len(seqs) + 1, count + 1)):
                        raise AssertionError(f"inconsistent ledger view: {view['totals']}, {seqs}")
                    if any(event["source"] not in ("check", "reservation_consume") for event in events):
                        raise AssertionError("unexpected event source")
                    counts.append(count)
                if counts != sorted(counts):
                    raise AssertionError("accepted_count went backwards during reads")
            except BaseException as error:  # noqa: BLE001
                with list_lock:
                    errors.append(error)
            with list_lock:
                reader_counts.append(counts)

        threads = [threading.Thread(target=spend_check) for _ in range(100)]
        for index, rid in enumerate(rids):
            threads.append(threading.Thread(target=settle, args=(True, rid)))
            threads.append(threading.Thread(target=settle, args=(False, rid)))
        threads += [threading.Thread(target=read_ledger, args=(index,)) for index in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])

        ledger = limiter.ledger("hot", 1000)
        events = ledger["events"]
        booked = 100 + len(consumed)
        self.assertEqual([event["seq"] for event in events], list(range(1, booked + 1)))  # no gap/dup
        self.assertEqual(ledger["totals"], {"accepted_count": booked, "accepted_cost": booked})
        self.assertEqual(limiter.state("hot")["used"], booked)   # totals never under/over-count
        self.assertEqual(len(consumed) + len(rolled), 100)
        self.assertEqual(set(consumed) & set(rolled), set())
        self.assertEqual([event["source"] for event in events].count("check"), 100)
        consume_events = [event for event in events if event["source"] == "reservation_consume"]
        self.assertEqual(len(consume_events), len(consumed))
        self.assertEqual({event["reservation_id"] for event in consume_events}, set(consumed))
        self.assertTrue(all(event["reservation_id"] is None
                            for event in events if event["source"] == "check"))
        self.assertTrue(all(counts[-1] <= booked for counts in reader_counts))

    def test_concurrent_expiry_settlement_never_lands_in_the_ledger(self) -> None:
        limiter = Limiter(self.clock)
        limiter.configure("hot", {"capacity": 10, "refill_per_second": 0.0001})
        ids = [limiter.reserve("hot", 1, ttl_seconds=10)["reservation_id"] for _ in range(10)]
        self.clock.t += 10
        errors: list[BaseException] = []

        def finish(do_consume: bool, rid: str) -> None:
            try:
                if do_consume:
                    limiter.consume(rid)
                else:
                    limiter.rollback(rid)
                errors.append(AssertionError("expired reservation unexpectedly succeeded"))
            except LimitNotFound:
                pass
            except BaseException as error:  # noqa: BLE001
                errors.append(error)

        def read_ledger() -> None:
            try:
                for _ in range(50):
                    view = limiter.ledger("hot", 1000)
                    if view["totals"] != {"accepted_count": 0, "accepted_cost": 0}:
                        raise AssertionError("expiry posted a ledger event")
            except BaseException as error:  # noqa: BLE001
                errors.append(error)

        threads = []
        for index, rid in enumerate(ids):
            threads.append(threading.Thread(target=finish, args=(True, rid)))
            threads.append(threading.Thread(target=finish, args=(False, rid)))
        threads += [threading.Thread(target=read_ledger) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        self.assertEqual(limiter.ledger("hot")["events"], [])
        self.assertEqual((limiter.state("hot")["remaining"], limiter.state("hot")["used"]), (10, 0))


class LedgerHttpTests(unittest.TestCase):
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

    def test_ledger_shape_for_a_check(self) -> None:
        clock = type(self).clock
        clock.t = 9000.0                                      # a fresh, dominant watermark
        self.request("PUT", "/v1/limits/l-1", {"capacity": 5, "refill_per_second": 1})
        self.request("POST", "/v1/check", {"key": "l-1", "cost": 2})
        status, body, _ = self.request("GET", "/v1/ledgers/l-1")
        self.assertEqual(status, 200)
        self.assertEqual(body, {"key": "l-1",
                                "totals": {"accepted_count": 1, "accepted_cost": 2},
                                "events": [{"seq": 1, "source": "check", "reservation_id": None,
                                            "cost": 2, "remaining": 3, "capacity": 5,
                                            "effective_at": 9000.0}]})

    def test_consume_event_and_totals_equal_limits_used(self) -> None:
        self.request("PUT", "/v1/limits/l-2", {"capacity": 5, "refill_per_second": 1})
        _, body, _ = self.request("POST", "/v1/reservations", {"key": "l-2", "cost": 3})
        rid = body["reservation_id"]
        self.assertEqual(self.request("POST", f"/v1/reservations/{rid}/consume", {})[0], 200)
        status, ledger, _ = self.request("GET", "/v1/ledgers/l-2")
        self.assertEqual(status, 200)
        event, = ledger["events"]
        self.assertEqual((event["source"], event["reservation_id"], event["cost"],
                          event["remaining"], event["capacity"]),
                         ("reservation_consume", rid, 3, 2, 5))
        _, state, _ = self.request("GET", "/v1/limits/l-2")
        self.assertEqual(ledger["totals"]["accepted_cost"], state["used"])
        self.assertEqual(ledger["totals"]["accepted_count"], 1)

    def test_unknown_key_ledger_is_404(self) -> None:
        status, body, _ = self.request("GET", "/v1/ledgers/no-such-ledger")
        self.assertEqual((status, body["error"]["code"]), (404, "not_found"))

    def test_events_query_window_is_honoured(self) -> None:
        self.request("PUT", "/v1/limits/l-3", {"capacity": 1000, "refill_per_second": 0.0001})
        for _ in range(3):
            self.request("POST", "/v1/check", {"key": "l-3"})
        for path, expected in [("/v1/ledgers/l-3", [1, 2, 3]),
                               ("/v1/ledgers/l-3?", [1, 2, 3]),
                               ("/v1/ledgers/l-3?events=1000", [1, 2, 3]),
                               ("/v1/ledgers/l-3?events=2", [2, 3]),
                               ("/v1/ledgers/l-3?events=1", [3])]:
            status, body, _ = self.request("GET", path)
            self.assertEqual(status, 200, path)
            self.assertEqual([event["seq"] for event in body["events"]], expected, path)
            self.assertEqual(body["totals"], {"accepted_count": 3, "accepted_cost": 3})

    def test_invalid_events_query_is_400(self) -> None:
        self.request("PUT", "/v1/limits/l-4", {"capacity": 1, "refill_per_second": 1})
        bad_paths = [
            "/v1/ledgers/l-4?events=0", "/v1/ledgers/l-4?events=1001",
            "/v1/ledgers/l-4?events=-1", "/v1/ledgers/l-4?events=1.5",
            "/v1/ledgers/l-4?events=abc", "/v1/ledgers/l-4?events=true",
            "/v1/ledgers/l-4?events=1x", "/v1/ledgers/l-4?events=",
            "/v1/ledgers/l-4?events=1%20",
            "/v1/ledgers/l-4?unknown=1", "/v1/ledgers/l-4?events=1&unknown=2",
            "/v1/ledgers/l-4?events=1&events=2", "/v1/ledgers/l-4?&",
        ]
        for path in bad_paths:
            status, body, _ = self.request("GET", path)
            self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"), path)

    def test_invalid_query_beats_unknown_key(self) -> None:
        status, body, _ = self.request("GET", "/v1/ledgers/missing-key?events=0")
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        status, body, _ = self.request("GET", "/v1/ledgers/missing-key?bogus=1")
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))

    def test_bad_routes_and_methods_are_404(self) -> None:
        # Path/method mismatches stay 404 even with a query string that would be valid for ledgers.
        self.assertEqual(self.request("GET", "/v1/ledgers")[0], 404)
        self.assertEqual(self.request("GET", "/v1/ledgers/a/b")[0], 404)
        self.assertEqual(self.request("GET", "/v1/nope?events=1")[0], 404)
        self.assertEqual(self.request("POST", "/v1/ledgers/l-4", {})[0], 404)
        self.assertEqual(self.request("PUT", "/v1/ledgers/l-4", {})[0], 404)
        self.assertEqual(self.request("DELETE", "/v1/ledgers/l-4")[0], 404)
        # Existing routes keep their old, query-agnostic behaviour.
        self.assertEqual(self.request("GET", "/health?anything=1")[0], 200)

    def test_ledger_read_is_still_empty_after_expiry_settlement(self) -> None:
        self.request("PUT", "/v1/limits/l-5", {"capacity": 5, "refill_per_second": 0.0001})
        _, body, _ = self.request("POST", "/v1/reservations",
                                  {"key": "l-5", "cost": 3, "ttl_seconds": 10})
        rid = body["reservation_id"]
        type(self).clock.t += 10
        status, ledger, _ = self.request("GET", "/v1/ledgers/l-5")
        self.assertEqual(status, 200)
        self.assertEqual(ledger["events"], [])                      # holds never post events
        self.assertEqual(self.request("POST", f"/v1/reservations/{rid}/consume", {})[0], 404)
        _, state, _ = self.request("GET", "/v1/limits/l-5")
        self.assertEqual((state["remaining"], state["used"]), (5, 0))
        _, ledger, _ = self.request("GET", "/v1/ledgers/l-5")
        self.assertEqual((ledger["totals"], ledger["events"]),
                         ({"accepted_count": 0, "accepted_cost": 0}, []))


class CheckKeyValidationUnitTests(unittest.TestCase):
    """Limiter.check applies the same key rule as configure/reserve, before any state work."""

    def setUp(self) -> None:
        self.clock = Clock()
        self.limiter = Limiter(self.clock)
        self.limiter.configure("tenant-a", {"capacity": 5, "refill_per_second": 1.0})

    def test_invalid_keys_are_invalid_request(self) -> None:
        for bad_key in [1, 0, 1.5, True, False, None, "", "x" * 201, ["tenant-a"], {"k": 1}]:
            with self.assertRaises(InvalidRequest, msg=repr(bad_key)):
                self.limiter.check(bad_key, 1)

    def test_boundary_length_keys_are_accepted(self) -> None:
        long_key = "k" * 200
        self.limiter.configure(long_key, {"capacity": 1, "refill_per_second": 1.0})
        self.assertTrue(self.limiter.check(long_key, 1)["allowed"])
        self.limiter.configure("q", {"capacity": 1, "refill_per_second": 1.0})
        self.assertTrue(self.limiter.check("q", 1)["allowed"])

    def test_key_and_cost_both_invalid_is_invalid_request(self) -> None:
        for bad_key, bad_cost in [(None, 0), ("", "1"), (1, True), (["k"], 1.5), ("x" * 201, None)]:
            with self.assertRaises(InvalidRequest):
                self.limiter.check(bad_key, bad_cost)

    def test_valid_but_unconfigured_key_is_not_found(self) -> None:
        with self.assertRaises(LimitNotFound):
            self.limiter.check("ghost", 1)

    def test_rejected_checks_leave_quota_reservations_and_ledger_untouched(self) -> None:
        limiter = Limiter(self.clock)
        limiter.configure("frozen", {"capacity": 5, "refill_per_second": 0.0001})
        reservation = limiter.reserve("frozen", 2, ttl_seconds=60)   # tokens 3
        self.assertTrue(limiter.check("frozen", 1)["allowed"])       # tokens 2, used 1
        before_ledger = limiter.ledger("frozen", 1000)
        for bad_key in [1, True, None, "", "x" * 201, ["frozen"], {"key": 1}]:
            with self.assertRaises(InvalidRequest):
                limiter.check(bad_key, 1)
        with self.assertRaises(InvalidRequest):
            limiter.check("frozen", 0)                               # valid key, invalid cost
        with self.assertRaises(InvalidRequest):
            limiter.check(None, 0)                                   # both invalid
        with self.assertRaises(LimitNotFound):
            limiter.check("ghost", 1)                                # valid key, unknown
        state = limiter.state("frozen")
        self.assertEqual((state["remaining"], state["used"]), (2, 1))
        self.assertEqual(limiter.ledger("frozen", 1000), before_ledger)
        self.assertIn(reservation["reservation_id"], limiter._reservations)
        # The hold is still live and rolls back normally.
        self.assertTrue(limiter.rollback(reservation["reservation_id"])["rolled_back"])

    def test_rejected_checks_ignore_clock_regression_and_stall(self) -> None:
        clock = Clock()
        clock.t = 100.0
        limiter = Limiter(clock)
        limiter.configure("k", {"capacity": 10, "refill_per_second": 1.0})
        self.assertTrue(limiter.check("k", 10)["allowed"])           # empty at t=100
        clock.t = 90.0                                               # regressed
        for bad_key in ["", None, 1, "x" * 201]:
            with self.assertRaises(InvalidRequest):
                limiter.check(bad_key, 1)
        self.assertEqual(limiter.state("k")["remaining"], 0)         # effectively still t=100
        clock.t = 200.0
        with self.assertRaises(InvalidRequest):
            limiter.check(None, 1)                                   # must not sample t=200
        clock.t = 105.0                                              # below the rejected reading
        # A rejected check never ticks: only 100→105 refills, the t=200 reading is gone.
        self.assertEqual(limiter.state("k")["remaining"], 5)


class CheckHttpValidationTests(unittest.TestCase):
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

    def test_invalid_keys_are_400_and_change_nothing(self) -> None:
        self.request("PUT", "/v1/limits/ck-1", {"capacity": 3, "refill_per_second": 1})
        status, body, _ = self.request("POST", "/v1/check", {"key": "ck-1", "cost": 1})
        self.assertEqual(status, 200)
        self.assertEqual(body, {"allowed": True, "remaining": 2, "capacity": 3})
        for bad_key in [1, True, None, "", "x" * 201, ["ck-1"], {"k": 1}, 1.5]:
            status, body, _ = self.request("POST", "/v1/check", {"key": bad_key, "cost": 1})
            self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"), bad_key)
        _, state, _ = self.request("GET", "/v1/limits/ck-1")
        self.assertEqual((state["remaining"], state["used"]), (2, 1))
        _, ledger, _ = self.request("GET", "/v1/ledgers/ck-1")
        self.assertEqual(ledger["totals"], {"accepted_count": 1, "accepted_cost": 1})

    def test_key_and_cost_both_invalid_is_400(self) -> None:
        for body in [{"key": None, "cost": 0}, {"key": "", "cost": "x"}, {"key": 1, "cost": True}]:
            status, parsed, _ = self.request("POST", "/v1/check", body)
            self.assertEqual((status, parsed["error"]["code"]), (400, "invalid_request"), body)

    def test_unknown_key_is_404_and_invalid_cost_is_400(self) -> None:
        status, body, _ = self.request("POST", "/v1/check", {"key": "ghost", "cost": 1})
        self.assertEqual((status, body["error"]["code"]), (404, "not_found"))
        self.request("PUT", "/v1/limits/ck-2", {"capacity": 1, "refill_per_second": 1})
        status, body, _ = self.request("POST", "/v1/check", {"key": "ck-2", "cost": 0})
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        _, state, _ = self.request("GET", "/v1/limits/ck-2")
        self.assertEqual((state["remaining"], state["used"]), (1, 0))


class RouteClassificationHttpTests(unittest.TestCase):
    """Exact path segments and supported methods only; mismatches are 404 before any body work."""

    @classmethod
    def setUpClass(cls) -> None:
        import http.client

        cls.http_client = http.client
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

    def raw_request(self, method: str, path: str, payload: bytes | None,
                    content_length: str | object = "auto") -> tuple[int, dict]:
        connection = self.http_client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        connection.putrequest(method, path)
        if content_length != "omit":
            connection.putheader("Content-Length",
                                 str(len(payload)) if content_length == "auto" else content_length)
        connection.endheaders(payload if payload is not None else b"")
        response = connection.getresponse()
        body = json.loads(response.read() or b"{}")
        connection.close()
        return response.status, body

    def test_extra_slashes_never_reach_check_or_configure(self) -> None:
        self.request("PUT", "/v1/limits/m-3", {"capacity": 2, "refill_per_second": 1})
        for path in ["//v1/check", "/v1//check", "/v1/check/", "/v1/check/extra"]:
            status, parsed = self.raw_request("POST", path, b'{"key": "m-3", "cost": 1}')
            self.assertEqual((status, parsed["error"]["code"]), (404, "not_found"), path)
        _, state, _ = self.request("GET", "/v1/limits/m-3")
        self.assertEqual((state["remaining"], state["used"]), (2, 0))     # nothing deducted
        for path in ["//v1/limits/m-4", "/v1//limits/m-4", "/v1/limits/m-4/", "/v1/limits/m-4/extra"]:
            status, _ = self.raw_request("PUT", path, b'{"capacity": 5, "refill_per_second": 1}')
            self.assertEqual(status, 404, path)
        self.assertEqual(self.request("GET", "/v1/limits/m-4")[0], 404)     # never configured
        for path in ["//health", "/health/", "/"]:
            status, _ = self.raw_request("GET", path, None, content_length="omit")
            self.assertEqual(status, 404, path)

    def test_extra_slashes_never_reach_consume(self) -> None:
        self.request("PUT", "/v1/limits/m-2", {"capacity": 2, "refill_per_second": 1})
        _, body, _ = self.request("POST", "/v1/reservations", {"key": "m-2", "cost": 1})
        rid = body["reservation_id"]
        for path in [f"/v1/reservations/{rid}/consume/", f"//v1/reservations/{rid}/consume",
                     f"/v1//reservations/{rid}/consume", f"/v1/reservations//{rid}/consume",
                     f"/v1/reservations/{rid}//consume", f"/v1/reservations/{rid}/consume/extra"]:
            status, parsed = self.raw_request("POST", path, b"{}")
            self.assertEqual((status, parsed["error"]["code"]), (404, "not_found"), path)
        # The hold was neither consumed nor refunded by the malformed attempts.
        status, body, _ = self.request("POST", f"/v1/reservations/{rid}/consume", {})
        self.assertEqual((status, body["consumed"], body["used"]), (200, True, 1))

    def test_unsupported_methods_on_known_paths_are_404(self) -> None:
        cases = [("GET", "/v1/check"), ("PUT", "/v1/check"), ("DELETE", "/v1/check"),
                 ("PATCH", "/v1/check"), ("OPTIONS", "/v1/check"), ("HEAD", "/v1/check"),
                 ("POST", "/health"), ("PUT", "/health"), ("DELETE", "/health"), ("PATCH", "/health"),
                 ("POST", "/v1/limits/m-5"), ("DELETE", "/v1/limits/m-5"), ("PATCH", "/v1/limits/m-5"),
                 ("GET", "/v1/reservations"), ("PUT", "/v1/reservations"), ("PATCH", "/v1/reservations"),
                 ("POST", "/v1/ledgers/m-5"), ("PATCH", "/v1/ledgers/m-5"),
                 ("GET", "/v1/reservations/m-5/consume"), ("DELETE", "/v1/reservations/m-5/consume")]
        for method, path in cases:
            payload = b'{"capacity": 1, "refill_per_second": 1}' if method in ("POST", "PUT", "PATCH") else None
            status, parsed = self.raw_request(method, path, payload,
                                              content_length="auto" if payload is not None else "omit")
            if method == "HEAD":                                   # a HEAD response carries no body
                self.assertEqual(status, 404, (method, path))
                continue
            self.assertEqual((status, parsed["error"]["code"]), (404, "not_found"), (method, path))
        self.assertEqual(self.request("GET", "/v1/limits/m-5")[0], 404)     # never configured

    def test_method_or_path_mismatch_beats_body_validation(self) -> None:
        # Bad Content-Length and garbage bodies are never even inspected off-route.
        status, parsed = self.raw_request("PATCH", "/v1/check", b"\xff\xfe", content_length="nine")
        self.assertEqual((status, parsed["error"]["code"]), (404, "not_found"))
        status, parsed = self.raw_request("POST", "/v1/check/", b"not json", content_length="nine")
        self.assertEqual((status, parsed["error"]["code"]), (404, "not_found"))
        status, parsed = self.raw_request("PUT", "/v1/limits", b"{")
        self.assertEqual((status, parsed["error"]["code"]), (404, "not_found"))
        status, parsed = self.raw_request("GET", "/v1/check", None, content_length="nine")
        self.assertEqual((status, parsed["error"]["code"]), (404, "not_found"))
        # The service itself is unaffected.
        self.assertEqual(self.request("GET", "/health")[0], 200)


class ReconfigureRefillUnitTests(unittest.TestCase):
    """PUT hot reconfiguration settles under the OLD configuration up to the reconfigure's
    effective moment before the new capacity/rate governs anything."""

    def setUp(self) -> None:
        self.clock = Clock()
        self.clock.t = 100.0
        self.limiter = Limiter(self.clock)

    def test_reconfigure_refills_at_the_old_rate_before_swapping(self) -> None:
        self.limiter.configure("k", {"capacity": 10, "refill_per_second": 2.0})
        self.assertTrue(self.limiter.check("k", 10)["allowed"])      # empty at t=100
        self.clock.t = 103.0
        self.limiter.configure("k", {"capacity": 10, "refill_per_second": 5.0})
        # 3 seconds at the OLD rate 2: the new rate 5 never applies retroactively.
        self.assertEqual(self.limiter.state("k")["remaining"], 6)
        self.clock.t = 104.0
        # From the reconfigure on, the NEW rate 5 governs, capped at the new capacity.
        self.assertEqual(self.limiter.state("k")["remaining"], 10)

    def test_reconfigure_caps_the_old_rate_refill_at_the_new_capacity(self) -> None:
        self.limiter.configure("k", {"capacity": 10, "refill_per_second": 2.0})
        self.limiter.check("k", 10)                                  # empty at t=100
        self.clock.t = 103.0
        self.limiter.configure("k", {"capacity": 4, "refill_per_second": 5.0})
        # 6 tokens earned at the old rate, then capped at the new capacity 4.
        self.assertEqual(self.limiter.state("k")["remaining"], 4)
        self.clock.t = 104.0
        self.assertEqual(self.limiter.state("k")["remaining"], 4)    # still capped at 4

    def test_growing_capacity_never_tops_up_the_bucket(self) -> None:
        self.limiter.configure("k", {"capacity": 10, "refill_per_second": 2.0})
        self.limiter.check("k", 10)
        self.clock.t = 103.0
        self.limiter.configure("k", {"capacity": 100, "refill_per_second": 5.0})
        self.assertEqual(self.limiter.state("k")["remaining"], 6)    # not 100, not 16

    def test_rate_drop_still_earns_the_old_rate_until_the_swap(self) -> None:
        self.limiter.configure("k", {"capacity": 10, "refill_per_second": 5.0})
        self.limiter.check("k", 10)                                  # empty at t=100
        self.clock.t = 103.0
        self.limiter.configure("k", {"capacity": 10, "refill_per_second": 1.0})
        # 3 seconds at the OLD rate 5 = 15, capped at 10; the new rate 1 applies only after.
        self.assertEqual(self.limiter.state("k")["remaining"], 10)
        self.clock.t = 104.0
        self.assertEqual(self.limiter.state("k")["remaining"], 10)

    def test_due_reservation_settles_old_refill_then_refund_then_new_cap(self) -> None:
        self.limiter.configure("k", {"capacity": 10, "refill_per_second": 1.0})
        reservation = self.limiter.reserve("k", 8, ttl_seconds=5)    # tokens 2, due 105
        self.clock.t = 106.0
        self.limiter.configure("k", {"capacity": 4, "refill_per_second": 1.0})
        # Old-config refill 2 + 6*1 = 8, refund 8 -> 16, capped at the NEW capacity 4.
        state = self.limiter.state("k")
        self.assertEqual((state["remaining"], state["used"]), (4, 0))
        with self.assertRaises(LimitNotFound):
            self.limiter.rollback(reservation["reservation_id"])     # refunded exactly once

    def test_due_refund_is_capped_at_the_new_not_the_old_capacity(self) -> None:
        self.limiter.configure("k", {"capacity": 10, "refill_per_second": 1.0})
        self.limiter.reserve("k", 8, ttl_seconds=5)                  # tokens 2, due 105
        self.clock.t = 106.0
        self.limiter.configure("k", {"capacity": 20, "refill_per_second": 1.0})
        # Refill 8 under the old config, refund 8 -> 16: the new capacity 20 does not clip
        # what the old capacity 10 would have.
        self.assertEqual(self.limiter.state("k")["remaining"], 16)

    def test_live_reservation_survives_and_used_and_ledger_are_untouched(self) -> None:
        self.limiter.configure("k", {"capacity": 10, "refill_per_second": 1.0})
        self.limiter.check("k", 2)                                   # tokens 8, used 2
        reservation = self.limiter.reserve("k", 4, ttl_seconds=100)  # tokens 4
        self.clock.t = 103.0
        self.limiter.configure("k", {"capacity": 6, "refill_per_second": 2.0})
        # Old-rate refill 4 + 3*1 = 7, capped at the new capacity 6; the hold is NOT due.
        state = self.limiter.state("k")
        self.assertEqual((state["remaining"], state["used"]), (6, 2))
        self.assertEqual(self.limiter.ledger("k")["totals"],
                         {"accepted_count": 1, "accepted_cost": 2})
        self.assertIn(reservation["reservation_id"], self.limiter._reservations)
        result = self.limiter.rollback(reservation["reservation_id"])  # still live, refunds once
        self.assertTrue(result["rolled_back"])
        self.assertEqual(result["remaining"], 6)                     # 6 + 4 capped at 6

    def test_due_hierarchy_reservation_settles_as_one_unit_at_reconfigure(self) -> None:
        self.limiter.configure("org", {"capacity": 10, "refill_per_second": 1.0})
        self.limiter.configure("leaf", {"capacity": 10, "refill_per_second": 1.0})
        rid = self.limiter.hierarchy_reserve(["org", "leaf"], 6, ttl_seconds=5)["reservation_id"]
        self.clock.t = 106.0                                         # both layers hold 4, due 105
        self.limiter.configure("leaf", {"capacity": 3, "refill_per_second": 1.0})
        # The cross-layer hold lapses as one unit in this same critical section: org is refilled
        # and refunded under its own (unchanged) capacity, leaf under the old config then capped
        # at its NEW capacity. No layer is left half-refunded.
        self.assertEqual(self.limiter.state("org")["remaining"], 10)
        self.assertEqual(self.limiter.state("leaf")["remaining"], 3)
        self.assertEqual(self.limiter.state("org")["used"], 0)
        self.assertEqual(self.limiter.state("leaf")["used"], 0)
        self.assertEqual(self.limiter._hierarchy_reservations, {})
        with self.assertRaises(LimitNotFound):
            self.limiter.hierarchy_rollback(rid)

    def test_stalled_clock_reconfigure_refills_nothing(self) -> None:
        self.limiter.configure("k", {"capacity": 10, "refill_per_second": 2.0})
        self.limiter.check("k", 10)                                  # empty at t=100
        self.limiter.configure("k", {"capacity": 10, "refill_per_second": 5.0})  # same instant
        self.assertEqual(self.limiter.state("k")["remaining"], 0)

    def test_reconfigure_under_regression_anchors_and_never_recounts(self) -> None:
        self.limiter.configure("k", {"capacity": 10, "refill_per_second": 2.0})
        self.limiter.check("k", 10)                                  # empty at t=100
        self.clock.t = 90.0                                          # regressed reading
        self.limiter.configure("k", {"capacity": 10, "refill_per_second": 5.0})
        self.assertEqual(self.limiter.state("k")["remaining"], 0)    # treated as still t=100
        self.clock.t = 101.0
        # Only 100->101 counts, at the new rate; the regressed interval is never recounted.
        self.assertEqual(self.limiter.state("k")["remaining"], 5)
        self.clock.t = 103.0
        self.assertEqual(self.limiter.state("k")["remaining"], 10)   # 5 + 2*5, capped at 10

    def test_invalid_reconfigure_changes_nothing_and_never_ticks(self) -> None:
        self.limiter.configure("k", {"capacity": 10, "refill_per_second": 1.0})
        self.limiter.check("k", 6)                                   # tokens 4, used 6
        reservation = self.limiter.reserve("k", 2, ttl_seconds=5)    # tokens 2, due 105
        ledger_before = self.limiter.ledger("k", 1000)
        self.clock.t = 110.0                                         # past the hold's expiry
        bad_payloads = [
            {"capacity": 10, "refill_per_second": 1, "extra": 1},    # unknown field
            {"capacity": True, "refill_per_second": 1},              # boolean capacity
            {"capacity": 4.5, "refill_per_second": 1},               # float capacity
            {"capacity": 0, "refill_per_second": 1},                 # capacity too small
            {"capacity": 1_000_001, "refill_per_second": 1},         # capacity too large
            {"capacity": 10, "refill_per_second": 0},                # non-positive rate
            {"capacity": 10, "refill_per_second": -2},
            {"capacity": 10, "refill_per_second": 1_000_001},        # rate too large
            {"capacity": 10, "refill_per_second": True},             # boolean rate
            {"capacity": 10},                                        # missing rate
            "nope",                                                  # not an object
        ]
        for bad in bad_payloads:
            with self.assertRaises(InvalidRequest, msg=repr(bad)):
                self.limiter.configure("k", bad)
        with self.assertRaises(InvalidRequest):                      # invalid key also rejected
            self.limiter.configure("", {"capacity": 1, "refill_per_second": 1})
        # The failed reconfigures never sampled the clock: the watermark is still 100, so at
        # t=104 the hold is not yet due and only 4 seconds of refill have accrued. Had any
        # rejection ticked, the watermark would pin 110, refund the hold early and show 10.
        self.clock.t = 104.0
        state = self.limiter.state("k")
        self.assertEqual((state["remaining"], state["used"]), (6, 6))
        self.assertEqual(state["limit"], {"capacity": 10, "refill_per_second": 1.0})
        self.assertEqual(self.limiter.ledger("k", 1000), ledger_before)
        self.assertIn(reservation["reservation_id"], self.limiter._reservations)
        self.clock.t = 105.0                                         # the hold lapses on schedule
        self.assertEqual(self.limiter.state("k")["remaining"], 9)    # 7 + 2 refunded

    def test_concurrent_reconfigure_and_spending_serialize_without_oversell(self) -> None:
        limiter = Limiter(self.clock)                                # clock frozen at t=100
        limiter.configure("hot", {"capacity": 100, "refill_per_second": 0.0001})
        outcomes: list[bool] = []
        errors: list[BaseException] = []
        lock = threading.Lock()

        def spend() -> None:
            try:
                limiter.check("hot", 1)
                ok = True
            except OverQuota:
                ok = False
            except BaseException as error:  # noqa: BLE001
                with lock:
                    errors.append(error)
                return
            with lock:
                outcomes.append(ok)

        def reconfigure(capacity: int) -> None:
            try:
                limiter.configure("hot", {"capacity": capacity, "refill_per_second": 0.0001})
            except BaseException as error:  # noqa: BLE001
                with lock:
                    errors.append(error)

        threads = []
        for index in range(200):
            threads.append(threading.Thread(target=spend))
            if index % 4 == 0:                                       # 50 reconfigures interleaved
                threads.append(threading.Thread(target=reconfigure, args=(100 + (index % 3) * 50,)))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        # No reconfiguration conjured or destroyed tokens: exactly the 100 held tokens were spent.
        self.assertEqual(sum(outcomes), 100)
        state = limiter.state("hot")
        self.assertEqual((state["remaining"], state["used"]), (0, 100))


class ReconfigureRefillHttpTests(unittest.TestCase):
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

    def test_put_refills_at_the_old_rate_then_applies_the_new_one(self) -> None:
        clock = type(self).clock
        clock.t = 300.0                                              # a fresh, dominant watermark
        self.request("PUT", "/v1/limits/rc-1", {"capacity": 10, "refill_per_second": 2})
        status, body, _ = self.request("POST", "/v1/check", {"key": "rc-1", "cost": 10})
        self.assertEqual((status, body["remaining"]), (200, 0))      # empty at t=300
        clock.t = 303.0
        status, body, _ = self.request("PUT", "/v1/limits/rc-1", {"capacity": 10, "refill_per_second": 5})
        self.assertEqual((status, body["limit"]), (200, {"capacity": 10, "refill_per_second": 5.0}))
        _, state, _ = self.request("GET", "/v1/limits/rc-1")
        self.assertEqual(state["remaining"], 6)                      # 3s at the OLD rate 2
        clock.t = 304.0
        _, state, _ = self.request("GET", "/v1/limits/rc-1")
        self.assertEqual(state["remaining"], 10)                     # 6 + 1s at the NEW rate 5

    def test_put_caps_the_old_rate_refill_at_a_smaller_new_capacity(self) -> None:
        clock = type(self).clock
        clock.t = 200.0                                              # a fresh, dominant watermark
        self.request("PUT", "/v1/limits/rc-2", {"capacity": 10, "refill_per_second": 2})
        self.request("POST", "/v1/check", {"key": "rc-2", "cost": 10})
        clock.t = 203.0
        status, _, _ = self.request("PUT", "/v1/limits/rc-2", {"capacity": 4, "refill_per_second": 5})
        self.assertEqual(status, 200)
        _, state, _ = self.request("GET", "/v1/limits/rc-2")
        self.assertEqual(state["remaining"], 4)                      # 6 earned, capped at 4
        clock.t = 204.0
        _, state, _ = self.request("GET", "/v1/limits/rc-2")
        self.assertEqual(state["remaining"], 4)                      # still capped at 4

    def test_invalid_put_is_400_and_leaves_state_and_clock_untouched(self) -> None:
        clock = type(self).clock
        clock.t = 100.0
        self.request("PUT", "/v1/limits/rc-3", {"capacity": 5, "refill_per_second": 1})
        self.request("POST", "/v1/check", {"key": "rc-3", "cost": 5})  # empty at t=100
        clock.t = 110.0
        for body in [{"capacity": 5, "refill_per_second": 1, "extra": 1},
                     {"capacity": True, "refill_per_second": 1},
                     {"capacity": 2.5, "refill_per_second": 1},
                     {"capacity": 0, "refill_per_second": 1},
                     {"capacity": 5, "refill_per_second": 0},
                     {"capacity": 5, "refill_per_second": -1},
                     {"capacity": 5, "refill_per_second": 1_000_001}]:
            status, parsed, _ = self.request("PUT", "/v1/limits/rc-3", body)
            self.assertEqual((status, parsed["error"]["code"]), (400, "invalid_request"), body)
        clock.t = 103.0
        _, state, _ = self.request("GET", "/v1/limits/rc-3")
        # The rejections never ticked: only 100->103 refills, and the old config still governs.
        self.assertEqual((state["remaining"], state["used"]), (3, 5))
        self.assertEqual(state["limit"], {"capacity": 5, "refill_per_second": 1.0})


if __name__ == "__main__":
    unittest.main()
