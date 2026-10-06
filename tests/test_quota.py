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


class DriftClock:
    """Clock whose readings jump forward by `step` on every read after the clock is (re)set.

    Any operation that takes more than one reading per call observes a different moment for each
    read; the limiter must act on exactly one effective moment per critical section.
    """

    def __init__(self, start: float = 100.0, step: float = 1000.0) -> None:
        self.step = step
        self.set(start)

    def set(self, t: float) -> None:
        self.t = t
        self._reads = 0

    def __call__(self) -> float:
        self._reads += 1
        return self.t + (self._reads - 1) * self.step


class JitterClock:
    """Clock that jitters backwards on every other read while its base only advances."""

    def __init__(self, start: float = 0.0) -> None:
        self.t = start
        self._reads = 0

    def __call__(self) -> float:
        self._reads += 1
        return self.t - 5.0 if self._reads % 2 == 0 else self.t


class ClockRegressionUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.limiter = Limiter(self.clock)

    def test_regressed_reading_refills_nothing_and_recovery_counts_forward_time_once(self) -> None:
        self.clock.t = 100.0
        self.limiter.configure("tenant-a", {"capacity": 10, "refill_per_second": 1.0})
        self.assertEqual(self.limiter.check("tenant-a", 10)["remaining"], 0)

        self.clock.t = 90.0                                    # clock jumps backwards
        self.assertEqual(self.limiter.state("tenant-a")["remaining"], 0)   # no phantom refill
        self.assertEqual(self.limiter.state("tenant-a")["remaining"], 0)   # stable while regressed

        self.clock.t = 101.0                                   # recovers just past the waterline
        self.assertEqual(self.limiter.state("tenant-a")["remaining"], 1)   # only 1 real second
        self.clock.t = 95.0                                    # regresses again below the waterline
        self.assertEqual(self.limiter.state("tenant-a")["remaining"], 1)   # rollback interval not re-counted
        self.clock.t = 101.0                                   # same effective moment again
        self.assertEqual(self.limiter.state("tenant-a")["remaining"], 1)   # no double refill
        self.clock.t = 102.0
        self.assertEqual(self.limiter.state("tenant-a")["remaining"], 2)

    def test_stalled_clock_refills_zero(self) -> None:
        self.limiter.configure("tenant-a", {"capacity": 4, "refill_per_second": 1.0})
        self.limiter.check("tenant-a", 4)
        for _ in range(5):
            self.assertEqual(self.limiter.state("tenant-a")["remaining"], 0)
        with self.assertRaises(OverQuota):
            self.limiter.check("tenant-a", 1)

    def test_regression_neither_expires_nor_extends_a_reservation(self) -> None:
        self.clock.t = 100.0
        self.limiter.configure("tenant-a", {"capacity": 5, "refill_per_second": 0.0001})
        reservation = self.limiter.reserve("tenant-a", 3, ttl_seconds=10)   # due at effective 110

        self.clock.t = 95.0                                    # regressed: hold must survive
        state = self.limiter.state("tenant-a")
        self.assertEqual((state["remaining"], state["used"]), (2, 0))

        self.clock.t = 109.0                                   # forward but still short of expiry
        self.assertEqual(self.limiter.state("tenant-a")["remaining"], 2)

        self.clock.t = 95.0                                    # regressed again: still held, not due
        self.assertEqual(self.limiter.state("tenant-a")["remaining"], 2)

        self.clock.t = 110.0                                   # effective expiry moment
        state = self.limiter.state("tenant-a")
        self.assertEqual((state["remaining"], state["used"]), (5, 0))      # refunded exactly once
        self.assertEqual(self.limiter.state("tenant-a")["remaining"], 5)   # same moment: no second refund
        with self.assertRaises(LimitNotFound):
            self.limiter.rollback(reservation["reservation_id"])
        with self.assertRaises(LimitNotFound):
            self.limiter.consume(reservation["reservation_id"])
        state = self.limiter.state("tenant-a")                 # failed settle attempts change nothing
        self.assertEqual((state["remaining"], state["used"]), (5, 0))

    def test_single_effective_moment_per_call_despite_intra_call_drift(self) -> None:
        clock = DriftClock(start=100.0, step=1000.0)           # every extra read in a call jumps +1000s
        limiter = Limiter(clock)
        clock.set(100.0)
        limiter.configure("tenant-a", {"capacity": 5, "refill_per_second": 0.0001})
        clock.set(100.0)
        reservation = limiter.reserve("tenant-a", 2, ttl_seconds=10)   # created at effective 100

        clock.set(109.0)
        self.assertEqual(limiter.state("tenant-a")["remaining"], 3)    # held: not yet due
        clock.set(110.0)
        self.assertEqual(limiter.state("tenant-a")["remaining"], 5)    # due at created_at + ttl
        with self.assertRaises(LimitNotFound):
            limiter.rollback(reservation["reservation_id"])

    def test_intra_call_drift_cannot_skip_refill_forward(self) -> None:
        clock = DriftClock(start=100.0, step=1000.0)
        limiter = Limiter(clock)
        clock.set(100.0)
        limiter.configure("tenant-a", {"capacity": 10, "refill_per_second": 1.0})
        clock.set(100.0)
        self.assertTrue(limiter.check("tenant-a", 10)["allowed"])      # empty at effective 100
        clock.set(101.0)
        # A second in-call read would jump to 1101; the bucket watermark must stay at effective 101,
        # so the next real second still refills exactly one token.
        self.assertEqual(limiter.state("tenant-a")["remaining"], 1)
        clock.set(102.0)
        self.assertEqual(limiter.state("tenant-a")["remaining"], 2)

    def test_regression_preserves_reconfigure_starting_point(self) -> None:
        self.clock.t = 100.0
        self.limiter.configure("tenant-a", {"capacity": 10, "refill_per_second": 1.0})
        self.limiter.check("tenant-a", 7)                      # 3 tokens left at effective 100
        self.clock.t = 90.0                                    # regressed
        self.limiter.configure("tenant-a", {"capacity": 2, "refill_per_second": 1.0})
        self.assertEqual(self.limiter.state("tenant-a")["remaining"], 2)   # min(3, 2), no refill
        self.clock.t = 101.0
        self.assertEqual(self.limiter.state("tenant-a")["remaining"], 2)   # 1s refill capped at 2

    def test_consume_replay_is_stable_across_regression(self) -> None:
        self.clock.t = 100.0
        self.limiter.configure("tenant-a", {"capacity": 5, "refill_per_second": 1.0})
        reservation = self.limiter.reserve("tenant-a", 2)
        first = self.limiter.consume(reservation["reservation_id"])
        self.clock.t = 50.0                                    # deep regression
        self.assertEqual(self.limiter.consume(reservation["reservation_id"]), first)
        state = self.limiter.state("tenant-a")
        self.assertEqual((state["remaining"], state["used"]), (3, 2))

    def test_concurrent_jittering_clock_never_oversells_or_loses_tokens(self) -> None:
        clock = JitterClock(start=1000.0)
        limiter = Limiter(clock)
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
        self.assertEqual(sum(outcomes), 100)                   # jitter never mints extra tokens
        state = limiter.state("hot")
        live = [r for r in limiter._reservations.values() if r.key == "hot"]
        self.assertEqual(state["remaining"] + state["used"] + sum(r.cost for r in live), 100)


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

    def test_regressed_clock_over_http_refills_nothing_and_recovers_once(self) -> None:
        # unittest runs this after the holds_ test (alphabetical), whose waterline reached 210,
        # so this scenario plays out on a later segment of the shared injected clock.
        type(self).clock.t = 300.0
        self.request("PUT", "/v1/limits/skew-1", {"capacity": 10, "refill_per_second": 1})
        status, body, _ = self.request("POST", "/v1/check", {"key": "skew-1", "cost": 10})
        self.assertEqual((status, body["remaining"]), (200, 0))

        type(self).clock.t = 290.0                             # regressed
        _, state, _ = self.request("GET", "/v1/limits/skew-1")
        self.assertEqual((state["remaining"], state["used"]), (0, 10))

        type(self).clock.t = 301.0                             # one real second past the waterline
        _, state, _ = self.request("GET", "/v1/limits/skew-1")
        self.assertEqual(state["remaining"], 1)
        type(self).clock.t = 295.0                             # regressed again: no re-count
        _, state, _ = self.request("GET", "/v1/limits/skew-1")
        self.assertEqual(state["remaining"], 1)

    def test_regressed_clock_over_http_holds_reservation_until_effective_expiry(self) -> None:
        type(self).clock.t = 200.0
        self.request("PUT", "/v1/limits/skew-2", {"capacity": 4, "refill_per_second": 0.0001})
        _, body, _ = self.request("POST", "/v1/reservations",
                                  {"key": "skew-2", "cost": 3, "ttl_seconds": 10})
        rid = body["reservation_id"]

        type(self).clock.t = 195.0                             # regressed: still held
        _, state, _ = self.request("GET", "/v1/limits/skew-2")
        self.assertEqual((state["remaining"], state["used"]), (1, 0))

        type(self).clock.t = 210.0                             # effective expiry moment
        _, state, _ = self.request("GET", "/v1/limits/skew-2")
        self.assertEqual((state["remaining"], state["used"]), (4, 0))
        _, state, _ = self.request("GET", "/v1/limits/skew-2")
        self.assertEqual(state["remaining"], 4)                # refunded exactly once

        self.assertEqual(self.request("DELETE", f"/v1/reservations/{rid}")[0], 404)
        status, body, _ = self.request("POST", f"/v1/reservations/{rid}/consume", {})
        self.assertEqual((status, body["error"]["code"]), (404, "not_found"))
        _, state, _ = self.request("GET", "/v1/limits/skew-2")
        self.assertEqual((state["remaining"], state["used"]), (4, 0))


if __name__ == "__main__":
    unittest.main()
