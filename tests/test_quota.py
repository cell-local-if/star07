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


class LedgerUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.limiter = Limiter(self.clock)
        self.limiter.configure("tenant-a", {"capacity": 10, "refill_per_second": 1.0})

    def test_check_and_consume_each_append_one_event(self) -> None:
        self.assertTrue(self.limiter.check("tenant-a", 2)["allowed"])       # tokens 8
        reservation = self.limiter.reserve("tenant-a", 3, ttl_seconds=60)   # tokens 5
        self.limiter.consume(reservation["reservation_id"])
        ledger = self.limiter.ledger("tenant-a")
        self.assertEqual(ledger["key"], "tenant-a")
        self.assertEqual(ledger["totals"], {"accepted_count": 2, "accepted_cost": 5})
        self.assertEqual(ledger["totals"]["accepted_cost"], self.limiter.state("tenant-a")["used"])
        check_event, consume_event = ledger["events"]
        self.assertEqual(check_event, {"seq": 1, "source": "check", "reservation_id": None,
                                       "cost": 2, "remaining": 8, "capacity": 10,
                                       "effective_at": self.clock.t})
        self.assertEqual(consume_event, {"seq": 2, "source": "reservation_consume",
                                         "reservation_id": reservation["reservation_id"],
                                         "cost": 3, "remaining": 5, "capacity": 10,
                                         "effective_at": self.clock.t})

    def test_effective_at_is_the_producing_operations_moment(self) -> None:
        self.limiter.check("tenant-a", 1)
        self.clock.t += 5.0
        self.limiter.check("tenant-a", 1)
        events = self.limiter.ledger("tenant-a")["events"]
        self.assertEqual([event["effective_at"] for event in events], [1000.0, 1005.0])

    def test_failed_operations_append_nothing(self) -> None:
        reservation = self.limiter.reserve("tenant-a", 2, ttl_seconds=5)
        self.limiter.rollback(reservation["reservation_id"])                # refund: no event
        with self.assertRaises(OverQuota):
            self.limiter.check("tenant-a", 100)                             # over quota: no event
        with self.assertRaises(InvalidRequest):
            self.limiter.check("tenant-a", 0)                               # validation: no event
        with self.assertRaises(LimitNotFound):
            self.limiter.consume("nope")
        expired = self.limiter.reserve("tenant-a", 1, ttl_seconds=5)
        self.clock.t += 5
        with self.assertRaises(LimitNotFound):
            self.limiter.consume(expired["reservation_id"])                 # expiry: no event
        ledger = self.limiter.ledger("tenant-a")
        self.assertEqual(ledger["totals"], {"accepted_count": 0, "accepted_cost": 0})
        self.assertEqual(ledger["events"], [])

    def test_duplicate_consume_appends_nothing(self) -> None:
        reservation = self.limiter.reserve("tenant-a", 2)
        first = self.limiter.consume(reservation["reservation_id"])
        self.clock.t += 3.0
        self.assertEqual(self.limiter.consume(reservation["reservation_id"]), first)
        ledger = self.limiter.ledger("tenant-a")
        self.assertEqual(ledger["totals"], {"accepted_count": 1, "accepted_cost": 2})
        self.assertEqual([event["seq"] for event in ledger["events"]], [1])

    def test_events_window_returns_most_recent_in_ascending_seq(self) -> None:
        for _ in range(5):
            self.limiter.check("tenant-a", 1)
        ledger = self.limiter.ledger("tenant-a", events_limit=3)
        self.assertEqual([event["seq"] for event in ledger["events"]], [3, 4, 5])
        self.assertEqual(ledger["totals"], {"accepted_count": 5, "accepted_cost": 5})
        self.assertEqual(len(self.limiter.ledger("tenant-a")["events"]), 5)  # default 100 covers all

    def test_ledger_survives_reconfigure_and_still_matches_used(self) -> None:
        self.limiter.check("tenant-a", 4)
        self.limiter.configure("tenant-a", {"capacity": 3, "refill_per_second": 2.0})
        self.limiter.check("tenant-a", 1)
        ledger = self.limiter.ledger("tenant-a")
        self.assertEqual(ledger["totals"], {"accepted_count": 2, "accepted_cost": 5})
        self.assertEqual(ledger["totals"]["accepted_cost"], self.limiter.state("tenant-a")["used"])
        self.assertEqual([event["capacity"] for event in ledger["events"]], [10, 3])

    def test_unknown_key_is_not_found(self) -> None:
        with self.assertRaises(LimitNotFound):
            self.limiter.ledger("absent")

    def test_ledger_read_does_not_book_or_refund_early_under_regression(self) -> None:
        limiter = Limiter(self.clock)
        limiter.configure("frozen", {"capacity": 5, "refill_per_second": 0.0001})
        limiter.reserve("frozen", 3, ttl_seconds=10)                        # due at 1010
        self.clock.t = 995.0                                                # regressed below watermark
        ledger = limiter.ledger("frozen")
        self.assertEqual(ledger["totals"], {"accepted_count": 0, "accepted_cost": 0})
        self.assertEqual(limiter.state("frozen")["remaining"], 2)           # still held, no refund
        self.clock.t = 1010.0
        self.assertEqual(limiter.ledger("frozen")["events"], [])            # expiry refunds, no event
        self.assertEqual(limiter.state("frozen")["remaining"], 5)

    def test_concurrent_mixed_operations_keep_seq_gapless_and_totals_exact(self) -> None:
        limiter = Limiter(self.clock)
        limiter.configure("hot", {"capacity": 400, "refill_per_second": 0.0001})
        rids = [limiter.reserve("hot", 1, ttl_seconds=3600)["reservation_id"] for _ in range(200)]
        errors: list[BaseException] = []

        def work(index: int) -> None:
            try:
                if index % 2 == 0:
                    limiter.consume(rids[index // 2])
                else:
                    limiter.rollback(rids[index // 2])
                limiter.check("hot", 1)
                ledger = limiter.ledger("hot")
                seqs = [event["seq"] for event in ledger["events"]]
                if seqs != list(range(seqs[0], seqs[0] + len(seqs))):
                    errors.append(AssertionError(f"seq gap or duplicate: {seqs}"))
                if ledger["totals"]["accepted_count"] != len(limiter._ledgers["hot"]):
                    errors.append(AssertionError("totals do not cover every event"))
            except (OverQuota, LimitNotFound):
                pass
            except BaseException as error:  # noqa: BLE001
                errors.append(error)

        threads = [threading.Thread(target=work, args=(i,)) for i in range(400)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        ledger = limiter.ledger("hot", events_limit=1000)
        self.assertEqual([event["seq"] for event in ledger["events"]],
                         list(range(1, len(ledger["events"]) + 1)))
        self.assertEqual(ledger["totals"]["accepted_count"], len(ledger["events"]))
        self.assertEqual(ledger["totals"]["accepted_cost"], limiter.state("hot")["used"])
        self.assertEqual(ledger["totals"]["accepted_cost"],
                         sum(event["cost"] for event in ledger["events"]))


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

    def test_ledger_lifecycle_over_http(self) -> None:
        self.request("PUT", "/v1/limits/l-1", {"capacity": 5, "refill_per_second": 1})
        self.request("POST", "/v1/check", {"key": "l-1", "cost": 2})
        _, reservation, _ = self.request("POST", "/v1/reservations", {"key": "l-1", "cost": 1})
        rid = reservation["reservation_id"]
        self.request("POST", f"/v1/reservations/{rid}/consume", {})

        status, ledger, _ = self.request("GET", "/v1/ledgers/l-1")
        self.assertEqual(status, 200)
        self.assertEqual(ledger["key"], "l-1")
        self.assertEqual(ledger["totals"], {"accepted_count": 2, "accepted_cost": 3})
        _, state, _ = self.request("GET", "/v1/limits/l-1")
        self.assertEqual(ledger["totals"]["accepted_cost"], state["used"])
        self.assertEqual([event["seq"] for event in ledger["events"]], [1, 2])
        self.assertEqual(ledger["events"][0]["source"], "check")
        self.assertIsNone(ledger["events"][0]["reservation_id"])
        self.assertEqual(ledger["events"][1]["source"], "reservation_consume")
        self.assertEqual(ledger["events"][1]["reservation_id"], rid)

        status, ledger, _ = self.request("GET", "/v1/ledgers/l-1?events=1")
        self.assertEqual(status, 200)
        self.assertEqual([event["seq"] for event in ledger["events"]], [2])

    def test_unconfigured_key_is_404(self) -> None:
        status, body, _ = self.request("GET", "/v1/ledgers/absent")
        self.assertEqual((status, body["error"]["code"]), (404, "not_found"))

    def test_invalid_events_param_and_unknown_query_are_400(self) -> None:
        self.request("PUT", "/v1/limits/l-2", {"capacity": 2, "refill_per_second": 1})
        for suffix in ["?events=0", "?events=1001", "?events=-1", "?events=1.5", "?events=abc",
                       "?events=", "?events=1&events=2", "?foo=1", "?events=1&foo=2"]:
            status, body, _ = self.request("GET", f"/v1/ledgers/l-2{suffix}")
            self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"), suffix)
        # Validation failure books nothing and the key stays readable.
        status, ledger, _ = self.request("GET", "/v1/ledgers/l-2?events=1000")
        self.assertEqual(status, 200)
        self.assertEqual(ledger["totals"], {"accepted_count": 0, "accepted_cost": 0})

    def test_bad_routes_and_methods_are_404(self) -> None:
        self.assertEqual(self.request("GET", "/v1/ledgers")[0], 404)
        self.assertEqual(self.request("GET", "/v1/ledgers/a/b")[0], 404)
        self.assertEqual(self.request("POST", "/v1/ledgers/l-2", {})[0], 404)
        self.assertEqual(self.request("PUT", "/v1/ledgers/l-2", {})[0], 404)
        self.assertEqual(self.request("DELETE", "/v1/ledgers/l-2")[0], 404)


if __name__ == "__main__":
    unittest.main()
