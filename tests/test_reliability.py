import asyncio
import unittest

from reliability import AsyncCircuitBreaker, CircuitOpenError, RollingLatency


class CircuitBreakerTests(unittest.IsolatedAsyncioTestCase):
    async def test_opens_after_threshold_and_recovers_after_successful_probe(self):
        breaker = AsyncCircuitBreaker(failure_threshold=2, recovery_timeout=0.01)
        await breaker.record_failure()
        await breaker.record_failure()
        with self.assertRaises(CircuitOpenError):
            await breaker.before_call()
        await asyncio.sleep(0.02)
        await breaker.before_call()
        await breaker.record_success()
        await breaker.before_call()
        self.assertEqual((await breaker.snapshot()).state, "closed")

    async def test_only_one_half_open_probe_is_allowed(self):
        breaker = AsyncCircuitBreaker(failure_threshold=1, recovery_timeout=0.01)
        await breaker.record_failure()
        await asyncio.sleep(0.02)
        await breaker.before_call()
        with self.assertRaises(CircuitOpenError):
            await breaker.before_call()
        await breaker.record_failure()

    async def test_failed_half_open_probe_restarts_recovery_window(self):
        breaker = AsyncCircuitBreaker(failure_threshold=1, recovery_timeout=0.04)
        await breaker.record_failure()
        await asyncio.sleep(0.05)
        await breaker.before_call()
        await breaker.record_failure()
        with self.assertRaises(CircuitOpenError):
            await breaker.before_call()
        await asyncio.sleep(0.05)
        await breaker.before_call()
        await breaker.record_success()

    async def test_abandoned_half_open_probe_can_be_retried(self):
        breaker = AsyncCircuitBreaker(failure_threshold=1, recovery_timeout=0.01)
        await breaker.record_failure()
        await asyncio.sleep(0.02)
        await breaker.before_call()
        await breaker.abandon_probe()
        await breaker.before_call()
        await breaker.record_success()

    async def test_latency_window_is_bounded_and_reports_errors(self):
        metrics = RollingLatency(capacity=8)
        for i in range(20):
            await metrics.add(i / 1000, i % 2 == 0)
        result = await metrics.snapshot()
        self.assertEqual(result["count"], 8)
        self.assertEqual(result["errors"], 4)
        self.assertGreaterEqual(result["p95_ms"], result["p50_ms"])


if __name__ == "__main__":
    unittest.main()


class ConfirmationStoreTests(unittest.TestCase):
    def test_user_chat_binding_and_one_shot(self):
        from confirmations import ConfirmationStore
        store = ConfirmationStore(ttl=30)
        token = store.create(user_id=1, chat_id=2, message_id=3, command="!бан @x")
        self.assertIsNone(store.consume(token, user_id=2, chat_id=2))
        self.assertIsNone(store.consume(token, user_id=1, chat_id=4))
        self.assertEqual(store.consume(token, user_id=1, chat_id=2)["command"], "!бан @x")
        self.assertIsNone(store.consume(token, user_id=1, chat_id=2))

    def test_confirmation_capacity_is_bounded(self):
        from confirmations import ConfirmationStore
        store = ConfirmationStore(ttl=30, capacity=2)
        first = store.create(user_id=1, chat_id=2, message_id=3, command="a")
        store.create(user_id=1, chat_id=2, message_id=4, command="b")
        store.create(user_id=1, chat_id=2, message_id=5, command="c")
        self.assertIsNone(store.consume(first, user_id=1, chat_id=2))
