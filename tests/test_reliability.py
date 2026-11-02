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
