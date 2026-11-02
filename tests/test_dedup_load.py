import asyncio
import unittest

from durable_runtime import TelegramUpdateDedupMiddleware


class AtomicRuntime:
    def __init__(self):
        self.ids = set()

    async def claim_update(self, update_id):
        if update_id in self.ids:
            return False
        self.ids.add(update_id)
        return True

    async def finish_update(self, update_id, error=None):
        return None


class QuietLogger:
    def exception(self, *args, **kwargs):
        pass

    def info(self, *args, **kwargs):
        pass


class DedupLoadTests(unittest.IsolatedAsyncioTestCase):
    async def test_500_parallel_duplicates_dispatch_once(self):
        middleware = TelegramUpdateDedupMiddleware(AtomicRuntime(), QuietLogger())
        handled = 0
        lock = asyncio.Lock()

        async def handler(event, data):
            nonlocal handled
            async with lock:
                handled += 1
            await asyncio.sleep(0)
            return True

        class Update:
            update_id = 987654321

        results = await asyncio.gather(
            *(middleware(handler, Update(), {}) for _ in range(500))
        )
        self.assertEqual(handled, 1)
        self.assertEqual(sum(result is True for result in results), 1)


if __name__ == "__main__":
    unittest.main()
