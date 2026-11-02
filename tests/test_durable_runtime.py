import asyncio
import unittest

from durable_runtime import TelegramUpdateDedupMiddleware


class RuntimeStub:
    def __init__(self):
        self.seen = set()
        self.finished = []

    async def claim_update(self, update_id):
        if update_id in self.seen:
            return False
        self.seen.add(update_id)
        return True

    async def finish_update(self, update_id, error=None):
        self.finished.append((update_id, error))


class Update:
    def __init__(self, update_id):
        self.update_id = update_id


class Logger:
    def __init__(self):
        self.errors = 0

    def exception(self, *args, **kwargs):
        self.errors += 1

    def info(self, *args, **kwargs):
        pass


class MiddlewareTests(unittest.IsolatedAsyncioTestCase):
    async def test_duplicate_update_is_not_dispatched_twice(self):
        runtime = RuntimeStub()
        middleware = TelegramUpdateDedupMiddleware(runtime, Logger())
        calls = []

        async def handler(event, data):
            calls.append(event.update_id)
            return "ok"

        self.assertEqual(await middleware(handler, Update(100), {}), "ok")
        self.assertIsNone(await middleware(handler, Update(100), {}))
        self.assertEqual(calls, [100])
        self.assertEqual(runtime.finished, [(100, None)])

    async def test_failed_handler_is_recorded_and_exception_propagates(self):
        runtime = RuntimeStub()
        middleware = TelegramUpdateDedupMiddleware(runtime, Logger())

        async def handler(event, data):
            raise RuntimeError("handler failure")

        with self.assertRaisesRegex(RuntimeError, "handler failure"):
            await middleware(handler, Update(101), {})
        self.assertEqual(runtime.finished[0][0], 101)
        self.assertIn("RuntimeError", runtime.finished[0][1])

    async def test_database_failure_fails_open_to_keep_updates_flowing(self):
        class BrokenRuntime:
            async def claim_update(self, update_id):
                raise OSError("database offline")

        logger = Logger()
        middleware = TelegramUpdateDedupMiddleware(BrokenRuntime(), logger)
        called = []

        async def handler(event, data):
            called.append(event.update_id)
            return True

        self.assertTrue(await middleware(handler, Update(102), {}))
        self.assertEqual(called, [102])
        self.assertEqual(logger.errors, 1)


if __name__ == "__main__":
    unittest.main()
