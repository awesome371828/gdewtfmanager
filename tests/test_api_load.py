"""Local concurrency/load smoke test for the documented GDE.WTF API.

Uses a local HTTP stub; never sends moderation actions to the live GDE.WTF service.
Run: python -m unittest discover -s tests -p 'test_api_load.py' -v
"""
import asyncio
import time
import unittest
from aiohttp import web, ClientSession, TCPConnector


class LocalAPILoadTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        async def handler(request):
            await asyncio.sleep(0.015)
            return web.json_response({"ok": True, "path": request.path})
        self.app = web.Application()
        self.app.router.add_get("/api/ext/ping", handler)
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await self.site.start()
        self.port = self.site._server.sockets[0].getsockname()[1]
        self.url = f"http://127.0.0.1:{self.port}/api/ext/ping"

    async def asyncTearDown(self):
        await self.runner.cleanup()

    async def test_100_concurrent_read_requests_complete_without_errors(self):
        started = time.perf_counter()
        async with ClientSession(connector=TCPConnector(limit=100, limit_per_host=100)) as session:
            async def request():
                async with session.get(self.url) as response:
                    self.assertEqual(response.status, 200)
                    data = await response.json()
                    self.assertTrue(data["ok"])
            await asyncio.gather(*(request() for _ in range(100)))
        elapsed = time.perf_counter() - started
        self.assertLess(elapsed, 3.0, f"local stub load took {elapsed:.2f}s")
