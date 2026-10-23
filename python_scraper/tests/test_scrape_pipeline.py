import asyncio
import json
from pathlib import Path
import tempfile
import time
import unittest

from python_scraper import scrape_pipeline as pipeline


class CheckpointTests(unittest.TestCase):
    def test_invalid_request_rates_are_rejected(self):
        for rate in (0, -1, float('nan'), float('inf')):
            with self.assertRaises(ValueError):
                pipeline.RequestClient(None, requests_per_second=rate)

    def test_pending_chunk_survives_restart_and_range_change_resets_it(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'state.json'
            state = pipeline.Checkpoint(path, 10, 30)
            state.begin([12, 14])
            resumed = pipeline.Checkpoint(path, 10, 30)
            self.assertEqual(resumed.pending, [12, 14])
            self.assertEqual(resumed.next_dot, 10)
            resumed.commit(15, 14)
            self.assertEqual(pipeline.Checkpoint(path, 10, 30).next_dot, 15)
            changed = pipeline.Checkpoint(path, 20, 30)
            self.assertEqual(changed.next_dot, 20)
            self.assertIsNone(changed.pending)

    def test_completed_sweep_prioritizes_frontier_and_periodically_revisits_gaps(self):
        with tempfile.TemporaryDirectory() as folder:
            state = pipeline.Checkpoint(Path(folder) / 'state.json', 1, 100000)
            state.commit(100001, 40000)
            state.complete(now=1000)
            state.prepare(now=1100, gap_seconds=1000, lookback=10, frontier_window=100)
            self.assertEqual((state.next_dot, state.scan_end), (39990, 40100))
            state.commit(40101, 40001)
            state.complete(now=1200)
            state.prepare(now=2100, gap_seconds=1000)
            self.assertEqual((state.next_dot, state.scan_end), (1, 100000))

    def test_corrupt_checkpoint_fails_instead_of_silently_restarting(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'state.json'
            path.write_text('{broken', encoding='utf-8')
            with self.assertRaises(ValueError):
                pipeline.Checkpoint(path, 1, 10)

    def test_pending_chunk_remembers_its_end_when_chunk_size_changes(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'state.json'
            state = pipeline.Checkpoint(path, 1, 100)
            state.begin([1, 2], chunk_end=10)
            self.assertEqual(pipeline.Checkpoint(path, 1, 100).data['pending_end'], 10)


class HttpTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from aiohttp import web, ClientSession
        self.web = web
        self.routes = web.Application()
        self.statuses = []
        self.times = []

        async def handler(request):
            self.times.append(time.monotonic())
            status, headers, payload = self.statuses.pop(0)
            return web.Response(status=status, headers=headers, text=payload,
                                content_type='application/json')

        self.routes.router.add_get('/test', handler)
        self.runner = web.AppRunner(self.routes)
        await self.runner.setup()
        site = web.TCPSite(self.runner, '127.0.0.1', 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        self.url = f'http://127.0.0.1:{port}/test'
        self.session = ClientSession()

    async def asyncTearDown(self):
        await self.session.close()
        await self.runner.cleanup()

    async def test_rate_limit_waits_then_recovers_and_records_statuses(self):
        self.statuses = [(429, {'Retry-After': '0.08'}, ''), (200, {}, '{"id": 3}')]
        client = pipeline.RequestClient(self.session, requests_per_second=1000)
        result = await client.get_json(self.url)
        self.assertEqual(result, {'id': 3})
        self.assertGreaterEqual(self.times[1] - self.times[0], 0.075)
        self.assertEqual(client.metrics['http_statuses'], {'429': 1, '200': 1})
        self.assertEqual(client.metrics['retries'], 1)

    async def test_only_404_is_missing_and_forbidden_is_not_retried(self):
        self.statuses = [(404, {}, ''), (403, {}, '')]
        client = pipeline.RequestClient(self.session, requests_per_second=1000)
        self.assertIsNone(await client.get_json(self.url))
        with self.assertRaises(pipeline.RequestFailure):
            await client.get_json(self.url)
        self.assertEqual(len(self.times), 2)

    async def test_bad_json_and_400_are_not_classified_as_missing(self):
        for status, text in [(200, '<html>blocked</html>'), (400, '{}')]:
            self.statuses = [(status, {}, text)]
            client = pipeline.RequestClient(self.session, requests_per_second=1000, attempts=1)
            with self.assertRaises(pipeline.RequestFailure):
                await client.get_json(self.url)

    async def test_forbidden_stops_queued_and_future_requests(self):
        self.statuses = [(403, {}, ''), (200, {}, '{}')]
        client = pipeline.RequestClient(self.session, requests_per_second=1)
        results = await asyncio.gather(
            *(client.get_json(self.url) for _ in range(5)), return_exceptions=True)
        self.assertTrue(all(isinstance(result, pipeline.AccessDenied) for result in results), results)
        with self.assertRaises(pipeline.AccessDenied):
            await client.get_json(self.url)
        self.assertEqual(len(self.times), 1)
        self.assertEqual(client.metrics['retries'], 0)

    async def test_forbidden_preserves_support_reference_without_response_secrets(self):
        self.statuses = [(403, {'Set-Cookie': 'private=session'},
                          '<h1>Access Denied</h1>Reference&#32;&#35;18&#46;abc&#46;123&#46;def\n')]
        client = pipeline.RequestClient(self.session, requests_per_second=1000)
        with self.assertRaises(pipeline.AccessDenied) as caught:
            await client.get_json(self.url)
        self.assertIn('18.abc.123.def', str(caught.exception))
        denial = client.metrics['access_denial']
        self.assertEqual(denial['reference'], '18.abc.123.def')
        self.assertEqual(denial['url'], self.url)
        self.assertIn('time_utc', denial)
        self.assertNotIn('private=session', json.dumps(denial))
        self.assertEqual(client.metrics['requests'], 1)
        self.assertEqual(client.metrics['retries'], 0)

    async def test_forbidden_latches_before_reading_slow_diagnostic_body(self):
        from unittest.mock import AsyncMock, Mock
        import aiohttp
        reading, release = asyncio.Event(), asyncio.Event()

        async def read_body(_):
            reading.set()
            await release.wait()
            raise aiohttp.ClientPayloadError('truncated denial page')

        response = Mock(status=403, headers={})
        response.content.read = read_body
        context = AsyncMock()
        context.__aenter__.return_value = response
        session = Mock()
        session.get.return_value = context
        client = pipeline.RequestClient(session, requests_per_second=1000)
        request = asyncio.create_task(client.get_json(self.url))
        await reading.wait()
        try:
            self.assertIsNotNone(client.access_denied)
            with self.assertRaises(pipeline.AccessDenied):
                await client.get_json(self.url)
        finally:
            release.set()
            result = await asyncio.gather(request, return_exceptions=True)
        self.assertIsInstance(result[0], pipeline.AccessDenied)
        self.assertEqual(session.get.call_count, 1)
        self.assertEqual(client.metrics['retries'], 0)

    async def test_pool_cancellation_preserves_fragmented_denial_reference(self):
        from unittest.mock import AsyncMock, Mock
        reading = asyncio.Event()
        fragments = [b'<h1>Access Denied</h1>Reference&#32;&#35;18.',
                     b'abc.123.def\n', b'']

        async def read_body(_):
            reading.set()
            await asyncio.sleep(.03)
            return fragments.pop(0)

        response = Mock(status=403, headers={})
        response.content.read = read_body
        context = AsyncMock()
        context.__aenter__.return_value = response
        session = Mock()
        session.get.return_value = context
        client = pipeline.RequestClient(session, requests_per_second=1000)

        async def fetch(dot):
            if dot == 2:
                await reading.wait()
            return await client.get_json(self.url)

        async def save(batch):
            self.fail('Denied records must not be saved')

        with self.assertRaises(ExceptionGroup):
            await pipeline.process_chunk([1, 2], fetch, save, workers=2)
        self.assertEqual(client.metrics['access_denial']['reference'], '18.abc.123.def')
        self.assertEqual(session.get.call_count, 1)

    async def test_shared_pacing_limits_concurrent_requests(self):
        self.statuses = [(200, {}, '{}')] * 3
        client = pipeline.RequestClient(self.session, requests_per_second=40)
        await asyncio.gather(*(client.get_json(self.url) for _ in range(3)))
        self.assertGreaterEqual(self.times[-1] - self.times[0], 0.04)

    async def test_confirmed_missing_responses_allow_rate_recovery(self):
        self.statuses = [(404, {}, '')] * 100
        client = pipeline.RequestClient(self.session, requests_per_second=2000)
        client.rate = 1000  # simulate an earlier 429 reducing the rate
        for _ in range(100):
            self.assertIsNone(await client.get_json(self.url))
        self.assertGreater(client.rate, 1000)

    async def test_retry_after_supports_http_date(self):
        self.assertEqual(pipeline.retry_after_seconds('Thu, 01 Jan 1970 00:01:40 GMT', now=90), 10)
        self.assertIsNone(pipeline.retry_after_seconds('garbage'))


class PipelineTests(unittest.IsolatedAsyncioTestCase):
    async def test_cancellation_waits_for_database_thread_to_finish(self):
        import threading
        started, release, settled = threading.Event(), threading.Event(), threading.Event()

        def database_write():
            started.set()
            release.wait(timeout=2)
            settled.set()

        task = asyncio.create_task(pipeline.blocking_call(database_write))
        while not started.is_set():
            await asyncio.sleep(.001)
        task.cancel()
        await asyncio.sleep(.01)
        self.assertFalse(task.done())
        release.set()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(settled.is_set())

    async def test_writer_starts_before_discovery_finishes_and_flushes_partial_batch(self):
        written = asyncio.Event()
        saved = []

        async def fetch(dot):
            if dot == 2:
                await asyncio.wait_for(written.wait(), 2)
            return (dot, {'entityId': dot}, {})

        async def save(batch):
            saved.extend(dot for dot, _, _ in batch)
            written.set()

        await pipeline.process_chunk([1, 2, 3], fetch, save, workers=2, batch_size=1)
        self.assertCountEqual(saved, [1, 2, 3])

    async def test_errors_cancel_pipeline_without_hanging(self):
        async def fetch(dot):
            return (dot, {}, {})

        async def save(batch):
            raise RuntimeError('database unavailable')

        with self.assertRaises(ExceptionGroup):
            await asyncio.wait_for(
                pipeline.process_chunk(range(100), fetch, save, workers=2, batch_size=2), 2)

    async def test_batches_reduce_save_calls_and_preserve_all_results(self):
        batches = []

        async def fetch(dot):
            return (dot, {}, {}) if dot != 5 else None

        async def save(batch):
            batches.append(list(batch))

        await pipeline.process_chunk(range(12), fetch, save, workers=3, batch_size=5)
        self.assertEqual([len(batch) for batch in batches], [5, 5, 1])
        self.assertEqual(len({item[0] for batch in batches for item in batch}), 11)


if __name__ == '__main__':
    unittest.main()
