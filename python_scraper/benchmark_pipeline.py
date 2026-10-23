"""Offline processing-pattern comparison, not a production speed claim.

Run from the repo root: python -m python_scraper.benchmark_pipeline
Both paths use the same loopback HTTP fixtures and simulated DB round-trip
latency. No MOTUS or Supabase traffic, credentials or personal data are used.
"""
import asyncio
import json
import time

from aiohttp import ClientSession, web

from .scrape_pipeline import process_chunk


async def benchmark(count=100, concurrency=20):
    async def handler(request):
        await asyncio.sleep(.01)
        return web.json_response({'entityId': int(request.match_info['dot'])})

    app = web.Application()
    app.router.add_get('/{kind}/{dot}', handler)
    runner = web.AppRunner(app)
    await runner.setup()
    try:
        site = web.TCPSite(runner, '127.0.0.1', 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        async with ClientSession() as session:
            async def get(kind, dot):
                async with session.get(f'http://127.0.0.1:{port}/{kind}/{dot}') as response:
                    return await response.json()

            semaphore = asyncio.Semaphore(concurrency)

            async def discover(dot):
                async with semaphore:
                    return dot, await get('carrier', dot)

            t0 = time.perf_counter()
            found = await asyncio.gather(*(discover(dot) for dot in range(count)))
            for dot, carrier in found:
                await asyncio.sleep(.01)  # individual carrier database write
                await get('equipment', dot)
                await asyncio.sleep(.04)  # four related-table database writes
            sequential_seconds = time.perf_counter() - t0

            saved, writes = 0, 0

            async def fetch(dot):
                carrier = await get('carrier', dot)
                matrix = await get('equipment', dot)
                return dot, carrier, matrix

            async def persist(batch):
                nonlocal saved, writes
                # Carrier upsert, four replacements (delete + insert each).
                await asyncio.sleep(.09)
                writes += 9
                saved += len(batch)

            t0 = time.perf_counter()
            await process_chunk(range(count), fetch, persist, workers=concurrency, batch_size=25)
            pipeline_seconds = time.perf_counter() - t0
            result = {'fixture_carriers': count, 'http_requests_per_path': count * 2,
                      'old_pattern_seconds': round(sequential_seconds, 3),
                      'pipeline_seconds': round(pipeline_seconds, 3),
                      'old_pattern_db_requests': count * 5,
                      'pipeline_db_requests': writes, 'pipeline_saved': saved,
                      'synthetic_speedup': round(sequential_seconds / pipeline_seconds, 2)}
            print(json.dumps(result, indent=2))
            return result
    finally:
        await runner.cleanup()


if __name__ == '__main__':
    asyncio.run(benchmark())
