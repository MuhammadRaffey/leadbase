"""Bounded scraping, request pacing and durable scan progress (Python 3.11+)."""

import asyncio
from collections import Counter
from datetime import timezone
from email.utils import parsedate_to_datetime
import json
import math
import os
from pathlib import Path
import random
import time


class RequestFailure(RuntimeError):
    """An unresolved request, never evidence that a carrier does not exist."""


class AccessDenied(RequestFailure):
    """The source denied access; queued work must stop until access is restored."""


async def blocking_call(function, *args):
    """Let an in-flight database call settle before cancellation releases its client."""
    task = asyncio.create_task(asyncio.to_thread(function, *args))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        try:
            await task
        finally:
            raise


def retry_after_seconds(value, now=None):
    if not value:
        return None
    try:
        seconds = float(value)
        return max(0.0, seconds) if math.isfinite(seconds) else None
    except (ValueError, TypeError):
        try:
            date = parsedate_to_datetime(value)
            if date.tzinfo is None:
                date = date.replace(tzinfo=timezone.utc)
            return max(0.0, date.timestamp() - (time.time() if now is None else now))
        except (ValueError, TypeError, OverflowError):
            return None


class RequestClient:
    """One pacing budget and cooldown for ALL requests to the source host."""

    def __init__(self, session, requests_per_second=0.5, attempts=3, timeout=15):
        if (requests_per_second <= 0 or attempts < 1 or timeout <= 0 or
                not math.isfinite(requests_per_second) or not math.isfinite(timeout)):
            raise ValueError('Rate, attempts and timeout must be positive')
        self.session = session
        self.max_rate = requests_per_second
        self.rate = requests_per_second
        self.attempts = attempts
        self.timeout = timeout
        self.lock = asyncio.Lock()
        self.next_request = 0.0
        self.cooldown_until = 0.0
        self.success_streak = 0
        self.access_denied = None
        self.metrics = {'requests': 0, 'retries': 0, 'timeouts': 0,
                        'request_seconds': 0.0, 'http_statuses': Counter()}

    async def _pace(self):
        # Sleep outside the lock, then recheck: a newly received 429 must also
        # delay workers that were already waiting for a request slot.
        while True:
            async with self.lock:
                if self.access_denied is not None:
                    raise AccessDenied(self.access_denied)
                now = time.monotonic()
                delay = max(self.next_request, self.cooldown_until) - now
                if delay <= 0:
                    self.next_request = now + 1 / self.rate
                    return
            await asyncio.sleep(delay)

    def _success(self):
        self.success_streak += 1
        if self.success_streak >= 100:
            self.rate = min(self.max_rate, self.rate * 1.1)
            self.success_streak = 0

    async def get_json(self, url):
        import aiohttp

        for attempt in range(self.attempts):
            await self._pace()
            started = time.monotonic()
            self.metrics['requests'] += 1
            delay = None
            try:
                async with self.session.get(
                    url, timeout=aiohttp.ClientTimeout(total=self.timeout)
                ) as response:
                    status = response.status
                    self.metrics['http_statuses'][str(status)] += 1
                    if status == 404:
                        self._success()
                        return None
                    if status == 200:
                        data = await response.json()
                        if not isinstance(data, dict):
                            raise ValueError('Expected a JSON object')
                        self._success()
                        return data
                    if status == 403:
                        self.access_denied = (
                            f'HTTP 403: {url}; source denied access. '
                            'Queued requests stopped; restore source access before restarting.')
                        raise AccessDenied(self.access_denied)
                    if status != 429 and not 500 <= status < 600:
                        raise RequestFailure(f'HTTP {status}: {url}')
                    delay = retry_after_seconds(response.headers.get('Retry-After'))
                    reason = f'HTTP {status}'
                    if status == 429:
                        self.rate = max(0.1, self.rate / 2)
                        self.success_streak = 0
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
                reason = type(exc).__name__
                if isinstance(exc, asyncio.TimeoutError):
                    self.metrics['timeouts'] += 1
            finally:
                self.metrics['request_seconds'] += time.monotonic() - started

            delay = delay if delay is not None else min(30, 2 ** attempt) + random.uniform(0, 0.25)
            # Shared cooldown prevents the rest of the pool continuing a burst.
            self.cooldown_until = max(self.cooldown_until, time.monotonic() + delay)
            if attempt + 1 == self.attempts:
                raise RequestFailure(f'{reason} after {self.attempts} attempts: {url}')
            self.metrics['retries'] += 1
        raise AssertionError('unreachable')


class Checkpoint:
    """Only committed chunks advance; pending IDs survive partial database writes."""

    def __init__(self, path, start_dot, end_dot, initial_data=None):
        self.path = Path(path)
        self.start_dot, self.end_dot = start_dot, end_dot
        self.data = {}
        if initial_data is not None:
            self.data = json.loads(json.dumps(initial_data))
        elif self.path.exists():
            try:
                self.data = json.loads(self.path.read_text(encoding='utf-8'))
                if not isinstance(self.data, dict):
                    raise ValueError('Checkpoint must be an object')
            except (OSError, ValueError) as exc:
                raise ValueError(f'Cannot read checkpoint {self.path}: {exc}') from exc
        if (self.data.get('version') != 2 or
                self.data.get('range') != [start_dot, end_dot]):
            self.data = {'version': 2, 'range': [start_dot, end_dot],
                         'next_dot': start_dot, 'scan_end': end_dot,
                         'frontier': start_dot, 'mode': 'full', 'pending': None,
                         'last_full_scan': None, 'completed': False}

    @property
    def next_dot(self):
        return self.data['next_dot']

    @property
    def scan_end(self):
        return self.data['scan_end']

    @property
    def pending(self):
        return self.data['pending']

    def save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(self.path.suffix + '.tmp')
        with temporary.open('w', encoding='utf-8') as handle:
            json.dump(self.data, handle, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, self.path)

    def prepare(self, now=None, gap_seconds=7 * 86400, lookback=1000, frontier_window=10000):
        if not self.data['completed']:
            return
        now = time.time() if now is None else now
        last_full = self.data['last_full_scan']
        full = last_full is None or now - last_full >= gap_seconds
        self.data.update(completed=False, pending=None, mode='full' if full else 'frontier')
        if full:
            self.data.update(next_dot=self.start_dot, scan_end=self.end_dot)
        else:
            frontier = self.data['frontier']
            self.data.update(next_dot=max(self.start_dot, frontier - lookback),
                             scan_end=min(self.end_dot, frontier + frontier_window))
        self.save()

    def begin(self, dots, chunk_end=None):
        self.data['pending'] = list(dots)
        self.data['pending_end'] = chunk_end
        self.save()

    def commit(self, next_dot, frontier):
        self.data.update(next_dot=next_dot, pending=None, pending_end=None,
                         frontier=max(self.data['frontier'], frontier))
        self.save()

    def complete(self, now=None):
        self.data['completed'] = True
        if self.data['mode'] == 'full':
            self.data['last_full_scan'] = time.time() if now is None else now
        self.save()


async def process_chunk(dots, fetch, save, workers=30, batch_size=100, flush_seconds=1):
    """Overlap discovery/enrichment with a single off-thread batch writer.

    TaskGroup cancels every stage on failure, including a producer blocked on a
    full queue. There is no checkpoint acknowledgement until the group exits.
    """
    if workers < 1 or batch_size < 1:
        raise ValueError('Worker count and batch size must be positive')
    inputs = asyncio.Queue(maxsize=workers * 2)
    outputs = asyncio.Queue(maxsize=max(batch_size * 2, workers))

    async def produce():
        for dot in dots:
            await inputs.put(dot)
        for _ in range(workers):
            await inputs.put(None)

    async def worker():
        while True:
            dot = await inputs.get()
            if dot is None:
                await outputs.put(None)
                return
            result = await fetch(dot)
            if result is not None:
                await outputs.put(result)

    async def write():
        batch, finished = [], 0
        while finished < workers:
            try:
                result = await asyncio.wait_for(outputs.get(), timeout=flush_seconds)
            except asyncio.TimeoutError:
                if batch:
                    await save(batch)
                    batch = []
                continue
            if result is None:
                finished += 1
            else:
                batch.append(result)
            if len(batch) >= batch_size:
                await save(batch)
                batch = []
        if batch:
            await save(batch)

    async with asyncio.TaskGroup() as group:
        group.create_task(produce())
        for _ in range(workers):
            group.create_task(worker())
        group.create_task(write())


def existing_dots(client, start_dot, end_dot):
    """Read only this chunk's existing IDs using indexed keyset pagination."""
    known = set()
    cursor = start_dot - 1
    while True:
        rows = (client.table('carriers').select('usdot_number_num')
                .gt('usdot_number_num', cursor).lte('usdot_number_num', end_dot)
                .order('usdot_number_num').limit(1000).execute().data or [])
        if not rows:
            return known
        values = [int(row['usdot_number_num']) for row in rows]
        known.update(values)
        cursor = max(values)


def save_batch(client, batch, build_carrier_row, mirror_tables=True):
    """Persist one replayable batch. Existing tables need no schema migration.

    Only newly discovered/pending carriers reach this writer. Replace their
    related snapshots so replay after a partial failure does not append duplicates.
    Run a single scraper per checkpoint/database range.
    """
    rows = [build_carrier_row(dot, carrier) for dot, carrier, _ in batch]
    saved = client.table('carriers').upsert(rows, on_conflict='usdot_number').execute().data or []
    ids = {str(row['usdot_number']): row['id'] for row in saved}
    if any(str(row['usdot_number']) not in ids for row in rows):
        raise RuntimeError('Carrier upsert did not return every carrier ID')
    vehicles, cargo = [], []
    for row, (dot, carrier, matrix) in zip(rows, batch):
        carrier_id = ids[str(row['usdot_number'])]
        entity = (matrix or {}).get('entity') or {}
        for equipment in entity.get('entityEquipment') or []:
            vehicle_type = (equipment.get('equipmentType') or {}).get('equipmentTypeDesc')
            if vehicle_type:
                vehicles.append({'carrier_id': carrier_id, 'usdot_number': row['usdot_number'],
                                 'vehicle_type': vehicle_type,
                                 'owned': _integer(equipment.get('owned')),
                                 'term_leased': _integer(equipment.get('termLeased'))})
        for item in entity.get('entityCargoClassification') or []:
            description = (item.get('cargoClassification') or {}).get('cargoClassificationDescription')
            if description == 'Please Describe':
                description = item.get('otherDescription') or description
            if description:
                cargo.append({'carrier_id': carrier_id, 'usdot_number': row['usdot_number'],
                              'classification': description})
    tables = [('vehicles', vehicles), ('cargo_classifications', cargo)]
    if mirror_tables:
        tables += [('carrier_vehicles', vehicles), ('carrier_cargo', [
            {**{k: v for k, v in item.items() if k != 'classification'},
             'cargo_type': item['classification']} for item in cargo])]
    for table, records in tables:
        client.table(table).delete().in_('carrier_id', list(ids.values())).execute()
        # Bound payloads even when one carrier has many equipment categories.
        for offset in range(0, len(records), 500):
            client.table(table).insert(records[offset:offset + 500]).execute()


def _integer(value):
    try:
        return int(value or 0)
    except (ValueError, TypeError):
        return 0
