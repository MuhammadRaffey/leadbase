"""Offline end-to-end tests with an in-memory PostgREST boundary."""
import csv
import asyncio
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
import subprocess
import sys
from unittest.mock import patch

from python_scraper import auto_scraper_12h as scraper
from python_scraper.scrape_pipeline import Checkpoint


class MemoryDatabase:
    def __init__(self):
        self.tables = {name: [] for name in (
            'carriers', 'vehicles', 'cargo_classifications', 'carrier_vehicles',
            'carrier_cargo', 'sync_runs')}
        self.fail_table = None
        self.carrier_batches = []

    def table(self, name):
        return Query(self, name)


class Query:
    def __init__(self, db, name):
        self.db, self.name = db, name
        self.operation, self.payload, self.predicates = 'select', None, []
        self.maximum = None
        self.ordering = None

    def select(self, *args, **kwargs):
        return self

    def order(self, key, desc=False):
        self.ordering = (key, desc)
        return self

    def limit(self, number):
        self.maximum = number
        return self

    def gt(self, key, value):
        self.predicates.append(lambda row: row[key] > value)
        return self

    def lte(self, key, value):
        self.predicates.append(lambda row: row[key] <= value)
        return self

    def eq(self, key, value):
        def matches(row):
            if '->>' in key:
                column, prop = key.split('->>')
                return row.get(column, {}).get(prop) == value
            return row.get(key) == value
        self.predicates.append(matches)
        return self

    def in_(self, key, values):
        self.predicates.append(lambda row: row[key] in values)
        return self

    def delete(self):
        self.operation = 'delete'
        return self

    def insert(self, payload):
        self.operation, self.payload = 'insert', payload
        return self

    def upsert(self, payload, **kwargs):
        self.operation, self.payload = 'upsert', payload
        return self

    def update(self, payload):
        self.operation, self.payload = 'update', payload
        return self

    def execute(self):
        if self.name == self.db.fail_table and self.operation == 'insert':
            raise RuntimeError('injected database failure')
        rows = self.db.tables[self.name]
        matches = [row for row in rows if all(p(row) for p in self.predicates)]
        if self.operation == 'select':
            if self.ordering:
                key, desc = self.ordering
                matches.sort(key=lambda row: row[key], reverse=desc)
            return SimpleNamespace(data=matches[:self.maximum])
        if self.operation == 'delete':
            self.db.tables[self.name] = [row for row in rows if row not in matches]
            return SimpleNamespace(data=matches)
        if self.operation == 'update':
            for row in matches:
                row.update(self.payload)
            return SimpleNamespace(data=matches)
        payload = self.payload if isinstance(self.payload, list) else [self.payload]
        if self.operation == 'upsert' and self.name == 'carriers':
            self.db.carrier_batches.append(len(payload))
        result = []
        for item in payload:
            old = next((r for r in rows if self.operation == 'upsert' and
                        r.get('usdot_number') == item.get('usdot_number')), None)
            if old is None:
                old = {'id': len(rows) + 1}
                rows.append(old)
            old.update(item)
            result.append(dict(old))
        return SimpleNamespace(data=result)


class FakeRequests:
    instances = []
    fail_dot = None

    def __init__(self, session, **kwargs):
        self.metrics = {'requests': 0, 'retries': 0, 'timeouts': 0,
                        'request_seconds': 0, 'http_statuses': {}}
        self.dots = []
        self.instances.append(self)

    async def get_json(self, url):
        self.metrics['requests'] += 1
        dot = int(url.rsplit('/', 1)[-1])
        if '/carriers/' in url:
            self.dots.append(dot)
            if dot == self.fail_dot:
                raise RuntimeError('injected HTTP failure')
            if dot == 12:
                return None
            return {'entityId': dot, 'entityDotNumber': {'dotNumber': dot},
                    'entityName': f'Carrier {dot}'}
        return {'entity': {
            'entityEquipment': [{'equipmentType': {'equipmentTypeDesc': 'Truck Tractors'}, 'owned': 2}],
            'entityCargoClassification': [{'cargoClassification': {
                'cargoClassificationDescription': 'General Freight'}}]}}


class CycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.path = Path(self.folder.name)
        self.db = MemoryDatabase()
        FakeRequests.instances = []
        FakeRequests.fail_dot = None
        self.patcher = patch.object(scraper, 'RequestClient', FakeRequests, create=True)
        self.patcher.start()

    async def asyncTearDown(self):
        self.patcher.stop()
        self.folder.cleanup()

    async def run_cycle(self):
        return await scraper.run_single_scrape(
            self.db, 10, 14, 2, str(self.path / 'leads.csv'), str(self.path / 'state.json'),
            batch_size=3, requests_per_second=1000, chunk_size=5)

    async def test_cycle_saves_complete_records_in_batches_and_marks_complete(self):
        count = await self.run_cycle()
        self.assertEqual(count, 4)
        for name in ('carriers', 'vehicles', 'cargo_classifications', 'carrier_vehicles', 'carrier_cargo'):
            self.assertEqual(len(self.db.tables[name]), 4, name)
        self.assertEqual(self.db.carrier_batches, [3, 1])
        self.assertEqual(self.db.tables['sync_runs'][-1]['status'], 'completed')
        with (self.path / 'leads.csv').open(encoding='utf-8') as handle:
            self.assertEqual(len(list(csv.DictReader(handle))), 4)
        self.assertTrue(Checkpoint(self.path / 'state.json', 10, 14).data['completed'])

    async def test_partial_database_failure_replays_carriers_without_duplicate_equipment(self):
        self.db.fail_table = 'cargo_classifications'
        with self.assertRaises(ExceptionGroup):
            await self.run_cycle()
        state = Checkpoint(self.path / 'state.json', 10, 14)
        self.assertEqual(state.next_dot, 10)
        self.assertIsNotNone(state.pending)
        self.assertEqual(self.db.tables['sync_runs'][-1]['status'], 'failed')
        self.db.fail_table = None
        await self.run_cycle()
        self.assertEqual(set(FakeRequests.instances[-1].dots), {10, 11, 12, 13, 14})
        self.assertEqual(len(self.db.tables['vehicles']), 4)
        self.assertEqual(len(self.db.tables['cargo_classifications']), 4)

    async def test_lost_local_checkpoint_recovers_pending_ids_from_database(self):
        self.db.fail_table = 'cargo_classifications'
        with self.assertRaises(ExceptionGroup):
            await self.run_cycle()
        (self.path / 'state.json').unlink()
        self.db.fail_table = None
        await self.run_cycle()
        self.assertEqual(set(FakeRequests.instances[-1].dots), {10, 11, 12, 13, 14})
        self.assertEqual(len(self.db.tables['cargo_classifications']), 4)

    async def test_http_failure_keeps_chunk_pending(self):
        FakeRequests.fail_dot = 11
        with self.assertRaises(ExceptionGroup):
            await self.run_cycle()
        state = Checkpoint(self.path / 'state.json', 10, 14)
        self.assertFalse(state.data['completed'])
        self.assertEqual(state.next_dot, 10)
        self.assertIn(11, state.pending)

    async def test_existing_carriers_are_skipped_before_http(self):
        self.db.tables['carriers'].append({'id': 1, 'usdot_number': '10', 'usdot_number_num': 10})
        await self.run_cycle()
        self.assertNotIn(10, FakeRequests.instances[-1].dots)
        self.assertEqual(len(self.db.tables['carriers']), 4)

    async def test_time_budget_leaves_resumable_state(self):
        await scraper.run_single_scrape(
            self.db, 10, 14, 2, str(self.path / 'leads.csv'), str(self.path / 'state.json'),
            max_run_seconds=.000001)
        state = Checkpoint(self.path / 'state.json', 10, 14)
        self.assertFalse(state.data['completed'])
        self.assertEqual(state.next_dot, 10)
        self.assertEqual(self.db.tables['sync_runs'][-1]['status'], 'paused')

    async def test_time_budget_interrupts_a_slow_chunk_and_keeps_pending_ids(self):
        original = FakeRequests.get_json

        async def slow_request(instance, url):
            if '/carriers/11' in url:
                await asyncio.sleep(1)
            return await original(instance, url)

        with patch.object(FakeRequests, 'get_json', slow_request):
            await scraper.run_single_scrape(
                self.db, 10, 14, 2, str(self.path / 'leads.csv'), str(self.path / 'state.json'),
                max_run_seconds=.25, chunk_size=5)
        state = Checkpoint(self.path / 'state.json', 10, 14)
        self.assertEqual(state.next_dot, 10)
        self.assertIsNotNone(state.pending)
        self.assertEqual(self.db.tables['sync_runs'][-1]['status'], 'paused')

    async def test_csv_replay_after_checkpoint_failure_does_not_append_duplicate_rows(self):
        with patch.object(Checkpoint, 'commit', side_effect=OSError('disk unavailable')):
            with self.assertRaises(OSError):
                await self.run_cycle()
        await self.run_cycle()
        with (self.path / 'leads.csv').open(encoding='utf-8') as handle:
            self.assertEqual(len(list(csv.DictReader(handle))), 4)


class FieldMappingTests(unittest.TestCase):
    def test_entity_id_is_not_used_as_a_usdot_number(self):
        carrier = {'entityId': 900, 'entityName': 'Test Carrier'}
        self.assertEqual(scraper.build_supabase_row(123, carrier)['usdot_number'], '123')
        self.assertEqual(scraper.build_row(123, carrier)['USDOT Number'], '123')


class CliTests(unittest.TestCase):
    def test_tuning_flags_are_available_without_loading_credentials(self):
        result = subprocess.run([sys.executable, '-m', 'python_scraper.auto_scraper_12h', '--help'],
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0)
        for flag in ('--requests-per-second', '--batch-size', '--state-file', '--gap-days', '--max-run-seconds'):
            self.assertIn(flag, result.stdout)


if __name__ == '__main__':
    unittest.main()
