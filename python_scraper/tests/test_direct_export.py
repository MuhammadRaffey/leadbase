import asyncio
import csv
import inspect
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from python_scraper import direct_export as direct
from python_scraper.scrape_pipeline import RequestFailure


class FakeHttp:
    calls = []
    fail = None
    wrong_dot = False

    def __init__(self, session, **kwargs):
        self.metrics = {'requests': 0, 'retries': 0, 'timeouts': 0,
                        'http_statuses': {}, 'request_seconds': 0}

    async def get_json(self, url):
        dot = int(url.rsplit('/', 1)[-1])
        self.calls.append(dot)
        self.metrics['requests'] += 1
        await asyncio.sleep(.01 if dot == self.fail else .001)
        if dot == self.fail:
            raise RequestFailure('unresolved HTTP response')
        if dot == 12:
            return None
        return {'entityId': f'entity-{dot}', 'entityName': f'Carrier {dot}',
                'entityDotNumber': {'dotNumber': dot + 1 if self.wrong_dot else dot}}


class DirectExportTests(unittest.IsolatedAsyncioTestCase):
    def test_default_rate_matches_conservative_scheduled_scraper(self):
        from python_scraper.auto_scraper_12h import REQUESTS_PER_SECOND
        self.assertEqual(inspect.signature(direct.export_direct).parameters[
            'requests_per_second'].default, REQUESTS_PER_SECOND)

    async def asyncSetUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.path = Path(self.folder.name)
        self.csv = self.path / 'direct.csv'
        self.db = self.path / 'progress.sqlite3'
        FakeHttp.calls, FakeHttp.fail, FakeHttp.wrong_dot = [], None, False
        self.patcher = patch.object(direct, 'RequestClient', FakeHttp)
        self.patcher.start()
        self.addCleanup(self.patcher.stop)

    async def run_export(self, **kwargs):
        return await direct.export_direct(10, 12, self.csv, self.db, workers=2,
                                          requests_per_second=1000, batch_size=1, **kwargs)

    async def test_range_is_exhaustive_only_after_all_responses_are_resolved(self):
        summary = await self.run_export()
        self.assertEqual(summary['saved'], 2)
        self.assertEqual(summary['missing'], 1)
        self.assertTrue(summary['range_complete'])
        self.assertEqual(set(FakeHttp.calls), {10, 11, 12})
        with self.csv.open(newline='', encoding='utf-8') as handle:
            self.assertEqual([row['USDOT Number'] for row in csv.DictReader(handle)], ['10', '11'])

    async def test_resume_repairs_missing_csv_without_refetching_saved_responses(self):
        FakeHttp.fail = 11
        with self.assertRaises(ExceptionGroup):
            await self.run_export()
        self.assertEqual(FakeHttp.calls.count(10), 1)
        self.csv.unlink()
        FakeHttp.fail = None
        summary = await self.run_export()
        self.assertEqual(FakeHttp.calls.count(10), 1)
        self.assertTrue(summary['range_complete'])
        with self.csv.open(newline='', encoding='utf-8') as handle:
            self.assertEqual({row['USDOT Number'] for row in csv.DictReader(handle)}, {'10', '11'})

    async def test_id_list_is_not_mislabeled_as_exhaustive_range(self):
        summary = await self.run_export(ids=[10, 10, 11, 99])
        self.assertEqual(sorted(FakeHttp.calls), [10, 11])
        self.assertEqual(summary['selection'], 'provided_ids')
        self.assertFalse(summary['range_complete'])
        self.assertTrue(summary['selection_complete'])

    async def test_mismatched_response_does_not_save_wrong_carrier(self):
        FakeHttp.wrong_dot = True
        with self.assertRaises(ExceptionGroup):
            await self.run_export(ids=[10])
        with self.csv.open(newline='', encoding='utf-8') as handle:
            self.assertEqual(list(csv.DictReader(handle)), [])

    async def test_state_and_csv_must_be_distinct(self):
        with self.assertRaises(ValueError):
            await direct.export_direct(10, 12, self.csv, self.csv)

    async def test_bounded_run_does_not_claim_the_range_is_complete(self):
        summary = await self.run_export(max_new_checks=1)
        self.assertEqual(summary['status'], 'paused')
        self.assertEqual(summary['checked_this_run'], 1)
        self.assertFalse(summary['range_complete'])
        self.assertFalse(summary['selection_complete'])


class DiscoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_paginate_filter_and_deduplicate(self):
        class Http:
            async def get_json(self, url):
                return ({'total': 4, 'data': [{'dotNumber': 10}, {'dotNumber': 99}]}
                        if 'skip=0&' in url else
                        {'total': 4, 'data': [{'dotNumber': 11}, {'dotNumber': 10}]})
        ids, report = await direct.discover_ids(Http(), ['TRUCK'], 10, 12, page_size=2)
        self.assertEqual(ids, [10, 11])
        self.assertTrue(report['queries'][0]['query_complete'])
        self.assertFalse(report['range_complete'])

    async def test_page_budget_is_explicitly_partial(self):
        class Http:
            async def get_json(self, url):
                return {'total': 100, 'data': [{'dotNumber': 10}]}
        _, report = await direct.discover_ids(Http(), ['TRUCK'], 10, 12, max_pages=1)
        self.assertFalse(report['queries'][0]['query_complete'])

    async def test_repeated_page_is_rejected(self):
        class Http:
            async def get_json(self, url):
                return {'total': 100, 'data': [{'dotNumber': 10}]}
        with self.assertRaisesRegex(ValueError, 'repeated'):
            await direct.discover_ids(Http(), ['TRUCK'], 10, 12)
