import csv
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from python_scraper import bulk_export as exporter


class BulkExportTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.root = Path(self.folder.name)
        self.source = self.root / 'census.csv'
        self.live = self.root / 'live.csv'
        self.output = self.root / 'output.csv'

    def write(self, path, rows):
        with path.open('w', newline='', encoding='utf-8') as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)

    def test_snapshot_dates_are_distinct_and_live_data_wins(self):
        self.write(self.source, [
            {'dot_number': '10', 'legal_name': 'Published', 'phone': '00123',
             'email_address': 'a@example.test', 'status_code': 'A',
             'add_date': '20260514', 'mcs150_date': '20260515'},
            {'dot_number': '11', 'legal_name': 'Second', 'phone': '',
             'email_address': '', 'status_code': 'I',
             'add_date': '20260501', 'mcs150_date': ''},
            {'dot_number': '99', 'legal_name': 'Outside', 'phone': '',
             'email_address': '', 'status_code': '', 'add_date': '', 'mcs150_date': ''}])
        self.write(self.live, [{'USDOT Number': '10', 'Legal Business Name': 'Live',
            'Create Date': '2026-05-16T12:21:23Z', 'Update Date': '', 'DOT Status': 'Active'}])
        summary = exporter.export_csv(self.source, self.output, 10, 11,
                                      live_csv=self.live, source_updated='2026-10-22')
        with self.output.open(newline='', encoding='utf-8') as handle:
            rows = list(csv.DictReader(handle))
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]['Legal Business Name'], 'Live')
        self.assertEqual(rows[0]['Data Source'], 'MOTUS live')
        self.assertEqual(rows[1]['Create Date'], '')
        self.assertEqual(rows[1]['Update Date'], '')
        self.assertEqual(rows[1]['Census Add Date'], '2026-05-01')
        self.assertEqual(rows[1]['DOT Status Code'], 'I')
        self.assertEqual(rows[1]['Out of Service'], '')
        self.assertFalse(summary['live_scan_complete'])
        self.assertEqual(summary['rows'], 2)

    def test_bad_source_preserves_existing_output(self):
        self.output.write_text('previous export', encoding='utf-8')
        self.write(self.source, [{'dot_number': '10', 'legal_name': 'First'},
                                 {'dot_number': '10', 'legal_name': 'Duplicate'}])
        with self.assertRaises(ValueError):
            exporter.export_csv(self.source, self.output, 10, 20)
        self.assertEqual(self.output.read_text(), 'previous export')

    def test_live_only_records_are_retained_and_contact_values_remain_strings(self):
        self.write(self.source, [{'dot_number': '10', 'legal_name': 'First', 'phone': '00123'}])
        self.write(self.live, [{'USDOT Number': '11', 'Legal Business Name': 'New Live'}])
        exporter.export_csv(self.source, self.output, 10, 11, live_csv=self.live)
        with self.output.open(newline='', encoding='utf-8') as handle:
            rows = list(csv.DictReader(handle))
        self.assertEqual([r['USDOT Number'] for r in rows], ['10', '11'])
        self.assertEqual(rows[0]['Business Telephone No.'], '00123')

    def test_output_cannot_replace_a_source(self):
        with self.assertRaises(ValueError):
            exporter.export_csv(self.source, self.source, 10, 11)

    def test_download_pages_by_last_id_instead_of_enumerating_gaps(self):
        responses = [b'{"rowsUpdatedAt":1000}', b'[{"records":"3"}]',
                     b'dot_number,legal_name\n10,First\n12,Second\n',
                     b'dot_number,legal_name\n20,Third\n', b'{"rowsUpdatedAt":1000}']
        urls = []
        def open_url(url, **kwargs):
            urls.append(url)
            return io.BytesIO(responses.pop(0))
        with patch.object(exporter.urllib.request, 'urlopen', side_effect=open_url):
            exporter.download_source(self.source, 10, 20)
        with self.source.open(newline='', encoding='utf-8') as handle:
            self.assertEqual([r['dot_number'] for r in csv.DictReader(handle)], ['10', '12', '20'])
        self.assertIn('dot_number > 12', exporter.urllib.parse.unquote_plus(urls[3]))

    def test_incomplete_download_preserves_previous_snapshot(self):
        self.source.write_text('previous snapshot', encoding='utf-8')
        responses = [b'{"rowsUpdatedAt":1000}', b'[{"records":"2"}]',
                     b'dot_number,legal_name\n10,First\n', b'dot_number,legal_name\n']
        with patch.object(exporter.urllib.request, 'urlopen',
                          side_effect=lambda *a, **k: io.BytesIO(responses.pop(0))):
            with self.assertRaises(ValueError):
                exporter.download_source(self.source, 10, 20)
        self.assertEqual(self.source.read_text(), 'previous snapshot')
