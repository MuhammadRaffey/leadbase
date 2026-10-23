"""Export published FMCSA carriers in bulk; never enumerate unused USDOT numbers."""
import argparse
import csv
from datetime import datetime, timezone
import io
import json
import os
from pathlib import Path
import time
import urllib.parse
import urllib.request

SOURCE = 'https://data.transportation.gov/Trucking-and-Motorcoaches/Company-Census-File/az4n-8mr2'
API = 'https://data.transportation.gov/resource/az4n-8mr2'
FIELDS = ['USDOT Number', 'Legal Business Name', 'Business Telephone No.',
          'Business Email', 'DOT Status', 'Out of Service', 'Update Date', 'Create Date',
          'DOT Status Code', 'MCS-150 Date', 'Census Add Date', 'Data Source',
          'Source Snapshot Updated']


def census_date(value):
    try:
        return datetime.strptime(value, '%Y%m%d').date().isoformat() if value else ''
    except ValueError:
        return value  # Preserve source data rather than invent a date.


def export_csv(source_csv, output_csv, start, end, *, live_csv=None, source_updated=''):
    started = time.perf_counter()
    source_csv, output_csv = Path(source_csv), Path(output_csv)
    live_csv = Path(live_csv) if live_csv else None
    if start < 1 or end < start:
        raise ValueError('Invalid USDOT range')
    if output_csv.resolve() in {p.resolve() for p in (source_csv, live_csv) if p}:
        raise ValueError('Output must not overwrite an input')
    rows = {}
    with source_csv.open(newline='', encoding='utf-8-sig') as handle:
        reader = csv.DictReader(handle)
        if not {'dot_number', 'legal_name'} <= set(reader.fieldnames or []):
            raise ValueError('Expected the FMCSA Company Census CSV schema')
        for record in reader:
            dot = int(record['dot_number'])
            if not start <= dot <= end:
                continue
            if dot in rows:
                raise ValueError(f'Duplicate census USDOT {dot}')
            rows[dot] = dict.fromkeys(FIELDS, '')
            rows[dot].update({
                'USDOT Number': str(dot), 'Legal Business Name': record['legal_name'],
                'Business Telephone No.': record.get('phone', ''),
                'Business Email': record.get('email_address', ''),
                # Status codes, OOS orders, MCS-150 dates and MOTUS lifecycle
                # dates have different meanings. Do not substitute one for another.
                'DOT Status Code': record.get('status_code', ''),
                'MCS-150 Date': census_date(record.get('mcs150_date', '')),
                'Census Add Date': census_date(record.get('add_date', '')),
                'Data Source': 'FMCSA Company Census',
                'Source Snapshot Updated': source_updated})
    snapshot_count = len(rows)
    live_count, live_seen = 0, set()
    if live_csv:
        with live_csv.open(newline='', encoding='utf-8-sig') as handle:
            reader = csv.DictReader(handle)
            if not {'USDOT Number', 'Legal Business Name'} <= set(reader.fieldnames or []):
                raise ValueError('Expected the live MOTUS export CSV schema')
            for record in reader:
                dot = int(record['USDOT Number'])
                if not start <= dot <= end:
                    continue
                if dot in live_seen:
                    raise ValueError(f'Duplicate live USDOT {dot}')
                if not record['Legal Business Name'].strip():
                    raise ValueError(f'Missing live carrier name for {dot}')
                live_seen.add(dot)
                # Preserve the whole live row; avoid silently filling blank live
                # fields with older snapshot values from a different observation.
                rows[dot] = {key: record.get(key, '') for key in FIELDS}
                rows[dot]['Data Source'] = 'MOTUS live'
                live_count += 1
    temporary = output_csv.with_suffix(output_csv.suffix + '.tmp')
    try:
        with temporary.open('w', newline='', encoding='utf-8') as handle:
            writer = csv.DictWriter(handle, fieldnames=FIELDS)
            writer.writeheader()
            writer.writerows(rows[dot] for dot in sorted(rows))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, output_csv)
    finally:
        temporary.unlink(missing_ok=True)
    summary = {'range': [start, end], 'rows': len(rows), 'snapshot_records': snapshot_count,
               'live_records': live_count, 'source': SOURCE,
               'source_snapshot_updated': source_updated,
               'generated_utc': datetime.now(timezone.utc).isoformat(),
               'elapsed_seconds': round(time.perf_counter() - started, 3),
               'live_scan_complete': False,
               'coverage': 'All provided census records in range plus provided live records. '
                           'Absence from the census is not proof of absence from MOTUS.'}
    manifest = output_csv.with_suffix('.source.json')
    temporary = manifest.with_suffix('.json.tmp')
    temporary.write_text(json.dumps(summary, indent=2), encoding='utf-8')
    os.replace(temporary, manifest)
    return summary


def download_source(path, start, end):
    """Keyset-page the official source; validate count and version before replacing."""
    path = Path(path)
    if start < 1 or end < start:
        raise ValueError('Invalid USDOT range')

    def get(url):
        with urllib.request.urlopen(url, timeout=120) as response:
            return response.read().decode('utf-8-sig')

    metadata_url = 'https://data.transportation.gov/api/views/az4n-8mr2.json'
    metadata = json.loads(get(metadata_url))
    where = f'dot_number between {start} and {end}'
    query = urllib.parse.urlencode({'$where': where, '$select': 'count(*) as records'})
    expected = int(json.loads(get(f'{API}.json?{query}'))[0]['records'])
    temporary = path.with_suffix('.csv.download')
    count, cursor = 0, start - 1
    try:
        with temporary.open('w', newline='', encoding='utf-8') as handle:
            writer = None
            fields = None
            while count < expected:
                query = urllib.parse.urlencode({'$where': f'{where} and dot_number > {cursor}',
                                                '$order': 'dot_number', '$limit': 10000})
                page = csv.DictReader(io.StringIO(get(f'{API}.csv?{query}')))
                if not {'dot_number', 'legal_name'} <= set(page.fieldnames or []):
                    raise ValueError('Invalid source response schema')
                if writer is None:
                    fields = page.fieldnames
                    writer = csv.DictWriter(handle, fieldnames=fields)
                    writer.writeheader()
                elif page.fieldnames != fields:
                    raise ValueError('Source schema changed during download')
                received = 0
                for row in page:
                    dot = int(row['dot_number'])
                    if not cursor < dot <= end:
                        raise ValueError('Unordered or out-of-range source page')
                    writer.writerow(row)
                    cursor, count, received = dot, count + 1, received + 1
                if not received:
                    raise ValueError('Source ended before the expected record count')
                print(json.dumps({'downloaded': count, 'expected': expected}), flush=True)
            if count != expected or json.loads(get(metadata_url))['rowsUpdatedAt'] != metadata['rowsUpdatedAt']:
                raise ValueError('Source changed during download; retry the export')
            # An empty range still needs a valid CSV header.
            if writer is None:
                handle.write('dot_number,legal_name\r\n')
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return datetime.fromtimestamp(metadata['rowsUpdatedAt'], timezone.utc).isoformat()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--start-dot', type=int, default=4582560)
    parser.add_argument('--end-dot', type=int, default=10000000)
    parser.add_argument('--source-csv', type=Path, required=True)
    parser.add_argument('--live-csv', type=Path)
    parser.add_argument('--output-csv', type=Path, required=True)
    parser.add_argument('--source-updated', default='')
    parser.add_argument('--refresh', action='store_true', help='Download current range from the official bulk API first')
    args = parser.parse_args()
    started = time.perf_counter()
    if args.output_csv.resolve() in {p.resolve() for p in (args.source_csv, args.live_csv) if p}:
        parser.error('Output must not overwrite an input')
    if args.refresh and args.live_csv and args.source_csv.resolve() == args.live_csv.resolve():
        parser.error('Refresh must not overwrite live records')
    updated = download_source(args.source_csv, args.start_dot, args.end_dot) if args.refresh else args.source_updated
    summary = export_csv(args.source_csv, args.output_csv, args.start_dot, args.end_dot,
                         live_csv=args.live_csv, source_updated=updated)
    print(json.dumps({**summary, 'source_downloaded': args.refresh,
                      'total_elapsed_seconds': round(time.perf_counter() - started, 3)}), flush=True)


if __name__ == '__main__':
    main()
