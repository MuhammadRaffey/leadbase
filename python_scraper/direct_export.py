"""Concurrent, resumable CSV extraction directly from public MOTUS carrier details."""
import argparse
import asyncio
import csv
from itertools import islice
import json
import math
import os
from pathlib import Path
import sqlite3
import time
from urllib.parse import urlencode

import aiohttp

from .auto_scraper_12h import FIELDS, HEADERS, REQUESTS_PER_SECOND, build_row, get_legal_name
from .scrape_pipeline import RequestClient, process_chunk


async def discover_ids(http, queries, start, end, *, max_pages=5, page_size=100):
    """Discover candidates through public name searches; never certify range coverage."""
    if max_pages < 1 or not 1 <= page_size <= 100:
        raise ValueError('Search pages must be positive and page size must be 1–100')
    queries = list(dict.fromkeys(query.strip() for query in queries))
    if not queries or any(len(query) < 3 for query in queries):
        raise ValueError('Each search query must contain at least three characters')
    ids, reports = set(), []
    for query in queries:
        seen, fetched, total = set(), 0, None
        for page in range(max_pages):
            params = urlencode({'query': query, 'skip': page * page_size, 'limit': page_size})
            response = await http.get_json('https://motus.dot.gov/api/carriers/search?' + params)
            if not isinstance(response, dict):
                raise ValueError('Invalid search response')
            rows, total = response.get('data'), response.get('total')
            if not isinstance(rows, list) or type(total) is not int or total < 0:
                raise ValueError('Invalid search rows or count')
            fingerprint = json.dumps(rows, sort_keys=True)
            if rows and fingerprint in seen:
                raise ValueError('Search repeated a page; pagination is unreliable')
            seen.add(fingerprint)
            if not rows and fetched < total:
                raise ValueError('Search ended before its reported total')
            for row in rows:
                if not isinstance(row, dict):
                    raise ValueError('Invalid search row')
                dot = row.get('dotNumber')
                if dot is not None:
                    dot = int(dot)
                    if start <= dot <= end:
                        ids.add(dot)
            fetched += len(rows)
            if fetched >= total:
                break
        reports.append({'query': query, 'reported_total': total, 'summaries_read': fetched,
                        'query_complete': fetched >= total})
    return sorted(ids), {'source': 'MOTUS public search', 'queries': reports,
                         'candidate_count': len(ids), 'range_complete': False}


def rebuild_csv(database, output):
    """SQLite is authoritative: repair any crash between DB commit and CSV flush."""
    temporary = output.with_suffix(output.suffix + '.tmp')
    try:
        with temporary.open('w', newline='', encoding='utf-8') as handle:
            writer = csv.DictWriter(handle, fieldnames=FIELDS)
            writer.writeheader()
            for payload, in database.execute('SELECT row_json FROM responses WHERE status=200 ORDER BY dot'):
                writer.writerow(json.loads(payload))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, output)
    finally:
        temporary.unlink(missing_ok=True)


async def export_direct(start, end, output_csv, state_db, *, ids=None, workers=8,
                        requests_per_second=REQUESTS_PER_SECOND, batch_size=25, max_new_checks=0,
                        max_seconds=0, close_connections=True, discovery=None):
    output, state_db = Path(output_csv), Path(state_db)
    if start < 1 or end < start or min(workers, batch_size, requests_per_second) <= 0:
        raise ValueError('Invalid range, worker count, batch size or rate')
    if (not math.isfinite(requests_per_second) or not math.isfinite(max_seconds)
            or min(max_new_checks, max_seconds) < 0 or output.resolve() == state_db.resolve()):
        raise ValueError('Invalid budget or conflicting output/state paths')
    selected = sorted({int(dot) for dot in ids if start <= int(dot) <= end}) if ids is not None else None
    started = time.monotonic()
    database = sqlite3.connect(state_db)
    http, failure, status = None, None, 'failed'
    previous, initialized = None, False
    newly_saved, newly_checked = 0, 0
    try:
        database.execute('CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)')
        database.execute('CREATE TABLE IF NOT EXISTS responses '
                         '(dot INTEGER PRIMARY KEY, status INTEGER NOT NULL CHECK(status IN (200,404)), row_json TEXT)')
        previous = database.execute("SELECT value FROM metadata WHERE key='range'").fetchone()
        if previous and json.loads(previous[0]) != [start, end]:
            raise ValueError('Checkpoint belongs to a different range; choose another state database')
        database.execute("INSERT OR REPLACE INTO metadata VALUES ('range',?)", (json.dumps([start, end]),))
        database.commit()
        if output.exists() and output.stat().st_size and not database.execute('SELECT 1 FROM responses LIMIT 1').fetchone():
            raise ValueError('Existing CSV has no matching checkpoint; choose a new output file')
        rebuild_csv(database, output)
        initialized = True

        def pending():
            for dot in selected if selected is not None else range(start, end + 1):
                if not database.execute('SELECT 1 FROM responses WHERE dot=?', (dot,)).fetchone():
                    yield dot

        candidates = islice(pending(), max_new_checks) if max_new_checks else pending()
        connector = aiohttp.TCPConnector(limit=workers, force_close=close_connections)
        async with aiohttp.ClientSession(headers=HEADERS, connector=connector) as session:
            http = RequestClient(session, requests_per_second=requests_per_second, timeout=20)
            with output.open('a', newline='', encoding='utf-8') as handle:
                writer = csv.DictWriter(handle, fieldnames=FIELDS)

                async def fetch(dot):
                    carrier = await http.get_json(f'https://motus.dot.gov/api/carriers/{dot}')
                    if carrier is not None:
                        actual = (carrier.get('entityDotNumber') or {}).get('dotNumber')
                        if (not carrier.get('entityId') or not get_legal_name(carrier)
                                or (actual is not None and str(actual) != str(dot))):
                            raise ValueError(f'Invalid or mismatched carrier response for {dot}')
                    return dot, carrier, None

                async def save(batch):
                    nonlocal newly_saved, newly_checked
                    rows = [(dot, 200 if carrier is not None else 404,
                             json.dumps(build_row(dot, carrier)) if carrier is not None else None)
                            for dot, carrier, _ in batch]
                    with database:
                        database.executemany('INSERT INTO responses VALUES (?,?,?)', rows)
                    writer.writerows(json.loads(payload) for _, code, payload in rows if code == 200)
                    handle.flush()
                    newly_checked += len(rows)
                    newly_saved += sum(code == 200 for _, code, _ in rows)
                    elapsed = max(time.monotonic() - started, .001)
                    print(json.dumps({'checked_this_run': newly_checked, 'saved_this_run': newly_saved,
                                      'elapsed_seconds': round(elapsed, 2),
                                      'http_statuses': dict(http.metrics['http_statuses'])}), flush=True)

                try:
                    async with asyncio.timeout(max_seconds or None):
                        await process_chunk(candidates, fetch, save, workers=workers,
                                            batch_size=batch_size, flush_seconds=1)
                    status = 'completed'
                except TimeoutError:
                    status = 'paused'
                finally:
                    handle.flush()
                    os.fsync(handle.fileno())
        rebuild_csv(database, output)
    except BaseException as error:
        failure = repr(error)
        raise
    finally:
        elapsed = max(time.monotonic() - started, .001)
        # A failure during initial validation must not overwrite unrelated files.
        if initialized:
            counts = dict(database.execute('SELECT status,count(*) FROM responses GROUP BY status'))
            checked = sum(counts.values())
            selection_checked = (sum(bool(database.execute('SELECT 1 FROM responses WHERE dot=?', (dot,)).fetchone())
                                     for dot in selected) if selected is not None else checked)
            selection_total = len(selected) if selected is not None else end - start + 1
            selection_complete = selection_checked == selection_total
            if status == 'completed' and not selection_complete:
                status = 'paused'
            summary = {'status': status, 'range': [start, end], 'source': 'MOTUS direct API',
                       'selection': 'provided_ids' if selected is not None else 'exhaustive_range',
                       'selection_total': selection_total, 'selection_complete': selection_complete,
                       'range_complete': checked == end - start + 1,
                       'saved': counts.get(200, 0), 'missing': counts.get(404, 0),
                       'checked_this_run': newly_checked, 'saved_this_run': newly_saved,
                       'elapsed_seconds': round(elapsed, 3),
                       'saved_per_minute': round(newly_saved / elapsed * 60, 2),
                       'configured_rps': requests_per_second, 'workers': workers,
                       'http': dict(http.metrics) if http else {}, 'error': failure}
            if discovery is not None:
                summary['selection'] = 'public_search_candidates'
                summary['discovery'] = discovery
                summary['total_elapsed_seconds'] = round(elapsed + discovery['elapsed_seconds'], 3)
            if not previous or json.loads(previous[0]) == [start, end]:
                manifest = output.with_suffix('.source.json')
                temporary = manifest.with_suffix('.json.tmp')
                temporary.write_text(json.dumps(summary, indent=2), encoding='utf-8')
                os.replace(temporary, manifest)
            print(json.dumps(summary), flush=True)
        database.close()
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--start-dot', type=int, default=4582560)
    parser.add_argument('--end-dot', type=int, default=10000000)
    parser.add_argument('--output-csv', type=Path, required=True)
    parser.add_argument('--state-db', type=Path, required=True)
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument('--ids-csv', type=Path, help='Candidate IDs; does not establish exhaustive range coverage')
    selection.add_argument('--query', action='append', help='Public name search, repeatable; partial range coverage')
    parser.add_argument('--max-search-pages', type=int, default=5)
    parser.add_argument('--workers', type=int, default=8)
    parser.add_argument('--requests-per-second', type=float, default=REQUESTS_PER_SECOND,
                        help='Shared request cap (default: %(default)s); no published MOTUS allowance')
    parser.add_argument('--batch-size', type=int, default=25)
    parser.add_argument('--max-new-checks', type=int, default=0)
    parser.add_argument('--max-seconds', type=float, default=0)
    parser.add_argument('--keep-connections', action='store_true')
    args = parser.parse_args()
    ids = None
    if args.ids_csv:
        if args.ids_csv.resolve() in {args.output_csv.resolve(), args.state_db.resolve()}:
            parser.error('Input IDs must not be overwritten')
        with args.ids_csv.open(newline='', encoding='utf-8-sig') as handle:
            reader = csv.DictReader(handle)
            if 'USDOT Number' not in (reader.fieldnames or []):
                parser.error('ID CSV must contain a USDOT Number column')
            ids = [int(row['USDOT Number']) for row in reader]
    async def run():
        candidates, discovery = ids, None
        if args.query:
            started = time.monotonic()
            async with aiohttp.ClientSession(headers=HEADERS) as session:
                http = RequestClient(session, requests_per_second=args.requests_per_second, timeout=20)
                candidates, discovery = await discover_ids(http, args.query, args.start_dot, args.end_dot,
                                                           max_pages=args.max_search_pages)
                discovery['elapsed_seconds'] = round(time.monotonic() - started, 3)
                discovery['http'] = dict(http.metrics)
        return await export_direct(args.start_dot, args.end_dot, args.output_csv, args.state_db,
                                   ids=candidates, discovery=discovery, workers=args.workers,
                                   requests_per_second=args.requests_per_second,
                                   batch_size=args.batch_size, max_new_checks=args.max_new_checks,
                                   max_seconds=args.max_seconds, close_connections=not args.keep_connections)
    asyncio.run(run())


if __name__ == '__main__':
    main()
