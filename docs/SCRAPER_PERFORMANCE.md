# Scraper performance and recovery

The scheduled entry point remains `python_scraper/auto_scraper_12h.py` (Python 3.11+).
It uses the existing Supabase schema and MOTUS endpoints. No proxy subscription,
browser fleet, database migration, or frontend change is required.

## Fast local CSV export

For a local contact CSV, use the bulk exporter instead of scanning every possible
USDOT number. For 4582560–10000000, the published census snapshot contains 81,531
records. The range contains 5,417,441 possible integers; those are not all carriers.
The original download has already been saved locally. Rebuilding a contact CSV
from that source and the collected live records took 2.322 seconds on this machine.
That timing excludes the original network download.

A subsequent end-to-end `--refresh` run downloaded and validated all 81,531 rows
and generated the contact CSV in 67.440 seconds (2.287 seconds of export processing).
The source reported an update time of 2026-10-23T13:52:51Z. Final validation found
81,531 unique in-range IDs, 81,526 nonblank phone numbers and 81,529 nonblank emails;
124 rows came from collected live MOTUS records and 81,407 from the census snapshot.
This measurement is for bulk published data, not live API confirmation of every row.

```powershell
python -m python_scraper.bulk_export --source-csv carriers_4582560_10000000_official_census.csv --live-csv carriers_4582560_10000000.csv --output-csv carriers_4582560_10000000_fast.csv --source-updated 2026-10-22T17:06:07+00:00
```

For later refreshes, add `--refresh` and omit `--source-updated`. The exporter asks
the official DOT API for the record count and downloads ordered pages of up to
10,000 existing carriers using a last-ID cursor. It checks source version, count,
schema, order and range before replacing the cached CSV. It does not issue a
request for each possible USDOT number, and requires no Supabase credentials.
Use `--start-dot` and `--end-dot` to change the default range.

The result includes source provenance per row and an adjacent `.source.json`
coverage manifest. The 124 collected live MOTUS rows take precedence as complete
rows. Snapshot-only rows contain census status codes and census dates in separate
columns: blank MOTUS lifecycle dates or OOS fields mean unavailable, not negative.
Absence from the published snapshot does not establish absence from live MOTUS.
See the FMCSA coverage and update limitations below. The previous one-off
full-range live scan has been stopped; the live-only CSV is retained.

## What changed

- Discovery and equipment/cargo enrichment run in bounded concurrent workers.
  A single writer saves batches in a thread, allowing HTTP workers to continue.
- Existing carrier IDs are read only for the current chunk, using indexed keyset
  pagination. The entire carrier table is no longer downloaded on every run.
- A shared requests-per-second budget covers both MOTUS endpoints. HTTP 429
  reduces the rate and honors `Retry-After`; transient errors use exponential
  backoff with jitter. Successful traffic gradually restores the configured cap.
- Only HTTP 404 means missing. HTTP 400/403, invalid JSON, exhausted retries and
  invalid enrichment responses fail visibly and leave the chunk pending. Confirm
  MOTUS's actual missing-record response contract during the first bounded live run;
  the old script treated every 400 as a missing carrier without inspecting its cause.
- Carrier writes and related-table writes are batched. Both canonical tables and
  legacy mirror tables remain enabled. `--canonical-only` explicitly disables
  mirror writes for installations that do not have those tables.
- TLS certificate verification is enabled.

## Running and tuning

Install the existing dependencies with `pip install -r python_scraper/requirements.txt`.
Set `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` in the root `.env` or process
environment. Missing credentials now fail instead of silently producing CSV-only
results that the dashboard would never see.

```powershell
python python_scraper/auto_scraper_12h.py --once --requests-per-second 0.5 --batch-size 100
```

`--concurrency` defaults to 4 workers. This is a ceiling for in-flight
work, not a request rate. The 0.5 requests/second default follows observed access
denials at higher rates; it is not a published MOTUS allowance. Tune against observed successful
saved leads/minute, latency, 429s and timeouts; higher concurrency alone is not a
throughput guarantee. Batch size defaults to 100 carriers; chunks default to 5,000
USDOT numbers. CSV writes flush once per committed chunk.

Before a full live run, compare the old and new paths on the same bounded DOT range,
using separate state/CSV files and a staging database if possible. Measure valid,
fully persisted leads rather than HTTP attempts. The first phase has not been
benchmarked against MOTUS or a production Supabase database.

## Recovery and scheduling

`--state-file` defaults to `motus_scraper_state_v10.json`. Progress is written
atomically before each chunk and committed only after every save and CSV flush
for that chunk succeeds. The same progress is written ahead to the existing
`sync_runs.stats.checkpoint` field before any carrier writes. Startup prefers the
latest database checkpoint for the configured range, so a lost or stale Actions
cache cannot hide partially saved carriers. Failed/interrupted chunks retain their candidate IDs;
restart retries them even when their carrier rows already exist. Related snapshots
are replaced for these candidates, so partial retries do not append duplicates.
The writer is allowed to finish an in-flight database operation during cancellation.

Related-table replacements are multiple database requests, not one transaction.
Readers can briefly see an incomplete snapshot; the pending checkpoint repairs it
on retry. Run one scraper per database range/checkpoint; the GitHub workflow
serializes its runs, but a separate local daemon must not run over the same range
at the same time. Keep the checkpoint and its `sync_runs` history when restarting
after a partial failure. Database checkpoint writes are required: a failure in this
step aborts the run before data can get ahead of recoverable progress. Deleting both
local and database checkpoints loses pending repair IDs; existing incomplete carrier
rows would then need an explicit equipment/cargo backfill.

The local CSV is a backup for chunks processed on that machine, not a full database
export. On local replay its uncommitted chunk is truncated before rewriting. Actions
caches only scan metadata, not CSV/contact data. A new runner starts a new local CSV.

After the initial full scan, subsequent runs scan a window from 1,000 numbers below
the highest known/discovered DOT through 10,000 above it (bounded by the configured
range). A full gap sweep repeats every seven days. Tune `--frontier-window` and
`--gap-days` for freshness; these windows do not refresh profiles already present.
An interrupted full sweep resumes before switching to incremental work.

The configured end DOT remains a hard ceiling. Increase `--end-dot` when needed;
changing the configured range resets scan progress to a full sweep. Legacy state
files without version 2 start a full sweep; corrupt JSON fails visibly.

GitHub Actions restores the most recent matching checkpoint cache and saves an
updated cache even after a failed scrape step. Caches can be evicted; loss of a cache
falls back to the durable database copy. Without either checkpoint, a new sweep
starts and skips existing carriers. The workflow uses a 5.5-hour budget inside its
six-hour job limit, including cancellation within a slow chunk, to leave time for
checkpoint saving.

## Metrics and offline verification

### MOTUS access denial

A 403 opens a shared stop condition for that request client: requests waiting for
a pacing slot and future calls fail without contacting the source. Requests already
in flight may finish. Restart only after access is restored. This preserves pending
work; it does not remove a remote access restriction.

On 2026-10-23, both the MOTUS homepage and carrier API returned an Akamai Access
Denied page, and the user confirmed the same result in their normal browser.
This establishes that the denial also affected the normal browser at that time,
but does not identify the exact CDN rule. Later the same connection recovered
without a network change: the homepage and API returned 200. A serial standard
Python client completed 40 lookups (1 HTTP 200, 39 HTTP 404) with one-second pauses
after responses and no 403. A separate async check using uncompressed responses
also succeeded. Some header/client combinations were still denied, and other
async combinations timed out. These observations do not isolate a single WAF rule.

The scheduled scraper now uses its normal library User-Agent, no fabricated
browser Origin/Referer, and `Accept-Encoding: identity`, with a shared 0.5 request
per second default. This configuration has produced valid live responses, but
does not guarantee that future requests will avoid a source restriction. The
current browser search route is `/public/search`; `/customer/search` redirected
home during this investigation. If denied again, stop and send the time,
URL and denial reference to [FMCSA support](https://www.fmcsa.dot.gov/registration/ask-fmcsa)
for investigation; do not keep issuing a blocked scan.

The one-off local resume runner (`scratch/resume_live_csv.py`) writes the original
eight-column CSV from live MOTUS responses. It prioritizes IDs in the downloaded
census snapshot, then checks every remaining number in the requested range. It
never treats a missing census ID as evidence of a missing MOTUS record. Each
response/row is checkpointed, and any unresolved request stops the run for review.

For bulk public data, FMCSA publishes the
[Company Census File](https://data.transportation.gov/Trucking-and-Motorcoaches/Company-Census-File/az4n-8mr2).
The official JSON/CSV API accepts a server-side range filter, avoiding individual
lookups for every possible number. The local file
`carriers_4582560_4592559_official_census.csv` contains the 154 published records
returned for that range, with provenance in its adjacent `.source.json` file.
This is a separate source snapshot, not a completed MOTUS scan. FMCSA documents
that census data is refreshed daily from a 24-hour-old database and excludes some
entity types; see the [Open Data Program](https://www.fmcsa.dot.gov/registration/fmcsa-data-dissemination-program).

### Direct CSV extraction and measured limits

`python_scraper.direct_export` writes the eight contact columns directly from MOTUS
carrier details, without Supabase or census data. It omits the equipment request,
which is unnecessary for this CSV, and overlaps requests through the shared rate
limiter. SQLite records successful and missing responses; rerunning the same command
repairs the CSV from the checkpoint and resumes unresolved work. A 403 stops the run
and is never recorded as a missing carrier. Keep output and checkpoint files together.

For previously confirmed IDs:

```powershell
python -m python_scraper.direct_export --ids-csv carriers_4582560_10000000.csv --output-csv direct_contacts.csv --state-db direct_contacts.sqlite3 --max-seconds 180
```

For candidates discovered from MOTUS public name search:

```powershell
python -m python_scraper.direct_export --query TRANSPORT --max-search-pages 5 --output-csv direct_search.csv --state-db direct_search.sqlite3 --max-new-checks 100 --max-seconds 120
```

Search filters candidates to USDOT 4,582,560–10,000,000 by default. Repeat `--query`
for additional names. Search summaries lack contact details, so each candidate still
requires a detail request. Name searches provide partial directory coverage, even
when every page of an individual query is retrieved. The adjacent `.source.json`
records query truncation, range coverage, timings and HTTP responses. Discovery time
is separate from detail extraction time and included in `total_elapsed_seconds`.
`--max-seconds` limits detail extraction; discovery is bounded by pages and retries.

Live checks on 2026-10-23 found:

| Test | Observed result |
| --- | --- |
| 60 previously confirmed carriers, 4 requests/second cap | 16.815 seconds, 60 HTTP 200, no retries |
| 100 previously confirmed carriers, 8 requests/second cap | 29.783 seconds, 2 recovered timeouts; slower throughput |
| Contiguous numeric scan at 4 requests/second | Stopped after 6.602 seconds on HTTP 403; incomplete |
| Five TRANSPORT search pages, then in-range details | 500 summaries in 13.236 seconds, 3 contacts in 1.226 seconds; no 403 |
| Subsequent 124-ID CSV verification | Stopped on HTTP 403 for USDOT 4,582,560; incomplete |

Known-carrier samples reused previously fetched IDs and may benefit from caching.
These short checks do not establish sustained throughput or a safe source rate.
The former four-request default was a tested tuning point, not a published allowance.
The direct exporter now shares the scheduled scraper's conservative **0.5
requests/second** default, including public search discovery. Explicit rate overrides
remain available; a successful short benchmark does not establish an allowed rate.
The full numeric range has **not** been completed directly, and the source's access
denials remain unresolved. Successful search/detail calls alongside denied calls do
not identify which source rule caused the denial. The bulk export's earlier
67-second result measures a different data source and does not apply here.

#### Follow-up verification on 2026-10-23

The previously denied `/api/carriers/4582560` returned HTTP 200 in a fresh check.
Two bounded runs with one worker and a 0.5 request/second cap then produced:

| Test | Observed result |
| --- | --- |
| Numeric range 4582560–4582579 | All 20 resolved in 66.754 seconds: 1 HTTP 200, 19 HTTP 404; one recovered timeout, no 403 |
| First 20 of the 124 previously confirmed IDs | 20 HTTP 200 in 38.806 seconds; no retries, timeouts or 403 |

The known-ID run used the corrected default rate without a command-line override.
Its manifest remains `paused` because only 20 of 124 candidates were requested.
Evidence is saved in `scratch/403_numeric_check.source.json` and
`scratch/403_known_id_check.source.json`, with their CSVs and SQLite checkpoints.
These checks demonstrate restored access for these samples; they do **not** prove
that rate caused the earlier denial or that the full range can run without denial.
The exact source restriction remains unidentified.

On a future 403, the shared client immediately stops queued traffic and records
`http.access_denial`: UTC time, requested URL, source date, content type and a decoded
denial reference when available. It reads at most 4 KiB for at most one second;
an unreadable denial page still stops the run without retrying. It does not record
response cookies or the response body. Include the reference in an FMCSA support
request. [Akamai's troubleshooting guidance](https://community.akamai.com/customers/s/article/Access-denied-to-various-sites-served-through-Akamai?language=en_US)
discusses client reputation as a possible cause; this is not a diagnosis of this
particular denial.

### Verification

Each run prints a JSON summary and saves it into `sync_runs.stats`: HTTP status
counts, requests, retries, timeouts, average request latency, database time/batches,
discovery/enrichment time, missing/existing counts, and saved leads/minute. Concurrent
request/discovery/enrichment seconds are summed work durations, not wall-clock shares.
`new_leads` counts completed carrier snapshots saved in that run, including replays.
Failed runs have status `failed`; a time-budget stop has status `paused`. With `--once`,
an error exits nonzero so Actions reports a failure.

```powershell
python -m unittest discover -s python_scraper/tests -v
python -m python_scraper.benchmark_pipeline
```

These commands use temporary files, a loopback HTTP server and an in-memory database.
They do not need Supabase credentials or issue requests to MOTUS. Do not discover
tests across the entire `python_scraper` folder: older `test_*.py` probe scripts there
can call production services.

The processing-pattern benchmark uses 100 synthetic carriers, identical local HTTP
fixtures, and simulated database request latency. One Windows/Python 3.14 run measured
8.060 seconds for the old serial enrichment/save pattern and 0.482 seconds for the
new pipeline, with 500 versus 36 simulated database requests. This isolates batching
and overlap; it bypasses source request pacing and does not predict production speed.
