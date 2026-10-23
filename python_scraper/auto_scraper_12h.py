#!/usr/bin/env python3
"""
LeadBase 12-Hour Automated Continuous Gap Scraper
============================================================================
Runs continuously every 12 hours:
1. Resumes the last committed chunk and looks up only that chunk's known carriers.
2. Discovers and enriches carriers concurrently with shared request pacing.
3. Saves replayable database batches while HTTP workers continue.
4. Records throughput, latency, HTTP statuses and failures in sync_runs.
5. Uses a frontier window after the first sweep, with periodic full gap scans.
"""

import os
import sys
import csv
import json
import time
import math
import argparse
import asyncio
import aiohttp
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

if __package__:
    from .scrape_pipeline import Checkpoint, RequestClient, blocking_call, existing_dots, process_chunk, save_batch
else:
    from scrape_pipeline import Checkpoint, RequestClient, blocking_call, existing_dots, process_chunk, save_batch

# ── Environment & Paths ───────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
WORKSPACE_ROOT = os.path.dirname(SCRIPT_DIR)

load_dotenv(os.path.join(WORKSPACE_ROOT, ".env"))
load_dotenv(os.path.join(SCRIPT_DIR, ".env"))
load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
DEFAULT_START_DOT   = 4582560
DEFAULT_END_DOT     = 10000000
CHUNK_SIZE          = 5000         # Scan in batches of 5,000 DOTs
CONCURRENCY         = 4            # In-flight ceiling; shared pacing controls traffic
REQUESTS_PER_SECOND = 0.5          # Conservative after observed MOTUS access denials
INTERVAL_HOURS      = 12           # Run every 12 hours
OUTPUT_CSV          = os.path.join(WORKSPACE_ROOT, "unadded_leads_from_4582560_v10.csv")
STATE_FILE          = os.path.join(WORKSPACE_ROOT, "motus_scraper_state_v10.json")

HEADERS = {
    "Accept":     "application/json",
    "Accept-Encoding": "identity",
}

FIELDS = [
    "USDOT Number", "Legal Business Name", "Business Telephone No.",
    "Business Email", "DOT Status", "Out of Service", "Update Date", "Create Date"
]

# ── Field Extractors ──────────────────────────────────────────────────────────
def get_phone(carrier):
    for p in (carrier.get("phoneNumbers") or []):
        ph = (p.get("phoneNumber") or "").strip()
        if ph:
            return ph
    return ""

def get_email(carrier):
    for e in (carrier.get("emailAddresses") or []):
        em = (e.get("emailAddress") or "").strip()
        if em:
            return em
    return ""

def get_legal_name(carrier):
    for n in (carrier.get("entityNames") or []):
        if n.get("nameType") == "Legal":
            return (n.get("entityName") or "").strip()
    return (carrier.get("entityName") or "").strip()

def get_dba_name(carrier):
    for n in (carrier.get("entityNames") or []):
        if n.get("nameType") == "DBA":
            return (n.get("entityName") or "").strip()
    return ""

def get_principal_address(carrier):
    locs = carrier.get("locations") or []
    for loc in locs:
        line1 = (loc.get("addressLine1") or "").strip()
        city = (loc.get("city") or "").strip()
        state = (loc.get("state") or "").strip()
        zip_code = (loc.get("zipCode") or "").strip()
        parts = [p for p in [line1, city, state, zip_code] if p]
        if parts:
            return ", ".join(parts)
    return ""

def get_dot_status(carrier):
    dn = carrier.get("entityDotNumber") or {}
    st = dn.get("dotNumberStatus") or {}
    return (st.get("dotNumberStatus") or st.get("status") or "").strip()

def get_dot_number(carrier):
    dot_obj = carrier.get("entityDotNumber") or {}
    return str(dot_obj.get("dotNumber") or carrier.get("entityId") or "")

def parse_dt(v):
    if not v or not str(v).strip():
        return None
    s = str(v).strip()
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except Exception:
        return None

def build_row(dot, carrier):
    return {
        "USDOT Number":           str(dot),
        "Legal Business Name":    get_legal_name(carrier),
        "Business Telephone No.": get_phone(carrier),
        "Business Email":         get_email(carrier),
        "DOT Status":             get_dot_status(carrier),
        "Out of Service":         str(carrier.get("outOfService", "")),
        "Update Date":            carrier.get("updateDate") or carrier.get("createDate") or "",
        "Create Date":            carrier.get("createDate") or "",
    }

def build_supabase_row(dot, carrier):
    create_date_raw = carrier.get("createDate") or carrier.get("updateDate")
    update_date_raw = carrier.get("updateDate") or carrier.get("createDate")
    create_date = parse_dt(create_date_raw)
    update_date = parse_dt(update_date_raw)
    now_iso = datetime.now(timezone.utc).isoformat()
    # The lookup key is authoritative; entityId is a different identifier.
    usdot = str(dot)
    
    ced = carrier.get("carrierEntityDetail") or {}
    business_type = (ced.get("businessType") or {}).get("businessTypeName", "") if isinstance(ced.get("businessType"), dict) else ""
    dun_no = str(ced.get("dunBradstreetNo") or "") if ced.get("dunBradstreetNo") not in (0, "0", None) else ""
    state_incorp = ced.get("stateOfIncorp") or ""
    
    row = {
        "usdot_number":       usdot,
        "legal_name":         get_legal_name(carrier),
        "phone":              get_phone(carrier),
        "email":              get_email(carrier),
        "carrier_status":     get_dot_status(carrier) or "Active",
        "out_of_service":     bool(carrier.get("outOfService") or False),
        "added_to_motus":     create_date,
        "motus_entry_date":   create_date,
        "motus_last_updated": update_date,
        "scraped_at":         now_iso,
        "profile_url":        f"https://motus.dot.gov/customer/{usdot}/account",
        "dba_name":           get_dba_name(carrier),
        "principal_address":  get_principal_address(carrier),
        "mailing_address":    "",
        "duns":               dun_no,
        "form_of_business":   business_type,
        "state_incorporated": state_incorp,
        "new_entrant_status": "",
        "raw_data":           carrier
    }
    if usdot.isdigit():
        row["usdot_number_num"] = int(usdot)
    return row

async def run_single_scrape(client, start_dot, end_dot, concurrency, output_csv, state_file,
                            *, requests_per_second=REQUESTS_PER_SECOND, batch_size=100, chunk_size=CHUNK_SIZE,
                            gap_days=7, frontier_window=10000, mirror_tables=True,
                            max_run_seconds=0):
    if start_dot < 1 or end_dot < start_dot:
        raise ValueError("Invalid USDOT range")
    if min(concurrency, batch_size, chunk_size, requests_per_second, gap_days, frontier_window) <= 0:
        raise ValueError("Concurrency, batch size, chunk size, rate and scan intervals must be positive")
    if not all(math.isfinite(value) for value in (requests_per_second, gap_days, max_run_seconds)) or max_run_seconds < 0:
        raise ValueError("Rate and run intervals must be finite, with a nonnegative time budget")
    range_key = f"{start_dot}-{end_dot}"
    durable_state = None
    if client:
        latest = await blocking_call(lambda: client.table("sync_runs").select("stats")
            .eq("run_type", "12h_schedule").eq("stats->>checkpoint_range", range_key)
            .order("id", desc=True).limit(1).execute())
        if latest.data:
            durable_state = latest.data[0]["stats"].get("checkpoint")
    # Prefer the write-ahead DB copy: an Actions cache can be stale or evicted.
    checkpoint = Checkpoint(state_file, start_dot, end_dot, initial_data=durable_state)
    checkpoint.prepare(gap_seconds=gap_days * 86400, frontier_window=frontier_window)
    checkpoint.save()
    started = time.monotonic()
    run_id = None
    metrics = {"start_dot": start_dot, "end_dot": end_dot,
               "concurrency": concurrency, "dots_probed": 0, "new_leads": 0,
               "missing": 0, "skipped_existing": 0, "database_seconds": 0.0,
               "database_batches": 0, "database_lookup_seconds": 0.0,
               "discovery_seconds": 0.0, "enrichment_seconds": 0.0}

    def run_stats():
        return {**metrics, "checkpoint_range": range_key,
                "checkpoint": json.loads(json.dumps(checkpoint.data))}

    if client:
        result = await blocking_call(lambda: client.table("sync_runs").insert({
            "run_type": "12h_schedule", "started_at": datetime.now(timezone.utc).isoformat(),
            "status": "running", "stats": run_stats()}).execute())
        if not result.data:
            raise RuntimeError("sync_runs did not return a run ID; cannot checkpoint safely")
        run_id = result.data[0]["id"]

    async def record_checkpoint():
        if client and run_id:
            snapshot = run_stats()
            result = await blocking_call(lambda: client.table("sync_runs").update({
                "stats": snapshot}).eq("id", run_id).execute())
            if not result.data:
                raise RuntimeError("Could not persist the database checkpoint")
    http = None
    status = "failed"
    error_summary = None
    try:
        # TLS verification is enabled; one session reuses connections for the run.
        connector = aiohttp.TCPConnector(limit=concurrency, ttl_dns_cache=300)
        async with aiohttp.ClientSession(headers=HEADERS, connector=connector) as session:
            http = RequestClient(session, requests_per_second=requests_per_second)
            file_exists = os.path.exists(output_csv) and os.path.getsize(output_csv) > 0
            with open(output_csv, "a", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=FIELDS, extrasaction="ignore")
                if not file_exists:
                    writer.writeheader()
                    csvfile.flush()
                while checkpoint.next_dot <= checkpoint.scan_end:
                    if max_run_seconds and time.monotonic() - started >= max_run_seconds:
                        status = "paused"
                        print("[Checkpoint] Time budget reached; next run will resume.", flush=True)
                        return metrics["new_leads"]
                    chunk_start = checkpoint.next_dot
                    chunk_end = min(chunk_start + chunk_size - 1, checkpoint.scan_end)
                    if checkpoint.pending is not None:
                        # Re-fetch pending IDs even if the carrier upsert succeeded:
                        # equipment or cargo may still be missing after a partial save.
                        dots = checkpoint.pending
                        chunk_end = checkpoint.data.get("pending_end") or chunk_end
                        # If the CSV was flushed but the checkpoint commit failed,
                        # remove that chunk's local rows before replaying it.
                        offset = checkpoint.data.get("csv_offset")
                        if (checkpoint.data.get("csv_path") == os.path.abspath(output_csv)
                                and offset is not None and os.path.getsize(output_csv) >= offset):
                            csvfile.flush()
                            csvfile.seek(offset)
                            csvfile.truncate()
                    else:
                        lookup_started = time.monotonic()
                        known = await blocking_call(existing_dots, client, chunk_start, chunk_end) if client else set()
                        metrics["database_lookup_seconds"] += time.monotonic() - lookup_started
                        metrics["skipped_existing"] += len(known)
                        if known:
                            checkpoint.data["frontier"] = max(checkpoint.data["frontier"], max(known))
                        dots = [dot for dot in range(chunk_start, chunk_end + 1) if dot not in known]
                        checkpoint.data.update(csv_offset=os.path.getsize(output_csv),
                                               csv_path=os.path.abspath(output_csv))
                        checkpoint.begin(dots, chunk_end=chunk_end)
                        # Persist candidate IDs BEFORE any carrier write. Even a
                        # killed runner can then repair partially saved children.
                        await record_checkpoint()
                    frontier = checkpoint.data["frontier"]
                    csv_rows = []

                    async def fetch(dot):
                        nonlocal frontier
                        t0 = time.monotonic()
                        carrier = await http.get_json(f"https://motus.dot.gov/api/carriers/{dot}")
                        metrics["discovery_seconds"] += time.monotonic() - t0
                        metrics["dots_probed"] += 1
                        if carrier is None:
                            metrics["missing"] += 1
                            return None
                        if not carrier or not carrier.get("entityId"):
                            raise ValueError(f"Carrier {dot} has no entityId; refusing to mark it saved")
                        frontier = max(frontier, dot)
                        t0 = time.monotonic()
                        matrix = await http.get_json(
                            f"https://motus.dot.gov/api/public-registration-matrix/{carrier['entityId']}")
                        metrics["enrichment_seconds"] += time.monotonic() - t0
                        if matrix is None or not isinstance(matrix.get("entity"), dict):
                            raise ValueError(f"Equipment response for {dot} is missing or invalid")
                        return dot, carrier, matrix

                    async def persist(batch):
                        t0 = time.monotonic()
                        if client:
                            await blocking_call(save_batch, client, batch, build_supabase_row, mirror_tables)
                        metrics["database_seconds"] += time.monotonic() - t0
                        metrics["database_batches"] += 1
                        csv_rows.extend(build_row(dot, carrier) for dot, carrier, _ in batch)
                        metrics["new_leads"] += len(batch)

                    remaining = max(0, max_run_seconds - (time.monotonic() - started)) if max_run_seconds else None
                    try:
                        async with asyncio.timeout(remaining):
                            await process_chunk(dots, fetch, persist, workers=concurrency, batch_size=batch_size)
                    except TimeoutError:
                        status = "paused"
                        print("[Checkpoint] Time budget reached within chunk; pending IDs retained.", flush=True)
                        return metrics["new_leads"]
                    # Write the local backup only after the entire chunk is durable.
                    writer.writerows(csv_rows)
                    csvfile.flush()
                    os.fsync(csvfile.fileno())
                    checkpoint.commit(chunk_end + 1, frontier)
                    await record_checkpoint()
                    elapsed = time.monotonic() - started
                    print(f"[Chunk] {chunk_start:,}-{chunk_end:,} | Saved: {metrics['new_leads']:,} | "
                          f"Probed: {metrics['dots_probed']:,} | "
                          f"{metrics['new_leads'] / max(elapsed / 60, .001):.1f} saved leads/min", flush=True)
            checkpoint.complete()
            status = "completed"
            return metrics["new_leads"]
    except BaseException as exc:
        error_summary = str(exc)
        raise
    finally:
        duration = time.monotonic() - started
        if http:
            metrics.update(http.metrics)
        metrics["duration_seconds"] = round(duration, 3)
        metrics["rate_dots_per_min"] = round(metrics["dots_probed"] / max(duration / 60, .001), 1)
        metrics["saved_leads_per_min"] = round(metrics["new_leads"] / max(duration / 60, .001), 1)
        metrics["avg_request_ms"] = round(1000 * metrics.get("request_seconds", 0) / max(metrics.get("requests", 0), 1), 1)
        if error_summary:
            metrics["error"] = error_summary[:1000]
        print(json.dumps({"status": status, **metrics}), flush=True)
        if client and run_id:
            try:
                await blocking_call(lambda: client.table("sync_runs").update({
                    "status": status, "completed_at": datetime.now(timezone.utc).isoformat(),
                    "duration_seconds": round(duration, 1), "stats": run_stats()}).eq("id", run_id).execute())
            except Exception as exc:
                print(f"[WARN] Could not finalize sync run: {type(exc).__name__}", flush=True)


# ── Main 12-Hour Infinite Runner ──────────────────────────────────────────────
async def main_scheduler():
    parser = argparse.ArgumentParser(description="LeadBase 12-Hour Automated Continuous Scraper")
    parser.add_argument("--interval", type=int, default=INTERVAL_HOURS, help=f"Hours between cycles (default: {INTERVAL_HOURS})")
    parser.add_argument("--start-dot", type=int, default=DEFAULT_START_DOT, help=f"Starting USDOT (default: {DEFAULT_START_DOT})")
    parser.add_argument("--end-dot", type=int, default=DEFAULT_END_DOT, help=f"Ending USDOT (default: {DEFAULT_END_DOT})")
    parser.add_argument("--concurrency", type=int, default=CONCURRENCY, help=f"Async HTTP concurrency (default: {CONCURRENCY})")
    parser.add_argument("--requests-per-second", type=float, default=REQUESTS_PER_SECOND, help=f"Shared MOTUS request budget, including enrichment (default: {REQUESTS_PER_SECOND})")
    parser.add_argument("--batch-size", type=int, default=100, help="Carriers per database write batch (default: 100)")
    parser.add_argument("--chunk-size", type=int, default=CHUNK_SIZE, help="USDOTs per durable checkpoint (default: 5000)")
    parser.add_argument("--state-file", default=STATE_FILE, help="Persistent checkpoint JSON path")
    parser.add_argument("--output-csv", default=OUTPUT_CSV, help="Local CSV backup path")
    parser.add_argument("--gap-days", type=float, default=7, help="Days between full gap scans after initial sweep")
    parser.add_argument("--frontier-window", type=int, default=10000, help="USDOTs beyond highest known carrier in incremental mode")
    parser.add_argument("--max-run-seconds", type=float, default=0, help="Pause after this budget, preserving unfinished work; 0 means unlimited")
    parser.add_argument("--canonical-only", action="store_true", help="Write only vehicles/cargo_classifications; opt out of legacy mirror tables")
    parser.add_argument("--once", action="store_true", help="Run a single scrape cycle and exit immediately (ideal for cron / GitHub Actions)")
    args = parser.parse_args()
    if (args.start_dot < 1 or args.end_dot < args.start_dot or
            min(args.interval, args.concurrency, args.batch_size, args.chunk_size,
                args.requests_per_second, args.gap_days, args.frontier_window) <= 0 or
            args.max_run_seconds < 0 or not all(math.isfinite(value) for value in
                (args.requests_per_second, args.gap_days, args.max_run_seconds))):
        parser.error("Provide a valid DOT range, positive tuning values and a finite nonnegative time budget")

    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY")
    if not url or not key:
        parser.error("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_ANON_KEY) are required")
    from supabase import create_client
    client = create_client(url, key)

    print("=" * 72, flush=True)
    print("LEADBASE AUTOMATED 12-HOUR SCRAPER SERVICE INITIALIZED", flush=True)
    print(f"Interval    : Every {args.interval} hours ({args.interval * 3600:,} seconds)")
    print(f"Target Range: USDOT {args.start_dot:,} to {args.end_dot:,}")
    print(f"Concurrency : {args.concurrency}")
    print(f"Request cap : {args.requests_per_second}/second, shared with equipment lookups")
    print(f"Batch size  : {args.batch_size}")
    print(f"Checkpoint  : {args.state_file}")
    print(f"Output File : {args.output_csv}")
    print("=" * 72, flush=True)

    cycle_count = 0
    while True:
        cycle_count += 1
        print(f"\n>>> Running Cycle #{cycle_count} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} <<<", flush=True)

        try:
            await run_single_scrape(
                client=client,
                start_dot=args.start_dot,
                end_dot=args.end_dot,
                concurrency=args.concurrency,
                output_csv=args.output_csv,
                state_file=args.state_file,
                requests_per_second=args.requests_per_second,
                batch_size=args.batch_size,
                chunk_size=args.chunk_size,
                gap_days=args.gap_days,
                frontier_window=args.frontier_window,
                mirror_tables=not args.canonical_only,
                max_run_seconds=args.max_run_seconds,
            )
        except Exception as e:
            print(f"[ERROR] Cycle #{cycle_count} encountered an error: {e}", flush=True)
            if args.once:
                raise

        if args.once:
            print("[*] Single-cycle execution (--once) complete. Exiting.", flush=True)
            break

        next_run_time = datetime.now() + timedelta(hours=args.interval)
        print(f"[*] Sleeping for {args.interval} hours... Next cycle starts at {next_run_time.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
        await asyncio.sleep(args.interval * 3600)

if __name__ == "__main__":
    try:
        asyncio.run(main_scheduler())
    except KeyboardInterrupt:
        print("\n[!] 12-Hour Scraper stopped by user. Exiting cleanly.", flush=True)
