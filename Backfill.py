"""
CheckoutChamp -> BigQuery  BACKFILL SCRIPT
================================================================
Runs month-by-month from START to END.
- Tracks progress in a local file so it can RESUME if interrupted
- Skips already-completed months automatically
- Separate from the daily pipeline
================================================================
"""

# !pip install pyarrow google-cloud-bigquery requests --upgrade -q

import os
import time
import json
import logging
from datetime import datetime, timedelta, timezone, date
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import calendar
import threading

import requests
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import bigquery
from google.oauth2 import service_account

# ================================================================
# CONFIG
# ================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("cc_backfill.log")],
)
logger = logging.getLogger(__name__)

# -- Backfill date range -----------------------------------------
BACKFILL_START = "2024-01-01"   # YYYY-MM-DD  first day to backfill
BACKFILL_END   = "2024-09-30"   # YYYY-MM-DD  last day to backfill

# -- Progress tracking file (resume support) ---------------------
PROGRESS_FILE  = "cc_backfill_progress.json"

# -- CheckoutChamp credentials -----------------------------------
CC_LOGIN_ID      = "i1"
CC_PASSWORD      = "ktF8Qa3"
CC_API_BASE      = "https://y/"
RESULTS_PER_PAGE = 200

# -- Parallelism -------------------------------------------------
PAGE_WORKERS   = 3
DAY_WORKERS    = 3
MAX_CONCURRENT = 9

# -- BigQuery config ---------------------------------------------
BQ_PROJECT = "mythical-willow-431913-s4"
BQ_DATASET = "Test1"
BQ_TABLE   = "CheckoutChamp_Orders"

RAW_DIR = "./data/backfill/"
os.makedirs(RAW_DIR, exist_ok=True)

# -- Inline service account --------------------------------------
SERVICE_ACCOUNT_INFO = {
    "type": "service_account",
    "project_id": "mythical-willow-431913-s4",
    "private_key_id": "34cac7e0bde852591c96e29adba87d332d1022d9",
    "private_key": (
        "-----BEGIN PRIVATE KEY----CggEAaevgGaJNnTTKFiUJWfwoVSMuhoFCmqIzNzQgjNOiIHiY\nmxKclNHJIh0+CpeMtxuZIRTOaOeMBarY9PxtwA09tX0Z+H4S4hGfMLejAnoeLsiW\nw5R/b64xJG1/DaUDVX+MeT3YS6NkoqpPYrEGCkNlJfau3BTGolCnIuc4vboIw/FH\nfD3fCIx++/c2A9QCDJcaHHFgDx8H59OzuaqpiCaFrq1QNiqKhDcdrPPzlgXN5Wyj\nKLKC3LUaHSVpsskiBNRHFuWhXEc8wgWs0MHAJX9xticwmjxB+ecBqXv3sCeDVsdH\nh/b+xdWD+pNgc1U6IM2bP62lTw2SG1W/z/Bf2SK8mQKBgQD+UGXI9siR7n5V1rEi\nCEzvJnxAVX9owpfwFvEWG2Fm0fFSvjdCbjG5tdupPTAly18yZLL/RDqxCMBsy5+m\nN76amecFFRisFxjKLxMKFIfL2esly4wvP565Q+nTcBXFnmNY3ZY/69eRlGCE6myD\nX1YlEA/5t2WjDFPcHaUAD7JiuwKBgQDmHH3g0c6l/kVCxEEsto6gdACgeKW0be4Z\n4FPev7pOkbAM7OpSA0quEfvWHUgXVL0N6Y7JpFAa54X8LwXxHsml0Jo0MXMPMjhM\nCX+6crdgXuOPUekUpqbQ7fxXNKG66MydtjLZqajUz5Uq/zymlrPDzKhN5wk4FggM\ngZtpoPV7xQKBgERA5aBzA0+PN57oGPAuVB+XL5/AkopWN5r7PUcWoCSNUfxICuKs\nWnIiKcsZHfP2yhznQ9cYw7vBwoswdy+QJHqvtX36tH1zUXbp/W0mJ3ABk4e4Qm5n\n37yPSpExstYv9S/jgLC1JkzvCpyBog/8JU2bKv51RzTkWRlpZ2BF1jWDAoGAQKlg\n9fv/BcYd0FU1u0rRaWUvh+hfKAR8E+llqAJYaBuoTPmGHuWt5pxHGDPCPkwhk/c9\nmIwDtou4qtTL5qWwJFgp/OCoZGzIRRWmPs5dmUcQywVJafQqjCtT7W1sxQkF9ots\nXp2+Q47Ra/OtJ2LRwQORh9KUVJ5cRKdm9Je2Y8ECgYEAgRRiIJfDBdxY2phau0UW\nctt404cXNqhjWQ+5+kFeyFqeKDsT/L4WiXRinKsJtWGjT5/DNa/0xM/cz4vHyOe/\nmCsET9T8bIsvvdoch3WjhVEyFyLgwsYtaok7kP/sb/1P5LAPjZDkzIwmzhNqE198\nQAUDfLpoSu+ywe4OpQiDJ+8=\n-----END PRIVATE KEY-----\n"
    ),
    "client_email": "etl-473@mviceaccount.com",
    "client_id": "1128802866",
    "auth_uri": "https://accounm/o/oauth2/auth",
    "token_uri": "https://oauth2]/token",
    "auth_provider_x509_cert_url": "https://www.2/v1/certs",
    "client_x509_cert_url": (
        "https://www.googleapis.com/ra/x509/"
        "etl-473%40mythical-willow-431913-s4.iam.gserviceaccount.com"
    ),
    "universe_domain": "googleapis.com",
}

# ================================================================
# PROGRESS TRACKING  (resume support)
# ================================================================
def load_progress() -> dict:
    """Load completed months from progress file."""
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {"completed_months": [], "failed_months": []}

def save_progress(progress: dict) -> None:
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)

# ================================================================
# MONTH GENERATION
# ================================================================
def get_months(start: str, end: str) -> List[Dict]:
    """
    Generate list of month chunks between start and end.
    Each chunk: { "label": "2024-01", "start": "2024-01-01", "end": "2024-01-31" }
    The first and last months are clipped to the actual start/end dates.
    """
    s = datetime.strptime(start, "%Y-%m-%d").date()
    e = datetime.strptime(end,   "%Y-%m-%d").date()

    months = []
    cur = date(s.year, s.month, 1)

    while cur <= e:
        last_day = calendar.monthrange(cur.year, cur.month)[1]
        month_end = date(cur.year, cur.month, last_day)

        chunk_start = max(cur, s)
        chunk_end   = min(month_end, e)

        months.append({
            "label": cur.strftime("%Y-%m"),
            "start": chunk_start.strftime("%Y-%m-%d"),
            "end":   chunk_end.strftime("%Y-%m-%d"),
        })

        # Advance to first day of next month
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)

    return months

def get_date_range(start: str, end: str) -> List[str]:
    s = datetime.strptime(start, "%Y-%m-%d").date()
    e = datetime.strptime(end,   "%Y-%m-%d").date()
    out, cur = [], s
    while cur <= e:
        out.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return out

# ================================================================
# HELPERS
# ================================================================
def safe_str(v) -> str:
    return str(v).strip() if v is not None else ""

def safe_float(v, default: float = 0.0) -> float:
    try:
        return float(v) if v not in (None, "") else default
    except (ValueError, TypeError):
        return default

def safe_date(s: Optional[str]):
    if not s:
        return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except ValueError:
        return None

def cc_date(d: str) -> str:
    return datetime.strptime(d, "%Y-%m-%d").strftime("%m/%d/%Y")

def process_order(raw: Dict) -> Dict:
    return {
        "Order_ID":        safe_str(raw.get("orderId")),
        "Actual_Order_ID": safe_str(raw.get("actualOrderId")),
        "Date_Created":    safe_str(raw.get("dateCreated")),
        "Date_Updated":    safe_str(raw.get("dateUpdated")),
        "Order_Type":      safe_str(raw.get("orderType")),
        "Order_Status":    safe_str(raw.get("orderStatus")),
        "Campaign_Name":   safe_str(raw.get("campaignName")),
        "First_Name":      safe_str(raw.get("firstName")),
        "Last_Name":       safe_str(raw.get("lastName")),
        "Email_Address":   safe_str(raw.get("emailAddress")),
        "Price":           str(safe_float(raw.get("price"))),
        "Custom_1":        safe_str(raw.get("custom1")),
        "Custom_2":        safe_str(raw.get("custom2")),
        "UTM_Source":      safe_str(raw.get("UTMSource")),
        "UTM_Medium":      safe_str(raw.get("UTMMedium")),
        "UTM_Campaign":    safe_str(raw.get("UTMCampaign")),
        "day":             safe_date(raw.get("dateCreated")),
    }

# ================================================================
# FETCH
# ================================================================
_session = requests.Session()
_session.mount("https://", requests.adapters.HTTPAdapter(pool_maxsize=10))
_api_semaphore = threading.Semaphore(MAX_CONCURRENT)

def fetch_page(day: str, page: int) -> List[Dict]:
    params = {
        "loginId":        CC_LOGIN_ID,
        "password":       CC_PASSWORD,
        "startDate":      cc_date(day),
        "endDate":        cc_date(day),
        "resultsPerPage": RESULTS_PER_PAGE,
        "page":           page,
    }
    for attempt in range(1, 6):
        try:
            with _api_semaphore:
                resp = _session.post(CC_API_BASE, params=params, timeout=30)
            resp.raise_for_status()
            body = resp.json()
            if body.get("result") != "SUCCESS":
                raise ValueError(f"API error: {body}")
            data = body.get("message", {}).get("data", [])
            return data if isinstance(data, list) else []
        except Exception as exc:
            if attempt == 5:
                raise
            wait = min(2 ** attempt, 30)
            logger.warning(f"[{day}] page {page} attempt {attempt} failed ({exc}), retry in {wait}s")
            time.sleep(wait)
    return []

def fetch_day(day: str) -> List[Dict]:
    p1 = fetch_page(day, 1)
    if not p1:
        return []

    all_raw = list(p1)

    if len(p1) < RESULTS_PER_PAGE:
        return [process_order(o) for o in all_raw]

    page = 2
    while True:
        batch_pages = list(range(page, page + PAGE_WORKERS))
        results = {}
        with ThreadPoolExecutor(max_workers=PAGE_WORKERS) as ex:
            futures = {ex.submit(fetch_page, day, p): p for p in batch_pages}
            for f in as_completed(futures):
                results[futures[f]] = f.result()

        done = False
        for p in sorted(batch_pages):
            batch = results[p]
            all_raw.extend(batch)
            if len(batch) < RESULTS_PER_PAGE:
                done = True
                break
        if done:
            break
        page += PAGE_WORKERS

    # Deduplicate by orderId
    seen_ids = set()
    unique_raw = []
    for o in all_raw:
        oid = o.get("orderId", "")
        if oid and oid not in seen_ids:
            seen_ids.add(oid)
            unique_raw.append(o)
        elif not oid:
            unique_raw.append(o)
    return [process_order(o) for o in unique_raw]

# ================================================================
# PARQUET + BQ
# ================================================================
def save_parquet(rows: List[Dict], path: str) -> None:
    table = pa.table({
        "Order_ID":        [r["Order_ID"]        for r in rows],
        "Actual_Order_ID": [r["Actual_Order_ID"] for r in rows],
        "Date_Created":    [r["Date_Created"]    for r in rows],
        "Date_Updated":    [r["Date_Updated"]    for r in rows],
        "Order_Type":      [r["Order_Type"]      for r in rows],
        "Order_Status":    [r["Order_Status"]    for r in rows],
        "Campaign_Name":   [r["Campaign_Name"]   for r in rows],
        "First_Name":      [r["First_Name"]      for r in rows],
        "Last_Name":       [r["Last_Name"]       for r in rows],
        "Email_Address":   [r["Email_Address"]   for r in rows],
        "Price":           [r["Price"]           for r in rows],
        "Custom_1":        [r["Custom_1"]        for r in rows],
        "Custom_2":        [r["Custom_2"]        for r in rows],
        "UTM_Source":      [r["UTM_Source"]      for r in rows],
        "UTM_Medium":      [r["UTM_Medium"]      for r in rows],
        "UTM_Campaign":    [r["UTM_Campaign"]    for r in rows],
        "day":             [r["day"]             for r in rows],
    })
    pq.write_table(table, path, compression="zstd")

def make_bq_client() -> bigquery.Client:
    creds = service_account.Credentials.from_service_account_info(
        SERVICE_ACCOUNT_INFO,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return bigquery.Client(project=BQ_PROJECT, credentials=creds)

def upload_to_bq(path: str, client: bigquery.Client) -> None:
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    cfg = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition="WRITE_APPEND",
    )
    with open(path, "rb") as f:
        job = client.load_table_from_file(f, table_id, job_config=cfg)
        job.result()

# ================================================================
# PROCESS ONE MONTH
# ================================================================
def process_month(month: Dict, bq_client: bigquery.Client) -> int:
    """Fetch all days in a month, combine into one parquet, upload once. Returns row count."""
    label      = month["label"]
    days       = get_date_range(month["start"], month["end"])
    all_rows   = []
    failed_days = []

    logger.info(f"  [{label}] Fetching {len(days)} days in parallel...")

    with ThreadPoolExecutor(max_workers=DAY_WORKERS) as ex:
        future_to_day = {ex.submit(fetch_day, day): day for day in days}
        for future in as_completed(future_to_day):
            day = future_to_day[future]
            try:
                rows = future.result()
                all_rows.extend(rows)
                if rows:
                    logger.info(f"  [{label}] {day} -> {len(rows)} rows")
            except Exception as exc:
                logger.error(f"  [{label}] {day} FAILED -> {exc}")
                failed_days.append(day)

    if failed_days:
        raise RuntimeError(f"Failed days in {label}: {failed_days}")

    if not all_rows:
        logger.info(f"  [{label}] No orders found — skipping upload")
        return 0

    # Final dedup across the whole month
    seen = set()
    deduped = []
    for r in all_rows:
        oid = r["Order_ID"]
        if oid and oid not in seen:
            seen.add(oid)
            deduped.append(r)
        elif not oid:
            deduped.append(r)

    logger.info(f"  [{label}] {len(all_rows)} raw -> {len(deduped)} after dedup")

    path = os.path.join(RAW_DIR, f"cc_backfill_{label}.parquet")
    save_parquet(deduped, path)
    upload_to_bq(path, bq_client)
    logger.info(f"  [{label}] Uploaded {len(deduped)} rows to BQ")
    return len(deduped)

# ================================================================
# MAIN
# ================================================================
def main():
    months   = get_months(BACKFILL_START, BACKFILL_END)
    progress = load_progress()
    done_set = set(progress["completed_months"])

    total_months  = len(months)
    pending       = [m for m in months if m["label"] not in done_set]
    already_done  = total_months - len(pending)

    logger.info("=" * 60)
    logger.info(f"BACKFILL: {BACKFILL_START} -> {BACKFILL_END}")
    logger.info(f"Total months : {total_months}")
    logger.info(f"Already done : {already_done}  (from {PROGRESS_FILE})")
    logger.info(f"Remaining    : {len(pending)}")
    logger.info("=" * 60)

    if not pending:
        logger.info("All months already completed!")
        return

    bq_client  = make_bq_client()
    total_rows = 0

    for i, month in enumerate(pending, 1):
        label = month["label"]
        logger.info(f"\n[{i}/{len(pending)}] Processing {label}  "
                    f"({month['start']} -> {month['end']})")
        try:
            count = process_month(month, bq_client)
            total_rows += count
            progress["completed_months"].append(label)
            # Remove from failed if it was there before
            progress["failed_months"] = [m for m in progress["failed_months"] if m != label]
            save_progress(progress)
            logger.info(f"[{i}/{len(pending)}] {label} DONE — {count} rows  "
                        f"(total so far: {total_rows})")

        except Exception as exc:
            logger.error(f"[{i}/{len(pending)}] {label} FAILED -> {exc}")
            if label not in progress["failed_months"]:
                progress["failed_months"].append(label)
            save_progress(progress)
            # Continue to next month instead of aborting
            continue

        # Small pause between months
        time.sleep(2)

    logger.info("\n" + "=" * 60)
    logger.info(f"BACKFILL COMPLETE")
    logger.info(f"Total rows uploaded : {total_rows}")
    logger.info(f"Completed months    : {len(progress['completed_months'])}")
    if progress["failed_months"]:
        logger.warning(f"Failed months (re-run to retry): {progress['failed_months']}")
    else:
        logger.info("No failures!")

if __name__ == "__main__":
    main()
