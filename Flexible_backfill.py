"""
CheckoutChamp -> BigQuery  FLEXIBLE SCRIPT
================================================================
Fetches orders for a specified date range and uploads to BigQuery
Can run for: single day, date range, or yesterday (default)
================================================================
"""

!pip install pyarrow google-cloud-bigquery requests --upgrade -q

import os
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

import requests
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import bigquery
from google.oauth2 import service_account

# ================================================================
# CONFIG - CHANGE DATES HERE
# ================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("cc_fetch.log")],
)
logger = logging.getLogger(__name__)

# ================================================================
# DATE RANGE CONFIGURATION - CHOOSE ONE OPTION:
# ================================================================

# OPTION 1: Fetch yesterday only (default)
#MODE = "YESTERDAY"

# OPTION 2: Fetch specific date range (uncomment and set dates)
MODE = "RANGE"
START_DATE = "2026-01-01"  # YYYY-MM-DD
END_DATE = "2026-01-31"    # YYYY-MM-DD

# OPTION 3: Fetch single specific date (uncomment and set date)
# MODE = "SINGLE"
# SINGLE_DATE = "2024-05-15"  # YYYY-MM-DD

# ================================================================

# -- CheckoutChamp credentials -----------------------------------
CC_LOGIN_ID      = "danishapi1"
CC_PASSWORD      = "ktafyr*XbHF8Qa3"
CC_API_BASE      = "https://api.checkoutchamp.com/order/query/"
RESULTS_PER_PAGE = 200

# -- Parallelism -------------------------------------------------
PAGE_WORKERS   = 3
DAY_WORKERS    = 3  # For multi-day ranges
MAX_CONCURRENT = 9

# -- BigQuery config ---------------------------------------------
BQ_PROJECT = "mythical-willow-431913-s4"
BQ_DATASET = "Test1"
BQ_TABLE   = "CheckoutChamp_Orders"

RAW_DIR = "./data/fetch/"
os.makedirs(RAW_DIR, exist_ok=True)

# -- Inline service account --------------------------------------
SERVICE_ACCOUNT_INFO = {
    "type": "service_account",
    "project_id": "mythical-willow-431913-s4",
    "private_key_id": "34cac7e0bde852591c96e29adba87d332d1022d9",
    "private_key": (
        "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDkmIlKSCxOFX21\n7MIOF1ZlLNqxlr/0cMcqvnhOmtH/TEbyIC8AhHAnkxVEnCe+2qGC9nG85SyqPzK0\nzksVXLAllrPDImGsbV4CEF6A6EZIIqTyYQX6gBhvmrtlbKGlIT10VMDcofagSHMY\n+222jSIt148O/ofe0A2Er51xKkgYTbm/Y+oKx9tW/qQfCiFcw9xzCd59uuKfrCo7\nPKeGKFPAksWLCQlvhMRpi/v37IQRST+EHWEkkJ7SfLUsRiazpXYrIfB2j5WhgBV1\nHYlVYO/+PfNMwj5C06K7H3FdjPv5WJDPV0gTO2fVMnxrKbHW4OFYR8uiIgQarYoi\nO7cmrNLnAgMBAAECggEAaevgGaJNnTTKFiUJWfwoVSMuhoFCmqIzNzQgjNOiIHiY\nmxKclNHJIh0+CpeMtxuZIRTOaOeMBarY9PxtwA09tX0Z+H4S4hGfMLejAnoeLsiW\nw5R/b64xJG1/DaUDVX+MeT3YS6NkoqpPYrEGCkNlJfau3BTGolCnIuc4vboIw/FH\nfD3fCIx++/c2A9QCDJcaHHFgDx8H59OzuaqpiCaFrq1QNiqKhDcdrPPzlgXN5Wyj\nKLKC3LUaHSVpsskiBNRHFuWhXEc8wgWs0MHAJX9xticwmjxB+ecBqXv3sCeDVsdH\nh/b+xdWD+pNgc1U6IM2bP62lTw2SG1W/z/Bf2SK8mQKBgQD+UGXI9siR7n5V1rEi\nCEzvJnxAVX9owpfwFvEWG2Fm0fFSvjdCbjG5tdupPTAly18yZLL/RDqxCMBsy5+m\nN76amecFFRisFxjKLxMKFIfL2esly4wvP565Q+nTcBXFnmNY3ZY/69eRlGCE6myD\nX1YlEA/5t2WjDFPcHaUAD7JiuwKBgQDmHH3g0c6l/kVCxEEsto6gdACgeKW0be4Z\n4FPev7pOkbAM7OpSA0quEfvWHUgXVL0N6Y7JpFAa54X8LwXxHsml0Jo0MXMPMjhM\nCX+6crdgXuOPUekUpqbQ7fxXNKG66MydtjLZqajUz5Uq/zymlrPDzKhN5wk4FggM\ngZtpoPV7xQKBgERA5aBzA0+PN57oGPAuVB+XL5/AkopWN5r7PUcWoCSNUfxICuKs\nWnIiKcsZHfP2yhznQ9cYw7vBwoswdy+QJHqvtX36tH1zUXbp/W0mJ3ABk4e4Qm5n\n37yPSpExstYv9S/jgLC1JkzvCpyBog/8JU2bKv51RzTkWRlpZ2BF1jWDAoGAQKlg\n9fv/BcYd0FU1u0rRaWUvh+hfKAR8E+llqAJYaBuoTPmGHuWt5pxHGDPCPkwhk/c9\nmIwDtou4qtTL5qWwJFgp/OCoZGzIRRWmPs5dmUcQywVJafQqjCtT7W1sxQkF9ots\nXp2+Q47Ra/OtJ2LRwQORh9KUVJ5cRKdm9Je2Y8ECgYEAgRRiIJfDBdxY2phau0UW\nctt404cXNqhjWQ+5+kFeyFqeKDsT/L4WiXRinKsJtWGjT5/DNa/0xM/cz4vHyOe/\nmCsET9T8bIsvvdoch3WjhVEyFyLgwsYtaok7kP/sb/1P5LAPjZDkzIwmzhNqE198\nQAUDfLpoSu+ywe4OpQiDJ+8=\n-----END PRIVATE KEY-----\n"
    ),
    "client_email": "etl-473@mythical-willow-431913-s4.iam.gserviceaccount.com",
    "client_id": "112463139188528802866",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": (
        "https://www.googleapis.com/robot/v1/metadata/x509/"
        "etl-473%40mythical-willow-431913-s4.iam.gserviceaccount.com"
    ),
    "universe_domain": "googleapis.com",
}

# ================================================================
# DATE RANGE BUILDER
# ================================================================
def get_date_range() -> List[str]:
    """Returns list of dates to fetch based on MODE configuration."""
    if MODE == "YESTERDAY":
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
        return [yesterday]

    elif MODE == "SINGLE":
        return [SINGLE_DATE]

    elif MODE == "RANGE":
        start = datetime.strptime(START_DATE, "%Y-%m-%d").date()
        end = datetime.strptime(END_DATE, "%Y-%m-%d").date()
        dates = []
        current = start
        while current <= end:
            dates.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)
        return dates

    else:
        raise ValueError(f"Invalid MODE: {MODE}. Use 'YESTERDAY', 'SINGLE', or 'RANGE'")

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
    """Convert YYYY-MM-DD to MM/DD/YYYY for CheckoutChamp API."""
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
    """Fetch all orders for a single day with pagination."""
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
# MAIN
# ================================================================
def main():
    # Get date range based on MODE
    dates = get_date_range()

    logger.info("=" * 60)
    logger.info(f"MODE: {MODE}")
    if MODE == "RANGE":
        logger.info(f"Date range: {dates[0]} to {dates[-1]} ({len(dates)} days)")
    else:
        logger.info(f"Target date(s): {', '.join(dates)}")
    logger.info("=" * 60)

    bq_client = make_bq_client()
    total_rows = 0
    failed_dates = []

    # Single date processing
    if len(dates) == 1:
        day = dates[0]
        try:
            logger.info(f"Fetching data for {day}...")
            rows = fetch_day(day)

            if not rows:
                logger.info(f"No orders found for {day}")
                logger.info("=" * 60)
                logger.info("COMPLETE - No data to upload")
                return

            logger.info(f"Fetched {len(rows)} rows for {day}")

            path = os.path.join(RAW_DIR, f"cc_{day}.parquet")
            save_parquet(rows, path)
            logger.info(f"Saved to: {path}")

            upload_to_bq(path, bq_client)
            logger.info(f"Successfully uploaded {len(rows)} rows to BigQuery")
            total_rows = len(rows)

        except Exception as exc:
            logger.error(f"Failed to process {day}: {exc}")
            raise

    # Multi-date processing (parallel)
    else:
        logger.info(f"Fetching {len(dates)} days in parallel...")

        with ThreadPoolExecutor(max_workers=DAY_WORKERS) as ex:
            future_to_day = {ex.submit(fetch_day, day): day for day in dates}
            results = {}

            for future in as_completed(future_to_day):
                day = future_to_day[future]
                try:
                    rows = future.result()
                    results[day] = rows
                    if rows:
                        logger.info(f"  {day} -> {len(rows)} rows")
                    else:
                        logger.info(f"  {day} -> 0 rows")
                except Exception as exc:
                    logger.error(f"  {day} FAILED -> {exc}")
                    failed_dates.append(day)
                    results[day] = []

        # Combine all results
        all_rows = []
        for day in sorted(results.keys()):
            all_rows.extend(results[day])

        if not all_rows:
            logger.info("No orders found for any date in range")
            logger.info("=" * 60)
            logger.info("COMPLETE - No data to upload")
            return

        # Global deduplication
        seen = set()
        deduped = []
        for r in all_rows:
            oid = r["Order_ID"]
            if oid and oid not in seen:
                seen.add(oid)
                deduped.append(r)
            elif not oid:
                deduped.append(r)

        logger.info(f"Total: {len(all_rows)} raw -> {len(deduped)} after dedup")

        # Save and upload
        filename = f"cc_{dates[0]}_to_{dates[-1]}.parquet"
        path = os.path.join(RAW_DIR, filename)
        save_parquet(deduped, path)
        logger.info(f"Saved to: {path}")

        upload_to_bq(path, bq_client)
        logger.info(f"Successfully uploaded {len(deduped)} rows to BigQuery")
        total_rows = len(deduped)

    logger.info("=" * 60)
    logger.info(f"COMPLETE")
    logger.info(f"Total rows uploaded: {total_rows}")
    if failed_dates:
        logger.warning(f"Failed dates: {', '.join(failed_dates)}")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()
