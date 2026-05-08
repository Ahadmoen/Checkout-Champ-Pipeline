"""
CheckoutChamp -> BigQuery Pipeline  (FAST PARALLEL VERSION)
================================================================
  - Fetches orders via CheckoutChamp REST API (paginated)
  - Saves to Parquet (zstd compressed)
  - Appends to BigQuery table

Columns:
  Order_ID | Actual_Order_ID | Date_Created | Date_Updated
  Order_Type | Order_Status | Campaign_Name | First_Name
  Last_Name | Email_Address | Price | Custom_1 | Custom_2
  UTM_Source | UTM_Medium | UTM_Campaign | day

Speed features:
  - Parallel page fetching per day  (PAGE_WORKERS)
  - Parallel day processing         (DAY_WORKERS)
  - Shared requests.Session         (TCP keep-alive)
  - Single BQ client reused         (no reconnect per day)
================================================================
"""

# !pip install pyarrow google-cloud-bigquery requests --upgrade -q

import os
import json
import time
import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

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
    handlers=[logging.StreamHandler(), logging.FileHandler("cc_extraction.log")],
)
logger = logging.getLogger(__name__)

# -- CheckoutChamp credentials -----------------------------------
CC_LOGIN_ID      = os.environ["CC_LOGIN_ID"]
CC_PASSWORD      = os.environ["CC_PASSWORD"]
CC_API_BASE      = "https://api.checkoutchamp.com/order/query/"
RESULTS_PER_PAGE = 200

# -- Parallelism -------------------------------------------------
PAGE_WORKERS     = 3
DAY_WORKERS      = 3
MAX_CONCURRENT   = 9

# -- BigQuery config ---------------------------------------------
BQ_PROJECT = os.environ["BQ_PROJECT"]
BQ_DATASET = os.environ["BQ_DATASET"]
BQ_TABLE   = os.environ["BQ_TABLE"]

RAW_DIR = "./data/raw/"
os.makedirs(RAW_DIR, exist_ok=True)

# -- Service account loaded from env -----------------------------
SERVICE_ACCOUNT_INFO = json.loads(os.environ["GCP_SERVICE_ACCOUNT_JSON"])

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
    """Parse 'YYYY-MM-DD HH:MM:SS' or 'YYYY-MM-DD' -> date, else None."""
    if not s:
        return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except ValueError:
        return None

def cc_date(d: str) -> str:
    """Convert YYYY-MM-DD -> MM/DD/YYYY for CheckoutChamp API."""
    return datetime.strptime(d, "%Y-%m-%d").strftime("%m/%d/%Y")

def get_date_range(start: str, end: str) -> List[str]:
    """Return every date between start and end inclusive as YYYY-MM-DD strings."""
    s = datetime.strptime(start, "%Y-%m-%d").date()
    e = datetime.strptime(end,   "%Y-%m-%d").date()
    out, cur = [], s
    while cur <= e:
        out.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return out

# ================================================================
# ROW PROCESSING
# ================================================================
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
# FETCH  --  parallel page probing per day
# ================================================================
_session = requests.Session()
_session.mount('https://', requests.adapters.HTTPAdapter(pool_maxsize=10))

_api_semaphore = threading.Semaphore(MAX_CONCURRENT)

def fetch_page(day: str, page: int) -> List[Dict]:
    """Fetch one page for a given day. Retries up to 5x on failure."""
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
# SAVE TO PARQUET
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
    logger.info(f"Saved -> {path}")

# ================================================================
# BIGQUERY UPLOAD
# ================================================================
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
    logger.info(f"Uploaded to BQ: {table_id}")

# ================================================================
# ENTRY POINT
# ================================================================
def main():
    # -- Set date range here -------------------------------------
    START = "2026-02-01"
    END   = "2026-02-28"
    # For daily runs uncomment below:
    # yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    # START = END = yesterday
    # ------------------------------------------------------------

    dates = get_date_range(START, END)
    logger.info(f"Processing {len(dates)} days ({START} -> {END}) | "
                f"day_workers={DAY_WORKERS}  page_workers={PAGE_WORKERS}")

    bq_client   = make_bq_client()
    total_rows  = 0
    failed_days = []

    with ThreadPoolExecutor(max_workers=DAY_WORKERS) as ex:
        future_to_day = {ex.submit(fetch_day, day): day for day in dates}

        for i, future in enumerate(as_completed(future_to_day), 1):
            day = future_to_day[future]
            try:
                rows = future.result()
                if not rows:
                    logger.info(f"[{day}] ({i}/{len(dates)}) skipped — no orders")
                    continue

                seen = set()
                rows = [r for r in rows if r["Order_ID"] not in seen and not seen.add(r["Order_ID"]) or not r["Order_ID"]]
                path = os.path.join(RAW_DIR, f"cc_{day}.parquet")
                save_parquet(rows, path)
                upload_to_bq(path, bq_client)
                total_rows += len(rows)
                logger.info(f"[{day}] ({i}/{len(dates)}) OK — {len(rows)} rows")

            except Exception as exc:
                logger.error(f"[{day}] FAILED -> {exc}")
                failed_days.append(day)

    logger.info("=" * 60)
    logger.info(f"DONE.  Total rows: {total_rows}  |  Failed: {len(failed_days)}")
    if failed_days:
        logger.warning(f"Re-run these dates: {failed_days}")

if __name__ == "__main__":
    main()
