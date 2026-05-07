"""
CheckoutChamp → BigQuery Pipeline
══════════════════════════════════════════════════════════════════
Mirrors the Meta Ads pipeline structure:
  • Fetches orders via CheckoutChamp REST API (paginated)
  • Saves to Parquet (zstd compressed)
  • Appends to BigQuery table

Columns:
  Order_ID | Actual_Order_ID | Date_Created | Date_Updated
  Order_Type | Order_Status | Campaign_Name | First_Name
  Last_Name | Email_Address | Price | Custom_1 | Custom_2
  UTM_Source | UTM_Medium | UTM_Campaign | day (partition)
══════════════════════════════════════════════════════════════════
"""

# ── Install dependencies (run once in Colab / notebook) ──────────
!pip install pyarrow google-cloud-bigquery --upgrade -q

import os
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import bigquery
from google.oauth2 import service_account

# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("cc_extraction.log")],
)
logger = logging.getLogger(__name__)

# ── CheckoutChamp credentials ─────────────────────────────────
CC_LOGIN_ID      = os.environ["CC_LOGIN_ID"]
CC_PASSWORD      = os.environ["CC_PASSWORD"]
CC_API_BASE      = "https://api.checkoutchamp.com/order/query/"
RESULTS_PER_PAGE = 200

# ── BigQuery config ───────────────────────────────────────────
BQ_PROJECT = os.environ["BQ_PROJECT"]
BQ_DATASET = os.environ["BQ_DATASET"]
BQ_TABLE   = os.environ["BQ_TABLE"]

RAW_DIR = "./data/raw/"
os.makedirs(RAW_DIR, exist_ok=True)

# ── Service account loaded from env or file ───────────────────
import json
SERVICE_ACCOUNT_INFO = json.loads(os.environ["GCP_SERVICE_ACCOUNT_JSON"])

# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════
def safe_str(v) -> str:
    return str(v).strip() if v is not None else ""

def safe_float(v, default: float = 0.0) -> float:
    try:
        return float(v) if v not in (None, "") else default
    except (ValueError, TypeError):
        return default

def safe_date(s: Optional[str]):
    """Parse 'YYYY-MM-DD HH:MM:SS' or 'YYYY-MM-DD' → date, else None."""
    if not s:
        return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except ValueError:
        return None

def cc_date(d: str) -> str:
    """Convert YYYY-MM-DD → MM/DD/YYYY for CheckoutChamp API."""
    return datetime.strptime(d, "%Y-%m-%d").strftime("%m/%d/%Y")

# ═══════════════════════════════════════════════════════════════
# ROW PROCESSING  (mirrors process_row in Meta pipeline)
# ═══════════════════════════════════════════════════════════════
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

# ═══════════════════════════════════════════════════════════════
# FETCH WITH RETRY  (mirrors @retry + get_insights)
# ═══════════════════════════════════════════════════════════════
def fetch_page(start_date: str, end_date: str, page: int) -> List[Dict]:
    """Fetch one page from the CC API. Retries up to 5× on transient errors."""
    params = {
        "loginId":        CC_LOGIN_ID,
        "password":       CC_PASSWORD,
        "startDate":      cc_date(start_date),
        "endDate":        cc_date(end_date),
        "resultsPerPage": RESULTS_PER_PAGE,
        "page":           page,
    }
    for attempt in range(1, 6):
        try:
            resp = requests.post(CC_API_BASE, params=params, timeout=30)
            resp.raise_for_status()
            body = resp.json()
            if body.get("result") != "SUCCESS":
                raise ValueError(f"API error: {body}")
            data = body.get("message", {}).get("data", [])
            return data if isinstance(data, list) else []
        except (requests.RequestException, ValueError) as exc:
            if attempt == 5:
                raise
            wait = min(2 ** attempt, 60)
            logger.warning(f"Page {page} attempt {attempt} failed ({exc}), retrying in {wait}s…")
            time.sleep(wait)
    return []

def fetch_all_orders(start: str, end: str) -> List[Dict]:
    """Paginate through all CC orders for the date range."""
    logger.info(f"Fetching CheckoutChamp orders {start} → {end}")
    rows: List[Dict] = []
    page = 1

    while True:
        logger.info(f"  → page {page}")
        raw_orders = fetch_page(start, end, page)

        if not raw_orders:
            logger.info(f"  ✓ No more orders on page {page}")
            break

        rows.extend(process_order(o) for o in raw_orders)
        logger.info(f"  ✓ Page {page}: {len(raw_orders)} orders")

        if len(raw_orders) < RESULTS_PER_PAGE:
            break

        page += 1
        time.sleep(0.5)

    logger.info(f"Total orders fetched: {len(rows)}")
    return rows

# ═══════════════════════════════════════════════════════════════
# SAVE TO PARQUET  (mirrors save_parquet)
# ═══════════════════════════════════════════════════════════════
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
    logger.info(f"Saved → {path}")

# ═══════════════════════════════════════════════════════════════
# BIGQUERY UPLOAD  (mirrors BQ block in main())
# ═══════════════════════════════════════════════════════════════
def upload_to_bq(path: str) -> None:
    credentials = service_account.Credentials.from_service_account_info(
        SERVICE_ACCOUNT_INFO,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client   = bigquery.Client(project=BQ_PROJECT, credentials=credentials)
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    cfg      = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition="WRITE_APPEND",
    )
    with open(path, "rb") as f:
        job = client.load_table_from_file(f, table_id, job_config=cfg)
        job.result()
    logger.info(f"Uploaded to BQ table: {table_id}")

# ═══════════════════════════════════════════════════════════════
# ENTRY POINT  (mirrors main())
# ═══════════════════════════════════════════════════════════════
def main():
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    START = yesterday
    END   = yesterday

    # ── Override here for backfills ──────────────────────────
    # START = "2026-04-01"
    # END   = "2026-04-30"
    # ────────────────────────────────────────────────────────

    rows = fetch_all_orders(START, END)

    if not rows:
        logger.warning("No orders returned — nothing to upload.")
        return

    path = os.path.join(RAW_DIR, f"cc_orders_{START}_{END}.parquet")
    save_parquet(rows, path)
    upload_to_bq(path)
    logger.info("ALL DONE!")


if __name__ == "__main__":
    main()
