# CheckoutChamp → BigQuery Pipelines

Three scripts that pull order data from the CheckoutChamp REST API, save it as compressed Parquet, and load it into BigQuery.

---

## Scripts at a glance

| Script | Purpose | When to use |
|--------|---------|-------------|
| `daily_pipeline.py` | Fetches one day (yesterday by default) with full parallelism | Scheduled daily runs |
| `flexible_backfill.py` | Fetches yesterday / single date / date range | Ad-hoc fills, re-runs, testing |
| `backfill.py` | Fetches a long range month-by-month with resume support | Initial historical load |

---

## How it works (all three scripts share the same flow)

```
1. Build date list      →  one date (daily) or a range (backfill)
2. Fetch page 1         →  POST to CheckoutChamp API
3. Parallel pagination  →  fetch remaining pages in parallel (PAGE_WORKERS)
4. Deduplicate          →  remove duplicate Order_IDs within each day
5. Save Parquet         →  zstd-compressed, written to ./data/raw/ (or ./data/backfill/)
6. Upload to BigQuery   →  WRITE_APPEND load job from Parquet file
7. Log progress         →  console + .log file
```

For multi-day runs, steps 2–5 run in parallel across days (DAY_WORKERS).

---

## Requirements

```
pyarrow
google-cloud-bigquery
google-auth
requests
```

```bash
pip install pyarrow google-cloud-bigquery google-auth requests --upgrade
```

---

## Setup

### Environment variables

All credentials should come from environment variables. Never hardcode them in source files.

| Variable | Required | Description |
|----------|:--------:|-------------|
| `CC_LOGIN_ID` | ✅ | CheckoutChamp API login ID |
| `CC_PASSWORD` | ✅ | CheckoutChamp API password |
| `BQ_PROJECT` | ✅ | GCP project ID |
| `BQ_DATASET` | ✅ | BigQuery dataset name |
| `BQ_TABLE` | ✅ | BigQuery table name |
| `GCP_SERVICE_ACCOUNT_JSON` | ✅ | Full service account JSON as a string |

Set locally:

```bash
export CC_LOGIN_ID="your-login-id"
export CC_PASSWORD="your-password"
export BQ_PROJECT="your-gcp-project"
export BQ_DATASET="your_dataset"
export BQ_TABLE="CheckoutChamp_Orders"
export GCP_SERVICE_ACCOUNT_JSON='{"type":"service_account","project_id":"..."}'
```

Or use a `.env` file (never commit it to git):

```bash
# .env
CC_LOGIN_ID=your-login-id
CC_PASSWORD=your-password
BQ_PROJECT=your-gcp-project
BQ_DATASET=your_dataset
BQ_TABLE=CheckoutChamp_Orders
GCP_SERVICE_ACCOUNT_JSON={"type":"service_account",...}
```

---

## BigQuery table schema

| Column | Type | Description |
|--------|------|-------------|
| `Order_ID` | STRING | CheckoutChamp order ID |
| `Actual_Order_ID` | STRING | Actual order ID |
| `Date_Created` | STRING | Order creation timestamp |
| `Date_Updated` | STRING | Last update timestamp |
| `Order_Type` | STRING | Type of order |
| `Order_Status` | STRING | Current order status |
| `Campaign_Name` | STRING | Campaign the order came from |
| `First_Name` | STRING | Customer first name |
| `Last_Name` | STRING | Customer last name |
| `Email_Address` | STRING | Customer email |
| `Price` | STRING | Order price |
| `Custom_1` | STRING | Custom field 1 |
| `Custom_2` | STRING | Custom field 2 |
| `UTM_Source` | STRING | UTM source |
| `UTM_Medium` | STRING | UTM medium |
| `UTM_Campaign` | STRING | UTM campaign |
| `day` | DATE | Parsed date from `Date_Created` (partition key) |

> **Note:** The table must exist in BigQuery before running. All uploads use `WRITE_APPEND` — existing data is never deleted.

---

## Script details

---

### 1. `daily_pipeline.py` — Daily scheduled run

The main production script. Designed to run once per day via a scheduler.

**Default behaviour:** fetches yesterday and appends to BigQuery.

**Parallelism config:**

```python
PAGE_WORKERS   = 3   # parallel page fetches per day
DAY_WORKERS    = 3   # parallel days processed at once
MAX_CONCURRENT = 9   # global API semaphore (PAGE_WORKERS × DAY_WORKERS)
```

**To change the date range** (for ad-hoc use):

```python
START = "2026-02-01"
END   = "2026-02-28"
```

**For daily runs**, uncomment in `main()`:

```python
yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
START = END = yesterday
```

**Run:**

```bash
python daily_pipeline.py
```

**Log file:** `cc_extraction.log`

**Output directory:** `./data/raw/`

---

### 2. `flexible_backfill.py` — Flexible date fetcher

For ad-hoc use: yesterday, a single date, or a date range. Choose a mode at the top of the file.

**Mode configuration:**

```python
# OPTION 1 — fetch yesterday (default)
MODE = "YESTERDAY"

# OPTION 2 — fetch a date range
MODE = "RANGE"
START_DATE = "2026-01-01"
END_DATE   = "2026-01-31"

# OPTION 3 — fetch a single date
MODE = "SINGLE"
SINGLE_DATE = "2026-05-15"
```

**Behaviour by mode:**

| Mode | Parallelism | Output file |
|------|-------------|-------------|
| `YESTERDAY` | Single day | `cc_YYYY-MM-DD.parquet` |
| `SINGLE` | Single day | `cc_YYYY-MM-DD.parquet` |
| `RANGE` | Parallel days (`DAY_WORKERS`) | `cc_START_to_END.parquet` |

For `RANGE` mode, all days are fetched in parallel, globally deduplicated, then uploaded as one Parquet file in a single BigQuery load job.

**Run:**

```bash
python flexible_backfill.py
```

**Log file:** `cc_fetch.log`

**Output directory:** `./data/fetch/`

---

### 3. `backfill.py` — Historical backfill with resume

Designed for the initial historical load. Processes one month at a time and tracks progress so it can resume from where it left off if interrupted.

**Date range config:**

```python
BACKFILL_START = "2024-01-01"
BACKFILL_END   = "2024-09-30"
```

**Resume support:**

Progress is saved to `cc_backfill_progress.json` after each month completes. If the script crashes or is stopped, re-running it will skip already-completed months automatically.

```json
{
  "completed_months": ["2024-01", "2024-02", "2024-03"],
  "failed_months": ["2024-04"]
}
```

To restart from scratch, delete `cc_backfill_progress.json`.

**Per-month flow:**

```
Month chunk  →  fetch all days in parallel (DAY_WORKERS)
             →  deduplicate across the whole month
             →  save one Parquet file per month
             →  single BigQuery load job per month
             →  save progress  →  move to next month
```

Failed months are logged and recorded in the progress file. The script continues to the next month rather than aborting, so one bad month doesn't stop the entire backfill.

**Run:**

```bash
python backfill.py
```

**Log file:** `cc_backfill.log`

**Output directory:** `./data/backfill/`

**Progress file:** `cc_backfill_progress.json`

---

## Parallelism & rate limiting

All three scripts share the same fetch architecture:

```
DAY_WORKERS  ──┐
               ├──  ThreadPoolExecutor (days in parallel)
               │
               └──  per day: PAGE_WORKERS  ──┐
                                              ├──  ThreadPoolExecutor (pages in parallel)
                                              │
                                              └──  _api_semaphore (MAX_CONCURRENT total)
```

The global `threading.Semaphore(MAX_CONCURRENT)` ensures no more than `MAX_CONCURRENT` API calls are in flight at once, regardless of how many threads are active. Adjust these if you hit rate limits:

```python
PAGE_WORKERS   = 3   # lower if API returns 429s
DAY_WORKERS    = 3   # lower if memory is a concern
MAX_CONCURRENT = 9   # should equal PAGE_WORKERS × DAY_WORKERS
```

---

## Retry logic

Every page fetch retries up to **5 times** with exponential back-off:

| Attempt | Wait |
|---------|------|
| 1 | 2 s |
| 2 | 4 s |
| 3 | 8 s |
| 4 | 16 s |
| 5 | raises exception |

---

## Scheduling (production)

### Cloud Run Job + Cloud Scheduler

```bash
# Build and push
gcloud builds submit --tag gcr.io/$PROJECT/cc-daily

# Create job
gcloud run jobs create cc-daily-job \
  --image gcr.io/$PROJECT/cc-daily \
  --region us-central1 \
  --set-env-vars BQ_PROJECT=$PROJECT,BQ_DATASET=your_dataset,BQ_TABLE=CheckoutChamp_Orders \
  --set-secrets CC_LOGIN_ID=cc-login-id:latest \
  --set-secrets CC_PASSWORD=cc-password:latest \
  --set-secrets GCP_SERVICE_ACCOUNT_JSON=gcp-sa-json:latest

# Schedule daily at 07:00 UTC
gcloud scheduler jobs create http cc-daily-trigger \
  --schedule "0 7 * * *" \
  --uri "https://<cloud-run-job-trigger-url>" \
  --time-zone "UTC"
```

---

## Recommended workflow

```
First time setup
  └──  backfill.py           ← load all historical data

Ongoing
  └──  daily_pipeline.py     ← runs every morning via scheduler

Fix a gap / re-run a date
  └──  flexible_backfill.py  ← MODE = "SINGLE" or "RANGE"
```
