# E-Commerce Incremental Batch ELT Pipeline

An end-to-end, locally deployed **incremental batch ELT** project using **Python, MySQL, Apache Airflow, PySpark, Docker, MinIO, and Power BI Desktop**. It loads new and updated e-commerce orders, maintains Bronze/Silver/Gold Parquet snapshots, produces a **five-table Star Schema**, and automatically synchronizes and refreshes an existing Power BI report in an interactive Windows session.

> **Project status (verified 1 November 2026):** The 11:00 PM Asia/Karachi Airflow schedule triggered a successful batch. Five Gold tables were published, local Power BI files synchronized, and the Power BI Desktop report was automatically opened, refreshed, validated, and saved. This demonstrates a successful test run—not guaranteed 24/7 operation under sleep, locked Windows, or infrastructure failure.

## 1. Project Overview

The initial e-commerce CSV dataset is imported into MySQL. Subsequent batch runs do **not** extract the complete source table every time. Instead, a composite watermark (`last_modified_at`, `OrderID`) identifies new and updated rows. A consistent MySQL read snapshot and upper timestamp cutoff are used for each incremental extraction.

PySpark validates and transforms extracted data through three layers:

- **Bronze:** extracted records stored as Parquet for each batch.
- **Silver:** cleaned current order state, including soft-deleted orders, produced by merging incremental changes with the previous Silver snapshot.
- **Gold:** analytics-ready **Star Schema** tables built from active Silver orders.

Gold datasets are stored in MinIO and synchronized to stable local Parquet files. A Windows-based watcher detects a new completed Gold snapshot and automates Power BI Desktop refresh, model validation, and PBIX save.

## 2. Architecture and Execution Flow

```text
Initial CSV (one-time import)
            |
            v
     MySQL sales_orders
            |
     Incremental read
 (last_modified_at, OrderID)
            ^
            | triggered by
   Airflow DAG (Docker)
    daily 23:00 PKT
            |
     Authenticated HTTP
            v
  Windows Airflow Bridge
       :8765 /run
            |
            v
   scripts/run_batch.py
            |
            v
    PySpark processing
            |
   +--------+---------+
   |        |         |
 Bronze   Silver     Gold
 Parquet  snapshot  Star Schema
   |        |         |
   +--------+---------+
            |
      MinIO data lake
            |
      sync_powerbi_gold.py
            |
    Five local Gold Parquet files
     + sync_metadata.json
            |
   Power BI watcher (--watch)
   polls completed run_id
            |
    Power BI Desktop .pbix
      Open -> Refresh
      -> Compare DAX model metrics
         with DuckDB Gold metrics
      -> Save PBIX
            |
      last_success.json

Windows Task Scheduler:
  - Starts/supervises the Airflow Bridge at logon
  - Starts Power BI Auto Refresh watcher
  - Runs a Power BI Watchdog to restart a missing watcher

MySQL watermark is committed after successful batch publication.
```

**Responsibilities:** Airflow schedules and monitors the batch; Windows Bridge launches the Windows runner; Spark performs data processing; MinIO persists published snapshots; the Power BI watcher independently handles desktop report refresh. The watchdog monitors watcher process availability; it does not extract or transform data.

## 3. Technology Stack

| Technology | Purpose |
|---|---|
| Python | Initial import, extraction, batch runner, bridge, Gold sync, watcher |
| MySQL | Source table and persistent incremental watermark |
| Apache Airflow | Daily scheduling, task monitoring, batch trigger |
| Docker Desktop | Local Airflow, Spark and MinIO runtime |
| PySpark / Apache Spark | Cleaning, validation, Silver merge, Gold modeling |
| MinIO | Bronze/Silver/Gold object storage and versioned snapshots |
| Apache Parquet | Columnar dataset format |
| Power BI Desktop | Sales and business reporting |
| DuckDB | Read local Parquet files to calculate expected Gold metrics |
| pywinauto | Identify and interact with Power BI Desktop UI |
| pywin32 | Windows mutex used to prevent concurrent watcher instances |
| PowerShell / MSOLAP | Query Power BI's local tabular model using DAX |
| Windows Task Scheduler | Launch background processes and run watchdog |

## 4. Source Data and MySQL

The initial dataset is imported into `ecommerce_api_db.sales_orders` using `scripts/import_orders.py`.

- Original CSV: approximately **2,000 rows**, **23 columns**.
- Initial import: **1,980 unique OrderIDs** after eliminating 20 duplicate OrderIDs.
- Source changes are represented using `last_modified_at` and `is_deleted`.
- A composite index on `(last_modified_at, OrderID)` supports ordered incremental extraction.
- Watermark state is stored in `ecommerce_api_db.pipeline_watermark` for pipeline `ecommerce_orders`.

These figures describe the initial imported dataset, not the later evolving source-table row count.

### Initial import

```text
data/kaggle/Ecommerce_2000.csv
          |
          v
 scripts/import_orders.py
          |
          v
ecommerce_api_db.sales_orders
```

The importer uses CSV reading, data type conversions, deduplication by `OrderID`, and batched MySQL inserts.

## 5. Incremental Loading and Watermark

**Composite cursor:**

```text
(last_modified_at, OrderID)
```

A simplified extraction predicate:

```sql
SELECT *
FROM sales_orders
WHERE (
       last_modified_at > :last_timestamp
       OR (
          last_modified_at = :last_timestamp
          AND OrderID > :last_order_id
       )
      )
  AND last_modified_at < :upper_cutoff
ORDER BY last_modified_at ASC, OrderID ASC;
```

*The placeholders above are illustrative; the actual Python driver uses its own parameter binding.*

### How a batch works

1. Read the previously committed watermark.
2. Open a MySQL transaction with a consistent read snapshot.
3. Set an upper timestamp cutoff (the runner uses a two-second safety gap).
4. Read rows after the composite watermark and before the cutoff, sorted by timestamp and OrderID.
5. Write the extracted records to run-specific local JSONL.
6. Process and publish the batch using Spark and MinIO.
7. Advance the watermark with a compare-and-swap (CAS) update after publication.
8. If no source changes are found, skip unnecessary data processing and leave the watermark unchanged.

**Example watermark:**

```text
last_modified_at = 2026-11-01 18:51:02.318119
last_order_id     = ORD-AUTO-NOV01-010
```

The combined cursor handles multiple records having the same timestamp. CAS helps detect a competing watermark update; it does not by itself implement a distributed execution lock.

**Limits:** A timestamp cursor can miss certain late-committing transactions whose timestamps are older than an already advanced watermark. Physical hard deletes are not visible from a normal table scan. Stronger change tracking would require a CDC approach, reconciliation strategy, or both.

## 6. Bronze, Silver and Gold Layers

### Bronze — extracted batch data

Source JSONL is processed by Spark and written as Parquet, under a versioned batch path:

```text
bronze/orders/batch_id=<run_id>/
```

Bronze retains extracted batch-level data for investigation and reprocessing. The Parquet representation is not a byte-for-byte archive of the incoming JSONL.

### Silver — latest cleaned order state

The Silver pipeline:

- Casts fields to usable types and standardizes values.
- Validates required fields, quantities and discount ranges.
- Writes rejected data for failed quality checks.
- Combines the previous Silver snapshot with the newly extracted changes.
- Uses a window by `OrderID`, ordered by latest `last_modified_at` and incoming-batch priority, to keep one current record per order.
- Retains soft-deleted rows (`is_deleted = 1`) as part of current order state.

```text
silver/orders/snapshot_id=<run_id>/
silver/orders/current.json
```

**Implementation detail:** Extraction is incremental, but the current Silver merge reads previous state and writes a **new full Silver snapshot**. It is not an incremental Delta/Iceberg `MERGE INTO`.

### Gold — five-table Star Schema

Gold is built from the active Silver records (`is_deleted = 0`).

| Table | Grain / purpose | Key details |
|---|---|---|
| `fact_sales` | **One row per OrderID** | `OrderID` logical unique key; measure columns and dimension foreign keys |
| `dim_date` | One row per unique date | `DateKey`, calendar attributes |
| `dim_customer` | Latest customer attributes | `CustomerID`, customer, city/state, membership details |
| `dim_product` | Product category/subcategory combination | Deterministic `ProductKey` generated from category fields |
| `dim_payment` | Payment method/device combination | Deterministic `PaymentKey` generated from payment attributes |

The fact table includes references such as `DateKey`, `CustomerID`, `ProductKey` and `PaymentKey`, plus measures including `Quantity`, `UnitPrice`, `TotalAmount`, `DiscountAmount` and `FinalAmount`.

**Logical primary key:** `fact_sales.OrderID`. Parquet does not enforce SQL primary-key constraints itself; the Spark pipeline validates uniqueness.

The dimension relationships are conceptually one-to-many from each dimension to the fact. `dim_customer` currently has a **Type 1-like latest-state design**; a fully historical SCD Type 2 with version-specific `CustomerKey`, `ValidFrom`, and `ValidTo` is not implemented.

Gold objects are published under versioned paths such as:

```text
gold/fact_sales/snapshot_id=<run_id>/
gold/dim_date/snapshot_id=<run_id>/
gold/dim_customer/snapshot_id=<run_id>/
gold/dim_product/snapshot_id=<run_id>/
gold/dim_payment/snapshot_id=<run_id>/
```

Current pointer/manifest objects identify the published versions.

**Implementation detail:** The current Gold tables are regenerated from the latest complete Silver snapshot rather than incrementally MERGEd into existing Gold tables.

## 7. Airflow Scheduling and Windows Bridge

**DAG:** `ecommerce_incremental_batch_elt`

**Schedule:** Daily at **11:00 PM Asia/Karachi**.

**Main Airflow task:** `run_incremental_batch`.

```text
Airflow scheduler
       |
       v
Airflow DAG/task
       |
 authenticated POST
 http://host.docker.internal:8765/run
       |
       v
scripts/airflow_bridge.py
       |
       +--> scripts/run_batch.py
       |
       +--> scripts/sync_powerbi_gold.py
       |
       v
batch/sync result returned to Airflow
```

The Bridge is started through `scripts/start_airflow_bridge.ps1`. It runs as an available Windows process and waits for Airflow requests. **Task Scheduler starting the Bridge does not itself schedule the daily ELT batch.**

The Bridge checks an authentication token and uses an in-process lock for overlapping requests. Airflow limits active DAG runs to one.

The current DAG represents the major processing stages inside **one Airflow task**, not separate Bronze/Silver/Gold Airflow tasks. A failure in a Spark substep therefore fails this main task.

**Requirements for the schedule:** Machine awake, Docker available, Airflow scheduler running, MySQL and MinIO accessible, and Bridge reachable.

## 8. MinIO Storage

Local MinIO configuration used in the project:

| Item | Value |
|---|---|
| Container | `ecommerce-batch-minio` |
| Bucket | `ecommerce-batch-lake` |
| Host API | `http://127.0.0.1:9100` |
| Host Console | `http://127.0.0.1:9101` |
| Container API | Port `9000` |
| Container Console | Port `9001` |

Port mappings should be checked with `docker port ecommerce-batch-minio`; container logs can show internal ports that differ from Windows host ports.

**Note:** Historical snapshots need a safe cleanup and retention policy. Do not delete snapshot files currently referenced by published pointers or needed for recovery.

## 9. Automatic Power BI Gold Synchronization

`scripts/sync_powerbi_gold.py` synchronizes the five published MinIO Gold datasets into stable local paths:

```text
data/powerbi_gold/
    fact_sales.parquet
    dim_date.parquet
    dim_customer.parquet
    dim_product.parquet
    dim_payment.parquet
    sync_metadata.json
```

The sync process downloads Parquet parts, consolidates as required, atomically replaces each local file, and finally writes `sync_metadata.json` to indicate the completed snapshot and its `run_id`.

**Consistency limit:** Individual file replacements are atomic, but the five-file collection does not have a single cross-file atomic swap. A versioned-directory publication model would strengthen snapshot consistency.

The Power BI Desktop report is located at:

```text
Dashboard/Ecommerece_sales.pbix
```

The dashboard is manually designed, but later report refresh/save is automated through the watcher described next.

## 10. Power BI Auto Refresh Watcher

**File:** `scripts/auto_refresh_powerbi.py`

**Purpose:** Continuously monitor new Gold snapshots and refresh/save the existing Power BI Desktop report.

The script supports:

```powershell
python .\scripts\auto_refresh_powerbi.py --baseline
python .\scripts\auto_refresh_powerbi.py --once
python .\scripts\auto_refresh_powerbi.py --watch
```

| Mode | Meaning |
|---|---|
| `--baseline` | Verify an already-open/current model matches Gold and record its run as a baseline |
| `--once` | Attempt one refresh for the current Gold run |
| `--watch` | Monitor and automatically handle new completed Gold runs |

### Watcher algorithm

1. Poll `data/powerbi_gold/sync_metadata.json` approximately every **30 seconds**.
2. Check metadata reports `status = complete`, expected five datasets, and non-empty local Parquet files.
3. Compare current `run_id` against `data/powerbi_automation/last_success.json`.
4. If it is an unprocessed snapshot, use **DuckDB** to compute expected Gold metrics from Parquet.
5. Open the existing PBIX automatically using `os.startfile()` if needed.
6. Use **PowerShell + local MSOLAP connection + DAX** to read the Power BI model's current row counts, distinct orders, revenue, quantity, gross and discount measures.
7. Back up the PBIX to `data/pbix_backup/`.
8. Use **pywinauto** to find the verified Power BI Home ribbon Refresh control and click it.
9. Poll the DAX model until its metrics change and match expected Gold values **three consecutive times**.
10. Wait an additional **20 seconds**, revalidate, send **Ctrl+S**, and verify the PBIX file has been updated and stabilized.
11. Record the successful `run_id` and log `POWERBI_AUTO_REFRESH_OK`.

### Watcher state and logs

```text
data/powerbi_automation/
    last_success.json
    last_attempt.json
    refresh.log
```

The watcher uses a named Windows **mutex** (`Local\EcommercePowerBIRefresh`) to prevent another automation process in the same local mutex namespace from running simultaneously.

When model metrics already match Gold **before** clicking Refresh, the code records `completion_unverified` and skips automatic Save for that run because it cannot independently prove refresh completion from unchanged metrics. Ordinary failures are logged and retried after approximately **300 seconds** (five minutes), subject to process/session availability.

**Important:** This automation requires a logged-in, **unlocked, interactive Windows desktop session**. Locked, logged-out or sleeping conditions cannot be treated as reliably supported.

## 11. Watchdog and Task Scheduler

The watcher and watchdog have different responsibilities:

| Component | Responsibility |
|---|---|
| **Watcher** (`auto_refresh_powerbi.py --watch`) | Detect Gold runs; refresh, validate and save Power BI |
| **Watchdog** (`powerbi_watchdog.ps1`) | Check whether the watcher process exists; restart the scheduled watcher task if missing |
| **Windows Task Scheduler** | Launch Bridge/watcher at logon and run the watchdog periodically |
| **Airflow** | Trigger scheduled ELT batches, not the Power BI UI clicks |

Configured Task Scheduler jobs include:

```text
Ecommerce Batch Bridge
Ecommerce Power BI Auto Refresh
Ecommerce Power BI Watchdog
```

The watchdog is configured to check periodically (previously every five minutes), plus logon behavior. A process-existence watchdog can restart a missing watcher, but may **not** detect a watcher that is alive yet hung. A future heartbeat-based monitor would improve that.

The Bridge and Power BI watcher are separate processes. The Bridge starts the batch and Gold sync; the watcher reacts to completed sync metadata.

## 12. Power BI Dashboard and Business Metrics

The Gold model supports measures and analysis such as:

- Total Orders and Total Revenue
- Distinct Customers and Units Sold
- Revenue over time
- Category/subcategory performance
- Payment method and device analysis
- Customer state/city and membership analysis

**Metric definitions:** Revenue is based on `FinalAmount` for active orders. It is not equivalent to profit, because complete product cost/expense data is not modeled here. Exact KPI names and report visuals should be checked in the saved PBIX.

## 13. Verified Scheduled Test — 1 November 2026

A controlled test inserted **10** new orders, updated **5**, and soft-deleted **5**. The five active test records contributed **3,600.00** in revenue.

The 11 PM scheduled run produced:

| Verification item | Result |
|---|---|
| Airflow schedule | Triggered automatically |
| Batch run | Successful |
| Watermark | Committed |
| `fact_sales` | 1,997 rows |
| `dim_date` | 898 rows |
| `dim_customer` | 1,571 rows |
| `dim_product` | 51 rows |
| `dim_payment` | 21 rows |
| Gold synchronization | `POWERBI_GOLD_SYNC_OK` |
| Desktop refresh | Automatically clicked |
| Model validation | Gold/model metrics matched 3/3 |
| PBIX save | Verified |
| Final watcher status | `POWERBI_AUTO_REFRESH_OK` |

The tested Gold snapshot ID was:

```text
20261101_230006_bf0d7999
```

Verified revenue after this batch: **465,814.70** (previous report: **462,214.70**).

This confirms the tested end-to-end sequence, including automatic desktop refresh/save in that available interactive Windows session. It does not establish reliability for every future run or for Windows lock/sleep scenarios.

## 14. Main Project Files

```text
ecommerce-api-elt/
|
|-- scripts/
|   |-- import_orders.py
|   |-- run_batch.py
|   |-- spark_batch.py
|   |-- airflow_bridge.py
|   |-- start_airflow_bridge.ps1
|   |-- sync_powerbi_gold.py
|   |-- auto_refresh_powerbi.py
|   |-- powerbi_watchdog.ps1
|
|-- airflow/
|   |-- dags/
|       |-- ecommerce_batch_dag.py
|
|-- Dashboard/
|   |-- Ecommerece_sales.pbix          # local; do not commit
|
|-- data/                                # local generated files; do not commit
|   |-- kaggle/
|   |-- auto_runs/
|   |-- powerbi_gold/
|   |-- powerbi_automation/
|   |-- pbix_backup/
|
|-- requirements.txt                     # if present
|-- PROJECT_DOCUMENTATION.md             # this document
```

This is a **focused logical tree**, not a claim that every listed file exists in every GitHub clone. The GitHub repository's staged/tracked files should be verified independently.

## 15. Setup and Useful Commands

### Local prerequisites

- Windows 10/11 and PowerShell
- Python and required dependencies
- MySQL Server and initial source table
- Docker Desktop / Linux containers
- Airflow, Spark image, MinIO
- Power BI Desktop and compatible local MSOLAP provider
- Interactive Windows desktop for unattended UI operation

Useful Python packages for the watcher:

```powershell
pip install duckdb pywinauto pywin32
```

Other pipeline dependencies include packages such as PyMySQL and MinIO's Python SDK. Install the versions specified in the project's actual dependency files where available.

### Check container and ports

```powershell
docker ps
docker ps -a --filter "name=ecommerce-batch-minio"
docker port ecommerce-batch-minio
Test-NetConnection 127.0.0.1 -Port 9101
```

### Run a batch manually

From the project folder, with required credentials already configured securely:

```powershell
python .\scripts\run_batch.py
```

**Caution:** Do not run a manual batch concurrently with the scheduled batch; the current system does not provide robust cross-process locking.

### Run Gold sync manually

```powershell
python .\scripts\sync_powerbi_gold.py
```

### Watch refresh logs

```powershell
Get-Content .\data\powerbi_automation\refresh.log -Tail 60
```

### Check scheduled tasks

```powershell
Get-ScheduledTask -TaskName "Ecommerce Batch Bridge"
Get-ScheduledTask -TaskName "Ecommerce Power BI Auto Refresh"
Get-ScheduledTask -TaskName "Ecommerce Power BI Watchdog"
```

### Check source, active rows and watermark

```sql
SELECT COUNT(*) AS total_rows
FROM ecommerce_api_db.sales_orders;

SELECT COUNT(*) AS active_rows
FROM ecommerce_api_db.sales_orders
WHERE is_deleted = 0;

SELECT *
FROM ecommerce_api_db.pipeline_watermark;
```

## 16. Reliability, Recovery and Limitations

This is a working local implementation with known design limitations:

| Area | Current behavior / limitation | Possible improvement |
|---|---|---|
| Late-committing updates | Timestamp watermark may miss changes committed after watermark advances with an earlier timestamp | CDC, reconciliation, carefully designed overlap |
| Hard deletes | Not detected by normal incremental table query | CDC delete events or periodic reconciliation |
| Soft deletes | `is_deleted` state retained in Silver; active Gold excludes deleted rows | Maintain deletion audit/history as needed |
| Silver scalability | Full Silver snapshot read/merge/rewrite for small changes | Delta Lake / Apache Iceberg MERGE |
| Gold scalability | All five Gold datasets rebuilt | Incremental fact/dimension maintenance |
| Extraction memory | Buffered cursor may still materialize large results | Unbuffered cursor or robust keyset paging |
| Cross-system atomicity | MySQL watermark and MinIO publication are not one transaction | Durable run-state/reconciliation protocol |
| Concurrency | Airflow task constraint and Bridge lock do not cover all direct/manual executions | Database-backed execution lease |
| Data quality | Invalid rows can fail the whole batch | Safe quarantine and replay process |
| Storage | Full snapshots accumulate | Retention, compaction, orphan cleanup |
| Gold local sync | Five files are not collectively atomic | Versioned local directories + active pointer |
| Desktop automation | Requires awake, logged-in and unlocked session | Power BI Service/gateway where supported |
| UI stability | Refresh control depends partly on ribbon accessibility and geometry | More reliable supported refresh mechanism |
| Watchdog | Detects missing process, not necessarily a hung watcher | Heartbeat and liveness checks |
| Validation | Matching aggregates do not prove every individual row | Row-level checksums / deeper reconciliation |
| Customer history | Latest customer dimension attributes only | SCD Type 2 if historical attribute reporting is required |
| Product identity | Product category/subcategory key, not actual SKU | Introduce ProductID/SKU if available |

Retries must be paired with idempotent publication and recovery logic. Incomplete jobs can leave intermediate run files or snapshots, and should be reconciled before cleanup.

## 17. Security and GitHub Publishing

This documentation contains **no passwords, API tokens, private keys, or personal credentials**. Do not commit:

- `.env`, encrypted secrets, credential stores or tokens
- Local CSV/raw records or generated Parquet files
- MySQL database backups and runtime logs
- PBIX report files if these contain private data or connections
- `last_success.json`, `last_attempt.json` and operational state
- Temporary batches, backups and local secret-bearing configuration

Suggested `.gitignore`:

```gitignore
.env
.env.*
!.env.example
*.enc
*.pem
*.key
*.pfx
*.p12

__pycache__/
*.pyc
.pytest_cache/
.venv/
venv/

data/
logs/
airflow/logs/
*.log
*.db
*.sqlite
*.parquet
*.csv
*.pbix
*.zip

readme.txt
README.txt
```

`.gitignore` does **not** remove already tracked files or delete previously committed secrets from Git history. Review `git ls-files` and `git diff --cached` before pushing, and rotate any secrets previously exposed.

The documentation is intentionally named **`PROJECT_DOCUMENTATION.md`**, rather than `readme.txt`.

### Publish updated documentation

After copying this `.md` file to the local repository:

```powershell
cd "$env:TEMP\ecommerce-github-upload"

git status --short
git add PROJECT_DOCUMENTATION.md
git diff --cached --check
git diff --cached --name-only

git commit -m "Update documentation for Star Schema and Power BI automation"
git push origin main
```

Review the staged file list before committing. Do not use `git add .` indiscriminately when local secrets or runtime files could exist.

## 18. Conclusion

The project demonstrates a tested **incremental batch ELT** workflow, combining composite-watermark extraction from MySQL, PySpark Bronze/Silver/Gold processing, a five-table Star Schema in MinIO, Airflow scheduling, a Windows HTTP Bridge, local Gold synchronization, and automated Power BI Desktop refresh/validation/save.

The successful scheduled test shows that these components worked together in the tested local environment. Future improvements should focus first on **change-capture correctness, failure recovery and cross-process consistency**, then on **incremental table maintenance, monitoring, and reducing dependence on interactive Power BI Desktop UI automation**.
