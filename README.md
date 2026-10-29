
# E-Commerce Incremental Batch ELT Pipeline

An automated incremental batch ELT pipeline built using Python, MySQL, Apache Airflow, PySpark, Docker, and MinIO. The project extracts new and updated records, processes data through Bronze, Silver, and Gold layers, and prepares analytics-ready datasets for Power BI reporting.

## Project Overview

This project demonstrates an end-to-end data engineering pipeline that processes e-commerce order data using incremental loading.

Instead of extracting the complete dataset during every execution, the pipeline identifies new and updated records using a timestamp-based watermark combined with OrderID.

Apache Airflow orchestrates the scheduled execution, PySpark processes the data, and MinIO stores the results in Parquet format.

The final Gold datasets are used to build an interactive Power BI sales dashboard.

## Pipeline Architecture

```text
              E-Commerce Dataset
                      |
                      v
                MySQL Database
                      |
                      v
           Incremental Extraction
          (Timestamp + OrderID)
                      |
                      v
              Apache Airflow
               (Scheduling)
                      |
                      v
              Python / PySpark
                      |
                      v
                  MinIO
          +-------------------+
          |                   |
          v                   |
      Bronze Layer            |
      Raw Parquet             |
          |                   |
          v                   |
      Silver Layer            |
      Cleaned & Merged        |
          |                   |
          v                   |
       Gold Layer             |
     Aggregated Data          |
          |                   |
          v                   |
    Local Gold File Sync      |
          |                   |
          v                   |
      Power BI Desktop        |
          |                   |
          v                   |
      Sales Dashboard         |
                              |
           Watermark Updated After
           Successful Batch Processing
```

## Technology Stack

| Technology | Purpose |
|------------|---------|
| Python | Data extraction and pipeline execution |
| MySQL | Source database |
| Apache Airflow | Workflow orchestration and scheduling |
| PySpark | Distributed data processing and transformations |
| Docker | Containerized execution environment |
| MinIO | Object storage and data lake |
| Apache Parquet | Columnar data storage format |
| Power BI | Data visualization and reporting |

## Key Features

- Automated incremental batch processing
- Timestamp and OrderID-based watermark tracking
- Extraction of new and updated records
- Bronze, Silver, and Gold data architecture
- PySpark-based data transformations
- Parquet storage in MinIO
- Silver-layer merging and upsert processing
- Soft-delete handling
- Scheduled workflow execution using Apache Airflow
- Automatic Gold data synchronization for Power BI
- Watermark updates after successful batch processing
- Basic error handling and recovery support

## Data Processing Layers

### 1. Bronze Layer

The Bronze layer stores extracted source records in Parquet format.

**Purpose:**
- Preserve extracted records
- Maintain batch-level history
- Support auditing and debugging

Storage location:

```text
bronze/orders/batch_id=<run_id>/
```

### 2. Silver Layer

The Silver layer processes the extracted data and merges it with the previous snapshot.

**Purpose:**
- Apply data transformations
- Maintain updated records
- Prevent duplicate OrderIDs
- Track soft-deleted records
- Prepare consistent datasets for aggregation

Storage location:

```text
silver/orders/snapshot_id=<run_id>/
```

The current Silver snapshot is identified using:

```text
silver/orders/current.json
```

### 3. Gold Layer

The Gold layer contains aggregated, analytics-ready datasets.

Three datasets are generated:

| Dataset | Description |
|---------|-------------|
| daily_sales | Daily revenue, order count, and units sold |
| category_sales | Category-wise revenue and order count |
| customer_summary | Customer spending and order activity |

Storage locations:

```text
gold/daily_sales/
gold/category_sales/
gold/customer_summary/
```

These datasets are used for Power BI dashboard reporting.

## Incremental Loading Strategy

The pipeline uses two columns:

- `last_modified_at`
- `OrderID`

The extraction condition is:

```sql
SELECT *
FROM sales_orders
WHERE
    last_modified_at > :last_timestamp
    OR (
        last_modified_at = :last_timestamp
        AND OrderID > :last_order_id
    )
ORDER BY last_modified_at, OrderID;
```

The production extraction also applies an upper timestamp boundary.

### How It Works

1. Read the previously stored watermark.
2. Identify new and updated records.
3. Extract the records from MySQL.
4. Process the extracted data using PySpark.
5. Store the results in MinIO.
6. Publish the latest snapshot references.
7. Commit the new watermark after successful processing.

If no new or updated records are available, the pipeline skips unnecessary processing.

### Watermark Management

The watermark is maintained in the MySQL table:

```text
pipeline_watermark
```

It stores the last processed timestamp and OrderID.

This helps prevent repeated extraction of already processed records during normal incremental execution.

## Workflow Orchestration

Apache Airflow is used to schedule and monitor pipeline execution.

**DAG Name:**

```text
ecommerce_incremental_batch_elt
```

**Schedule:**

```text
Daily at 11:00 PM (Asia/Karachi)
```

The Airflow container communicates with the Windows batch runner through a local Python HTTP bridge.

Execution sequence:

```text
Airflow DAG
    |
    v
Python HTTP Bridge
    |
    v
run_batch.py
    |
    v
MySQL -> PySpark -> MinIO
    |
    v
sync_powerbi_gold.py
    |
    v
Local Power BI Parquet Files
```

The Gold synchronization step is executed after successful batch processing when the updated bridge configuration is active.

## Project Structure

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
|
|-- airflow/
|   |-- dags/
|       |-- ecommerce_batch_dag.py
|
|-- data/
|   |-- kaggle/
|   |-- auto_runs/
|   |-- powerbi_gold/
|
|-- README.md
|-- requirements.txt
```

Note: The structure highlights the main project files. Local generated data and credentials should not be committed to GitHub.

## Setup and Requirements

### Prerequisites

- Windows 10 or Windows 11
- Python 3.10+
- MySQL Server
- Docker Desktop
- Apache Airflow
- MinIO
- PySpark
- Power BI Desktop

### Python Dependencies

Install the required Python packages:

```bash
pip install pymysql minio
```

Additional dependencies may be required by supporting scripts.

### Docker Requirements

The local environment uses:

```text
Spark Image: ecommerce-spark:local
MinIO Container: ecommerce-batch-minio
MinIO Bucket: ecommerce-batch-lake
```

MinIO API:

```text
http://127.0.0.1:9100
```

MinIO Console:

```text
http://127.0.0.1:9101
```

## Manual Pipeline Execution

Open Windows PowerShell in the project directory:

```powershell
cd "$HOME\OneDrive\Desktop\ecommerce-api-elt"
```

Configure credentials:

```powershell
$env:MYSQL_PASSWORD = Read-Host "MySQL password"

$env:MINIO_ROOT_USER = "batchadmin"

$env:MINIO_ROOT_PASSWORD = Read-Host "MinIO password"
```

Execute the batch:

```powershell
python .\scripts\run_batch.py
```

The pipeline will:

1. Read the stored watermark.
2. Extract incremental records.
3. Execute PySpark transformations.
4. Upload Bronze, Silver, and Gold datasets.
5. Update the watermark after successful processing.

### Expected Success Message

```text
SUCCESS: Bronze, Silver, Gold published to MinIO.
WATERMARK COMMITTED: ...
```

If no changes are found:

```text
NO CHANGES: Nothing uploaded, watermark unchanged.
```

## Airflow Automation

Airflow is available locally at:

```text
http://127.0.0.1:8085
```

The pipeline uses a scheduled Airflow DAG to trigger batch execution automatically.

The Windows bridge receives authorized requests from Airflow and executes the batch script.

The bridge can be started using Windows Task Scheduler, allowing it to run without keeping a PowerShell terminal open.

**Note:** Scheduled execution requires the computer to remain powered on and awake, with Docker, MySQL, MinIO, Airflow, and the bridge available.

## Power BI Integration

Power BI Desktop reads the processed Gold datasets from fixed local Parquet file paths.

### Gold Datasets

```text
data/powerbi_gold/daily_sales.parquet

data/powerbi_gold/category_sales.parquet

data/powerbi_gold/customer_summary.parquet
```

### Dashboard Metrics

The dashboard includes:

- Total Revenue
- Total Orders
- Total Units Sold
- Monthly Revenue Trend
- Category-wise Revenue
- Customer Spending Analysis

### Data Refresh

The Gold synchronization script downloads the latest MinIO Gold snapshots and prepares stable Parquet files for Power BI.

Manual synchronization:

```powershell
python .\scripts\sync_powerbi_gold.py
```

Expected output:

```text
POWERBI_GOLD_SYNC_OK
```

After synchronization, open Power BI Desktop and select:

```text
Home -> Refresh
```

When integrated with the Airflow bridge, Gold file synchronization runs automatically after a successful batch.

Power BI Desktop still requires a manual report refresh. Fully unattended report refresh requires an appropriate Power BI Service and gateway configuration.

## Pipeline Verification

### Check the Production Watermark

```sql
SELECT *
FROM ecommerce_api_db.pipeline_watermark;
```

### Check Total Source Records

```sql
SELECT COUNT(*)
FROM ecommerce_api_db.sales_orders;
```

### Check Active Records

```sql
SELECT COUNT(*)
FROM ecommerce_api_db.sales_orders
WHERE is_deleted = 0;
```

### Test Incremental Loading

1. Insert a new record into MySQL.
2. Trigger the Airflow DAG or run the batch manually.
3. Verify that the watermark advances.
4. Check the new MinIO snapshots.
5. Verify the Gold synchronization output.
6. Refresh the Power BI dashboard.
7. Compare the dashboard results with the source data.

## Error Handling and Recovery

The pipeline includes basic recovery mechanisms:

- Database watermark updates occur after processing.
- A failed batch does not intentionally advance the watermark.
- MinIO snapshots are versioned.
- Previous Silver snapshots can be reused.
- Failed extraction or processing can be retried.
- The Airflow bridge prevents overlapping requests within the same bridge process.

This local prototype does not provide fully atomic transactions across MySQL and MinIO.

Only one batch runner should be active at a time.

## Current Limitations

- Hard deletes are not detected automatically.
- Soft deletes are tracked using the `is_deleted` column.
- Large incremental extractions may require streaming cursors.
- Cross-process concurrency protection needs further improvement.
- Failed runs may leave temporary local files or unpublished snapshots.
- Historical MinIO snapshots require a retention and cleanup policy.
- Timestamp-based extraction depends on reliable source modification timestamps.
- Strict transaction-level CDC guarantees are not provided.
- Power BI Desktop report refresh is currently manual.
- The local environment depends on the Windows laptop and Docker services.

## Future Improvements

- Optimize incremental extraction for millions of records.
- Add cross-process locking.
- Improve transaction and snapshot consistency.
- Implement automated cleanup and retention policies.
- Add structured logging and monitoring.
- Improve error notifications and retry mechanisms.
- Introduce automated data quality checks.
- Evaluate CDC-based ingestion for stronger change tracking.
- Deploy the pipeline on a dedicated server or cloud infrastructure.
- Enable scheduled Power BI Service refresh where supported.

## Security Considerations

Database passwords and MinIO credentials should never be hardcoded or committed to the repository.

Use environment variables or an appropriate secrets-management mechanism.

Recommended `.gitignore` entries:

```gitignore
__pycache__/
*.pyc
.venv/

.env
.env.*
*.enc

data/
*.log
```

## Conclusion

This project demonstrates a practical incremental batch ELT architecture for processing e-commerce data.

It combines watermark-based MySQL extraction, PySpark transformations, MinIO data lake storage, Apache Airflow orchestration, and Power BI analytics.

The implementation provides a foundation for automated batch processing, incremental data management, and business intelligence reporting while identifying areas for further production hardening.
