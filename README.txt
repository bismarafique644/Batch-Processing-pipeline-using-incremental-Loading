E-COMMERCE INCREMENTAL BATCH ELT (BEFORE AIRFLOW)

1. Extract ZIP into C:\Users\DELL E7270\OneDrive\Desktop\ecommerce-api-elt.
   The ZIP has scripts/run_batch.py and scripts/spark_batch.py.
   It does not replace your existing scripts or data.
2. Docker Desktop must run; ecommerce-batch-minio container must be Up.
   Required local image: ecommerce-spark:local (PySpark 3.5.6).
   MinIO bucket: ecommerce-batch-lake, endpoint http://127.0.0.1:9100.
3. Windows PowerShell:
   cd "$HOME\OneDrive\Desktop\ecommerce-api-elt"
   $env:MYSQL_PASSWORD = Read-Host "MySQL password"
   $env:MINIO_ROOT_USER = "batchadmin"
   $env:MINIO_ROOT_PASSWORD = Read-Host "MinIO password"
   python .\scripts\run_batch.py
4. To inspect: http://127.0.0.1:9101 -> ecommerce-batch-lake
   bronze/orders/batch_id=...
   silver/orders/snapshot_id=...
   silver/orders/current.json
   gold/daily_sales/snapshot_id=...
   gold/category_sales/snapshot_id=...
   gold/customer_summary/snapshot_id=...
5. In MySQL Workbench:
   SELECT * FROM ecommerce_api_db.pipeline_watermark;

This creates a full pipeline per manual trigger; Airflow scheduling comes afterward.
It uses PRODUCTION watermark, not the test watermark. First run may reprocess
all 1981 source orders because the production watermark is still at 1900.
It uses an initial local Silver snapshot if no MinIO current pointer exists.
Subsequent runs use the latest MinIO Silver snapshot.
Soft-delete flags are retained in Silver and filtered out from Gold.
Hard-deletes are not detectable.

CAVEATS: A basic local prototype, not a production ACID lakehouse.
Use only one runner at a time. MinIO snapshots are versioned; keep historical files.
A failure between publishing pointers and watermark update is replayable; avoid
multiple simultaneous runs. Do not change MySQL source during an in-progress
batch if you need strict transaction-level consistency; update timestamps must
reflect source changes. Bronze files are Parquet, not JSONL (JSONL is staging).
MinIO credentials are supplied by environment/prompt, not hardcoded.
