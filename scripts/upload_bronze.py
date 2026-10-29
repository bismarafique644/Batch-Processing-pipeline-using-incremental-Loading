
import os
from pathlib import Path
from minio import Minio

BASE_DIR = Path(__file__).resolve().parent.parent
PARQUET_DIR = BASE_DIR / "data" / "parquet" / "bronze_orders"

client = Minio(
    "127.0.0.1:9100",
    access_key=os.environ["MINIO_ROOT_USER"],
    secret_key=os.environ["MINIO_ROOT_PASSWORD"],
    secure=False
)

bucket = "ecommerce-batch-lake"

if not client.bucket_exists(bucket):
    client.make_bucket(bucket)

parquet_files = list(PARQUET_DIR.glob("*.parquet"))

if not parquet_files:
    raise RuntimeError("No Parquet files found!")

# First batch: use a unique batch identifier.
# Don't overwrite previously uploaded Bronze files.
batch_id = "initial_20261028_220224"

for file in parquet_files:
    object_name = f"bronze/orders/batch_id={batch_id}/{file.name}"

    client.fput_object(
        bucket,
        object_name,
        str(file),
        content_type="application/octet-stream"
    )

    print("Uploaded:", object_name)

print("Bronze upload successful!")
