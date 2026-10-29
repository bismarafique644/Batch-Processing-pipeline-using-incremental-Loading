"""Sync latest consistent MinIO Gold snapshots into three stable Power BI Parquet files.

Prerequisites: Python minio package, Docker Desktop running, ecommerce-spark:local
image, MINIO_ROOT_USER and MINIO_ROOT_PASSWORD environment variables.
Run from any working directory: python scripts/sync_powerbi_gold.py
"""
import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

from minio import Minio

BASE = Path(__file__).resolve().parent.parent
DATA_ROOT = BASE / "data"
DEST = DATA_ROOT / "powerbi_gold"
BUCKET = "ecommerce-batch-lake"
DATASETS = ("daily_sales", "category_sales", "customer_summary")


def read_pointer(client, dataset):
    response = client.get_object(BUCKET, f"gold/{dataset}/current.json")
    try:
        pointer = json.loads(response.read().decode("utf-8"))
    finally:
        response.close()
        response.release_conn()
    prefix = pointer.get("prefix")
    run_id = pointer.get("run_id")
    if not isinstance(prefix, str) or not prefix.startswith(
        f"gold/{dataset}/snapshot_id="
    ) or not prefix.endswith("/") or not isinstance(run_id, str) or not run_id:
        raise RuntimeError(f"Invalid current.json for {dataset}: {pointer}")
    if prefix != f"gold/{dataset}/snapshot_id={run_id}/":
        raise RuntimeError(f"Pointer prefix/run_id mismatch for {dataset}")
    return pointer


def main():
    DATA_ROOT.mkdir(parents=True, exist_ok=True)
    user = os.environ.get("MINIO_ROOT_USER")
    password = os.environ.get("MINIO_ROOT_PASSWORD")
    if not user or not password:
        raise RuntimeError("Set MINIO_ROOT_USER and MINIO_ROOT_PASSWORD first")
    client = Minio("127.0.0.1:9100", access_key=user, secret_key=password, secure=False)
    pointers = {name: read_pointer(client, name) for name in DATASETS}
    run_ids = {pointer["run_id"] for pointer in pointers.values()}
    if len(run_ids) != 1:
        raise RuntimeError(f"Inconsistent Gold snapshots: {run_ids}. Retry after batch completes.")
    run_id = next(iter(run_ids))
    print(f"Gold snapshot: {run_id}")

    with tempfile.TemporaryDirectory(prefix="gold_sync_", dir=DATA_ROOT) as tmp:
        stage = Path(tmp)
        for name in DATASETS:
            prefix = pointers[name]["prefix"]
            objects = [obj for obj in client.list_objects(
                BUCKET, prefix=prefix, recursive=True
            ) if obj.object_name.endswith(".parquet")]
            if not objects:
                raise RuntimeError(f"No Parquet parts found for {name} at {prefix}")
            input_dir = stage / name / "parts"
            input_dir.mkdir(parents=True)
            for index, obj in enumerate(objects):
                # Keep unique local filenames, even if source has nested partitions.
                target = input_dir / f"part_{index:05d}.parquet"
                client.fget_object(BUCKET, obj.object_name, str(target))
            print(f"Downloaded {name}: {len(objects)} Parquet part(s)")

        # Docker's Linux container reads the Windows-mounted project directory.
        rel_stage = stage.relative_to(BASE).as_posix()
        spark_code = r'''
import sys
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[2]").appName("PowerBIGoldSync").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
root = sys.argv[1]
try:
    for name in ("daily_sales", "category_sales", "customer_summary"):
        frame = spark.read.parquet(f"{root}/{name}/parts")
        rows = frame.count()
        if rows <= 0:
            raise RuntimeError(f"Empty Gold dataset: {name}")
        frame.coalesce(1).write.mode("overwrite").parquet(f"{root}/{name}/merged")
        print(f"MERGED {name}: {rows} rows", flush=True)
finally:
    spark.stop()
'''
        cmd = [
            "docker", "run", "--rm", "--entrypoint", "python",
            "-v", f"{BASE}:/app", "ecommerce-spark:local",
            "-c", spark_code, f"/app/{rel_stage}"
        ]
        print("Consolidating all Parquet parts with Docker PySpark...")
        subprocess.run(cmd, check=True)

        # Ensure pointers still target the same batch: avoid publishing stale/mixed sets.
        current = {name: read_pointer(client, name) for name in DATASETS}
        if current != pointers:
            raise RuntimeError("Gold pointers changed during sync. Please rerun.")

        completed = {}
        for name in DATASETS:
            parts = list((stage / name / "merged").glob("part-*.parquet"))
            if len(parts) != 1:
                raise RuntimeError(f"Unexpected Spark output count for {name}: {len(parts)}")
            completed[name] = parts[0]

        DEST.mkdir(parents=True, exist_ok=True)
        for name, source in completed.items():
            destination = DEST / f"{name}.parquet"
            # Copy to same destination filesystem then replace atomically per file.
            temporary = DEST / f"{name}.parquet.new"
            shutil.copyfile(source, temporary)
            os.replace(temporary, destination)
            print(f"UPDATED: {destination}")

        (DEST / "sync_metadata.json").write_text(
            json.dumps({"run_id": run_id, "datasets": list(DATASETS)}, indent=2),
            encoding="utf-8",
        )
    print("POWERBI_GOLD_SYNC_OK")


if __name__ == "__main__":
    main()
