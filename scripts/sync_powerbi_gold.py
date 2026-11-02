
"""Sync latest consistent Gold Star Schema snapshots for Power BI.

Supports both:
    gold/<table>/snapshot_id=<run_id>/  (normal batch)
    gold/<table>/run_id=<run_id>/       (migration)

Downloads five Gold datasets from MinIO, consolidates
Parquet files using Docker PySpark, and updates local
Power BI Parquet files.
"""

import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

from minio import Minio


# ============================================================
# 1. CONFIGURATION
# ============================================================

BASE = Path(__file__).resolve().parent.parent

DATA_ROOT = BASE / "data"
DEST = DATA_ROOT / "powerbi_gold"

BUCKET = "ecommerce-batch-lake"
IMAGE = "ecommerce-spark:local"

DATASETS = (
    "fact_sales",
    "dim_date",
    "dim_customer",
    "dim_product",
    "dim_payment",
)


# ============================================================
# 2. READ AND VALIDATE GOLD POINTER
# ============================================================

def read_pointer(client, dataset):

    key = f"gold/{dataset}/current.json"

    response = client.get_object(BUCKET, key)

    try:
        pointer = json.loads(
            response.read().decode("utf-8")
        )
    finally:
        response.close()
        response.release_conn()

    prefix = pointer.get("prefix")
    run_id = pointer.get("run_id")

    if (
        not isinstance(run_id, str)
        or not run_id
        or not isinstance(prefix, str)
        or not prefix
    ):
        raise RuntimeError(
            f"Invalid pointer for {dataset}: {pointer}"
        )

    # Normal incremental batch convention.
    snapshot_prefix = (
        f"gold/{dataset}/snapshot_id={run_id}/"
    )

    # One-time migration convention.
    migration_prefix = (
        f"gold/{dataset}/run_id={run_id}/"
    )

    allowed_prefixes = {
        snapshot_prefix,
        migration_prefix,
    }

    if prefix not in allowed_prefixes:
        raise RuntimeError(
            f"Pointer prefix/run_id mismatch: {dataset}. "
            f"Actual prefix: {prefix}. "
            f"Expected one of: {sorted(allowed_prefixes)}"
        )

    return pointer


# ============================================================
# 3. VALIDATE CONSISTENT GOLD SNAPSHOT
# ============================================================

def get_consistent_pointers(client):

    pointers = {
        name: read_pointer(client, name)
        for name in DATASETS
    }

    run_ids = {
        pointer["run_id"]
        for pointer in pointers.values()
    }

    if len(run_ids) != 1:
        raise RuntimeError(
            "Inconsistent Gold snapshot run IDs: "
            f"{sorted(run_ids)}. "
            "Do not sync while a batch is publishing."
        )

    run_id = next(iter(run_ids))

    return run_id, pointers


# ============================================================
# 4. DOWNLOAD ALL PARQUET PARTS
# ============================================================

def download_dataset(client, dataset, pointer, stage):

    prefix = pointer["prefix"]

    objects = sorted(
        [
            obj
            for obj in client.list_objects(
                BUCKET,
                prefix=prefix,
                recursive=True
            )
            if obj.object_name.endswith(".parquet")
        ],
        key=lambda obj: obj.object_name
    )

    if not objects:
        raise RuntimeError(
            f"No Parquet files for {dataset}: {prefix}"
        )

    input_dir = stage / dataset / "parts"

    input_dir.mkdir(
        parents=True,
        exist_ok=True
    )

    for index, obj in enumerate(objects):

        target = (
            input_dir
            / f"part_{index:05d}.parquet"
        )

        client.fget_object(
            BUCKET,
            obj.object_name,
            str(target)
        )

        if target.stat().st_size != obj.size:
            raise RuntimeError(
                "Downloaded file size mismatch: "
                f"{obj.object_name}"
            )

    print(
        f"DOWNLOADED: {dataset} "
        f"({len(objects)} Parquet parts)",
        flush=True
    )


# ============================================================
# 5. CONSOLIDATE PARQUET USING DOCKER SPARK
# ============================================================

def consolidate_parquet(stage):

    relative_stage = (
        stage.relative_to(BASE).as_posix()
    )

    spark_code = r'''
import sys

from pyspark.sql import SparkSession, functions as F

root = sys.argv[1]

datasets = (
    "fact_sales",
    "dim_date",
    "dim_customer",
    "dim_product",
    "dim_payment",
)

keys = {
    "fact_sales": "OrderID",
    "dim_date": "DateKey",
    "dim_customer": "CustomerID",
    "dim_product": "ProductKey",
    "dim_payment": "PaymentKey",
}

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("PowerBIStarSchemaSync")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    for name in datasets:

        frame = spark.read.parquet(
            f"{root}/{name}/parts"
        )

        row_count = frame.count()

        if row_count <= 0:
            raise RuntimeError(
                f"Empty Gold dataset: {name}"
            )

        key = keys[name]

        if frame.filter(
            F.col(key).isNull()
        ).limit(1).count():
            raise RuntimeError(
                f"Null keys in {name}"
            )

        duplicate = (
            frame.groupBy(key)
            .count()
            .filter(F.col("count") > 1)
            .limit(1)
            .count()
        )

        if duplicate:
            raise RuntimeError(
                f"Duplicate keys in {name}"
            )

        frame.coalesce(1).write.mode(
            "overwrite"
        ).parquet(
            f"{root}/{name}/merged"
        )

        print(
            f"MERGED {name}: {row_count} rows",
            flush=True
        )

finally:
    spark.stop()
'''

    command = [
        "docker",
        "run",
        "--rm",
        "--entrypoint",
        "python",
        "-v",
        f"{BASE}:/app",
        IMAGE,
        "-c",
        spark_code,
        f"/app/{relative_stage}"
    ]

    print(
        "Consolidating 5 Star Schema datasets "
        "using Docker PySpark...",
        flush=True
    )

    subprocess.run(
        command,
        cwd=BASE,
        check=True
    )


# ============================================================
# 6. LOCATE COMPLETED PARQUET FILES
# ============================================================

def get_completed_files(stage):

    completed = {}

    for name in DATASETS:

        output_dir = stage / name / "merged"

        parts = list(
            output_dir.glob("part-*.parquet")
        )

        if len(parts) != 1:
            raise RuntimeError(
                f"Expected one Parquet file for {name}; "
                f"found {len(parts)}"
            )

        completed[name] = parts[0]

    return completed


# ============================================================
# 7. STAGE FILES FOR POWER BI
# ============================================================

def stage_powerbi_files(completed, run_id):

    DEST.mkdir(
        parents=True,
        exist_ok=True
    )

    staged = {}

    try:

        for name, source in completed.items():

            temporary = (
                DEST
                / f".{name}.{run_id}.parquet.new"
            )

            shutil.copyfile(
                source,
                temporary
            )

            if (
                temporary.stat().st_size
                != source.stat().st_size
            ):
                raise RuntimeError(
                    f"Local copy size mismatch: {name}"
                )

            staged[name] = temporary

    except Exception:

        for temporary in staged.values():
            temporary.unlink(missing_ok=True)

        raise

    return staged


# ============================================================
# 8. PUBLISH STABLE POWER BI FILES
# ============================================================

def publish_powerbi_files(staged, run_id):

    # Each individual file replacement is atomic.
    # The entire set of five files is not one transaction.

    for name in DATASETS:

        destination = (
            DEST / f"{name}.parquet"
        )

        os.replace(
            staged[name],
            destination
        )

        print(
            f"UPDATED: {destination}",
            flush=True
        )

    metadata = {
        "run_id": run_id,
        "datasets": list(DATASETS),
        "status": "complete"
    }

    metadata_temp = (
        DEST / "sync_metadata.json.new"
    )

    metadata_path = (
        DEST / "sync_metadata.json"
    )

    metadata_temp.write_text(
        json.dumps(metadata, indent=2),
        encoding="utf-8"
    )

    os.replace(
        metadata_temp,
        metadata_path
    )


# ============================================================
# 9. MAIN GOLD SYNC
# ============================================================

def main():

    DATA_ROOT.mkdir(
        parents=True,
        exist_ok=True
    )

    user = os.environ.get(
        "MINIO_ROOT_USER"
    )

    password = os.environ.get(
        "MINIO_ROOT_PASSWORD"
    )

    if not user or not password:
        raise RuntimeError(
            "Set MINIO_ROOT_USER and "
            "MINIO_ROOT_PASSWORD first"
        )

    client = Minio(
        "127.0.0.1:9100",
        access_key=user,
        secret_key=password,
        secure=False
    )

    # --------------------------------------------------------
    # STEP 1: Read and validate all five Gold pointers
    # --------------------------------------------------------

    run_id, pointers = (
        get_consistent_pointers(client)
    )

    print(
        f"Star Schema Gold snapshot: {run_id}",
        flush=True
    )

    # --------------------------------------------------------
    # STEP 2: Download and consolidate
    # --------------------------------------------------------

    with tempfile.TemporaryDirectory(
        prefix="gold_star_sync_",
        dir=DATA_ROOT
    ) as tmp:

        stage = Path(tmp)

        for name in DATASETS:

            download_dataset(
                client,
                name,
                pointers[name],
                stage
            )

        consolidate_parquet(stage)

        # ----------------------------------------------------
        # STEP 3: Check whether source pointers changed
        # ----------------------------------------------------

        current_run_id, current_pointers = (
            get_consistent_pointers(client)
        )

        if (
            current_run_id != run_id
            or current_pointers != pointers
        ):
            raise RuntimeError(
                "Gold pointers changed during sync. "
                "Retry after batch completes."
            )

        # ----------------------------------------------------
        # STEP 4: Validate and stage local Parquet files
        # ----------------------------------------------------

        completed = get_completed_files(stage)

        staged = stage_powerbi_files(
            completed,
            run_id
        )

        try:

            # ------------------------------------------------
            # STEP 5: Final pointer consistency check
            # ------------------------------------------------

            final_run_id, final_pointers = (
                get_consistent_pointers(client)
            )

            if (
                final_run_id != run_id
                or final_pointers != pointers
            ):
                raise RuntimeError(
                    "Gold pointers changed before "
                    "local publication."
                )

            # ------------------------------------------------
            # STEP 6: Publish local files
            # ------------------------------------------------

            publish_powerbi_files(
                staged,
                run_id
            )

        finally:

            for temporary in staged.values():
                temporary.unlink(missing_ok=True)

    print(
        "POWERBI_GOLD_SYNC_OK "
        f"run_id={run_id} tables={len(DATASETS)}",
        flush=True
    )


# ============================================================
# 10. ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()
