
"""Run one bounded MySQL -> MinIO Bronze/Silver/Gold Star Schema batch.

Windows dependencies:
    pip install pymysql minio

Required Docker image:
    ecommerce-spark:local

Gold outputs:
    fact_sales
    dim_date
    dim_customer
    dim_product
    dim_payment
"""

import json
import os
import subprocess
import sys
import uuid

from datetime import datetime, date
from decimal import Decimal
from getpass import getpass
from io import BytesIO
from pathlib import Path

import pymysql
from pymysql.cursors import DictCursor

from minio import Minio
from minio.error import S3Error


# ============================================================
# 1. CONFIGURATION
# ============================================================

BASE = Path(__file__).resolve().parent.parent

RUN_ROOT = BASE / "data" / "auto_runs"

BUCKET = "ecommerce-batch-lake"

PIPELINE = "ecommerce_orders"

IMAGE = "ecommerce-spark:local"

GOLD_TABLES = (
    "fact_sales",
    "dim_date",
    "dim_customer",
    "dim_product",
    "dim_payment"
)


# ============================================================
# 2. CREDENTIAL HANDLING
# ============================================================

def secret(name, prompt):
    value = os.getenv(name)

    return value if value else getpass(prompt)


# ============================================================
# 3. JSON SERIALIZATION
# ============================================================

def convert(value):

    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if isinstance(value, Decimal):
        return str(value)

    raise TypeError(
        f"Cannot encode {type(value)}"
    )


# ============================================================
# 4. DOWNLOAD PREVIOUS SILVER SNAPSHOT
# ============================================================

def download_snapshot(client, dst):

    try:
        response = client.get_object(
            BUCKET,
            "silver/orders/current.json"
        )

        try:
            pointer = json.loads(
                response.read().decode("utf8")
            )

        finally:
            response.close()
            response.release_conn()

        prefix = pointer["prefix"]

        objects = [
            obj
            for obj in client.list_objects(
                BUCKET,
                prefix=prefix,
                recursive=True
            )
            if obj.object_name.endswith(".parquet")
        ]

        if not objects:
            raise RuntimeError(
                "MinIO Silver pointer exists but "
                "no Parquet objects were found"
            )

        dst.mkdir(
            parents=True,
            exist_ok=True
        )

        for obj in objects:

            filename = Path(
                obj.object_name
            ).name

            target = dst / filename

            client.fget_object(
                BUCKET,
                obj.object_name,
                str(target)
            )

        print(
            "Previous Silver from MinIO:",
            prefix
        )

    except S3Error as exc:

        if exc.code != "NoSuchKey":
            raise

        legacy = (
            BASE
            / "data"
            / "parquet"
            / "silver_orders"
        )

        if (
            legacy.exists()
            and list(legacy.glob("*.parquet"))
        ):

            import shutil

            dst.mkdir(
                parents=True,
                exist_ok=True
            )

            for src in legacy.glob("*.parquet"):

                shutil.copy2(
                    src,
                    dst / src.name
                )

            print(
                "Previous Silver from initial local snapshot"
            )

        else:

            print(
                "No previous Silver; "
                "building initial snapshot"
            )


# ============================================================
# 5. VALIDATE PARQUET OUTPUT
# ============================================================

def validate_parquet_folder(directory):

    if not directory.exists():
        raise RuntimeError(
            f"Missing output directory: {directory}"
        )

    parts = sorted(
        directory.rglob("*.parquet")
    )

    if not parts:
        raise RuntimeError(
            f"No Parquet files found at {directory}"
        )

    return parts


def validate_all_outputs(run):

    print(
        "Validating Bronze, Silver and "
        "Gold Star Schema outputs..."
    )

    validate_parquet_folder(
        run / "bronze"
    )

    validate_parquet_folder(
        run / "silver"
    )

    for name in GOLD_TABLES:

        validate_parquet_folder(
            run / "gold" / name
        )

    print(
        "OUTPUT VALIDATION OK: "
        "Bronze, Silver and 5 Gold tables found."
    )


# ============================================================
# 6. UPLOAD PARQUET FILES TO MINIO
# ============================================================

def upload_folder(client, directory, prefix):

    parts = validate_parquet_folder(
        directory
    )

    for file in parts:

        relative_path = (
            file.relative_to(directory).as_posix()
        )

        object_key = (
            prefix + relative_path
        )

        client.fput_object(
            BUCKET,
            object_key,
            str(file),
            content_type="application/octet-stream"
        )

        result = client.stat_object(
            BUCKET,
            object_key
        )

        if (
            result.size
            != file.stat().st_size
        ):
            raise RuntimeError(
                f"Upload size mismatch: {object_key}"
            )

    print(
        f"UPLOADED: {prefix} "
        f"({len(parts)} Parquet files)"
    )

    return len(parts)


# ============================================================
# 7. WRITE JSON METADATA TO MINIO
# ============================================================

def write_json(client, key, obj):

    data = json.dumps(
        obj,
        default=convert,
        indent=2
    ).encode("utf8")

    client.put_object(
        BUCKET,
        key,
        BytesIO(data),
        len(data),
        content_type="application/json"
    )


# ============================================================
# 8. MAIN BATCH RUNNER
# ============================================================

def main():

    os.chdir(BASE)

    # --------------------------------------------------------
    # Credentials
    # --------------------------------------------------------

    mysql_password = secret(
        "MYSQL_PASSWORD",
        "MySQL app password: "
    )

    minio_user = os.getenv(
        "MINIO_ROOT_USER",
        "batchadmin"
    )

    minio_password = secret(
        "MINIO_ROOT_PASSWORD",
        "MinIO password: "
    )

    # --------------------------------------------------------
    # MinIO connection
    # --------------------------------------------------------

    client = Minio(
        "127.0.0.1:9100",
        access_key=minio_user,
        secret_key=minio_password,
        secure=False
    )

    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)

    # --------------------------------------------------------
    # MySQL connection
    # --------------------------------------------------------

    conn = pymysql.connect(
        host="127.0.0.1",
        port=3306,
        user="ecommerce_app",
        password=mysql_password,
        database="ecommerce_api_db",
        cursorclass=DictCursor,
        autocommit=False,
        charset="utf8mb4"
    )

    # --------------------------------------------------------
    # Generate unique batch ID
    # --------------------------------------------------------

    run_id = (
        datetime.now().strftime("%Y%m%d_%H%M%S")
        + "_"
        + uuid.uuid4().hex[:8]
    )

    run = RUN_ROOT / run_id

    run.mkdir(
        parents=True,
        exist_ok=False
    )

    count = 0
    last = None
    old = None
    watermark_committed = False

    try:

        # ====================================================
        # 9. READ PREVIOUS WATERMARK
        # ====================================================

        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT
                    last_modified_at,
                    last_order_id
                FROM pipeline_watermark
                WHERE pipeline_name = %s
                """,
                (PIPELINE,)
            )

            old = cur.fetchone()

            if old is None:
                raise RuntimeError(
                    "Missing production watermark"
                )

            print(
                "Previous watermark:",
                old["last_modified_at"],
                old["last_order_id"]
            )

        # Close the implicit transaction opened by the
        # watermark SELECT before starting a new snapshot.
        conn.rollback()

        # ====================================================
        # 10. BEGIN CONSISTENT MYSQL SNAPSHOT
        # ====================================================

        with conn.cursor() as cur:

            cur.execute(
                "SET SESSION TRANSACTION "
                "ISOLATION LEVEL REPEATABLE READ"
            )

            cur.execute(
                "START TRANSACTION WITH CONSISTENT SNAPSHOT"
            )

            cur.execute(
                """
                SELECT DATE_SUB(
                    CURRENT_TIMESTAMP(6),
                    INTERVAL 2 SECOND
                ) AS upper_time
                """
            )

            upper_time = (
                cur.fetchone()["upper_time"]
            )

            # ================================================
            # 11. INCREMENTAL EXTRACTION
            # ================================================

            cur.execute(
                """
                SELECT *
                FROM sales_orders
                WHERE
                    (
                        last_modified_at > %s
                        OR
                        (
                            last_modified_at = %s
                            AND OrderID > %s
                        )
                    )
                    AND last_modified_at < %s
                ORDER BY
                    last_modified_at ASC,
                    OrderID ASC
                """,
                (
                    old["last_modified_at"],
                    old["last_modified_at"],
                    old["last_order_id"],
                    upper_time
                )
            )

            source_file = (
                run / "source.jsonl"
            )

            with source_file.open(
                "w",
                encoding="utf8"
            ) as file:

                while True:

                    batch = cur.fetchmany(500)

                    if not batch:
                        break

                    for row in batch:

                        file.write(
                            json.dumps(
                                row,
                                default=convert,
                                ensure_ascii=False
                            )
                            + "\n"
                        )

                        count += 1

                        last = (
                            row["last_modified_at"],
                            row["OrderID"]
                        )

        # Close extraction's read-only transaction.
        conn.rollback()

        print(
            "Extracted:",
            count,
            "upper_time:",
            upper_time,
            "run_id:",
            run_id
        )

        # ====================================================
        # 12. HANDLE NO-CHANGE BATCH
        # ====================================================

        if count == 0:

            print(
                "NO CHANGES: Nothing uploaded, "
                "watermark unchanged."
            )

            return

        # ====================================================
        # 13. DOWNLOAD PREVIOUS SILVER
        # ====================================================

        download_snapshot(
            client,
            run / "previous_silver"
        )

        # ====================================================
        # 14. EXECUTE PYSPARK PROCESSING
        # ====================================================

        command = [
            "docker",
            "run",
            "--rm",
            "--entrypoint",
            "python",
            "-v",
            f"{BASE}:/app",
            IMAGE,
            "/app/scripts/spark_batch.py",
            run_id
        ]

        print(
            "Running PySpark "
            "Bronze + Silver + Gold Star Schema..."
        )

        subprocess.run(
            command,
            cwd=BASE,
            check=True
        )

        # ====================================================
        # 15. VERIFY SPARK OUTPUTS
        # ====================================================

        validate_all_outputs(run)

        # ====================================================
        # 16. UPLOAD BRONZE
        # ====================================================

        bronze_prefix = (
            f"bronze/orders/batch_id={run_id}/"
        )

        upload_folder(
            client,
            run / "bronze",
            bronze_prefix
        )

        # ====================================================
        # 17. UPLOAD SILVER
        # ====================================================

        silver_prefix = (
            f"silver/orders/snapshot_id={run_id}/"
        )

        upload_folder(
            client,
            run / "silver",
            silver_prefix
        )

        # ====================================================
        # 18. UPLOAD 5 STAR SCHEMA TABLES
        # ====================================================

        gold_prefixes = {}

        for name in GOLD_TABLES:

            prefix = (
                f"gold/{name}/snapshot_id={run_id}/"
            )

            upload_folder(
                client,
                run / "gold" / name,
                prefix
            )

            gold_prefixes[name] = prefix

        print(
            "All 5 Gold Star Schema tables uploaded."
        )

        # ====================================================
        # 19. CREATE BATCH MANIFEST
        # ====================================================

        manifest = {
            "run_id": run_id,
            "pipeline": PIPELINE,
            "bronze_prefix": bronze_prefix,
            "silver_prefix": silver_prefix,
            "gold_tables": list(GOLD_TABLES),
            "gold_prefixes": gold_prefixes,
            "source_count": count,
            "previous_watermark": {
                "last_modified_at": convert(
                    old["last_modified_at"]
                ),
                "last_order_id": old["last_order_id"]
            },
            "new_watermark": {
                "last_modified_at": convert(last[0]),
                "last_order_id": last[1]
            }
        }

        write_json(
            client,
            f"manifests/{run_id}.json",
            manifest
        )

        # ====================================================
        # 20. PUBLISH SILVER POINTER
        # ====================================================

        # Retain existing pointer-before-watermark sequence.
        # Note: MinIO pointers and MySQL commit are not atomic.

        write_json(
            client,
            "silver/orders/current.json",
            {
                "prefix": silver_prefix,
                "run_id": run_id
            }
        )

        # ====================================================
        # 21. PUBLISH GOLD STAR SCHEMA POINTERS
        # ====================================================

        for name in GOLD_TABLES:

            write_json(
                client,
                f"gold/{name}/current.json",
                {
                    "prefix": gold_prefixes[name],
                    "run_id": run_id
                }
            )

        print(
            "Silver and 5 Gold snapshot pointers published."
        )

        # ====================================================
        # 22. COMMIT MYSQL WATERMARK
        # ====================================================

        # Compare-and-swap update.
        # If another batch advanced the watermark,
        # this update must not overwrite it.

        with conn.cursor() as cur:

            cur.execute(
                """
                UPDATE pipeline_watermark
                SET
                    last_modified_at = %s,
                    last_order_id = %s
                WHERE
                    pipeline_name = %s
                    AND last_modified_at = %s
                    AND last_order_id = %s
                """,
                (
                    last[0],
                    last[1],
                    PIPELINE,
                    old["last_modified_at"],
                    old["last_order_id"]
                )
            )

            if cur.rowcount != 1:

                raise RuntimeError(
                    "Watermark changed concurrently; "
                    "rollback and replay required"
                )

        conn.commit()

        watermark_committed = True

        # ====================================================
        # 23. SUCCESS
        # ====================================================

        print(
            "SUCCESS: Bronze, Silver and "
            "5 Gold Star Schema tables published to MinIO."
        )

        print(
            "WATERMARK COMMITTED:",
            last[0],
            last[1]
        )

        print(
            "STAR_SCHEMA_PUBLISHED:",
            ", ".join(GOLD_TABLES)
        )

        print(
            "MinIO console: http://127.0.0.1:9101"
        )

    except Exception:

        if not watermark_committed:

            conn.rollback()

            print(
                "FAILED: MySQL watermark not committed "
                "by this run. Check MinIO pointers "
                "before retrying.",
                file=sys.stderr
            )

        else:

            print(
                "FAILED AFTER WATERMARK COMMIT: "
                "inspect published outputs.",
                file=sys.stderr
            )

        raise

    finally:

        conn.close()


# ============================================================
# 24. ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()
