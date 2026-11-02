
"""Safely clean legacy Gold aggregate datasets from MinIO.

Legacy datasets:
    gold/daily_sales/
    gold/category_sales/
    gold/customer_summary/

Protected datasets:
    bronze/
    silver/
    gold/fact_sales/
    gold/dim_date/
    gold/dim_customer/
    gold/dim_product/
    gold/dim_payment/

Usage:
    python scripts/cleanup_old_gold.py

    python scripts/cleanup_old_gold.py --execute

Default mode is DRY RUN.
Deletion requires --execute.
"""

import os
import sys
import json
import argparse
from datetime import datetime, timezone

from minio import Minio
from minio.error import S3Error
from minio.deleteobjects import DeleteObject


# ============================================================
# 1. CONFIGURATION
# ============================================================

BUCKET = "ecommerce-batch-lake"

MINIO_ENDPOINT = "127.0.0.1:9100"

OLD_GOLD_PREFIXES = (
    "gold/daily_sales/",
    "gold/category_sales/",
    "gold/customer_summary/",
)

NEW_GOLD_TABLES = (
    "fact_sales",
    "dim_date",
    "dim_customer",
    "dim_product",
    "dim_payment",
)


# ============================================================
# 2. CONNECT TO MINIO
# ============================================================

def connect_minio():

    user = os.environ.get("MINIO_ROOT_USER")
    password = os.environ.get("MINIO_ROOT_PASSWORD")

    if not user or not password:
        raise RuntimeError(
            "MINIO_ROOT_USER and MINIO_ROOT_PASSWORD "
            "environment variables are required."
        )

    client = Minio(
        MINIO_ENDPOINT,
        access_key=user,
        secret_key=password,
        secure=False,
    )

    if not client.bucket_exists(BUCKET):
        raise RuntimeError(
            f"MinIO bucket does not exist: {BUCKET}"
        )

    return client


# ============================================================
# 3. VERIFY NEW GOLD STAR SCHEMA
# ============================================================

def verify_new_gold(client):

    print("\n=== VERIFYING NEW GOLD STAR SCHEMA ===")

    run_ids = set()

    for table in NEW_GOLD_TABLES:

        pointer_key = f"gold/{table}/current.json"

        response = client.get_object(
            BUCKET,
            pointer_key
        )

        try:
            pointer = json.loads(
                response.read().decode("utf-8")
            )
        finally:
            response.close()
            response.release_conn()

        run_id = pointer.get("run_id")
        prefix = pointer.get("prefix")

        if not isinstance(run_id, str) or not run_id:
            raise RuntimeError(
                f"Invalid run_id for {table}"
            )

        allowed_prefixes = {
            f"gold/{table}/snapshot_id={run_id}/",
            f"gold/{table}/run_id={run_id}/",
        }

        if prefix not in allowed_prefixes:
            raise RuntimeError(
                f"Invalid Gold pointer prefix: {table}"
            )

        parquet_files = [
            obj
            for obj in client.list_objects(
                BUCKET,
                prefix=prefix,
                recursive=True
            )
            if obj.object_name.endswith(".parquet")
            and obj.size > 0
        ]

        if not parquet_files:
            raise RuntimeError(
                f"No Parquet files found for {table}"
            )

        run_ids.add(run_id)

        print(
            f"VERIFIED: {table} "
            f"run_id={run_id} "
            f"files={len(parquet_files)}"
        )

    if len(run_ids) != 1:
        raise RuntimeError(
            "Gold tables have inconsistent run IDs. "
            "Stop cleanup and investigate."
        )

    run_id = next(iter(run_ids))

    print(
        f"NEW_GOLD_VERIFIED_OK run_id={run_id}"
    )

    return run_id


# ============================================================
# 4. FIND LEGACY GOLD OBJECTS
# ============================================================

def find_legacy_objects(client):

    found = {}

    print("\n=== SCANNING LEGACY GOLD PREFIXES ===")

    for prefix in OLD_GOLD_PREFIXES:

        objects = list(
            client.list_objects(
                BUCKET,
                prefix=prefix,
                recursive=True
            )
        )

        # Extra safety: delete only exact allowed prefixes.
        for obj in objects:
            if not obj.object_name.startswith(prefix):
                raise RuntimeError(
                    f"Unexpected object path: {obj.object_name}"
                )

        found[prefix] = objects

        total_bytes = sum(
            obj.size or 0
            for obj in objects
        )

        print(
            f"FOUND: {prefix} "
            f"objects={len(objects)} "
            f"bytes={total_bytes}"
        )

    return found


# ============================================================
# 5. DELETE LEGACY GOLD OBJECTS
# ============================================================

def delete_legacy_objects(client, found):

    print("\n=== DELETING LEGACY GOLD OBJECTS ===")

    for prefix, objects in found.items():

        if not objects:
            print(f"ALREADY_EMPTY: {prefix}")
            continue

        errors = client.remove_objects(
            BUCKET,
            (
                DeleteObject(obj.object_name)
                for obj in objects
            )
        )

        failures = list(errors)

        if failures:
            for failure in failures:
                print(f"DELETE_ERROR: {failure}")

            raise RuntimeError(
                f"Deletion failed for {prefix}"
            )

        print(
            f"DELETED: {prefix} "
            f"objects={len(objects)}"
        )


# ============================================================
# 6. VERIFY DELETION
# ============================================================

def verify_cleanup(client):

    print("\n=== VERIFYING CLEANUP ===")

    for prefix in OLD_GOLD_PREFIXES:

        remaining = list(
            client.list_objects(
                BUCKET,
                prefix=prefix,
                recursive=True
            )
        )

        if remaining:
            raise RuntimeError(
                f"Legacy objects still exist: {prefix}"
            )

        print(f"EMPTY: {prefix}")

    # Confirm protected Gold datasets remain published.
    run_id = verify_new_gold(client)

    print(
        "\nOLD_GOLD_CLEANUP_OK "
        f"protected_gold_run_id={run_id}"
    )


# ============================================================
# 7. MAIN
# ============================================================

def main():

    parser = argparse.ArgumentParser(
        description=(
            "Safely remove legacy Gold aggregate "
            "objects from MinIO."
        )
    )

    parser.add_argument(
        "--execute",
        action="store_true",
        help="Perform deletion. Default is dry-run."
    )

    args = parser.parse_args()

    mode = (
        "EXECUTE" if args.execute else "DRY RUN"
    )

    print(f"\nCLEANUP MODE: {mode}")
    print(f"BUCKET: {BUCKET}")

    client = connect_minio()

    # Verify the current Star Schema first.
    verified_run_id = verify_new_gold(client)

    # Find only the explicitly allowed old prefixes.
    found = find_legacy_objects(client)

    total_objects = sum(
        len(objects)
        for objects in found.values()
    )

    if not args.execute:

        print("\n=== DRY RUN: NO DATA DELETED ===")

        for prefix, objects in found.items():
            for obj in objects:
                print(
                    f"WOULD_DELETE: {obj.object_name}"
                )

        print(
            "\nOLD_GOLD_DRY_RUN_OK "
            f"objects={total_objects} "
            f"protected_gold_run_id={verified_run_id}"
        )

        return

    # Extra safety before destructive operation.
    print(
        "\nWARNING: Legacy Gold objects will be "
        "removed from the current MinIO listing."
    )

    delete_legacy_objects(client, found)

    verify_cleanup(client)


# ============================================================
# 8. ENTRY POINT
# ============================================================

if __name__ == "__main__":

    try:
        main()
    except (S3Error, Exception) as exc:
        print(
            f"\nOLD_GOLD_CLEANUP_FAILED: {exc}",
            file=sys.stderr
        )
        sys.exit(1)
