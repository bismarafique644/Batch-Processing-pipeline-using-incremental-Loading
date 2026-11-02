
"""Airflow DAG: MySQL -> Bronze -> Silver -> Gold Star Schema -> Power BI.

The DAG calls the authenticated Windows Bridge.
The Bridge runs:
    1. run_batch.py
    2. sync_powerbi_gold.py

Gold Star Schema:
    fact_sales
    dim_date
    dim_customer
    dim_product
    dim_payment
"""

from datetime import timedelta
from pathlib import Path

import urllib.request
import urllib.error

import pendulum

from airflow.sdk import DAG, task


# ============================================================
# 1. CONFIGURATION
# ============================================================

BRIDGE_URL = "http://host.docker.internal:8765/run"

TOKEN_FILE = Path("/opt/airflow/bridge_token")

NO_CHANGES_MARKER = (
    "NO CHANGES: Nothing uploaded, watermark unchanged."
)

WATERMARK_MARKER = "WATERMARK COMMITTED"

STAR_SCHEMA_MARKER = "STAR_SCHEMA_PUBLISHED:"

POWERBI_SYNC_MARKER = "POWERBI_GOLD_SYNC_OK"


# ============================================================
# 2. DAG DEFINITION
# ============================================================

with DAG(
    dag_id="ecommerce_incremental_batch_elt",

    description=(
        "Incremental MySQL to MinIO Bronze/Silver/Gold "
        "Star Schema and Power BI Gold synchronization"
    ),

    start_date=pendulum.datetime(
        2026, 10, 29,
        tz="Asia/Karachi"
    ),

    schedule="0 23 * * *",

    catchup=False,

    max_active_runs=1,

    tags=[
        "ecommerce",
        "batch",
        "incremental",
        "star-schema",
        "powerbi"
    ],
):

    # ========================================================
    # 3. INCREMENTAL BATCH TASK
    # ========================================================

    @task(
        task_id="run_incremental_batch",
        retries=0,
        execution_timeout=timedelta(minutes=95),
    )
    def run_incremental_batch():

        # ====================================================
        # 4. READ AUTHENTICATION TOKEN
        # ====================================================

        if not TOKEN_FILE.exists():
            raise RuntimeError(
                "Bridge authentication token file is missing."
            )

        token = TOKEN_FILE.read_text(
            encoding="utf-8"
        ).strip()

        if not token:
            raise RuntimeError(
                "Bridge authentication token is empty."
            )

        # ====================================================
        # 5. CREATE HTTP REQUEST
        # ====================================================

        request = urllib.request.Request(
            BRIDGE_URL,
            method="POST",
            headers={
                "Authorization": f"Bearer {token}"
            },
            data=b"",
        )

        print(
            "Starting incremental batch through Windows Bridge...",
            flush=True
        )

        # ====================================================
        # 6. CALL WINDOWS BRIDGE
        # ====================================================

        try:

            with urllib.request.urlopen(
                request,
                timeout=5500
            ) as response:

                output = response.read().decode(
                    "utf-8"
                )

                print(output, flush=True)

                # ============================================
                # 7. IDENTIFY BATCH RESULT
                # ============================================

                batch_completed = (
                    WATERMARK_MARKER in output
                )

                no_changes = (
                    NO_CHANGES_MARKER in output
                )

                star_schema_published = (
                    STAR_SCHEMA_MARKER in output
                )

                powerbi_synced = (
                    POWERBI_SYNC_MARKER in output
                )

                # ============================================
                # 8. VALIDATE SUCCESSFUL BATCH
                # ============================================

                if batch_completed:

                    if not star_schema_published:
                        raise RuntimeError(
                            "Batch committed the watermark, "
                            "but Star Schema publication "
                            "was not confirmed."
                        )

                    if not powerbi_synced:
                        raise RuntimeError(
                            "Batch succeeded, but Power BI "
                            "Gold synchronization was "
                            "not confirmed."
                        )

                    print(
                        "AIRFLOW SUCCESS: Incremental batch "
                        "completed, 5 Star Schema datasets "
                        "published and Power BI files synced.",
                        flush=True
                    )

                # ============================================
                # 9. VALIDATE NO-CHANGE BATCH
                # ============================================

                elif no_changes:

                    if not powerbi_synced:
                        raise RuntimeError(
                            "No source changes, but Power BI "
                            "Gold synchronization failed "
                            "or was not confirmed."
                        )

                    print(
                        "AIRFLOW SUCCESS: No new records. "
                        "Existing Gold Star Schema "
                        "files synchronized.",
                        flush=True
                    )

                else:

                    raise RuntimeError(
                        "Bridge response did not confirm "
                        "successful batch completion."
                    )

        # ====================================================
        # 10. HTTP ERROR HANDLING
        # ====================================================

        except urllib.error.HTTPError as exc:

            error_body = exc.read().decode(
                "utf-8",
                errors="replace"
            )

            print(
                f"Bridge HTTP error: {exc.code}",
                flush=True
            )

            print(
                error_body[-4000:],
                flush=True
            )

            raise RuntimeError(
                f"Windows Bridge returned HTTP {exc.code}. "
                "Check bridge logs for the actual error."
            ) from exc

        # ====================================================
        # 11. CONNECTION ERROR HANDLING
        # ====================================================

        except urllib.error.URLError as exc:

            raise RuntimeError(
                "Cannot connect to Windows Bridge "
                "at host.docker.internal:8765. "
                "Check bridge process, port binding, "
                "Docker connectivity and firewall. "
                f"Reason: {exc.reason}"
            ) from exc

        # ====================================================
        # 12. SOCKET TIMEOUT HANDLING
        # ====================================================

        except TimeoutError as exc:

            raise RuntimeError(
                "Windows Bridge request timed out. "
                "Check whether the batch or Gold sync "
                "is still running before retrying."
            ) from exc

    # ========================================================
    # 13. REGISTER TASK
    # ========================================================

    run_incremental_batch()
