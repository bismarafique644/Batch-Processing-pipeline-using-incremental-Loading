
from datetime import timedelta
from pathlib import Path
import urllib.request
import urllib.error

import pendulum
from airflow.sdk import DAG, task


BRIDGE_URL = "http://host.docker.internal:8765/run"
TOKEN_FILE = Path("/opt/airflow/bridge_token")


with DAG(
    dag_id="ecommerce_incremental_batch_elt",
    description="MySQL to MinIO Bronze, Silver and Gold incremental ELT",
    start_date=pendulum.datetime(
        2026, 10, 29, tz="Asia/Karachi"
    ),
    schedule="0 23 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "batch", "incremental"],
):

    @task(
        task_id="run_incremental_batch",
        retries=0,
        execution_timeout=timedelta(minutes=65),
    )
    def run_incremental_batch():

        # Read authentication token
        token = TOKEN_FILE.read_text(
            encoding="utf-8"
        ).strip()

        # Send request to Windows Python Bridge
        request = urllib.request.Request(
            BRIDGE_URL,
            method="POST",
            headers={
                "Authorization": f"Bearer {token}"
            },
            data=b"",
        )

        try:
            with urllib.request.urlopen(
                request,
                timeout=3650
            ) as response:

                output = response.read().decode("utf-8")
                print(output)

                # Case 1: New records processed successfully
                batch_completed = (
                    "WATERMARK COMMITTED" in output
                )

                # Case 2: No new or updated records
                no_changes = (
                    "NO CHANGES: Nothing uploaded, watermark unchanged."
                    in output
                )

                # Both are valid successful outcomes
                if batch_completed:
                    print(
                        "AIRFLOW SUCCESS: Incremental batch completed."
                    )

                elif no_changes:
                    print(
                        "AIRFLOW SUCCESS: No new records to process."
                    )

                else:
                    raise RuntimeError(
                        "Batch response did not confirm "
                        "successful completion."
                    )

        except urllib.error.HTTPError as exc:
            raise RuntimeError(
                f"Windows batch runner returned HTTP {exc.code}. "
                "Check the Windows Bridge console logs."
            ) from exc

    run_incremental_batch()
