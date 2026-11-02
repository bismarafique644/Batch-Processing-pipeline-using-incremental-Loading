
"""Authenticated Windows bridge for Airflow batch orchestration.

Flow:
    Airflow -> /run
    -> run_batch.py
    -> sync_powerbi_gold.py
    -> response to Airflow

Gold model:
    fact_sales
    dim_date
    dim_customer
    dim_product
    dim_payment
"""

import os
import sys
import hmac
import threading
import subprocess

from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


# ============================================================
# 1. CONFIGURATION
# ============================================================

BASE = Path(__file__).resolve().parent.parent

TOKEN = os.environ["BATCH_BRIDGE_TOKEN"]

HOST = "0.0.0.0"
PORT = 8765

BATCH_TIMEOUT = 3600
SYNC_TIMEOUT = 1800

LOCK = threading.Lock()

GOLD_TABLES = (
    "fact_sales",
    "dim_date",
    "dim_customer",
    "dim_product",
    "dim_payment"
)


# ============================================================
# 2. HTTP HANDLER
# ============================================================

class Handler(BaseHTTPRequestHandler):

    # --------------------------------------------------------
    # HEALTH ENDPOINT
    # --------------------------------------------------------

    def do_GET(self):

        if self.path != "/health":
            self.send_error(404)
            return

        self.reply(
            200,
            "Batch runner available"
        )

    # --------------------------------------------------------
    # RUN ENDPOINT
    # --------------------------------------------------------

    def do_POST(self):

        if self.path != "/run":
            self.send_error(404)
            return

        # ====================================================
        # 3. AUTHENTICATION
        # ====================================================

        provided = self.headers.get(
            "Authorization",
            ""
        )

        expected = "Bearer " + TOKEN

        if not hmac.compare_digest(
            provided,
            expected
        ):
            self.send_error(
                401,
                "Unauthorized"
            )
            return

        # ====================================================
        # 4. PREVENT CONCURRENT BRIDGE RUNS
        # ====================================================

        if not LOCK.acquire(blocking=False):

            self.reply(
                409,
                "A batch is already running"
            )
            return

        try:

            print(
                "STARTING INCREMENTAL BATCH",
                flush=True
            )

            # =================================================
            # 5. RUN MYSQL -> BRONZE -> SILVER -> GOLD
            # =================================================

            batch = subprocess.run(
                [
                    sys.executable,
                    str(
                        BASE
                        / "scripts"
                        / "run_batch.py"
                    )
                ],
                cwd=str(BASE),
                capture_output=True,
                text=True,
                timeout=BATCH_TIMEOUT,
                check=False
            )

            print(
                batch.stdout,
                flush=True
            )

            print(
                batch.stderr,
                flush=True
            )

            if batch.returncode != 0:

                self.reply(
                    500,
                    "Batch execution failed. "
                    "Check bridge logs."
                )
                return

            # =================================================
            # 6. VALIDATE BATCH RESULT
            # =================================================

            batch_success = (
                "WATERMARK COMMITTED"
                in batch.stdout
            )

            no_changes = (
                "NO CHANGES: Nothing uploaded, "
                "watermark unchanged."
                in batch.stdout
            )

            if not (
                batch_success or no_changes
            ):

                self.reply(
                    500,
                    "Batch success marker missing"
                )
                return

            if batch_success:

                if (
                    "STAR_SCHEMA_PUBLISHED:"
                    not in batch.stdout
                ):

                    self.reply(
                        500,
                        "Star Schema publication "
                        "marker missing"
                    )
                    return

                print(
                    "BATCH SUCCESS: "
                    "5 Star Schema tables published.",
                    flush=True
                )

            else:

                print(
                    "NO SOURCE CHANGES: "
                    "Watermark unchanged.",
                    flush=True
                )

            # =================================================
            # 7. SYNC LATEST GOLD DATA FOR POWER BI
            # =================================================

            print(
                "STARTING POWER BI GOLD SYNC",
                flush=True
            )

            sync = subprocess.run(
                [
                    sys.executable,
                    str(
                        BASE
                        / "scripts"
                        / "sync_powerbi_gold.py"
                    )
                ],
                cwd=str(BASE),
                capture_output=True,
                text=True,
                timeout=SYNC_TIMEOUT,
                check=False
            )

            print(
                sync.stdout,
                flush=True
            )

            print(
                sync.stderr,
                flush=True
            )

            if sync.returncode != 0:

                self.reply(
                    500,
                    "Power BI Gold sync failed. "
                    "Check bridge logs."
                )
                return

            if (
                "POWERBI_GOLD_SYNC_OK"
                not in sync.stdout
            ):

                self.reply(
                    500,
                    "Power BI Gold sync "
                    "success marker missing"
                )
                return

            # =================================================
            # 8. RETURN SUCCESS TO AIRFLOW
            # =================================================

            print(
                "PIPELINE SUCCESS",
                flush=True
            )

            print(
                "Gold tables:",
                ", ".join(GOLD_TABLES),
                flush=True
            )

            message = (
                batch.stdout[-12000:]
                + "\n"
                + sync.stdout[-4000:]
            )

            self.reply(
                200,
                message
            )

        # ====================================================
        # 9. ERROR HANDLING
        # ====================================================

        except subprocess.TimeoutExpired as exc:

            print(
                "PROCESS TIMEOUT:",
                repr(exc),
                flush=True
            )

            self.reply(
                500,
                "Batch or Gold sync timed out"
            )

        except Exception as exc:

            print(
                "Batch runner error:",
                repr(exc),
                flush=True
            )

            self.reply(
                500,
                "Batch runner failed"
            )

        finally:

            LOCK.release()

    # ========================================================
    # 10. HTTP RESPONSE
    # ========================================================

    def reply(self, status, message):

        body = message.encode("utf-8")

        self.send_response(status)

        self.send_header(
            "Content-Type",
            "text/plain; charset=utf-8"
        )

        self.send_header(
            "Content-Length",
            str(len(body))
        )

        self.end_headers()

        self.wfile.write(body)


# ============================================================
# 11. START BRIDGE SERVER
# ============================================================

if __name__ == "__main__":

    server = ThreadingHTTPServer(
        (HOST, PORT),
        Handler
    )

    print(
        f"Batch bridge listening on port {PORT}",
        flush=True
    )

    print(
        "Star Schema Gold tables:",
        ", ".join(GOLD_TABLES),
        flush=True
    )

    server.serve_forever()
