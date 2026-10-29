
import os
import sys
import hmac
import threading
import subprocess
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BASE = Path(__file__).resolve().parent.parent
TOKEN = os.environ["BATCH_BRIDGE_TOKEN"]
LOCK = threading.Lock()


class Handler(BaseHTTPRequestHandler):

    def do_GET(self):
        if self.path != "/health":
            self.send_error(404)
            return

        self.reply(200, "Batch runner available")

    def do_POST(self):
        if self.path != "/run":
            self.send_error(404)
            return

        provided = self.headers.get("Authorization", "")
        expected = "Bearer " + TOKEN

        if not hmac.compare_digest(provided, expected):
            self.send_error(401, "Unauthorized")
            return

        if not LOCK.acquire(blocking=False):
            self.reply(409, "A batch is already running")
            return

        try:
            # STEP 1: Run existing incremental batch
            batch = subprocess.run(
                [sys.executable, str(BASE / "scripts" / "run_batch.py")],
                cwd=str(BASE),
                capture_output=True,
                text=True,
                timeout=3600
            )

            print(batch.stdout, flush=True)
            print(batch.stderr, flush=True)

            if batch.returncode != 0:
                self.reply(500, "Batch execution failed")
                return

            success_markers = (
                "WATERMARK COMMITTED",
                "NO CHANGES: Nothing uploaded, watermark unchanged."
            )

            if not any(marker in batch.stdout for marker in success_markers):
                self.reply(500, "Batch success marker missing")
                return

            # STEP 2: Automatically sync latest Gold files
            sync = subprocess.run(
                [
                    sys.executable,
                    str(BASE / "scripts" / "sync_powerbi_gold.py")
                ],
                cwd=str(BASE),
                capture_output=True,
                text=True,
                timeout=1800
            )

            print(sync.stdout, flush=True)
            print(sync.stderr, flush=True)

            if (
                sync.returncode != 0
                or "POWERBI_GOLD_SYNC_OK" not in sync.stdout
            ):
                self.reply(500, "Power BI Gold sync failed")
                return

            # STEP 3: Return success to Airflow
            message = (
                batch.stdout[-12000:]
                + "\n"
                + sync.stdout[-4000:]
            )

            self.reply(200, message)

        except subprocess.TimeoutExpired:
            self.reply(500, "Batch or Gold sync timed out")

        except Exception as exc:
            print("Batch runner error:", repr(exc), flush=True)
            self.reply(500, "Batch runner failed")

        finally:
            LOCK.release()

    def reply(self, status, message):
        body = message.encode("utf-8")
        self.send_response(status)
        self.send_header(
            "Content-Type", "text/plain; charset=utf-8"
        )
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    server = ThreadingHTTPServer(("0.0.0.0", 8765), Handler)
    print("Batch bridge listening on port 8765", flush=True)
    server.serve_forever()
