
"""
Automatic Power BI Desktop refresh for E-Commerce ELT.

Flow:
Airflow -> MySQL -> Spark -> MinIO Gold -> Local Parquet
-> Power BI Desktop -> Refresh -> Validate -> Save PBIX

Requires:
    pip install duckdb pywinauto pywin32

Usage:
    python scripts/auto_refresh_powerbi.py --baseline
    python scripts/auto_refresh_powerbi.py --once
    python scripts/auto_refresh_powerbi.py --watch

Important:
Power BI Desktop UI automation requires a logged-in,
unlocked, interactive Windows desktop session.
"""

import argparse
import base64
import json
import os
import shutil
import subprocess
import sys
import time

from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import duckdb
import win32api
import win32event
import winerror

from pywinauto import Desktop
from pywinauto.keyboard import send_keys
from pywinauto.mouse import click


# ============================================================
# 1. CONFIGURATION
# ============================================================

PROJECT = Path(
    r"C:\Users\DELL E7270\OneDrive\Desktop\ecommerce-api-elt"
)

PBIX = PROJECT / "Dashboard" / "Ecommerece_sales.pbix"

GOLD = PROJECT / "data" / "powerbi_gold"
METADATA = GOLD / "sync_metadata.json"

STATE_DIR = PROJECT / "data" / "powerbi_automation"

STATE_FILE = STATE_DIR / "last_success.json"
ATTEMPT_FILE = STATE_DIR / "last_attempt.json"
LOG_FILE = STATE_DIR / "refresh.log"

BACKUP_DIR = PROJECT / "data" / "pbix_backup"

REPORT_TITLE = "ecommerece_sales"

DATASETS = (
    "fact_sales",
    "dim_date",
    "dim_customer",
    "dim_product",
    "dim_payment",
)

CHECK_INTERVAL = 30
RETRY_INTERVAL = 300

MODEL_TIMEOUT = 900
SAVE_TIMEOUT = 120

MODEL_MATCH_CHECKS = 3
MODEL_POLL_INTERVAL = 5
MODEL_SETTLE_SECONDS = 20

REFRESH_AUTOMATION_ID = "refreshQueries"
REFRESH_PARENT_NAME = "Queries"

STATE_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# 2. LOGGING
# ============================================================

def log(message):

    timestamp = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    line = f"[{timestamp}] {message}"

    print(line, flush=True)

    with LOG_FILE.open("a", encoding="utf-8") as file:
        file.write(line + "\n")


# ============================================================
# 3. STATE MANAGEMENT
# ============================================================

def write_json_atomic(path, payload):

    path.parent.mkdir(parents=True, exist_ok=True)

    temporary = path.with_suffix(".tmp")

    temporary.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8"
    )

    os.replace(temporary, path)


def read_json(path):

    if not path.is_file():
        return {}

    return json.loads(
        path.read_text(encoding="utf-8")
    )


def read_success():

    return read_json(STATE_FILE).get("run_id")


def write_success(run_id, status):

    write_json_atomic(
        STATE_FILE,
        {
            "run_id": run_id,
            "status": status,
            "completed_at": datetime.now().isoformat(),
            "pbix": str(PBIX),
        }
    )


def read_attempt():

    return read_json(ATTEMPT_FILE)


def write_attempt(run_id, status, details=""):

    write_json_atomic(
        ATTEMPT_FILE,
        {
            "run_id": run_id,
            "status": status,
            "details": details,
            "updated_at": datetime.now().isoformat(),
        }
    )


# ============================================================
# 4. VERIFY GOLD SNAPSHOT
# ============================================================

def get_run_id():

    if not METADATA.is_file():
        raise RuntimeError(
            "Gold sync metadata file is missing"
        )

    data = read_json(METADATA)

    run_id = data.get("run_id")

    if (
        data.get("status") != "complete"
        or not isinstance(run_id, str)
        or not run_id
    ):
        raise RuntimeError(
            "Gold synchronization is incomplete"
        )

    datasets = data.get("datasets", [])

    if set(datasets) != set(DATASETS):
        raise RuntimeError(
            "Expected five Star Schema Gold datasets"
        )

    for dataset in DATASETS:

        path = GOLD / f"{dataset}.parquet"

        if not path.is_file():
            raise RuntimeError(
                f"Missing Power BI Parquet: {path}"
            )

        if path.stat().st_size == 0:
            raise RuntimeError(
                f"Empty Power BI Parquet: {path}"
            )

    return run_id


# ============================================================
# 5. EXPECTED GOLD METRICS
# ============================================================

def money_cents(value):

    amount = Decimal(
        str(value if value is not None else 0)
    )

    return int(
        (amount * 100).quantize(
            Decimal("1"),
            rounding=ROUND_HALF_UP
        )
    )


def expected_metrics():

    con = duckdb.connect(":memory:")

    try:

        metrics = {}

        for dataset in DATASETS:

            path = GOLD / f"{dataset}.parquet"

            count = con.execute(
                """
                SELECT COUNT(*)
                FROM read_parquet(?)
                """,
                [str(path)]
            ).fetchone()[0]

            metrics[dataset] = int(count)

        fact = con.execute(
            """
            SELECT
                COUNT(DISTINCT OrderID),
                COALESCE(SUM(FinalAmount), 0),
                COALESCE(SUM(Quantity), 0),
                COALESCE(SUM(TotalAmount), 0),
                COALESCE(SUM(DiscountAmount), 0)
            FROM read_parquet(?)
            """,
            [str(GOLD / "fact_sales.parquet")]
        ).fetchone()

        metrics.update({
            "distinct_orders": int(fact[0]),
            "revenue_cents": money_cents(fact[1]),
            "quantity": int(fact[2]),
            "gross_cents": money_cents(fact[3]),
            "discount_cents": money_cents(fact[4]),
        })

        return metrics

    finally:
        con.close()


# ============================================================
# 6. DAX MODEL QUERY
# ============================================================

DAX = """
EVALUATE
ROW(
    "fact_sales", COUNTROWS('fact_sales'),
    "dim_date", COUNTROWS('dim_date'),
    "dim_customer", COUNTROWS('dim_customer'),
    "dim_product", COUNTROWS('dim_product'),
    "dim_payment", COUNTROWS('dim_payment'),

    "distinct_orders",
        DISTINCTCOUNT('fact_sales'[OrderID]),

    "revenue",
        SUM('fact_sales'[FinalAmount]),

    "quantity",
        SUM('fact_sales'[Quantity]),

    "gross",
        SUM('fact_sales'[TotalAmount]),

    "discount",
        SUM('fact_sales'[DiscountAmount])
)
"""


PS_QUERY = r"""
$ErrorActionPreference = 'Stop'

$processes = @(
    Get-CimInstance Win32_Process |
        Where-Object { $_.Name -eq 'msmdsrv.exe' }
)

if ($processes.Count -ne 1) {
    throw "Expected one Power BI model engine; found $($processes.Count)"
}

$workspace = [regex]::Match(
    $processes[0].CommandLine,
    'AnalysisServicesWorkspace_[A-Za-z0-9-]+'
).Value

if (-not $workspace) {
    throw "Analysis Services workspace not found"
}

$portFile = Join-Path `
    $env:LOCALAPPDATA `
    "Microsoft\Power BI Desktop\AnalysisServicesWorkspaces\$workspace\Data\msmdsrv.port.txt"

if (-not (Test-Path -LiteralPath $portFile)) {
    throw "Power BI model port file not found"
}

$digits = (
    Get-Content -LiteralPath $portFile -Raw
) -replace '[^0-9]', ''

if (-not $digits) {
    throw "Invalid Power BI model port"
}

$port = [int]$digits

if ($port -lt 1 -or $port -gt 65535) {
    throw "Power BI model port outside valid range"
}

$conn = New-Object System.Data.OleDb.OleDbConnection(
    "Provider=MSOLAP;Data Source=localhost:$port;"
)

try {
    $conn.Open()

    $cmd = $conn.CreateCommand()
    $cmd.CommandTimeout = 20

    $cmd.CommandText = @'
__DAX__
'@

    $reader = $cmd.ExecuteReader()

    try {

        if (-not $reader.Read()) {
            throw "DAX query returned no row"
        }

        $values = @()

        for ($i = 0; $i -lt $reader.FieldCount; $i++) {

            $item = $reader.GetValue($i)

            if ($item -is [DBNull]) {
                $values += $null
            }
            else {
                $values += $item
            }
        }

        [pscustomobject]@{
            port = $port
            values = $values
        } | ConvertTo-Json -Compress -Depth 4

    }
    finally {
        $reader.Close()
    }
}
finally {
    $conn.Dispose()
}
"""


def model_metrics():

    script = PS_QUERY.replace("__DAX__", DAX)

    encoded = base64.b64encode(
        script.encode("utf-16le")
    ).decode("ascii")

    command = [
        "powershell.exe",
        "-NoProfile",
        "-NonInteractive",
        "-EncodedCommand",
        encoded,
    ]

    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=45
    )

    if result.returncode != 0:

        error = (
            result.stderr or result.stdout
        ).strip()

        raise RuntimeError(
            "Power BI DAX query failed: "
            + error[-600:]
        )

    payload = json.loads(
        result.stdout.strip()
    )

    values = payload["values"]

    if len(values) != 10:
        raise RuntimeError(
            "Unexpected DAX metric count"
        )

    metrics = {
        dataset: int(values[index])
        for index, dataset in enumerate(DATASETS)
    }

    metrics.update({
        "distinct_orders": int(values[5]),
        "revenue_cents": money_cents(values[6]),
        "quantity": int(values[7] or 0),
        "gross_cents": money_cents(values[8]),
        "discount_cents": money_cents(values[9]),
    })

    return metrics


# ============================================================
# 7. FIND / OPEN POWER BI REPORT
# ============================================================

def find_report_window(timeout=150):

    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:

        found = []

        for window in Desktop(backend="uia").windows():

            try:

                title = window.window_text().strip().lower()
                class_name = window.class_name().lower()

                if (
                    title == REPORT_TITLE
                    and class_name.startswith(
                        "windowsforms10.window"
                    )
                    and window.is_visible()
                ):
                    found.append(window)

            except Exception:
                continue

        if len(found) == 1:
            return found[0]

        if len(found) > 1:
            raise RuntimeError(
                "Multiple Power BI report windows found"
            )

        time.sleep(2)

    raise RuntimeError(
        "Power BI report window not found. "
        "Windows must be logged in and unlocked."
    )


def open_report():

    if not PBIX.is_file():
        raise FileNotFoundError(PBIX)

    try:
        return find_report_window(timeout=2)

    except RuntimeError as exc:

        if "Multiple" in str(exc):
            raise

    log("Opening Power BI report automatically")

    os.startfile(str(PBIX))

    window = find_report_window(timeout=150)

    log("POWERBI_REPORT_OPENED")

    return window


# ============================================================
# 8. VERIFIED REFRESH BUTTON - FIXED
# ============================================================

def refresh_button(window):
    """
    Power BI Desktop may expose 1, 2 or 3
    Refresh controls in UI Automation.

    The verified Home ribbon Refresh button
    is the rightmost query Refresh control.

    Previously verified rectangle:
        (L586, T72, R631, B145)

    Previously verified click position:
        (617, 87)

    If UI geometry changes unexpectedly,
    stop rather than clicking an unknown control.
    """

    # --------------------------------------------------------
    # Ensure Home tab is selected if identifiable
    # --------------------------------------------------------

    home_tabs = []

    for control in window.descendants(
        control_type="TabItem"
    ):
        try:

            if (
                control.window_text().strip().lower()
                == "home"
                and control.is_visible()
            ):
                home_tabs.append(control)

        except Exception:
            continue

    selected_home = []

    for tab in home_tabs:
        try:
            if tab.is_selected():
                selected_home.append(tab)
        except Exception:
            continue

    if not selected_home:

        if len(home_tabs) == 1:

            home_tabs[0].click_input()

            time.sleep(1)

        else:
            raise RuntimeError(
                "Cannot uniquely identify the Home ribbon"
            )

    # --------------------------------------------------------
    # Find valid Refresh controls
    # --------------------------------------------------------

    unique = {}

    for control in window.descendants(
        control_type="Button"
    ):

        try:

            name = control.window_text().strip().lower()

            automation_id = (
                control.element_info.automation_id
            )

            if name != "refresh":
                continue

            if automation_id != REFRESH_AUTOMATION_ID:
                continue

            if not control.is_visible():
                continue

            if not control.is_enabled():
                continue

            parent = control.parent()

            if (
                parent is None
                or parent.window_text().strip()
                != REFRESH_PARENT_NAME
            ):
                continue

            rect = control.rectangle()

            if (
                rect.width() <= 0
                or rect.height() <= 0
            ):
                continue

            key = (
                rect.left,
                rect.top,
                rect.right,
                rect.bottom,
            )

            unique[key] = control

        except Exception:
            continue

    controls = list(unique.values())

    log(
        f"REFRESH CONTROLS FOUND: {len(controls)}"
    )

    for index, control in enumerate(
        controls,
        start=1
    ):
        log(
            f"REFRESH CANDIDATE {index}: "
            f"{control.rectangle()}"
        )

    # --------------------------------------------------------
    # FIX: Accept 1, 2 or 3 controls
    # --------------------------------------------------------

    if not controls:
        raise RuntimeError(
            "No valid Power BI Refresh button found"
        )

    if len(controls) > 3:
        raise RuntimeError(
            "Unexpected number of Power BI "
            f"Refresh controls: {len(controls)}"
        )

    controls.sort(
        key=lambda c: c.rectangle().left
    )

    # The user's verified Home Refresh button
    # was the rightmost candidate.
    selected = controls[-1]

    rect = selected.rectangle()
    window_rect = window.rectangle()

    # --------------------------------------------------------
    # Safety checks
    # --------------------------------------------------------

    if any(
        abs(c.rectangle().top - rect.top) > 10
        for c in controls
    ):
        raise RuntimeError(
            "Refresh controls are not in the same ribbon row"
        )

    if not (
        35 <= rect.width() <= 65
        and 55 <= rect.height() <= 90
    ):
        raise RuntimeError(
            "Refresh control dimensions changed"
        )

    relative_x = rect.left - window_rect.left
    relative_y = rect.top - window_rect.top

    if not (
        350 <= relative_x <= 750
        and 40 <= relative_y <= 110
    ):
        raise RuntimeError(
            "Refresh button outside verified ribbon region"
        )

    log(
        f"REFRESH BUTTON SELECTED: {rect}"
    )

    return selected


# ============================================================
# 9. AUTOMATIC REFRESH CLICK
# ============================================================

def click_refresh(window):

    button = refresh_button(window)

    rect = button.rectangle()

    x = rect.right - 14
    y = rect.top + 15

    if not (
        rect.left < x < rect.right
        and rect.top < y < rect.bottom
    ):
        raise RuntimeError(
            "Refresh click point is outside the button"
        )

    window.set_focus()

    time.sleep(1)

    log(
        f"CLICKING REFRESH: X={x}, Y={y}"
    )

    click(
        button="left",
        coords=(x, y)
    )

    log("REFRESH_CLICKED")


# ============================================================
# 10. BACKUP PBIX
# ============================================================

def backup_report():

    BACKUP_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    timestamp = datetime.now().strftime(
        "%Y%m%d_%H%M%S"
    )

    filename = (
        "Ecommerece_sales_before_refresh_"
        f"{timestamp}.pbix"
    )

    destination = BACKUP_DIR / filename

    shutil.copy2(PBIX, destination)

    log(
        f"BACKUP CREATED: {destination.name}"
    )


# ============================================================
# 11. AUTOMATIC PBIX SAVE
# ============================================================

def save_pbix():

    window = find_report_window(timeout=15)

    before = PBIX.stat().st_mtime_ns

    window.set_focus()

    time.sleep(1)

    send_keys("^s")

    log("SAVE SHORTCUT SENT")

    deadline = time.monotonic() + SAVE_TIMEOUT

    while time.monotonic() < deadline:

        time.sleep(3)

        try:

            current = PBIX.stat()

            if (
                current.st_mtime_ns > before
                and current.st_size > 0
            ):

                signature = (
                    current.st_size,
                    current.st_mtime_ns
                )

                time.sleep(4)

                after = PBIX.stat()

                if (
                    after.st_size,
                    after.st_mtime_ns
                ) == signature:

                    log("PBIX SAVE VERIFIED")

                    return True

        except OSError:
            continue

    raise RuntimeError(
        "PBIX Save could not be verified"
    )


# ============================================================
# 12. WAIT FOR MODEL UPDATE
# ============================================================

def wait_for_model_update(run_id, expected, before):

    deadline = time.monotonic() + MODEL_TIMEOUT

    matched = 0
    last_error = None

    observed_change = False

    while time.monotonic() < deadline:

        if get_run_id() != run_id:
            raise RuntimeError(
                "Gold snapshot changed during Refresh"
            )

        try:

            actual = model_metrics()

            last_error = None

            if actual != before:
                observed_change = True

            if actual == expected and observed_change:

                matched += 1

                log(
                    f"MODEL MATCHED: "
                    f"{matched}/{MODEL_MATCH_CHECKS}"
                )

                if matched >= MODEL_MATCH_CHECKS:

                    log(
                        "MODEL METRICS MATCH LATEST GOLD"
                    )

                    return True

            else:

                matched = 0

                log(
                    f"WAITING FOR MODEL UPDATE: {actual}"
                )

        except Exception as exc:

            matched = 0
            last_error = str(exc)

            log(
                f"Model query retry: {last_error}"
            )

        time.sleep(MODEL_POLL_INTERVAL)

    raise RuntimeError(
        "Power BI model did not update before timeout. "
        f"Last error: {last_error}"
    )


# ============================================================
# 13. COMPLETE AUTOMATIC REFRESH
# ============================================================

def refresh_once(run_id):

    if not PBIX.is_file():
        raise FileNotFoundError(PBIX)

    if get_run_id() != run_id:
        raise RuntimeError(
            "Gold snapshot changed before Refresh"
        )

    log(
        f"Starting Power BI refresh for run_id={run_id}"
    )

    expected = expected_metrics()

    if get_run_id() != run_id:
        raise RuntimeError(
            "Gold changed while reading expected metrics"
        )

    log(
        f"Expected Gold metrics: {expected}"
    )

    # --------------------------------------------------------
    # Automatically open Power BI
    # --------------------------------------------------------

    window = open_report()

    window.set_focus()

    before = model_metrics()

    log(
        f"Before refresh model metrics: {before}"
    )

    already_matching = before == expected

    if already_matching:

        log(
            "MODEL_ALREADY_MATCHES_GOLD: "
            "Refresh will still be attempted. "
            "Completion needs separate verification."
        )

    # --------------------------------------------------------
    # Backup report
    # --------------------------------------------------------

    backup_report()

    # --------------------------------------------------------
    # Click Refresh automatically
    # --------------------------------------------------------

    click_refresh(window)

    write_attempt(
        run_id,
        "refresh_clicked"
    )

    # --------------------------------------------------------
    # Already matching case
    # --------------------------------------------------------

    if already_matching:

        log(
            "UNVERIFIED_REFRESH: Model matched Gold "
            "before Refresh click. Cannot independently "
            "confirm that Refresh completed."
        )

        write_attempt(
            run_id,
            "completion_unverified",
            "Metrics matched before Refresh. "
            "Automatic Save skipped."
        )

        return False

    # --------------------------------------------------------
    # Wait for updated model
    # --------------------------------------------------------

    wait_for_model_update(
        run_id,
        expected,
        before
    )

    # --------------------------------------------------------
    # Allow the model to settle
    # --------------------------------------------------------

    log(
        "Waiting for Power BI model to settle"
    )

    time.sleep(MODEL_SETTLE_SECONDS)

    # --------------------------------------------------------
    # Final validation
    # --------------------------------------------------------

    if get_run_id() != run_id:
        raise RuntimeError(
            "Gold snapshot changed before Save"
        )

    final = model_metrics()

    if final != expected:
        raise RuntimeError(
            "Power BI model no longer matches Gold. "
            f"Final metrics: {final}"
        )

    log(
        "MODEL_VALIDATED: "
        "Starting automatic PBIX Save"
    )

    # --------------------------------------------------------
    # Save report
    # --------------------------------------------------------

    save_pbix()

    if get_run_id() != run_id:
        raise RuntimeError(
            "Gold changed after PBIX Save"
        )

    # --------------------------------------------------------
    # Record successful completion
    # --------------------------------------------------------

    write_success(
        run_id,
        "model_metrics_matched_and_pbix_saved"
    )

    write_attempt(
        run_id,
        "completed"
    )

    log(
        f"POWERBI_AUTO_REFRESH_OK run_id={run_id}"
    )

    return True


# ============================================================
# 14. WATCH FOR NEW GOLD SNAPSHOTS
# ============================================================

def watch():

    log(
        "Power BI watcher started. "
        "Windows must remain logged in and unlocked."
    )

    retry_after = 0.0

    while True:

        try:

            run_id = get_run_id()

            if run_id == read_success():

                retry_after = 0.0

            else:

                attempt = read_attempt()

                if (
                    attempt.get("run_id") == run_id
                    and attempt.get("status")
                    == "completion_unverified"
                ):

                    # Avoid clicking Refresh repeatedly
                    # when the model already matched Gold.
                    pass

                elif time.monotonic() >= retry_after:

                    log(
                        f"NEW GOLD SNAPSHOT: {run_id}"
                    )

                    try:

                        refreshed = refresh_once(
                            run_id
                        )

                        if refreshed:
                            retry_after = 0.0

                        else:
                            retry_after = (
                                time.monotonic()
                                + RETRY_INTERVAL
                            )

                    except Exception as exc:

                        log(
                            "POWERBI_AUTO_REFRESH_FAILED: "
                            f"{exc}"
                        )

                        write_attempt(
                            run_id,
                            "failed",
                            str(exc)
                        )

                        retry_after = (
                            time.monotonic()
                            + RETRY_INTERVAL
                        )

        except Exception as exc:

            log(
                f"WATCHER_CHECK_FAILED: {exc}"
            )

        time.sleep(CHECK_INTERVAL)


# ============================================================
# 15. COMMAND LINE
# ============================================================

def main():

    parser = argparse.ArgumentParser()

    group = parser.add_mutually_exclusive_group(
        required=True
    )

    group.add_argument(
        "--baseline",
        action="store_true"
    )

    group.add_argument(
        "--once",
        action="store_true"
    )

    group.add_argument(
        "--watch",
        action="store_true"
    )

    args = parser.parse_args()

    mutex = win32event.CreateMutex(
        None,
        False,
        r"Local\EcommercePowerBIRefresh"
    )

    if (
        win32api.GetLastError()
        == winerror.ERROR_ALREADY_EXISTS
    ):
        win32api.CloseHandle(mutex)

        raise RuntimeError(
            "Another Power BI refresh automation "
            "is already running"
        )

    try:

        if args.baseline:

            run_id = get_run_id()

            expected = expected_metrics()

            open_report()

            actual = model_metrics()

            if actual != expected:
                raise RuntimeError(
                    "Cannot record baseline: "
                    "Power BI model does not match Gold"
                )

            write_success(
                run_id,
                "manual_baseline_metrics_match"
            )

            write_attempt(
                run_id,
                "baseline"
            )

            log(
                f"BASELINE_RECORDED run_id={run_id}"
            )

        elif args.once:

            refresh_once(
                get_run_id()
            )

        else:

            watch()

    finally:

        win32api.CloseHandle(mutex)


# ============================================================
# 16. ENTRY POINT
# ============================================================

if __name__ == "__main__":

    try:
        main()

    except KeyboardInterrupt:

        log(
            "Power BI automation stopped by user"
        )

        sys.exit(130)

    except Exception as exc:

        log(
            f"POWERBI_AUTO_REFRESH_FAILED: {exc}"
        )

        sys.exit(1)
