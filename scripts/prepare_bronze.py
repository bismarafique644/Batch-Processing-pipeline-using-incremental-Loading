
import os
import json
import pymysql

from pathlib import Path
from datetime import date, datetime
from decimal import Decimal
from pymysql.cursors import DictCursor

BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT = BASE_DIR / "data" / "staging" / "orders_incremental.jsonl"
OUTPUT.parent.mkdir(parents=True, exist_ok=True)

connection = pymysql.connect(
    host="127.0.0.1",
    port=3306,
    user="ecommerce_app",
    password=os.environ["MYSQL_PASSWORD"],
    database="ecommerce_api_db",
    cursorclass=DictCursor
)

def serialize(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"Unsupported value: {type(value)}")

try:
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT last_modified_at, last_order_id
            FROM pipeline_watermark
            WHERE pipeline_name = 'ecommerce_orders'
        """)
        watermark = cursor.fetchone()

        if watermark is None:
            raise RuntimeError("Watermark not found")

        # Fixed extraction boundary for this batch
        cursor.execute("SELECT CURRENT_TIMESTAMP(6) AS upper_time")
        upper_time = cursor.fetchone()["upper_time"]

        cursor.execute("""
            SELECT *
            FROM sales_orders
            WHERE (
                last_modified_at > %s
                OR (
                    last_modified_at = %s
                    AND OrderID > %s
                )
            )
            AND last_modified_at < %s
            ORDER BY last_modified_at, OrderID
        """, (
            watermark["last_modified_at"],
            watermark["last_modified_at"],
            watermark["last_order_id"],
            upper_time
        ))

        count = 0

        with OUTPUT.open("w", encoding="utf-8") as file:
            while True:
                rows = cursor.fetchmany(500)
                if not rows:
                    break

                for row in rows:
                    file.write(
                        json.dumps(row, default=serialize) + "\n"
                    )
                    count += 1

        print("Batch upper time:", upper_time)
        print("Incremental records:", count)
        print("Staging file:", OUTPUT)
        print("Watermark unchanged")

finally:
    connection.close()
