
import os
import json
from pathlib import Path
from datetime import datetime, date
from decimal import Decimal

import pymysql
from pymysql.cursors import DictCursor

BASE = Path(__file__).resolve().parent.parent
OUTPUT = BASE / "data" / "staging" / "silver_changes.jsonl"
OUTPUT.parent.mkdir(parents=True, exist_ok=True)


def serialize(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"Unsupported type: {type(value)}")


connection = pymysql.connect(
    host="127.0.0.1",
    port=3306,
    user="ecommerce_app",
    password=os.environ["MYSQL_PASSWORD"],
    database="ecommerce_api_db",
    cursorclass=DictCursor
)

try:
    with connection.cursor() as cursor:
        # Use separate checkpoint for development.
        cursor.execute("""
            SELECT last_modified_at, last_order_id
            FROM pipeline_watermark_test
            WHERE pipeline_name = 'ecommerce_orders_test'
        """)
        watermark = cursor.fetchone()

        if watermark is None:
            raise RuntimeError("Test watermark not found")

        cursor.execute("""
            SELECT CURRENT_TIMESTAMP(6) AS upper_time
        """)
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

        rows = cursor.fetchall()

        with OUTPUT.open("w", encoding="utf-8") as file:
            for row in rows:
                file.write(
                    json.dumps(row, default=serialize) + "\n"
                )

        print("Incremental records:", len(rows))
        for row in rows:
            print(row["OrderID"], row["FinalAmount"])

        print("Output:", OUTPUT)
        print("Watermark unchanged")

finally:
    connection.close()
