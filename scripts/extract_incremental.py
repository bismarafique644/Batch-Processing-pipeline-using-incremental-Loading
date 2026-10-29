
import os
import pymysql
from pymysql.cursors import DictCursor

conn = pymysql.connect(
    host="127.0.0.1",
    user="ecommerce_app",
    password=os.environ["MYSQL_PASSWORD"],
    database="ecommerce_api_db",
    cursorclass=DictCursor
)

try:
    with conn.cursor() as cursor:

        # Last successful watermark read karo
        cursor.execute("""
            SELECT last_modified_at, last_order_id
            FROM pipeline_watermark
            WHERE pipeline_name = 'ecommerce_orders'
        """)
        watermark = cursor.fetchone()

        if not watermark:
            raise RuntimeError("Pipeline watermark not found")

        print("Current watermark:", watermark)

        # Timestamp + ID incremental extraction
        cursor.execute("""
            SELECT *
            FROM sales_orders
            WHERE last_modified_at > %s
               OR (
                   last_modified_at = %s
                   AND OrderID > %s
               )
            ORDER BY last_modified_at ASC, OrderID ASC
        """, (
            watermark["last_modified_at"],
            watermark["last_modified_at"],
            watermark["last_order_id"]
        ))

        rows = cursor.fetchall()

        print("Incremental records found:", len(rows))

        for row in rows[:5]:
            print(
                row["OrderID"],
                row["last_modified_at"],
                row["FinalAmount"]
            )

finally:
    conn.close()
