
import os
import csv
import pymysql
from pathlib import Path
from datetime import datetime
from decimal import Decimal

BASE = Path(__file__).resolve().parent.parent
CSV_FILE = BASE / "data" / "kaggle" / "Ecommerce_2000.csv"

columns = [
    "OrderID", "CustomerID", "FirstName", "LastName",
    "State", "City", "OrderDate", "OrderSeason",
    "OrderDayOfWeek", "ProductCategory", "ProductSubCategory",
    "UnitPrice", "Quantity", "TotalAmount",
    "DiscountPercent", "DiscountAmount", "FinalAmount",
    "PaymentMethod", "DeviceType", "MembershipTier",
    "ShippingDays", "Returned", "ReviewScore"
]

connection = pymysql.connect(
    host="127.0.0.1",
    port=3306,
    user="ecommerce_app",
    password=os.environ["MYSQL_PASSWORD"],
    database="ecommerce_api_db",
    charset="utf8mb4",
    autocommit=False
)

seen_ids = set()
rows_to_insert = []
total_rows = 0
duplicates = 0

with open(CSV_FILE, "r", encoding="utf-8-sig", newline="") as f:
    reader = csv.DictReader(f)

    for row in reader:
        total_rows += 1

        order_id = row["OrderID"]

        if order_id in seen_ids:
            duplicates += 1
            continue

        seen_ids.add(order_id)

        row["OrderDate"] = datetime.strptime(
            row["OrderDate"], "%m/%d/%Y"
        ).date()

        for field in [
            "UnitPrice", "TotalAmount", "DiscountPercent",
            "DiscountAmount", "FinalAmount"
        ]:
            row[field] = Decimal(row[field])

        for field in [
            "Quantity", "ShippingDays", "ReviewScore"
        ]:
            row[field] = int(row[field])

        rows_to_insert.append(
            tuple(row[col] for col in columns)
        )

column_names = ", ".join(f"`{col}`" for col in columns)
placeholders = ", ".join(["%s"] * len(columns))

query = f"""
INSERT INTO sales_orders ({column_names})
VALUES ({placeholders})
"""

try:
    with connection.cursor() as cursor:
        # Batch inserts are faster than inserting one row at a time.
        batch_size = 500

        for start in range(0, len(rows_to_insert), batch_size):
            batch = rows_to_insert[start:start + batch_size]
            cursor.executemany(query, batch)

    connection.commit()

    print("IMPORT SUCCESSFUL")
    print("CSV ROWS:", total_rows)
    print("DUPLICATE ORDER IDS SKIPPED:", duplicates)
    print("IMPORTED ORDERS:", len(rows_to_insert))

except Exception:
    connection.rollback()
    raise

finally:
    connection.close()
