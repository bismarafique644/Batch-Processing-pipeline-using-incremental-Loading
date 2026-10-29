import csv
from collections import Counter

path = "data/kaggle/Ecommerce_2000.csv"

with open(path, "r", encoding="utf-8-sig", newline="") as f:
    reader = csv.DictReader(f)
    columns = reader.fieldnames
    rows = list(reader)

print("TOTAL ROWS:", len(rows))
print("TOTAL COLUMNS:", len(columns))

print("\nCOLUMN NAMES:")
print(columns)

print("\nMISSING VALUES:")
for column in columns:
    missing = sum(
        1 for row in rows
        if row[column] is None or row[column].strip() == ""
    )
    print(column, ":", missing)

print("\nFIRST 5 RECORDS:")
for row in rows[:5]:
    print(row)

duplicates = len(rows) - len({
    tuple(row[column] for column in columns)
    for row in rows
})

print("\nDUPLICATE ROWS:", duplicates)

if "OrderID" in columns:
    ids = [row["OrderID"] for row in rows]
    print("DUPLICATE ORDER IDs:", len(ids) - len(set(ids)))