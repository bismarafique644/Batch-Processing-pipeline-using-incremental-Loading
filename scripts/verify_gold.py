
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("VerifySilverGold")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

silver = spark.read.parquet(
    "/app/data/verify_silver/latest.parquet"
)

active = silver.filter(F.col("is_deleted") == False)

daily = spark.read.parquet(
    "/app/data/verify_gold/daily_sales"
)

category = spark.read.parquet(
    "/app/data/verify_gold/category_sales"
)

customer = spark.read.parquet(
    "/app/data/verify_gold/customer_summary"
)

silver_count = active.count()

silver_revenue = active.agg(
    F.sum(F.col("FinalAmount").cast("decimal(18,2)"))
).first()[0]

print("\n=== SILVER ACTIVE ===")
print("Orders:", silver_count)
print("Revenue:", silver_revenue)

checks = [
    ("Daily Sales", daily, "revenue"),
    ("Category Sales", category, "revenue"),
    ("Customer Summary", customer, "total_spend"),
]

for name, df, revenue_col in checks:

    result = df.agg(
        F.sum("order_count").alias("orders"),
        F.sum(revenue_col).alias("revenue")
    ).first()

    orders_match = result["orders"] == silver_count
    revenue_match = result["revenue"] == silver_revenue

    print(f"\n=== {name.upper()} ===")
    print("Orders:", result["orders"])
    print("Revenue:", result["revenue"])
    print("Orders match:", orders_match)
    print("Revenue match:", revenue_match)

    if not orders_match or not revenue_match:
        print("VALIDATION FAILED")
    else:
        print("VALIDATION PASSED")

spark.stop()
