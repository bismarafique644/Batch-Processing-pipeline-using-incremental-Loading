
"""One-time rebuild of Gold Star Schema from existing Silver.

Does not access MySQL.
Does not update watermark.
Does not overwrite Bronze or Silver.
Does not publish MinIO pointers.
"""

from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window


BASE = Path("/app")
SILVER_PATH = BASE / "data" / "migration_silver"
GOLD_PATH = BASE / "data" / "migration_gold"

TABLES = (
    "fact_sales",
    "dim_date",
    "dim_customer",
    "dim_product",
    "dim_payment",
)


spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("RebuildGoldFromSilver")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


def validate_keys(df, table_name, key):
    if df.filter(F.col(key).isNull()).limit(1).count():
        raise RuntimeError(
            f"{table_name}: null primary key"
        )

    duplicates = (
        df.groupBy(key)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )

    if duplicates:
        raise RuntimeError(
            f"{table_name}: duplicate primary key"
        )


try:
    # ========================================================
    # 1. READ EXISTING SILVER
    # ========================================================

    if not SILVER_PATH.exists():
        raise RuntimeError(
            "Downloaded Silver folder not found"
        )

    silver = spark.read.parquet(str(SILVER_PATH))

    silver_count = silver.count()

    if silver_count == 0:
        raise RuntimeError("Silver snapshot is empty")

    validate_keys(silver, "silver", "OrderID")

    active = silver.filter(
        F.col("is_deleted") == F.lit(False)
    )

    active_count = active.count()

    if active_count == 0:
        raise RuntimeError("No active Silver orders")

    print("SILVER_ROWS:", silver_count, flush=True)
    print("ACTIVE_ROWS:", active_count, flush=True)

    # ========================================================
    # 2. GENERATE STAR SCHEMA KEYS
    # ========================================================

    star = (
        active
        .withColumn(
            "DateKey",
            F.date_format(
                F.col("OrderDate"),
                "yyyyMMdd"
            ).cast("int")
        )
        .withColumn(
            "ProductKey",
            F.sha2(
                F.to_json(
                    F.struct(
                        F.col("ProductCategory"),
                        F.col("ProductSubCategory")
                    ),
                    options={"ignoreNullFields": "false"}
                ),
                256
            )
        )
        .withColumn(
            "PaymentKey",
            F.sha2(
                F.to_json(
                    F.struct(
                        F.col("PaymentMethod"),
                        F.col("DeviceType")
                    ),
                    options={"ignoreNullFields": "false"}
                ),
                256
            )
        )
    )

    # ========================================================
    # 3. DIM DATE
    # ========================================================

    dim_date = (
        star
        .select("DateKey", "OrderDate")
        .dropDuplicates(["DateKey"])
        .withColumn("Year", F.year("OrderDate"))
        .withColumn("Month", F.month("OrderDate"))
        .withColumn(
            "MonthName",
            F.date_format("OrderDate", "MMMM")
        )
        .withColumn("Quarter", F.quarter("OrderDate"))
        .withColumn(
            "DayOfWeek",
            F.date_format("OrderDate", "EEEE")
        )
    )

    # ========================================================
    # 4. DIM CUSTOMER
    # ========================================================

    customer_window = (
        Window
        .partitionBy("CustomerID")
        .orderBy(
            F.col("last_modified_at").desc(),
            F.col("OrderID").desc()
        )
    )

    dim_customer = (
        star
        .withColumn(
            "_customer_rank",
            F.row_number().over(customer_window)
        )
        .filter(F.col("_customer_rank") == 1)
        .select(
            "CustomerID",
            "FirstName",
            "LastName",
            "City",
            "State",
            "MembershipTier"
        )
    )

    # ========================================================
    # 5. DIM PRODUCT
    # ========================================================

    dim_product = (
        star
        .select(
            "ProductKey",
            "ProductCategory",
            "ProductSubCategory"
        )
        .dropDuplicates(["ProductKey"])
    )

    # ========================================================
    # 6. DIM PAYMENT
    # ========================================================

    dim_payment = (
        star
        .select(
            "PaymentKey",
            "PaymentMethod",
            "DeviceType"
        )
        .dropDuplicates(["PaymentKey"])
    )

    # ========================================================
    # 7. FACT SALES
    # ========================================================

    fact_sales = (
        star
        .select(
            "OrderID",
            "DateKey",
            "CustomerID",
            "ProductKey",
            "PaymentKey",
            "Quantity",
            "UnitPrice",
            "TotalAmount",
            "DiscountPercent",
            "DiscountAmount",
            "FinalAmount",
            "ShippingDays",
            "Returned",
            "ReviewScore"
        )
    )

    # ========================================================
    # 8. VALIDATION
    # ========================================================

    star_tables = [
        ("fact_sales", fact_sales, "OrderID"),
        ("dim_date", dim_date, "DateKey"),
        ("dim_customer", dim_customer, "CustomerID"),
        ("dim_product", dim_product, "ProductKey"),
        ("dim_payment", dim_payment, "PaymentKey"),
    ]

    for name, df, key in star_tables:
        validate_keys(df, name, key)

    fact_count = fact_sales.count()

    if fact_count != active_count:
        raise RuntimeError(
            "Fact count != active Silver count"
        )

    silver_revenue = (
        active
        .agg(F.sum("FinalAmount").alias("total"))
        .first()["total"]
    )

    fact_revenue = (
        fact_sales
        .agg(F.sum("FinalAmount").alias("total"))
        .first()["total"]
    )

    if silver_revenue != fact_revenue:
        raise RuntimeError(
            "Fact revenue != active Silver revenue"
        )

    relationships = [
        ("DateKey", dim_date),
        ("CustomerID", dim_customer),
        ("ProductKey", dim_product),
        ("PaymentKey", dim_payment),
    ]

    for key, dimension in relationships:
        missing = (
            fact_sales
            .select(key)
            .distinct()
            .join(
                dimension.select(key),
                on=key,
                how="left_anti"
            )
            .limit(1)
            .count()
        )

        if missing:
            raise RuntimeError(
                f"Missing dimension reference: {key}"
            )

    # ========================================================
    # 9. WRITE GOLD LOCALLY
    # ========================================================

    for name, df, key in star_tables:
        output = GOLD_PATH / name

        df.write.mode("overwrite").parquet(str(output))

        print(
            f"GOLD_TABLE_OK {name} rows={df.count()}",
            flush=True
        )

    print(
        f"REBUILD_GOLD_OK tables=5 "
        f"silver={silver_count} "
        f"active={active_count} "
        f"fact_sales={fact_count} "
        f"revenue={fact_revenue}",
        flush=True
    )

finally:
    spark.stop()
