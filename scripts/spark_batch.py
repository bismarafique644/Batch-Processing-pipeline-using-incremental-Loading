
"""Incremental batch: JSONL -> Bronze -> Silver -> Gold Star Schema."""

import sys
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window


# ============================================================
# 1. INITIALIZATION
# ============================================================

run_id = sys.argv[1]
base = Path("/app/data/auto_runs") / run_id

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("EcommerceIncrementalBatch")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


try:
    # ========================================================
    # 2. READ INCREMENTAL DATA
    # ========================================================

    source_path = base / "source.jsonl"
    incoming = spark.read.json(str(source_path))

    source_count = incoming.count()

    if source_count == 0:
        raise RuntimeError(
            "Empty extraction must be skipped by runner"
        )

    # ========================================================
    # 3. DATA TYPE CONVERSION
    # ========================================================

    decimal_types = {
        "UnitPrice": "decimal(12,2)",
        "TotalAmount": "decimal(14,2)",
        "DiscountPercent": "decimal(8,2)",
        "DiscountAmount": "decimal(14,2)",
        "FinalAmount": "decimal(14,2)"
    }

    for name, dtype in decimal_types.items():
        incoming = incoming.withColumn(
            name,
            F.col(name).cast(dtype)
        )

    for name in (
        "Quantity",
        "ShippingDays",
        "ReviewScore"
    ):
        incoming = incoming.withColumn(
            name,
            F.col(name).cast("int")
        )

    incoming = (
        incoming
        .withColumn("OrderDate", F.to_date("OrderDate"))
        .withColumn(
            "last_modified_at",
            F.to_timestamp("last_modified_at")
        )
        .withColumn(
            "is_deleted",
            F.col("is_deleted").cast("boolean")
        )
    )

    # ========================================================
    # 4. TEXT CLEANING
    # ========================================================

    text_cols = [
        "OrderID",
        "CustomerID",
        "FirstName",
        "LastName",
        "State",
        "City",
        "OrderSeason",
        "OrderDayOfWeek",
        "ProductCategory",
        "ProductSubCategory",
        "PaymentMethod",
        "DeviceType",
        "MembershipTier",
        "Returned"
    ]

    for name in text_cols:
        incoming = incoming.withColumn(
            name,
            F.when(
                F.trim(F.col(name)) == "",
                F.lit(None)
            ).otherwise(
                F.trim(F.col(name))
            )
        )

    for name in (
        "OrderSeason",
        "OrderDayOfWeek",
        "PaymentMethod",
        "DeviceType",
        "MembershipTier",
        "Returned"
    ):
        incoming = incoming.withColumn(
            name,
            F.initcap(F.col(name))
        )

    # ========================================================
    # 5. DATA VALIDATION
    # ========================================================

    required = [
        "OrderID",
        "last_modified_at",
        "is_deleted",
        "CustomerID",
        "OrderDate",
        "Quantity",
        "UnitPrice",
        "TotalAmount",
        "DiscountPercent",
        "DiscountAmount",
        "FinalAmount"
    ]

    errors = [
        F.when(
            F.col(name).isNull(),
            F.lit(name + " is missing or invalid")
        )
        for name in required
    ]

    errors += [
        F.when(
            F.col("Quantity") <= 0,
            "Quantity must be positive"
        ),
        F.when(
            (F.col("UnitPrice") < 0) |
            (F.col("TotalAmount") < 0) |
            (F.col("DiscountAmount") < 0) |
            (F.col("FinalAmount") < 0),
            "Negative amount"
        ),
        F.when(
            (F.col("DiscountPercent") < 0) |
            (F.col("DiscountPercent") > 100),
            "Invalid discount percentage"
        ),
        F.when(
            F.col("ShippingDays") < 0,
            "Negative shipping days"
        ),
        F.when(
            (F.col("ReviewScore") < 1) |
            (F.col("ReviewScore") > 5),
            "Invalid review score"
        ),
        F.when(
            F.col("OrderDate") > F.current_date(),
            "Future order date"
        )
    ]

    checked = incoming.withColumn(
        "rejection_reasons",
        F.filter(
            F.array(*errors),
            lambda x: x.isNotNull()
        )
    )

    bad = checked.filter(
        F.size("rejection_reasons") > 0
    )

    bad_count = bad.count()

    bad.write.mode("overwrite").parquet(
        str(base / "rejected")
    )

    if bad_count:
        raise RuntimeError(
            f"{bad_count} invalid source rows; "
            f"inspect data/auto_runs/{run_id}/rejected"
        )

    clean = checked.drop("rejection_reasons")

    # ========================================================
    # 6. BRONZE LAYER
    # ========================================================

    # Preserve original extracted values in Parquet.

    spark.read.json(
        str(source_path)
    ).write.mode("overwrite").parquet(
        str(base / "bronze")
    )

    # ========================================================
    # 7. SILVER LAYER - UPSERT
    # ========================================================

    previous_path = base / "previous_silver"

    if (
        previous_path.exists()
        and list(previous_path.glob("*.parquet"))
    ):
        previous = spark.read.parquet(
            str(previous_path)
        )

        previous = previous.select(
            *[
                F.col(field.name)
                .cast(field.dataType)
                .alias(field.name)
                for field in clean.schema.fields
            ]
        )

        previous = previous.withColumn(
            "_priority", F.lit(0)
        )

        clean = clean.withColumn(
            "_priority", F.lit(1)
        )

        combined = previous.unionByName(clean)

    else:
        combined = clean.withColumn(
            "_priority", F.lit(1)
        )

    ordering = (
        Window
        .partitionBy("OrderID")
        .orderBy(
            F.col("last_modified_at").desc(),
            F.col("_priority").desc()
        )
    )

    merged = (
        combined
        .withColumn(
            "_rn",
            F.row_number().over(ordering)
        )
        .filter(F.col("_rn") == 1)
        .drop("_rn", "_priority")
    )

    silver_count = merged.count()

    if (
        merged.select("OrderID")
        .distinct()
        .count() != silver_count
    ):
        raise RuntimeError(
            "Silver contains duplicate OrderIDs"
        )

    merged.write.mode("overwrite").parquet(
        str(base / "silver")
    )

    # ========================================================
    # 8. ACTIVE ORDERS
    # ========================================================

    active = merged.filter(
        F.col("is_deleted") == F.lit(False)
    )

    active_count = active.count()

    # ========================================================
    # 9. GENERATE STAR SCHEMA KEYS
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
    # 10. DIM DATE
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
        .withColumn(
            "Quarter",
            F.quarter("OrderDate")
        )
        .withColumn(
            "DayOfWeek",
            F.date_format("OrderDate", "EEEE")
        )
    )

    # ========================================================
    # 11. DIM CUSTOMER
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
    # 12. DIM PRODUCT
    # ========================================================

    # ProductKey represents category + subcategory.
    # Source dataset does not contain ProductID.

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
    # 13. DIM PAYMENT
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
    # 14. FACT SALES
    # ========================================================

    # Grain: one row per OrderID.

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
    # 15. VALIDATE STAR SCHEMA
    # ========================================================

    star_tables = [
        ("fact_sales", fact_sales, "OrderID"),
        ("dim_date", dim_date, "DateKey"),
        ("dim_customer", dim_customer, "CustomerID"),
        ("dim_product", dim_product, "ProductKey"),
        ("dim_payment", dim_payment, "PaymentKey")
    ]

    for name, df, key in star_tables:

        if (
            df.filter(F.col(key).isNull())
            .limit(1)
            .count() > 0
        ):
            raise RuntimeError(
                f"{name} contains null primary keys"
            )

        if (
            df.groupBy(key)
            .count()
            .filter(F.col("count") > 1)
            .limit(1)
            .count() > 0
        ):
            raise RuntimeError(
                f"{name} contains duplicate primary keys"
            )

    # ========================================================
    # 16. FACT AND SILVER RECONCILIATION
    # ========================================================

    fact_count = fact_sales.count()

    if fact_count != active_count:
        raise RuntimeError(
            "FactSales row count differs from active Silver"
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
            "FactSales revenue differs from active Silver"
        )

    # ========================================================
    # 17. CHECK FACT FOREIGN KEYS
    # ========================================================

    relationships = [
        ("DateKey", dim_date),
        ("CustomerID", dim_customer),
        ("ProductKey", dim_product),
        ("PaymentKey", dim_payment)
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
                f"FactSales has missing dimension references: {key}"
            )

    # ========================================================
    # 18. WRITE ONLY 5 GOLD STAR SCHEMA TABLES
    # ========================================================

    for name, df, key in star_tables:

        df.write.mode("overwrite").parquet(
            str(base / "gold" / name)
        )

    # ========================================================
    # 19. SUCCESS LOG
    # ========================================================

    print(
        f"BATCH_OK "
        f"source={source_count} "
        f"silver={silver_count} "
        f"active={active_count} "
        f"fact_sales={fact_count} "
        f"rejected=0"
    )

    print("STAR_SCHEMA_OK tables=5")

finally:
    spark.stop()
