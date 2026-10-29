
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ==========================================
# 1. Spark Session
# ==========================================

spark = (
    SparkSession.builder
    .appName("EcommerceSilverCleaning")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

BRONZE = "/app/data/parquet/bronze_orders"
SILVER = "/app/data/parquet/silver_orders_next"
REJECTED = "/app/data/parquet/rejected_orders_next"

try:
    # ==========================================
    # 2. Read Bronze Parquet
    # ==========================================

    df = spark.read.parquet(BRONZE)

    print("Bronze records:", df.count())

    # ==========================================
    # 3. Text Cleaning
    # ==========================================

    text_columns = [
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

    for col_name in text_columns:
        df = df.withColumn(
            col_name,
            F.trim(F.col(col_name))
        )

        # Empty strings -> NULL
        df = df.withColumn(
            col_name,
            F.when(
                F.col(col_name) == "",
                F.lit(None)
            ).otherwise(F.col(col_name))
        )

    # Standardize selected text columns
    standardize_columns = [
        "OrderSeason",
        "OrderDayOfWeek",
        "PaymentMethod",
        "DeviceType",
        "MembershipTier",
        "Returned"
    ]

    for col_name in standardize_columns:
        df = df.withColumn(
            col_name,
            F.initcap(F.col(col_name))
        )

    # ==========================================
    # 4. Data Type Conversion
    # ==========================================

    decimal_columns = {
        "UnitPrice": "decimal(12,2)",
        "TotalAmount": "decimal(14,2)",
        "DiscountPercent": "decimal(8,2)",
        "DiscountAmount": "decimal(14,2)",
        "FinalAmount": "decimal(14,2)"
    }

    for col_name, dtype in decimal_columns.items():
        df = df.withColumn(
            col_name,
            F.col(col_name).cast(dtype)
        )

    integer_columns = [
        "Quantity",
        "ShippingDays",
        "ReviewScore"
    ]

    for col_name in integer_columns:
        df = df.withColumn(
            col_name,
            F.col(col_name).cast("int")
        )

    df = (
        df
        .withColumn(
            "OrderDate",
            F.to_date(F.col("OrderDate"))
        )
        .withColumn(
            "last_modified_at",
            F.to_timestamp(F.col("last_modified_at"))
        )
        .withColumn(
            "is_deleted",
            F.col("is_deleted").cast("boolean")
        )
    )

    # ==========================================
    # 5. Required Fields Validation
    # ==========================================

    required_fields = [
        "OrderID",
        "CustomerID",
        "OrderDate",
        "last_modified_at",
        "Quantity",
        "UnitPrice",
        "TotalAmount",
        "DiscountPercent",
        "DiscountAmount",
        "FinalAmount",
        "is_deleted"
    ]

    for col_name in required_fields:
        df = df.withColumn(
            f"_error_missing_{col_name}",
            F.when(
                F.col(col_name).isNull(),
                F.lit(
                    f"{col_name} is missing or invalid"
                )
            )
        )

    # ==========================================
    # 6. Business Rule Validation
    # ==========================================

    df = (
        df
        .withColumn(
            "_error_quantity_value",
            F.when(
                F.col("Quantity").isNotNull()
                & (F.col("Quantity") <= 0),
                F.lit("Quantity must be positive")
            )
        )
        .withColumn(
            "_error_negative_price",
            F.when(
                (F.col("UnitPrice") < 0)
                | (F.col("TotalAmount") < 0)
                | (F.col("DiscountAmount") < 0)
                | (F.col("FinalAmount") < 0),
                F.lit("Negative monetary value")
            )
        )
        .withColumn(
            "_error_discount_range",
            F.when(
                (F.col("DiscountPercent") < 0)
                | (F.col("DiscountPercent") > 100),
                F.lit(
                    "Discount percent must be between 0 and 100"
                )
            )
        )
        .withColumn(
            "_error_shipping_days",
            F.when(
                F.col("ShippingDays") < 0,
                F.lit("ShippingDays cannot be negative")
            )
        )
        .withColumn(
            "_error_review_score",
            F.when(
                (F.col("ReviewScore") < 1)
                | (F.col("ReviewScore") > 5),
                F.lit("ReviewScore must be between 1 and 5")
            )
        )
        .withColumn(
            "_error_order_date",
            F.when(
                F.col("OrderDate") > F.current_date(),
                F.lit("OrderDate cannot be in the future")
            )
        )
    )

    # ==========================================
    # 7. Collect Rejection Reasons
    # ==========================================

    error_columns = [
        col_name
        for col_name in df.columns
        if col_name.startswith("_error_")
    ]

    # PySpark 3.5.6 compatible:
    # Filter removes NULL entries from error array.
    df = df.withColumn(
        "rejection_reasons",
        F.filter(
            F.array(
                *[F.col(c) for c in error_columns]
            ),
            lambda x: x.isNotNull()
        )
    )

    valid = df.filter(
        F.size(F.col("rejection_reasons")) == 0
    )

    rejected = df.filter(
        F.size(F.col("rejection_reasons")) > 0
    )

    # ==========================================
    # 8. Deduplicate Valid Records
    # ==========================================

    business_columns = [
        col_name
        for col_name in valid.columns
        if not col_name.startswith("_error_")
        and col_name != "rejection_reasons"
    ]

    valid = valid.withColumn(
        "_row_hash",
        F.sha2(
            F.to_json(
                F.struct(
                    *[
                        F.col(c)
                        for c in business_columns
                    ]
                )
            ),
            256
        )
    )

    window = (
        Window
        .partitionBy("OrderID")
        .orderBy(
            F.col("last_modified_at").desc(),
            F.col("_row_hash").desc()
        )
    )

    silver = (
        valid
        .withColumn(
            "_rn",
            F.row_number().over(window)
        )
        .filter(F.col("_rn") == 1)
        .drop(
            "_rn",
            "_row_hash",
            "rejection_reasons"
        )
        .drop(*error_columns)
    )

    rejected = rejected.drop(*error_columns)

    # ==========================================
    # 9. Data Quality Report
    # ==========================================

    bronze_count = df.count()
    valid_count = valid.count()
    silver_count = silver.count()
    rejected_count = rejected.count()

    print("\n========== DATA QUALITY REPORT ==========")
    print("Bronze records:", bronze_count)
    print("Valid source rows:", valid_count)
    print("Silver unique orders:", silver_count)
    print("Rejected records:", rejected_count)
    print(
        "Duplicate valid rows removed:",
        valid_count - silver_count
    )

    # ==========================================
    # 10. Write Parquet Outputs
    # ==========================================

    silver.write.mode("overwrite").parquet(SILVER)

    rejected.write.mode("overwrite").parquet(REJECTED)

    print("\nCLEANING COMPLETED")
    print("Silver output:", SILVER)
    print("Rejected output:", REJECTED)
    print("Watermark unchanged")

finally:
    spark.stop()
