
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("EcommerceSilverIncrementalUpsert")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

EXISTING = "/app/data/parquet/silver_orders"
CHANGES = "/app/data/staging/silver_changes.jsonl"
OUTPUT = "/app/data/parquet/silver_orders_merged"
REJECTED = "/app/data/parquet/silver_changes_rejected"

try:
    # 1. Read existing Silver snapshot
    previous = spark.read.parquet(EXISTING)

    print("Existing Silver:", previous.count())

    # 2. Read new incremental batch
    changes = spark.read.json(CHANGES)

    print("Incoming changes:", changes.count())

    # 3. Clean text fields
    text_columns = [
        "OrderID", "CustomerID", "FirstName", "LastName",
        "State", "City", "OrderSeason", "OrderDayOfWeek",
        "ProductCategory", "ProductSubCategory",
        "PaymentMethod", "DeviceType",
        "MembershipTier", "Returned"
    ]

    for name in text_columns:
        changes = changes.withColumn(
            name,
            F.when(
                F.trim(F.col(name)) == "",
                F.lit(None)
            ).otherwise(F.trim(F.col(name)))
        )

    for name in [
        "OrderSeason", "OrderDayOfWeek",
        "PaymentMethod", "DeviceType",
        "MembershipTier", "Returned"
    ]:
        changes = changes.withColumn(
            name, F.initcap(F.col(name))
        )

    # 4. Convert to existing Silver datatypes
    for field in previous.schema.fields:
        name = field.name

        if name == "OrderDate":
            changes = changes.withColumn(
                name, F.to_date(F.col(name))
            )
        elif name == "last_modified_at":
            changes = changes.withColumn(
                name, F.to_timestamp(F.col(name))
            )
        else:
            changes = changes.withColumn(
                name, F.col(name).cast(field.dataType)
            )

    changes = changes.select(*previous.columns)

    # 5. Validate incoming records
    required = [
        "OrderID", "last_modified_at", "is_deleted"
    ]

    errors = [
        F.when(
            F.col(name).isNull(),
            F.lit(f"{name} missing")
        )
        for name in required
    ]

    # Validate business fields for active records
    active = F.col("is_deleted") == F.lit(False)

    for name in [
        "CustomerID", "OrderDate", "Quantity",
        "UnitPrice", "TotalAmount",
        "DiscountPercent", "DiscountAmount",
        "FinalAmount"
    ]:
        errors.append(
            F.when(
                active & F.col(name).isNull(),
                F.lit(f"{name} missing")
            )
        )

    errors.extend([
        F.when(
            active & (F.col("Quantity") <= 0),
            F.lit("Invalid quantity")
        ),
        F.when(
            active & (
                (F.col("UnitPrice") < 0) |
                (F.col("TotalAmount") < 0) |
                (F.col("FinalAmount") < 0) |
                (F.col("DiscountAmount") < 0)
            ),
            F.lit("Negative monetary value")
        ),
        F.when(
            active & (
                (F.col("DiscountPercent") < 0) |
                (F.col("DiscountPercent") > 100)
            ),
            F.lit("Invalid discount")
        ),
        F.when(
            active & (F.col("ShippingDays") < 0),
            F.lit("Invalid shipping days")
        ),
        F.when(
            active & (
                (F.col("ReviewScore") < 1) |
                (F.col("ReviewScore") > 5)
            ),
            F.lit("Invalid review score")
        ),
        F.when(
            active & (F.col("OrderDate") > F.current_date()),
            F.lit("Future order date")
        )
    ])

    checked = changes.withColumn(
        "rejection_reasons",
        F.filter(
            F.array(*errors),
            lambda x: x.isNotNull()
        )
    )

    rejected = checked.filter(
        F.size(F.col("rejection_reasons")) > 0
    )

    clean = checked.filter(
        F.size(F.col("rejection_reasons")) == 0
    ).drop("rejection_reasons")

    rejected_count = rejected.count()

    print("Rejected changes:", rejected_count)

    rejected.write.mode("overwrite").parquet(REJECTED)

    if rejected_count > 0:
        raise RuntimeError(
            "Invalid incoming records. UPSERT stopped."
        )

    # 6. Merge existing + incoming orders
    previous = previous.withColumn(
        "_priority", F.lit(0)
    )

    clean = clean.withColumn(
        "_priority", F.lit(1)
    )

    combined = previous.unionByName(clean)

    window = Window.partitionBy("OrderID").orderBy(
        F.col("last_modified_at").desc(),
        F.col("_priority").desc()
    )

    merged = (
        combined
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "_priority")
    )

    # 7. Verify uniqueness
    total = merged.count()
    unique = merged.select("OrderID").distinct().count()

    if total != unique:
        raise RuntimeError("Duplicate OrderIDs remain")

    active_count = merged.filter(
        F.col("is_deleted") == False
    ).count()

    deleted_count = merged.filter(
        F.col("is_deleted") == True
    ).count()

    # 8. Save merged Silver snapshot
    merged.write.mode("overwrite").parquet(OUTPUT)

    # 9. Show report
    print("\n===== SILVER UPSERT REPORT =====")
    print("Total unique orders:", total)
    print("Active orders:", active_count)
    print("Soft-deleted orders:", deleted_count)
    print("Rejected changes:", rejected_count)

    print("\nUpdated order:")
    merged.filter(
        F.col("OrderID") == "ORD-010001"
    ).select(
        "OrderID", "FinalAmount", "last_modified_at"
    ).show(truncate=False)

    print("Silver output:", OUTPUT)
    print("Watermark unchanged")

finally:
    spark.stop()
