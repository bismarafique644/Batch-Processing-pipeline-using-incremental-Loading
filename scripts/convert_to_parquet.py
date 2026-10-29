
from pyspark.sql import SparkSession
from pathlib import Path

spark = (
    SparkSession.builder
    .appName("EcommerceBatchBronze")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

input_path = "/app/data/staging/orders_incremental.jsonl"
output_path = "/app/data/parquet/bronze_orders"

try:
    # Read extracted incremental batch
    df = spark.read.json(input_path)

    print("Total records:", df.count())

    # Write Parquet files
    df.coalesce(1).write.mode("overwrite").parquet(output_path)

    print("Parquet created successfully!")
    print("Output:", output_path)

finally:
    spark.stop()
