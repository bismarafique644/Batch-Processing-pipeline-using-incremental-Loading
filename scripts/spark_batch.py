"""One bounded incremental batch: JSONL -> Bronze / Silver / Gold Parquet."""
import sys
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

run_id = sys.argv[1]
base = Path('/app/data/auto_runs') / run_id
spark = SparkSession.builder.master('local[2]').appName('EcommerceIncrementalBatch').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
try:
    incoming = spark.read.json(str(base / 'source.jsonl'))
    source_count = incoming.count()
    if source_count == 0:
        raise RuntimeError('Empty extraction must be skipped by runner')

    # Canonical types; timestamps retain microsecond precision.
    decimal_types = {'UnitPrice':'decimal(12,2)', 'TotalAmount':'decimal(14,2)',
                     'DiscountPercent':'decimal(8,2)', 'DiscountAmount':'decimal(14,2)',
                     'FinalAmount':'decimal(14,2)'}
    for name, dtype in decimal_types.items():
        incoming = incoming.withColumn(name, F.col(name).cast(dtype))
    for name in ('Quantity', 'ShippingDays', 'ReviewScore'):
        incoming = incoming.withColumn(name, F.col(name).cast('int'))
    incoming = (incoming.withColumn('OrderDate', F.to_date('OrderDate'))
        .withColumn('last_modified_at', F.to_timestamp('last_modified_at'))
        .withColumn('is_deleted', F.col('is_deleted').cast('boolean')))
    text_cols = ['OrderID','CustomerID','FirstName','LastName','State','City',
        'OrderSeason','OrderDayOfWeek','ProductCategory','ProductSubCategory',
        'PaymentMethod','DeviceType','MembershipTier','Returned']
    for name in text_cols:
        incoming = incoming.withColumn(name, F.when(F.trim(F.col(name)) == '', F.lit(None)).otherwise(F.trim(F.col(name))))
    for name in ('OrderSeason','OrderDayOfWeek','PaymentMethod','DeviceType','MembershipTier','Returned'):
        incoming = incoming.withColumn(name, F.initcap(F.col(name)))

    required = ['OrderID','last_modified_at','is_deleted','CustomerID','OrderDate',
        'Quantity','UnitPrice','TotalAmount','DiscountPercent','DiscountAmount','FinalAmount']
    errors = [F.when(F.col(n).isNull(), F.lit(n + ' is missing or invalid')) for n in required]
    errors += [
        F.when(F.col('Quantity') <= 0, 'Quantity must be positive'),
        F.when((F.col('UnitPrice') < 0) | (F.col('TotalAmount') < 0) | (F.col('DiscountAmount') < 0) | (F.col('FinalAmount') < 0), 'Negative amount'),
        F.when((F.col('DiscountPercent') < 0) | (F.col('DiscountPercent') > 100), 'Invalid discount percentage'),
        F.when(F.col('ShippingDays') < 0, 'Negative shipping days'),
        F.when((F.col('ReviewScore') < 1) | (F.col('ReviewScore') > 5), 'Invalid review score'),
        F.when(F.col('OrderDate') > F.current_date(), 'Future order date')]
    checked = incoming.withColumn('rejection_reasons', F.filter(F.array(*errors), lambda x: x.isNotNull()))
    bad = checked.filter(F.size('rejection_reasons') > 0)
    bad_count = bad.count()
    bad.write.mode('overwrite').parquet(str(base / 'rejected'))
    if bad_count:
        raise RuntimeError(f'{bad_count} invalid source rows; outputs not published; inspect data/auto_runs/{run_id}/rejected')
    clean = checked.drop('rejection_reasons')

    # Bronze includes source-exact record values: infer schema from raw JSONL, no cleaning.
    spark.read.json(str(base / 'source.jsonl')).write.mode('overwrite').parquet(str(base / 'bronze'))

    previous_path = base / 'previous_silver'
    if previous_path.exists() and list(previous_path.glob('*.parquet')):
        previous = spark.read.parquet(str(previous_path))
        # Align to canonical schema, for legacy Silver snapshots.
        previous = previous.select(*[F.col(field.name).cast(field.dataType).alias(field.name) for field in clean.schema.fields])
        previous = previous.withColumn('_priority', F.lit(0))
        clean = clean.withColumn('_priority', F.lit(1))
        combined = previous.unionByName(clean)
    else:
        combined = clean.withColumn('_priority', F.lit(1))
    ordering = Window.partitionBy('OrderID').orderBy(F.col('last_modified_at').desc(), F.col('_priority').desc())
    merged = (combined.withColumn('_rn', F.row_number().over(ordering))
        .filter(F.col('_rn') == 1).drop('_rn', '_priority'))
    silver_count = merged.count()
    if merged.select('OrderID').distinct().count() != silver_count:
        raise RuntimeError('Silver has duplicate OrderIDs')
    merged.write.mode('overwrite').parquet(str(base / 'silver'))

    active = merged.filter(F.col('is_deleted') == F.lit(False))
    active_count = active.count()
    daily = (active.groupBy('OrderDate').agg(
        F.count('*').alias('order_count'),
        F.sum('FinalAmount').cast('decimal(18,2)').alias('revenue'),
        F.sum('Quantity').alias('units_sold')).orderBy('OrderDate'))
    category = (active.groupBy('ProductCategory').agg(
        F.count('*').alias('order_count'),
        F.sum('FinalAmount').cast('decimal(18,2)').alias('revenue')).orderBy('ProductCategory'))
    customer = (active.groupBy('CustomerID').agg(
        F.count('*').alias('order_count'),
        F.sum('FinalAmount').cast('decimal(18,2)').alias('total_spend'),
        F.max('OrderDate').alias('last_order_date')))
    for name, df in [('daily_sales',daily),('category_sales',category),('customer_summary',customer)]:
        df.write.mode('overwrite').parquet(str(base / 'gold' / name))
    print(f'BATCH_OK source={source_count} silver={silver_count} active={active_count} rejected=0')
finally:
    spark.stop()
