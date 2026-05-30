"""Gold layer: monthly subscription KPIs (active subs, MRR, new/canceled) from SCD2 Silver history."""

import sys
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, count, sum as spark_sum, when, lit, max as spark_max, to_date, expr, \
    last_day
from delta.tables import DeltaTable

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [gold_aggregate] %(message)s",
)
logger = logging.getLogger(__name__)

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")

SILVER_PATH = os.getenv("SILVER_PATH", "s3a://data-lake/silver/")
GOLD_PATH = os.getenv("GOLD_PATH", "s3a://data-lake/gold/")
CHECKPOINT_PATH = os.getenv("GOLD_CHECKPOINT_PATH", "s3a://data-lake/meta/gold_checkpoint/")

spark = SparkSession.builder \
    .appName("GoldAggregate") \
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Gold runs incrementally by calendar month of valid_from (not by ingest batch).
try:
    checkpoint_df = spark.read.format("delta").load(CHECKPOINT_PATH)
    last_processed_month = checkpoint_df.agg(spark_max("last_processed_month")).collect()[0][0]
    if last_processed_month is None:
        last_processed_month = "1970-01"
except Exception:
    last_processed_month = "1970-01"
logger.info("Using last_processed_month checkpoint: %s", last_processed_month)

silver_df = spark.read.format("delta").load(SILVER_PATH) \
    .withColumn("valid_from_month", date_format(to_date(col("valid_from")), "yyyy-MM")) \
    .filter(col("valid_from_month") > last_processed_month)

if silver_df.count() == 0:
    logger.info("No new Silver records for Gold aggregation; exiting")
    spark.stop()
    sys.exit(0)

affected_months = silver_df.select("valid_from_month").distinct().collect()
months_to_recompute = [row["valid_from_month"] for row in affected_months]
logger.info("Months to recompute: %s", months_to_recompute)

# Full Silver history is needed for point-in-time monthly metrics (SCD2 intervals).
all_silver = spark.read.format("delta").load(SILVER_PATH)

monthly_aggregates_list = []
new_subs_list = []
canceled_subs_list = []

for month in months_to_recompute:
    year, month_num = month.split("-")
    month_start = f"{year}-{month_num}-01"

    # Subscription is active in month M if valid_from <= end(M) and valid_to is open or >= start(M).
    active_subs = all_silver.filter(
        (col("valid_from") <= expr(f"last_day('{month_start}')")) &
        ((col("valid_to").isNull()) | (col("valid_to") >= to_date(lit(month_start))))
    )

    monthly_agg = active_subs.groupBy("plan_type") \
        .agg(
        count("subscription_id").alias("total_active_subs"),
        spark_sum("price").alias("mrr")
    ) \
        .withColumn("year_month", lit(month))

    monthly_aggregates_list.append(monthly_agg)

    new_subs_month = all_silver.filter(
        (col("operation") == "INSERT") &
        (date_format(to_date(col("start_date")), "yyyy-MM") == month)
    ).groupBy("plan_type") \
        .agg(count("subscription_id").alias("new_subs_count")) \
        .withColumn("year_month", lit(month))

    new_subs_list.append(new_subs_month)

    canceled_subs_month = all_silver.filter(
        (col("status") == "canceled") &
        (col("operation").isin(["UPDATE", "INSERT"])) &
        (date_format(to_date(col("cancel_date")), "yyyy-MM") == month)
    ).groupBy("plan_type") \
        .agg(count("subscription_id").alias("canceled_subs_count")) \
        .withColumn("year_month", lit(month))

    canceled_subs_list.append(canceled_subs_month)

from functools import reduce

monthly_aggregates = reduce(lambda a, b: a.union(b), monthly_aggregates_list)
new_subs = reduce(lambda a, b: a.union(b), new_subs_list)
canceled_subs = reduce(lambda a, b: a.union(b), canceled_subs_list)

gold_df = monthly_aggregates \
    .join(new_subs, ["year_month", "plan_type"], "left") \
    .join(canceled_subs, ["year_month", "plan_type"], "left") \
    .fillna(0, subset=["new_subs_count", "canceled_subs_count"]) \
    .orderBy("year_month", "plan_type")

# Upsert affected months idempotently; bootstrap overwrites on first run.
try:
    gold_table = DeltaTable.forPath(spark, GOLD_PATH)

    for month in months_to_recompute:
        gold_table.alias("target") \
            .merge(
            gold_df.filter(col("year_month") == month).alias("source"),
            "target.year_month = source.year_month AND target.plan_type = source.plan_type"
        ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
except Exception:
    logger.warning("Gold table not found; writing initial snapshot")
    gold_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .partitionBy("year_month") \
        .save(GOLD_PATH)

# Track the latest Silver month processed so the next run skips already-published periods.
max_processed_month = silver_df.agg(spark_max("valid_from_month")).collect()[0][0]
checkpoint_data = spark.createDataFrame([(max_processed_month,)], ["last_processed_month"])
checkpoint_data.write \
    .format("delta") \
    .mode("overwrite") \
    .save(CHECKPOINT_PATH)

# Colocate plan_type in files for typical BI filters on Gold.
delta_table = DeltaTable.forPath(spark, GOLD_PATH)
delta_table.optimize().executeZOrderBy(["plan_type"])

logger.info("Gold aggregation completed successfully")
spark.stop()