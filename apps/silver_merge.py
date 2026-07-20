"""Silver layer: apply DQ rules and build subscription history with SCD Type 2 semantics."""

import sys
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lit, max as spark_max, row_number
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [silver_merge] %(message)s",
)
logger = logging.getLogger(__name__)

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")

BRONZE_PATH = os.getenv("BRONZE_PATH", "s3a://data-lake/bronze/")
SILVER_PATH = os.getenv("SILVER_PATH", "s3a://data-lake/silver/")
DQE_PATH = os.getenv("DQE_PATH", "s3a://data-lake/dqe_errors/")
CHECKPOINT_PATH = os.getenv("SILVER_CHECKPOINT_PATH", "s3a://data-lake/meta/silver_checkpoint/")

spark = SparkSession.builder \
    .appName("SilverMerge") \
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

VALID_PLAN_TYPES = ['basic', 'premium', 'family']
VALID_STATUSES = ['active', 'past_due', 'canceled', 'paused']

# Process only Bronze rows ingested after the last successful Silver run.
try:
    checkpoint_df = spark.read.format("delta").load(CHECKPOINT_PATH)
    last_processed_ts = checkpoint_df.agg(spark_max("last_processed")).collect()[0][0]
    if last_processed_ts is None:
        last_processed_ts = lit("1970-01-01 00:00:00").cast(TimestampType())
except AnalysisException as exc:
    msg = str(exc).lower()
    if "path does not exist" in msg or "not a delta table" in msg:
        last_processed_ts = lit("1970-01-01 00:00:00").cast(TimestampType())
    else:
        raise
logger.info("Using last_processed_ts checkpoint")

bronze_df = spark.read.format("delta").load(BRONZE_PATH) \
    .withColumn("event_ts", to_timestamp(col("timestamp"))) \
    .filter(col("ingest_timestamp") > last_processed_ts)

if bronze_df.rdd.isEmpty():
    logger.info("No new records in Bronze; exiting")
    spark.stop()
    sys.exit(0)

# Split valid facts from rule violations; invalid rows go to DQE for manual review/replay.
valid_df = bronze_df.filter(
    (col("price") > 0) &
    (col("user_id").isNotNull()) &
    (col("plan_type").isin(VALID_PLAN_TYPES)) &
    (col("status").isin(VALID_STATUSES))
)

error_df = bronze_df.exceptAll(valid_df)

if not error_df.rdd.isEmpty():
    logger.warning("Found invalid records; writing to DQE")
    error_df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(DQE_PATH)

# CDC event types from the source system.
inserts_raw_df = valid_df.filter(col("operation") == "INSERT")
updates_raw_df = valid_df.filter(col("operation") == "UPDATE")

# One INSERT / one UPDATE per subscription per batch (latest event wins).
# Without this on the UPDATE side, two UPDATEs for the same subscription in
# one batch would both pass the "differs from current" filter below and each
# get written as is_current=True, violating the SCD2 invariant checked at
# the end of this script.
insert_rank = Window.partitionBy("subscription_id").orderBy(col("event_ts").desc())
inserts_df = (
    inserts_raw_df.withColumn("_insert_rn", row_number().over(insert_rank))
    .filter(col("_insert_rn") == 1)
    .drop("_insert_rn")
)

update_rank = Window.partitionBy("subscription_id").orderBy(col("event_ts").desc())
updates_df = (
    updates_raw_df.withColumn("_update_rn", row_number().over(update_rank))
    .filter(col("_update_rn") == 1)
    .drop("_update_rn")
)

try:
    silver_current = spark.read.format("delta").load(SILVER_PATH)
    silver_exists = True
except AnalysisException as exc:
    msg = str(exc).lower()
    if "path does not exist" in msg or "not a delta table" in msg:
        silver_exists = False
    else:
        raise

if silver_exists:
    logger.info("Silver exists; running incremental SCD2 merge flow")
    # Current snapshot per subscription — used to detect attribute changes on UPDATE.
    silver_active = silver_current.filter(col("is_current") == True) \
        .select("subscription_id", "plan_type", "price", "billing_cycle", "status")

    updates_with_changes = updates_df.alias("upd") \
        .join(silver_active.alias("sil"), "subscription_id", "inner") \
        .filter(
        (col("upd.plan_type") != col("sil.plan_type")) |
        (col("upd.price") != col("sil.price")) |
        (col("upd.billing_cycle") != col("sil.billing_cycle")) |
        (col("upd.status") != col("sil.status"))
    ) \
        .select(col("upd.*"))

    if not updates_with_changes.rdd.isEmpty():
        # Close open version at UPDATE event time, then append the new current row.
        close_events = updates_with_changes.groupBy("subscription_id") \
            .agg(spark_max("event_ts").alias("close_valid_to"))

        closed_versions = silver_current \
            .filter(col("is_current") == True) \
            .join(close_events, "subscription_id", "inner") \
            .withColumn("valid_to", col("close_valid_to")) \
            .withColumn("is_current", lit(False))

        # NOTE for portfolio: append-based SCD2 closure is kept for demo simplicity.
        # In production, prefer atomic Delta MERGE/UPDATE and document this trade-off in README.
        closed_versions.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .partitionBy("plan_type") \
            .save(SILVER_PATH)

        new_versions_df = updates_with_changes.select(
            col("subscription_id"),
            col("user_id"),
            col("plan_type"),
            col("price"),
            col("billing_cycle"),
            col("status"),
            col("auto_renew"),
            col("start_date"),
            col("cancel_date"),
            col("operation"),
            col("event_ts").alias("timestamp"),
            col("event_ts").alias("valid_from"),
            lit(None).cast(TimestampType()).alias("valid_to"),
            lit(True).alias("is_current"),
            col("ingest_timestamp"),
            col("source_file"),
            col("dt")
        )

        new_versions_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .partitionBy("plan_type") \
            .save(SILVER_PATH)

    # INSERT only for subscriptions not yet present in Silver (left_anti).
    existing_ids = silver_current.select("subscription_id").distinct()

    new_inserts_df = inserts_df.join(existing_ids, "subscription_id", "left_anti") \
        .select(
        col("subscription_id"),
        col("user_id"),
        col("plan_type"),
        col("price"),
        col("billing_cycle"),
        col("status"),
        col("auto_renew"),
        col("start_date"),
        col("cancel_date"),
        col("operation"),
        col("event_ts").alias("timestamp"),
        col("event_ts").alias("valid_from"),
        lit(None).cast(TimestampType()).alias("valid_to"),
        lit(True).alias("is_current"),
        col("ingest_timestamp"),
        col("source_file"),
        col("dt")
    )

    if not new_inserts_df.rdd.isEmpty():
        new_inserts_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .partitionBy("plan_type") \
            .save(SILVER_PATH)

else:
    logger.info("Silver not found; running initial INSERT+UPDATE bootstrap")
    # First load: no prior Silver state to diff UPDATEs against, so full SCD2
    # history can't be reconstructed retroactively. Instead, take the latest
    # valid event per subscription — INSERT or UPDATE — as its current row.
    # (Using inserts_df alone here would silently drop every subscription
    # whose only events in this initial batch are UPDATEs.)
    bootstrap_rank = Window.partitionBy("subscription_id").orderBy(col("event_ts").desc())
    silver_initial = (
        valid_df.withColumn("_rn", row_number().over(bootstrap_rank))
        .filter(col("_rn") == 1)
        .drop("_rn")
        .select(
            col("subscription_id"),
            col("user_id"),
            col("plan_type"),
            col("price"),
            col("billing_cycle"),
            col("status"),
            col("auto_renew"),
            col("start_date"),
            col("cancel_date"),
            col("operation"),
            col("event_ts").alias("timestamp"),
            col("event_ts").alias("valid_from"),
            lit(None).cast(TimestampType()).alias("valid_to"),
            lit(True).alias("is_current"),
            col("ingest_timestamp"),
            col("source_file"),
            col("dt")
        )
    )

    silver_initial.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .partitionBy("plan_type") \
        .save(SILVER_PATH)

# Post-write invariant check: no subscription may have multiple current versions.
silver_after_df = spark.read.format("delta").load(SILVER_PATH)
current_violations = (
    silver_after_df.filter(col("is_current") == True)
    .groupBy("subscription_id")
    .count()
    .filter(col("count") > 1)
    .count()
)
if current_violations > 0:
    raise RuntimeError(
        f"SCD2 invariant violated: {current_violations} subscriptions have multiple current rows"
    )

# Commit Silver checkpoint only when post-write SCD2 checks pass.
max_ingest_ts = bronze_df.agg(spark_max("ingest_timestamp")).collect()[0][0]
checkpoint_data = spark.createDataFrame([(max_ingest_ts,)], ["last_processed"])
checkpoint_data.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .save(CHECKPOINT_PATH)

logger.info("Silver merge completed successfully")
spark.stop()