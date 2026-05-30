"""Bronze layer: ingest raw JSON from the lake into Delta with dedup and basic DQ routing."""

import sys
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, input_file_name, to_date, to_timestamp, max as spark_max, lit, expr, coalesce, sha2, concat_ws
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType, TimestampType

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [bronze_ingest] %(message)s",
)
logger = logging.getLogger(__name__)

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")

RAW_PATH = os.getenv("RAW_PATH", "s3a://data-lake/raw/")
BRONZE_PATH = os.getenv("BRONZE_PATH", "s3a://data-lake/bronze/")
BRONZE_CHECKPOINT_PATH = os.getenv("BRONZE_CHECKPOINT_PATH", "s3a://data-lake/meta/bronze_checkpoint/")
BRONZE_DQE_PATH = os.getenv("BRONZE_DQE_PATH", "s3a://data-lake/dqe_bronze/")
LATE_EVENT_GRACE_HOURS = int(os.getenv("LATE_EVENT_GRACE_HOURS", "24"))

# MinIO is accessed via S3A; Delta extensions enable ACID writes to bronze/checkpoint tables.
spark = SparkSession.builder \
    .appName("BronzeIngest") \
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

json_schema = StructType([
    StructField("operation", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("subscription_id", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("plan_type", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("billing_cycle", StringType(), True),
    StructField("status", StringType(), True),
    StructField("auto_renew", BooleanType(), True),
    StructField("start_date", StringType(), True),
    StructField("cancel_date", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("event_id", StringType(), True),
])

# Incremental watermark by business event time (not ingest time) with a grace window for late arrivals.
try:
    checkpoint_df = spark.read.format("delta").load(BRONZE_CHECKPOINT_PATH)
    checkpoint_value = checkpoint_df.agg(spark_max("last_processed_event_ts")).collect()[0][0]
    last_processed_event_ts = checkpoint_value if checkpoint_value is not None else "1970-01-01 00:00:00"
except Exception:
    last_processed_event_ts = "1970-01-01 00:00:00"
logger.info("Using Bronze checkpoint: %s", last_processed_event_ts)

# Do not pass .schema(): explicit schema suppresses _corrupt_record.
# Spark also omits _corrupt_record when inference sees only valid JSON.
CORRUPT_COL = "_corrupt_record"
raw_df = (
    spark.read
    .option("multiLine", "true")
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", CORRUPT_COL)
    .json(RAW_PATH + "dt=*/*.json")
)
logger.info("Read raw records from %s", RAW_PATH)

if CORRUPT_COL in raw_df.columns:
    corrupt_df = raw_df.filter(col(CORRUPT_COL).isNotNull())
    valid_raw_df = raw_df.filter(col(CORRUPT_COL).isNull()).drop(CORRUPT_COL)
else:
    logger.info("No %s column (all JSON parsed); skipping corrupt split", CORRUPT_COL)
    corrupt_df = raw_df.limit(0)
    valid_raw_df = raw_df

for field in json_schema.fields:
    if field.name not in valid_raw_df.columns:
        valid_raw_df = valid_raw_df.withColumn(field.name, lit(None).cast(field.dataType))

valid_raw_df = valid_raw_df.select([field.name for field in json_schema.fields])

# Route malformed JSON to a separate DQE Delta table instead of failing the batch.
corrupt_count = corrupt_df.count()
if corrupt_count > 0:
    (
        corrupt_df.withColumn("ingest_timestamp", current_timestamp())
        .withColumn("dq_error_reason", lit("Corrupted JSON record"))
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(BRONZE_DQE_PATH)
    )
    logger.warning("Corrupted JSON rows written to Bronze DQE: %s", corrupt_count)
raw_count = raw_df.count()
valid_count = valid_raw_df.count()

cutoff_expr = expr(f"INTERVAL {LATE_EVENT_GRACE_HOURS} HOURS")
cutoff_ts = lit(last_processed_event_ts).cast(TimestampType()) - cutoff_expr

# Hash-based idempotency key: safe to replay the same raw file or event payload.
bronze_df = valid_raw_df \
    .withColumn("ingest_timestamp", current_timestamp()) \
    .withColumn("event_ts", to_timestamp(col("timestamp"))) \
    .withColumn("source_file", input_file_name()) \
    .withColumn(
        "event_dedup_key",
        sha2(
            concat_ws(
                "||",
                coalesce(col("event_id"), lit("")),
                coalesce(col("subscription_id"), lit("")),
                coalesce(col("operation"), lit("")),
                coalesce(col("timestamp"), lit("")),
                coalesce(col("source_file"), lit("")),
            ),
            256,
        ),
    ) \
    .filter(col("operation").isNotNull() & (col("operation") != "")) \
    .filter(col("event_ts").isNotNull()) \
    .filter(col("event_ts") > cutoff_ts) \
    .dropDuplicates(["event_dedup_key"]) \
    .withColumn("dt", to_date(col("event_ts")))

# Skip keys already landed in Bronze on previous runs (cross-batch dedup).
try:
    existing_keys_df = (
        spark.read.format("delta")
        .load(BRONZE_PATH)
        .select("event_dedup_key")
        .distinct()
    )
    bronze_df = bronze_df.join(existing_keys_df, "event_dedup_key", "left_anti")
except Exception:
    logger.info("Bronze table not found yet; skipping cross-run anti-join")

written_count = bronze_df.count()
logger.info(
    "Bronze counters | raw=%s valid=%s corrupt=%s written=%s",
    raw_count,
    valid_count,
    corrupt_count,
    written_count,
)

if written_count == 0:
    logger.info("No new records after checkpoint and validation; exiting")
    spark.stop()
    sys.exit(0)

# Partition by event date for prune-friendly downstream reads.
bronze_df.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("dt") \
    .save(BRONZE_PATH)

# Advance checkpoint only after a successful Bronze append.
max_event_ts = bronze_df.agg(spark_max("event_ts")).collect()[0][0]
checkpoint_data = spark.createDataFrame([(max_event_ts,)], ["last_processed_event_ts"])
checkpoint_data.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .save(BRONZE_CHECKPOINT_PATH)

logger.info("Bronze ingest completed successfully")
spark.stop()

