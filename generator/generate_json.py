"""Synthetic CDC JSON generator — writes events directly to MinIO raw/ prefix."""

import json
import logging
import os
import random
import time
import uuid
from datetime import datetime

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [generate_json] %(message)s",
)
logger = logging.getLogger(__name__)

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_BUCKET = os.getenv("S3_BUCKET", "data-lake")
RAW_PREFIX = os.getenv("RAW_PREFIX", "raw")
FILES_PER_DAY = int(os.getenv("GENERATOR_FILES_PER_DAY", "50"))
INVALID_RATIO = float(os.getenv("GENERATOR_INVALID_RATIO", "0.1"))

OPERATIONS = ["INSERT", "UPDATE"]
USER_IDS = list(range(1, 101))
VALID_PLAN_TYPES = ["basic", "premium", "family"]
VALID_STATUSES = ["active", "past_due", "canceled", "paused"]
BILLING_CYCLES = ["monthly", "yearly"]

# Three sample days per month (Jan–Mar 2025) → Gold charts span three months.
GENERATED_DATES = [
    datetime(2025, month, day)
    for month in (1, 2, 3)
    for day in (1, 2, 3)
]


def build_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_bucket(s3_client):
    try:
        s3_client.create_bucket(Bucket=S3_BUCKET)
        logger.info("Created bucket: %s", S3_BUCKET)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            raise


def random_datetime(date):
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return datetime(date.year, date.month, date.day, hour, minute, second).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def random_date():
    return random.choice(GENERATED_DATES).strftime("%Y-%m-%d")


def build_event(dt):
    # ~10% invalid rows to exercise Silver DQE rules in the pipeline demo.
    is_invalid = random.random() < INVALID_RATIO

    user_id = random.choice(USER_IDS)
    price = round(random.uniform(5.0, 50.0), 2)
    plan_type = random.choice(VALID_PLAN_TYPES)
    status = random.choice(VALID_STATUSES)

    if is_invalid:
        error_type = random.choice(["price", "user_id", "plan_type", "status"])
        if error_type == "price":
            price = random.choice([-10.0, 0.0, -0.01])
        elif error_type == "user_id":
            user_id = None
        elif error_type == "plan_type":
            plan_type = "invalid_plan"
        elif error_type == "status":
            status = "unknown"

    event_ts = random_datetime(dt)
    # Bias start_date to the event month so Gold "new subs" spread across months.
    if random.random() < 0.75:
        start_date = dt.strftime("%Y-%m-%d")
    else:
        start_date = random_date()

    return {
        "operation": random.choice(OPERATIONS),
        "timestamp": event_ts,
        "subscription_id": f"sub_abc{random.randint(1, 100)}",
        "user_id": user_id,
        "plan_type": plan_type,
        "price": price,
        "billing_cycle": random.choice(BILLING_CYCLES),
        "status": status,
        "auto_renew": random.choice([True, False]),
        "start_date": start_date,
        "cancel_date": random_date() if random.choice([True, False]) else None,
        "updated_at": random_datetime(dt),
    }, event_ts


def upload_events():
    s3_client = build_s3_client()

    for attempt in range(10):
        try:
            ensure_bucket(s3_client)
            break
        except ClientError:
            if attempt == 9:
                raise
            logger.warning("MinIO not ready, retrying in 3s...")
            time.sleep(3)

    uploaded = 0
    for dt in GENERATED_DATES:
        partition = dt.strftime("%Y-%m-%d")
        for _ in range(FILES_PER_DAY):
            event, event_ts = build_event(dt)
            safe_time = event_ts.replace(":", "-").replace("Z", "")
            unique = uuid.uuid4().hex[:8]
            key = f"{RAW_PREFIX}/dt={partition}/{safe_time}_{unique}.json"
            body = json.dumps(event, indent=2).encode("utf-8")
            s3_client.put_object(Bucket=S3_BUCKET, Key=key, Body=body, ContentType="application/json")
            uploaded += 1

    logger.info(
        "Uploaded %s JSON files to s3://%s/%s/ (days=%s, per_day=%s, months=Jan-Mar 2025)",
        uploaded,
        S3_BUCKET,
        RAW_PREFIX,
        len(GENERATED_DATES),
        FILES_PER_DAY,
    )


if __name__ == "__main__":
    upload_events()
