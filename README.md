# Spark Data Lake (Medallion)

Batch data lake demo for subscription **CDC** (Change Data Capture) events: **Raw → Bronze → Silver → Gold** on **Apache Spark**, **Delta Lake**, and **MinIO** (S3-compatible storage). Everything runs in **Docker Compose** with a **Makefile** orchestration layer.

Portfolio focus: incremental ingest, deduplication, data quality (DQ), SCD Type 2 history, and monthly business KPIs (MRR, active/new/canceled subscriptions).

## Architecture

```text
Generator (Docker)  →  MinIO raw/  →  Bronze (Delta)  →   Silver (SCD2 + DQ)   →   Gold (monthly KPIs)   →   Jupyter notebook (explore)
```

| Layer | Storage path (MinIO bucket `data-lake`) | Job / script | What happens |
|-------|-------------------------------------------|--------------|--------------|
| **Raw (landing zone)** | `raw/dt=YYYY-MM-DD/*.json` — Hive-style folders by event date | `generator/generate_json.py` (Docker, profile `tools`) | Generates synthetic subscription **Change Data Capture** events as JSON (operations **INSERT** and **UPDATE**). Writes files directly to MinIO via the S3 API. Demo data: three days in each of January, February, and March 2025 (~50 files per day). About 10% of rows are intentionally invalid to test downstream **Data Quality** rules. |
| **Bronze (immutable ingest)** | `bronze/` — Delta Lake table, partitioned by `dt` (event date) | `apps/bronze_ingest.py` | Reads new JSON from Raw. Parses without a fixed schema so corrupt documents can be detected. Adds `ingest_timestamp`, `source_file`, and `event_dedup_key`. Filters by business time (`event_ts`) using a checkpoint and a late-event grace window (`LATE_EVENT_GRACE_HOURS`). Skips keys already present in Bronze. Malformed JSON goes to `dqe_bronze/`. Valid rows are appended to Bronze Delta. Checkpoint in `meta/bronze_checkpoint/` advances only after a successful write. |
| **Silver (curated history)** | `silver/` — Delta Lake table, partitioned by `plan_type` | `apps/silver_merge.py` | Reads Bronze incrementally (rows with `ingest_timestamp` after the Silver checkpoint). Applies **Data Quality** filters: price greater than zero, valid plan and status, `user_id` not null. Failed rows are written to `dqe_errors/` (**Data Quality Exceptions**). Valid rows are merged using **Change Data Capture** semantics: new subscriptions from **INSERT**, attribute changes from **UPDATE**. Builds **Slowly Changing Dimension Type 2** history (`valid_from`, `valid_to`, `is_current`). One current version per subscription is enforced before the checkpoint in `meta/silver_checkpoint/` is updated. |
| **Gold (business aggregates)** | `gold/` — Delta Lake table, partitioned by `year_month` | `apps/gold_aggregate.py` | Computes monthly **Key Performance Indicators** from the full Silver history (point-in-time logic on **Slowly Changing Dimension Type 2** intervals): total active subscriptions, **Monthly Recurring Revenue**, new subscriptions, canceled subscriptions — by calendar month and plan type. Incremental by month; affected months are upserted with Delta **merge**. Checkpoint in `meta/gold_checkpoint/` stores the last processed month. Table is optimized with Z-order on `plan_type`. |
| **Meta (orchestration state)** | `meta/bronze_checkpoint/`, `meta/silver_checkpoint/`, `meta/gold_checkpoint/` | — (written by batch jobs) | Small Delta tables that store watermarks for incremental runs: last processed event time (Bronze), last processed ingest time (Silver), last processed calendar month (Gold). |
| **Data Quality Exceptions** | `dqe_bronze/` (bad JSON), `dqe_errors/` (failed Silver rules) | — (written by Bronze / Silver) | Quarantine areas for records that must not enter the main Silver or Gold path — for review, replay, or alerting. |

## Stack

- **Apache Spark 3.5** (standalone master + worker)
- **Delta Lake 3.0**
- **MinIO** — object storage (`s3a://data-lake/...`)
- **Python 3.8+** — PySpark jobs, generator (boto3), Jupyter notebook
- **Docker Compose** + **Make**

## Prerequisites

- Docker Desktop (or Docker Engine + Compose v2)
- ~4 GB free RAM for Spark worker
- Ports free: `9000`, `9001`, `7077`, `8080`, `8081`, `8889`

> **Note:** Jupyter in Docker uses port **8889** (not 8888) to avoid conflict with a local PyCharm/Jupyter server on the host.

## Quick start

From the project root:

```bash
make up          # MinIO + Spark + Jupyter
make run         # generate → bronze → silver → gold
make notebook    # http://localhost:8889/tree
```

Open **`notebooks/explore_medallion_lake.ipynb`** in Jupyter and run all cells.

### MinIO Console

- URL: http://localhost:9001  
- Login: `minioadmin` / `minioadmin`  
- Bucket: `data-lake`

### Other UIs

| Service | URL |
|---------|-----|
| Spark Master | http://localhost:8080 |
| Spark Worker | http://localhost:8081 |
| Jupyter (Docker) | http://localhost:8889/tree |

## Makefile commands

```bash
make help        # list commands
make up          # start infrastructure
make down        # stop containers
make generate    # write JSON to MinIO raw/
make bronze      # bronze ingest
make silver      # silver SCD2 merge
make gold        # gold aggregation
make run         # full pipeline
make notebook    # ensure Jupyter container is up
make clean       # docker compose down -v
```

## Fresh run (reset lake data)

MinIO data persists in `./minio_data`. For a clean pipeline run, delete folders in bucket **`data-lake`** via MinIO Console:

`raw/`, `bronze/`, `silver/`, `gold/`, `dqe_errors/`, `dqe_bronze/`, `meta/`

Then:

```bash
make run
```

## Project layout

```text
spark-data-lake/
├── apps/                    # Spark batch jobs (Bronze, Silver, Gold)
├── generator/               # Synthetic CDC JSON → MinIO raw/
├── notebooks/               # Portfolio exploration notebook
├── jupyter/                 # Jupyter config + start script
├── docs/
│   └── architecture.svg     # Architecture diagram (this README)
├── docker-compose.yaml
├── Dockerfile               # Spark + Jupyter image
├── Makefile
└── .env.example             # Path and tuning variables for jobs
```

## Configuration

Copy env template if you customize paths or generator volume:

```bash
cp .env.example .env
```

Spark jobs read S3/MinIO settings from environment variables inside containers (`S3_ENDPOINT=http://minio:9000`).
