# NYC Taxi Mobility Data Lakehouse

An end-to-end, production-grade data engineering platform that ingests, transforms, validates, and visualizes New York City taxi trip data. Built on a **Medallion Architecture (Bronze → Silver → Gold)**, the system processes over 3 million raw trip records from the NYC Taxi & Limousine Commission (TLC) for Q1 2024, turning raw Parquet files into curated, analytics-ready data marts served through a FastAPI backend and an interactive React dashboard.

The project demonstrates the complete lifecycle of modern data engineering — from raw data ingestion on cloud-compatible object storage, through distributed transformation with Apache Spark and Delta Lake, to orchestrated pipeline execution via Apache Airflow, and finally to a REST API and interactive frontend for business intelligence.

---

## Why a Data Lakehouse — Not a Data Warehouse

A traditional data warehouse (Snowflake, BigQuery, Redshift) was explicitly rejected for this project. The NYC taxi dataset and its intended workloads expose the core limitations of the warehouse model and make the lakehouse the correct architectural choice:

**1. The raw data is Parquet on a public CDN — not rows in a database**
NYC TLC distributes trip data as columnar Parquet files, not database exports. A warehouse would require an ETL step to load those files in before any work could begin. The lakehouse ingests them directly into the Bronze layer on MinIO with zero format conversion, treating object storage as the primary store from day one.

**2. Storage cost at scale**
Warehouses charge for proprietary compressed storage, typically at $20–$100/TB/month. MinIO on commodity hardware (or S3 in production) stores the same Delta Parquet data for under $2/TB/month — a 10–50x difference. With 3 months of NYC taxi data already reaching millions of rows, and the architecture designed to scale to years of history, storage cost is a real constraint, not a hypothetical one.

**3. Compute and storage scale independently**
Warehouses couple compute to storage: you pay for a warehouse cluster even when no queries are running. This project separates them — MinIO holds all data persistently at low cost, while Spark compute spins up only when the Airflow DAG runs. Idle time costs nothing on the compute side.

**4. Raw data must be preserved for reprocessing and ML**
The Bronze layer stores every raw record exactly as received, including malformed and out-of-range rows. A warehouse's schema-on-write model would reject or silently drop those rows at load time. The lakehouse preserves them in a Quarantine table with `dq_fail_reason` populated, making it possible to audit data quality issues, backfill after rule changes, or use raw signals for anomaly detection — none of which is feasible if bad data is discarded at ingest.

**5. The Silver layer requires schema-on-read flexibility**
The 7 data quality rules and derived columns (`trip_duration_minutes`, zone enrichment via broadcast join) are applied *after* ingestion, not before. The lakehouse's schema-on-read model lets raw data land in Bronze without a predefined schema contract, then enforces structure progressively in Silver. A warehouse forces the schema up front, requiring every rule change to be a migration.

**6. Open formats prevent vendor lock-in**
All data in this project is stored as **Delta Lake Parquet** — an open format readable by Spark, DuckDB, pandas, Trino, Flink, and dozens of other tools without any export or conversion step. A warehouse stores data in a proprietary format owned by the vendor; leaving means a full re-export. The lakehouse data is portable by design.

**7. The same data serves both BI and analytics workloads**
The Gold layer is queried by FastAPI → PostgreSQL for the React dashboard (BI use case), and the Silver layer is available directly for ad-hoc Spark SQL and DuckDB analysis (data science use case). A warehouse serves BI well but is awkward for the latter — data scientists typically need raw or semi-processed data, forcing a parallel copy back into a lake anyway. The lakehouse eliminates that duplication.

In summary: a warehouse would have added cost, format conversion overhead, schema rigidity, and vendor lock-in, while losing the raw data and ML flexibility that this architecture deliberately preserves. The lakehouse is not a trend choice here — it is the correct tool for the shape of the data, the workloads, and the scale.

---

## Architecture

```
NYC TLC CDN (Raw Parquet + CSV)
         │
         ▼
┌─────────────────────────────────────────────────────────┐
│  Airflow DAG  (nyc_taxi_lakehouse_pipeline, @daily)     │
│                                                         │
│  download_raw_data ──► bronze_ingestion                 │
│                              │                          │
│                        silver_transformation            │
│                              │                          │
│                         gold_aggregation                │
└─────────────────────────────────────────────────────────┘
         │                    │                    │
         ▼                    ▼                    ▼
    data/raw/           MinIO (S3)           PostgreSQL
   (Parquet/CSV)    Bronze│Silver│Gold      Gold Data Marts
                                                   │
                                             ┌─────┴──────┐
                                             ▼            ▼
                                         FastAPI      React UI
                                         :8000        :5173
```

---

## Tech Stack

| Layer | Technology | Version |
|---|---|---|
| Distributed Processing | PySpark | 3.5.4 |
| Data Lake Format | Delta Lake | 3.3.0 |
| Object Storage | MinIO (S3-compatible) | 2023-11-20 |
| Orchestration | Apache Airflow | 2.8.1 |
| Serving Database | PostgreSQL | 13 |
| Validation Engine | DuckDB | 1.2.2 |
| API Framework | FastAPI + Uvicorn | 0.115 |
| Frontend | React 19 + TypeScript + Vite | 19 / ~6.0 / 8 |
| Styling & Charts | Tailwind CSS + Recharts | 3.4 / 3.8 |
| Containerisation | Docker + Docker Compose | 24+ |
| Runtime | OpenJDK 17 + Python 3.11 | — |
| Testing | pytest | 8.3.5 |
| Linting / CI | ruff + GitHub Actions | — |

---

## Project Structure

```
.
├── dags/
│   └── taxi_pipeline.py              # Airflow DAG definition
├── src/
│   ├── config.py                     # Central config (paths, URLs, settings)
│   ├── ingestion/
│   │   └── download_data.py          # Download raw Parquet files from NYC TLC CDN
│   ├── pipeline/
│   │   ├── bronze_ingestion.py       # Raw → Bronze Delta tables
│   │   ├── silver_transformation.py  # Bronze → Silver (DQ + enrichment)
│   │   └── gold_aggregation.py       # Silver → Gold aggregated data marts
│   ├── analytics/
│   │   ├── validate_gold.py          # DuckDB-based Gold validation
│   │   └── push_gold_to_postgres.py  # Load Gold into PostgreSQL
│   └── utils/
│       └── spark_utils.py            # Shared SparkSession factory
├── frontend/
│   └── src/                          # React + TypeScript dashboard
├── api/
│   └── main.py                       # FastAPI application
├── docker/
│   └── postgres-init.sh              # Creates airflow + lakehouse databases
├── tests/
│   ├── conftest.py                   # Shared SparkSession fixture
│   ├── test_config.py                # Config validation (pure Python)
│   ├── test_download.py              # Ingestion tests with mocked HTTP
│   ├── test_silver_dq.py             # Silver DQ rule tests (PySpark)
│   ├── test_silver_transforms.py     # Derived-column & zone join tests (PySpark)
│   └── test_gold_aggregations.py     # Gold aggregation tests (PySpark)
├── hadoop/bin/                       # winutils.exe for PySpark on Windows
├── Dockerfile.airflow
├── docker-compose.yml
├── requirements.txt
└── pytest.ini
```

---

## Data Pipeline

### Stage 1 — Ingestion (`download_raw_data`)

Downloads 3 monthly NYC TLC Yellow Taxi Parquet files (Jan–Mar 2024) and the Taxi Zone lookup CSV from the official CDN into `data/raw/`. Downloads are streamed in 8 KB chunks and skipped if the file already exists, making the step safe to re-run.

### Stage 2 — Bronze (`bronze_ingestion`)

Reads the raw Parquet and CSV files with PySpark, appends three audit columns, and writes Delta tables to MinIO (`s3a://lakehouse/bronze/`). No records are dropped or modified at this layer — Bronze is the immutable source of truth.

| Audit column | Description |
|---|---|
| `ingested_at` | UTC timestamp of the ingestion run |
| `source_file` | Original filename |
| `batch_id` | UUID unique to each pipeline run |

### Stage 3 — Silver (`silver_transformation`)

Applies 7 data quality rules. Records failing any rule are written to a **Quarantine table** with a `dq_fail_reason` label; valid records are enriched and written to the Silver table.

**DQ rules:**

| # | Rule |
|---|---|
| 1 | `pickup_datetime` is not null |
| 2 | `dropoff_datetime` is not null |
| 3 | `dropoff_datetime > pickup_datetime` |
| 4 | `trip_distance > 0` |
| 5 | `fare_amount >= 0` |
| 6 | `total_amount >= 0` |
| 7 | `passenger_count > 0` |

**Enrichments added:** `trip_duration_minutes`, `pickup_date`, `pickup_hour`, `day_of_week`, `day_of_week_num`, pickup/dropoff zone names and boroughs via a broadcast join against the 265-row Taxi Zone lookup table.

### Stage 4 — Gold (`gold_aggregation`)

Produces 4 aggregated data marts from the Silver table, written as Delta tables to MinIO and pushed to PostgreSQL for the API.

| Mart | Description |
|---|---|
| `gold_daily_revenue_by_zone` | Total trips, revenue, passengers, avg fare — by day and pickup zone |
| `gold_hourly_performance` | Avg duration, distance, and speed (mph) — by hour of day |
| `gold_route_summary` | Top pickup → dropoff routes by trip volume |
| `gold_data_quality_summary` | Daily valid vs. invalid record counts |

---

## Frontend Dashboard

The React dashboard provides three analytics views:

- **Overview** — Executive KPIs: total trips, total revenue, average fare, average trip duration. Includes a daily trend line chart spanning Q1 2024.
- **Demand** — Hourly performance profile: trips by hour, average speed, average duration. Reveals peak demand windows and operational patterns.
- **Location** — Top pickup zones ranked by trip volume; top pickup-to-dropoff routes; borough-level breakdown.

Built with React 19, TypeScript, Tailwind CSS v4, and Recharts. Fetches from the FastAPI backend at `localhost:8000/api/*` and runs in production preview mode via Vite on port 5173.

---

## Key Engineering Decisions

**Medallion architecture** — strict layer separation means Bronze is never modified after ingestion. Business logic lives entirely in Silver and Gold, making audits, backfills, and rule changes safe and isolated.

**Delta Lake for ACID guarantees** — Delta tables provide transactional writes, time travel, and schema enforcement on plain Parquet files, enabling safe re-runs and rollback without a proprietary storage engine.

**Broadcast join optimization** — the 265-row Taxi Zone lookup table is broadcast to all Spark executors, avoiding a full shuffle when enriching millions of trip rows.

**DuckDB for validation** — the Gold validation step uses DuckDB rather than Spark, eliminating JVM overhead for lightweight post-pipeline checks and enabling fast local iteration.

**Pre-baked Docker JARs** — Spark, Delta, and Hadoop JARs are downloaded at image build time and baked into the Airflow Docker image, avoiding a 300 MB+ Ivy resolution on every container startup.

**Windows compatibility** — `winutils.exe` is bundled under `hadoop/bin/` so the full PySpark pipeline runs natively on Windows without WSL.

---

## Dataset

**Source:** NYC Taxi & Limousine Commission (TLC) Trip Record Data
- 3 monthly files: January, February, March 2024
- Format: Parquet (columnar, compressed)
- Volume: 3M+ trip records
- Supplementary: Taxi Zone lookup CSV (265 NYC zones with borough mapping)

---

## Prerequisites

| Requirement | Version |
|---|---|
| Docker Desktop | 24+ |
| Docker Compose | v2 (bundled with Docker Desktop) |
| Python *(local dev only)* | 3.10 – 3.13 |
| Node.js *(frontend dev only)* | 18+ |
| Java *(local PySpark only)* | 17 |

> **Windows only:** PySpark requires `winutils.exe` for local runs. The `hadoop/` directory ships with the correct binary. If the folder is missing, download it from the [cdarlint/winutils](https://github.com/cdarlint/winutils) repository and place it at `hadoop/bin/winutils.exe`.

---

## Quick Start — Docker (Recommended)

This brings up the full stack: Postgres, MinIO, Airflow, FastAPI, and the React dashboard.

```bash
# 1. Clone the repo
git clone <repo-url>
cd "NYC Taxi Mobility Data Lakehouse"

# 2. Set up environment variables
cp .env.example .env   # values are pre-filled for the local Docker stack

# 3. Build the Airflow image (downloads Spark/Delta JARs — takes ~5 min once)
docker compose build

# 4. Start all services
docker compose up -d

# 5. Wait ~60 seconds for Airflow to initialise, then open the UI
#    http://localhost:8080  (admin / admin)

# 6. Trigger the pipeline manually
#    In the Airflow UI → DAGs → nyc_taxi_lakehouse_pipeline → ▶ Trigger DAG
```

After the DAG finishes (~10–20 min depending on download speed):

```bash
# Push Gold tables to PostgreSQL so the API can serve them
docker compose exec airflow-webserver python src/analytics/push_gold_to_postgres.py
```

The dashboard will then be live at **http://localhost:5173**.

---

## Service URLs

| Service | URL | Credentials |
|---|---|---|
| React Dashboard | http://localhost:5173 | — |
| FastAPI (docs) | http://localhost:8000/docs | — |
| Airflow | http://localhost:8080 | admin / admin |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| PostgreSQL | localhost:5432 | postgres / postgres |

---

## Local Development (Without Docker)

Set up your environment file first, then install dependencies:

```bash
cp .env.example .env   # pre-filled with local dev defaults

python -m venv venv
venv\Scripts\activate        # Windows
# source venv/bin/activate   # macOS/Linux

pip install -r requirements.txt
```

Run each pipeline stage individually:

```bash
# 1. Download raw data
python src/ingestion/download_data.py

# 2. Bronze layer
python src/pipeline/bronze_ingestion.py

# 3. Silver layer
python src/pipeline/silver_transformation.py

# 4. Gold layer
python src/pipeline/gold_aggregation.py

# 5. Validate Gold with DuckDB (no JVM needed)
python src/analytics/validate_gold.py

# 6. Push Gold to PostgreSQL
python src/analytics/push_gold_to_postgres.py
```

> **Note:** Stages 2–4 require Java 17 and spin up a local PySpark session. On the first run, Ivy will download ~300 MB of Spark/Delta JARs into `~/.ivy2`. Subsequent runs are fast.

### Frontend dev server

```bash
cd frontend
npm install
npm run dev
# → http://localhost:5173
```

### API dev server

```bash
cd api
uvicorn main:app --reload --port 8000
# → http://localhost:8000/docs
```

---

## Running Tests

```bash
# All tests
python -m pytest

# Fast pure-Python tests only (no Spark / JVM needed)
python -m pytest tests/test_config.py tests/test_download.py

# PySpark tests (requires Java 17 + PySpark installed)
python -m pytest tests/test_silver_dq.py tests/test_silver_transforms.py tests/test_gold_aggregations.py

# Verbose output
python -m pytest -v
```

| Test file | What it covers | Spark needed |
|---|---|---|
| `test_config.py` | All paths, URLs, and pipeline settings | No |
| `test_download.py` | Streaming download with mocked HTTP | No |
| `test_silver_dq.py` | All 7 DQ rules, valid/invalid row splitting | Yes |
| `test_silver_transforms.py` | Duration math, temporal features, column renames, zone join | Yes |
| `test_gold_aggregations.py` | Revenue sums, speed formula, null-zone filtering, sort order | Yes |

The GitHub Actions CI pipeline gates on four sequential jobs: **Ruff lint → unit tests → PySpark tests → frontend build** (ESLint + TypeScript check + Vite production build). All jobs must pass before a branch can merge.

---

## Configuration

All pipeline paths and URLs live in `src/config.py`. Key settings:

| Setting | Default | Description |
|---|---|---|
| `USE_MINIO` | `True` | Write to MinIO (`s3a://`) vs. local filesystem |
| `PIPELINE_SETTINGS["delta_write_mode"]` | `"overwrite"` | `"overwrite"` for dev, `"append"` for prod |
| `PIPELINE_SETTINGS["dataset_months"]` | `[1, 2, 3]` | Q1 2024 |

All credentials are read from environment variables — there are no hardcoded fallbacks. Copy `.env.example` to `.env` and fill in your values before running anything. The `.env.example` ships with the correct defaults for the local Docker stack.

---

## Stopping the Stack

```bash
# Stop all containers (keep volumes)
docker compose down

# Stop and delete all data volumes (full reset)
docker compose down -v
```
