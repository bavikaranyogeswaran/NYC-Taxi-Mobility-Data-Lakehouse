# NYC Taxi Mobility Data Lakehouse

An end-to-end data engineering project that ingests NYC TLC Yellow Taxi trip records (Q1 2024), processes them through a **Medallion Architecture** (Bronze → Silver → Gold), and exposes the results through a FastAPI backend and a React dashboard.

---

## Architecture

```
NYC TLC CDN
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

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion & Processing | PySpark 3.5.4, Delta Lake 3.3.0 |
| Orchestration | Apache Airflow 2.8.1 |
| Storage | MinIO (S3-compatible object store) |
| Serving DB | PostgreSQL 13 |
| Validation | DuckDB 1.2.2 |
| API | FastAPI 0.115, Uvicorn |
| Frontend | React 19, TypeScript, Vite, Tailwind CSS v4, Recharts |
| Containerisation | Docker Compose |
| Testing | pytest 8.3.5 |

---

## Project Structure

```
.
├── dags/
│   └── taxi_pipeline.py          # Airflow DAG definition
├── src/
│   ├── config.py                 # Central config (paths, URLs, settings)
│   ├── ingestion/
│   │   └── download_data.py      # Download raw Parquet files from NYC TLC CDN
│   ├── pipeline/
│   │   ├── bronze_ingestion.py   # Raw → Bronze Delta tables
│   │   ├── silver_transformation.py  # Bronze → Silver (DQ + enrichment)
│   │   └── gold_aggregation.py   # Silver → Gold aggregated data marts
│   ├── analytics/
│   │   ├── validate_gold.py      # DuckDB-based Gold validation
│   │   └── push_gold_to_postgres.py  # Load Gold into PostgreSQL
│   ├── utils/
│   │   └── spark_utils.py        # Shared SparkSession factory
│   └── frontend/
│       └── src/                  # React + TypeScript dashboard
├── api/
│   └── main.py                   # FastAPI application
├── docker/
│   ├── postgres-init.sql         # Creates airflow + superset databases
│   └── superset_config.py
├── tests/
│   ├── conftest.py               # Shared SparkSession fixture
│   ├── test_config.py            # Config validation (pure Python)
│   ├── test_download.py          # Ingestion tests with mocked HTTP
│   ├── test_silver_dq.py         # Silver DQ rule tests (PySpark)
│   ├── test_silver_transforms.py # Derived-column & zone join tests (PySpark)
│   └── test_gold_aggregations.py # Gold aggregation tests (PySpark)
├── hadoop/bin/                   # winutils.exe for PySpark on Windows
├── Dockerfile.airflow
├── docker-compose.yml
├── requirements.txt
└── pytest.ini
```

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

# 2. Build the Airflow image (downloads Spark/Delta JARs — takes ~5 min once)
docker compose build

# 3. Start all services
docker compose up -d

# 4. Wait ~60 seconds for Airflow to initialise, then open the UI
#    http://localhost:8080  (admin / admin)

# 5. Trigger the pipeline manually
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

## Pipeline Stages

### 1 · Ingestion (`download_raw_data`)

Downloads three NYC TLC Yellow Taxi Parquet files (Jan–Mar 2024) and the Taxi Zone lookup CSV from the official CDN into `data/raw/`. Downloads are streamed in 8 KB chunks and skipped if the file already exists, making the step safe to re-run.

### 2 · Bronze (`bronze_ingestion`)

Reads the raw Parquet and CSV files, appends three audit columns, and writes Delta tables to MinIO.

| Audit column | Description |
|---|---|
| `ingested_at` | UTC timestamp of the ingestion run |
| `source_file` | Original filename |
| `batch_id` | UUID unique to each pipeline run |

No records are dropped or modified at this layer.

### 3 · Silver (`silver_transformation`)

Applies seven data-quality rules. Records failing any rule are written to a quarantine table with a `dq_fail_reason` label; valid records are enriched and written to the Silver table.

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

**Enrichments added:** `trip_duration_minutes`, `pickup_date`, `pickup_hour`, `day_of_week`, `day_of_week_num`, pickup/dropoff zone names and boroughs (broadcast join against the 265-row Taxi Zone lookup table).

### 4 · Gold (`gold_aggregation`)

Produces four aggregated data marts from the Silver table:

| Mart | Description |
|---|---|
| `gold_daily_revenue_by_zone` | Total trips, revenue, passengers, avg fare — by day and pickup zone |
| `gold_hourly_performance` | Avg duration, distance, and speed (mph) — by hour of day |
| `gold_route_summary` | Top pickup → dropoff routes by trip volume |
| `gold_data_quality_summary` | Daily valid vs. invalid record counts |

Each mart is written as a Delta table to MinIO and pushed to PostgreSQL for the API.

---

## Local Development (Without Docker)

Install dependencies first:

```bash
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
cd src/frontend
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

# Verbose output with coverage summary
python -m pytest -v
```

| Test file | What it covers | Spark needed |
|---|---|---|
| `test_config.py` | All paths, URLs, and pipeline settings | No |
| `test_download.py` | Streaming download with mocked HTTP | No |
| `test_silver_dq.py` | All 7 DQ rules, valid/invalid row splitting | Yes |
| `test_silver_transforms.py` | Duration math, temporal features, column renames, zone join | Yes |
| `test_gold_aggregations.py` | Revenue sums, speed formula, null-zone filtering, sort order | Yes |

---

## Configuration

All pipeline paths and URLs live in one file — `src/config.py`. Key settings:

| Setting | Default | Description |
|---|---|---|
| `USE_MINIO` | `True` | Write to MinIO (`s3a://`) vs. local filesystem |
| `PIPELINE_SETTINGS["delta_write_mode"]` | `"overwrite"` | `"overwrite"` for dev, `"append"` for prod |
| `PIPELINE_SETTINGS["dataset_months"]` | `[1, 2, 3]` | Q1 2024 |

MinIO and PostgreSQL credentials are set via environment variables in `docker-compose.yml`. For local runs the pipeline falls back to `minioadmin / minioadmin` and `localhost:5432`.

---

## Stopping the Stack

```bash
# Stop all containers (keep volumes)
docker compose down

# Stop and delete all data volumes (full reset)
docker compose down -v
```
