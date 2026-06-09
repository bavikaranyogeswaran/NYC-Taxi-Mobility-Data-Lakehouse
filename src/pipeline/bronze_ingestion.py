"""
bronze_ingestion.py
─────────────────────────────────────────────────────────────────
Bronze Layer — Phase 2 (Tasks 2.1 – 2.8)

Reads raw NYC TLC Yellow Taxi Parquet files and the Taxi Zone
Lookup CSV from data/raw/, appends audit metadata columns, and
writes both datasets as Delta tables into data/bronze/.

Bronze rules:
  - Do NOT rename or drop any source columns.
  - Do NOT clean or filter any records.
  - Only add traceability columns: source_file, ingested_at, batch_id.
  - All original bad records must be preserved for audit purposes.

Usage:
    python src/pipeline/bronze_ingestion.py

─────────────────────────────────────────────────────────────────
"""

import sys
import uuid
from pathlib import Path
from datetime import datetime

# ── Resolve project root ──────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from config import (
    RAW_YELLOW_TAXI_DIR,
    RAW_TAXI_ZONES_DIR,
    BRONZE_YELLOW_TAXI_PATH,
    BRONZE_TAXI_ZONES_PATH,
    LOGS_DIR,
    PIPELINE_SETTINGS,
)
from utils.logging_utils import get_logger
from utils.metrics_utils import push_metrics

log = get_logger(__name__, LOGS_DIR / "bronze_ingestion.log")


# ─────────────────────────────────────────────────────────────────────────────
# TASK 2.2 — Initialize SparkSession with Delta Lake extensions
# ─────────────────────────────────────────────────────────────────────────────
def create_spark_session():
    from utils.spark_utils import create_spark_session as _create
    return _create("NYC_Taxi_Bronze_Ingestion")


# ─────────────────────────────────────────────────────────────────────────────
# TASK 2.3 – 2.6 — Load raw files and append audit metadata
# ─────────────────────────────────────────────────────────────────────────────
def ingest_yellow_taxi(spark, batch_id: str):
    from pyspark.sql import functions as F

    raw_path = str(RAW_YELLOW_TAXI_DIR / "*.parquet")
    log.info("reading_raw_files", table="yellow_taxi", path=raw_path)

    df = spark.read.parquet(raw_path)
    raw_count = df.count()
    log.info("data_loaded", source="raw_parquet", table="yellow_taxi", record_count=raw_count)

    df = df.withColumn("ingested_at", F.current_timestamp())
    df = df.withColumn(
        "source_file",
        F.element_at(F.split(F.input_file_name(), "/"), -1),
    )
    df = df.withColumn("batch_id", F.lit(batch_id))

    log.info("audit_columns_added", batch_id=batch_id)
    return df


def ingest_taxi_zones(spark, batch_id: str):
    from pyspark.sql import functions as F

    zone_path = str(RAW_TAXI_ZONES_DIR / "taxi_zone_lookup.csv")
    log.info("reading_raw_files", table="taxi_zones", path=zone_path)

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(zone_path)
    )

    zone_count = df.count()
    log.info("data_loaded", source="taxi_zone_csv", table="taxi_zones", record_count=zone_count)

    df = (
        df
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("source_file", F.lit("taxi_zone_lookup.csv"))
        .withColumn("batch_id",    F.lit(batch_id))
    )
    return df


# ─────────────────────────────────────────────────────────────────────────────
# TASK 2.7 — Save Yellow Taxi DataFrame as a Delta table
# TASK 2.8 — Save Taxi Zones DataFrame as a Delta table
# ─────────────────────────────────────────────────────────────────────────────
def save_as_delta(df, output_path: str, table_name: str, write_mode: str) -> int:
    log.info("writing_table", table=table_name, path=output_path, mode=write_mode)
    df.write.format("delta").mode(write_mode).save(output_path)
    written = df.count()
    log.info("data_written", table=table_name, record_count=written, path=output_path)
    return written


# ─────────────────────────────────────────────────────────────────────────────
# Main pipeline runner
# ─────────────────────────────────────────────────────────────────────────────
def run_bronze_ingestion() -> None:
    start = datetime.now()
    write_mode = PIPELINE_SETTINGS["delta_write_mode"]
    batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"

    log.info("stage_start", stage="bronze", batch_id=batch_id, write_mode=write_mode)

    spark = create_spark_session()

    try:
        yellow_df = ingest_yellow_taxi(spark, batch_id)
        rows_written = save_as_delta(
            df=yellow_df,
            output_path=BRONZE_YELLOW_TAXI_PATH,
            table_name="bronze_yellow_taxi_trips",
            write_mode=write_mode,
        )

        zones_df = ingest_taxi_zones(spark, batch_id)
        save_as_delta(
            df=zones_df,
            output_path=BRONZE_TAXI_ZONES_PATH,
            table_name="bronze_taxi_zones",
            write_mode=write_mode,
        )

        elapsed = (datetime.now() - start).total_seconds()
        log.info("stage_complete", stage="bronze", duration_seconds=round(elapsed, 1))

        push_metrics(
            "bronze_ingestion",
            bronze_rows_written=rows_written,
            bronze_duration_seconds=elapsed,
        )

    finally:
        spark.stop()
        log.info("spark_stopped", stage="bronze")


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    run_bronze_ingestion()
