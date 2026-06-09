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
import logging
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

# ── Logging setup ─────────────────────────────────────────────────────────────
LOGS_DIR.mkdir(parents=True, exist_ok=True)
log_file = LOGS_DIR / "bronze_ingestion.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# TASK 2.2 — Initialize SparkSession with Delta Lake extensions
# ─────────────────────────────────────────────────────────────────────────────
def create_spark_session():
    """
    Boots a local PySpark session with Delta Lake support.
    Delegates to the shared spark_utils factory which auto-detects
    pre-baked Docker JARs vs. local Ivy resolution.
    """
    from utils.spark_utils import create_spark_session as _create
    return _create("NYC_Taxi_Bronze_Ingestion")


# ─────────────────────────────────────────────────────────────────────────────
# TASK 2.3 – 2.6 — Load raw files and append audit metadata
# ─────────────────────────────────────────────────────────────────────────────
def ingest_yellow_taxi(spark, batch_id: str):
    """
    TASK 2.3 : Reads all Yellow Taxi Parquet files from data/raw/yellow_taxi/
               into a single Spark DataFrame.
    TASK 2.4 : Appends ingested_at — exact timestamp when this batch ran.
    TASK 2.5 : Appends source_file — the name of the raw file each row came from.
    TASK 2.6 : Appends batch_id   — groups all rows ingested in the same run.

    Why we keep ALL columns and ALL rows (including bad records):
      Bronze is the audit layer. Any filtering happens in Silver.
      If Silver has a bug, we can always re-derive it from Bronze without
      going back to the original download.
    """
    from pyspark.sql import functions as F

    raw_path = str(RAW_YELLOW_TAXI_DIR / "*.parquet")
    logger.info(f"Reading raw Parquet files from: {raw_path}")

    # TASK 2.3: Load raw Parquet — Spark reads all 3 monthly files as one DataFrame
    df = spark.read.parquet(raw_path)

    raw_count = df.count()
    logger.info(f"Raw records loaded: {raw_count:,}")

    # TASK 2.4: ingested_at — when did this row enter the lakehouse?
    df = df.withColumn("ingested_at", F.current_timestamp())

    # TASK 2.5: source_file — which Parquet file did this row come from?
    # input_file_name() returns the full path; basename() extracts just the filename.
    df = df.withColumn(
        "source_file",
        F.element_at(F.split(F.input_file_name(), "/"), -1),
    )

    # TASK 2.6: batch_id — unique ID for this pipeline run
    # Every row ingested in the same execution gets the same batch_id.
    # This lets us query "show me everything from run X" for debugging.
    df = df.withColumn("batch_id", F.lit(batch_id))

    logger.info(f"Audit columns added: ingested_at, source_file, batch_id={batch_id}")
    return df


def ingest_taxi_zones(spark, batch_id: str):
    """
    Reads the Taxi Zone Lookup CSV into a Spark DataFrame and appends
    the same audit metadata columns for consistency across all Bronze tables.
    """
    from pyspark.sql import functions as F

    zone_path = str(RAW_TAXI_ZONES_DIR / "taxi_zone_lookup.csv")
    logger.info(f"Reading taxi zone CSV from: {zone_path}")

    df = (
        spark.read
        .option("header", "true")       # first row is the column header
        .option("inferSchema", "true")  # auto-detect int vs string types
        .csv(zone_path)
    )

    logger.info(f"Zone records loaded: {df.count():,}")

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
def save_as_delta(df, output_path: str, table_name: str, write_mode: str) -> None:
    """Saves DataFrame as a Delta table to the specified Bronze layer path."""
    logger.info(f"Writing Bronze table [{table_name}] to: {output_path}  (mode={write_mode})")

    (
        df.write
        .format("delta")
        .mode(write_mode)
        .save(output_path)
    )

    # Confirm by counting
    written = df.count()
    logger.info(f"Saved {table_name}: {written:,} rows -> {output_path}")


# ─────────────────────────────────────────────────────────────────────────────
# Main pipeline runner
# ─────────────────────────────────────────────────────────────────────────────
def run_bronze_ingestion() -> None:
    """
    Orchestrates the full Bronze ingestion:
      1. Start Spark
      2. Load + enrich Yellow Taxi data
      3. Load + enrich Taxi Zone data
      4. Write both as Delta tables
    """
    start = datetime.now()
    write_mode = PIPELINE_SETTINGS["delta_write_mode"]

    # Unique ID for this entire pipeline run
    batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"

    logger.info("=" * 60)
    logger.info("NYC Taxi Lakehouse — Bronze Ingestion")
    logger.info(f"Batch ID  : {batch_id}")
    logger.info(f"Write mode: {write_mode}")
    logger.info("=" * 60)

    spark = create_spark_session()

    try:
        # ── Yellow Taxi trips (Tasks 2.3 – 2.7) ──────────────────────────────
        yellow_df = ingest_yellow_taxi(spark, batch_id)
        save_as_delta(
            df=yellow_df,
            output_path=BRONZE_YELLOW_TAXI_PATH,
            table_name="bronze_yellow_taxi_trips",
            write_mode=write_mode,
        )

        # ── Taxi Zone lookup (Tasks 2.3 – 2.8) ───────────────────────────────
        zones_df = ingest_taxi_zones(spark, batch_id)
        save_as_delta(
            df=zones_df,
            output_path=BRONZE_TAXI_ZONES_PATH,
            table_name="bronze_taxi_zones",
            write_mode=write_mode,
        )

        elapsed = (datetime.now() - start).total_seconds()
        logger.info("=" * 60)
        logger.info(f"Bronze ingestion complete in {elapsed:.1f}s")
        logger.info("=" * 60)

    finally:
        spark.stop()
        logger.info("SparkSession stopped.")


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    run_bronze_ingestion()
