"""
silver_transformation.py
─────────────────────────────────────────────────────────────────
Silver Layer — Phase 3 (Tasks 3.1 – 3.11)

Reads the Bronze Delta tables, applies data quality rules, routes
bad records to the Quarantine folder, enriches valid records with
derived metrics and zone name lookups, then writes a clean Silver
Delta table.

Silver rules:
  - Read ONLY from Bronze (never from raw files).
  - Enforce strict business data quality constraints.
  - Save ALL rejected rows to Quarantine with a reason column.
  - Add derived columns that make dashboarding fast.
  - Join taxi zone names so the BI tool never sees raw IDs.

Usage:
    python src/pipeline/silver_transformation.py
─────────────────────────────────────────────────────────────────
"""

import sys
import logging
from pathlib import Path
from datetime import datetime

# ── Resolve project root ──────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from config import (
    BRONZE_YELLOW_TAXI_PATH,
    BRONZE_TAXI_ZONES_PATH,
    SILVER_YELLOW_TAXI_PATH,
    QUARANTINE_YELLOW_TAXI_PATH,
    LOGS_DIR,
    PIPELINE_SETTINGS,
)

# ── Logging setup ─────────────────────────────────────────────────────────────
LOGS_DIR.mkdir(parents=True, exist_ok=True)
log_file = LOGS_DIR / "silver_transformation.log"

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
# SparkSession (same pattern as Bronze — with Windows HADOOP_HOME fix)
# ─────────────────────────────────────────────────────────────────────────────
def create_spark_session():
    """Boots PySpark+Delta, delegating to the shared spark_utils factory."""
    from utils.spark_utils import create_spark_session as _create
    return _create("NYC_Taxi_Silver_Transformation")


# ─────────────────────────────────────────────────────────────────────────────
# TASK 3.2 — Read the Bronze yellow_taxi Delta table
# ─────────────────────────────────────────────────────────────────────────────
def read_bronze_trips(spark):
    """
    Reads the Bronze yellow_taxi Delta table.

    We read from Bronze (not raw Parquet) so that our audit columns
    (ingested_at, source_file, batch_id) are already present and any
    re-processing starts from the same trusted checkpoint.
    """
    logger.info(f"Reading Bronze trips from: {BRONZE_YELLOW_TAXI_PATH}")
    df = spark.read.format("delta").load(BRONZE_YELLOW_TAXI_PATH)
    logger.info(f"Bronze trips loaded: {df.count():,} rows")
    return df


def read_bronze_zones(spark):
    """Reads the Bronze taxi_zones Delta table."""
    logger.info(f"Loading Bronze taxi zones from: {BRONZE_TAXI_ZONES_PATH}")
    df = spark.read.format("delta").load(BRONZE_TAXI_ZONES_PATH)
    logger.info(f"Bronze zones loaded: {df.count():,} rows")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# TASK 3.3 — Define Data Quality filter conditions
# ─────────────────────────────────────────────────────────────────────────────
def get_dq_conditions(df):
    """
    Returns a Spark Column expression that is True for VALID rows.

    Rules (from the project specification):
      1. pickup_datetime must not be null
      2. dropoff_datetime must not be null
      3. dropoff_datetime must be AFTER pickup_datetime
      4. trip_distance must be > 0  (zero-distance trips are suspicious)
      5. fare_amount must be >= 0   (negative fares are data errors)
      6. total_amount must be >= 0
      7. passenger_count must be > 0

    We don't filter on location IDs here — the zone join will produce
    nulls for unknown IDs, which we can track in the Gold DQ table.
    """
    from pyspark.sql import functions as F

    valid_condition = (
        F.col("tpep_pickup_datetime").isNotNull()  # type: ignore[call-arg]
        & F.col("tpep_dropoff_datetime").isNotNull()  # type: ignore[call-arg]
        & (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
        & (F.col("trip_distance") > 0)
        & (F.col("fare_amount") >= 0)
        & (F.col("total_amount") >= 0)
        & (F.col("passenger_count") > 0)
    )
    return valid_condition


# ─────────────────────────────────────────────────────────────────────────────
# TASK 3.4 & 3.5 — Create Invalid Records DataFrame and save to Quarantine
# ─────────────────────────────────────────────────────────────────────────────
def quarantine_invalid_records(df, valid_condition, write_mode: str) -> int:
    """
    TASK 3.4: Builds the "Invalid Records" DataFrame by applying the
              INVERSE of the DQ conditions.
    TASK 3.5: Saves rejected rows to data/quarantine/yellow_taxi/ as Delta.

    Why save to Quarantine instead of just dropping?
      - Data engineers can inspect WHY rows failed (e.g. upstream system bug).
      - The Gold DQ metrics table reads from Quarantine to calculate invalid_rate.
      - If a DQ rule is wrong, you can re-process from Bronze without re-downloading.

    A dq_fail_reason column is added so analysts know which rule was violated.
    """
    from pyspark.sql import functions as F

    logger.info("Identifying invalid records ...")

    # Build individual failure conditions with readable labels
    # Each row gets labelled with the FIRST rule it fails
    invalid_df = df.filter(~valid_condition).withColumn(
        "dq_fail_reason",
        F.when(F.col("tpep_pickup_datetime").isNull(), "null_pickup_datetime")  # type: ignore[call-arg]
        .when(F.col("tpep_dropoff_datetime").isNull(), "null_dropoff_datetime")  # type: ignore[call-arg]
        .when(
            F.col("tpep_dropoff_datetime") <= F.col("tpep_pickup_datetime"),
            "dropoff_before_pickup",
        )
        .when(F.col("trip_distance") <= 0, "zero_or_negative_distance")
        .when(F.col("fare_amount") < 0, "negative_fare")
        .when(F.col("total_amount") < 0, "negative_total_amount")
        .when(F.col("passenger_count") <= 0, "zero_or_negative_passengers")
        .otherwise("multiple_failures"),
    )

    invalid_count = invalid_df.count()
    logger.info(f"Invalid records found: {invalid_count:,}")

    if invalid_count > 0:
        quarantine_path = QUARANTINE_YELLOW_TAXI_PATH
        logger.info(f"Writing {invalid_count:,} quarantine records to: {quarantine_path}")
        (
            invalid_df.write
            .format("delta")
            .mode(write_mode)
            .save(quarantine_path)
        )
        logger.info("Quarantine write complete.")

        # Log a breakdown of failure reasons
        logger.info("Quarantine failure reason breakdown:")
        invalid_df.groupBy("dq_fail_reason").count().orderBy("count", ascending=False).show(
            truncate=False
        )

    return invalid_count


# ─────────────────────────────────────────────────────────────────────────────
# TASK 3.6 — Create Valid Records DataFrame
# ─────────────────────────────────────────────────────────────────────────────
def filter_valid_records(df, valid_condition):
    """
    TASK 3.6: Applies the DQ conditions to produce a clean DataFrame
              containing only valid, trustworthy trip records.
    """
    valid_df = df.filter(valid_condition)
    valid_count = valid_df.count()
    logger.info(f"Valid records: {valid_count:,}")
    return valid_df


# ─────────────────────────────────────────────────────────────────────────────
# TASK 3.7 & 3.8 — Add derived metric and temporal feature columns
# ─────────────────────────────────────────────────────────────────────────────
def add_derived_columns(df):
    """
    TASK 3.7: Calculates trip_duration_minutes from the pickup/dropoff timestamps.
              Pre-calculating this avoids the BI tool having to compute it at
              query time across millions of rows.

    TASK 3.8: Extracts pickup_date, pickup_hour, and day_of_week so that
              dashboards can GROUP BY these without expensive timestamp parsing.

    Also standardises raw column names to consistent snake_case for Silver.
    """
    from pyspark.sql import functions as F

    logger.info("Adding derived columns and standardising column names ...")

    df = (
        df
        # ── TASK 3.7: Trip duration in minutes ───────────────────────────────
        .withColumn(
            "trip_duration_minutes",
            (
                F.unix_timestamp("tpep_dropoff_datetime")
                - F.unix_timestamp("tpep_pickup_datetime")
            ) / 60.0,
        )

        # ── TASK 3.8: Temporal features ──────────────────────────────────────
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .withColumn("day_of_week", F.date_format("tpep_pickup_datetime", "EEEE"))
        # day_of_week_num: 1=Sunday ... 7=Saturday (useful for sorting)
        .withColumn("day_of_week_num", F.dayofweek("tpep_pickup_datetime"))

        # ── Standardise column names to snake_case ────────────────────────────
        .withColumnRenamed("VendorID",              "vendor_id")
        .withColumnRenamed("tpep_pickup_datetime",  "pickup_datetime")
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
        .withColumnRenamed("RatecodeID",            "rate_code_id")
        .withColumnRenamed("PULocationID",          "pickup_location_id")
        .withColumnRenamed("DOLocationID",          "dropoff_location_id")
        .withColumnRenamed("Airport_fee",           "airport_fee")
    )

    return df


# ─────────────────────────────────────────────────────────────────────────────
# TASK 3.9 & 3.10 — Load Taxi Zones and Broadcast Join
# ─────────────────────────────────────────────────────────────────────────────
def enrich_with_zones(trips_df, zones_df):
    """
    TASK 3.9 : Loads the Bronze taxi_zones Delta table (already done by caller).
    TASK 3.10: Performs TWO Broadcast Joins — one for pickup zone, one for dropoff zone.

    Why Broadcast Join?
      taxi_zones has only 265 rows. Broadcasting it to all Spark executors
      means Spark avoids a costly shuffle of the 9M-row trips table.
      This makes the join ~10x faster than a standard sort-merge join.

    The result replaces raw integer IDs like "132" with readable strings
    like "JFK Airport" and "Queens" so dashboards are human-friendly.
    """
    from pyspark.sql import functions as F

    logger.info("Joining trips with taxi zone lookup (Broadcast Join) ...")

    # Prepare zone lookup with distinct aliases for pickup and dropoff joins
    pickup_zones = zones_df.select(
        F.col("LocationID").alias("pu_location_id"),
        F.col("Zone").alias("pickup_zone"),
        F.col("Borough").alias("pickup_borough"),
        F.col("service_zone").alias("pickup_service_zone"),
    )

    dropoff_zones = zones_df.select(
        F.col("LocationID").alias("do_location_id"),
        F.col("Zone").alias("dropoff_zone"),
        F.col("Borough").alias("dropoff_borough"),
        F.col("service_zone").alias("dropoff_service_zone"),
    )

    # Left join so trips with unknown location IDs are not dropped
    # (they pass DQ but get null zone names — tracked in Gold DQ table)
    enriched_df = (
        trips_df
        .join(F.broadcast(pickup_zones),
              trips_df["pickup_location_id"] == pickup_zones["pu_location_id"],
              how="left")
        .drop("pu_location_id")

        .join(F.broadcast(dropoff_zones),
              trips_df["dropoff_location_id"] == dropoff_zones["do_location_id"],
              how="left")
        .drop("do_location_id")
    )

    logger.info("Zone join complete.")
    return enriched_df


# ─────────────────────────────────────────────────────────────────────────────
# TASK 3.11 — Write to Silver Delta table
# ─────────────────────────────────────────────────────────────────────────────
def save_silver_table(df, write_mode: str) -> None:
    """
    TASK 3.11: Writes the enriched, validated DataFrame as a Delta table.
    We select and reorder columns into the final Silver schema here so
    the output matches the specification exactly.
    """

    logger.info("Writing clean records to Silver Delta table ...")

    # Select final Silver schema — drop Bronze audit cols that don't belong here
    silver_df = df.select(
        # Identifiers
        "vendor_id",
        "rate_code_id",
        "payment_type",
        "store_and_fwd_flag",
        # Timestamps & temporal features
        "pickup_datetime",
        "dropoff_datetime",
        "pickup_date",
        "pickup_hour",
        "day_of_week",
        "day_of_week_num",
        "trip_duration_minutes",
        # Trip metrics
        "passenger_count",
        "trip_distance",
        # Fares
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "congestion_surcharge",
        "airport_fee",
        "total_amount",
        # Locations (IDs + readable names)
        "pickup_location_id",
        "pickup_zone",
        "pickup_borough",
        "pickup_service_zone",
        "dropoff_location_id",
        "dropoff_zone",
        "dropoff_borough",
        "dropoff_service_zone",
        # Lineage columns (kept for traceability back to Bronze)
        "source_file",
        "ingested_at",
        "batch_id",
    )

    (
        silver_df.write
        .format("delta")
        .mode(write_mode)
        .save(SILVER_YELLOW_TAXI_PATH)
    )

    written = silver_df.count()
    logger.info(f"Saved {written:,} clean rows to Silver -> {SILVER_YELLOW_TAXI_PATH}")


# ─────────────────────────────────────────────────────────────────────────────
# Main pipeline runner
# ─────────────────────────────────────────────────────────────────────────────
def run_silver_transformation() -> None:
    """
    Orchestrates the full Silver transformation:
      1. Start Spark
      2. Read Bronze trips + zones
      3. Apply DQ rules → quarantine invalids
      4. Enrich valid records with derived columns + zone names
      5. Write Silver Delta table
      6. Print summary statistics
    """
    start = datetime.now()
    write_mode = PIPELINE_SETTINGS["delta_write_mode"]

    logger.info("=" * 60)
    logger.info("NYC Taxi Lakehouse -- Silver Transformation")
    logger.info(f"Write mode: {write_mode}")
    logger.info("=" * 60)

    spark = create_spark_session()

    try:
        # ── TASKS 3.2 & 3.9: Read Bronze tables ──────────────────────────────
        bronze_trips = read_bronze_trips(spark)
        bronze_zones = read_bronze_zones(spark)
        total_bronze = bronze_trips.count()

        # ── TASK 3.3: Define DQ conditions ───────────────────────────────────
        valid_condition = get_dq_conditions(bronze_trips)

        # ── TASKS 3.4 & 3.5: Quarantine invalid records ───────────────────────
        invalid_count = quarantine_invalid_records(bronze_trips, valid_condition, write_mode)

        # ── TASK 3.6: Filter valid records ────────────────────────────────────
        valid_trips = filter_valid_records(bronze_trips, valid_condition)
        valid_count = valid_trips.count()

        # ── TASKS 3.7 & 3.8: Add derived columns ─────────────────────────────
        enriched_trips = add_derived_columns(valid_trips)

        # ── TASK 3.10: Broadcast join with zones ──────────────────────────────
        final_trips = enrich_with_zones(enriched_trips, bronze_zones)

        # ── TASK 3.11: Write Silver Delta table ───────────────────────────────
        save_silver_table(final_trips, write_mode)

        # ── Summary ───────────────────────────────────────────────────────────
        elapsed = (datetime.now() - start).total_seconds()
        invalid_rate = (invalid_count / total_bronze * 100) if total_bronze > 0 else 0

        logger.info("=" * 60)
        logger.info("Silver Transformation Summary")
        logger.info(f"  Total Bronze records : {total_bronze:,}")
        logger.info(f"  Valid records        : {valid_count:,}")
        logger.info(f"  Quarantined records  : {invalid_count:,}")
        logger.info(f"  Invalid rate         : {invalid_rate:.2f}%")
        logger.info(f"  Total runtime        : {elapsed:.1f}s")
        logger.info("=" * 60)

    finally:
        spark.stop()
        logger.info("SparkSession stopped.")


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    run_silver_transformation()
