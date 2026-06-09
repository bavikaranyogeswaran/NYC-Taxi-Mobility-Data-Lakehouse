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
from utils.logging_utils import get_logger
from utils.metrics_utils import push_metrics

log = get_logger(__name__, LOGS_DIR / "silver_transformation.log")


# ─────────────────────────────────────────────────────────────────────────────
# SparkSession
# ─────────────────────────────────────────────────────────────────────────────
def create_spark_session():
    from utils.spark_utils import create_spark_session as _create
    return _create("NYC_Taxi_Silver_Transformation")


# ─────────────────────────────────────────────────────────────────────────────
# TASK 3.2 — Read the Bronze yellow_taxi Delta table
# ─────────────────────────────────────────────────────────────────────────────
def read_bronze_trips(spark):
    log.info("reading_bronze", table="yellow_taxi", path=BRONZE_YELLOW_TAXI_PATH)
    df = spark.read.format("delta").load(BRONZE_YELLOW_TAXI_PATH)
    count = df.count()
    log.info("data_loaded", source="bronze", table="yellow_taxi", record_count=count)
    return df


def read_bronze_zones(spark):
    log.info("reading_bronze", table="taxi_zones", path=BRONZE_TAXI_ZONES_PATH)
    df = spark.read.format("delta").load(BRONZE_TAXI_ZONES_PATH)
    count = df.count()
    log.info("data_loaded", source="bronze", table="taxi_zones", record_count=count)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# TASK 3.3 — Define Data Quality filter conditions
# ─────────────────────────────────────────────────────────────────────────────
def get_dq_conditions(df):
    from pyspark.sql import functions as F

    return (
        F.col("tpep_pickup_datetime").isNotNull()  # type: ignore[call-arg]
        & F.col("tpep_dropoff_datetime").isNotNull()  # type: ignore[call-arg]
        & (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
        & (F.col("trip_distance") > 0)
        & (F.col("fare_amount") >= 0)
        & (F.col("total_amount") >= 0)
        & (F.col("passenger_count") > 0)
    )


# ─────────────────────────────────────────────────────────────────────────────
# TASK 3.4 & 3.5 — Quarantine invalid records
# ─────────────────────────────────────────────────────────────────────────────
def quarantine_invalid_records(df, valid_condition, write_mode: str) -> int:
    from pyspark.sql import functions as F

    log.info("dq_identifying_invalids")

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
    log.info("dq_invalids_found", invalid_count=invalid_count)

    if invalid_count > 0:
        log.info("writing_quarantine", path=QUARANTINE_YELLOW_TAXI_PATH, record_count=invalid_count)
        invalid_df.write.format("delta").mode(write_mode).save(QUARANTINE_YELLOW_TAXI_PATH)
        log.info("quarantine_written", record_count=invalid_count)

        # Log reason breakdown (shown in Airflow task logs)
        invalid_df.groupBy("dq_fail_reason").count().orderBy("count", ascending=False).show(
            truncate=False
        )

    return invalid_count


# ─────────────────────────────────────────────────────────────────────────────
# TASK 3.6 — Filter valid records
# ─────────────────────────────────────────────────────────────────────────────
def filter_valid_records(df, valid_condition):
    valid_df = df.filter(valid_condition)
    valid_count = valid_df.count()
    log.info("dq_valid_records", valid_count=valid_count)
    return valid_df


# ─────────────────────────────────────────────────────────────────────────────
# TASK 3.7 & 3.8 — Add derived metric and temporal feature columns
# ─────────────────────────────────────────────────────────────────────────────
def add_derived_columns(df):
    from pyspark.sql import functions as F

    log.info("adding_derived_columns")

    df = (
        df
        .withColumn(
            "trip_duration_minutes",
            (
                F.unix_timestamp("tpep_dropoff_datetime")
                - F.unix_timestamp("tpep_pickup_datetime")
            ) / 60.0,
        )
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .withColumn("day_of_week", F.date_format("tpep_pickup_datetime", "EEEE"))
        .withColumn("day_of_week_num", F.dayofweek("tpep_pickup_datetime"))
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
    from pyspark.sql import functions as F

    log.info("zone_join_start")

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

    log.info("zone_join_complete")
    return enriched_df


# ─────────────────────────────────────────────────────────────────────────────
# TASK 3.11 — Write to Silver Delta table
# ─────────────────────────────────────────────────────────────────────────────
def save_silver_table(df, write_mode: str) -> int:
    log.info("writing_table", table="silver_yellow_taxi", path=SILVER_YELLOW_TAXI_PATH, mode=write_mode)

    silver_df = df.select(
        "vendor_id", "rate_code_id", "payment_type", "store_and_fwd_flag",
        "pickup_datetime", "dropoff_datetime", "pickup_date", "pickup_hour",
        "day_of_week", "day_of_week_num", "trip_duration_minutes",
        "passenger_count", "trip_distance",
        "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
        "improvement_surcharge", "congestion_surcharge", "airport_fee", "total_amount",
        "pickup_location_id", "pickup_zone", "pickup_borough", "pickup_service_zone",
        "dropoff_location_id", "dropoff_zone", "dropoff_borough", "dropoff_service_zone",
        "source_file", "ingested_at", "batch_id",
    )

    silver_df.write.format("delta").mode(write_mode).save(SILVER_YELLOW_TAXI_PATH)
    written = silver_df.count()
    log.info("data_written", table="silver_yellow_taxi", record_count=written, path=SILVER_YELLOW_TAXI_PATH)
    return written


# ─────────────────────────────────────────────────────────────────────────────
# Main pipeline runner
# ─────────────────────────────────────────────────────────────────────────────
def run_silver_transformation() -> None:
    start = datetime.now()
    write_mode = PIPELINE_SETTINGS["delta_write_mode"]

    log.info("stage_start", stage="silver", write_mode=write_mode)

    spark = create_spark_session()

    try:
        bronze_trips = read_bronze_trips(spark)
        bronze_zones = read_bronze_zones(spark)
        total_bronze = bronze_trips.count()

        valid_condition = get_dq_conditions(bronze_trips)
        invalid_count = quarantine_invalid_records(bronze_trips, valid_condition, write_mode)
        valid_trips = filter_valid_records(bronze_trips, valid_condition)
        valid_count = valid_trips.count()

        enriched_trips = add_derived_columns(valid_trips)
        final_trips = enrich_with_zones(enriched_trips, bronze_zones)
        save_silver_table(final_trips, write_mode)

        elapsed = (datetime.now() - start).total_seconds()
        invalid_rate = (invalid_count / total_bronze * 100) if total_bronze > 0 else 0.0

        log.info(
            "stage_complete",
            stage="silver",
            total_bronze_records=total_bronze,
            valid_records=valid_count,
            quarantined_records=invalid_count,
            invalid_rate_pct=round(invalid_rate, 2),
            duration_seconds=round(elapsed, 1),
        )

        push_metrics(
            "silver_transform",
            silver_valid_rows=valid_count,
            silver_invalid_rows=invalid_count,
            silver_invalid_rate_pct=invalid_rate,
            silver_duration_seconds=elapsed,
        )

    finally:
        spark.stop()
        log.info("spark_stopped", stage="silver")


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    run_silver_transformation()
