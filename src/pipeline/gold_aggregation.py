"""
gold_aggregation.py
─────────────────────────────────────────────────────────────────
Gold Layer — Phase 4 (Tasks 4.1 – 4.5)

Reads the clean, enriched Silver Delta table and produces aggregated
business-level metrics. These tables are highly optimized for direct
dashboard consumption (e.g., Superset).

Gold Data Marts produced:
  1. gold_daily_revenue_by_zone : Revenue & trip volumes aggregated by day and pickup zone.
  2. gold_hourly_performance    : Avg duration, avg distance, and avg speed by hour of day.
  3. gold_route_summary         : Top pickup-to-dropoff routes by volume.
  4. gold_data_quality_summary  : Daily invalid rates from the quarantine table.

Usage:
    python src/pipeline/gold_aggregation.py
─────────────────────────────────────────────────────────────────
"""

import os
import sys
import logging
from pathlib import Path
from datetime import datetime

# ── Resolve project root ──────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from config import (
    SILVER_YELLOW_TAXI_PATH,
    QUARANTINE_YELLOW_TAXI_PATH,
    GOLD_DIR,
    LOGS_DIR,
    PIPELINE_SETTINGS,
)

# ── Logging setup ─────────────────────────────────────────────────────────────
LOGS_DIR.mkdir(parents=True, exist_ok=True)
log_file = LOGS_DIR / "gold_aggregation.log"

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
# SparkSession
# ─────────────────────────────────────────────────────────────────────────────
def create_spark_session():
    """Boots PySpark+Delta, delegating to the shared spark_utils factory."""
    from utils.spark_utils import create_spark_session as _create
    return _create("NYC_Taxi_Gold_Aggregation")


# ─────────────────────────────────────────────────────────────────────────────
# TASK 4.2 — Read Silver Table
# ─────────────────────────────────────────────────────────────────────────────
def read_silver_trips(spark):
    """Reads the clean Silver Delta table as the source of truth."""
    path = SILVER_YELLOW_TAXI_PATH
    logger.info(f"Reading Silver trips from: {path}")
    df = spark.read.format("delta").load(path)
    logger.info(f"Silver records loaded: {df.count():,}")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# TASK 4.3 — Aggregate: Daily Revenue by Zone
# ─────────────────────────────────────────────────────────────────────────────
def aggregate_daily_revenue_by_zone(df):
    """
    Creates a data mart answering:
      "Which zones generate the most revenue and trips on a given day?"

    Aggregates:
      - Total trips
      - Total revenue (sum of total_amount)
      - Total passengers
      - Average fare per trip
    """
    from pyspark.sql import functions as F

    logger.info("Aggregating daily revenue by pickup zone ...")

    agg_df = (
        df.groupBy("pickup_date", "pickup_borough", "pickup_zone")
        .agg(
            F.count("*").alias("total_trips"),
            F.sum("total_amount").alias("total_revenue"),
            F.sum("passenger_count").alias("total_passengers"),
            F.avg("fare_amount").alias("avg_fare"),
        )
        # Filter out rows where the zone didn't map properly
        .where("pickup_zone IS NOT NULL")
        .orderBy(F.desc("pickup_date"), F.desc("total_revenue"))
    )

    return agg_df


# ─────────────────────────────────────────────────────────────────────────────
# TASK 4.4 — Aggregate: Hourly Performance
# ─────────────────────────────────────────────────────────────────────────────
def aggregate_hourly_performance(df):
    """
    Creates a data mart answering:
      "How does taxi speed and trip duration vary by hour of the day?"

    Aggregates:
      - Total trips
      - Average trip duration (minutes)
      - Average trip distance (miles)
      - Imputed average speed (mph) = distance / (duration / 60)
    """
    from pyspark.sql import functions as F

    logger.info("Aggregating hourly performance metrics ...")

    agg_df = (
        df.groupBy("pickup_hour")
        .agg(
            F.count("*").alias("total_trips"),
            F.avg("trip_duration_minutes").alias("avg_duration_minutes"),
            F.avg("trip_distance").alias("avg_distance_miles"),
        )
        # Calculate mph (handle division by zero just in case)
        .withColumn(
            "avg_speed_mph",
            F.when(
                F.col("avg_duration_minutes") > 0,
                F.col("avg_distance_miles") / (F.col("avg_duration_minutes") / 60.0)
            ).otherwise(0)
        )
        .orderBy("pickup_hour")
    )

    return agg_df


# ─────────────────────────────────────────────────────────────────────────────
# TASK 4.6 — Aggregate: Route Summary
# ─────────────────────────────────────────────────────────────────────────────
def aggregate_route_summary(df):
    """
    Creates a data mart answering:
      "What are the most popular routes between zones?"
    """
    from pyspark.sql import functions as F

    logger.info("Aggregating route summary ...")

    agg_df = (
        df.groupBy("pickup_zone", "dropoff_zone")
        .agg(
            F.count("*").alias("total_trips"),
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("fare_amount").alias("avg_fare")
        )
        .where("pickup_zone IS NOT NULL AND dropoff_zone IS NOT NULL")
        .orderBy(F.desc("total_trips"))
    )
    return agg_df

# ─────────────────────────────────────────────────────────────────────────────
# TASK 4.7 — Aggregate: Data Quality Summary
# ─────────────────────────────────────────────────────────────────────────────
def aggregate_dq_summary(spark):
    """
    Reads from Quarantine and Silver to provide a high-level summary
    of data quality over time.
    """
    from pyspark.sql import functions as F

    logger.info("Aggregating data quality summary ...")

    silver_df = spark.read.format("delta").load(SILVER_YELLOW_TAXI_PATH)
    quarantine_df = spark.read.format("delta").load(QUARANTINE_YELLOW_TAXI_PATH)

    # Get daily valid counts
    valid_daily = (
        silver_df.groupBy("pickup_date")
        .agg(F.count("*").alias("valid_records"))
        .withColumnRenamed("pickup_date", "dq_date")
    )

    # Get daily invalid counts (using ingested_at since bad records might have null dates)
    invalid_daily = (
        quarantine_df.withColumn("dq_date", F.to_date("ingested_at"))
        .groupBy("dq_date", "dq_fail_reason")
        .agg(F.count("*").alias("invalid_records"))
    )

    # Join them
    dq_summary = invalid_daily.join(valid_daily, "dq_date", "full_outer")

    return dq_summary

# ─────────────────────────────────────────────────────────────────────────────
# TASK 4.5 — Save Gold Tables
# ─────────────────────────────────────────────────────────────────────────────
def save_gold_table(df, folder_name: str, table_name: str, write_mode: str):
    """Saves an aggregated DataFrame as a Delta table in the Gold layer."""
    path_str = f"{GOLD_DIR}/{folder_name}"

    logger.info(f"Writing Gold table [{table_name}] to: {path_str}  (mode={write_mode})")

    (
        df.write
        .format("delta")
        .mode(write_mode)
        .save(path_str)
    )
    written = df.count()
    logger.info(f"Saved {table_name}: {written:,} rows -> {path_str}")

# ─────────────────────────────────────────────────────────────────────────────
# TASK 7.2 — Save Gold Tables to Postgres (Serving Layer for Superset)
# ─────────────────────────────────────────────────────────────────────────────
def save_gold_to_postgres(df, table_name: str, write_mode: str):
    """Pushes Gold Data Marts to Postgres so Superset can query them instantly."""
    # If running inside Airflow Docker, host is 'postgres', else '127.0.0.1'
    pg_host = os.environ.get("POSTGRES_HOST", "127.0.0.1")
    url = f"jdbc:postgresql://{pg_host}:5432/superset"
    properties = {
        "user": "superset",
        "password": "superset",
        "driver": "org.postgresql.Driver",
        # Use stringtype=unspecified to avoid strict typing errors
        "stringtype": "unspecified"
    }

    logger.info(f"Pushing Gold table [{table_name}] to Postgres at {pg_host}...")
    (
        df.write.jdbc(
            url=url,
            table=table_name,
            mode=write_mode,
            properties=properties
        )
    )
    logger.info(f"Successfully pushed [{table_name}] to Postgres.")


# ─────────────────────────────────────────────────────────────────────────────
# Main pipeline runner
# ─────────────────────────────────────────────────────────────────────────────
def run_gold_aggregation() -> None:
    """Orchestrates the Gold aggregation pipeline."""
    start = datetime.now()
    write_mode = PIPELINE_SETTINGS["delta_write_mode"]

    logger.info("=" * 60)
    logger.info("NYC Taxi Lakehouse -- Gold Aggregation")
    logger.info(f"Write mode: {write_mode}")
    logger.info("=" * 60)

    spark = create_spark_session()

    try:
        # Task 4.2
        silver_df = read_silver_trips(spark)

        # ── 1. Daily Revenue by Zone
        daily_revenue_df = aggregate_daily_revenue_by_zone(silver_df)
        save_gold_table(
            df=daily_revenue_df,
            folder_name="daily_revenue_by_zone",
            table_name="gold_daily_revenue_by_zone",
            write_mode=write_mode,
        )
        save_gold_to_postgres(daily_revenue_df, "gold_daily_revenue", write_mode)

        # ── 2. Hourly Performance
        hourly_performance_df = aggregate_hourly_performance(silver_df)
        save_gold_table(
            df=hourly_performance_df,
            folder_name="hourly_performance",
            table_name="gold_hourly_performance",
            write_mode=write_mode,
        )
        save_gold_to_postgres(hourly_performance_df, "gold_hourly_performance", write_mode)

        # ── 3. Route Summary
        route_summary_df = aggregate_route_summary(silver_df)
        save_gold_table(
            df=route_summary_df,
            folder_name="route_summary",
            table_name="gold_route_summary",
            write_mode=write_mode,
        )
        save_gold_to_postgres(route_summary_df, "gold_route_summary", write_mode)

        # ── 4. Data Quality Summary
        dq_summary_df = aggregate_dq_summary(spark)
        save_gold_table(
            df=dq_summary_df,
            folder_name="data_quality_summary",
            table_name="gold_data_quality_summary",
            write_mode=write_mode,
        )
        save_gold_to_postgres(dq_summary_df, "gold_dq_summary", write_mode)

        elapsed = (datetime.now() - start).total_seconds()
        logger.info("=" * 60)
        logger.info(f"Gold aggregation complete in {elapsed:.1f}s")
        logger.info("=" * 60)

    finally:
        spark.stop()
        logger.info("SparkSession stopped.")


if __name__ == "__main__":
    run_gold_aggregation()
