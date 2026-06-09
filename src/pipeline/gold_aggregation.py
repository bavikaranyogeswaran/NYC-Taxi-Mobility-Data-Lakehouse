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

Results are written to MinIO (Delta tables) and pushed to PostgreSQL
for the FastAPI backend and React dashboard.

Usage:
    python src/pipeline/gold_aggregation.py
─────────────────────────────────────────────────────────────────
"""

import os
import sys
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
from utils.logging_utils import get_logger
from utils.metrics_utils import push_metrics

log = get_logger(__name__, LOGS_DIR / "gold_aggregation.log")


# ─────────────────────────────────────────────────────────────────────────────
# SparkSession
# ─────────────────────────────────────────────────────────────────────────────
def create_spark_session():
    from utils.spark_utils import create_spark_session as _create
    return _create("NYC_Taxi_Gold_Aggregation")


# ─────────────────────────────────────────────────────────────────────────────
# TASK 4.2 — Read Silver Table
# ─────────────────────────────────────────────────────────────────────────────
def read_silver_trips(spark):
    log.info("reading_silver", path=SILVER_YELLOW_TAXI_PATH)
    df = spark.read.format("delta").load(SILVER_YELLOW_TAXI_PATH)
    count = df.count()
    log.info("data_loaded", source="silver", table="yellow_taxi", record_count=count)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# TASK 4.3 — Aggregate: Daily Revenue by Zone
# ─────────────────────────────────────────────────────────────────────────────
def aggregate_daily_revenue_by_zone(df):
    from pyspark.sql import functions as F

    log.info("aggregating", mart="daily_revenue_by_zone")

    return (
        df.groupBy("pickup_date", "pickup_borough", "pickup_zone")
        .agg(
            F.count("*").alias("total_trips"),
            F.sum("total_amount").alias("total_revenue"),
            F.sum("passenger_count").alias("total_passengers"),
            F.avg("fare_amount").alias("avg_fare"),
        )
        .where("pickup_zone IS NOT NULL")
        .orderBy(F.desc("pickup_date"), F.desc("total_revenue"))
    )


# ─────────────────────────────────────────────────────────────────────────────
# TASK 4.4 — Aggregate: Hourly Performance
# ─────────────────────────────────────────────────────────────────────────────
def aggregate_hourly_performance(df):
    from pyspark.sql import functions as F

    log.info("aggregating", mart="hourly_performance")

    return (
        df.groupBy("pickup_hour")
        .agg(
            F.count("*").alias("total_trips"),
            F.avg("trip_duration_minutes").alias("avg_duration_minutes"),
            F.avg("trip_distance").alias("avg_distance_miles"),
        )
        .withColumn(
            "avg_speed_mph",
            F.when(
                F.col("avg_duration_minutes") > 0,
                F.col("avg_distance_miles") / (F.col("avg_duration_minutes") / 60.0)
            ).otherwise(0)
        )
        .orderBy("pickup_hour")
    )


# ─────────────────────────────────────────────────────────────────────────────
# TASK 4.6 — Aggregate: Route Summary
# ─────────────────────────────────────────────────────────────────────────────
def aggregate_route_summary(df):
    from pyspark.sql import functions as F

    log.info("aggregating", mart="route_summary")

    return (
        df.groupBy("pickup_zone", "dropoff_zone")
        .agg(
            F.count("*").alias("total_trips"),
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("fare_amount").alias("avg_fare")
        )
        .where("pickup_zone IS NOT NULL AND dropoff_zone IS NOT NULL")
        .orderBy(F.desc("total_trips"))
    )


# ─────────────────────────────────────────────────────────────────────────────
# TASK 4.7 — Aggregate: Data Quality Summary
# ─────────────────────────────────────────────────────────────────────────────
def aggregate_dq_summary(spark):
    from pyspark.sql import functions as F

    log.info("aggregating", mart="data_quality_summary")

    silver_df = spark.read.format("delta").load(SILVER_YELLOW_TAXI_PATH)
    quarantine_df = spark.read.format("delta").load(QUARANTINE_YELLOW_TAXI_PATH)

    valid_daily = (
        silver_df.groupBy("pickup_date")
        .agg(F.count("*").alias("valid_records"))
        .withColumnRenamed("pickup_date", "dq_date")
    )

    invalid_daily = (
        quarantine_df.withColumn("dq_date", F.to_date("ingested_at"))
        .groupBy("dq_date", "dq_fail_reason")
        .agg(F.count("*").alias("invalid_records"))
    )

    return invalid_daily.join(valid_daily, "dq_date", "full_outer")


# ─────────────────────────────────────────────────────────────────────────────
# TASK 4.5 — Save Gold Tables
# ─────────────────────────────────────────────────────────────────────────────
def save_gold_table(df, folder_name: str, table_name: str, write_mode: str) -> int:
    path_str = f"{GOLD_DIR}/{folder_name}"
    log.info("writing_table", table=table_name, path=path_str, mode=write_mode)
    df.write.format("delta").mode(write_mode).save(path_str)
    written = df.count()
    log.info("data_written", table=table_name, record_count=written, path=path_str)
    return written


# ─────────────────────────────────────────────────────────────────────────────
# TASK 7.2 — Save Gold Tables to Postgres
# ─────────────────────────────────────────────────────────────────────────────
def save_gold_to_postgres(df, table_name: str, write_mode: str):
    pg_host = os.environ.get("PG_HOST", "127.0.0.1")
    pg_port = os.environ.get("PG_PORT", "5432")
    pg_db   = os.environ.get("PG_DB", "lakehouse")
    pg_user = os.environ.get("PG_USER")
    pg_pass = os.environ.get("PG_PASS")

    if not pg_user or not pg_pass:
        raise EnvironmentError("PG_USER and PG_PASS environment variables must be set")

    url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"
    properties = {
        "user": pg_user,
        "password": pg_pass,
        "driver": "org.postgresql.Driver",
        "stringtype": "unspecified"
    }

    log.info("pushing_to_postgres", table=table_name, host=pg_host)
    df.write.jdbc(url=url, table=table_name, mode=write_mode, properties=properties)
    log.info("postgres_push_complete", table=table_name)


# ─────────────────────────────────────────────────────────────────────────────
# Main pipeline runner
# ─────────────────────────────────────────────────────────────────────────────
def run_gold_aggregation() -> None:
    start = datetime.now()
    write_mode = PIPELINE_SETTINGS["delta_write_mode"]

    log.info("stage_start", stage="gold", write_mode=write_mode)

    spark = create_spark_session()

    try:
        silver_df = read_silver_trips(spark)

        daily_revenue_df = aggregate_daily_revenue_by_zone(silver_df)
        daily_rows = save_gold_table(daily_revenue_df, "daily_revenue_by_zone", "gold_daily_revenue_by_zone", write_mode)
        save_gold_to_postgres(daily_revenue_df, "gold_daily_revenue", write_mode)

        hourly_df = aggregate_hourly_performance(silver_df)
        hourly_rows = save_gold_table(hourly_df, "hourly_performance", "gold_hourly_performance", write_mode)
        save_gold_to_postgres(hourly_df, "gold_hourly_performance", write_mode)

        route_df = aggregate_route_summary(silver_df)
        save_gold_table(route_df, "route_summary", "gold_route_summary", write_mode)
        save_gold_to_postgres(route_df, "gold_route_summary", write_mode)

        dq_df = aggregate_dq_summary(spark)
        save_gold_table(dq_df, "data_quality_summary", "gold_data_quality_summary", write_mode)
        save_gold_to_postgres(dq_df, "gold_dq_summary", write_mode)

        elapsed = (datetime.now() - start).total_seconds()
        log.info("stage_complete", stage="gold", duration_seconds=round(elapsed, 1))

        push_metrics(
            "gold_aggregation",
            gold_daily_summary_rows=daily_rows,
            gold_hourly_demand_rows=hourly_rows,
            gold_duration_seconds=elapsed,
        )

    finally:
        spark.stop()
        log.info("spark_stopped", stage="gold")


if __name__ == "__main__":
    run_gold_aggregation()
