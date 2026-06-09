"""
test_gold_aggregations.py
──────────────────────────────────────────────────────────────────
PySpark unit tests for the Gold aggregation logic in
src/pipeline/gold_aggregation.py.

Each mart is tested against a small in-memory Silver DataFrame so
the tests run fast and require no Delta Lake or Postgres.
──────────────────────────────────────────────────────────────────
"""

import pytest
from datetime import date

from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField,
    DateType, StringType, IntegerType, DoubleType,
)

from pipeline.gold_aggregation import (
    aggregate_daily_revenue_by_zone,
    aggregate_hourly_performance,
    aggregate_route_summary,
)


# ── Shared Silver schema (only the columns each mart actually uses) ────────────

SILVER_SCHEMA = StructType([
    StructField("pickup_date",           DateType(),    nullable=True),
    StructField("pickup_borough",        StringType(),  nullable=True),
    StructField("pickup_zone",           StringType(),  nullable=True),
    StructField("dropoff_zone",          StringType(),  nullable=True),
    StructField("pickup_hour",           IntegerType(), nullable=True),
    StructField("total_amount",          DoubleType(),  nullable=True),
    StructField("fare_amount",           DoubleType(),  nullable=True),
    StructField("passenger_count",       IntegerType(), nullable=True),
    StructField("trip_distance",         DoubleType(),  nullable=True),
    StructField("trip_duration_minutes", DoubleType(),  nullable=True),
])


def _row(**overrides) -> Row:
    defaults = dict(
        pickup_date           = date(2024, 1, 15),
        pickup_borough        = "Manhattan",
        pickup_zone           = "Midtown Center",
        dropoff_zone          = "JFK Airport",
        pickup_hour           = 10,
        total_amount          = 25.0,
        fare_amount           = 20.0,
        passenger_count       = 1,
        trip_distance         = 5.0,
        trip_duration_minutes = 30.0,
    )
    defaults.update(overrides)
    return Row(**defaults)


# ── aggregate_daily_revenue_by_zone ───────────────────────────────────────────

class TestDailyRevenueByZone:
    def test_single_trip_aggregates_correctly(self, spark):
        df = spark.createDataFrame([_row()], schema=SILVER_SCHEMA)
        result = aggregate_daily_revenue_by_zone(df).first()
        assert result["total_trips"]    == 1
        assert result["total_revenue"]  == pytest.approx(25.0)
        assert result["total_passengers"] == 1
        assert result["avg_fare"]       == pytest.approx(20.0)

    def test_two_trips_same_zone_and_date_summed(self, spark):
        rows = [
            _row(total_amount=25.0, fare_amount=20.0, passenger_count=1),
            _row(total_amount=30.0, fare_amount=24.0, passenger_count=2),
        ]
        df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)
        result = aggregate_daily_revenue_by_zone(df).first()
        assert result["total_trips"]      == 2
        assert result["total_revenue"]    == pytest.approx(55.0)
        assert result["total_passengers"] == 3
        assert result["avg_fare"]         == pytest.approx(22.0)

    def test_different_zones_produce_separate_rows(self, spark):
        rows = [
            _row(pickup_zone="Midtown Center"),
            _row(pickup_zone="JFK Airport", pickup_borough="Queens"),
        ]
        df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)
        result = aggregate_daily_revenue_by_zone(df)
        assert result.count() == 2

    def test_null_pickup_zone_excluded(self, spark):
        """Rows with unknown pickup zones (null) should be filtered out."""
        rows = [
            _row(pickup_zone="Midtown Center"),
            _row(pickup_zone=None),             # should be excluded
        ]
        df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)
        result = aggregate_daily_revenue_by_zone(df)
        zones = [r["pickup_zone"] for r in result.collect()]
        assert None not in zones
        assert len(zones) == 1

    def test_different_dates_produce_separate_rows(self, spark):
        rows = [
            _row(pickup_date=date(2024, 1, 15)),
            _row(pickup_date=date(2024, 1, 16)),
        ]
        df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)
        result = aggregate_daily_revenue_by_zone(df)
        assert result.count() == 2


# ── aggregate_hourly_performance ──────────────────────────────────────────────

class TestHourlyPerformance:
    def test_average_speed_calculation(self, spark):
        """
        5 miles in 30 minutes = 10 mph.
        avg_speed_mph = avg_distance / (avg_duration / 60)
                      = 5 / (30 / 60)
                      = 5 / 0.5 = 10.0
        """
        row = _row(pickup_hour=10, trip_distance=5.0, trip_duration_minutes=30.0)
        df = spark.createDataFrame([row], schema=SILVER_SCHEMA)
        result = aggregate_hourly_performance(df).first()
        assert result["avg_speed_mph"] == pytest.approx(10.0, rel=1e-3)

    def test_zero_duration_produces_zero_speed(self, spark):
        """Guard against division by zero when avg_duration_minutes == 0."""
        row = _row(pickup_hour=10, trip_distance=5.0, trip_duration_minutes=0.0)
        df = spark.createDataFrame([row], schema=SILVER_SCHEMA)
        result = aggregate_hourly_performance(df).first()
        assert result["avg_speed_mph"] == pytest.approx(0.0)

    def test_24_hours_produce_24_rows(self, spark):
        """One trip per hour should produce 24 rows in the mart."""
        rows = [_row(pickup_hour=h) for h in range(24)]
        df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)
        result = aggregate_hourly_performance(df)
        assert result.count() == 24

    def test_hours_ordered_ascending(self, spark):
        """Result should be sorted 0 → 23."""
        rows = [_row(pickup_hour=h) for h in [23, 5, 12, 0]]
        df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)
        result = aggregate_hourly_performance(df)
        hours = [r["pickup_hour"] for r in result.collect()]
        assert hours == sorted(hours)

    def test_multiple_trips_same_hour_averaged(self, spark):
        """Two trips in hour 10 should produce a single row with avg metrics."""
        rows = [
            _row(pickup_hour=10, trip_distance=4.0, trip_duration_minutes=20.0),
            _row(pickup_hour=10, trip_distance=6.0, trip_duration_minutes=40.0),
        ]
        df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)
        result = aggregate_hourly_performance(df).first()
        assert result["total_trips"]          == 2
        assert result["avg_distance_miles"]   == pytest.approx(5.0)
        assert result["avg_duration_minutes"] == pytest.approx(30.0)


# ── aggregate_route_summary ───────────────────────────────────────────────────

class TestRouteSummary:
    def test_single_route_aggregates_correctly(self, spark):
        df = spark.createDataFrame([_row()], schema=SILVER_SCHEMA)
        result = aggregate_route_summary(df).first()
        assert result["total_trips"]  == 1
        assert result["avg_distance"] == pytest.approx(5.0)
        assert result["avg_fare"]     == pytest.approx(20.0)

    def test_two_trips_same_route_summed(self, spark):
        rows = [
            _row(pickup_zone="Midtown Center", dropoff_zone="JFK Airport",
                 trip_distance=10.0, fare_amount=30.0),
            _row(pickup_zone="Midtown Center", dropoff_zone="JFK Airport",
                 trip_distance=20.0, fare_amount=50.0),
        ]
        df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)
        result = aggregate_route_summary(df).first()
        assert result["total_trips"]  == 2
        assert result["avg_distance"] == pytest.approx(15.0)
        assert result["avg_fare"]     == pytest.approx(40.0)

    def test_different_routes_separate_rows(self, spark):
        rows = [
            _row(pickup_zone="Midtown Center", dropoff_zone="JFK Airport"),
            _row(pickup_zone="Upper East Side", dropoff_zone="LaGuardia Airport"),
        ]
        df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)
        result = aggregate_route_summary(df)
        assert result.count() == 2

    def test_null_pickup_zone_excluded(self, spark):
        rows = [
            _row(pickup_zone="Midtown Center", dropoff_zone="JFK Airport"),
            _row(pickup_zone=None, dropoff_zone="JFK Airport"),   # excluded
        ]
        df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)
        result = aggregate_route_summary(df)
        pickup_zones = [r["pickup_zone"] for r in result.collect()]
        assert None not in pickup_zones

    def test_null_dropoff_zone_excluded(self, spark):
        rows = [
            _row(pickup_zone="Midtown Center", dropoff_zone="JFK Airport"),
            _row(pickup_zone="Midtown Center", dropoff_zone=None),  # excluded
        ]
        df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)
        result = aggregate_route_summary(df)
        dropoff_zones = [r["dropoff_zone"] for r in result.collect()]
        assert None not in dropoff_zones

    def test_ordered_by_total_trips_descending(self, spark):
        """Most popular route should appear first."""
        rows = (
            [_row(pickup_zone="Midtown Center", dropoff_zone="JFK Airport")] * 5
            + [_row(pickup_zone="Upper East Side", dropoff_zone="LaGuardia Airport")] * 2
        )
        df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)
        result = aggregate_route_summary(df)
        trip_counts = [r["total_trips"] for r in result.collect()]
        assert trip_counts == sorted(trip_counts, reverse=True)
