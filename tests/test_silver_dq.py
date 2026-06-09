"""
test_silver_dq.py
──────────────────────────────────────────────────────────────────
PySpark unit tests for the Silver layer Data Quality rules.

Tests every rule in get_dq_conditions() independently so a
regression immediately shows which rule broke.
──────────────────────────────────────────────────────────────────
"""

from datetime import datetime

from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField,
    TimestampType, DoubleType, IntegerType,
)

from pipeline.silver_transformation import get_dq_conditions


# ── Schema & helpers ──────────────────────────────────────────────────────────

DQ_SCHEMA = StructType([
    StructField("tpep_pickup_datetime",  TimestampType(), nullable=True),
    StructField("tpep_dropoff_datetime", TimestampType(), nullable=True),
    StructField("trip_distance",         DoubleType(),    nullable=True),
    StructField("fare_amount",           DoubleType(),    nullable=True),
    StructField("total_amount",          DoubleType(),    nullable=True),
    StructField("passenger_count",       IntegerType(),   nullable=True),
])

_PICKUP  = datetime(2024, 1, 15, 10,  0, 0)
_DROPOFF = datetime(2024, 1, 15, 10, 30, 0)


def _make_row(**overrides) -> Row:
    """Returns a fully-valid Row, with any field overridden by kwargs."""
    defaults = dict(
        tpep_pickup_datetime  = _PICKUP,
        tpep_dropoff_datetime = _DROPOFF,
        trip_distance         = 5.0,
        fare_amount           = 20.0,
        total_amount          = 25.0,
        passenger_count       = 1,
    )
    defaults.update(overrides)
    return Row(**defaults)


def _valid_count(spark, *rows) -> int:
    """Creates a DataFrame from rows and returns the count passing DQ."""
    df = spark.createDataFrame(list(rows), schema=DQ_SCHEMA)
    return df.filter(get_dq_conditions(df)).count()


def _invalid_count(spark, *rows) -> int:
    """Returns the count failing DQ (the quarantine candidates)."""
    df = spark.createDataFrame(list(rows), schema=DQ_SCHEMA)
    return df.filter(~get_dq_conditions(df)).count()


# ── Happy path ────────────────────────────────────────────────────────────────

class TestValidRows:
    def test_single_clean_row_passes(self, spark):
        assert _valid_count(spark, _make_row()) == 1

    def test_multiple_clean_rows_all_pass(self, spark):
        rows = [
            _make_row(),
            _make_row(trip_distance=2.0, fare_amount=10.0, total_amount=11.0),
            _make_row(passenger_count=4, trip_distance=12.5),
        ]
        df = spark.createDataFrame(rows, schema=DQ_SCHEMA)
        condition = get_dq_conditions(df)
        assert df.filter(condition).count() == 3

    def test_zero_fare_is_valid(self, spark):
        """fare_amount=0 is a legitimate free trip."""
        assert _valid_count(spark, _make_row(fare_amount=0.0)) == 1

    def test_zero_total_amount_is_valid(self, spark):
        assert _valid_count(spark, _make_row(total_amount=0.0)) == 1


# ── Null datetime rules ───────────────────────────────────────────────────────

class TestNullDatetimeRules:
    def test_null_pickup_datetime_fails(self, spark):
        assert _valid_count(spark, _make_row(tpep_pickup_datetime=None)) == 0

    def test_null_dropoff_datetime_fails(self, spark):
        assert _valid_count(spark, _make_row(tpep_dropoff_datetime=None)) == 0

    def test_both_datetimes_null_fails(self, spark):
        assert _valid_count(
            spark,
            _make_row(tpep_pickup_datetime=None, tpep_dropoff_datetime=None),
        ) == 0


# ── Chronological ordering rule ───────────────────────────────────────────────

class TestChronologicalOrderRule:
    def test_dropoff_before_pickup_fails(self, spark):
        row = _make_row(
            tpep_pickup_datetime  = datetime(2024, 1, 15, 10, 30, 0),
            tpep_dropoff_datetime = datetime(2024, 1, 15, 10,  0, 0),  # reversed
        )
        assert _valid_count(spark, row) == 0

    def test_dropoff_equal_to_pickup_fails(self, spark):
        """Zero-duration trips are invalid (>  not >=)."""
        row = _make_row(
            tpep_pickup_datetime  = _PICKUP,
            tpep_dropoff_datetime = _PICKUP,   # same instant
        )
        assert _valid_count(spark, row) == 0

    def test_dropoff_one_second_after_pickup_passes(self, spark):
        row = _make_row(
            tpep_pickup_datetime  = datetime(2024, 1, 15, 10, 0,  0),
            tpep_dropoff_datetime = datetime(2024, 1, 15, 10, 0,  1),
        )
        assert _valid_count(spark, row) == 1


# ── Trip distance rule ────────────────────────────────────────────────────────

class TestTripDistanceRule:
    def test_zero_distance_fails(self, spark):
        assert _valid_count(spark, _make_row(trip_distance=0.0)) == 0

    def test_negative_distance_fails(self, spark):
        assert _valid_count(spark, _make_row(trip_distance=-1.0)) == 0

    def test_tiny_positive_distance_passes(self, spark):
        assert _valid_count(spark, _make_row(trip_distance=0.01)) == 1


# ── Fare amount rule ──────────────────────────────────────────────────────────

class TestFareAmountRule:
    def test_negative_fare_fails(self, spark):
        assert _valid_count(spark, _make_row(fare_amount=-0.01)) == 0

    def test_large_negative_fare_fails(self, spark):
        assert _valid_count(spark, _make_row(fare_amount=-100.0)) == 0


# ── Total amount rule ─────────────────────────────────────────────────────────

class TestTotalAmountRule:
    def test_negative_total_amount_fails(self, spark):
        assert _valid_count(spark, _make_row(total_amount=-0.01)) == 0


# ── Passenger count rule ──────────────────────────────────────────────────────

class TestPassengerCountRule:
    def test_zero_passengers_fails(self, spark):
        assert _valid_count(spark, _make_row(passenger_count=0)) == 0

    def test_negative_passengers_fails(self, spark):
        assert _valid_count(spark, _make_row(passenger_count=-1)) == 0

    def test_six_passengers_passes(self, spark):
        assert _valid_count(spark, _make_row(passenger_count=6)) == 1


# ── Mixed valid / invalid ─────────────────────────────────────────────────────

class TestMixedRows:
    def test_only_invalid_rows_produce_zero_valid(self, spark):
        rows = [
            _make_row(fare_amount=-1.0),
            _make_row(trip_distance=0.0),
            _make_row(passenger_count=0),
        ]
        df = spark.createDataFrame(rows, schema=DQ_SCHEMA)
        condition = get_dq_conditions(df)
        assert df.filter(condition).count() == 0
        assert df.filter(~condition).count() == 3

    def test_mixed_batch_splits_correctly(self, spark):
        rows = [
            _make_row(),                          # valid
            _make_row(fare_amount=-5.0),           # invalid — negative fare
            _make_row(trip_distance=0.0),          # invalid — zero distance
            _make_row(passenger_count=2),          # valid
        ]
        df = spark.createDataFrame(rows, schema=DQ_SCHEMA)
        condition = get_dq_conditions(df)
        assert df.filter(condition).count()  == 2
        assert df.filter(~condition).count() == 2
