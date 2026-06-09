"""
test_silver_transforms.py
──────────────────────────────────────────────────────────────────
PySpark unit tests for the Silver layer transformations:
  - add_derived_columns  (duration, temporal features, renames)
  - enrich_with_zones    (broadcast join against zone lookup)
──────────────────────────────────────────────────────────────────
"""

import pytest
from datetime import datetime

from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField,
    TimestampType, DoubleType, IntegerType, StringType,
)

from pipeline.silver_transformation import add_derived_columns, enrich_with_zones


# ── Schema for add_derived_columns input (Bronze column names) ────────────────

BRONZE_SCHEMA = StructType([
    StructField("tpep_pickup_datetime",  TimestampType(), nullable=True),
    StructField("tpep_dropoff_datetime", TimestampType(), nullable=True),
    StructField("VendorID",              IntegerType(),   nullable=True),
    StructField("RatecodeID",            IntegerType(),   nullable=True),
    StructField("PULocationID",          IntegerType(),   nullable=True),
    StructField("DOLocationID",          IntegerType(),   nullable=True),
    StructField("Airport_fee",           DoubleType(),    nullable=True),
])


def _bronze_row(**overrides) -> Row:
    defaults = dict(
        tpep_pickup_datetime  = datetime(2024, 1, 15, 10,  0, 0),
        tpep_dropoff_datetime = datetime(2024, 1, 15, 11,  0, 0),  # 60 min trip
        VendorID              = 1,
        RatecodeID            = 1,
        PULocationID          = 132,
        DOLocationID          = 161,
        Airport_fee           = 0.0,
    )
    defaults.update(overrides)
    return Row(**defaults)


# ── add_derived_columns: trip duration ────────────────────────────────────────

class TestTripDuration:
    def test_sixty_minute_trip(self, spark):
        df = spark.createDataFrame([_bronze_row()], schema=BRONZE_SCHEMA)
        result = add_derived_columns(df).first()
        assert result["trip_duration_minutes"] == pytest.approx(60.0)

    def test_ninety_minute_trip(self, spark):
        row = _bronze_row(
            tpep_pickup_datetime  = datetime(2024, 1, 15,  9,  0, 0),
            tpep_dropoff_datetime = datetime(2024, 1, 15, 10, 30, 0),
        )
        df = spark.createDataFrame([row], schema=BRONZE_SCHEMA)
        result = add_derived_columns(df).first()
        assert result["trip_duration_minutes"] == pytest.approx(90.0)

    def test_five_minute_trip(self, spark):
        row = _bronze_row(
            tpep_pickup_datetime  = datetime(2024, 1, 15, 10,  0, 0),
            tpep_dropoff_datetime = datetime(2024, 1, 15, 10,  5, 0),
        )
        df = spark.createDataFrame([row], schema=BRONZE_SCHEMA)
        result = add_derived_columns(df).first()
        assert result["trip_duration_minutes"] == pytest.approx(5.0)


# ── add_derived_columns: temporal features ────────────────────────────────────

class TestTemporalFeatures:
    def test_pickup_date_extracted(self, spark):
        """pickup_date should be the date part of the pickup timestamp."""
        df = spark.createDataFrame([_bronze_row()], schema=BRONZE_SCHEMA)
        result = add_derived_columns(df).first()
        assert str(result["pickup_date"]) == "2024-01-15"

    def test_pickup_hour_extracted(self, spark):
        """pickup_hour should be the integer hour of the pickup."""
        row = _bronze_row(
            tpep_pickup_datetime  = datetime(2024, 1, 15, 14, 30, 0),
            tpep_dropoff_datetime = datetime(2024, 1, 15, 15,  0, 0),
        )
        df = spark.createDataFrame([row], schema=BRONZE_SCHEMA)
        result = add_derived_columns(df).first()
        assert result["pickup_hour"] == 14

    def test_midnight_hour_is_zero(self, spark):
        row = _bronze_row(
            tpep_pickup_datetime  = datetime(2024, 1, 15,  0,  5, 0),
            tpep_dropoff_datetime = datetime(2024, 1, 15,  0, 20, 0),
        )
        df = spark.createDataFrame([row], schema=BRONZE_SCHEMA)
        result = add_derived_columns(df).first()
        assert result["pickup_hour"] == 0

    def test_day_of_week_monday(self, spark):
        """2024-01-15 is a Monday."""
        df = spark.createDataFrame([_bronze_row()], schema=BRONZE_SCHEMA)
        result = add_derived_columns(df).first()
        assert result["day_of_week"] == "Monday"

    def test_day_of_week_num_monday_is_2(self, spark):
        """Spark dayofweek: 1=Sunday, 2=Monday … 7=Saturday."""
        df = spark.createDataFrame([_bronze_row()], schema=BRONZE_SCHEMA)
        result = add_derived_columns(df).first()
        assert result["day_of_week_num"] == 2

    def test_day_of_week_sunday(self, spark):
        """2024-01-14 is a Sunday."""
        row = _bronze_row(
            tpep_pickup_datetime  = datetime(2024, 1, 14, 10, 0, 0),
            tpep_dropoff_datetime = datetime(2024, 1, 14, 10, 30, 0),
        )
        df = spark.createDataFrame([row], schema=BRONZE_SCHEMA)
        result = add_derived_columns(df).first()
        assert result["day_of_week"] == "Sunday"
        assert result["day_of_week_num"] == 1


# ── add_derived_columns: column renaming ─────────────────────────────────────

class TestColumnRenaming:
    def test_old_names_removed(self, spark):
        df = spark.createDataFrame([_bronze_row()], schema=BRONZE_SCHEMA)
        cols = set(add_derived_columns(df).columns)
        assert "VendorID"             not in cols
        assert "tpep_pickup_datetime"  not in cols
        assert "tpep_dropoff_datetime" not in cols
        assert "RatecodeID"            not in cols
        assert "PULocationID"          not in cols
        assert "DOLocationID"          not in cols
        assert "Airport_fee"           not in cols

    def test_new_names_present(self, spark):
        df = spark.createDataFrame([_bronze_row()], schema=BRONZE_SCHEMA)
        cols = set(add_derived_columns(df).columns)
        assert "vendor_id"          in cols
        assert "pickup_datetime"    in cols
        assert "dropoff_datetime"   in cols
        assert "rate_code_id"       in cols
        assert "pickup_location_id" in cols
        assert "dropoff_location_id" in cols
        assert "airport_fee"        in cols

    def test_derived_columns_present(self, spark):
        df = spark.createDataFrame([_bronze_row()], schema=BRONZE_SCHEMA)
        cols = set(add_derived_columns(df).columns)
        assert "trip_duration_minutes" in cols
        assert "pickup_date"           in cols
        assert "pickup_hour"           in cols
        assert "day_of_week"           in cols
        assert "day_of_week_num"       in cols


# ── enrich_with_zones ─────────────────────────────────────────────────────────

TRIPS_SCHEMA = StructType([
    StructField("pickup_location_id",  IntegerType(), nullable=True),
    StructField("dropoff_location_id", IntegerType(), nullable=True),
])

ZONES_SCHEMA = StructType([
    StructField("LocationID",    IntegerType(), nullable=True),
    StructField("Zone",          StringType(),  nullable=True),
    StructField("Borough",       StringType(),  nullable=True),
    StructField("service_zone",  StringType(),  nullable=True),
])


class TestZoneEnrichment:
    def _zones_df(self, spark):
        rows = [
            Row(LocationID=132, Zone="JFK Airport",     Borough="Queens",    service_zone="Airports"),
            Row(LocationID=161, Zone="Midtown Center",  Borough="Manhattan", service_zone="Yellow Zone"),
            Row(LocationID=1,   Zone="Newark Airport",  Borough="EWR",       service_zone="EWR"),
        ]
        return spark.createDataFrame(rows, schema=ZONES_SCHEMA)

    def test_zone_names_joined_correctly(self, spark):
        trips = spark.createDataFrame(
            [Row(pickup_location_id=132, dropoff_location_id=161)],
            schema=TRIPS_SCHEMA,
        )
        result = enrich_with_zones(trips, self._zones_df(spark)).first()
        assert result["pickup_zone"]    == "JFK Airport"
        assert result["pickup_borough"] == "Queens"
        assert result["dropoff_zone"]   == "Midtown Center"
        assert result["dropoff_borough"] == "Manhattan"

    def test_unknown_location_id_produces_null_zone(self, spark):
        """An unknown location ID (e.g. 999) should give null zone (left join)."""
        trips = spark.createDataFrame(
            [Row(pickup_location_id=999, dropoff_location_id=161)],
            schema=TRIPS_SCHEMA,
        )
        result = enrich_with_zones(trips, self._zones_df(spark)).first()
        assert result["pickup_zone"] is None
        assert result["dropoff_zone"] == "Midtown Center"

    def test_row_count_preserved_after_join(self, spark):
        """Left join must never drop rows."""
        rows = [
            Row(pickup_location_id=132, dropoff_location_id=161),
            Row(pickup_location_id=999, dropoff_location_id=999),  # both unknown
            Row(pickup_location_id=1,   dropoff_location_id=132),
        ]
        trips = spark.createDataFrame(rows, schema=TRIPS_SCHEMA)
        result = enrich_with_zones(trips, self._zones_df(spark))
        assert result.count() == 3
