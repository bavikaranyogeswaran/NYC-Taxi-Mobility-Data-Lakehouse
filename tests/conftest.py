"""
conftest.py
──────────────────────────────────────────────────────────────────
Shared pytest fixtures for the NYC Taxi Mobility Data Lakehouse.

A single, session-scoped SparkSession is created once and reused
across all PySpark test modules, keeping total startup time low.
──────────────────────────────────────────────────────────────────
"""

import pytest


@pytest.fixture(scope="session")
def spark():
    """
    Session-scoped SparkSession for unit tests.

    Uses local[1] (single thread) to keep tests fast and deterministic.
    Delta Lake JARs are NOT loaded here — we test pure PySpark logic only.

    PySpark is imported lazily inside the fixture so that pure-Python tests
    (test_config, test_download) can still run in environments where PySpark
    is not installed.
    """
    pytest.importorskip("pyspark", reason="pyspark not installed — skipping Spark tests")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("nyc_taxi_lakehouse_tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
