"""
config.py
─────────────────────────────────────────────────────────────────
Central configuration for the NYC Taxi Mobility Data Lakehouse.

All dataset URLs, local storage paths, and pipeline settings
are defined here so every script imports from a single source
of truth — no hardcoded strings scattered across the codebase.
─────────────────────────────────────────────────────────────────
"""

from pathlib import Path

# ── Root project directory (the folder that contains this file) ──────────────
ROOT_DIR = Path(__file__).resolve().parent.parent

# ── Local storage paths ───────────────────────────────────────────────────────
DATA_DIR       = ROOT_DIR / "data"
RAW_DIR        = DATA_DIR / "raw"
BRONZE_DIR     = DATA_DIR / "bronze"
SILVER_DIR     = DATA_DIR / "silver"
GOLD_DIR       = DATA_DIR / "gold"
QUARANTINE_DIR = DATA_DIR / "quarantine"
LOGS_DIR       = ROOT_DIR / "logs"

# ── Raw sub-paths ─────────────────────────────────────────────────────────────
RAW_YELLOW_TAXI_DIR = RAW_DIR / "yellow_taxi"
RAW_TAXI_ZONES_DIR  = RAW_DIR / "taxi_zones"

# ── Bronze sub-paths ──────────────────────────────────────────────────────────
BRONZE_YELLOW_TAXI_PATH = BRONZE_DIR / "yellow_taxi"
BRONZE_TAXI_ZONES_PATH  = BRONZE_DIR / "taxi_zones"

# ── Silver sub-paths ──────────────────────────────────────────────────────────
SILVER_YELLOW_TAXI_PATH = SILVER_DIR / "yellow_taxi"

# ── Gold sub-paths ────────────────────────────────────────────────────────────
GOLD_DAILY_SUMMARY_PATH  = GOLD_DIR / "daily_summary"
GOLD_HOURLY_DEMAND_PATH  = GOLD_DIR / "hourly_demand"
GOLD_ZONE_REVENUE_PATH   = GOLD_DIR / "zone_revenue"
GOLD_ROUTE_SUMMARY_PATH  = GOLD_DIR / "route_summary"
GOLD_DQ_METRICS_PATH     = GOLD_DIR / "dq_metrics"

# ── Quarantine sub-paths ──────────────────────────────────────────────────────
QUARANTINE_YELLOW_TAXI_PATH = QUARANTINE_DIR / "yellow_taxi"

# ─────────────────────────────────────────────────────────────────────────────
# NYC TLC Dataset Download URLs
# Source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
# CDN base: https://d37ci6vzurychx.cloudfront.net
#
# We use January–March 2024 (Q1 2024) as our starting dataset.
# 2025 data is excluded because the TLC publishes data with a ~2-month lag
# and not all 2025 months may be available yet.
# ─────────────────────────────────────────────────────────────────────────────

NYC_TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net"

# Yellow Taxi Trip Records — Q1 2024 (Parquet format)
YELLOW_TAXI_URLS = {
    "yellow_tripdata_2024-01.parquet": f"{NYC_TLC_BASE_URL}/trip-data/yellow_tripdata_2024-01.parquet",
    "yellow_tripdata_2024-02.parquet": f"{NYC_TLC_BASE_URL}/trip-data/yellow_tripdata_2024-02.parquet",
    "yellow_tripdata_2024-03.parquet": f"{NYC_TLC_BASE_URL}/trip-data/yellow_tripdata_2024-03.parquet",
}

# Taxi Zone Lookup Table — maps LocationID to Zone name and Borough
TAXI_ZONE_URLS = {
    "taxi_zone_lookup.csv": f"{NYC_TLC_BASE_URL}/misc/taxi_zone_lookup.csv",
}

# Data Dictionary (reference only — not downloaded into the pipeline)
DATA_DICTIONARY_URL = f"{NYC_TLC_BASE_URL}/document/data-dictionary_trip_records_yellow.pdf"

# ── Pipeline settings ─────────────────────────────────────────────────────────
PIPELINE_SETTINGS = {
    # Dataset being processed
    "dataset_name": "yellow_taxi",
    "dataset_year": 2024,
    "dataset_months": [1, 2, 3],

    # Delta Lake write mode
    # "overwrite" → replaces existing data on each run (safe for development)
    # "append"    → adds new data (used in production incremental loads)
    "delta_write_mode": "overwrite",

    # Download settings
    "download_chunk_size_bytes": 8192,   # 8 KB chunks — keeps memory low for large files
    "download_timeout_seconds": 300,     # 5-minute timeout per file
}
