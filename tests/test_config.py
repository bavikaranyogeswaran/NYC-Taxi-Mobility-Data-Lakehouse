"""
test_config.py
──────────────────────────────────────────────────────────────────
Pure-Python unit tests for src/config.py.

Validates that all paths, URLs, and pipeline settings are correctly
defined without starting Spark.
──────────────────────────────────────────────────────────────────
"""

from config import (
    ROOT_DIR,
    BRONZE_DIR,
    SILVER_DIR,
    GOLD_DIR,
    QUARANTINE_DIR,
    RAW_YELLOW_TAXI_DIR,
    RAW_TAXI_ZONES_DIR,
    BRONZE_YELLOW_TAXI_PATH,
    BRONZE_TAXI_ZONES_PATH,
    SILVER_YELLOW_TAXI_PATH,
    GOLD_DAILY_SUMMARY_PATH,
    GOLD_HOURLY_DEMAND_PATH,
    GOLD_ROUTE_SUMMARY_PATH,
    GOLD_DQ_METRICS_PATH,
    QUARANTINE_YELLOW_TAXI_PATH,
    NYC_TLC_BASE_URL,
    YELLOW_TAXI_URLS,
    TAXI_ZONE_URLS,
    PIPELINE_SETTINGS,
)


# ── Root directory ────────────────────────────────────────────────────────────

def test_root_dir_is_valid_path():
    assert ROOT_DIR.exists(), f"ROOT_DIR not found: {ROOT_DIR}"


def test_raw_dirs_are_under_root():
    assert str(RAW_YELLOW_TAXI_DIR).startswith(str(ROOT_DIR))
    assert str(RAW_TAXI_ZONES_DIR).startswith(str(ROOT_DIR))


# ── Lakehouse layer paths contain correct tier names ──────────────────────────

def test_bronze_dir_contains_bronze():
    assert "bronze" in BRONZE_DIR


def test_silver_dir_contains_silver():
    assert "silver" in SILVER_DIR


def test_gold_dir_contains_gold():
    assert "gold" in GOLD_DIR


def test_quarantine_dir_contains_quarantine():
    assert "quarantine" in QUARANTINE_DIR


# ── Sub-paths are under their tier roots ──────────────────────────────────────

def test_bronze_subpaths_under_bronze():
    assert BRONZE_YELLOW_TAXI_PATH.startswith(BRONZE_DIR)
    assert BRONZE_TAXI_ZONES_PATH.startswith(BRONZE_DIR)


def test_silver_subpath_under_silver():
    assert SILVER_YELLOW_TAXI_PATH.startswith(SILVER_DIR)


def test_gold_subpaths_under_gold():
    assert GOLD_DAILY_SUMMARY_PATH.startswith(GOLD_DIR)
    assert GOLD_HOURLY_DEMAND_PATH.startswith(GOLD_DIR)
    assert GOLD_ROUTE_SUMMARY_PATH.startswith(GOLD_DIR)
    assert GOLD_DQ_METRICS_PATH.startswith(GOLD_DIR)


def test_quarantine_subpath_under_quarantine():
    assert QUARANTINE_YELLOW_TAXI_PATH.startswith(QUARANTINE_DIR)


# ── Download URLs ─────────────────────────────────────────────────────────────

def test_yellow_taxi_url_count():
    """Must have exactly 3 files (Jan, Feb, Mar 2024)."""
    assert len(YELLOW_TAXI_URLS) == 3


def test_yellow_taxi_urls_use_cdn():
    for filename, url in YELLOW_TAXI_URLS.items():
        assert url.startswith(NYC_TLC_BASE_URL), (
            f"{filename} URL does not start with CDN base: {url}"
        )


def test_yellow_taxi_filenames_are_parquet():
    for filename in YELLOW_TAXI_URLS:
        assert filename.endswith(".parquet"), f"Expected .parquet: {filename}"


def test_yellow_taxi_urls_cover_q1_2024():
    filenames = list(YELLOW_TAXI_URLS.keys())
    assert any("2024-01" in f for f in filenames), "January 2024 file missing"
    assert any("2024-02" in f for f in filenames), "February 2024 file missing"
    assert any("2024-03" in f for f in filenames), "March 2024 file missing"


def test_taxi_zone_url_is_csv():
    for filename, url in TAXI_ZONE_URLS.items():
        assert filename.endswith(".csv"), f"Expected .csv: {filename}"
        assert url.startswith(NYC_TLC_BASE_URL)


# ── Pipeline settings ─────────────────────────────────────────────────────────

def test_pipeline_settings_has_required_keys():
    required = {
        "dataset_name",
        "dataset_year",
        "dataset_months",
        "delta_write_mode",
        "download_chunk_size_bytes",
        "download_timeout_seconds",
    }
    missing = required - set(PIPELINE_SETTINGS.keys())
    assert not missing, f"Missing keys in PIPELINE_SETTINGS: {missing}"


def test_pipeline_settings_dataset_name():
    assert PIPELINE_SETTINGS["dataset_name"] == "yellow_taxi"


def test_pipeline_settings_dataset_year():
    assert PIPELINE_SETTINGS["dataset_year"] == 2024


def test_pipeline_settings_dataset_months():
    assert PIPELINE_SETTINGS["dataset_months"] == [1, 2, 3]


def test_pipeline_settings_write_mode_valid():
    assert PIPELINE_SETTINGS["delta_write_mode"] in ("overwrite", "append")


def test_pipeline_settings_chunk_size_positive():
    assert PIPELINE_SETTINGS["download_chunk_size_bytes"] > 0


def test_pipeline_settings_timeout_positive():
    assert PIPELINE_SETTINGS["download_timeout_seconds"] > 0
