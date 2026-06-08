"""
validate_gold.py
─────────────────────────────────────────────────────────────────
Validation Layer — Phase 5 (Tasks 5.1 – 5.3)

Uses DuckDB to run ultra-fast OLAP queries directly against the
Gold Delta tables without needing a Spark cluster or JVM.
This script proves that the data is queryable and prints out
summary metrics for visual inspection before dashboarding.

Usage:
    python src/analytics/validate_gold.py
─────────────────────────────────────────────────────────────────
"""

import sys
from pathlib import Path

# Force UTF-8 encoding for Windows console (fixes DuckDB .show() border characters)
if hasattr(sys.stdout, 'encoding') and sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')

# ── Resolve project root ──────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from config import ROOT_DIR
import duckdb

# Build a local Path to the Gold layer (DuckDB reads from local filesystem)
_LOCAL_GOLD_DIR = ROOT_DIR / "data" / "gold"

def run_validation():
    print("=" * 70)
    print("NYC Taxi Lakehouse -- DuckDB Gold Validation")
    print("=" * 70)
    
    # 1. Connect to an in-memory DuckDB instance
    con = duckdb.connect(database=':memory:')
    
    # We use DuckDB's native read_parquet to avoid Windows delta-kernel IO bugs.
    # On Windows, delta_scan can sometimes fail to parse the _delta_log directory.
    
    # Paths to the Gold Delta tables (pointing to the Parquet data files)
    daily_revenue_path = (_LOCAL_GOLD_DIR / "daily_revenue_by_zone" / "*.parquet").resolve().as_posix()
    hourly_perf_path = (_LOCAL_GOLD_DIR / "hourly_performance" / "*.parquet").resolve().as_posix()
    route_summary_path = (_LOCAL_GOLD_DIR / "route_summary" / "*.parquet").resolve().as_posix()
    dq_summary_path = (_LOCAL_GOLD_DIR / "data_quality_summary" / "*.parquet").resolve().as_posix()
    
    print("\n[2/3] Querying Gold Data Marts directly from disk...")
    
    # ─────────────────────────────────────────────────────────────────────────
    # Query 1: Top 5 Highest Revenue Days & Zones
    # ─────────────────────────────────────────────────────────────────────────
    print("\n--- Top 5 Highest Revenue Pickup Zones by Day ---")
    query_revenue = f"""
        SELECT 
            pickup_date, 
            pickup_borough, 
            pickup_zone, 
            total_trips, 
            ROUND(total_revenue, 2) AS total_revenue_usd
        FROM read_parquet('{daily_revenue_path}')
        ORDER BY total_revenue DESC
        LIMIT 5;
    """
    con.sql(query_revenue).show()
    
    # ─────────────────────────────────────────────────────────────────────────
    # Query 2: Peak Hours for Traffic (Slowest average speeds)
    # ─────────────────────────────────────────────────────────────────────────
    print("\n--- Top 5 Slowest Hours (Traffic Analysis) ---")
    query_hourly = f"""
        SELECT 
            pickup_hour, 
            total_trips, 
            ROUND(avg_duration_minutes, 1) AS avg_duration_min, 
            ROUND(avg_speed_mph, 1) AS avg_speed_mph
        FROM read_parquet('{hourly_perf_path}')
        ORDER BY avg_speed_mph ASC
        LIMIT 5;
    """
    con.sql(query_hourly).show()

    # ─────────────────────────────────────────────────────────────────────────
    # Query 3: Most Popular Routes
    # ─────────────────────────────────────────────────────────────────────────
    print("\n--- Top 5 Most Popular Taxi Routes ---")
    query_routes = f"""
        SELECT 
            pickup_zone, 
            dropoff_zone, 
            total_trips, 
            ROUND(avg_distance, 1) AS avg_dist_miles,
            ROUND(avg_fare, 2) AS avg_fare_usd
        FROM read_parquet('{route_summary_path}')
        ORDER BY total_trips DESC
        LIMIT 5;
    """
    con.sql(query_routes).show()
    
    # ─────────────────────────────────────────────────────────────────────────
    # Query 4: Overall Data Quality Health
    # ─────────────────────────────────────────────────────────────────────────
    print("\n--- High-Level Data Quality Summary ---")
    query_dq = f"""
        SELECT 
            SUM(valid_records) AS total_valid,
            SUM(invalid_records) AS total_invalid,
            ROUND((SUM(invalid_records)*100.0) / (SUM(valid_records) + SUM(invalid_records)), 2) AS failure_rate_pct
        FROM read_parquet('{dq_summary_path}');
    """
    con.sql(query_dq).show()

    print("\n[3/3] Validation complete. Data is ready for Superset!")
    print("=" * 70)

if __name__ == "__main__":
    run_validation()
