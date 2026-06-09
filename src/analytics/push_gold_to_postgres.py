"""
push_gold_to_postgres.py
────────────────────────────────────────────────────────────────────────
One-shot script: reads Gold Parquet files locally with DuckDB, then
writes them to the lakehouse Postgres DB via psycopg2.

Run this ONCE after a successful pipeline run to populate Postgres
so the FastAPI / React dashboard has data to serve.

Usage:
    .venv\\Scripts\\python src\\analytics\\push_gold_to_postgres.py
────────────────────────────────────────────────────────────────────────
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

import duckdb
import psycopg2
import psycopg2.extras
import datetime

# ── Postgres connection ───────────────────────────────────────────────────────
PG_HOST = "127.0.0.1"
PG_PORT = 5432
PG_DB   = "lakehouse"
PG_USER = "lakehouse"
PG_PASS = "lakehouse"

# ── Gold parquet folders ──────────────────────────────────────────────────────
GOLD_BASE = PROJECT_ROOT / "data" / "gold"

TABLES = {
    "gold_daily_revenue":    GOLD_BASE / "daily_revenue_by_zone",
    "gold_hourly_performance": GOLD_BASE / "hourly_performance",
    "gold_route_summary":    GOLD_BASE / "route_summary",
    "gold_dq_summary":       GOLD_BASE / "data_quality_summary",
}


def push_table(con_duckdb, con_pg, table_name: str, parquet_dir: Path):
    glob_path = str(parquet_dir / "*.parquet").replace("\\", "/")
    print(f"  Reading {table_name} from {glob_path} ...")

    result = con_duckdb.execute(f"SELECT * FROM read_parquet('{glob_path}')")
    cols = [d[0] for d in result.description]
    col_types = [d[1] for d in result.description]  # duckdb type codes
    rows = result.fetchall()

    if not rows:
        print(f"  WARNING: no data in {table_name}, skipping.")
        return

    # Build Postgres CREATE TABLE DDL based on DuckDB column types
    def pg_type(duckdb_type_code):
        t = str(duckdb_type_code).upper()
        if "INT" in t:
            return "BIGINT"
        if "FLOAT" in t or "DOUBLE" in t or "DECIMAL" in t or "HUGEINT" in t:
            return "DOUBLE PRECISION"
        if "TIMESTAMP" in t:
            return "TIMESTAMP"
        if "DATE" in t:
            return "DATE"
        if "BOOL" in t:
            return "BOOLEAN"
        return "TEXT"

    col_defs = ", ".join(f'"{c}" {pg_type(ct)}' for c, ct in zip(cols, col_types))
    create_sql = f'DROP TABLE IF EXISTS {table_name}; CREATE TABLE {table_name} ({col_defs});'

    with con_pg.cursor() as cur:
        cur.execute(create_sql)
        con_pg.commit()

        # Sanitise values: convert non-standard types to Python natives
        def sanitise(v):
            if v is None:
                return None
            if isinstance(v, (datetime.date, datetime.datetime)):
                return v
            try:
                f = float(v)
                return f
            except (TypeError, ValueError):
                pass
            return str(v)

        clean_rows = [tuple(sanitise(v) for v in row) for row in rows]

        placeholders = ",".join(["%s"] * len(cols))
        col_names = ",".join([f'"{c}"' for c in cols])
        insert_sql = f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})"
        psycopg2.extras.execute_batch(cur, insert_sql, clean_rows, page_size=500)
        con_pg.commit()

    print(f"  OK {table_name}: {len(rows):,} rows pushed to Postgres")


def main():
    print("=" * 60)
    print("NYC Taxi Lakehouse — Push Gold to Postgres")
    print("=" * 60)

    con_pg = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASS
    )
    con_duckdb = duckdb.connect()

    for table_name, parquet_dir in TABLES.items():
        if not parquet_dir.exists():
            print(f"  SKIP {table_name}: folder not found at {parquet_dir}")
            continue
        try:
            push_table(con_duckdb, con_pg, table_name, parquet_dir)
        except Exception as e:
            print(f"  ERROR {table_name}: {e}")

    con_pg.close()
    con_duckdb.close()
    print("\nDone -- all Gold tables are now in Postgres!")
    print(f"  Lakehouse DB: postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}/{PG_DB}")


if __name__ == "__main__":
    main()
