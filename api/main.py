from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
from psycopg2.extras import RealDictCursor
import os

app = FastAPI(title="NYC Taxi Lakehouse API")

# Allow React app on localhost
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For dev, allow all
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Connect to the local Postgres database where Gold data is loaded
PG_HOST = os.environ.get("PG_HOST", "127.0.0.1")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DB = os.environ.get("PG_DB", "lakehouse")
PG_USER = os.environ.get("PG_USER", "lakehouse")
PG_PASS = os.environ.get("PG_PASS", "lakehouse")

def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname=PG_DB,
            user=PG_USER, password=PG_PASS
        )
        return conn
    except Exception as e:
        print(f"Error connecting to Postgres: {e}")
        return None

@app.get("/api/overview")
def get_overview():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Aggregate overall stats
            cur.execute("""
                SELECT
                    COALESCE(SUM(r.total_trips), 0) as total_trips,
                    COALESCE(SUM(r.total_revenue), 0) as total_revenue,
                    COALESCE(AVG(r.avg_fare), 0) as average_fare,
                    COALESCE((SELECT AVG(avg_duration_minutes) FROM gold_hourly_performance), 0) as average_duration_minutes
                FROM gold_daily_revenue r
            """)
            summary = cur.fetchone()

            # Daily trend for a mini chart
            cur.execute("""
                SELECT pickup_date, total_revenue, total_trips
                FROM gold_daily_revenue
                ORDER BY pickup_date ASC
            """)
            daily_trend = cur.fetchall()

            return {
                "summary": summary,
                "daily_trend": daily_trend
            }
    finally:
        conn.close()

@app.get("/api/demand")
def get_demand_analysis():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT pickup_hour, total_trips, avg_duration_minutes, avg_speed_mph
                FROM gold_hourly_performance
                ORDER BY pickup_hour ASC
            """)
            return cur.fetchall()
    finally:
        conn.close()

@app.get("/api/locations")
def get_location_analysis():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Top pickup zones by revenue
            cur.execute("""
                SELECT pickup_zone, SUM(total_trips) as trips
                FROM gold_route_summary
                WHERE pickup_zone != 'Unknown' AND pickup_zone IS NOT NULL
                GROUP BY pickup_zone
                ORDER BY trips DESC
                LIMIT 10
            """)
            top_pickups = cur.fetchall()

            # Top routes
            cur.execute("""
                SELECT pickup_zone, dropoff_zone, total_trips
                FROM gold_route_summary
                WHERE pickup_zone != 'Unknown' AND dropoff_zone != 'Unknown'
                ORDER BY total_trips DESC
                LIMIT 10
            """)
            top_routes = cur.fetchall()

            return {
                "top_pickups": top_pickups,
                "top_routes": top_routes
            }
    finally:
        conn.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
