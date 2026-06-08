from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Default arguments for the DAG
default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id="nyc_taxi_lakehouse_pipeline",
    default_args=default_args,
    description="End-to-End PySpark Lakehouse Pipeline for NYC Taxi Data",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["lakehouse", "pyspark", "minio"],
) as dag:

    # ─────────────────────────────────────────────────────────────────
    # Task 1: Ingestion
    # Downloads raw Parquet files from NYC TLC to local storage
    # ─────────────────────────────────────────────────────────────────
    download_raw_data = BashOperator(
        task_id="download_raw_data",
        bash_command="cd /opt/airflow && python src/ingestion/download_data.py",
    )

    # ─────────────────────────────────────────────────────────────────
    # Task 2: Bronze
    # Reads raw data and saves it to MinIO as Delta Lake tables
    # ─────────────────────────────────────────────────────────────────
    bronze_ingestion = BashOperator(
        task_id="bronze_ingestion",
        bash_command="cd /opt/airflow && python src/pipeline/bronze_ingestion.py",
    )

    # ─────────────────────────────────────────────────────────────────
    # Task 3: Silver
    # Applies DQ rules, enriches with zone names, saves to MinIO
    # ─────────────────────────────────────────────────────────────────
    silver_transformation = BashOperator(
        task_id="silver_transformation",
        bash_command="cd /opt/airflow && python src/pipeline/silver_transformation.py",
    )

    # ─────────────────────────────────────────────────────────────────
    # Task 4: Gold
    # Aggregates Data Marts for Superset dashboards, saves to MinIO
    # ─────────────────────────────────────────────────────────────────
    gold_aggregation = BashOperator(
        task_id="gold_aggregation",
        bash_command="cd /opt/airflow && python src/pipeline/gold_aggregation.py",
    )

    # Define task dependencies (Execution Order)
    download_raw_data >> bronze_ingestion >> silver_transformation >> gold_aggregation
