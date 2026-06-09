-- Initialize databases and users for Airflow
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
