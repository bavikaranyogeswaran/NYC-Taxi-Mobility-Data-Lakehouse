-- Initialize databases and users for Airflow and the lakehouse serving layer
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

CREATE USER lakehouse WITH PASSWORD 'lakehouse';
CREATE DATABASE lakehouse;
GRANT ALL PRIVILEGES ON DATABASE lakehouse TO lakehouse;
