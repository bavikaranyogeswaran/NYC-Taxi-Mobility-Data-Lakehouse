-- Initialize databases and users for Airflow and Superset
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

CREATE USER superset WITH PASSWORD 'superset';
CREATE DATABASE superset;
GRANT ALL PRIVILEGES ON DATABASE superset TO superset;
