"""
spark_utils.py
──────────────────────────────────────────────────────────────────────
Shared SparkSession factory used by all pipeline scripts.

JAR strategy
────────────
When running inside the Airflow Docker container, the JARs are
pre-baked at /opt/spark/jars (see Dockerfile.airflow).  We pass them
directly via spark.jars so Ivy never needs to hit Maven Central.

When running locally on Windows (dev mode), we fall back to
configure_spark_with_delta_pip + extra_packages which lets Ivy resolve
and cache the jars in ~/.ivy2 the first time.
──────────────────────────────────────────────────────────────────────
"""

import os
from pathlib import Path

# ── Project root (two levels up from src/utils/) ──────────────────────────────
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# Pre-baked JAR directory inside the Docker image (set in Dockerfile.airflow)
_DOCKER_JARS_DIR = Path(os.environ.get("SPARK_JARS_DIR", "/opt/spark/jars"))

# Maven coordinates used when falling back to Ivy resolution
_EXTRA_PACKAGES = [
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    "org.postgresql:postgresql:42.6.0",
]

# S3 / MinIO endpoint: inside Docker it's the service name, locally it's 127.0.0.1
_MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://127.0.0.1:9000")
_MINIO_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
_MINIO_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")


def create_spark_session(app_name: str):
    """
    Returns a fully-configured SparkSession with Delta Lake + S3A support.

    Auto-detects whether the pre-baked JARs are present (Docker) or whether
    Ivy resolution is needed (local Windows dev).
    """
    import logging
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip

    logger = logging.getLogger(__name__)

    # ── Windows / local fix: ensure HADOOP_HOME + winutils.exe are on PATH ────
    hadoop_home = str(_PROJECT_ROOT / "hadoop")
    os.environ.setdefault("HADOOP_HOME", hadoop_home)
    os.environ.setdefault("hadoop.home.dir", hadoop_home)
    if os.name == "nt":  # Windows only
        os.environ["PATH"] = hadoop_home + r"\bin;" + os.environ.get("PATH", "")

    logger.info(f"Initialising SparkSession [{app_name}]  |  MinIO={_MINIO_ENDPOINT}")

    # ── Base builder shared by both paths ─────────────────────────────────────
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # S3A / MinIO settings
        .config("spark.hadoop.fs.s3a.endpoint", _MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", _MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", _MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.driver.memory", "4g")
        .config("spark.log.level", "WARN")
    )

    use_prebaked = _DOCKER_JARS_DIR.exists() and any(_DOCKER_JARS_DIR.iterdir())

    if use_prebaked:
        # ── Docker path: reference pre-downloaded JARs directly ───────────────
        jars = ",".join(str(p) for p in _DOCKER_JARS_DIR.glob("*.jar"))
        logger.info(f"Using pre-baked JARs from {_DOCKER_JARS_DIR}  ({len(list(_DOCKER_JARS_DIR.glob('*.jar')))} files)")
        builder = builder.config("spark.jars", jars)
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    else:
        # ── Local path: let Ivy download + cache the packages ──────────────────
        logger.info("Pre-baked JARs not found — using Ivy resolution (first run may be slow)")
        spark = configure_spark_with_delta_pip(
            builder, extra_packages=_EXTRA_PACKAGES
        ).getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"SparkSession ready  |  version={spark.version}")
    return spark
