"""Spark and Iceberg runtime configuration from environment variables."""

import os
from dataclasses import dataclass

from clickpulse.common.constants import (
    HDFS_RAW_PATH,
    ICEBERG_CURATED_TABLE,
    ICEBERG_DATABASE,
    ICEBERG_REJECTED_TABLE,
)

ICEBERG_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
ICEBERG_CATALOG_CLASS = "org.apache.iceberg.spark.SparkCatalog"


@dataclass(frozen=True)
class SparkRuntimeConfig:
    master: str
    catalog: str
    catalog_type: str
    namespace: str
    table: str
    rejected_table: str
    ledger_table: str
    warehouse: str
    raw_path: str
    iceberg_runtime_jar: str | None
    driver_host: str | None
    driver_bind_address: str | None


def runtime_config() -> SparkRuntimeConfig:
    """Read runtime settings without embedding machine-specific addresses."""
    hdfs_uri = os.environ.get("HDFS_NAMENODE_URI", "hdfs://localhost:9000").rstrip("/")
    raw_root = os.environ.get("HDFS_RAW_PATH", HDFS_RAW_PATH)
    if raw_root.startswith("hdfs://"):
        raw_path = raw_root.rstrip("/")
    else:
        raw_path = f"{hdfs_uri}/{raw_root.strip('/')}"
    warehouse = os.environ.get(
        "ICEBERG_WAREHOUSE", f"{hdfs_uri}/clickpulse/warehouse"
    )
    return SparkRuntimeConfig(
        master=os.environ.get("SPARK_MASTER", "local[*]"),
        catalog=os.environ.get("ICEBERG_CATALOG", "clickpulse_catalog"),
        catalog_type=os.environ.get("ICEBERG_CATALOG_TYPE", "hadoop"),
        namespace=os.environ.get("ICEBERG_DATABASE", ICEBERG_DATABASE),
        table=os.environ.get("ICEBERG_TABLE", ICEBERG_CURATED_TABLE),
        rejected_table=os.environ.get("ICEBERG_REJECTED_TABLE", ICEBERG_REJECTED_TABLE),
        ledger_table=os.environ.get("ICEBERG_LEDGER_TABLE", "clickpulse_batch_ledger"),
        warehouse=warehouse,
        raw_path=raw_path,
        iceberg_runtime_jar=os.environ.get("ICEBERG_RUNTIME_JAR"),
        driver_host=os.environ.get("SPARK_DRIVER_HOST"),
        driver_bind_address=os.environ.get("SPARK_DRIVER_BIND_ADDRESS"),
    )


def create_iceberg_spark_session(app_name: str = "clickpulse"):
    """Create Spark with the configured Iceberg catalog and SQL extensions."""
    from pathlib import Path

    from pyspark.sql import SparkSession

    config = runtime_config()
    builder = (
        SparkSession.builder.appName(app_name)
        .master(config.master)
        .config("spark.sql.extensions", ICEBERG_EXTENSIONS)
        .config(f"spark.sql.catalog.{config.catalog}", ICEBERG_CATALOG_CLASS)
        .config(f"spark.sql.catalog.{config.catalog}.type", config.catalog_type)
        .config(f"spark.sql.catalog.{config.catalog}.warehouse", config.warehouse)
        .config("spark.sql.session.timeZone", "UTC")
    )
    if config.iceberg_runtime_jar:
        jar_path = Path(config.iceberg_runtime_jar)
        if not jar_path.is_file():
            raise FileNotFoundError(f"ICEBERG_RUNTIME_JAR does not exist: {jar_path}")
        builder = builder.config("spark.jars", str(jar_path.resolve()))
    if config.driver_host:
        builder = builder.config("spark.driver.host", config.driver_host)
    if config.driver_bind_address:
        builder = builder.config("spark.driver.bindAddress", config.driver_bind_address)
    return builder.getOrCreate()
