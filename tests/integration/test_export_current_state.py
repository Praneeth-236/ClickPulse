"""VM3 integration coverage for Iceberg current-state Parquet export."""

import os

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("CLICKPULSE_RUN_ICEBERG_INTEGRATION") != "1"
    or not os.environ.get("ICEBERG_RUNTIME_JAR"),
    reason="requires configured Spark 3.3.4 and Iceberg runtime jar",
)


def test_export_validates_source_and_replaces_parquet_safely(tmp_path, monkeypatch):
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    import clickpulse.druid.export_current_state as exporter
    from clickpulse.spark.runtime import create_iceberg_spark_session

    catalog = "clickpulse_export_test"
    namespace = "acceptance"
    table = "current_events"
    table_id = f"{catalog}.{namespace}.{table}"
    warehouse = (tmp_path / "warehouse").resolve().as_uri()
    destination = (tmp_path / "exports" / "current-state").resolve().as_uri()
    monkeypatch.setenv("SPARK_MASTER", "local[2]")
    monkeypatch.setenv("ICEBERG_CATALOG", catalog)
    monkeypatch.setenv("ICEBERG_CATALOG_TYPE", "hadoop")
    monkeypatch.setenv("ICEBERG_DATABASE", namespace)
    monkeypatch.setenv("ICEBERG_TABLE", table)
    monkeypatch.setenv("ICEBERG_WAREHOUSE", warehouse)
    monkeypatch.setenv("SPARK_DRIVER_HOST", "127.0.0.1")
    monkeypatch.setenv("SPARK_DRIVER_BIND_ADDRESS", "127.0.0.1")

    spark = create_iceberg_spark_session("clickpulse-export-integration")
    schema = StructType(
        [
            StructField("event_id", StringType(), nullable=True),
            StructField("payload", IntegerType(), nullable=True),
        ]
    )
    try:
        assert spark.version == "3.3.4"
        spark.sql(f"CREATE NAMESPACE {catalog}.{namespace}")
        spark.sql(
            f"CREATE TABLE {table_id} (event_id STRING, payload INT) USING iceberg"
        )
        with pytest.raises(ValueError, match="empty"):
            exporter.export_current_state(spark, destination)

        spark.createDataFrame([("evt-a", 1), ("evt-b", 2)], schema).writeTo(
            table_id
        ).append()
        first = exporter.export_current_state(spark, destination)
        assert first.record_count == 2
        assert spark.read.parquet(destination).count() == 2

        original_write = exporter._write_staged_parquet

        def fail_write(_dataframe, _staging_path):
            raise OSError("simulated staging write failure")

        monkeypatch.setattr(exporter, "_write_staged_parquet", fail_write)
        with pytest.raises(OSError, match="staging write failure"):
            exporter.export_current_state(spark, destination)
        assert spark.read.parquet(destination).count() == 2
        monkeypatch.setattr(exporter, "_write_staged_parquet", original_write)

        spark.createDataFrame([("evt-c", 3)], schema).writeTo(table_id).append()
        second = exporter.export_current_state(spark, destination)
        assert second.record_count == 3
        assert spark.read.parquet(destination).count() == 3

        spark.createDataFrame([("evt-a", 4)], schema).writeTo(table_id).append()
        with pytest.raises(ValueError, match="duplicate event_id"):
            exporter.export_current_state(spark, destination)
        assert spark.read.parquet(destination).count() == 3

        spark.sql(f"DELETE FROM {table_id}")
        spark.createDataFrame([(None, 5)], schema).writeTo(table_id).append()
        with pytest.raises(ValueError, match="null event_id"):
            exporter.export_current_state(spark, destination)
        assert spark.read.parquet(destination).count() == 3
    finally:
        spark.stop()
