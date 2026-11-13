"""Safely export the complete Iceberg current state as a Parquet directory."""

import uuid
from dataclasses import dataclass
from typing import Any

from clickpulse.spark.runtime import create_iceberg_spark_session, runtime_config
from clickpulse.spark.upsert_curated import _table_identifier


@dataclass(frozen=True)
class CurrentStateExportResult:
    """Metadata describing a completed current-state export."""

    source_table: str
    destination: str
    record_count: int
    schema_json: str


def _validate_export_counts(
    record_count: int, null_event_id_count: int, duplicate_id_count: int
) -> None:
    if record_count == 0:
        raise ValueError(
            "Curated Iceberg source table is empty; refusing unexpected empty export"
        )
    if null_event_id_count:
        raise ValueError(
            f"Curated Iceberg source contains {null_event_id_count} null event_id values"
        )
    if duplicate_id_count:
        raise ValueError(
            f"Curated Iceberg source contains {duplicate_id_count} duplicate event_id groups"
        )


def _validate_current_state(dataframe: Any) -> int:
    from pyspark.sql import functions as F

    if "event_id" not in dataframe.columns:
        raise ValueError("Curated Iceberg source is missing required event_id column")
    record_count = dataframe.count()
    null_count = dataframe.where(F.col("event_id").isNull()).count()
    duplicate_count = (
        dataframe.groupBy("event_id")
        .count()
        .where(F.col("count") > 1)
        .count()
    )
    _validate_export_counts(record_count, null_count, duplicate_count)
    return record_count


def _promote_staging_export(
    filesystem: Any, staging_path: Any, destination_path: Any, backup_path: Any
) -> None:
    """Promote a staged directory, restoring the old directory on failure."""
    had_previous = filesystem.exists(destination_path)
    if had_previous and not filesystem.rename(destination_path, backup_path):
        raise RuntimeError(f"Could not move current export to backup: {backup_path}")

    try:
        if not filesystem.rename(staging_path, destination_path):
            raise RuntimeError(f"Could not promote staged export to: {destination_path}")
    except Exception as promotion_error:
        if had_previous:
            try:
                restored = filesystem.rename(backup_path, destination_path)
            except Exception as rollback_error:
                raise RuntimeError(
                    "Export promotion failed and backup rollback raised an error; "
                    f"backup remains at {backup_path}"
                ) from rollback_error
            if not restored:
                raise RuntimeError(
                    "Export promotion failed and backup rollback failed; "
                    f"backup remains at {backup_path}"
                ) from promotion_error
        raise

    if had_previous and not filesystem.delete(backup_path, True):
        raise RuntimeError(
            "Export was promoted, but the previous export backup could not be removed: "
            f"{backup_path}"
        )


def _filesystem_for_path(spark: Any, hadoop_path: Any) -> Any:
    configuration = spark.sparkContext._jsc.hadoopConfiguration()
    return hadoop_path.getFileSystem(configuration)


def _write_staged_parquet(dataframe: Any, staging_path: str) -> None:
    dataframe.write.mode("errorifexists").parquet(staging_path)


def export_current_state(
    spark: Any | None = None,
    destination: str | None = None,
) -> CurrentStateExportResult:
    """Export all rows from the configured logical Iceberg table to Parquet.

    When called without a SparkSession this function owns and stops the session.
    Callers that supply a session retain ownership of it.
    """
    config = runtime_config()
    session = spark if spark is not None else create_iceberg_spark_session(
        "clickpulse-current-state-export"
    )
    output_path = destination or config.export_path
    table_identifier = _table_identifier(config.catalog, config.namespace, config.table)
    try:
        current_state = session.table(table_identifier)
        from pyspark.sql import functions as F

        # Persist the exact verified source snapshot for both writing and readback.
        verified_state = current_state.persist()
        try:
            record_count = _validate_current_state(verified_state)
            verified_count = verified_state.count()
            if verified_count != record_count:
                raise RuntimeError(
                    f"Materialized source count {verified_count} differs from validated "
                    f"count {record_count}"
                )
            destination_path = session.sparkContext._jvm.org.apache.hadoop.fs.Path(
                output_path
            )
            filesystem = _filesystem_for_path(session, destination_path)
            staging_path = session.sparkContext._jvm.org.apache.hadoop.fs.Path(
                f"{output_path}.staging-{uuid.uuid4().hex}"
            )
            backup_path = session.sparkContext._jvm.org.apache.hadoop.fs.Path(
                f"{output_path}.backup-{uuid.uuid4().hex}"
            )
            stage_fs = _filesystem_for_path(session, staging_path)
            try:
                if stage_fs.exists(staging_path):
                    raise FileExistsError(
                        f"Unique staging path already exists: {staging_path}"
                    )
                _write_staged_parquet(verified_state, str(staging_path))
                staged = session.read.parquet(str(staging_path))
                staged_count = staged.count()
                staged_null_count = staged.where(F.col("event_id").isNull()).count()
                staged_duplicate_count = (
                    staged.groupBy("event_id")
                    .count()
                    .where(F.col("count") > 1)
                    .count()
                )
                _validate_export_counts(
                    staged_count, staged_null_count, staged_duplicate_count
                )
                if staged_count != record_count:
                    raise RuntimeError(
                        f"Staged Parquet row count {staged_count} differs from source "
                        f"count {record_count}"
                    )
                _promote_staging_export(
                    filesystem, staging_path, destination_path, backup_path
                )
                return CurrentStateExportResult(
                    source_table=table_identifier,
                    destination=output_path,
                    record_count=record_count,
                    schema_json=current_state.schema.json(),
                )
            finally:
                if stage_fs.exists(staging_path):
                    stage_fs.delete(staging_path, True)
        finally:
            verified_state.unpersist()
    finally:
        if spark is None:
            session.stop()
