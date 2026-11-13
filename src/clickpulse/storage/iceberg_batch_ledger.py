"""Durable ClickPulse batch ledger stored in an Iceberg table.

The ledger and curated table use separate Iceberg commits. Curated commits carry
the batch ID in snapshot summary metadata so recovery can detect the commit/ledger
gap. This is a recoverable protocol, not a cross-table atomic transaction.
"""

import hashlib
import json
import os
import subprocess
import tempfile
import uuid
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from clickpulse.storage.batch_ledger import (
    AttemptStatus,
    BatchRecord,
    BatchStage,
    ErrorMetadata,
    FailedAttemptMetadata,
    IcebergCommitReference,
    ProcessingCounts,
    SourceIdentity,
    StageMetadata,
)


def compute_source_sha256(
    source_path: str | Path,
    *,
    hdfs_cli: str | None = None,
    chunk_size: int = 1024 * 1024,
) -> str:
    """Hash exact source bytes incrementally, locally or through ``hdfs dfs -cat``."""
    source = str(source_path)
    parsed = urlparse(source)
    if parsed.scheme not in ("hdfs", "viewfs"):
        local_path = Path(unquote(parsed.path)) if parsed.scheme == "file" else Path(source)
        digest = hashlib.sha256()
        with local_path.open("rb") as stream:
            while chunk := stream.read(chunk_size):
                digest.update(chunk)
        return digest.hexdigest()

    command = hdfs_cli or os.environ.get("HDFS_CLI", "hdfs")
    with tempfile.TemporaryFile() as stderr_file:
        process = subprocess.Popen(
            [command, "dfs", "-cat", source],
            stdout=subprocess.PIPE,
            stderr=stderr_file,
        )
        if process.stdout is None:
            raise RuntimeError("Could not open HDFS command output")
        digest = hashlib.sha256()
        try:
            while chunk := process.stdout.read(chunk_size):
                digest.update(chunk)
        finally:
            process.stdout.close()
        return_code = process.wait()
        if return_code:
            stderr_file.seek(0)
            detail = stderr_file.read().decode("utf-8", errors="replace")[-2000:]
            raise RuntimeError(f"HDFS read failed for {source}: {detail}")
        return digest.hexdigest()


def _timestamp_to_json(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def batch_record_to_json(record: BatchRecord) -> str:
    """Serialize the complete domain record without losing retry/stage history."""
    value = {
        "batch_id": record.batch_id,
        "checksum": record.checksum,
        "source_identity": asdict(record.source_identity),
        "last_completed_stage": (
            record.last_completed_stage.value if record.last_completed_stage else None
        ),
        "current_attempt_status": record.current_attempt_status.value,
        "current_stage": record.current_stage.value if record.current_stage else None,
        "current_attempt_id": record.current_attempt_id,
        "current_stage_started_at": (
            _timestamp_to_json(record.current_stage_started_at)
            if record.current_stage_started_at
            else None
        ),
        "stage_metadata": [
            {
                "stage": item.stage.value,
                "started_at": _timestamp_to_json(item.started_at),
                "completed_at": _timestamp_to_json(item.completed_at),
                "counts": asdict(item.counts) if item.counts else None,
            }
            for item in record.stage_metadata
        ],
        "processing_counts": asdict(record.processing_counts),
        "failed_attempts": [
            {
                "attempt_id": item.attempt_id,
                "failed_stage": item.failed_stage.value,
                "started_at": _timestamp_to_json(item.started_at),
                "failed_at": _timestamp_to_json(item.failed_at),
                "error": {
                    "error_type": item.error.error_type,
                    "message": item.error.message,
                    "occurred_at": _timestamp_to_json(item.error.occurred_at),
                    "retryable": item.error.retryable,
                },
            }
            for item in record.failed_attempts
        ],
        "iceberg_commit_reference": (
            {
                "table_identifier": record.iceberg_commit_reference.table_identifier,
                "snapshot_id": record.iceberg_commit_reference.snapshot_id,
                "committed_at": _timestamp_to_json(
                    record.iceberg_commit_reference.committed_at
                ),
            }
            if record.iceberg_commit_reference
            else None
        ),
    }
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def batch_record_from_json(encoded: str) -> BatchRecord:
    """Deserialize and revalidate a durable batch record."""
    value = json.loads(encoded)
    stage_metadata = tuple(
        StageMetadata(
            stage=BatchStage(item["stage"]),
            started_at=datetime.fromisoformat(item["started_at"]),
            completed_at=datetime.fromisoformat(item["completed_at"]),
            counts=ProcessingCounts(**item["counts"]) if item["counts"] else None,
        )
        for item in value["stage_metadata"]
    )
    failed_attempts = tuple(
        FailedAttemptMetadata(
            attempt_id=item["attempt_id"],
            failed_stage=BatchStage(item["failed_stage"]),
            started_at=datetime.fromisoformat(item["started_at"]),
            failed_at=datetime.fromisoformat(item["failed_at"]),
            error=ErrorMetadata(
                error_type=item["error"]["error_type"],
                message=item["error"]["message"],
                occurred_at=datetime.fromisoformat(item["error"]["occurred_at"]),
                retryable=item["error"]["retryable"],
            ),
        )
        for item in value["failed_attempts"]
    )
    commit = value["iceberg_commit_reference"]
    return BatchRecord(
        batch_id=value["batch_id"],
        checksum=value["checksum"],
        source_identity=SourceIdentity(**value["source_identity"]),
        last_completed_stage=(
            BatchStage(value["last_completed_stage"])
            if value["last_completed_stage"]
            else None
        ),
        current_attempt_status=AttemptStatus(value["current_attempt_status"]),
        current_stage=BatchStage(value["current_stage"]) if value["current_stage"] else None,
        current_attempt_id=value["current_attempt_id"],
        current_stage_started_at=(
            datetime.fromisoformat(value["current_stage_started_at"])
            if value["current_stage_started_at"]
            else None
        ),
        stage_metadata=stage_metadata,
        processing_counts=ProcessingCounts(**value["processing_counts"]),
        failed_attempts=failed_attempts,
        iceberg_commit_reference=(
            IcebergCommitReference(
                table_identifier=commit["table_identifier"],
                snapshot_id=commit["snapshot_id"],
                committed_at=datetime.fromisoformat(commit["committed_at"]),
            )
            if commit
            else None
        ),
    )


class IcebergBatchLedger:
    """Persist BatchRecord state as one JSON value per key in an Iceberg table."""

    def __init__(
        self,
        spark: Any,
        catalog: str,
        namespace: str,
        table: str = "clickpulse_batch_ledger",
    ) -> None:
        from clickpulse.spark.upsert_curated import _quote_identifier, _table_identifier

        self.spark = spark
        self.identifier = _table_identifier(catalog, namespace, table)
        namespace_id = ".".join(
            _quote_identifier(part) for part in (catalog, namespace)
        )
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace_id}")
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {self.identifier} ("
            "batch_id STRING NOT NULL, record_json STRING NOT NULL, updated_at TIMESTAMP NOT NULL"
            ") USING iceberg"
        )

    def get(self, batch_id: str) -> BatchRecord | None:
        from pyspark.sql import functions as F

        rows = (
            self.spark.table(self.identifier)
            .where(F.col("batch_id") == batch_id)
            .limit(2)
            .collect()
        )
        if len(rows) > 1:
            raise RuntimeError(f"Ledger contains duplicate rows for batch {batch_id}")
        return batch_record_from_json(rows[0].record_json) if rows else None

    def save(self, record: BatchRecord) -> None:
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType, StructField, StructType, TimestampType

        existing_count = (
            self.spark.table(self.identifier)
            .where(F.col("batch_id") == record.batch_id)
            .count()
        )
        if existing_count > 1:
            raise RuntimeError(f"Ledger contains duplicate rows for batch {record.batch_id}")
        schema = StructType(
            [
                StructField("batch_id", StringType(), False),
                StructField("record_json", StringType(), False),
                StructField("updated_at", TimestampType(), False),
            ]
        )
        row = (
            record.batch_id,
            batch_record_to_json(record),
            datetime.now(timezone.utc).replace(tzinfo=None),
        )
        view = f"clickpulse_ledger_update_{uuid.uuid4().hex}"
        self.spark.createDataFrame([row], schema).createOrReplaceTempView(view)
        try:
            self.spark.sql(
                f"MERGE INTO {self.identifier} AS t USING {view} AS s "
                "ON t.batch_id = s.batch_id "
                "WHEN MATCHED THEN UPDATE SET "
                "t.record_json = s.record_json, t.updated_at = s.updated_at "
                "WHEN NOT MATCHED THEN INSERT (batch_id, record_json, updated_at) "
                "VALUES (s.batch_id, s.record_json, s.updated_at)"
            )
        finally:
            self.spark.catalog.dropTempView(view)
        if self.get(record.batch_id) is None:
            raise RuntimeError(f"Ledger write did not persist batch {record.batch_id}")

    def register(self, checksum: str, source_identity: SourceIdentity) -> tuple[BatchRecord, bool]:
        existing = self.get(checksum)
        if existing is not None:
            if existing.checksum != checksum:
                raise RuntimeError("Stored batch checksum does not match the requested identity")
            return existing, False
        record = BatchRecord(
            batch_id=checksum,
            checksum=checksum,
            source_identity=source_identity,
        )
        self.save(record)
        return record, True
