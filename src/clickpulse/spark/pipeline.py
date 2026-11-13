"""Spark batch orchestration with an Iceberg-backed durable processing ledger."""

import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from clickpulse.spark.upsert_curated import SparkUpsertResult, run_upsert
from clickpulse.spark.validate_and_deduplicate import (
    SparkDeduplicationResult,
    SparkValidationResult,
    deduplicate_validated_dataframe,
    read_jsonl_occurrences,
    validate_occurrences_dataframe,
)
from clickpulse.storage.batch_ledger import (
    AttemptStatus,
    BatchRecord,
    BatchStage,
    ErrorMetadata,
    IcebergCommitReference,
    ProcessingCounts,
    SourceIdentity,
    advance_batch_stage,
    fail_batch_attempt,
    start_batch_stage,
)
from clickpulse.storage.iceberg_batch_ledger import (
    IcebergBatchLedger,
    compute_source_sha256,
)


@dataclass(frozen=True)
class BatchPipelineResult:
    batch_id: str
    replayed: bool
    validation: SparkValidationResult | None
    deduplication: SparkDeduplicationResult | None
    upsert: SparkUpsertResult | None


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _source_identity(source_path: str) -> SourceIdentity:
    parsed = urlparse(source_path)
    if parsed.scheme in ("hdfs", "viewfs", "file"):
        source_name = Path(unquote(parsed.path)).name
    else:
        source_name = Path(source_path).name
    name = source_name or source_path
    return SourceIdentity(name or source_path, source_path)


def _next_stage(record: BatchRecord) -> BatchStage | None:
    ordered = tuple(BatchStage)
    if record.last_completed_stage is None:
        return ordered[0]
    index = ordered.index(record.last_completed_stage) + 1
    return ordered[index] if index < len(ordered) else None


def _counts_with(record: BatchRecord, **updates: int) -> ProcessingCounts:
    values = {
        field: getattr(record.processing_counts, field)
        for field in ProcessingCounts.__dataclass_fields__
    }
    values.update(updates)
    return ProcessingCounts(**values)


def _begin_stage(
    ledger: IcebergBatchLedger, record: BatchRecord, stage: BatchStage
) -> BatchRecord:
    if (
        record.current_attempt_status is AttemptStatus.RUNNING
        and record.current_stage is stage
        and record.current_stage_started_at is not None
    ):
        return record
    attempt_id = uuid.uuid4().hex
    started = start_batch_stage(
        record,
        stage,
        attempt_id=attempt_id,
        started_at=_now(),
    )
    ledger.save(started)
    return started


def _finish_stage(
    ledger: IcebergBatchLedger,
    record: BatchRecord,
    stage: BatchStage,
    *,
    counts: ProcessingCounts,
    commit_reference: IcebergCommitReference | None = None,
    completed_at: datetime | None = None,
) -> BatchRecord:
    completed = advance_batch_stage(
        record,
        stage,
        completed_at=completed_at or _now(),
        counts=counts,
        iceberg_commit_reference=commit_reference,
    )
    ledger.save(completed)
    return completed


def _record_failure(ledger: IcebergBatchLedger, record: BatchRecord, error: Exception) -> None:
    if (
        record.current_attempt_status is AttemptStatus.RUNNING
        and record.current_stage is not None
        and record.current_stage_started_at is not None
    ):
        occurred_at = _now()
        failure = fail_batch_attempt(
            record,
            ErrorMetadata(
                error_type=type(error).__name__,
                message="Batch stage failed; inspect sanitized application logs.",
                occurred_at=occurred_at,
                retryable=True,
            ),
            failed_at=occurred_at,
        )
        ledger.save(failure)


def _configured_catalog_table_exists(spark: Any, table_identifier: str) -> bool:
    """Check table existence through its configured Spark V2 catalog.

    Spark 3.3's ``spark.catalog.tableExists`` does not accept catalog-qualified
    identifiers. Calling the configured ``TableCatalog`` directly avoids
    resolving this identifier against the session's default catalog. Catalog,
    authorization, and connectivity errors are intentionally allowed to
    propagate; only the catalog's normal ``False`` result means absent.
    """
    import re

    parts = tuple(part.strip("`") for part in table_identifier.split("."))
    if len(parts) != 3 or any(
        re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", part) is None for part in parts
    ):
        raise ValueError("table_identifier must be a catalog.namespace.table identifier")
    catalog_name, namespace, table_name = parts

    jvm = spark._jvm
    namespace_array = jvm.java.lang.reflect.Array.newInstance(
        jvm.java.lang.String, 1
    )
    jvm.java.lang.reflect.Array.set(namespace_array, 0, namespace)
    identifier = jvm.org.apache.spark.sql.connector.catalog.Identifier.of(
        namespace_array, table_name
    )
    catalog = (
        spark._jsparkSession.sessionState()
        .catalogManager()
        .catalog(catalog_name)
    )
    return bool(catalog.tableExists(identifier))


def _find_curated_commit(
    spark: Any, table_identifier: str, batch_id: str
) -> tuple[IcebergCommitReference, dict[str, int]] | None:
    from pyspark.sql import functions as F

    if not _configured_catalog_table_exists(spark, table_identifier):
        return None

    snapshots = spark.table(f"{table_identifier}.snapshots").where(
        F.col("summary").getItem("clickpulse.batch_id") == batch_id
    )
    snapshot = snapshots.orderBy(F.col("committed_at").desc()).first()
    if snapshot is None:
        return None
    reference = IcebergCommitReference(
        table_identifier=table_identifier,
        snapshot_id=str(snapshot.snapshot_id),
        committed_at=snapshot.committed_at.replace(tzinfo=timezone.utc),
    )
    summary = snapshot.summary
    counts = {
        field: int(summary.get(f"clickpulse.{field}", "0"))
        for field in ("insert_count", "update_count", "drop_count")
    }
    return reference, counts


def recover_batch(
    spark: Any,
    ledger: IcebergBatchLedger,
    record: BatchRecord,
    *,
    curated_table_identifier: str,
) -> BatchRecord:
    """Reconcile a persisted running/failed batch with curated Iceberg snapshots."""
    if record.last_completed_stage is BatchStage.COMPLETED:
        return record

    recovered_commit = _find_curated_commit(
        spark, curated_table_identifier, record.batch_id
    )
    if recovered_commit is not None and _next_stage(record) is BatchStage.CURATED_COMMITTED:
        commit, operation_counts = recovered_commit
        if not (
            record.current_attempt_status is AttemptStatus.RUNNING
            and record.current_stage is BatchStage.CURATED_COMMITTED
        ):
            record = _begin_stage(ledger, record, BatchStage.CURATED_COMMITTED)
        record = _finish_stage(
            ledger,
            record,
            BatchStage.CURATED_COMMITTED,
            counts=_counts_with(record, **operation_counts),
            commit_reference=commit,
        )
        return record

    if record.current_attempt_status is AttemptStatus.RUNNING and record.current_stage is not None:
        _record_failure(
            ledger,
            record,
            RuntimeError("Processing attempt was interrupted before its stage was recorded"),
        )
        recovered = ledger.get(record.batch_id)
        return recovered if recovered is not None else record
    return record


def _complete_batch(ledger: IcebergBatchLedger, record: BatchRecord) -> BatchRecord:
    record = _begin_stage(ledger, record, BatchStage.COMPLETED)
    return _finish_stage(
        ledger,
        record,
        BatchStage.COMPLETED,
        counts=record.processing_counts,
    )


def process_batch(
    source_path: str,
    *,
    spark: Any | None = None,
    after_curated_commit: Callable[[str], None] | None = None,
) -> BatchPipelineResult:
    """Process one JSONL source through validation, deduplication and Iceberg upsert.

    The source SHA-256 is streamed from the original file bytes. Completed replay
    returns before Spark reads or validates records. A process crash after the
    curated commit is recovered from the batch marker in Iceberg snapshot metadata.
    """
    from clickpulse.spark.runtime import create_iceberg_spark_session, runtime_config
    from clickpulse.spark.upsert_curated import _table_identifier

    session = spark if spark is not None else create_iceberg_spark_session("clickpulse-batch")
    config = runtime_config()
    ledger = IcebergBatchLedger(
        session,
        config.catalog,
        config.namespace,
        config.ledger_table,
    )
    batch_id = compute_source_sha256(source_path)
    record, _ = ledger.register(batch_id, _source_identity(source_path))
    if record.last_completed_stage is BatchStage.COMPLETED:
        return BatchPipelineResult(batch_id, True, None, None, None)

    curated_table = _table_identifier(config.catalog, config.namespace, config.table)
    record = recover_batch(
        session,
        ledger,
        record,
        curated_table_identifier=curated_table,
    )
    if record.last_completed_stage is BatchStage.COMPLETED:
        return BatchPipelineResult(batch_id, True, None, None, None)

    if _next_stage(record) is BatchStage.COMPLETED:
        record = _complete_batch(ledger, record)
        return BatchPipelineResult(batch_id, True, None, None, None)

    raw_occurrences = read_jsonl_occurrences(session, source_path)
    if record.last_completed_stage is None:
        record = _begin_stage(ledger, record, BatchStage.RECEIVED)
        record = _finish_stage(
            ledger,
            record,
            BatchStage.RECEIVED,
            counts=record.processing_counts,
        )

    if _next_stage(record) is BatchStage.VALIDATED:
        record = _begin_stage(ledger, record, BatchStage.VALIDATED)
    validation = None
    try:
        validation = validate_occurrences_dataframe(raw_occurrences)
        if _next_stage(record) is BatchStage.VALIDATED:
            record = _finish_stage(
                ledger,
                record,
                BatchStage.VALIDATED,
                counts=_counts_with(
                    record,
                    raw_count=validation.metrics.raw_line_count,
                    valid_count=validation.metrics.valid_record_count,
                    reject_count=validation.metrics.rejected_occurrence_count,
                ),
            )
    except Exception as exc:
        _record_failure(ledger, record, exc)
        raise

    if _next_stage(record) is BatchStage.DEDUPLICATED:
        record = _begin_stage(ledger, record, BatchStage.DEDUPLICATED)
    deduplication = None
    try:
        deduplication = deduplicate_validated_dataframe(validation.validated_records)
        if _next_stage(record) is BatchStage.DEDUPLICATED:
            metrics = deduplication.metrics
            record = _finish_stage(
                ledger,
                record,
                BatchStage.DEDUPLICATED,
                counts=_counts_with(
                    record,
                    exact_duplicate_count=metrics.exact_duplicate_excess,
                    conflict_resolution_count=(
                        metrics.conflicting_event_id_groups
                        - metrics.unresolved_latest_timestamp_tie_count
                    ),
                ),
            )
    except Exception as exc:
        _record_failure(ledger, record, exc)
        raise

    if _next_stage(record) is BatchStage.CURATED_COMMITTED:
        record = _begin_stage(ledger, record, BatchStage.CURATED_COMMITTED)
    upsert: SparkUpsertResult | None = None
    commit_returned = False
    try:
        if _next_stage(record) is BatchStage.CURATED_COMMITTED:
            upsert = run_upsert(
                deduplication.winners,
                spark=session,
                unresolved_ties=deduplication.unresolved_ties,
                batch_id=batch_id,
            )
            commit_returned = True
            # This hook deliberately runs before the ledger advances the stage.
            # Raising here emulates a process loss after the Iceberg commit.
            if after_curated_commit is not None:
                after_curated_commit(batch_id)
            commit_reference = None
            if upsert.iceberg_snapshot_id and upsert.iceberg_committed_at:
                commit_reference = IcebergCommitReference(
                    table_identifier=curated_table,
                    snapshot_id=upsert.iceberg_snapshot_id,
                    committed_at=upsert.iceberg_committed_at,
                )
            metrics = upsert.metrics
            record = _finish_stage(
                ledger,
                record,
                BatchStage.CURATED_COMMITTED,
                counts=_counts_with(
                    record,
                    insert_count=metrics.insert_count,
                    update_count=metrics.update_count,
                    drop_count=metrics.drop_count,
                ),
                commit_reference=commit_reference,
            )
    except Exception as exc:
        # Preserve RUNNING for the explicit post-commit crash hook so startup
        # recovery can reconcile it from the Iceberg snapshot marker.
        if not commit_returned:
            _record_failure(ledger, record, exc)
        raise

    record = _complete_batch(ledger, record)
    return BatchPipelineResult(batch_id, False, validation, deduplication, upsert)
