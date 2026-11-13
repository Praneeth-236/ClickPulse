"""Tests for exact source hashing and durable ledger serialization."""

from datetime import datetime, timezone

from clickpulse.storage.batch_ledger import (
    BatchRecord,
    BatchStage,
    ErrorMetadata,
    ProcessingCounts,
    SourceIdentity,
    advance_batch_stage,
    compute_batch_id,
    fail_batch_attempt,
    start_batch_stage,
)
from clickpulse.storage.iceberg_batch_ledger import (
    batch_record_from_json,
    batch_record_to_json,
    compute_source_sha256,
)


def test_streaming_source_hash_preserves_exact_local_bytes(tmp_path):
    lf = tmp_path / "lf.jsonl"
    crlf = tmp_path / "crlf.jsonl"
    lf.write_bytes(b'{"event_id":"x"}\n')
    crlf.write_bytes(b'{"event_id":"x"}\r\n')
    assert compute_source_sha256(lf) == compute_batch_id(lf.read_bytes())
    assert compute_source_sha256(crlf) == compute_batch_id(crlf.read_bytes())
    assert compute_source_sha256(lf) != compute_source_sha256(crlf)


def test_durable_serialization_preserves_failed_attempt_history():
    now = datetime(2025, 1, 1, tzinfo=timezone.utc)
    digest = compute_batch_id(b"original exact bytes")
    batch = BatchRecord(
        batch_id=digest,
        checksum=digest,
        source_identity=SourceIdentity("input.jsonl", "hdfs://namenode/raw/input.jsonl"),
    )
    started = start_batch_stage(
        batch, BatchStage.RECEIVED, attempt_id="attempt-1", started_at=now
    )
    received = advance_batch_stage(
        started,
        BatchStage.RECEIVED,
        completed_at=now,
        counts=ProcessingCounts(raw_count=5),
    )
    retry = start_batch_stage(
        received,
        BatchStage.VALIDATED,
        attempt_id="attempt-2",
        started_at=now,
    )
    failed = fail_batch_attempt(
        retry,
        ErrorMetadata("ValidationError", "sanitized", now, True),
        failed_at=now,
    )

    restored = batch_record_from_json(batch_record_to_json(failed))
    assert restored == failed
    assert restored.last_completed_stage is BatchStage.RECEIVED
    assert restored.failed_attempts[0].attempt_id == "attempt-2"
    assert restored.failed_attempts[0].started_at == now
    assert restored.failed_attempts[0].failed_at == now
    assert restored.processing_counts.raw_count == 5
