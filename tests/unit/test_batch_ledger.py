"""Tests for backend-independent batch identity and state domain behavior."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone, tzinfo

import pytest

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
    advance_batch_stage,
    compute_batch_id,
    fail_batch_attempt,
    get_batch_id,
    start_batch_stage,
    validate_stage_transition,
)

NOW = datetime(2025, 1, 1, 12, tzinfo=UTC)
OFFSET = timezone(timedelta(hours=5, minutes=30))


def batch_record(payload=b"batch", **changes):
    batch_id = compute_batch_id(payload)
    values = {
        "batch_id": batch_id,
        "source_identity": SourceIdentity("clicks_day1.jsonl", "hdfs:///raw/day1"),
        "checksum": batch_id,
    }
    values.update(changes)
    return BatchRecord(**values)


def complete_stage(batch, stage, attempt_id="attempt-1", started_at=NOW):
    running = start_batch_stage(
        batch, stage, attempt_id=attempt_id, started_at=started_at
    )
    return advance_batch_stage(
        running, stage, completed_at=started_at + timedelta(minutes=1)
    )


def fail_current_stage(batch, attempt_id, started_at, failed_at):
    running = start_batch_stage(
        batch,
        BatchStage.VALIDATED,
        attempt_id=attempt_id,
        started_at=started_at,
    )
    error = ErrorMetadata("ValidationError", "sanitized details", failed_at, True)
    return fail_batch_attempt(running, error, failed_at=failed_at)


def test_identical_original_bytes_produce_identical_hashes_and_legacy_alias():
    payload = b'{"event_id":"evt-1"}\n'
    assert compute_batch_id(payload) == compute_batch_id(payload)
    assert get_batch_id(payload) == compute_batch_id(payload)


def test_different_original_bytes_produce_different_hashes():
    assert compute_batch_id(b"first") != compute_batch_id(b"second")


def test_crlf_and_lf_hashes_are_different():
    assert compute_batch_id(b"first\r\nsecond\r\n") != compute_batch_id(
        b"first\nsecond\n"
    )


def test_empty_bytes_have_a_deterministic_sha256_hash():
    expected = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    assert compute_batch_id(b"") == expected


@pytest.mark.parametrize("value", ["text", bytearray(b"bytes"), memoryview(b"bytes"), None])
def test_batch_hash_rejects_non_bytes_inputs(value):
    with pytest.raises(TypeError, match="must be bytes"):
        compute_batch_id(value)


def test_batch_record_accepts_canonical_sha256_identity():
    digest = "a" * 64
    record = BatchRecord(digest, SourceIdentity("source.jsonl"), digest)
    assert record.batch_id == record.checksum == digest


@pytest.mark.parametrize(
    "digest",
    [
        "a" * 62 + "  ",  # Same string length but only 31 decoded bytes.
        "A" * 64,
        "g" + "a" * 63,
        "a" * 63,
        "a" * 65,
    ],
)
def test_batch_record_rejects_noncanonical_or_wrong_length_digests(digest):
    with pytest.raises(ValueError, match="64 lowercase hexadecimal"):
        BatchRecord(digest, SourceIdentity("source.jsonl"), digest)


def test_batch_id_and_checksum_must_match():
    with pytest.raises(ValueError, match="batch_id must equal checksum"):
        BatchRecord("a" * 64, SourceIdentity("source.jsonl"), "b" * 64)


def test_all_forward_stage_transitions_are_allowed_one_at_a_time():
    previous = None
    for stage in BatchStage:
        validate_stage_transition(previous, stage)
        previous = stage


@pytest.mark.parametrize(
    ("previous", "target"),
    [
        (None, BatchStage.VALIDATED),
        (BatchStage.RECEIVED, BatchStage.DEDUPLICATED),
        (BatchStage.VALIDATED, BatchStage.RECEIVED),
        (BatchStage.COMPLETED, BatchStage.COMPLETED),
    ],
)
def test_invalid_or_skipped_stage_transitions_are_rejected(previous, target):
    with pytest.raises(ValueError, match="Invalid batch stage transition"):
        validate_stage_transition(previous, target)


def test_stage_advance_requires_starting_and_completing_each_stage():
    batch = batch_record()
    with pytest.raises(ValueError, match="no attempt is running"):
        advance_batch_stage(batch, BatchStage.RECEIVED, completed_at=NOW)
    receiving = start_batch_stage(
        batch, BatchStage.RECEIVED, attempt_id="attempt-1", started_at=NOW
    )
    with pytest.raises(ValueError, match="already running"):
        start_batch_stage(
            receiving, BatchStage.RECEIVED, attempt_id="attempt-2", started_at=NOW
        )
    received = advance_batch_stage(
        receiving, BatchStage.RECEIVED, completed_at=NOW + timedelta(minutes=1)
    )
    with pytest.raises(ValueError, match="expected VALIDATED"):
        start_batch_stage(
            received, BatchStage.DEDUPLICATED, attempt_id="attempt-2", started_at=NOW
        )


def test_failed_attempt_preserves_last_successful_stage_and_retry_details():
    received = complete_stage(batch_record(), BatchStage.RECEIVED)
    failed = fail_current_stage(
        received, "attempt-2", NOW + timedelta(minutes=2), NOW + timedelta(minutes=3)
    )
    assert failed.last_completed_stage is BatchStage.RECEIVED
    assert failed.current_stage is BatchStage.VALIDATED
    assert failed.current_attempt_status is AttemptStatus.FAILED
    assert failed.failed_attempts[0].attempt_id == "attempt-2"
    assert failed.failed_attempts[0].failed_stage is BatchStage.VALIDATED
    assert failed.failed_attempts[0].started_at == NOW + timedelta(minutes=2)
    assert failed.failed_attempts[0].failed_at == NOW + timedelta(minutes=3)
    assert failed.stage_metadata == received.stage_metadata

    retry_started = start_batch_stage(
        failed,
        BatchStage.VALIDATED,
        attempt_id="attempt-3",
        started_at=NOW + timedelta(minutes=4),
    )
    retry_failed = fail_batch_attempt(
        retry_started,
        ErrorMetadata(
            "ValidationError", "second sanitized failure", NOW + timedelta(minutes=6), True
        ),
    )
    assert len(retry_failed.failed_attempts) == 2
    assert [failure.attempt_id for failure in retry_failed.failed_attempts] == [
        "attempt-2",
        "attempt-3",
    ]
    assert [failure.started_at for failure in retry_failed.failed_attempts] == [
        NOW + timedelta(minutes=2),
        NOW + timedelta(minutes=4),
    ]
    assert [failure.failed_at for failure in retry_failed.failed_attempts] == [
        NOW + timedelta(minutes=3),
        NOW + timedelta(minutes=6),
    ]
    assert retry_failed.last_completed_stage is BatchStage.RECEIVED


def test_failed_at_cannot_precede_error_occurrence():
    received = complete_stage(batch_record(), BatchStage.RECEIVED)
    running = start_batch_stage(
        received,
        BatchStage.VALIDATED,
        attempt_id="attempt-2",
        started_at=NOW + timedelta(minutes=2),
    )
    error = ErrorMetadata(
        "ValidationError", "sanitized details", NOW + timedelta(minutes=4), True
    )

    with pytest.raises(ValueError, match="failed_at cannot precede error.occurred_at"):
        fail_batch_attempt(
            running, error, failed_at=NOW + timedelta(minutes=3, seconds=59)
        )

    failed = fail_batch_attempt(
        running, error, failed_at=NOW + timedelta(minutes=4)
    )
    assert failed.failed_attempts[0].failed_at == error.occurred_at
    assert failed.last_completed_stage is BatchStage.RECEIVED


def test_failed_attempt_id_cannot_be_reused_but_unique_retry_id_is_allowed():
    received = complete_stage(batch_record(), BatchStage.RECEIVED)
    failed = fail_current_stage(
        received, "attempt-2", NOW + timedelta(minutes=2), NOW + timedelta(minutes=3)
    )
    with pytest.raises(ValueError, match="cannot reuse failed-attempt history"):
        start_batch_stage(
            failed,
            BatchStage.VALIDATED,
            attempt_id="attempt-2",
            started_at=NOW + timedelta(minutes=4),
        )
    retried = start_batch_stage(
        failed,
        BatchStage.VALIDATED,
        attempt_id="attempt-3",
        started_at=NOW + timedelta(minutes=4),
    )
    assert retried.current_attempt_id == "attempt-3"
    assert retried.failed_attempts[0].attempt_id == "attempt-2"


def test_next_stage_must_start_at_or_after_previous_stage_completion():
    received = complete_stage(batch_record(), BatchStage.RECEIVED)
    valid = start_batch_stage(
        received,
        BatchStage.VALIDATED,
        attempt_id="attempt-2",
        started_at=NOW + timedelta(minutes=1),
    )
    assert valid.current_stage is BatchStage.VALIDATED

    with pytest.raises(ValueError, match="before the prior stage completes"):
        start_batch_stage(
            received,
            BatchStage.VALIDATED,
            attempt_id="attempt-2",
            started_at=NOW + timedelta(seconds=59),
        )


def test_batchrecord_rejects_nonchronological_stage_metadata_and_active_stage():
    received = complete_stage(batch_record(), BatchStage.RECEIVED)
    reversed_stage_metadata = StageMetadata(
        BatchStage.VALIDATED,
        NOW,
        NOW + timedelta(minutes=2),
    )
    with pytest.raises(ValueError, match="prior stage completes"):
        replace(
            received,
            last_completed_stage=BatchStage.VALIDATED,
            stage_metadata=(*received.stage_metadata, reversed_stage_metadata),
            current_attempt_status=AttemptStatus.RUNNING,
            current_attempt_id="attempt-2",
        )

    active_backwards = start_batch_stage(
        received,
        BatchStage.VALIDATED,
        attempt_id="attempt-2",
        started_at=NOW + timedelta(minutes=1),
    )
    with pytest.raises(ValueError, match="prior stage completes"):
        replace(active_backwards, current_stage_started_at=NOW)


@pytest.mark.parametrize(
    "factory",
    [
        lambda naive: StageMetadata(BatchStage.RECEIVED, naive, naive),
        lambda naive: ErrorMetadata("Error", "message", naive, True),
        lambda naive: IcebergCommitReference("catalog.db.table", "opaque-ref", naive),
        lambda naive: FailedAttemptMetadata(
            "attempt",
            BatchStage.RECEIVED,
            naive,
            naive,
            ErrorMetadata("Error", "message", NOW, True),
        ),
    ],
)
def test_timestamp_models_reject_naive_datetimes(factory):
    with pytest.raises(ValueError, match="timezone-aware"):
        factory(datetime(2025, 1, 1))


class MalformedTimezone(tzinfo):
    def utcoffset(self, dt):
        return "not a timedelta"


def test_malformed_timezone_implementation_raises_meaningful_value_error():
    malformed = datetime(2025, 1, 1, tzinfo=MalformedTimezone())
    with pytest.raises(ValueError, match="occurred_at must have a valid timezone"):
        ErrorMetadata("Error", "message", malformed, True)


@pytest.mark.parametrize(
    ("error_type", "message", "occurred_at", "retryable", "exception"),
    [
        (None, "message", NOW, True, TypeError),
        ("Error", None, NOW, True, TypeError),
        ("Error", "message", "2025-01-01T12:00:00Z", True, TypeError),
        ("Error", "message", NOW, 1, TypeError),
        ("  ", "message", NOW, True, ValueError),
    ],
)
def test_error_metadata_validates_field_types(
    error_type, message, occurred_at, retryable, exception
):
    with pytest.raises(exception):
        ErrorMetadata(error_type, message, occurred_at, retryable)


@pytest.mark.parametrize(
    ("error_time", "valid"),
    [
        (NOW - timedelta(microseconds=1), False),
        (NOW, True),
        (NOW + timedelta(seconds=1), True),
        (NOW + timedelta(minutes=1), True),
        (NOW + timedelta(minutes=1, microseconds=1), False),
    ],
)
def test_error_occurred_time_must_fall_within_inclusive_attempt_interval(
    error_time, valid
):
    started_at = NOW
    failed_at = NOW + timedelta(minutes=1)
    error = ErrorMetadata("Error", "message", error_time, True)
    attempt_values = {
        "attempt_id": "attempt-1",
        "failed_stage": BatchStage.VALIDATED,
        "started_at": started_at,
        "failed_at": failed_at,
        "error": error,
    }
    if valid:
        attempt = FailedAttemptMetadata(**attempt_values)
        assert attempt.error.occurred_at == error_time
    else:
        with pytest.raises(ValueError, match="between started_at and failed_at"):
            FailedAttemptMetadata(**attempt_values)


def test_utc_and_non_utc_timestamps_are_accepted_and_normalized():
    stage = StageMetadata(
        BatchStage.RECEIVED,
        datetime(2025, 1, 1, 17, 30, tzinfo=OFFSET),
        datetime(2025, 1, 1, 12, 1, tzinfo=UTC),
    )
    assert stage.started_at == NOW
    assert stage.completed_at == NOW + timedelta(minutes=1)
    assert stage.started_at.tzinfo is UTC


def test_timestamp_comparison_uses_normalized_instants_across_offsets():
    stage = StageMetadata(
        BatchStage.RECEIVED,
        datetime(2025, 1, 1, 17, 30, tzinfo=OFFSET),
        datetime(2025, 1, 1, 12, 30, tzinfo=UTC),
    )
    assert stage.completed_at > stage.started_at
    with pytest.raises(ValueError, match="cannot precede"):
        StageMetadata(
            BatchStage.RECEIVED,
            datetime(2025, 1, 1, 17, 30, tzinfo=OFFSET),
            datetime(2025, 1, 1, 11, 59, tzinfo=UTC),
        )


def test_batchrecord_rejects_inconsistent_lifecycle_combinations():
    base = batch_record()
    with pytest.raises(ValueError, match="NOT_STARTED"):
        replace(base, current_stage=BatchStage.RECEIVED)
    with pytest.raises(ValueError, match="SUCCEEDED requires COMPLETED"):
        replace(base, current_attempt_status=AttemptStatus.SUCCEEDED)

    receiving = start_batch_stage(
        base, BatchStage.RECEIVED, attempt_id="attempt-1", started_at=NOW
    )
    with pytest.raises(ValueError, match="active, started stage"):
        advance_batch_stage(
            receiving, BatchStage.COMPLETED, completed_at=NOW + timedelta(minutes=1)
        )


def test_succeeded_requires_completed_stage_and_all_stage_metadata():
    base = batch_record()
    with pytest.raises(ValueError, match="SUCCEEDED requires COMPLETED"):
        replace(base, current_attempt_status=AttemptStatus.SUCCEEDED)

    completed = base
    for index, stage in enumerate(BatchStage):
        completed = complete_stage(
            completed,
            stage,
            attempt_id=f"completion-{index}",
            started_at=NOW + timedelta(minutes=index * 2),
        )
    incomplete_metadata = completed.stage_metadata[:-1]
    with pytest.raises(ValueError, match="Completed stage COMPLETED is missing stage metadata"):
        replace(completed, stage_metadata=incomplete_metadata)


def test_completed_stage_cannot_be_claimed_without_all_required_stages():
    with pytest.raises(ValueError, match="Completed stage RECEIVED is missing stage metadata"):
        replace(batch_record(), last_completed_stage=BatchStage.COMPLETED)


def test_not_started_cannot_have_an_active_current_stage():
    with pytest.raises(ValueError, match="NOT_STARTED"):
        replace(batch_record(), current_stage=BatchStage.RECEIVED)


def test_batch_cannot_restart_after_all_required_stages_complete():
    batch = batch_record()
    for index, stage in enumerate(BatchStage):
        batch = complete_stage(
            batch,
            stage,
            attempt_id=f"attempt-{index}",
            started_at=NOW + timedelta(minutes=index * 2),
        )
    assert batch.last_completed_stage is BatchStage.COMPLETED
    assert batch.current_attempt_status is AttemptStatus.SUCCEEDED
    with pytest.raises(ValueError, match="completed batch"):
        start_batch_stage(
            batch,
            BatchStage.COMPLETED,
            attempt_id="restart",
            started_at=NOW + timedelta(hours=1),
        )
    with pytest.raises(ValueError, match="Invalid batch stage transition"):
        validate_stage_transition(BatchStage.COMPLETED, BatchStage.RECEIVED)


def test_failed_attempt_cannot_advance_last_successful_stage():
    received = complete_stage(batch_record(), BatchStage.RECEIVED)
    failed = fail_current_stage(
        received,
        "attempt-2",
        NOW + timedelta(minutes=2),
        NOW + timedelta(minutes=3),
    )
    falsely_completed = StageMetadata(
        BatchStage.VALIDATED,
        NOW + timedelta(minutes=2),
        NOW + timedelta(minutes=3),
    )

    with pytest.raises(ValueError, match="Invalid batch stage transition"):
        replace(
            failed,
            last_completed_stage=BatchStage.VALIDATED,
            stage_metadata=(*failed.stage_metadata, falsely_completed),
        )


def test_collection_inputs_are_normalized_and_cannot_mutate_batchrecord():
    received = complete_stage(batch_record(), BatchStage.RECEIVED)
    mutable_stage_metadata = list(received.stage_metadata)
    mutable_failures = []
    immutable = replace(
        received,
        stage_metadata=mutable_stage_metadata,
        failed_attempts=mutable_failures,
    )
    mutable_stage_metadata.clear()
    mutable_failures.append("later mutation")
    assert isinstance(immutable.stage_metadata, tuple)
    assert len(immutable.stage_metadata) == 1
    assert immutable.failed_attempts == ()


def test_collection_models_reject_wrong_element_types():
    with pytest.raises(TypeError, match="StageMetadata"):
        replace(batch_record(), stage_metadata=["mutable bad value"])


def test_processing_counts_keep_reject_separate_from_drop():
    counts = ProcessingCounts(raw_count=148, valid_count=133, reject_count=15, drop_count=12)
    assert counts.reject_count == 15
    assert counts.drop_count == 12
    with pytest.raises(ValueError, match="non-negative integer"):
        ProcessingCounts(reject_count=-1)


def test_iceberg_snapshot_reference_is_opaque_but_nonempty():
    ref = IcebergCommitReference("catalog.db.table", "opaque / snapshot reference", NOW)
    assert ref.snapshot_id == "opaque / snapshot reference"
    with pytest.raises(ValueError, match="snapshot_id"):
        IcebergCommitReference("catalog.db.table", " \t ", NOW)
    with pytest.raises(ValueError, match="table_identifier"):
        IcebergCommitReference(" \t ", "opaque", NOW)


def test_iceberg_commit_reference_requires_curated_committed_progress():
    ref = IcebergCommitReference("catalog.db.table", "opaque", NOW)
    with pytest.raises(ValueError, match="requires CURATED_COMMITTED"):
        batch_record(iceberg_commit_reference=ref)


def test_processing_count_types_are_strict():
    with pytest.raises(ValueError, match="non-negative integer"):
        ProcessingCounts(drop_count=True)
