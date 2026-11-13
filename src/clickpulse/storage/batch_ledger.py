"""Backend-independent batch identity and processing-state domain models.

This module deliberately provides no persistence or concurrency backend.
"""

import hashlib
import re
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from enum import Enum
from typing import TypeVar

try:
    from enum import StrEnum
except ImportError:  # Python 3.10 target runtime
    class StrEnum(str, Enum):
        """Small StrEnum compatibility shim for Python 3.10."""

        def __str__(self) -> str:
            return self.value


class BatchStage(StrEnum):
    """Successfully completed processing milestones, in required order."""

    RECEIVED = "RECEIVED"
    VALIDATED = "VALIDATED"
    DEDUPLICATED = "DEDUPLICATED"
    CURATED_COMMITTED = "CURATED_COMMITTED"
    COMPLETED = "COMPLETED"


class AttemptStatus(StrEnum):
    """Status of the current processing attempt, independent of batch progress."""

    NOT_STARTED = "NOT_STARTED"
    RUNNING = "RUNNING"
    FAILED = "FAILED"
    SUCCEEDED = "SUCCEEDED"


@dataclass(frozen=True)
class SourceIdentity:
    """Identity of a source location; content identity is stored separately."""

    source_name: str
    source_uri: str | None = None

    def __post_init__(self) -> None:
        _require_text(self.source_name, "source_name")
        if self.source_uri is not None:
            _require_text(self.source_uri, "source_uri")


@dataclass(frozen=True)
class ProcessingCounts:
    """Cumulative pipeline counts; rejected occurrences and upsert drops differ."""

    raw_count: int = 0
    valid_count: int = 0
    reject_count: int = 0
    exact_duplicate_count: int = 0
    conflict_resolution_count: int = 0
    insert_count: int = 0
    update_count: int = 0
    drop_count: int = 0

    def __post_init__(self) -> None:
        for name, value in self.__dict__.items():
            if type(value) is not int or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")


@dataclass(frozen=True)
class ErrorMetadata:
    """Error details; callers are responsible for sanitizing messages before construction."""

    error_type: str
    message: str
    occurred_at: datetime
    retryable: bool

    def __post_init__(self) -> None:
        _require_text(self.error_type, "error_type")
        if not isinstance(self.message, str):
            raise TypeError("message must be a string")
        object.__setattr__(self, "occurred_at", _normalize_utc(self.occurred_at, "occurred_at"))
        if not isinstance(self.retryable, bool):
            raise TypeError("retryable must be a bool")


@dataclass(frozen=True)
class FailedAttemptMetadata:
    """Immutable timing and error evidence for one failed stage attempt.

    ``error.occurred_at`` may equal either boundary and must fall within the
    inclusive interval from ``started_at`` through ``failed_at``.
    """

    attempt_id: str
    failed_stage: BatchStage
    started_at: datetime
    failed_at: datetime
    error: ErrorMetadata

    def __post_init__(self) -> None:
        _require_text(self.attempt_id, "attempt_id")
        if not isinstance(self.failed_stage, BatchStage):
            raise TypeError("failed_stage must be a BatchStage")
        started_at = _normalize_utc(self.started_at, "started_at")
        failed_at = _normalize_utc(self.failed_at, "failed_at")
        if failed_at < started_at:
            raise ValueError("failed_at cannot precede started_at")
        if not isinstance(self.error, ErrorMetadata):
            raise TypeError("error must be ErrorMetadata")
        if not started_at <= self.error.occurred_at <= failed_at:
            raise ValueError("error.occurred_at must be between started_at and failed_at")
        object.__setattr__(self, "started_at", started_at)
        object.__setattr__(self, "failed_at", failed_at)


@dataclass(frozen=True)
class StageMetadata:
    """Timing and optional count snapshot for one successfully completed stage."""

    stage: BatchStage
    started_at: datetime
    completed_at: datetime
    counts: ProcessingCounts | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.stage, BatchStage):
            raise TypeError("stage must be a BatchStage")
        started_at = _normalize_utc(self.started_at, "started_at")
        completed_at = _normalize_utc(self.completed_at, "completed_at")
        if completed_at < started_at:
            raise ValueError("completed_at cannot precede started_at")
        object.__setattr__(self, "started_at", started_at)
        object.__setattr__(self, "completed_at", completed_at)
        if self.counts is not None and not isinstance(self.counts, ProcessingCounts):
            raise TypeError("counts must be ProcessingCounts or None")


@dataclass(frozen=True)
class IcebergCommitReference:
    """Opaque snapshot reference; its format awaits the runtime contract."""

    table_identifier: str
    snapshot_id: str
    committed_at: datetime

    def __post_init__(self) -> None:
        _require_text(self.table_identifier, "table_identifier")
        _require_text(self.snapshot_id, "snapshot_id")
        object.__setattr__(
            self, "committed_at", _normalize_utc(self.committed_at, "committed_at")
        )


@dataclass(frozen=True)
class BatchRecord:
    """Domain state for one content-identified source batch."""

    batch_id: str
    source_identity: SourceIdentity
    checksum: str
    last_completed_stage: BatchStage | None = None
    current_attempt_status: AttemptStatus = AttemptStatus.NOT_STARTED
    current_stage: BatchStage | None = None
    current_attempt_id: str | None = None
    current_stage_started_at: datetime | None = None
    stage_metadata: tuple[StageMetadata, ...] = ()
    processing_counts: ProcessingCounts = ProcessingCounts()
    failed_attempts: tuple[FailedAttemptMetadata, ...] = ()
    iceberg_commit_reference: IcebergCommitReference | None = None

    def __post_init__(self) -> None:
        _validate_sha256(self.batch_id, "batch_id")
        _validate_sha256(self.checksum, "checksum")
        if self.batch_id != self.checksum:
            raise ValueError("batch_id must equal checksum for content-identified batches")
        if not isinstance(self.source_identity, SourceIdentity):
            raise TypeError("source_identity must be a SourceIdentity")
        if not isinstance(self.current_attempt_status, AttemptStatus):
            raise TypeError("current_attempt_status must be an AttemptStatus")
        if self.last_completed_stage is not None and not isinstance(
            self.last_completed_stage, BatchStage
        ):
            raise TypeError("last_completed_stage must be a BatchStage or None")
        if self.current_stage is not None and not isinstance(self.current_stage, BatchStage):
            raise TypeError("current_stage must be a BatchStage or None")
        if self.current_attempt_id is not None:
            _require_text(self.current_attempt_id, "current_attempt_id")
        if self.current_stage_started_at is not None:
            object.__setattr__(
                self,
                "current_stage_started_at",
                _normalize_utc(self.current_stage_started_at, "current_stage_started_at"),
            )

        stage_metadata = _immutable_tuple(
            self.stage_metadata, StageMetadata, "stage_metadata"
        )
        failed_attempts = _immutable_tuple(
            self.failed_attempts, FailedAttemptMetadata, "failed_attempts"
        )
        object.__setattr__(self, "stage_metadata", stage_metadata)
        object.__setattr__(self, "failed_attempts", failed_attempts)
        if not isinstance(self.processing_counts, ProcessingCounts):
            raise TypeError("processing_counts must be ProcessingCounts")
        if self.iceberg_commit_reference is not None and not isinstance(
            self.iceberg_commit_reference, IcebergCommitReference
        ):
            raise TypeError("iceberg_commit_reference must be IcebergCommitReference or None")

        metadata_by_stage = {item.stage: item for item in stage_metadata}
        if len(metadata_by_stage) != len(stage_metadata):
            raise ValueError("stage_metadata cannot contain duplicate stages")
        last_index = (
            -1
            if self.last_completed_stage is None
            else _STAGE_ORDER.index(self.last_completed_stage)
        )
        for stage in _STAGE_ORDER[: last_index + 1]:
            if stage not in metadata_by_stage:
                raise ValueError(f"Completed stage {stage.value} is missing stage metadata")
        if any(_STAGE_ORDER.index(item.stage) > last_index for item in stage_metadata):
            raise ValueError("stage_metadata cannot include an incomplete stage")
        previous_metadata = None
        for stage in _STAGE_ORDER[: last_index + 1]:
            metadata = metadata_by_stage[stage]
            if (
                previous_metadata is not None
                and metadata.started_at < previous_metadata.completed_at
            ):
                raise ValueError("A stage cannot start before the prior stage completes")
            previous_metadata = metadata
        failed_attempt_ids = [item.attempt_id for item in failed_attempts]
        if len(failed_attempt_ids) != len(set(failed_attempt_ids)):
            raise ValueError("failed-attempt IDs must be unique")

        self._validate_state(metadata_by_stage)

    def _validate_state(self, metadata_by_stage: dict[BatchStage, StageMetadata]) -> None:
        status = self.current_attempt_status
        if status is AttemptStatus.NOT_STARTED:
            if (
                self.last_completed_stage is not None
                or self.current_stage is not None
                or self.current_attempt_id is not None
                or self.current_stage_started_at is not None
                or self.failed_attempts
                or self.stage_metadata
            ):
                raise ValueError("NOT_STARTED batches cannot contain attempt or progress state")
        elif status is AttemptStatus.RUNNING:
            if not self.current_attempt_id:
                raise ValueError("RUNNING batches require current_attempt_id")
            if self.current_attempt_id in {item.attempt_id for item in self.failed_attempts}:
                raise ValueError("A new attempt ID cannot reuse failed-attempt history")
            if self.current_stage is None:
                if self.current_stage_started_at is not None:
                    raise ValueError("No current stage may have a current-stage start time")
                if self.last_completed_stage in (None, BatchStage.COMPLETED):
                    raise ValueError("RUNNING without a current stage requires prior progress")
            else:
                validate_stage_transition(self.last_completed_stage, self.current_stage)
                if self.current_stage_started_at is None:
                    raise ValueError("RUNNING current stage requires current_stage_started_at")
                self._validate_current_stage_chronology(
                    metadata_by_stage, self.current_stage_started_at
                )
        elif status is AttemptStatus.FAILED:
            if self.current_stage is None or not self.current_attempt_id:
                raise ValueError("FAILED batches require a failed current stage and attempt ID")
            if self.current_stage_started_at is not None:
                raise ValueError("Failed stage timing belongs in failed_attempts")
            validate_stage_transition(self.last_completed_stage, self.current_stage)
            if not self.failed_attempts:
                raise ValueError("FAILED batches require failed-attempt metadata")
            latest_failure = self.failed_attempts[-1]
            if (
                latest_failure.attempt_id != self.current_attempt_id
                or latest_failure.failed_stage is not self.current_stage
            ):
                raise ValueError("Latest failed attempt must match the current failed stage")
            self._validate_current_stage_chronology(metadata_by_stage, latest_failure.started_at)
        elif status is AttemptStatus.SUCCEEDED:
            if (
                self.last_completed_stage is not BatchStage.COMPLETED
                or self.current_stage is not None
                or self.current_stage_started_at is not None
                or not self.current_attempt_id
            ):
                raise ValueError("SUCCEEDED requires COMPLETED and no active stage")

        if (
            self.last_completed_stage is BatchStage.COMPLETED
            and status is not AttemptStatus.SUCCEEDED
        ):
            raise ValueError("COMPLETED batches must have SUCCEEDED attempt status")
        if self.iceberg_commit_reference is not None and (
            self.last_completed_stage is None
            or _STAGE_ORDER.index(self.last_completed_stage)
            < _STAGE_ORDER.index(BatchStage.CURATED_COMMITTED)
        ):
            raise ValueError("Iceberg commit reference requires CURATED_COMMITTED progress")

    def _validate_current_stage_chronology(
        self,
        metadata_by_stage: dict[BatchStage, StageMetadata],
        current_started_at: datetime,
    ) -> None:
        if self.last_completed_stage is None:
            return
        previous = metadata_by_stage[self.last_completed_stage]
        if current_started_at < previous.completed_at:
            raise ValueError("A stage cannot start before the prior stage completes")


_STAGE_ORDER = tuple(BatchStage)
_T = TypeVar("_T")
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")


def _require_text(value: str, field_name: str) -> None:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    if not value.strip():
        raise ValueError(f"{field_name} must not be empty or whitespace-only")


def _validate_sha256(value: str, field_name: str) -> None:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    if _SHA256_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be 64 lowercase hexadecimal characters")
    decoded = bytes.fromhex(value)
    if len(decoded) != 32:
        raise ValueError(f"{field_name} must decode to exactly 32 bytes")


def _normalize_utc(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime")
    try:
        offset = value.utcoffset()
    except (OverflowError, TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must have a valid timezone") from exc
    if value.tzinfo is None or offset is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    try:
        return value.astimezone(timezone.utc)
    except (OverflowError, TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} cannot be normalized to UTC") from exc


def _immutable_tuple(
    values: tuple[_T, ...], item_type: type[_T], field_name: str
) -> tuple[_T, ...]:
    if isinstance(values, (str, bytes, dict)):
        raise TypeError(f"{field_name} must be a collection of {item_type.__name__}")
    try:
        frozen_values = tuple(values)
    except TypeError as exc:
        raise TypeError(f"{field_name} must be iterable") from exc
    if any(not isinstance(item, item_type) for item in frozen_values):
        raise TypeError(f"{field_name} must contain only {item_type.__name__} values")
    return frozen_values


def compute_batch_id(original_bytes: bytes) -> str:
    """Return SHA-256 hex digest of exact source bytes, without normalization."""
    if not isinstance(original_bytes, bytes):
        raise TypeError("original_bytes must be bytes")
    return hashlib.sha256(original_bytes).hexdigest()


def get_batch_id(file_bytes: bytes) -> str:
    """Compatibility wrapper for existing callers; use ``compute_batch_id``."""
    return compute_batch_id(file_bytes)


def validate_stage_transition(
    last_completed_stage: BatchStage | None,
    target_stage: BatchStage,
) -> None:
    """Raise unless target_stage is exactly the next stage in the state machine."""
    if last_completed_stage is not None and not isinstance(last_completed_stage, BatchStage):
        raise TypeError("last_completed_stage must be a BatchStage or None")
    if not isinstance(target_stage, BatchStage):
        raise TypeError("target_stage must be a BatchStage")
    next_index = (
        0
        if last_completed_stage is None
        else _STAGE_ORDER.index(last_completed_stage) + 1
    )
    if next_index >= len(_STAGE_ORDER) or _STAGE_ORDER[next_index] is not target_stage:
        expected = (
            _STAGE_ORDER[next_index].value
            if next_index < len(_STAGE_ORDER)
            else "no next stage"
        )
        raise ValueError(f"Invalid batch stage transition; expected {expected}")


def _replace_stage_metadata(
    existing: tuple[StageMetadata, ...], new_metadata: StageMetadata
) -> tuple[StageMetadata, ...]:
    by_stage = {metadata.stage: metadata for metadata in existing}
    by_stage[new_metadata.stage] = new_metadata
    return tuple(by_stage[stage] for stage in _STAGE_ORDER if stage in by_stage)


def start_batch_stage(
    batch: BatchRecord,
    stage: BatchStage,
    *,
    attempt_id: str,
    started_at: datetime,
) -> BatchRecord:
    """Begin exactly the next stage while retaining completed and failed history."""
    if (
        batch.current_attempt_status is AttemptStatus.RUNNING
        and batch.current_stage is not None
    ):
        raise ValueError("Cannot start a stage while an attempt is already running")
    if batch.current_attempt_status is AttemptStatus.SUCCEEDED:
        raise ValueError("A completed batch cannot start another stage")
    validate_stage_transition(batch.last_completed_stage, stage)
    _require_text(attempt_id, "attempt_id")
    if attempt_id in {item.attempt_id for item in batch.failed_attempts}:
        raise ValueError("attempt_id must be unique and cannot reuse failed-attempt history")
    started_at = _normalize_utc(started_at, "started_at")
    if batch.last_completed_stage is not None:
        previous = next(
            item
            for item in batch.stage_metadata
            if item.stage is batch.last_completed_stage
        )
        if started_at < previous.completed_at:
            raise ValueError("A stage cannot start before the prior stage completes")
    return replace(
        batch,
        current_attempt_status=AttemptStatus.RUNNING,
        current_stage=stage,
        current_attempt_id=attempt_id,
        current_stage_started_at=started_at,
    )


def advance_batch_stage(
    batch: BatchRecord,
    stage: BatchStage,
    *,
    completed_at: datetime,
    counts: ProcessingCounts | None = None,
    iceberg_commit_reference: IcebergCommitReference | None = None,
) -> BatchRecord:
    """Complete the active stage, rejecting skipped or backwards transitions."""
    if batch.current_attempt_status is not AttemptStatus.RUNNING:
        raise ValueError("Cannot complete a stage when no attempt is running")
    if batch.current_stage is not stage or batch.current_stage_started_at is None:
        raise ValueError("Completed stage must match an active, started stage")
    validate_stage_transition(batch.last_completed_stage, stage)
    completed_at = _normalize_utc(completed_at, "completed_at")
    metadata = StageMetadata(
        stage=stage,
        started_at=batch.current_stage_started_at,
        completed_at=completed_at,
        counts=counts,
    )
    if counts is not None and not isinstance(counts, ProcessingCounts):
        raise TypeError("counts must be ProcessingCounts or None")
    if iceberg_commit_reference is not None and not isinstance(
        iceberg_commit_reference, IcebergCommitReference
    ):
        raise TypeError("iceberg_commit_reference must be IcebergCommitReference or None")
    completed_status = (
        AttemptStatus.SUCCEEDED if stage is BatchStage.COMPLETED else AttemptStatus.RUNNING
    )
    return replace(
        batch,
        last_completed_stage=stage,
        current_attempt_status=completed_status,
        current_stage=None,
        current_stage_started_at=None,
        stage_metadata=_replace_stage_metadata(batch.stage_metadata, metadata),
        processing_counts=counts if counts is not None else batch.processing_counts,
        iceberg_commit_reference=(
            iceberg_commit_reference
            if iceberg_commit_reference is not None
            else batch.iceberg_commit_reference
        ),
    )


def fail_batch_attempt(
    batch: BatchRecord,
    error: ErrorMetadata,
    *,
    failed_at: datetime | None = None,
) -> BatchRecord:
    """Record a failed attempt without advancing completed stage progress.

    Error messages must be sanitized by the caller before ``ErrorMetadata`` is built.
    """
    if batch.current_attempt_status is not AttemptStatus.RUNNING:
        raise ValueError("Cannot fail a batch when no attempt is running")
    if (
        batch.current_stage is None
        or batch.current_attempt_id is None
        or batch.current_stage_started_at is None
    ):
        raise ValueError("Running attempt is missing its stage, ID, or start timestamp")
    if not isinstance(error, ErrorMetadata):
        raise TypeError("error must be ErrorMetadata")
    normalized_failed_at = _normalize_utc(
        failed_at if failed_at is not None else error.occurred_at, "failed_at"
    )
    if normalized_failed_at < error.occurred_at:
        raise ValueError("failed_at cannot precede error.occurred_at")
    failure = FailedAttemptMetadata(
        attempt_id=batch.current_attempt_id,
        failed_stage=batch.current_stage,
        started_at=batch.current_stage_started_at,
        failed_at=normalized_failed_at,
        error=error,
    )
    return replace(
        batch,
        current_attempt_status=AttemptStatus.FAILED,
        current_stage_started_at=None,
        failed_attempts=(*batch.failed_attempts, failure),
    )


def is_batch_completed(batch_id: str) -> bool:
    """Persistence-dependent lookup is intentionally unavailable in the domain layer."""
    raise NotImplementedError("Batch completion lookup requires a configured persistence backend")
