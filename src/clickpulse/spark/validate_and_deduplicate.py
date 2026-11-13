"""Pure within-batch deduplication rules for validated ClickPulse records."""

import json
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from clickpulse.common.validation import RawOccurrence, parse_occurrence, validate_event


@dataclass(frozen=True)
class ValidatedRecord:
    """An already-validated record paired with its original input occurrence."""

    record: Mapping[str, Any]
    occurrence: RawOccurrence


@dataclass(frozen=True)
class DeduplicatedRecord:
    """A selected event payload and all occurrences with identical content."""

    record: Mapping[str, Any]
    occurrences: tuple[RawOccurrence, ...]


@dataclass(frozen=True)
class UnresolvedTie:
    """Distinct latest record versions that share the greatest updated_at."""

    event_id: str
    updated_at: str
    candidates: tuple[DeduplicatedRecord, ...]


@dataclass(frozen=True)
class DeduplicationMetrics:
    valid_input_count: int
    exact_duplicate_excess: int
    unique_event_id_count: int
    repeated_event_id_groups: int
    conflicting_event_id_groups: int
    resolved_record_count: int


@dataclass(frozen=True)
class DeduplicationResult:
    winners: tuple[DeduplicatedRecord, ...]
    unresolved_ties: tuple[UnresolvedTie, ...]
    metrics: DeduplicationMetrics
    audit_records: tuple[ValidatedRecord, ...]


@dataclass(frozen=True)
class SparkValidationMetrics:
    """Counts calculated from the distributed occurrence DataFrame."""

    raw_line_count: int
    malformed_line_count: int
    valid_record_count: int
    rejected_occurrence_count: int


@dataclass(frozen=True)
class SparkValidationResult:
    """Validated and rejected DataFrames plus aggregate occurrence counts."""

    validated_records: Any
    rejected_occurrences: Any
    metrics: SparkValidationMetrics


@dataclass(frozen=True)
class SparkDeduplicationMetrics:
    """Deterministic metrics calculated by Spark aggregations."""

    valid_input_count: int
    exact_duplicate_excess: int
    unique_event_id_count: int
    repeated_event_id_groups: int
    conflicting_event_id_groups: int
    resolved_record_count: int
    unresolved_latest_timestamp_tie_count: int


@dataclass(frozen=True)
class SparkDeduplicationResult:
    """Distributed winners and unresolved latest-timestamp tie identifiers."""

    winners: Any
    unresolved_ties: Any
    metrics: SparkDeduplicationMetrics


@dataclass(frozen=True)
class SparkValidationDeduplicationResult:
    """Full Spark validation/deduplication output; no Iceberg dependency."""

    validated_records: Any
    rejected_occurrences: Any
    winners: Any
    unresolved_ties: Any
    validation_metrics: SparkValidationMetrics
    deduplication_metrics: SparkDeduplicationMetrics


def _canonical_value(value: Any) -> Any:
    """Convert JSON-like values to stable, type-aware content for comparisons."""
    if isinstance(value, Decimal):
        return {"__decimal__": str(value)}
    if isinstance(value, dict):
        return {key: _canonical_value(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return [_canonical_value(item) for item in value]
    if value is None or type(value) in (str, int, float, bool):
        return value
    raise TypeError(f"Validated record contains unsupported value type: {type(value).__name__}")


def _record_key(record: Mapping[str, Any]) -> str:
    return json.dumps(_canonical_value(dict(record)), sort_keys=True, separators=(",", ":"))


def _timestamp(value: str) -> datetime:
    """Parse a validated timezone-aware ISO timestamp for chronological comparison."""
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _occurrence_key(occurrence: RawOccurrence) -> tuple[str, int, str]:
    return occurrence.source_file, occurrence.source_line_number, occurrence.raw_line


def _record_sort_key(
    record: DeduplicatedRecord,
) -> tuple[str, tuple[str, int, str]]:
    return _record_key(record.record), min(map(_occurrence_key, record.occurrences))


def deduplicate_validated_records(
    records: Iterable[ValidatedRecord],
) -> DeduplicationResult:
    """Collapse exact duplicates and resolve each event ID by greatest updated_at.

    A tie between different content at the greatest timestamp is exposed in
    ``unresolved_ties`` and produces no winner for that event ID.
    """
    inputs = list(records)
    if any(not isinstance(item, ValidatedRecord) for item in inputs):
        raise TypeError("records must contain only ValidatedRecord instances")

    by_event_id: dict[str, list[ValidatedRecord]] = defaultdict(list)
    for item in inputs:
        by_event_id[item.record["event_id"]].append(item)

    winners: list[DeduplicatedRecord] = []
    unresolved: list[UnresolvedTie] = []
    exact_duplicate_excess = 0
    repeated_groups = 0
    conflicting_groups = 0

    for event_id in sorted(by_event_id):
        occurrences = by_event_id[event_id]
        if len(occurrences) > 1:
            repeated_groups += 1

        exact_groups: dict[str, list[ValidatedRecord]] = defaultdict(list)
        for item in occurrences:
            exact_groups[_record_key(item.record)].append(item)
        exact_duplicate_excess += len(occurrences) - len(exact_groups)

        versions = [
            DeduplicatedRecord(
                record=group[0].record,
                occurrences=tuple(sorted((item.occurrence for item in group), key=_occurrence_key)),
            )
            for group in exact_groups.values()
        ]
        versions.sort(key=_record_sort_key)
        if len(versions) > 1:
            conflicting_groups += 1

        greatest = max(_timestamp(version.record["updated_at"]) for version in versions)
        latest = [
            version
            for version in versions
            if _timestamp(version.record["updated_at"]) == greatest
        ]
        if len(latest) > 1:
            unresolved.append(
                UnresolvedTie(
                    event_id=event_id,
                    updated_at=str(latest[0].record["updated_at"]),
                    candidates=tuple(sorted(latest, key=_record_sort_key)),
                )
            )
        else:
            winners.append(latest[0])

    metrics = DeduplicationMetrics(
        valid_input_count=len(inputs),
        exact_duplicate_excess=exact_duplicate_excess,
        unique_event_id_count=len(by_event_id),
        repeated_event_id_groups=repeated_groups,
        conflicting_event_id_groups=conflicting_groups,
        resolved_record_count=len(winners),
    )
    audit_records = tuple(
        sorted(
            inputs,
            key=lambda item: (
                item.record["event_id"],
                _record_key(item.record),
                _occurrence_key(item.occurrence),
            ),
        )
    )
    return DeduplicationResult(
        winners=tuple(sorted(winners, key=lambda item: item.record["event_id"])),
        unresolved_ties=tuple(unresolved),
        metrics=metrics,
        audit_records=audit_records,
    )


def _spark_raw_occurrence_schema():
    """Return an explicit schema for line-oriented Spark input."""
    from pyspark.sql.types import LongType, StringType, StructField, StructType

    return StructType(
        [
            StructField("raw_line", StringType(), nullable=False),
            StructField("source_file", StringType(), nullable=False),
            StructField("source_line_number", LongType(), nullable=False),
        ]
    )


def _spark_validation_schema():
    """Return the explicit worker-UDF output schema."""
    from pyspark.sql.types import (
        ArrayType,
        BooleanType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    return StructType(
        [
            StructField("is_malformed_json", BooleanType(), nullable=False),
            StructField("event_id", StringType(), nullable=True),
            StructField("event_time", StringType(), nullable=True),
            StructField("updated_at", StringType(), nullable=True),
            StructField("updated_at_epoch_microseconds", LongType(), nullable=True),
            StructField("canonical_record_key", StringType(), nullable=True),
            StructField("rejection_reasons", ArrayType(StringType(), containsNull=False), False),
        ]
    )


def read_jsonl_occurrences(spark: Any, input_paths: str | Iterable[str]):
    """Read JSONL lines into a DataFrame with source path and 1-based line number.

    Each input must identify a single file. Spark's RDD ``zipWithIndex`` assigns
    stable line numbers without collecting record contents on the driver.
    Spark's text reader removes record separators; ``raw_line`` is the exact
    decoded line content excluding its LF/CRLF delimiter.
    """
    paths = [input_paths] if isinstance(input_paths, str) else list(input_paths)
    if not paths:
        raise ValueError("input_paths must contain at least one JSONL file")

    occurrence_rdds = []
    for path in paths:
        local_path = Path(path)
        if local_path.is_file():
            local_uri_path = local_path.resolve().as_posix()
            spark_path = f"file:/{local_uri_path}"
        else:
            spark_path = path

        def with_location(indexed_line, source_path=path):
            raw_line, zero_based_line_number = indexed_line
            return raw_line, source_path, int(zero_based_line_number) + 1

        occurrence_rdds.append(
            spark.sparkContext.textFile(spark_path).zipWithIndex().map(with_location)
        )

    occurrences = spark.sparkContext.union(occurrence_rdds)
    return spark.createDataFrame(occurrences, schema=_spark_raw_occurrence_schema())


def _timestamp_epoch_microseconds(value: str) -> int:
    utc_timezone = timezone.utc  # noqa: UP017 - datetime.UTC is unavailable on Python 3.10
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(utc_timezone)
    delta = parsed - datetime(1970, 1, 1, tzinfo=utc_timezone)
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def _validate_spark_occurrence(raw_line: str, source_file: str, source_line_number: int):
    """Python-worker function; applies the same parser and validator per row."""
    occurrence = RawOccurrence(raw_line, source_file, source_line_number)
    parsed = parse_occurrence(occurrence)
    reasons = (
        [parsed.parse_error]
        if parsed.parse_error is not None
        else validate_event(parsed.record)
    )
    record = parsed.record if isinstance(parsed.record, dict) else None
    updated_at = record.get("updated_at") if record is not None else None
    epoch = None
    if isinstance(updated_at, str) and _is_valid_timestamp(updated_at):
        epoch = _timestamp_epoch_microseconds(updated_at)
    record_key = _record_key(record) if record is not None else None
    event_id = record.get("event_id") if record is not None else None
    event_time = record.get("event_time") if record is not None else None
    return (
        parsed.parse_error is not None,
        event_id if isinstance(event_id, str) else None,
        event_time if isinstance(event_time, str) else None,
        updated_at if isinstance(updated_at, str) else None,
        epoch,
        record_key,
        reasons,
    )


def _is_valid_timestamp(value: str) -> bool:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.tzinfo is not None and parsed.utcoffset() is not None


def validate_occurrences_dataframe(raw_occurrences: Any) -> SparkValidationResult:
    """Validate raw occurrence rows in distributed Python-worker tasks."""
    from pyspark.sql import functions as F
    from pyspark.sql.functions import udf

    validate_udf = udf(_validate_spark_occurrence, _spark_validation_schema())
    parsed = raw_occurrences.withColumn(
        "_validation",
        validate_udf("raw_line", "source_file", "source_line_number"),
    )
    expanded = parsed.select(
        "raw_line",
        "source_file",
        "source_line_number",
        F.col("_validation.*"),
    )
    valid_condition = F.size("rejection_reasons") == 0
    validated = expanded.where(valid_condition).select(
        "raw_line",
        "source_file",
        "source_line_number",
        "event_id",
        "event_time",
        "updated_at",
        "updated_at_epoch_microseconds",
        "canonical_record_key",
    ).persist()
    rejected = (
        expanded.where(~valid_condition)
        .withColumn("processing_timestamp", F.current_timestamp())
        .select(
            "raw_line",
            "source_file",
            "source_line_number",
            "processing_timestamp",
            "rejection_reasons",
        )
        .persist()
    )
    counts = expanded.agg(
        F.count(F.lit(1)).alias("raw_line_count"),
        F.sum(F.when(F.col("is_malformed_json"), 1).otherwise(0)).alias(
            "malformed_line_count"
        ),
        F.sum(F.when(valid_condition, 1).otherwise(0)).alias("valid_record_count"),
        F.sum(F.when(~valid_condition, 1).otherwise(0)).alias("rejected_occurrence_count"),
    ).first()
    return SparkValidationResult(
        validated_records=validated,
        rejected_occurrences=rejected,
        metrics=SparkValidationMetrics(
            raw_line_count=int(counts.raw_line_count or 0),
            malformed_line_count=int(counts.malformed_line_count or 0),
            valid_record_count=int(counts.valid_record_count or 0),
            rejected_occurrence_count=int(counts.rejected_occurrence_count or 0),
        ),
    )


def deduplicate_validated_dataframe(validated_records: Any) -> SparkDeduplicationResult:
    """Deduplicate valid records with Spark aggregations and windows."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    per_id = validated_records.groupBy("event_id").agg(
        F.count(F.lit(1)).alias("input_count"),
        F.countDistinct("canonical_record_key").alias("version_count"),
        F.max("updated_at_epoch_microseconds").alias("latest_updated_at_epoch_microseconds"),
    )
    summary = per_id.agg(
        F.coalesce(F.sum("input_count"), F.lit(0)).alias("valid_input_count"),
        F.coalesce(F.sum(F.col("input_count") - F.col("version_count")), F.lit(0)).alias(
            "exact_duplicate_excess"
        ),
        F.count(F.lit(1)).alias("unique_event_id_count"),
        F.coalesce(F.sum(F.when(F.col("input_count") > 1, 1).otherwise(0)), F.lit(0)).alias(
            "repeated_event_id_groups"
        ),
        F.coalesce(F.sum(F.when(F.col("version_count") > 1, 1).otherwise(0)), F.lit(0)).alias(
            "conflicting_event_id_groups"
        ),
    )

    latest_rows = validated_records.join(
        per_id.select("event_id", "latest_updated_at_epoch_microseconds"),
        on="event_id",
        how="inner",
    ).where(
        F.col("updated_at_epoch_microseconds")
        == F.col("latest_updated_at_epoch_microseconds")
    )
    top_content_counts = latest_rows.groupBy("event_id").agg(
        F.countDistinct("canonical_record_key").alias("latest_content_count"),
        F.max("updated_at_epoch_microseconds").alias("updated_at_epoch_microseconds"),
        F.max("updated_at").alias("updated_at"),
    )
    unresolved = top_content_counts.where(F.col("latest_content_count") > 1).select(
        "event_id", "updated_at", "updated_at_epoch_microseconds", "latest_content_count"
    )
    resolved_latest = latest_rows.join(
        top_content_counts.where(F.col("latest_content_count") == 1).select("event_id"),
        on="event_id",
        how="inner",
    )
    representative_order = Window.partitionBy("event_id", "canonical_record_key").orderBy(
        F.col("source_file").asc(),
        F.col("source_line_number").asc(),
        F.col("raw_line").asc(),
    )
    winners = (
        resolved_latest.withColumn(
            "_representative_rank", F.row_number().over(representative_order)
        )
        .where(F.col("_representative_rank") == 1)
        .drop("_representative_rank", "latest_updated_at_epoch_microseconds")
        .orderBy("event_id")
    )
    tie_count = unresolved.count()
    metrics_row = summary.first()
    unique_count = int(metrics_row.unique_event_id_count or 0)
    metrics = SparkDeduplicationMetrics(
        valid_input_count=int(metrics_row.valid_input_count or 0),
        exact_duplicate_excess=int(metrics_row.exact_duplicate_excess or 0),
        unique_event_id_count=unique_count,
        repeated_event_id_groups=int(metrics_row.repeated_event_id_groups or 0),
        conflicting_event_id_groups=int(metrics_row.conflicting_event_id_groups or 0),
        resolved_record_count=unique_count - tie_count,
        unresolved_latest_timestamp_tie_count=tie_count,
    )
    return SparkDeduplicationResult(winners, unresolved, metrics)


def run_validation_and_deduplication(
    input_paths: str | Iterable[str], spark: Any | None = None
) -> SparkValidationDeduplicationResult:
    """Run distributed JSONL validation and deduplication and return output DataFrames.

    ``input_paths`` must contain individual JSONL files. No Iceberg catalog or
    table integration is required. The ``clickpulse`` package must be installed
    on workers or supplied to Spark with its standard ``--py-files`` option. A
    caller-supplied SparkSession is recommended for job lifecycle control;
    otherwise the active/default session is used.
    """
    from pyspark.sql import SparkSession

    session = spark if spark is not None else SparkSession.builder.getOrCreate()
    raw = read_jsonl_occurrences(session, input_paths)
    validation = validate_occurrences_dataframe(raw)
    deduplication = deduplicate_validated_dataframe(validation.validated_records)
    return SparkValidationDeduplicationResult(
        validated_records=validation.validated_records,
        rejected_occurrences=validation.rejected_occurrences,
        winners=deduplication.winners,
        unresolved_ties=deduplication.unresolved_ties,
        validation_metrics=validation.metrics,
        deduplication_metrics=deduplication.metrics,
    )
