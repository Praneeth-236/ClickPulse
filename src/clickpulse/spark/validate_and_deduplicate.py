"""Pure within-batch deduplication rules for validated ClickPulse records."""

import json
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from clickpulse.common.validation import RawOccurrence


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


def run_validation_and_deduplication() -> None:
    """Spark orchestration entry point, pending confirmed runtime interfaces."""
    raise NotImplementedError("Spark validation/deduplication integration is not configured")
