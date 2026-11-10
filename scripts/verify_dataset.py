"""Verify source JSONL datasets against ClickPulse validation/deduplication rules."""

import argparse
import json
import sys
from collections import Counter
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any

# Allow direct execution from a source checkout without requiring installation.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

validation_module = import_module("clickpulse.common.validation")
deduplication_module = import_module("clickpulse.spark.validate_and_deduplicate")
RawOccurrence = validation_module.RawOccurrence
parse_occurrence = validation_module.parse_occurrence
validate_event = validation_module.validate_event
DeduplicationResult = deduplication_module.DeduplicationResult
ValidatedRecord = deduplication_module.ValidatedRecord
deduplicate_validated_records = deduplication_module.deduplicate_validated_records

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DAY1_PATH = Path("data/sample/clicks_day1.jsonl")
DEFAULT_DAY2_PATH = Path("data/sample/clicks_day2.jsonl")


APPROVED_BASELINE = {
    "day1": {
        "raw_lines": 144,
        "valid_records": 128,
        "rejected_occurrences": 16,
        "exact_duplicate_excess": 6,
        "unique_event_id_count": 116,
        "repeated_event_id_groups": 11,
        "conflicting_event_id_groups": 6,
    },
    "day2": {
        "raw_lines": 148,
        "valid_records": 133,
        "rejected_occurrences": 15,
        "exact_duplicate_excess": 4,
        "unique_event_id_count": 125,
        "repeated_event_id_groups": 8,
        "conflicting_event_id_groups": 4,
    },
}


@dataclass(frozen=True)
class DatasetVerification:
    source_path: str
    raw_lines: int
    malformed_lines: int
    non_object_json_lines: int
    valid_records: int
    rejected_occurrences: int
    rejection_reason_counts: dict[str, int]
    deduplication: DeduplicationResult


def verify_dataset(path: str | Path) -> DatasetVerification:
    """Read every source occurrence and run the production validation/dedup logic."""
    source = Path(path)
    valid_records: list[ValidatedRecord] = []
    rejected = 0
    malformed = 0
    non_object = 0
    reasons: Counter[str] = Counter()
    raw_lines = 0

    with source.open("r", encoding="utf-8", newline="") as stream:
        for line_number, raw_line in enumerate(stream, start=1):
            raw_lines += 1
            occurrence = RawOccurrence(raw_line, str(source), line_number)
            parsed = parse_occurrence(occurrence)
            if parsed.parse_error:
                malformed += 1
                rejected += 1
                reasons.update([parsed.parse_error])
                continue
            if not isinstance(parsed.record, dict):
                non_object += 1
            occurrence_reasons = validate_event(parsed.record)
            if occurrence_reasons:
                rejected += 1
                reasons.update(occurrence_reasons)
                continue
            valid_records.append(ValidatedRecord(parsed.record, occurrence))

    result = deduplicate_validated_records(valid_records)
    return DatasetVerification(
        source_path=str(source.resolve()),
        raw_lines=raw_lines,
        malformed_lines=malformed,
        non_object_json_lines=non_object,
        valid_records=len(valid_records),
        rejected_occurrences=rejected,
        rejection_reason_counts=dict(sorted(reasons.items())),
        deduplication=result,
    )


def _metrics(report: DatasetVerification) -> dict[str, int]:
    return {
        "raw_lines": report.raw_lines,
        "valid_records": report.valid_records,
        "rejected_occurrences": report.rejected_occurrences,
        "exact_duplicate_excess": report.deduplication.metrics.exact_duplicate_excess,
        "unique_event_id_count": report.deduplication.metrics.unique_event_id_count,
        "repeated_event_id_groups": report.deduplication.metrics.repeated_event_id_groups,
        "conflicting_event_id_groups": report.deduplication.metrics.conflicting_event_id_groups,
    }


def _format_report(
    label: str, report: DatasetVerification, compare_baseline: bool
) -> dict[str, Any]:
    actual = _metrics(report)
    comparisons = {}
    if compare_baseline:
        baseline = APPROVED_BASELINE[label]
        comparisons = {
            metric: {
                "expected": expected,
                "actual": actual[metric],
                "matches": expected == actual[metric],
            }
            for metric, expected in baseline.items()
        }
    return {
        "dataset": label,
        "source_path": report.source_path,
        **actual,
        "malformed_lines": report.malformed_lines,
        "non_object_json_lines": report.non_object_json_lines,
        "rejection_reason_counts": report.rejection_reason_counts,
        "resolved_record_count": report.deduplication.metrics.resolved_record_count,
        "unresolved_latest_timestamp_ties": [
            {"event_id": tie.event_id, "updated_at": tie.updated_at}
            for tie in report.deduplication.unresolved_ties
        ],
        "baseline_comparison": comparisons,
    }


def _resolve_dataset_path(path: Path) -> Path:
    """Resolve relative input paths from the repository root, independent of cwd."""
    candidate = path if path.is_absolute() else REPOSITORY_ROOT / path
    return candidate.resolve()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--day1", type=Path, default=DEFAULT_DAY1_PATH, help="Day 1 JSONL path"
    )
    parser.add_argument(
        "--day2", type=Path, default=DEFAULT_DAY2_PATH, help="Day 2 JSONL path"
    )
    parser.add_argument(
        "--skip-baseline-comparison",
        action="store_true",
        help="Report actual metrics without comparing the approved baseline",
    )
    args = parser.parse_args()
    compare = not args.skip_baseline_comparison
    day1_path = _resolve_dataset_path(args.day1)
    day2_path = _resolve_dataset_path(args.day2)
    print(f"Day 1 input: {day1_path}", file=sys.stderr)
    print(f"Day 2 input: {day2_path}", file=sys.stderr)
    for label, path in (("Day 1", day1_path), ("Day 2", day2_path)):
        if not path.is_file():
            parser.error(f"{label} dataset file not found: {path}")

    reports = {
        "day1": verify_dataset(day1_path),
        "day2": verify_dataset(day2_path),
    }
    print(
        json.dumps(
            {
                label: _format_report(label, report, compare)
                for label, report in reports.items()
            },
            indent=2,
            sort_keys=True,
        )
    )
    if compare and any(
        actual[metric] != expected
        for label, report in reports.items()
        for metric, expected in APPROVED_BASELINE[label].items()
        for actual in [_metrics(report)]
    ):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
