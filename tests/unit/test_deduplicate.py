"""Tests for deterministic within-batch deduplication."""

from decimal import Decimal

import pytest

from clickpulse.common.validation import RawOccurrence, validate_event
from clickpulse.spark.validate_and_deduplicate import (
    ValidatedRecord,
    deduplicate_validated_records,
)


def record(event_id="evt-1", updated_at="2025-01-01T12:00:00Z", **changes):
    result = {
        "event_id": event_id,
        "user_id": "user-1",
        "session_id": "session-1",
        "event_type": "purchase",
        "product_id": "product-1",
        "category": "Books",
        "price": Decimal("12.50"),
        "quantity": 2,
        "device": "mobile",
        "event_time": "2025-01-01T11:00:00Z",
        "updated_at": updated_at,
    }
    result.update(changes)
    return result


def item(event_record, line_number, source_file="batch.jsonl"):
    assert validate_event(event_record) == []
    return ValidatedRecord(
        record=event_record,
        occurrence=RawOccurrence(str(event_record), source_file, line_number),
    )


def test_no_duplicates_produces_one_winner_per_id():
    result = deduplicate_validated_records([item(record("b"), 1), item(record("a"), 2)])
    assert [winner.record["event_id"] for winner in result.winners] == ["a", "b"]
    assert result.unresolved_ties == ()
    assert result.metrics.valid_input_count == 2
    assert result.metrics.unique_event_id_count == 2
    assert result.metrics.resolved_record_count == 2


def test_exact_duplicates_collapse_and_keep_all_occurrences():
    first, second = item(record(), 1), item(record(), 2)
    result = deduplicate_validated_records([first, second])
    assert len(result.winners) == 1
    assert result.winners[0].occurrences == (first.occurrence, second.occurrence)
    assert result.metrics.exact_duplicate_excess == 1
    assert result.metrics.repeated_event_id_groups == 1
    assert result.metrics.conflicting_event_id_groups == 0


def test_conflicting_records_choose_greatest_updated_at():
    older = item(record(price=Decimal("3")), 1)
    newer = item(record(updated_at="2025-01-02T12:00:00Z", price=Decimal("4")), 2)
    result = deduplicate_validated_records([older, newer])
    assert len(result.winners) == 1
    assert result.winners[0].record["price"] == Decimal("4")
    assert result.metrics.conflicting_event_id_groups == 1
    assert result.metrics.resolved_record_count == 1


def test_equal_timestamp_and_identical_content_are_exact_duplicates():
    result = deduplicate_validated_records([item(record(), 1), item(record(), 2)])
    assert len(result.winners) == 1
    assert result.unresolved_ties == ()
    assert result.metrics.exact_duplicate_excess == 1
    assert result.metrics.conflicting_event_id_groups == 0


def test_equal_latest_timestamp_and_different_content_stays_unresolved():
    first = item(record(price=Decimal("3")), 1)
    second = item(record(price=Decimal("4")), 2)
    result = deduplicate_validated_records([first, second])
    assert result.winners == ()
    assert len(result.unresolved_ties) == 1
    tie = result.unresolved_ties[0]
    assert tie.event_id == "evt-1"
    assert len(tie.candidates) == 2
    assert {candidate.record["price"] for candidate in tie.candidates} == {
        Decimal("3"), Decimal("4")
    }
    assert result.metrics.conflicting_event_id_groups == 1
    assert result.metrics.resolved_record_count == 0


def test_multiple_versions_select_only_the_greatest_updated_at():
    items = [
        item(record(updated_at="2025-01-01T12:00:00Z", price=Decimal("1")), 1),
        item(record(updated_at="2025-01-03T12:00:00Z", price=Decimal("3")), 3),
        item(record(updated_at="2025-01-02T12:00:00Z", price=Decimal("2")), 2),
    ]
    result = deduplicate_validated_records(items)
    assert result.winners[0].record["price"] == Decimal("3")
    assert result.metrics.conflicting_event_id_groups == 1
    assert result.metrics.resolved_record_count == 1


def test_multiple_event_ids_have_independent_groups_and_metrics():
    records = [
        item(record("a"), 1),
        item(record("a"), 2),
        item(record("b", price=Decimal("5")), 3),
        item(record("b", updated_at="2025-01-02T12:00:00Z", price=Decimal("6")), 4),
    ]
    result = deduplicate_validated_records(records)
    assert [winner.record["event_id"] for winner in result.winners] == ["a", "b"]
    assert result.metrics.valid_input_count == 4
    assert result.metrics.exact_duplicate_excess == 1
    assert result.metrics.unique_event_id_count == 2
    assert result.metrics.repeated_event_id_groups == 2
    assert result.metrics.conflicting_event_id_groups == 1
    assert result.metrics.resolved_record_count == 2


def test_results_and_audit_records_do_not_depend_on_input_order():
    records = [
        item(record("b", price=Decimal("1")), 1),
        item(record("a"), 2),
        item(record("b", updated_at="2025-01-02T12:00:00Z", price=Decimal("2")), 3),
    ]
    forward = deduplicate_validated_records(records)
    reverse = deduplicate_validated_records(reversed(records))
    assert forward == reverse


def test_occurrence_metadata_for_all_inputs_is_available_for_audit():
    original = [item(record(), 10, "one.jsonl"), item(record(), 20, "two.jsonl")]
    result = deduplicate_validated_records(original)
    assert {entry.occurrence for entry in result.audit_records} == {
        entry.occurrence for entry in original
    }
    assert result.winners[0].occurrences == tuple(entry.occurrence for entry in original)


def test_rejects_inputs_that_are_not_validated_record_wrappers():
    with pytest.raises(TypeError, match="ValidatedRecord"):
        deduplicate_validated_records([record()])
