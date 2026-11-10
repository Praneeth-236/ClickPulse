"""Functional tests for the pure cross-batch upsert decision engine."""

from decimal import Decimal

import pytest

from clickpulse.common.validation import RawOccurrence
from clickpulse.spark.upsert_curated import apply_upsert
from clickpulse.spark.validate_and_deduplicate import (
    DeduplicatedRecord,
    DeduplicationMetrics,
    DeduplicationResult,
    ValidatedRecord,
    deduplicate_validated_records,
)


def event(
    event_id="evt-1",
    event_time="2025-01-01T10:00:00Z",
    updated_at="2025-01-01T12:00:00Z",
    **changes,
):
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
        "event_time": event_time,
        "updated_at": updated_at,
    }
    result.update(changes)
    return result


def dedup(records):
    wrapped = [
        ValidatedRecord(record, RawOccurrence(str(record), "batch.jsonl", index))
        for index, record in enumerate(records, start=1)
    ]
    return deduplicate_validated_records(wrapped)


def test_new_id_is_inserted_with_utc_event_date():
    result = apply_upsert([], dedup([event()]))
    assert result.metrics.insert_count == 1
    assert result.metrics.update_count == result.metrics.drop_count == 0
    assert result.current_state[0]["event_date"] == "2025-01-01"
    assert result.decisions[0].operation == "INSERT"


def test_strictly_later_event_time_updates_existing_record():
    result = apply_upsert(
        [event(price=Decimal("1"))],
        dedup([event(event_time="2025-01-02T00:00:00Z", price=Decimal("2"))]),
    )
    assert result.metrics.update_count == 1
    assert result.decisions[0].operation == "UPDATE"
    assert result.current_state[0]["price"] == Decimal("2")


@pytest.mark.parametrize("event_time", ["2025-01-01T10:00:00Z", "2025-01-01T09:00:00Z"])
def test_equal_or_earlier_event_time_is_dropped(event_time):
    current = event(event_time="2025-01-01T10:00:00Z", price=Decimal("1"))
    incoming = event(event_time=event_time, updated_at="2025-01-09T00:00:00Z", price=Decimal("8"))
    result = apply_upsert([current], dedup([incoming]))
    assert result.metrics.drop_count == 1
    assert result.decisions[0].operation == "DROP"
    assert result.current_state[0]["price"] == Decimal("1")


def test_update_replaces_every_field_from_the_incoming_record():
    old = event(
        user_id="old-user", session_id="old-session", event_type="page_view",
        product_id="old-product", category="old-category", price=Decimal("1"), quantity=1,
        device="desktop", extra_old="remove-me",
    )
    new = event(
        event_time="2025-01-02T10:00:00Z", user_id="new-user", session_id="new-session",
        product_id=None, category=None, price=None, quantity=None, device="tablet",
    )
    result = apply_upsert([old], dedup([new]))
    expected = {**new, "event_date": "2025-01-02"}
    assert result.current_state == (expected,)
    assert "extra_old" not in result.current_state[0]


def test_event_date_movement_replaces_old_partition_date_once():
    old = event(event_time="2025-01-01T23:59:00Z")
    new = event(event_time="2025-01-02T00:01:00Z")
    result = apply_upsert([old], dedup([new]))
    assert result.metrics.update_count == 1
    assert result.current_state[0]["event_date"] == "2025-01-02"
    assert sum(row["event_id"] == "evt-1" for row in result.current_state) == 1


def test_invalid_update_is_reported_and_preserves_existing_record():
    old = event(price=Decimal("4"))
    invalid = event(event_time="2025-01-02T00:00:00Z", price=Decimal("-4"))
    occurrence = RawOccurrence("invalid raw", "batch.jsonl", 1)
    bad_winner = DeduplicatedRecord(invalid, (occurrence,))
    incoming = DeduplicationResult(
        winners=(bad_winner,),
        unresolved_ties=(),
        metrics=DeduplicationMetrics(1, 0, 1, 0, 0, 1),
        audit_records=(),
    )
    result = apply_upsert([old], incoming)
    assert result.invalid_event_ids == ("evt-1",)
    assert result.decisions[0].operation == "INVALID"
    assert result.current_state[0]["price"] == Decimal("4")
    assert (
        result.metrics.insert_count
        == result.metrics.update_count
        == result.metrics.drop_count
        == 0
    )


def test_multiple_ids_produce_independent_decisions_and_counts():
    existing = [event("update", price=Decimal("1")), event("drop")]
    incoming = dedup(
        [
            event("insert"),
            event("update", event_time="2025-01-02T00:00:00Z", price=Decimal("2")),
            event("drop", event_time="2024-12-31T00:00:00Z"),
        ]
    )
    result = apply_upsert(existing, incoming)
    assert result.metrics.insert_count == 1
    assert result.metrics.update_count == 1
    assert result.metrics.drop_count == 1
    assert {row["event_id"] for row in result.current_state} == {"insert", "update", "drop"}


def test_empty_input_preserves_current_state_and_has_zero_metrics():
    existing = [event()]
    result = apply_upsert(existing, dedup([]))
    assert result.current_state[0]["event_id"] == "evt-1"
    assert (
        result.metrics.insert_count
        == result.metrics.update_count
        == result.metrics.drop_count
        == 0
    )
    assert result.decisions == ()


def test_input_order_does_not_change_decisions_or_current_state():
    existing = [event("b"), event("a")]
    incoming_records = [event("c"), event("a", event_time="2025-01-02T00:00:00Z")]
    wrapped = [
        ValidatedRecord(record, RawOccurrence(str(record), "batch.jsonl", index))
        for index, record in enumerate(incoming_records, start=1)
    ]
    forward = apply_upsert(existing, deduplicate_validated_records(wrapped))
    reverse = apply_upsert(reversed(existing), deduplicate_validated_records(reversed(wrapped)))
    assert forward == reverse


def test_unresolved_tie_is_excluded_from_automatic_upsert():
    first = event("tie", price=Decimal("1"))
    second = event("tie", price=Decimal("2"))
    tied = dedup([first, second])
    result = apply_upsert([], tied)
    assert result.unresolved_event_ids == ("tie",)
    assert result.current_state == ()
    assert result.decisions == ()
    assert result.metrics.insert_count == 0


def test_duplicate_existing_ids_are_rejected_as_ambiguous_state():
    with pytest.raises(ValueError, match="duplicate event_id"):
        apply_upsert([event(), event()], dedup([]))
