"""Contract tests for occurrence parsing and event validation."""

from decimal import Decimal

import pytest

from clickpulse.common.validation import RawOccurrence, parse_occurrence, validate_event


def valid_record(**updates):
    record = {
        "event_id": "evt-1",
        "user_id": "user-1",
        "session_id": "session-1",
        "event_type": "purchase",
        "product_id": "product-1",
        "category": "Books",
        "price": Decimal("12.50"),
        "quantity": 2,
        "device": "mobile",
        "event_time": "2025-01-01T12:00:00Z",
        "updated_at": "2025-01-01T12:01:00Z",
    }
    record.update(updates)
    return record


def test_valid_event_has_no_rejection_reasons():
    assert validate_event(valid_record()) == []


@pytest.mark.parametrize("field", ["event_id", "user_id", "session_id"])
@pytest.mark.parametrize("value", [None, "", " \t "])
def test_missing_or_whitespace_required_identifier_is_rejected(field, value):
    assert validate_event(valid_record(**{field: value})) == [f"missing_or_empty_{field}"]


def test_multiple_simultaneous_validation_failures_are_all_returned():
    record = valid_record(
        event_id=" ", user_id=None, session_id=7, event_type="unknown", event_time="bad",
        updated_at="not-a-timestamp", price=Decimal("-1"), quantity=-2,
    )
    assert validate_event(record) == [
        "missing_or_empty_event_id",
        "missing_or_empty_user_id",
        "missing_or_empty_session_id",
        "invalid_event_type",
        "invalid_event_time",
        "negative_price",
        "negative_quantity",
        "invalid_updated_at",
    ]


def test_invalid_event_type_is_rejected():
    assert validate_event(valid_record(event_type="scroll")) == ["invalid_event_type"]


@pytest.mark.parametrize("value", [None, "", "yesterday", "2025-01-01T12:00:00"])
def test_invalid_and_timezone_naive_event_times_are_rejected(value):
    assert validate_event(valid_record(event_time=value)) == ["invalid_event_time"]


def test_negative_price_is_rejected_but_zero_is_valid():
    assert validate_event(valid_record(price=Decimal("-0.01"))) == ["negative_price"]
    assert validate_event(valid_record(price=Decimal("0"))) == []


def test_negative_quantity_is_rejected_but_zero_is_valid():
    assert validate_event(valid_record(quantity=-1)) == ["negative_quantity"]
    assert validate_event(valid_record(quantity=0)) == []


def test_nullable_fields_and_missing_optional_fields_are_valid():
    record = valid_record(product_id=None, category=None, price=None, quantity=None)
    assert validate_event(record) == []
    for field in ("product_id", "category", "price", "quantity", "device"):
        record.pop(field)
    assert validate_event(record) == []


@pytest.mark.parametrize(
    ("updates", "reason"),
    [
        ({"event_id": 3}, "missing_or_empty_event_id"),
        ({"event_type": 3}, "invalid_event_type"),
        ({"event_time": 123}, "invalid_event_time"),
        ({"price": "3.25"}, "invalid_price_type"),
        ({"price": True}, "invalid_price_type"),
        ({"quantity": 1.0}, "invalid_quantity_type"),
        ({"quantity": False}, "invalid_quantity_type"),
        ({"product_id": 4}, "invalid_product_id_type"),
        ({"category": []}, "invalid_category_type"),
        ({"device": {}}, "invalid_device_type"),
        ({"updated_at": 1}, "missing_or_empty_updated_at"),
    ],
)
def test_unexpected_python_types_are_rejected_without_coercion(updates, reason):
    assert validate_event(valid_record(**updates)) == [reason]


@pytest.mark.parametrize("value", [None, "", " \t "])
def test_missing_null_or_empty_updated_at_is_rejected(value):
    assert validate_event(valid_record(updated_at=value)) == ["missing_or_empty_updated_at"]


def test_absent_updated_at_is_rejected():
    record = valid_record()
    del record["updated_at"]
    assert validate_event(record) == ["missing_or_empty_updated_at"]


@pytest.mark.parametrize("value", ["yesterday", "2025-01-01T12:00:00"])
def test_malformed_or_timezone_naive_updated_at_is_rejected(value):
    assert validate_event(valid_record(updated_at=value)) == ["invalid_updated_at"]


def test_valid_updated_at_timestamp_is_accepted():
    assert validate_event(valid_record(updated_at="2025-01-01T12:01:00Z")) == []


def test_non_object_python_value_is_rejected():
    assert validate_event([valid_record()]) == ["unreadable_or_not_object"]


def test_raw_occurrence_keeps_original_line_and_source_location():
    line = ' { "event_id" : "evt-1" } '
    occurrence = RawOccurrence(line, "day1.jsonl", 9)
    parsed = parse_occurrence(occurrence)
    assert parsed.occurrence == occurrence
    assert parsed.record == {"event_id": "evt-1"}
    assert parsed.parse_error is None


def test_malformed_json_is_distinguished_from_non_object_json():
    malformed = parse_occurrence(RawOccurrence('{"event_id":', "day1.jsonl", 1))
    array = parse_occurrence(RawOccurrence("[]", "day1.jsonl", 2))
    assert malformed.parse_error == "unreadable_or_not_object"
    assert malformed.record is None
    assert array.parse_error is None
    assert array.record == []
    assert validate_event(array.record) == ["unreadable_or_not_object"]


@pytest.mark.parametrize("raw_line", ['"text"', "123", "true", "null", "[]"])
def test_all_non_object_json_top_level_values_are_rejected_as_objects(raw_line):
    parsed = parse_occurrence(RawOccurrence(raw_line, "day1.jsonl", 1))
    assert parsed.parse_error is None
    assert validate_event(parsed.record) == ["unreadable_or_not_object"]
