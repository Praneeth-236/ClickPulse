"""Occurrence parsing and contract validation for ClickPulse records."""

import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from clickpulse.common.constants import VALID_EVENT_TYPES


@dataclass(frozen=True)
class RawOccurrence:
    """One input line with its original source location, before any transformation."""

    raw_line: str
    source_file: str
    source_line_number: int


@dataclass(frozen=True)
class ParsedOccurrence:
    """Parsed view of an occurrence; ``parse_error`` distinguishes malformed JSON."""

    occurrence: RawOccurrence
    record: Any | None
    parse_error: str | None = None


def parse_occurrence(occurrence: RawOccurrence) -> ParsedOccurrence:
    """Parse one JSON line while retaining the exact original line and location."""
    try:
        record = json.loads(occurrence.raw_line, parse_float=Decimal)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return ParsedOccurrence(occurrence, None, "unreadable_or_not_object")
    return ParsedOccurrence(occurrence, record)


def _is_timestamp(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    # The contract specifies UTC Z timestamps; naive interpretation is unresolved.
    return parsed.tzinfo is not None


def validate_event(record: Any) -> list[str]:
    """Return every applicable contract rejection reason for a parsed JSON value."""
    if not isinstance(record, dict):
        return ["unreadable_or_not_object"]

    reasons: list[str] = []
    required_strings = ("event_id", "user_id", "session_id")
    for field in required_strings:
        value = record.get(field)
        if not isinstance(value, str) or not value.strip():
            reasons.append(f"missing_or_empty_{field}")

    event_type = record.get("event_type")
    if not isinstance(event_type, str) or event_type not in VALID_EVENT_TYPES:
        reasons.append("invalid_event_type")

    event_time = record.get("event_time")
    if not _is_timestamp(event_time):
        reasons.append("invalid_event_time")

    price = record.get("price")
    if price is not None:
        if isinstance(price, bool) or not isinstance(price, (int, Decimal)):
            reasons.append("invalid_price_type")
        elif price < 0:
            reasons.append("negative_price")

    quantity = record.get("quantity")
    if quantity is not None:
        if type(quantity) is not int:
            reasons.append("invalid_quantity_type")
        elif quantity < 0:
            reasons.append("negative_quantity")

    for field in ("product_id", "category", "device"):
        value = record.get(field)
        if value is not None and not isinstance(value, str):
            reasons.append(f"invalid_{field}_type")

    updated_at = record.get("updated_at")
    if not isinstance(updated_at, str) or not updated_at.strip():
        reasons.append("missing_or_empty_updated_at")
    elif not _is_timestamp(updated_at):
        reasons.append("invalid_updated_at")

    return reasons
