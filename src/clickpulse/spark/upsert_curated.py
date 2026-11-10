"""Pure cross-batch current-state upsert decisions for ClickPulse records."""

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

from clickpulse.common.validation import validate_event
from clickpulse.spark.validate_and_deduplicate import DeduplicationResult

Operation = Literal["INSERT", "UPDATE", "DROP", "INVALID"]


@dataclass(frozen=True)
class UpsertDecision:
    """Auditable decision for one incoming event ID."""

    event_id: str
    operation: Operation
    previous_record: Mapping[str, Any] | None
    resulting_record: Mapping[str, Any] | None
    incoming_record: Mapping[str, Any]
    incoming_occurrences: tuple[Any, ...]
    reason: str


@dataclass(frozen=True)
class UpsertMetrics:
    insert_count: int
    update_count: int
    drop_count: int


@dataclass(frozen=True)
class UpsertResult:
    current_state: tuple[Mapping[str, Any], ...]
    decisions: tuple[UpsertDecision, ...]
    metrics: UpsertMetrics
    unresolved_event_ids: tuple[str, ...]
    invalid_event_ids: tuple[str, ...]


def _parse_event_time(record: Mapping[str, Any]) -> datetime:
    value = record["event_time"]
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("event_time must include a timezone")
    return parsed.astimezone(UTC)


def _with_event_date(record: Mapping[str, Any]) -> dict[str, Any]:
    output = dict(record)
    output["event_date"] = _parse_event_time(record).date().isoformat()
    return output


def apply_upsert(
    existing_records: Iterable[Mapping[str, Any]],
    incoming: DeduplicationResult,
) -> UpsertResult:
    """Apply valid deduplication winners using strictly later ``event_time``.

    Records in unresolved latest-``updated_at`` ties are excluded from decisions.
    Invalid winners are surfaced as ``INVALID`` defensive decisions and never alter
    the state; normal pipeline validation should reject them before this function.
    """
    state: dict[str, dict[str, Any]] = {}
    for record in existing_records:
        event_id = record["event_id"]
        if event_id in state:
            raise ValueError(f"Existing curated state contains duplicate event_id: {event_id}")
        if validate_event(record):
            raise ValueError(f"Existing curated state contains invalid event_id: {event_id}")
        state[event_id] = _with_event_date(record)

    tie_ids = {tie.event_id for tie in incoming.unresolved_ties}
    winners_by_id: dict[str, Any] = {}
    for winner in incoming.winners:
        event_id = winner.record["event_id"]
        if event_id in tie_ids:
            raise ValueError(f"Unresolved event_id also appears among winners: {event_id}")
        if event_id in winners_by_id:
            raise ValueError(f"Incoming winners contain duplicate event_id: {event_id}")
        winners_by_id[event_id] = winner

    decisions: list[UpsertDecision] = []
    insert_count = update_count = drop_count = 0
    invalid_ids: list[str] = []

    for event_id in sorted(winners_by_id):
        winner = winners_by_id[event_id]
        record = winner.record
        previous = state.get(event_id)
        errors = validate_event(record)
        if errors:
            invalid_ids.append(event_id)
            decisions.append(
                UpsertDecision(
                    event_id=event_id,
                    operation="INVALID",
                    previous_record=previous,
                    resulting_record=previous,
                    incoming_record=dict(record),
                    incoming_occurrences=winner.occurrences,
                    reason="invalid incoming record: " + ", ".join(errors),
                )
            )
            continue

        incoming_with_date = _with_event_date(record)
        if previous is None:
            state[event_id] = incoming_with_date
            insert_count += 1
            operation: Operation = "INSERT"
            reason = "event_id absent from current state"
            resulting = incoming_with_date
        elif _parse_event_time(record) > _parse_event_time(previous):
            state[event_id] = incoming_with_date
            update_count += 1
            operation = "UPDATE"
            reason = "incoming event_time is later"
            resulting = incoming_with_date
        else:
            drop_count += 1
            operation = "DROP"
            reason = (
                "incoming event_time equals current event_time"
                if _parse_event_time(record) == _parse_event_time(previous)
                else "incoming event_time is earlier"
            )
            resulting = previous

        decisions.append(
            UpsertDecision(
                event_id=event_id,
                operation=operation,
                previous_record=previous,
                resulting_record=resulting,
                incoming_record=dict(record),
                incoming_occurrences=winner.occurrences,
                reason=reason,
            )
        )

    return UpsertResult(
        current_state=tuple(state[event_id] for event_id in sorted(state)),
        decisions=tuple(decisions),
        metrics=UpsertMetrics(insert_count, update_count, drop_count),
        unresolved_event_ids=tuple(sorted(tie_ids)),
        invalid_event_ids=tuple(sorted(invalid_ids)),
    )


def run_upsert() -> None:
    """Iceberg orchestration entry point, pending confirmed runtime interfaces."""
    raise NotImplementedError("Iceberg upsert integration is not configured")
