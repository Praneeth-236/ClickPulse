"""Dataset acceptance checks for validation, deduplication, and pure upsert."""

from pathlib import Path

from clickpulse.common.validation import RawOccurrence, parse_occurrence, validate_event
from clickpulse.spark.upsert_curated import apply_upsert
from scripts.verify_dataset import verify_dataset

ROOT = Path(__file__).resolve().parents[2]
DAY1_PATH = ROOT / "data/sample/clicks_day1.jsonl"
DAY2_PATH = ROOT / "data/sample/clicks_day2.jsonl"


def _invalid_event_ids(path):
    invalid = {}
    with path.open("r", encoding="utf-8", newline="") as stream:
        for line_number, raw_line in enumerate(stream, start=1):
            parsed = parse_occurrence(RawOccurrence(raw_line, str(path), line_number))
            if parsed.parse_error or validate_event(parsed.record):
                if isinstance(parsed.record, dict) and isinstance(
                    parsed.record.get("event_id"), str
                ):
                    invalid[parsed.record["event_id"]] = parsed.record
    return invalid


def test_day1_to_day2_dataset_upsert_acceptance():
    day1 = verify_dataset(DAY1_PATH)
    day2 = verify_dataset(DAY2_PATH)

    day1_curated = apply_upsert([], day1.deduplication)
    result = apply_upsert(day1_curated.current_state, day2.deduplication)

    assert day1_curated.metrics.insert_count == 116
    assert day2.deduplication.metrics.unique_event_id_count == 125
    assert len(
        {record["event_id"] for record in day1_curated.current_state}
        & {winner.record["event_id"] for winner in day2.deduplication.winners}
    ) == 24
    assert result.metrics.insert_count == 101
    assert result.metrics.update_count == 12
    assert result.metrics.drop_count == 12
    assert day2.rejected_occurrences == 15
    assert len(result.current_state) == 217
    assert len({record["event_id"] for record in result.current_state}) == 217

    updates = [decision for decision in result.decisions if decision.operation == "UPDATE"]
    assert len(updates) == 12
    for update in updates:
        expected = dict(update.incoming_record)
        expected["event_date"] = update.resulting_record["event_date"]
        assert update.resulting_record == expected
        assert update.previous_record["event_date"] != update.resulting_record["event_date"]
        assert sum(
            record["event_id"] == update.event_id for record in result.current_state
        ) == 1

    invalid_day2 = _invalid_event_ids(DAY2_PATH)
    targeted = {"evt_129bdeb398", "evt_b28a29110d"}
    assert targeted <= invalid_day2.keys()
    day1_by_id = {record["event_id"]: record for record in day1_curated.current_state}
    final_by_id = {record["event_id"]: record for record in result.current_state}
    for event_id in targeted:
        assert event_id in day1_by_id
        assert final_by_id[event_id] == day1_by_id[event_id]
