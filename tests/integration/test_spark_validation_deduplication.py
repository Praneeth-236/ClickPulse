"""Local Spark acceptance tests for distributed validation and deduplication."""

import json
import shutil
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from clickpulse.spark.validate_and_deduplicate import run_validation_and_deduplication

ROOT = Path(__file__).resolve().parents[2]
DAY1_PATH = ROOT / "data/sample/clicks_day1.jsonl"
DAY2_PATH = ROOT / "data/sample/clicks_day2.jsonl"


def test_upsert_parser_projects_a_single_event_id_column(spark):
    from clickpulse.spark.upsert_curated import _parse_incoming_records

    record = {
        "event_id": "evt-unique-column",
        "user_id": "user-1",
        "session_id": "session-1",
        "event_type": "page_view",
        "product_id": None,
        "category": None,
        "price": 2.5,
        "quantity": 1,
        "device": None,
        "event_time": "2025-01-01T12:00:00Z",
        "updated_at": "2025-01-01T12:01:00Z",
    }
    raw_line = json.dumps(record)
    incoming = spark.createDataFrame([(raw_line, record["event_id"])], ["raw_line", "event_id"])

    parsed = _parse_incoming_records(spark, incoming)

    assert parsed.columns.count("event_id") == 1
    assert parsed.select("event_id").first().event_id == record["event_id"]


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("clickpulse-validation-deduplication-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.executorEnv.PYTHONPATH", str(ROOT / "src"))
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        (
            DAY1_PATH,
            {
                "raw": 144,
                "malformed": 2,
                "valid": 128,
                "rejected": 16,
                "duplicates": 6,
                "unique": 116,
                "repeated": 11,
                "conflicting": 6,
                "resolved": 116,
                "ties": 0,
            },
        ),
        (
            DAY2_PATH,
            {
                "raw": 148,
                "malformed": 3,
                "valid": 133,
                "rejected": 15,
                "duplicates": 4,
                "unique": 125,
                "repeated": 8,
                "conflicting": 4,
                "resolved": 125,
                "ties": 0,
            },
        ),
    ],
)
def test_spark_dataset_validation_and_deduplication_acceptance(
    spark, path, expected, tmp_path
):
    local_input = tmp_path / path.name
    shutil.copyfile(path, local_input)
    result = run_validation_and_deduplication(str(local_input), spark=spark)

    assert result.validation_metrics.raw_line_count == expected["raw"]
    assert result.validation_metrics.malformed_line_count == expected["malformed"]
    assert result.validation_metrics.valid_record_count == expected["valid"]
    assert result.validation_metrics.rejected_occurrence_count == expected["rejected"]
    assert result.deduplication_metrics.valid_input_count == expected["valid"]
    assert result.deduplication_metrics.exact_duplicate_excess == expected["duplicates"]
    assert result.deduplication_metrics.unique_event_id_count == expected["unique"]
    assert result.deduplication_metrics.repeated_event_id_groups == expected["repeated"]
    assert result.deduplication_metrics.conflicting_event_id_groups == expected["conflicting"]
    assert result.deduplication_metrics.resolved_record_count == expected["resolved"]
    assert result.deduplication_metrics.unresolved_latest_timestamp_tie_count == expected["ties"]
    assert result.winners.count() == expected["resolved"]
    assert result.unresolved_ties.count() == expected["ties"]
    assert result.rejected_occurrences.count() == expected["rejected"]


def test_spark_rejections_preserve_occurrence_and_all_reasons(spark, tmp_path):
    lines = [
        '{"event_id":',
        "[]",
        json.dumps(
            {
                "event_id": " ",
                "user_id": None,
                "session_id": "s1",
                "event_type": "bad",
                "event_time": "2025-01-01T12:00:00Z",
                "updated_at": "2025-01-01T12:00:00Z",
                "price": -1.0,
            }
        ),
    ]
    input_path = tmp_path / "rejections.jsonl"
    input_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    result = run_validation_and_deduplication(str(input_path), spark=spark)
    rejected = result.rejected_occurrences.orderBy("source_line_number").collect()

    assert len(rejected) == 3
    assert rejected[0].source_line_number == 1
    assert rejected[0].raw_line == lines[0]
    assert rejected[0].rejection_reasons == ["unreadable_or_not_object"]
    assert rejected[1].source_line_number == 2
    assert rejected[1].rejection_reasons == ["unreadable_or_not_object"]
    assert rejected[2].source_line_number == 3
    assert set(rejected[2].rejection_reasons) == {
        "missing_or_empty_event_id",
        "missing_or_empty_user_id",
        "invalid_event_type",
        "negative_price",
    }
    assert all(row.source_file == str(input_path) for row in rejected)
    assert all(row.processing_timestamp is not None for row in rejected)


def test_spark_validation_keeps_zero_nullable_values_and_rejects_type_coercion(
    spark, tmp_path
):
    valid = {
        "event_id": "zero-values",
        "user_id": "user-1",
        "session_id": "session-1",
        "event_type": "page_view",
        "product_id": None,
        "category": None,
        "price": 0,
        "quantity": 0,
        "device": None,
        "event_time": "2025-01-01T12:00:00Z",
        "updated_at": "2025-01-01T12:01:00Z",
    }
    invalid_boolean = {**valid, "event_id": "boolean-values", "price": True, "quantity": False}
    invalid_strings = {**valid, "event_id": "string-values", "price": "0", "quantity": "0"}
    input_path = tmp_path / "strict-types.jsonl"
    input_path.write_text(
        "\n".join(json.dumps(row) for row in (valid, invalid_boolean, invalid_strings))
        + "\n",
        encoding="utf-8",
    )

    result = run_validation_and_deduplication(str(input_path), spark=spark)
    assert result.validation_metrics.valid_record_count == 1
    assert result.validation_metrics.rejected_occurrence_count == 2
    assert result.validated_records.select("event_id").first().event_id == "zero-values"
    # The invalid records remain in the rejection DataFrame instead of being coerced.
    rejected_rows = result.rejected_occurrences.orderBy("source_line_number").collect()
    assert rejected_rows[0].rejection_reasons == [
        "invalid_price_type",
        "invalid_quantity_type",
    ]
    assert rejected_rows[1].rejection_reasons == [
        "invalid_price_type",
        "invalid_quantity_type",
    ]


def test_spark_latest_timestamp_tie_has_no_winner(spark, tmp_path):
    records = [
        {
            "event_id": "tie-1",
            "user_id": "user-1",
            "session_id": "session-1",
            "event_type": "purchase",
            "product_id": "p1",
            "category": "Books",
            "price": 1.0,
            "quantity": 1,
            "device": "mobile",
            "event_time": "2025-01-01T12:00:00Z",
            "updated_at": "2025-01-01T12:01:00Z",
        },
        {
            "event_id": "tie-1",
            "user_id": "user-1",
            "session_id": "session-1",
            "event_type": "purchase",
            "product_id": "p2",
            "category": "Books",
            "price": 2.0,
            "quantity": 1,
            "device": "mobile",
            "event_time": "2025-01-01T12:00:00Z",
            "updated_at": "2025-01-01T12:01:00Z",
        },
    ]
    input_path = tmp_path / "tie.jsonl"
    input_path.write_text("\n".join(json.dumps(row) for row in records) + "\n", encoding="utf-8")

    result = run_validation_and_deduplication(str(input_path), spark=spark)

    assert result.winners.count() == 0
    tied_ids = [row.event_id for row in result.unresolved_ties.select("event_id").collect()]
    assert tied_ids == ["tie-1"]
    assert result.deduplication_metrics.unresolved_latest_timestamp_tie_count == 1
