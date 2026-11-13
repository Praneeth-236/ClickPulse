"""Backfill rejected raw occurrences without changing curated data or the batch ledger."""

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from dotenv import load_dotenv  # noqa: E402

from clickpulse.spark.runtime import create_iceberg_spark_session, runtime_config  # noqa: E402
from clickpulse.spark.validate_and_deduplicate import (  # noqa: E402
    read_jsonl_occurrences,
    validate_occurrences_dataframe,
)
from clickpulse.storage.iceberg_batch_ledger import compute_source_sha256  # noqa: E402
from clickpulse.storage.rejected_events import IcebergRejectedEventsStore  # noqa: E402


@dataclass(frozen=True)
class RejectedBackfillResult:
    input_path: str
    batch_id: str
    rejected_occurrence_count: int
    inserted_occurrence_count: int


def backfill_rejected_events(
    input_paths: list[str], *, spark: Any | None = None
) -> tuple[RejectedBackfillResult, ...]:
    """Validate paths and idempotently write rejected occurrences only.

    The function deliberately does not inspect or modify batch-ledger or curated
    tables. A caller-supplied SparkSession remains caller-owned.
    """
    if not input_paths:
        raise ValueError("At least one input path is required")

    session = spark if spark is not None else create_iceberg_spark_session(
        "clickpulse-rejected-backfill"
    )
    config = runtime_config()
    store = IcebergRejectedEventsStore(
        session,
        config.catalog,
        config.namespace,
        config.rejected_table,
    )
    results = []
    try:
        for input_path in input_paths:
            batch_id = compute_source_sha256(input_path)
            occurrences = read_jsonl_occurrences(session, input_path)
            validation = validate_occurrences_dataframe(occurrences)
            try:
                write_result = store.persist(validation.rejected_occurrences, batch_id)
                results.append(
                    RejectedBackfillResult(
                        input_path=input_path,
                        batch_id=batch_id,
                        rejected_occurrence_count=(
                            validation.metrics.rejected_occurrence_count
                        ),
                        inserted_occurrence_count=write_result.inserted_occurrence_count,
                    )
                )
            finally:
                validation.validated_records.unpersist()
                validation.rejected_occurrences.unpersist()
    finally:
        if spark is None:
            session.stop()
    return tuple(results)


def main() -> int:
    load_dotenv(ROOT / ".env")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-path",
        action="append",
        required=True,
        help="JSONL file path; repeat the option for each source batch",
    )
    args = parser.parse_args()
    results = backfill_rejected_events(args.input_path)
    for result in results:
        print(
            json.dumps(
                {
                    "input_path": result.input_path,
                    "batch_id": result.batch_id,
                    "rejected_occurrence_count": result.rejected_occurrence_count,
                    "inserted_occurrence_count": result.inserted_occurrence_count,
                },
                sort_keys=True,
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
