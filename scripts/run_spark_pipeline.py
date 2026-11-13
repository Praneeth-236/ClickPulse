"""Run configured ClickPulse batch files sequentially through Spark and Iceberg."""

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from dotenv import load_dotenv  # noqa: E402

from clickpulse.spark.pipeline import process_batch  # noqa: E402
from clickpulse.spark.runtime import create_iceberg_spark_session  # noqa: E402


def main() -> int:
    load_dotenv(ROOT / ".env")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-path",
        action="append",
        required=True,
        help="JSONL file path; repeat the option to process files sequentially",
    )
    args = parser.parse_args()
    spark = create_iceberg_spark_session("clickpulse-pipeline")
    try:
        for input_path in args.input_path:
            result = process_batch(input_path, spark=spark)
            try:
                output = {
                    "input_path": input_path,
                    "batch_id": result.batch_id,
                    "replayed": result.replayed,
                }
                if result.validation is not None:
                    output["validation"] = result.validation.metrics.__dict__
                if result.deduplication is not None:
                    output["deduplication"] = result.deduplication.metrics.__dict__
                if result.upsert is not None:
                    output["upsert"] = result.upsert.metrics.__dict__
                    output["iceberg_snapshot_id"] = result.upsert.iceberg_snapshot_id
                print(json.dumps(output, sort_keys=True))
            finally:
                if result.upsert is not None:
                    result.upsert.release()
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
