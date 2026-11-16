"""Kafka producer entry point for raw clickstream line ingestion."""

from __future__ import annotations

import argparse
import hashlib
import os
import sys
from pathlib import Path
from typing import Sequence

from kafka import KafkaProducer

from clickpulse.common.constants import KAFKA_TOPIC

DEFAULT_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "172.22.60.30:9092")
DEFAULT_INPUT_PATH = "data/sample/clicks_day1.jsonl"


def produce_file(
    input_path: str | Path = DEFAULT_INPUT_PATH,
    topic: str = KAFKA_TOPIC,
    batch_id: str | None = None,
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
) -> int:
    """Publish every original line from input_path to Kafka as raw bytes."""
    file_path = Path(input_path)
    if not file_path.exists():
        raise FileNotFoundError(f"Input dataset not found: {file_path}")

    raw_file_bytes = file_path.read_bytes()
    resolved_batch_id = batch_id or hashlib.sha256(raw_file_bytes).hexdigest()
    servers = [s.strip() for s in bootstrap_servers.split(",") if s.strip()]

    producer = KafkaProducer(
        bootstrap_servers=servers,
        acks="all",
        value_serializer=None,
    )

    sent_count = 0
    try:
        with file_path.open("rb") as fh:
            for line_num, raw_line in enumerate(fh, start=1):
                clean_bytes = raw_line.rstrip(b"\r\n")
                headers = [
                    ("source_file", file_path.name.encode("utf-8")),
                    ("source_line_number", str(line_num).encode("utf-8")),
                    ("batch_id", resolved_batch_id.encode("utf-8")),
                ]
                producer.send(topic, value=clean_bytes, headers=headers)
                sent_count += 1

        producer.flush()
        print(
            f"[producer] Published {sent_count} raw lines from '{file_path.name}' "
            f"to topic '{topic}' (batch_id={resolved_batch_id})."
        )
        return sent_count
    finally:
        producer.close()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Publish raw clickstream JSONL lines to Kafka.")
    parser.add_argument("--input", default=DEFAULT_INPUT_PATH, help="Path to source JSONL file.")
    parser.add_argument("--topic", default=KAFKA_TOPIC, help="Target Kafka topic.")
    parser.add_argument("--batch-id", default=None, help="Optional explicit batch identifier.")
    parser.add_argument(
        "--bootstrap-servers",
        default=DEFAULT_BOOTSTRAP_SERVERS,
        help="Comma-separated Kafka bootstrap servers.",
    )
    args = parser.parse_args(argv)

    try:
        produce_file(
            input_path=args.input,
            topic=args.topic,
            batch_id=args.batch_id,
            bootstrap_servers=args.bootstrap_servers,
        )
        return 0
    except Exception as exc:
        print(f"[producer] ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
