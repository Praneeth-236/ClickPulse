"""Kafka consumer and raw HDFS ingestion entry point."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from kafka import KafkaConsumer, TopicPartition

from clickpulse.common.constants import HDFS_RAW_PATH, KAFKA_TOPIC
from clickpulse.storage.batch_ledger import (
    get_batch_id,
    is_batch_completed,
    load_batch_record,
    update_batch_stage,
)

DEFAULT_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "172.22.60.30:9092,172.22.60.30:9094")
DEFAULT_GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP", "clickpulse-hdfs-raw-ingestor")
HDFS_MANIFEST_DIR = "/clickpulse/metadata/manifests"
HDFS_OFFSET_DIR = "/clickpulse/metadata/offsets"


def _run_hdfs(args: list[str]) -> subprocess.CompletedProcess[str]:
    res = subprocess.run(["hdfs", "dfs", *args], capture_output=True, text=True)
    if res.returncode != 0:
        raise RuntimeError(f"HDFS command failed ({' '.join(args)}): {res.stderr.strip()}")
    return res


def _hdfs_exists(hdfs_path: str) -> bool:
    res = subprocess.run(["hdfs", "dfs", "-test", "-e", hdfs_path], capture_output=True)
    return res.returncode == 0


def _decode_headers(headers: Sequence[tuple[str, bytes | None]] | None) -> dict[str, str]:
    decoded: dict[str, str] = {}
    if not headers:
        return decoded
    for key, val in headers:
        if val is None:
            decoded[key] = ""
        else:
            decoded[key] = val.decode("utf-8", errors="replace")
    return decoded


def _load_committed_offsets(offset_hdfs_path: str) -> dict[int, int]:
    if not _hdfs_exists(offset_hdfs_path):
        return {}
    res = _run_hdfs(["-cat", offset_hdfs_path])
    data = json.loads(res.stdout)
    return {int(part): int(off) for part, off in data.get("offsets", {}).items()}


def _save_committed_offsets(offset_hdfs_path: str, topic: str, group_id: str, offsets: dict[int, int]) -> None:
    _run_hdfs(["-mkdir", "-p", HDFS_OFFSET_DIR])
    payload = {
        "topic": topic,
        "group_id": group_id,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "offsets": {str(k): v for k, v in sorted(offsets.items())},
    }
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", delete=False, suffix=".json") as tmp_off:
        json.dump(payload, tmp_off, indent=2)
        local_off_path = Path(tmp_off.name)
    try:
        _run_hdfs(["-put", "-f", str(local_off_path), offset_hdfs_path])
    finally:
        local_off_path.unlink(missing_ok=True)


def consume_to_hdfs(
    topic: str = KAFKA_TOPIC,
    target_hdfs_path: str = HDFS_RAW_PATH,
    batch_id: str | None = None,
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
    group_id: str = DEFAULT_GROUP_ID,
    idle_timeout_ms: int = 3000,
) -> int:
    """Consume Kafka records via direct partition assignment and persist exact bytes to HDFS."""
    if batch_id and is_batch_completed(batch_id):
        print(f"[consumer] Batch '{batch_id}' already completed in ledger. Skipping.")
        return 0

    if batch_id:
        existing = load_batch_record(batch_id)
        if existing and existing.get("stages", {}).get("raw_ingestion", {}).get("status") == "completed":
            print(f"[consumer] Batch '{batch_id}' raw landing already committed. Skipping.")
            return 0

    servers = [s.strip() for s in bootstrap_servers.split(",") if s.strip()]
    offset_hdfs_path = f"{HDFS_OFFSET_DIR}/{group_id}_{topic}.json"
    print(f"[consumer] Connecting to Kafka brokers={servers}, topic='{topic}', group='{group_id}'")

    consumer = KafkaConsumer(
        bootstrap_servers=servers,
        enable_auto_commit=False,
        consumer_timeout_ms=idle_timeout_ms,
        value_deserializer=None,  # Keep raw bytes untouched
    )

    raw_buffer = bytearray()
    metadata_rows: list[dict[str, object]] = []
    record_count = 0

    try:
        partition_ids = sorted(consumer.partitions_for_topic(topic) or [])
        if not partition_ids:
            print(f"[consumer] Topic '{topic}' has no available partitions.")
            return 0

        partitions = [TopicPartition(topic, p) for p in partition_ids]
        consumer.assign(partitions)

        committed = _load_committed_offsets(offset_hdfs_path)
        beginning = consumer.beginning_offsets(partitions)
        end = consumer.end_offsets(partitions)

        has_unread = False
        next_offsets: dict[int, int] = dict(committed)
        for tp in partitions:
            start_offset = max(committed.get(tp.partition, beginning[tp]), beginning[tp])
            consumer.seek(tp, start_offset)
            if start_offset < end[tp]:
                has_unread = True

        if not has_unread:
            print(f"[consumer] No new messages available on topic '{topic}'.")
            return 0

        for msg in consumer:
            raw_bytes: bytes = msg.value if msg.value is not None else b""
            clean_line = raw_bytes.rstrip(b"\r\n")
            raw_buffer.extend(clean_line)
            raw_buffer.extend(b"\n")

            headers = _decode_headers(msg.headers)
            metadata_rows.append(
                {
                    "topic": msg.topic,
                    "partition": msg.partition,
                    "offset": msg.offset,
                    "kafka_timestamp_ms": msg.timestamp,
                    "line_sha256": hashlib.sha256(clean_line).hexdigest(),
                    "headers": headers,
                }
            )
            next_offsets[msg.partition] = msg.offset + 1
            record_count += 1

        if record_count == 0:
            print(f"[consumer] No new messages available on topic '{topic}'.")
            return 0

        raw_payload = bytes(raw_buffer)
        content_hash = get_batch_id(raw_payload)
        resolved_batch_id = batch_id or content_hash

        first_source = str(metadata_rows[0]["headers"].get("source_file", "")) if metadata_rows else ""
        sub_dir = ""
        if "day1" in first_source.lower():
            sub_dir = "day1"
        elif "day2" in first_source.lower():
            sub_dir = "day2"

        base_dir = target_hdfs_path.rstrip("/")
        dest_dir = f"{base_dir}/{sub_dir}" if sub_dir else base_dir
        final_raw_hdfs = f"{dest_dir}/batch_{resolved_batch_id}.jsonl"
        temp_raw_hdfs = f"{dest_dir}/._tmp_batch_{resolved_batch_id}.jsonl"
        final_manifest_hdfs = f"{HDFS_MANIFEST_DIR}/manifest_{resolved_batch_id}.json"

        _run_hdfs(["-mkdir", "-p", dest_dir, HDFS_MANIFEST_DIR])

        if not _hdfs_exists(final_raw_hdfs):
            with tempfile.NamedTemporaryFile(delete=False, suffix=".jsonl") as tmp_raw:
                tmp_raw.write(raw_payload)
                local_raw_path = Path(tmp_raw.name)

            try:
                _run_hdfs(["-put", "-f", str(local_raw_path), temp_raw_hdfs])
                _run_hdfs(["-mv", temp_raw_hdfs, final_raw_hdfs])
            finally:
                local_raw_path.unlink(missing_ok=True)
        else:
            print(f"[consumer] Raw file already exists at {final_raw_hdfs}; preserving committed file.")

        manifest = {
            "batch_id": resolved_batch_id,
            "content_sha256": content_hash,
            "topic": topic,
            "raw_hdfs_path": final_raw_hdfs,
            "record_count": record_count,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "records": metadata_rows,
        }

        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", delete=False, suffix=".json") as tmp_man:
            json.dump(manifest, tmp_man, indent=2)
            local_man_path = Path(tmp_man.name)

        try:
            _run_hdfs(["-put", "-f", str(local_man_path), final_manifest_hdfs])
        finally:
            local_man_path.unlink(missing_ok=True)

        update_batch_stage(
            batch_id=resolved_batch_id,
            stage="raw_ingestion",
            status="completed",
            extra={
                "source_file": first_source or "kafka_stream",
                "content_hash": content_hash,
                "raw_hdfs_path": final_raw_hdfs,
                "manifest_hdfs_path": final_manifest_hdfs,
                "record_count": record_count,
            },
        )

        # Save committed offsets in HDFS ONLY after raw landing + manifest + ledger succeed
        _save_committed_offsets(offset_hdfs_path, topic, group_id, next_offsets)
        print(
            f"[consumer] Committed {record_count} raw records to {final_raw_hdfs} "
            f"(batch_id={resolved_batch_id})."
        )
        return record_count
    finally:
        consumer.close()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ingest raw Kafka clickstream events into HDFS.")
    parser.add_argument("--topic", default=KAFKA_TOPIC, help="Kafka topic to consume from.")
    parser.add_argument("--target-hdfs-path", default=HDFS_RAW_PATH, help="Destination HDFS raw directory.")
    parser.add_argument("--batch-id", default=None, help="Optional explicit batch identifier.")
    parser.add_argument(
        "--bootstrap-servers",
        default=DEFAULT_BOOTSTRAP_SERVERS,
        help="Comma-separated Kafka bootstrap servers (default: 172.22.60.30:9092).",
    )
    parser.add_argument("--group-id", default=DEFAULT_GROUP_ID, help="Kafka consumer group ID.")
    parser.add_argument(
        "--idle-timeout-ms",
        type=int,
        default=3000,
        help="Exit cleanly after N milliseconds with no new Kafka records.",
    )
    args = parser.parse_args(argv)

    try:
        consume_to_hdfs(
            topic=args.topic,
            target_hdfs_path=args.target_hdfs_path,
            batch_id=args.batch_id,
            bootstrap_servers=args.bootstrap_servers,
            group_id=args.group_id,
            idle_timeout_ms=args.idle_timeout_ms,
        )
        return 0
    except Exception as exc:
        print(f"[consumer] ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
