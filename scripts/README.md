# Scripts

Place repeatable developer and operations scripts here, such as:
- local environment checks
- dataset profiling
- batch checksum generation
- service health checks
- submission validation

`backfill_rejected_events.py` revalidates supplied JSONL batches and idempotently
persists rejected occurrences without modifying the curated table or batch
ledger. Configure its Spark and Iceberg environment in `.env` first.
