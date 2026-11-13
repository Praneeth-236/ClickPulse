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

`export_current_state.py` reads the configured Iceberg current-state table and
replaces the Parquet export at `CLICKPULSE_EXPORT_PATH` (default:
`hdfs://<HDFS_NAMENODE_URI>/clickpulse/exports/current-state`). Configure the
Spark and Iceberg environment in `.env`, then run:

```bash
python scripts/export_current_state.py
```

Druid ingestion and datasource replacement are separate operations. Verify
the Druid ingestion result and replacement behavior independently after export.
