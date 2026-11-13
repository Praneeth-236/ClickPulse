# Spark and Iceberg batch processing

The Spark job reads JSONL paths directly from HDFS, validates occurrences,
persists rejected occurrences to the configured Iceberg rejected-events table,
deduplicates valid records, merges winners into the configured Iceberg
current-state table, and stores batch lifecycle records in an Iceberg ledger
table. Configure the values in `.env` or export environment variables before
running the script.

## VM3 execution

The deployment must provide Spark 3.3.4, Python 3.10, Java 11, Hadoop client
configuration for VM2, and the Iceberg 1.8.1 Spark 3.3 runtime jar. On VM3:

```bash
cd /path/to/ClickPulse
source .venv/bin/activate
export SPARK_MASTER='local[*]'
export SPARK_DRIVER_HOST='172.22.92.97'
export SPARK_DRIVER_BIND_ADDRESS='0.0.0.0'
export HDFS_NAMENODE_URI='hdfs://172.22.184.125:9000'
export HDFS_RAW_PATH='/clickpulse/raw'
export ICEBERG_CATALOG='clickpulse_catalog'
export ICEBERG_CATALOG_TYPE='hadoop'
export ICEBERG_DATABASE='retailmart'
export ICEBERG_TABLE='clickstream_events'
export ICEBERG_REJECTED_TABLE='rejected_events'
export ICEBERG_LEDGER_TABLE='clickpulse_batch_ledger'
export ICEBERG_WAREHOUSE='hdfs://172.22.184.125:9000/clickpulse/warehouse'
export CLICKPULSE_EXPORT_PATH='hdfs://172.22.184.125:9000/clickpulse/exports/current-state'
: "${ICEBERG_RUNTIME_JAR:?set this to the installed iceberg-spark-runtime-3.3_2.12-1.8.1.jar path}"
python scripts/run_spark_pipeline.py \
  --input-path 'hdfs://172.22.184.125:9000/clickpulse/raw/clicks_day1.jsonl' \
  --input-path 'hdfs://172.22.184.125:9000/clickpulse/raw/clicks_day2.jsonl'
```

Set `SPARK_MASTER` to the actual standalone master URL if the VM uses a Spark
cluster instead of local execution. Driver host and bind address must be
reachable from the workers through the VM3 ZeroTier interface. The source
package must be installed on worker Python environments when using executors
on other machines.

## Recovery boundaries

Rejected-event, curated-table, and batch-ledger writes are separate Iceberg
commits. They do not form one atomic transaction. Rejected occurrences use a
SHA-256 ID derived from the source batch ID, source path, and source line number;
an insert-only MERGE makes sequential retries idempotent. The curated snapshot
records the source batch SHA-256 and operation counts. On restart, the pipeline
looks for that marker before applying the same batch again, then advances the
ledger from the observed snapshot. Recovery depends on snapshot metadata
remaining available. The current design has no distributed lease or unique-key
constraint, so simultaneous workers processing the same batch are not
serialized. Do not describe this implementation as exactly-once processing.

## Backfill rejected occurrences

Completed batches retain the normal replay fast path and will not be re-read by
`run_spark_pipeline.py`. To backfill historical rejected occurrences without
changing the curated table or ledger, run the dedicated idempotent script:

```bash
python scripts/backfill_rejected_events.py \
  --input-path 'hdfs://172.22.184.125:9000/clickpulse/raw/clicks_day1.jsonl' \
  --input-path 'hdfs://172.22.184.125:9000/clickpulse/raw/clicks_day2.jsonl'
```

The script computes the original-byte SHA-256 batch IDs, runs the existing
distributed validator, and writes rejected occurrences only. Re-running it
does not update existing rejected rows. The rejected table is configurable via
`ICEBERG_REJECTED_TABLE` and defaults to `rejected_events`.

## Export current state for Druid

Export the complete logical Iceberg table (not its underlying data files) as
Parquet with:

```bash
python scripts/export_current_state.py
```

`CLICKPULSE_EXPORT_PATH` selects the destination and defaults to
`hdfs://<HDFS_NAMENODE_URI host>:9000/clickpulse/exports/current-state`.
The exporter validates non-null unique `event_id` values, stages and reads back
the Parquet output, then promotes it through Hadoop FileSystem directory
renames while retaining the prior export until promotion succeeds.

The old-to-backup and staging-to-destination renames are separate operations,
not one atomic transaction. A process or NameNode failure between them may
leave the destination missing with a backup or staging directory present; the
operator must inspect those paths and restore/promote the appropriate one
before rerunning. The exporter does not automatically infer recovery intent.
This export step does not ingest into or replace a Druid datasource. Verify
Druid ingestion and datasource replacement separately.

The curated `price` representation is `DECIMAL(38,18)`. The Spark path rejects
a validated value that cannot be represented exactly in that type instead of
silently rounding it. A wider price contract would require a coordinated table
schema decision.

## VM3 integration test

Copy the repository datasets to local temporary paths automatically via the
test fixture. Configure `ICEBERG_RUNTIME_JAR` and `CLICKPULSE_RUN_ICEBERG_INTEGRATION=1`
to run `python -m pytest -v tests/integration/test_iceberg_pipeline.py`. The
fixture expects Spark 3.3.4 and creates an isolated local Hadoop catalog
warehouse while using the configured Iceberg runtime jar.

The current-state exporter integration test uses the same environment gate and
can be run with `python -m pytest -v tests/integration/test_export_current_state.py`.
