# Spark and Iceberg batch processing

The Spark job reads JSONL paths directly from HDFS, validates and deduplicates
with Spark DataFrames, merges winners into the configured Iceberg current-state
table, and stores batch lifecycle records in an Iceberg ledger table. Configure
the values in `.env` or export environment variables before running the script.

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
export ICEBERG_LEDGER_TABLE='clickpulse_batch_ledger'
export ICEBERG_WAREHOUSE='hdfs://172.22.184.125:9000/clickpulse/warehouse'
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

The curated-table MERGE and batch-ledger MERGE are separate Iceberg commits.
The curated snapshot records the source batch SHA-256 and operation counts. On
restart, the pipeline looks for that marker before applying the same batch
again, then advances the ledger from the observed snapshot. This is a recovery
protocol, not a transaction spanning both tables. Recovery depends on snapshot
metadata remaining available. The current Iceberg ledger has no distributed
lease or unique-key constraint, so simultaneous workers processing the same
new batch are not serialized. Do not describe this implementation as
exactly-once processing.

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
