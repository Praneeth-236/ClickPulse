# Integration tests

Add integration tests for:
- Kafka producer -> two-broker Kafka cluster -> consumer.
- Consumer -> raw HDFS persistence.
- Spark -> Iceberg insert/update/drop/reject behavior.
- Airflow retry and replay behavior.
- Iceberg full-state export -> Druid datasource replacement.
- Dashboard filters and KPI correctness.

The exporter Iceberg integration test is available at
`tests/integration/test_export_current_state.py`. Set
`CLICKPULSE_RUN_ICEBERG_INTEGRATION=1` and `ICEBERG_RUNTIME_JAR` to run it on
VM3; it checks empty/null/duplicate source rejection, successful replacement,
and preservation of an existing export when staging fails.
