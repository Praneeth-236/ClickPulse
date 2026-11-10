# Integration tests

Add integration tests for:
- Kafka producer -> two-broker Kafka cluster -> consumer.
- Consumer -> raw HDFS persistence.
- Spark -> Iceberg insert/update/drop/reject behavior.
- Airflow retry and replay behavior.
- Iceberg full-state export -> Druid datasource replacement.
- Dashboard filters and KPI correctness.
