"""Airflow orchestration DAG for ClickPulse.

The DAG orchestrates ingestion and processing tasks. Kafka brokers are
long-running infrastructure services and must not be started/stopped here.
"""

# TODO: Implement the DAG after VM connectivity and component interfaces are agreed.
# Required properties: retries, safe reruns, batch identity, stage-level observability.
