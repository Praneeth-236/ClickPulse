"""Airflow orchestration DAG for ClickPulse.

The DAG orchestrates ingestion and processing tasks. Kafka brokers are
long-running infrastructure services and must not be started/stopped here.
"""

# TODO: Implement the DAG after VM connectivity and component interfaces are agreed.
# Required properties: retries, safe reruns, batch identity, stage-level observability.

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator

# Default settings applied to all tasks
default_args = {
    'owner': 'tejas',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2, # Safe reruns & fault tolerance
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'clickpulse_pipeline',
    default_args=default_args,
    description='Orchestrates ClickPulse: Kafka -> HDFS -> Spark -> Druid',
    schedule_interval=None, # Triggered manually for Day 1 and Day 2 runs
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    # 1. VM1 (Laptop 1): Trigger Gurudath's Kafka Producer remotely
    produce_data_vm1 = SSHOperator(
        task_id='produce_kafka_events_vm1',
        ssh_conn_id='vm1_ssh', # You will configure this ID in Airflow UI
        command='python3 /path/to/src/clickpulse/producer/producer.py',
        cmd_timeout=300
    )

    # 2. VM2 (Laptop 2): Run Gurudath's Kafka Consumer locally (lands data in HDFS)
    # Since Airflow is on VM2, we can just use BashOperator instead of SSH
    consume_data_vm2 = BashOperator(
        task_id='consume_to_hdfs_vm2',
        bash_command='python3 /path/to/src/clickpulse/ingestion/consumer.py'
    )

    # 3. VM3 (Laptop 3): Trigger Praneeth's Spark & Iceberg job remotely
    spark_process_vm3 = SSHOperator(
        task_id='spark_transform_and_upsert_vm3',
        ssh_conn_id='vm3_ssh',
        command='spark-submit /path/to/src/clickpulse/spark/etl.py',
        cmd_timeout=900
    )

    # 4. VM4 (Laptop 4): Trigger Shreyas's Druid ingestion remotely
    druid_ingest_vm4 = SSHOperator(
        task_id='sync_druid_serving_layer_vm4',
        ssh_conn_id='vm4_ssh',
        command='curl -X POST -H "Content-Type:application/json" -d @/path/to/src/clickpulse/druid/ingestion_spec.json http://localhost:8888/druid/indexer/v1/task',
        cmd_timeout=300
    )

    # Define the execution order
    produce_data_vm1 >> consume_data_vm2 >> spark_process_vm3 >> druid_ingest_vm4
