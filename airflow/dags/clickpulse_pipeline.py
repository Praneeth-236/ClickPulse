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

default_args = {
    'owner': 'tejas',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

# Assume standard repo clone path on all VMs: ~/ClickPulse
REPO_DIR = "~/ClickPulse"

with DAG(
    'clickpulse_pipeline',
    default_args=default_args,
    description='Orchestrates ClickPulse: Producer -> Consumer/HDFS -> Spark/Iceberg -> Druid',
    schedule_interval=None,  # Triggered manually for Day 1 and Day 2 runs
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    # 1. VM1 (Gurudath): Run Kafka Producer remotely
    produce_data_vm1 = SSHOperator(
        task_id='produce_kafka_events_vm1',
        ssh_conn_id='vm1_ssh',
        command=f'python3 {REPO_DIR}/src/clickpulse/producer/producer.py',
        cmd_timeout=300
    )

    # 2. VM2 (Gurudath + Tejas): Run Kafka Consumer locally on VM2 to write raw data to HDFS
    consume_data_vm2 = BashOperator(
        task_id='consume_to_hdfs_vm2',
        bash_command=f'python3 {REPO_DIR}/src/clickpulse/ingestion/consumer.py'
    )

    # 3. VM3 (Praneeth): Run Spark ETL & Iceberg Upsert remotely
    spark_process_vm3 = SSHOperator(
        task_id='spark_transform_and_upsert_vm3',
        ssh_conn_id='vm3_ssh',
        command=f'spark-submit {REPO_DIR}/src/clickpulse/spark/spark_processor.py',
        cmd_timeout=900
    )

    # 4. VM4 (Shreyas): Trigger Druid Ingestion remotely
    druid_ingest_vm4 = SSHOperator(
        task_id='sync_druid_serving_layer_vm4',
        ssh_conn_id='vm4_ssh',
        command=f'python3 {REPO_DIR}/src/clickpulse/druid/ingest.py',
        cmd_timeout=300
    )

    # Execution Order
    produce_data_vm1 >> consume_data_vm2 >> spark_process_vm3 >> druid_ingest_vm4

    # Define the execution order
    produce_data_vm1 >> consume_data_vm2 >> spark_process_vm3 >> druid_ingest_vm4
