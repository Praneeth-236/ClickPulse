from datetime import datetime, timedelta
from airflow import DAG

try:
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:
    from airflow.operators.bash import BashOperator

from airflow.providers.ssh.operators.ssh import SSHOperator

default_args = {
    'owner': 'tejas',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

REPO_DIR = "$HOME/ClickPulse"
VENV_PY = f"{REPO_DIR}/.venv/bin/python3"
ENV_PREFIX = f"cd {REPO_DIR} && PYTHONPATH={REPO_DIR}/src"

with DAG(
    'clickpulse_pipeline',
    default_args=default_args,
    description='Orchestrates ClickPulse: Producer -> Consumer/HDFS -> Spark Dedupe & Upsert -> Druid',
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    # 1. VM1 (Gurudath): Run Kafka Producer remotely
    produce_data_vm1 = SSHOperator(
        task_id='produce_kafka_events_vm1',
        ssh_conn_id='vm1_ssh',
        command=f'{ENV_PREFIX} {VENV_PY} src/clickpulse/producer/main.py',
        cmd_timeout=300,
    )

    # 2. VM2 (Gurudath + Tejas): Run Kafka Consumer locally to write raw data to HDFS
    consume_data_vm2 = BashOperator(
        task_id='consume_to_hdfs_vm2',
        bash_command=f'{ENV_PREFIX} {VENV_PY} src/clickpulse/ingestion/consumer.py',
    )

    # 3. VM3 (Praneeth): Validate and deduplicate raw HDFS events with Spark
    spark_dedupe_vm3 = SSHOperator(
        task_id='spark_validate_and_deduplicate_vm3',
        ssh_conn_id='vm3_ssh',
        command=f'{ENV_PREFIX} spark-submit src/clickpulse/spark/validate_and_deduplicate.py',
        cmd_timeout=900,
    )

    # 4. VM3 (Praneeth): Upsert curated Iceberg tables with Spark
    spark_upsert_vm3 = SSHOperator(
        task_id='spark_upsert_curated_vm3',
        ssh_conn_id='vm3_ssh',
        command=f'{ENV_PREFIX} spark-submit src/clickpulse/spark/upsert_curated.py',
        cmd_timeout=900,
    )

    # 5. VM4 (Shreyas): Export current state to Druid serving layer
    druid_export_vm4 = SSHOperator(
        task_id='druid_export_current_state_vm4',
        ssh_conn_id='vm4_ssh',
        command=f'{ENV_PREFIX} {VENV_PY} src/clickpulse/druid/export_current_state.py',
        cmd_timeout=300,
    )

    produce_data_vm1 >> consume_data_vm2 >> spark_dedupe_vm3 >> spark_upsert_vm3 >> druid_export_vm4
