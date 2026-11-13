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
HDFS_URI = "hdfs://172.22.184.125:9000"
ENV_PREFIX = (
    f"set -euo pipefail && "
    f"export SPARK_HOME=/opt/spark && "
    f"export HADOOP_HOME=${{HADOOP_HOME:-/opt/hadoop}} && "
    f"export PATH=/opt/spark/bin:$HADOOP_HOME/bin:$HOME/ClickPulse/.venv/bin:$PATH && "
    f"export PYTHONPATH={REPO_DIR}/src && "
    f"cd {REPO_DIR}"
)

CONSUME_CMD = f"""{ENV_PREFIX}
OUTPUT=$({VENV_PY} -u src/clickpulse/ingestion/consumer.py --idle-timeout-ms 10000)
echo "$OUTPUT" >&2
HDFS_REL_PATH=$(echo "$OUTPUT" | grep -oE '/clickpulse/raw/[^ )]+\\.jsonl' | tail -n 1 || true)
if [ -z "$HDFS_REL_PATH" ]; then
    HDFS_REL_PATH=$(hdfs dfs -ls -t /clickpulse/raw/day*/*.jsonl 2>/dev/null | awk 'NR==1 {{print $NF}}')
fi
if [ -z "$HDFS_REL_PATH" ]; then
    echo "ERROR: No raw batch file found in HDFS." >&2
    exit 1
fi
echo "{HDFS_URI}${{HDFS_REL_PATH}}"
"""

with DAG(
    'clickpulse_pipeline',
    default_args=default_args,
    description='Orchestrates ClickPulse: Producer (VM1) -> Consumer/HDFS (VM2) -> Spark Pipeline (VM3) -> Druid (VM4)',
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    # 1. VM1 (Gurudath): Publish clickstream events to Kafka
    produce_data_vm1 = SSHOperator(
        task_id='produce_kafka_events_vm1',
        ssh_conn_id='vm1_ssh',
        command=f'{ENV_PREFIX} && {VENV_PY} src/clickpulse/producer/main.py',
        cmd_timeout=300,
    )

    # 2. VM2 (Tejas): Ingest raw Kafka events to HDFS and push HDFS URI via XCom
    consume_data_vm2 = BashOperator(
        task_id='consume_to_hdfs_vm2',
        bash_command=CONSUME_CMD,
        do_xcom_push=True,
    )

    # 3. VM3 (Praneeth): Unified Spark validation, deduplication, ledger & Iceberg upsert
    spark_pipeline_vm3 = SSHOperator(
        task_id='spark_pipeline_vm3',
        ssh_conn_id='vm3_ssh',
        command=(
            f'{ENV_PREFIX} && spark-submit src/clickpulse/spark/pipeline.py '
            f'--input-path "{{{{ ti.xcom_pull(task_ids=\'consume_to_hdfs_vm2\') }}}}"'
        ),
        cmd_timeout=900,
    )

    # 4. VM4 (Shreyas): Export curated state to Apache Druid
    druid_export_vm4 = SSHOperator(
        task_id='druid_export_current_state_vm4',
        ssh_conn_id='vm4_ssh',
        command=f'{ENV_PREFIX} && {VENV_PY} src/clickpulse/druid/export_current_state.py',
        cmd_timeout=300,
    )

    produce_data_vm1 >> consume_data_vm2 >> spark_pipeline_vm3 >> druid_export_vm4
