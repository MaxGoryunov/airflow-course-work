from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="upload_to_hdfs",
    start_date=datetime(2026, 3, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    create_file = BashOperator(
        task_id="create_file",
        bash_command="echo 'Hello from Airflow' > /tmp/test.txt"
    )

    upload_to_hdfs = BashOperator(
        task_id="upload_to_hdfs",
        bash_command="hdfs dfs -mkdir -p /data && hdfs dfs -put -f /tmp/test.txt /data/"
    )

    create_file >> upload_to_hdfs
