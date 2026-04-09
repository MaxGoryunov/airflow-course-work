from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import time
import random
import os


def generate_json_data(**kwargs):
    ts = int(time.time())
    filename = f"data_{ts}.json"
    local_path = f"/tmp/{filename}"
    print(local_path)
    items = ["laptop", "mouse", "keyboard", "monitor"]
    records = [
        {"item": random.choice(items), "amount": random.choice([50, 100, 150]) + random.randint(0, 20), "ts": ts}
        for _ in range(5)
    ]
    
    with open(local_path, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    kwargs['ti'].xcom_push(key='file_info', value={"local": local_path, "name": filename})


with DAG(
    dag_id="stream_generator",
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/1 * * * *",
    catchup=False
) as dag:
    generate = PythonOperator(
        task_id="gen_json",
        python_callable=generate_json_data,
    )
    upload = BashOperator(
        task_id="upload_to_hdfs",
        bash_command="""
        L_PATH="{{ ti.xcom_pull(key='file_info')['local'] }}"
        F_NAME="{{ ti.xcom_pull(key='file_info')['name'] }}"
        hdfs dfs -put -f $L_PATH /user/airflow/success/$F_NAME && rm $L_PATH
        """
    )
    generate >> upload
    # generate = BashOperator(
    #     task_id="generate_stream",
    #     bash_command="""
    #     FILENAME="file_$(date +%s).txt" && \
    #     echo "hello streaming $(date)" > /tmp/$FILENAME && \
    #     hdfs dfs -put -f /tmp/$FILENAME /user/airflow/stream_input/ && \
    #     rm /tmp/$FILENAME
    #     """
    # )
