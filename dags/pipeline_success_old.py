from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import numpy as np
import random
import json
import time
from datetime import datetime


def generate_json_data(**kwargs):
    timestamp = int(time.time())
    filename = f"data_{timestamp}.json"
    local_path = f"/tmp/{filename}"
    hdfs_path = f"/user/airflow/success/{filename}"
    
    records = [{"user_id": i, "amount": random.choice([50, 100, 150]) + random.randint(0, 20), "item": "item_x"} for i in range(5)]
    
    with open(local_path, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    
    kwargs['ti'].xcom_push(key='local_path', value=local_path)
    kwargs['ti'].xcom_push(key='hdfs_path', value=hdfs_path)


with DAG(
    dag_id="pipeline_success_old",
    start_date=datetime(2026, 3, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    generate = PythonOperator(
        task_id="generate_transactions",
        python_callable=generate_json_data
    )

    # upload = BashOperator(
    #     task_id="upload_to_hdfs",
    #     bash_command="hdfs dfs -put -f /tmp/data.json /user/airflow/success/"
    # )
    upload = BashOperator(
        task_id="upload_to_hdfs",
        bash_command="""
        LOCAL_P="{{ ti.xcom_pull(key='local_path') }}"
        HDFS_P="{{ ti.xcom_pull(key='hdfs_path') }}"
        
        hdfs dfs -put -f $LOCAL_P $HDFS_P && \
        rm $LOCAL_P 
        """
    )

    spark_job = BashOperator(
        task_id="spark_job",
        bash_command="""
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
        /opt/spark-apps/test_spark.py
        """
    )
    
    archive_data = BashOperator(
        task_id="archive_processed_data",
        bash_command="""
        if hdfs dfs -test -e /user/airflow/success/*.json; then
            echo "Moving processed files to archive..."
            hdfs dfs -mv /user/airflow/success/*.json /user/airflow/archive/
        else
            echo "No files found to archive."
        fi
        """
    )

    generate >> upload >> spark_job >> archive_data
