from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import os


with DAG(
    dag_id="stream_generator",
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/1 * * * *",
    catchup=False
) as dag:

    generate = BashOperator(
        task_id="generate_stream",
        bash_command="""
        # Создаем уникальное имя один раз и сохраняем в переменную
        FILENAME="file_$(date +%s).txt" && \
        echo "hello streaming $(date)" > /tmp/$FILENAME && \
        hdfs dfs -put -f /tmp/$FILENAME /user/airflow/stream_input/ && \
        rm /tmp/$FILENAME
        """
    )