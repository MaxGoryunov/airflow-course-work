from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import clickhouse_connect
import random


def unstable_task():
    if random.random() < 0.7:
        raise Exception("Random error")
    print("Успех!")


# def check_db_availability():
#     if random.random() < 0.7:
#         raise Exception("Database is busy: Too many concurrent connections")
#     print("Database is ready for ML job!")


def check_db_availability():
    # try:
    #     if random.random() < 0.3:
    #         raise Exception("Database is busy: Too many concurrent connections.")
    #     client = clickhouse_connect.get_client(host='clickhouse', port=8123)
    #     client.command("SELECT 1")
    #     print("ClickHouse is accessible.")
    # except Exception as e:
    #     print(f"Database connection failed: {e}")
    #     raise ConnectionError("ClickHouse not reachable")
    if random.random() < 0.3:
        raise Exception("ClickHouse is currently overloaded (Simulated Error)")
    client = clickhouse_connect.get_client(host='clickhouse', port=8123)
    client.command("SELECT 1")

with DAG(
    dag_id="pipeline_fail_retry",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    check_db = PythonOperator(
        task_id="check_db",
        python_callable=check_db_availability,
        retries=5,
        retry_delay=10,
    )
    
    run_ml = BashOperator(
        task_id="run_spark_ml",
        bash_command="""
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark-apps/ml_job.py
        """
    )
    
    check_db >> run_ml
