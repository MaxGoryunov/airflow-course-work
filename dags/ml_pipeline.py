from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime


with DAG(
    dag_id="ml_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/10 * * * *",
    catchup=False
) as dag:

    run_ml = BashOperator(
        task_id="run_ml",
        bash_command="""
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --conf spark.driver.host=$(hostname -i | awk '{print $1}') \
        --conf spark.driver.bindAddress=0.0.0.0 \
        --conf spark.driver.port=7001 \
        --conf spark.blockManager.port=7002 \
        --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
        /opt/spark-apps/ml_job.py
        """
    )
