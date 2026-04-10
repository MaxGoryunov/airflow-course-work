from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime


with DAG(
    dag_id="ml_pipeline",
    start_date=datetime(2026, 3, 1),
    schedule_interval="*/10 * * * *",
    catchup=False
) as dag:

    run_ml = BashOperator(
        task_id="run_ml",
        bash_command="""
DRIVER_IP=$(hostname -I | tr ' ' '\n' | grep '172.18' | head -n 1)
/opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
--executor-memory 512m \
--driver-memory 1g \
--conf "spark.driver.host=${DRIVER_IP}" \
--conf spark.driver.bindAddress=0.0.0.0 \
--conf spark.driver.port=7001 \
--conf spark.blockManager.port=7002 \
--conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
/opt/spark-apps/ml_job.py
        """
    )
