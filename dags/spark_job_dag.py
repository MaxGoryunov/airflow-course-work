from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="spark_job_dag",
    start_date=datetime(2026, 3, 1),
    schedule_interval=None,
    catchup=False,
    tags=['spark']
) as dag:

    run_spark_job = BashOperator(
        task_id="run_spark_job",
        bash_command="""
        /opt/spark/bin/spark-submit \
            --master local[1] \
            --driver-memory 512m \
            --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
            /opt/spark-apps/spark_job.py
        """,
        env={
            "SPARK_HOME": "/opt/spark",
            "JAVA_HOME": "/usr/lib/jvm/temurin-11-jdk-amd64",
            "HADOOP_CONF_DIR": "/opt/spark/conf",
        }
    )
