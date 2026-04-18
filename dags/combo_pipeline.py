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


with DAG(
    dag_id="combined_pipeline",
    start_date=datetime(2026, 3, 1),
    schedule_interval="*/10 * * * *",
    catchup=False
) as dag:
    spark_task = BashOperator(
        task_id="spark_transfer",
        bash_command="""
if hdfs dfs -test -e /user/airflow/success/*.json; then
    DRIVER_IP=$(hostname -i | awk '{print $1}')
    /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --executor-memory 512m \
    --driver-memory 512m \
    --conf "spark.driver.host=${DRIVER_IP}" \
    --conf spark.driver.bindAddress=0.0.0.0 \
    --conf spark.driver.port=7001 \
    --conf spark.blockManager.port=7002 \
    --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
    /opt/spark-apps/request_spark.py
else
    echo "Files not found, skipping Spark job."
fi
        """
    )
    # DRIVER_IP=$(hostname -I | tr ' ' '\n' | grep '172.18' | head -n 1)
    run_ml = BashOperator(
        task_id="run_ml",
        bash_command="""
DRIVER_IP=$(hostname -i | awk '{print $1}')
/opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
--executor-memory 512m \
--driver-memory 512m \
--conf "spark.driver.host=${DRIVER_IP}" \
--conf spark.driver.bindAddress=0.0.0.0 \
--conf spark.driver.port=7001 \
--conf spark.blockManager.port=7002 \
--conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
/opt/spark-apps/forest_job.py
        """
    )
    
    archive_task = BashOperator(
        task_id="archive_data",
        bash_command="""
if hdfs dfs -test -e /user/airflow/success/*.json; then
    hdfs dfs -mv /user/airflow/success/*.json /user/airflow/archive/
fi

echo "Starting cleanup of files older than 7 days..."
current_time=$(date +%s)
hdfs dfs -ls /user/airflow/archive/*.json 2>/dev/null | while read f; do
    f_date=$(echo $f | awk '{print $6" "$7}')
    f_time=$(date -d "$f_date" +%s)
    f_path=$(echo $f | awk '{print $8}')
    
    if [ $(( (current_time - f_time) / 60 )) -gt 10080 ]; then
        echo "Deleting old archive file: $f_path"
        hdfs dfs -rm $f_path
    fi
done
        """
    )
    spark_task >> run_ml >> archive_task
