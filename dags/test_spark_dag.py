from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import os


def test_hdfs():
    """Тестовая функция для проверки HDFS"""
    import subprocess
    result = subprocess.run(['hdfs', 'dfs', '-ls', '/user/airflow'], 
                          capture_output=True, text=True)
    print(f"HDFS content: {result.stdout}")
    if result.returncode != 0:
        print(f"HDFS error: {result.stderr}")
        return False
    return True


def test_spark():
    """Тестовая функция для проверки Spark"""
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName("Test") \
        .master("local[2]") \
        .getOrCreate()
    
    print(f"Spark version: {spark.version}")
    
    data = [("test", 1)]
    df = spark.createDataFrame(data, ["word", "count"])
    df.show()
    
    spark.stop()
    return True

with DAG(
    dag_id="test_system_dag",
    start_date=datetime(2026, 3, 1),
    schedule_interval=None,
    catchup=False,
    tags=['test']
) as dag:

    # Создание файла
    create_file = BashOperator(
        task_id="create_test_file",
        bash_command="echo 'Hello Airflow' > /tmp/test_airflow.txt"
    )
    
    # Загрузка в HDFS
    upload_to_hdfs = BashOperator(
        task_id="upload_to_hdfs",
        bash_command="hdfs dfs -put -f /tmp/test_airflow.txt /user/airflow/"
    )
    
    # Проверка HDFS
    check_hdfs = PythonOperator(
        task_id="check_hdfs",
        python_callable=test_hdfs
    )
    
    # Spark задача
    run_spark = BashOperator(
        task_id="run_spark_test",
        bash_command="""
        /opt/spark/bin/spark-submit \
            --master local[2] \
            --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
            /opt/spark-apps/test_spark.py
        """
    )
    
    # Проверка Spark
    check_spark = PythonOperator(
        task_id="check_spark",
        python_callable=test_spark
    )
    
    # Чтение из HDFS
    read_from_hdfs = BashOperator(
        task_id="read_from_hdfs",
        bash_command="hdfs dfs -cat /user/airflow/spark_test_result/part-*.parquet 2>/dev/null || echo 'No result yet'"
    )
    
    create_file >> upload_to_hdfs >> check_hdfs >> run_spark >> check_spark >> read_from_hdfs
