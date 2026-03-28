from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("SparkJob") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

# Пример: создание данных и запись в HDFS
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
columns = ["name", "age"]
df = spark.createDataFrame(data, columns)

df.write.mode("overwrite").parquet("hdfs://namenode:9000/user/airflow/output")

print("Job completed successfully!")
spark.stop()