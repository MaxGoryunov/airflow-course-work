#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType
import clickhouse_connect
import subprocess
import sys


def main():
    spark = SparkSession.builder.appName("Request_Log_Transfer").getOrCreate()

    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("t", LongType(), True),
        StructField("hour", IntegerType(), True),
        StructField("requests", LongType(), True),
        StructField("size", DoubleType(), True),
        StructField("y", DoubleType(), True),
        StructField("y_lag1", DoubleType(), True),
        StructField("drift_factor", DoubleType(), True)
    ])

    try:
        input_path = "hdfs://namenode:9000/user/airflow/success/*.json"
        df = spark.read \
            .schema(schema) \
            .option("allowEmptyInput", "true") \
            .json(input_path)
        
        if df.count() == 0:
            print("No data to process.")
            spark.stop()
            return 0

        client = clickhouse_connect.get_client(host='clickhouse', port=8123)
        client.command("""
            CREATE TABLE IF NOT EXISTS streaming.request_logs (
                timestamp DateTime64(3),
                t Int64,
                hour Int8,
                requests Int64,
                size Float64,
                y Float64,
                drift_factor Float64,
                processed_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree() ORDER BY timestamp
        """)
        
        # insert_data = [(r[0], r[1], r[2], r[3], r[4], r[5], r[7]) for r in data_to_ch]
        insert_data = df.select("timestamp", "t", "hour", "requests", "size", "y", "drift_factor").collect()
        
        client.insert("streaming.request_logs", insert_data, 
                      column_names=["timestamp", "t", "hour", "requests", "size", "y", "drift_factor"])
        
        print(f"Loaded {len(insert_data)} request logs.")
        
        move_cmd = "hdfs dfs -mv /user/airflow/success/*.json /user/airflow/processing/"
        result = subprocess.run(move_cmd, shell=True, capture_output=True)

        if result.returncode == 0:
            print("Files moved to processing successfully.")
        else:
            print(f"Failed to move files: {result.stderr}")
        spark.stop()
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
