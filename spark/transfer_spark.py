#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import clickhouse_connect
import sys


def main():
    spark = SparkSession.builder \
        .appName("HDFS_to_ClickHouse_Batch") \
        .getOrCreate()
        # .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    
    schema = StructType([
        StructField("item", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("ts", LongType(), True)
    ])

    try:
        input_path = "hdfs://namenode:9000/user/airflow/success/*.json"
        df = spark.read.schema(schema).json(input_path)
        if df.count() == 0:
            print("No data to process.")
            spark.stop()
            return 0
        aggregated_df = df.groupBy("item") \
            .agg(_sum("amount").alias("total_amount")) \
            .withColumn("processed_at", current_timestamp())
        data_to_ch = [tuple(row) for row in aggregated_df.collect()]
        if not data_to_ch:
            print("Aggregation result is empty.")
            return
        
        client = clickhouse_connect.get_client(host='clickhouse', port=8123)
        client.command("""
            CREATE TABLE IF NOT EXISTS streaming.sales_stats (
                item String,
                total_amount Float64,
                processed_at DateTime
            ) ENGINE = MergeTree() ORDER BY processed_at
        """)
        client.insert("streaming.sales_stats", data_to_ch, 
                      column_names=["item", "total_amount", "processed_at"])
        print(f"Successfully loaded {len(data_to_ch)} rows to ClickHouse.")
        spark.stop()
    except Exception as e:
        print(f"Error in Spark job: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
