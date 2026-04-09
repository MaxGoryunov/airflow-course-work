#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum
import sys


def main():
    print("=" * 60)
    print("Spark Json Reader Script")
    print("=" * 60)
    
    try:
        spark = SparkSession.builder \
            .appName("HdfsToClickhouseJob") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .getOrCreate()
        
        input_path = "/user/airflow/success/*.json"
        print(f"Reading data from: {input_path}")
        df = spark.read.json(input_path)
        if df.rdd.isEmpty():
            print("No data found in HDFS success folder.")
            return 0
        print("\nInput Data Schema:")
        df.printSchema()
        df.show(5)
        
        result_df = df.select(spark_sum("amount").alias("total_amount"))
        total_val = result_df.collect()[0]["total_amount"]
        print(f"\nTotal amount calculated: {total_val}")
        
        output_path = "/user/airflow/spark_test_result"
        result_df.write.mode("overwrite").parquet(output_path)
        print(f"Aggregated result saved to: {output_path}")

        print("\n" + "=" * 60)
        print("OK Job Finished Successfully")
        print("=" * 60)
        spark.stop()
        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
