#!/usr/bin/env python3
from pyspark.sql import SparkSession
import sys
import os


def main():
    print("=" * 60)
    print("Starting Spark Job (Local Mode)")
    print("=" * 60)
    print(f"Python version: {sys.version}")
    print(f"PYSPARK_PYTHON: {os.environ.get('PYSPARK_PYTHON', 'Not set')}")
    
    try:
        spark = SparkSession.builder \
            .appName("AirflowSparkJob") \
            .config("spark.master", "local[2]") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .getOrCreate()
        
        print(f"Spark version: {spark.version}")
        print(f"Spark master: {spark.sparkContext.master}")
        
        sc = spark.sparkContext
        print(f"Default parallelism: {sc.defaultParallelism}")
        
        print("\nTesting basic RDD operation...")
        rdd = sc.parallelize(range(10), 2)
        result = rdd.map(lambda x: x * 2).collect()
        print(f"RDD test result: {result[:5]}...")
        
        print("\nCreating DataFrame...")
        data = [("Hello", 1), ("World", 2), ("Airflow", 3)]
        df = spark.createDataFrame(data, ["word", "count"])
        print("DataFrame created:")
        df.show()
        
        output_path = "/user/airflow/spark_test_result"
        print(f"\nSaving to HDFS: {output_path}")
        
        try:
            df.write.mode("overwrite").parquet(output_path)
            print("Data saved to HDFS successfully!")
            
            df_read = spark.read.parquet(output_path)
            print("\nData read from HDFS:")
            df_read.show()
            
        except Exception as e:
            print(f"Warning: Could not save to HDFS: {e}")
            local_path = "/tmp/spark_test_result"
            df.write.mode("overwrite").parquet(local_path)
            print(f"Data saved locally to: {local_path}")
        
        print("\n" + "=" * 60)
        print("Spark Job Completed Successfully!")
        print("=" * 60)
        
        spark.stop()
        return 0
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
