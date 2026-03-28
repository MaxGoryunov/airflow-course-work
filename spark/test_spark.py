#!/usr/bin/env python3
from pyspark.sql import SparkSession
import sys

def main():
    print("=" * 60)
    print("Spark Test Script")
    print("=" * 60)
    
    try:
        # Создание Spark сессии
        spark = SparkSession.builder \
            .appName("TestSparkJob") \
            .config("spark.master", "local[2]") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .getOrCreate()
        
        print(f"✓ Spark version: {spark.version}")
        
        # Создание тестовых данных
        data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
        df = spark.createDataFrame(data, ["name", "age"])
        
        print("\nTest DataFrame:")
        df.show()
        
        # Простая агрегация
        avg_age = df.agg({"age": "avg"}).collect()[0][0]
        print(f"\nAverage age: {avg_age}")
        
        # Сохранение в HDFS
        output_path = "/user/airflow/spark_test_result"
        df.write.mode("overwrite").parquet(output_path)
        print(f"\n✓ Data saved to HDFS: {output_path}")
        
        # Проверка чтения
        df_read = spark.read.parquet(output_path)
        print("\nData read from HDFS:")
        df_read.show()
        
        print("\n" + "=" * 60)
        print("✓ All tests passed!")
        print("=" * 60)
        
        spark.stop()
        return 0
        
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
