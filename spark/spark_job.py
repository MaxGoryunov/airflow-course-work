#!/usr/bin/env python3
from pyspark.sql import SparkSession
import sys
import os

def main():
    print("=" * 60)
    print("Starting Spark Job")
    print("=" * 60)
    
    # Выводим информацию об окружении
    print(f"Python version: {sys.version}")
    print(f"PYSPARK_PYTHON from env: {os.environ.get('PYSPARK_PYTHON', 'Not set')}")
    
    try:
        # Создание Spark сессии с явными настройками
        spark = SparkSession.builder \
            .appName("AirflowSparkJob") \
            .config("spark.master", "spark://spark-master:7077") \
            .config("spark.executor.memory", "512m") \
            .config("spark.executor.cores", "1") \
            .config("spark.driver.memory", "512m") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .config("spark.dynamicAllocation.enabled", "false") \
            .config("spark.pyspark.python", "/usr/bin/python3") \
            .config("spark.pyspark.driver.python", "/usr/bin/python3") \
            .getOrCreate()
        
        print(f"Spark version: {spark.version}")
        print(f"Spark master: {spark.sparkContext.master}")
        
        sc = spark.sparkContext
        print(f"Default parallelism: {sc.defaultParallelism}")
        
        # Простой тест
        print("\nTesting RDD operation...")
        rdd = sc.parallelize(range(4), 2)
        
        # Функция, которая будет выполняться на executor'ах
        def process(x):
            import os
            return f"Value {x} processed in executor, PYSPARK_PYTHON={os.environ.get('PYSPARK_PYTHON', 'Not set')}"
        
        result = rdd.map(process).collect()
        for r in result:
            print(r)
        
        # Работа с DataFrame
        print("\nCreating DataFrame...")
        data = [("Hello", 1), ("World", 2), ("Airflow", 3), ("Spark", 4)]
        df = spark.createDataFrame(data, ["word", "count"])
        print("DataFrame:")
        df.show()
        
        # Сохранение в HDFS
        output_path = "/user/airflow/spark_test_result"
        print(f"\nSaving to HDFS: {output_path}")
        
        try:
            df.write.mode("overwrite").parquet(output_path)
            print("✓ Data saved to HDFS successfully!")
            
            # Проверка
            df_read = spark.read.parquet(output_path)
            print("\nData read from HDFS:")
            df_read.show()
            
        except Exception as e:
            print(f"Could not save to HDFS: {e}")
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
