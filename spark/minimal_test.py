#!/usr/bin/env python3
from pyspark.sql import SparkSession
import sys
import os

print(f"Python version: {sys.version}")
print(f"PYSPARK_PYTHON: {os.environ.get('PYSPARK_PYTHON', 'Not set')}")

try:
    spark = SparkSession.builder \
        .appName("MinimalTest") \
        .config("spark.master", "local[1]") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()
    
    print(f"Spark version: {spark.version}")
    
    data = [1, 2, 3, 4, 5]
    rdd = spark.sparkContext.parallelize(data, 1)
    result = rdd.map(lambda x: x * 2).collect()
    print(f"Result: {result}")
    
    spark.stop()
    print("SUCCESS")
    
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
