from pyspark.sql import SparkSession
import sys

print(f"Python version: {sys.version}")
print("Creating Spark session...")

spark = SparkSession.builder \
    .appName("ImportTest") \
    .getOrCreate()

print(f"Spark version: {spark.version}")
print("SUCCESS: all imports work")

rdd = spark.sparkContext.parallelize(range(10))
print(f"Sum: {rdd.sum()}")

spark.stop()
