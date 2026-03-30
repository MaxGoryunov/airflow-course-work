from pyspark.sql import SparkSession
print("Creating Spark session...")
spark = SparkSession.builder.appName("test").getOrCreate()
print(f"Spark version: {spark.version}")
print("SUCCESS")
spark.stop()
