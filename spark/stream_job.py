from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("StreamingJob") \
    .config("spark.master", "local[2]") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.readStream \
    .format("text") \
    .load("/user/airflow/stream_input")

words = df.selectExpr("explode(split(value, ' ')) as word")
counts = words.groupBy("word").count()

query = counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("checkpointLocation", "/user/airflow/checkpoints/stream_job") \
    .start()

query.awaitTermination()
