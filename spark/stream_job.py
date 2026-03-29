from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("StreamingJob") \
    .config("spark.master", "local[2]") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

# 1. Устанавливаем уровень логирования сразу после создания сессии
spark.sparkContext.setLogLevel("WARN")

# Читаем поток
df = spark.readStream \
    .format("text") \
    .load("/user/airflow/stream_input")

# Обработка
words = df.selectExpr("explode(split(value, ' ')) as word")
counts = words.groupBy("word").count()

# Вывод
query = counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("checkpointLocation", "/user/airflow/checkpoints/stream_job") \
    .start()

query.awaitTermination()