from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HDFS Word Count") \
    .getOrCreate()

df = spark.read.text("hdfs://namenode:9000/data/test.txt")

words = df.selectExpr("explode(split(value, ' ')) as word")
word_count = words.groupBy("word").count()
word_count.show()
word_count.write.mode("overwrite").csv("hdfs://namenode:9000/data/output")

spark.stop()
