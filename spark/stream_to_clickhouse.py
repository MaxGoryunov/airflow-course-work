from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split
import clickhouse_connect
import sys


spark = SparkSession.builder \
    .appName("StreamToClickHouse") \
    .config("spark.master", "local[2]") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

try:
    client = clickhouse_connect.get_client(host='clickhouse', port=8123)
    client.command("CREATE DATABASE IF NOT EXISTS streaming")
    client.command("""
        CREATE TABLE IF NOT EXISTS streaming.word_count (
            word String,
            count UInt64
        ) ENGINE = Memory
    """)
    client.command("TRUNCATE TABLE streaming.word_count")
    print("ClickHouse: Created empty table word_count.")
except Exception as e:
    print(f"Ошибка при подготовке ClickHouse: {e}")
    sys.exit(1)

df = spark.readStream \
    .format("text") \
    .load("/user/airflow/stream_input")

words = df.select(explode(split(df.value, " ")).alias("word"))
counts = words.groupBy("word").count()


# def write_to_clickhouse(batch_df, batch_id):
#     client = clickhouse_connect.get_client(
#         host='clickhouse',
#         port=8123
#     )
#     rows = batch_df.collect()
#     data = [(row['word'], row['count']) for row in rows]
#     if data:
#         client.insert(
#             "streaming.word_count",
#             data,
#             column_names=["word", "count"]
#         )
#         print(f"Inserted {len(data)} rows into ClickHouse")
# def write_to_clickhouse(batch_df, batch_id):
#     try:
#         client = clickhouse_connect.get_client(host='clickhouse', port=8123)
#         rows = batch_df.collect()
#         data = [(row['word'], row['count']) for row in rows]
#         if data:
#             client.insert(
#                 "streaming.word_count",
#                 data,
#                 column_names=["word", "count"]
#             )
#             print(f"OK Batch {batch_id}: Inserted {len(data)} rows")
#     except Exception as e:
#         print(f"ERROR in Batch {batch_id}: {e}")
#         raise e
def write_to_clickhouse(batch_df, batch_id):
    # Здесь мы снова берем клиент, но уже для вставки данных
    internal_client = clickhouse_connect.get_client(host='clickhouse', port=8123)
    rows = batch_df.collect()
    data = [(row['word'], row['count']) for row in rows]
    if data:
        internal_client.insert(
            "streaming.word_count",
            data,
            column_names=["word", "count"]
        )
        print(f"Batch {batch_id}: Записано {len(data)} строк")


# query = counts.writeStream \
#     .outputMode("complete") \
#     .foreachBatch(write_to_clickhouse) \
#     .start()
query = counts.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_to_clickhouse) \
    .option("checkpointLocation", "/user/airflow/checkpoints/clickhouse_sync") \
    .start()

query.awaitTermination()
