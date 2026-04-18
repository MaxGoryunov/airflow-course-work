from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorAssembler
import pyspark.sql.functions as F
import datetime
import clickhouse_connect


def get_existing_paths(spark, paths):
    """Leaves only paths existing in HDFS"""
    sc = spark.sparkContext
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    conf = sc._jsc.hadoopConfiguration()
    fs = FileSystem.get(conf)
    
    existing_paths = []
    for p in paths:
        base_path = p.replace("/*.json", "")
        if fs.exists(Path(base_path)):
            existing_paths.append(p)
    return existing_paths


def safe_read_json(spark, path, schema):
    """Tries to read JSON. If the path is empty or not found returns empty dataframe with schema."""
    try:
        return spark.read.schema(schema).json(path)
    except Exception as e:
        print(f"Warning: Path {path} not found or empty. Returning empty DataFrame.")
        return spark.createDataFrame([], schema)


def main():
    spark = SparkSession.builder.appName("ML_Sliding_Window_Training").getOrCreate()
    
    # input_paths = [
    #     "hdfs://namenode:9000/user/airflow/archive/*.json",
    #     "hdfs://namenode:9000/user/airflow/success/*.json"
    # ]

    # valid_paths = get_existing_paths(spark, input_paths)
    # print("Valid paths:")
    # print(valid_paths)
    # if not valid_paths:
    #     print("Данные не найдены ни в одной из директорий.")
    #     spark.stop()
    #     exit(0)

    # df = spark.read.format("json") \
    #     .option("allowEmptyInput", "true") \
    #     .load(valid_paths)
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("t", LongType(), True),
        StructField("hour", IntegerType(), True),
        StructField("requests", LongType(), True),
        StructField("size", DoubleType(), True),
        StructField("y", DoubleType(), True),
        StructField("y_lag1", DoubleType(), True),
        StructField("drift_factor", DoubleType(), True)
    ])
    
    df_archive = safe_read_json(spark, "hdfs://namenode:9000/user/airflow/archive/*.json", schema)
    df_success = safe_read_json(spark, "hdfs://namenode:9000/user/airflow/success/*.json", schema)
    df = df_archive.union(df_success)
    
    if df.count() == 0:
        print("Neither success nor archive contains data for processing.")
        spark.stop()
        return
    else:
        print("Found data in success or archive. Proceeding to train the model.")
    
    assembler = VectorAssembler(inputCols=["requests", "size", "hour", "y_lag1"], outputCol="features")
    df = assembler.transform(df).orderBy("t")
    
    window_size = 100
    batch_size = 20
    all_metrics = []
    
    t_bounds = df.select(F.min("t"), F.max("t")).collect()[0]
    start_t, end_t = t_bounds[0], t_bounds[1]
    
    print("Calculating window stats")
    for end in range(start_t + window_size, end_t, batch_size):
        train_df = df.filter((F.col("t") < end) & (F.col("t") >= end - window_size))
        test_df = df.filter((F.col("t") >= end) & (F.col("t") < end + batch_size))
        
        if test_df.count() == 0: continue

        model = RandomForestRegressor(featuresCol="features", labelCol="y", numTrees=10).fit(train_df)
        preds = model.transform(test_df)
        
        stats = preds.select(
            F.lit(end).alias("t_end"),
            F.avg(F.abs(F.col("y") - F.col("prediction"))).alias("mae"),
            F.avg("prediction").alias("mean_pred")
        ).collect()[0]
        
        all_metrics.append((stats['t_end'], float(stats['mae']), float(stats['mean_pred'])))

    print("Creating result table")
    client = clickhouse_connect.get_client(host='clickhouse', port=8123)
    client.command("""
        CREATE TABLE IF NOT EXISTS streaming.ml_performance (
            t_end Int64, mae Float64, mean_pred Float64, processed_at DateTime
        ) ENGINE = MergeTree() ORDER BY t_end
    """)
    
    if all_metrics:
        processed_at = datetime.datetime.now()
        data_to_insert = [m + (processed_at,) for m in all_metrics]
        print("Storing data")
        client.insert("streaming.ml_performance", data_to_insert, 
                      column_names=["t_end", "mae", "mean_pred", "processed_at"])
    
    print("Finish")
    spark.stop()


if __name__ == "__main__":
    main()
