from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, FloatType
import clickhouse_connect


spark = SparkSession.builder \
    .appName("MLJob") \
    .config("spark.master", "spark://spark-master:7077") \
    .getOrCreate()

client = clickhouse_connect.get_client(host='clickhouse', port=8123)

query = "SELECT total_amount FROM streaming.sales_stats ORDER BY processed_at DESC LIMIT 100"
data = client.query(query).result_rows
schema = StructType([StructField("amount", FloatType(), True)])

if not data:
    print("No ClickHouse data. Generating empty dtaframe as placeholder.")
    df = spark.createDataFrame([], schema=schema)
else:
    df = spark.createDataFrame(data, schema=schema)


if df.count() < 2:
    print("Not enough data points (at least 2 are required).")
    spark.stop()
    exit(0)

# === ML pipeline ===
df = df.withColumn("index", monotonically_increasing_id())
assembler = VectorAssembler(inputCols=["index"], outputCol="features")
df = assembler.transform(df)

# Linear regression
lr = LinearRegression(featuresCol="features", labelCol="amount")
model = lr.fit(df)

# Results
coeff = model.coefficients[0]
intercept = model.intercept
print(f"Model trained: y = {coeff}x + {intercept}")

client.command("""
CREATE TABLE IF NOT EXISTS streaming.model_results (
    coeff Float64,
    intercept Float64,
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY updated_at
""")

client.insert(
    "streaming.model_results",
    [(float(coeff), float(intercept))],
    column_names=["coeff", "intercept"]
)

spark.stop()
