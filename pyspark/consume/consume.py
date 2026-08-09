import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, DoubleType
from pyspark.sql.functions import from_json

# Broker and topic come from the environment so the same script runs against a
# local compose stack or a remote broker. See .env.example.
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:19092")
SPARK_TOPIC = os.getenv("SPARK_TOPIC", "stock_json_topic_spark")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "earliest")

spark_session = SparkSession\
    .builder\
    .appName("RedpandaSparkStream")\
    .getOrCreate()


stream = spark_session\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)\
    .option("subscribe", SPARK_TOPIC)\
    .option("startingOffsets", STARTING_OFFSETS)\
    .load()

spark_session.sparkContext.setLogLevel('WARN')

stream.printSchema()

json_schema = StructType([
    StructField('event_time', StringType(), True), \
    StructField('ticker', StringType(), True), \
    StructField('price', DoubleType(), True) \
])

# Parse value from binay to string
json_df = stream.selectExpr("cast(value as string) as value")

# Apply Schema to JSON value column and expand the value
json_expanded_df = json_df.withColumn("value", from_json(json_df["value"], json_schema)).select("value.*") 

json_expanded_df.printSchema()

query = json_expanded_df \
    .writeStream \
    .format("console") \
    .start()

query.awaitTermination()
