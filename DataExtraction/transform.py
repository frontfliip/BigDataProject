from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, sum as spark_sum, count as spark_count
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, DoubleType, TimestampType

kafka_bootstrap_servers = "kafka:9092"
input_topic_name = "crypto"

spark = SparkSession \
    .builder \
    .appName("BitMexCryptoStream") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
    .config("spark.sql.streaming.checkpointLocation", "/opt/app/spark-checkpoint") \
    .getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", input_topic_name) \
    .option("startingOffsets", "earliest") \
    .load()

data = df.selectExpr("CAST(value AS STRING) as json")

schema = StructType([
    StructField("tag", StringType(), True),
    StructField("data", StructType([
        StructField("table", StringType(), True),
        StructField("action", StringType(), True),
        StructField("data", ArrayType(StructType([
            StructField("symbol", StringType(), True),
            StructField("id", IntegerType(), True),
            StructField("side", StringType(), True),
            StructField("size", IntegerType(), True),
            StructField("price", DoubleType(), True),
            StructField("timestamp", TimestampType(), True)
        ])))
    ]))
])

parsed_data = data.withColumn("jsonData", from_json(col("json"), schema)).select("jsonData.*")

filtered_data = parsed_data.filter(
    (col("data.table") == "orderBookL2_25") &
    (col("data.action") == "insert")
)

exploded_data = filtered_data.select(
    col("data.table"),
    col("data.action"),
    col("data.data").alias("trades")
).selectExpr("table", "action", "explode(trades) as trade")

trades_data = exploded_data.select(
    col("trade.symbol"),
    col("trade.size"),
    col("trade.timestamp")
)

aggregated_data = trades_data \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(window(col("timestamp"), "1 minute").alias("window"),
             col("symbol")
             ) \
    .agg(spark_count("size").alias("trade_count"),
         spark_sum("size").alias("trade_volume")
         ) \
    .select(col("window.start").alias("start_time"),
                col("window.end").alias("end_time"),
            col("symbol"),
            col("trade_count"),
            col("trade_volume")
            )

query = aggregated_data.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
