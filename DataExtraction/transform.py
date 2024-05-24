# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, from_json, window, sum as spark_sum, count as spark_count
# from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, DoubleType, TimestampType
#
# host = "cassandra"
# port = "9042"
# kafka_bootstrap_servers = "kafka:9092"
# input_topic_name = "crypto"
#
# spark = SparkSession.builder.appName("BitMexCryptoStream") \
#     .config("spark.jars.packages",
#             "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,com.datastax.spark:spark-cassandra-connector_2.12:3.0.0") \
#     .config("spark.sql.streaming.checkpointLocation", "/opt/app/spark-checkpoint") \
#     .config("spark.cassandra.connection.host", host) \
#     .config("spark.cassandra.connection.port", port) \
#     .getOrCreate()
#
#
# df = spark \
#     .readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
#     .option("subscribe", input_topic_name) \
#     .option("startingOffsets", "earliest") \
#     .load()
#
# data = df.selectExpr("CAST(value AS STRING) as json")
#
# schema = StructType([
#     StructField("tag", StringType(), True),
#     StructField("data", StructType([
#         StructField("table", StringType(), True),
#         StructField("action", StringType(), True),
#         StructField("data", ArrayType(StructType([
#             StructField("symbol", StringType(), True),
#             StructField("id", IntegerType(), True),
#             StructField("side", StringType(), True),
#             StructField("size", IntegerType(), True),
#             StructField("price", DoubleType(), True),
#             StructField("timestamp", TimestampType(), True)
#         ])))
#     ]))
# ])
#
# parsed_data = data.withColumn("jsonData", from_json(col("json"), schema)).select("jsonData.*")
#
# filtered_data = parsed_data.filter(
#     (col("data.table") == "orderBookL2_25") &
#     (col("data.action") == "insert")
# )
#
# exploded_data = filtered_data.select(
#     col("data.table"),
#     col("data.action"),
#     col("data.data").alias("trades")
# ).selectExpr("table", "action", "explode(trades) as trade")
#
# trades_data = exploded_data.select(
#     col("trade.symbol"),
#     col("trade.size"),
#     col("trade.timestamp")
# )
#
# aggregated_data = trades_data \
#     .withWatermark("timestamp", "1 minute") \
#     .groupBy(window(col("timestamp"), "1 minute").alias("window"),
#              col("symbol")
#              ) \
#     .agg(spark_count("size").alias("trade_count"),
#          spark_sum("size").alias("trade_volume")
#          ) \
#     .select(col("window.start").alias("start_time"),
#             col("window.end").alias("end_time"),
#             col("symbol"),
#             col("trade_count"),
#             col("trade_volume")
#             )
#
# query = aggregated_data.writeStream \
#     .format("org.apache.spark.sql.cassandra") \
#     .option("keyspace", "bitmex_stream_data") \
#     .option("table", "ad_hoc_data") \
#     .option("checkpointLocation", "/opt/app/cassandra_checkpoint") \
#     .start() \
#     .awaitTermination()
#
# # query = aggregated_data.writeStream \
# #     .outputMode("append") \
# #     .format("console") \
# #     .option("truncate", "false") \
# #     .start()
# #
# # query.awaitTermination()

import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, to_timestamp, sum as sum_agg, explode, count
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, DoubleType, TimestampType

logging.basicConfig(level=logging.DEBUG)

kafka_bootstrap_servers = "kafka:9092"
input_topic_name = "crypto"

spark = (SparkSession
         .builder
         .appName("BitMexCryptoStream")
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0")
         .config("spark.sql.streaming.checkpointLocation", "/opt/app/spark-checkpoint")
         .getOrCreate())

logging.info("Spark session created successfully.")

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

# parsed_data = data.withColumn("jsonData", from_json(col("json"), schema)).select("jsonData.*")
parsed_data = data.select(from_json("json", schema).alias("data")).select("data.*")

filtered_data = parsed_data.filter(
    (col("data.table") == "orderBookL2_25") &
    (col("data.table") != "instrument") &
    (col("data.action") == "insert")
)

exploded_data = filtered_data.select(
    col("tag").alias("coin"),
    col("data.table"),
    col("data.action"),
    explode(col("data.data")).alias("trade")
)
# ).selectExpr("table", "action", "explode(trades) as trade")

trades_data = exploded_data.select(
    col("trade.symbol"),
    col("trade.size"),
    to_timestamp("trade_details.timestamp").alias("timestamp")
)

aggregated_data = trades_data \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(col("symbol"),
             window(col("timestamp"), "1 minute").alias("window")
             ) \
    .agg(
        sum_agg(col("size") * col("price")).alias("trade_volume"),
        count("*").alias("trade_count")
    ) \
    .select(
            col("symbol"),
            col("trade_count"),
            col("trade_volume"),
            col("window.start").alias("start_time"),
            col("window.end").alias("end_time"),
            )

query = aggregated_data.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
