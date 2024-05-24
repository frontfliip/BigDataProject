from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as sum_agg, from_json, to_timestamp, window, count, explode
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DoubleType, IntegerType, TimestampType

# # Create a Spark Session
spark = SparkSession.builder \
    .appName("SparkToCassandraStreaming") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()


# Define the schema for parsing JSON from Kafka
schema = StructType([
    StructField("tag", StringType()),
    StructField("data", StructType([
        StructField("table", StringType()),
        StructField("action", StringType()),
        StructField("data", ArrayType(StructType([
            StructField("symbol", StringType()),
            StructField("id", IntegerType()),
            StructField("side", StringType()),
            StructField("size", IntegerType()),
            StructField("price", DoubleType()),
            StructField("timestamp", TimestampType())
        ])))
    ]))
])


# Read data from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "crypto") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json_string")

# Parse the JSON data
df_parsed = df.select(from_json("json_string", schema).alias("data")).select("data.*")

filtered_data = df_parsed.filter(
    (col("data.table") != "instrument") &
    (col("data.action") == "insert")
)

# Exploding nested array and preparing data
df_flattened = filtered_data.select(
    col("data.data.symbol").alias("symbol"),
    explode(col("data.data")).alias("trade_details")  # Exploding the array to create individual rows
)

# Now you can select individual fields from the exploded "trade_details" which is now a struct
df_trades = df_flattened.select(
    col("trade_details.symbol").alias("symbol"),
    col("trade_details.id"),
    col("trade_details.side"),
    col("trade_details.size"),
    col("trade_details.price"),
    to_timestamp("trade_details.timestamp").alias("timestamp")
)

df_trades = df_trades.filter(col("size").isNotNull() & col("price").isNotNull())


# Adding watermark to handle late data
df_trades = df_trades.withWatermark("timestamp", "1 minute")
# Aggregating data by coin and hourly windows
df_aggregated = df_trades.groupBy(
    "symbol",
    window("timestamp", "1 minutes").alias("minute_window")  # Tumbling window of 1 hour
).agg(
    sum_agg(col("size") * col("price")).alias("total_volume"),  # Calculate total volume
    count("*").alias("total_trades")  # Count the number of trades
).select(
    col("symbol").cast("string"),
    col("total_trades").alias("total_trades").cast("int"),
    col("total_volume").alias("trades_volume").cast("double"),
    col("minute_window.start").alias("timestamp").cast("timestamp")
)

df_1_aggregated = df_trades.groupBy(
    "symbol",
    window("timestamp", "5 minutes").alias("minute_window")
).agg(
    sum_agg(col("size") * col("price")).alias("total_volume"),
    count("*").alias("total_trades")
).select(
    col("symbol").cast("string"),
    col("total_trades").alias("total_trades").cast("int"),
    col("total_volume").alias("trades_volume").cast("double"),
    col("minute_window.start").alias("timestamp").cast("timestamp")
)

query = df_aggregated.writeStream \
    .foreachBatch(lambda batch_df, batch_id: batch_df.write \
                  .format("org.apache.spark.sql.cassandra") \
                  .mode("append") \
                  .options(table="ad_hoc_data", keyspace="bitmex_stream_data") \
                  .save()) \
    .outputMode("complete") \
    .start()

query_1 = df_1_aggregated.writeStream \
    .foreachBatch(lambda batch_df, batch_id: batch_df.write \
                  .format("org.apache.spark.sql.cassandra") \
                  .mode("append") \
                  .options(table="precomputed_data", keyspace="bitmex_stream_data") \
                  .save()) \
    .outputMode("complete") \
    .start()

query.awaitTermination()
query_1.awaitTermination()

#
# |coin|hour_timestamp     |total_volume        |num_trades|
# +----+-------------------+--------------------+----------+
# |sol |2024-05-23 17:00:00|3793512.2899999986  |58944     |
# |aevo|2024-05-23 16:00:00|28101.527999999962  |40148     |
# |eth |2024-05-23 17:00:00|7469792.500000001   |59678     |
# |sol |2024-05-23 16:00:00|1726220.7500000007  |40159     |
# |doge|2024-05-23 17:00:00|2.2749145950000003E7|59325     |
# |eth |2024-05-23 16:00:00|9478629.000000002   |40147     |
# |btc |2024-05-23 16:00:00|1.2321556275E11     |40624     |
# |doge|2024-05-23 16:00:00|750300.0000000001   |39960     |
# |btc |2024-05-23 17:00:00|1.860481273E11      |60665     |
# |aevo|2024-05-23 17:00:00|null                |58992     |
# +----+-------------------+--------------------+----------+

# |coin|hour_timestamp     |total_volume        |num_trades|
# +----+-------------------+--------------------+----------+
# |sol |2024-05-23 17:00:00|4070557.759999999   |65471     |
# |aevo|2024-05-23 16:00:00|30108.779999999962  |40152     |
# |eth |2024-05-23 17:00:00|1.5269820350000005E7|66360     |
# |doge|2024-05-23 17:00:00|2.6849159310000002E7|65918     |
# |btc |2024-05-23 17:00:00|1.9501098E11        |67475     |
# |aevo|2024-05-23 17:00:00|null                |65534     |
# +----+-------------------+--------------------+----------+


# |coin|hour_timestamp     |total_volume      |num_trades|
# +----+-------------------+------------------+----------+
# |doge|2024-05-23 18:27:00|354465.00000000006|6         |
# |btc |2024-05-23 18:30:00|9.90748094E10     |360       |
# |eth |2024-05-23 18:30:00|8082282.3500000015|27        |
# |eth |2024-05-23 18:27:00|1214988.6500000001|32        |
# |sol |2024-05-23 18:27:00|247622.4          |6         |
# |aevo|2024-05-23 18:09:00|67.2              |8         |
# |sol |2024-05-23 18:30:00|226937.55000000005|22        |
# |btc |2024-05-23 18:27:00|1.682622759E11    |413       |
# |sol |2024-05-23 18:24:00|171.86            |1         |
# |doge|2024-05-23 18:30:00|149997.41999999998|3         |
# +----+-------------------+------------------+----------+

# +----+-------------------+------------------+----------+
# |coin|hour_timestamp     |total_volume      |num_trades|
# +----+-------------------+------------------+----------+
# |doge|2024-05-23 18:27:00|354465.00000000006|6         |
# |eth |2024-05-23 18:30:00|1.15106253E7      |75        |
# |btc |2024-05-23 18:30:00|1.4548057455E11   |485       |
# |eth |2024-05-23 18:27:00|1214988.6500000001|32        |
# |sol |2024-05-23 18:27:00|247622.4          |6         |
# |btc |2024-05-23 18:33:00|3.26341042E10     |102       |
# |aevo|2024-05-23 18:09:00|109.2             |13        |
# |sol |2024-05-23 18:30:00|229508.55000000005|25        |
# |btc |2024-05-23 18:27:00|1.682622759E11    |413       |
# |sol |2024-05-23 18:33:00|155743.9          |12        |
# |sol |2024-05-23 18:24:00|171.86            |1         |
# |eth |2024-05-23 18:33:00|283549.35         |10        |
# |doge|2024-05-23 18:33:00|257978.26999999996|10        |
# |doge|2024-05-23 18:30:00|262404.77999999997|9         |
# +----+-------------------+------------------+----------+