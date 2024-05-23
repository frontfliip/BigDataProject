# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col, window, current_timestamp, hour, expr
# from pyspark.sql.types import StructType, StringType, DoubleType, LongType, IntegerType
#
# # Initialize Spark Session
# spark = SparkSession \
#     .builder \
#     .appName("CryptoDataProcessor") \
#     .getOrCreate()
#
# # Define schema for incoming data
# schema = StructType() \
#     .add("tag", StringType()) \
#     .add("data", StructType() \
#          .add("table", StringType()) \
#          .add("action", StringType()) \
#          .add("data", StringType()))
#
# # Read from Kafka
# df = spark \
#     .readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "kafka:9092") \
#     .option("subscribe", "crypto") \
#     .load()
#
# # Select and parse the data
# df_parsed = df.selectExpr("CAST(value AS STRING) as json") \
#     .select(from_json("json", schema).alias("data")) \
#     .select("data.*")
#
# # Filter out 'instrument' table messages
# df_filtered = df_parsed.filter("data.table != 'instrument'")
#
# # Extract necessary fields (assuming transaction ID from `data.data`)
# transaction_schema = StructType() \
#     .add("symbol", StringType()) \
#     .add("id", LongType()) \
#     .add("side", StringType()) \
#     .add("size", IntegerType()) \
#     .add("price", DoubleType()) \
#     .add("timestamp", StringType())
#
# df_transactions = df_filtered.withColumn("transaction", from_json("data.data", transaction_schema)) \
#     .select("tag", "transaction.*")
#
# # Aggregate by tag, hour, and count transactions
# df_aggregated = df_transactions \
#     .withColumn("timestamp", col("timestamp").cast("timestamp")) \
#     .withColumn("hour", hour("timestamp")) \
#     .groupBy("tag", "hour") \
#     .count()
#
# # Filter based on the last 6 hours excluding the last hour
# current_hour = hour(current_timestamp())
# df_result = df_aggregated.filter((col("hour") < current_hour) & (col("hour") >= current_hour - 6))
#
# # Write output (console for demonstration; in production, consider using a database or file system)
# query = df_result \
#     .writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .start()
#
# query.awaitTermination()



from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as sum_agg, from_json, to_timestamp, window, count, explode
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DoubleType, IntegerType, TimestampType

# # Create a Spark Session
# spark = SparkSession.builder \
#     .appName("CryptoTradeAggregator") \
#     .config("spark.cassandra.connection.host", "Cassandra_IP_Address") \
#     .config("spark.cassandra.connection.port", "9042") \
#     .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
#     .config("spark.cassandra.auth.username", "username") \
#     .config("spark.cassandra.auth.password", "password") \
#     .getOrCreate()

spark = SparkSession \
    .builder \
    .appName("Spark to Cassandra Streaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,com.datastax.spark:spark-cassandra-connector_2.12:3.0.0") \
    .config("spark.sql.streaming.checkpointLocation", "/opt/app/spark-checkpoint") \
    .config("spark.cassandra.connection.host", "cassandra") \
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

# Define the schema for parsing JSON from Kafka
# schema = StructType([
#     StructField("tag", StringType()),
#     StructField("data", StructType([
#         StructField("table", StringType()),
#         StructField("action", StringType()),
#         StructField("data", ArrayType(StructType([
#             StructField("symbol", StringType()),
#             StructField("id", IntegerType()),
#             StructField("side", StringType()),
#             StructField("size", IntegerType()),
#             StructField("price", DoubleType()),
#             StructField("timestamp", TimestampType())
#         ])))
#     ]))
# ])


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
df_flattened = df_parsed.select(
    col("tag").alias("coin"),
    explode(col("data.data")).alias("trade_details")  # Exploding the array to create individual rows
)

# Now you can select individual fields from the exploded "trade_details" which is now a struct
df_trades = df_flattened.select(
    "coin",
    col("trade_details.symbol"),
    col("trade_details.id"),
    col("trade_details.side"),
    col("trade_details.size"),
    col("trade_details.price"),
    to_timestamp("trade_details.timestamp").alias("timestamp")
)

df_trades = df_trades.filter(col("size").isNotNull() & col("price").isNotNull())


# Adding watermark to handle late data
df_trades = df_trades.withWatermark("timestamp", "10 minutes")
# Aggregating data by coin and hourly windows
df_aggregated = df_trades.groupBy(
    "coin",
    window("timestamp", "1 hour").alias("hour_window")  # Tumbling window of 1 hour
).agg(
    sum_agg(col("size") * col("price")).alias("total_volume"),  # Calculate total volume
    count("*").alias("num_trades")  # Count the number of trades
).select(
    "coin",
    col("hour_window.start").alias("hour_timestamp"),  # The start of the hour window
    "total_volume",
    "num_trades"  # The number of trades in each window
)

# Write the aggregated data to console for debugging
query = df_aggregated.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
# Write the aggregated data to Cassandra
# df_aggregated.writeStream \
#     .format("org.apache.spark.sql.cassandra") \
#     .option("keyspace", "crypto") \
#     .option("table", "trading_info") \
#     .option("checkpointLocation", "/path/to/checkpoint/dir") \
#     .outputMode("complete") \
#     .start() \
#     .awaitTermination()

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