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
from pyspark.sql.functions import col, sum as sum_agg, from_json, to_timestamp, window, count
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DoubleType, IntegerType, TimestampType

# Create a Spark Session
spark = SparkSession.builder \
    .appName("CryptoTradeAggregator") \
    .config("spark.cassandra.connection.host", "Cassandra_IP_Address") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
    .config("spark.cassandra.auth.username", "username") \
    .config("spark.cassandra.auth.password", "password") \
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
    .option("kafka.bootstrap.servers", "your_kafka_server:9092") \
    .option("subscribe", "crypto_topic") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json_string")


# Parse the JSON data
df_parsed = df.select(from_json("json_string", schema).alias("data")).select("data.*")

# Exploding nested array and preparing data
df_flattened = df_parsed.withColumn("trade_data", col("data.data.data")).select(
    col("tag").alias("coin"),
    col("trade_data.*")
)

# Filter out non-trade updates if necessary and convert timestamp to appropriate type
df_trades = df_flattened.withColumn("timestamp", to_timestamp("timestamp"))

df_aggregated = df_trades.groupBy(
    "coin",
    window("timestamp", "1 hour").alias("hour_window")
).agg(
    sum_agg(col("size") * col("price")).alias("total_volume"),  # Calculate total volume
    count("*").alias("num_trades")  # Count the number of trades
).select(
    "coin",
    col("hour_window.start").alias("hour_timestamp"),  # The start of the hour window
    "total_volume",
    "num_trades"  # The number of trades in each window
)


# Write the aggregated data to Cassandra
df_aggregated.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "crypto") \
    .option("table", "trading_info") \
    .option("checkpointLocation", "/path/to/checkpoint/dir") \
    .outputMode("complete") \
    .start() \
    .awaitTermination()
