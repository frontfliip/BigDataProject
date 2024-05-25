from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as sum_agg, from_json, to_timestamp, window, count, explode
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DoubleType, IntegerType, TimestampType

spark = SparkSession.builder \
    .appName("SparkToCassandraStreaming") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

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

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "crypto") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json_string")

df_parsed = df.select(from_json("json_string", schema).alias("data")).select("data.*")

filtered_data = df_parsed.filter(
    (col("data.table") != "instrument") &
    (col("data.action") == "insert")
)

df_flattened = filtered_data.select(
    col("data.data.symbol").alias("symbol"),
    explode(col("data.data")).alias("trade_details")
)

df_trades = df_flattened.select(
    col("trade_details.symbol").alias("symbol"),
    col("trade_details.id"),
    col("trade_details.side"),
    col("trade_details.size"),
    col("trade_details.price"),
    to_timestamp("trade_details.timestamp").alias("timestamp")
)

df_trades = df_trades.filter(col("size").isNotNull() & col("price").isNotNull())

df_trades_1_min = df_trades.withWatermark("timestamp", "1 minute")
df_trades_1_hour = df_trades_1_min
# df_trades_orders = df_trades_1_min

df_aggregated_1_min = df_trades_1_min.groupBy(
    "symbol",
    window("timestamp", "1 minute").alias("minute_window")
).agg(
    sum_agg(col("size") * col("price")).alias("total_volume"),
    count("*").alias("total_trades")
).select(
    col("symbol").cast("string"),
    col("total_trades").alias("total_trades").cast("int"),
    col("total_volume").alias("trades_volume").cast("double"),
    col("minute_window.start").alias("timestamp").cast("timestamp")
)

df_aggregated_1_hour = df_trades_1_hour.groupBy(
    "symbol",
    window("timestamp", "1 hour").alias("hour_window")
).agg(
    sum_agg(col("size") * col("price")).alias("total_volume"),
    count("*").alias("total_trades")
).select(
    col("symbol").cast("string"),
    col("total_trades").alias("total_trades").cast("int"),
    col("total_volume").alias("trades_volume").cast("double"),
    col("hour_window.start").alias("timestamp").cast("timestamp")
)

def process_order_book(batch_df, batch_id):
    buy_orders = batch_df.filter(col("side") == "Buy").select(
        col("symbol"),
        col("price").alias("buy_price"),
        col("timestamp").alias("time")
    )

    sell_orders = batch_df.filter(col("side") == "Sell").select(
        col("symbol"),
        col("price").alias("sell_price"),
        col("timestamp").alias("time")
    )

    joined_orders = buy_orders.join(sell_orders, ["symbol", "time"], "outer")

    joined_orders.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="currency_price_data", keyspace="bitmex_stream_data") \
        .save()

# query_orders = df_trades_orders.writeStream \
#     .foreachBatch(process_order_book) \
#     .outputMode("update") \
#     .start()

query_aggregated_1_min = df_aggregated_1_min.writeStream \
    .foreachBatch(lambda batch_df, batch_id: batch_df.write \
                  .format("org.apache.spark.sql.cassandra") \
                  .mode("append") \
                  .options(table="ad_hoc_data", keyspace="bitmex_stream_data") \
                  .save()) \
    .outputMode("complete") \
    .start()

query_aggregated_5_min = df_aggregated_1_hour.writeStream \
    .foreachBatch(lambda batch_df, batch_id: batch_df.write \
                  .format("org.apache.spark.sql.cassandra") \
                  .mode("append") \
                  .options(table="precomputed_data", keyspace="bitmex_stream_data") \
                  .save()) \
    .outputMode("complete") \
    .start()

query_aggregated_1_min.awaitTermination()
query_aggregated_5_min.awaitTermination()
