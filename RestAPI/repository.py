import logging
from datetime import datetime, timedelta
from cassandra.cluster import Cluster
import pytz

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, sum as spark_sum

class Repository:

    def __init__(self, host="cassandra", port=9042, keyspace="bitmex_stream_data"):
        self.host = host
        self.port = port
        self.keyspace = keyspace
        cluster = Cluster([self.host], port=self.port)
        self.session = cluster.connect(self.keyspace)
        # self.spark = SparkSession.builder \
            # .appName("TopCryptocurrencies") \
            # .config("spark.cassandra.connection.host", self.host) \
            # .config("spark.cassandra.connection.port", self.port) \
            # .getOrCreate()

    # def number_of_transaction_for_cryptocurrency_n_last_min(self, symbol, minutes):
    #     print(f"Start transactions for {symbol} in last {minutes} min", flush=True)
    #     now = datetime.utcnow()
    #     end_time = now.replace(second=0, microsecond=0)
    #     print(f"End time {end_time}", flush=True)
    #     start_time = end_time - timedelta(minutes=minutes)
    #     print(f"Start time {start_time}", flush=True)
    #     end_time -= timedelta(minutes=1)
    #     print(f"End time excluding current minute {end_time}", flush=True)
    #     query = """
    #         SELECT symbol, SUM(trade_count) as total_trades, SUM(trade_volume) as total_volume
    #         FROM ad_hoc_data
    #         WHERE symbol = %s
    #         AND timestamp >= %s
    #         AND timestamp <= %s;
    #     """
    #
    #     rows = self.session.execute(query, (symbol, start_time, end_time))
    #     if rows:
    #         for row in rows:
    #             return {"symbol": symbol, "total_trades": row.total_trades}
    #     else:
    #         return {"Message": f"No information about {symbol} in the database..."}

    # def get_top_n_cryptocurrencies_per_hour(self, top_n=5):
    #     now = datetime.utcnow()
    #     end_time = now.replace(second=0, microsecond=0)
    #     start_time = end_time - timedelta(hours=1)
    #
    #     df = self.spark.read \
    #         .format("org.apache.spark.sql.cassandra") \
    #         .options(table="ad_hoc_data", keyspace=self.keyspace) \
    #         .load()
    #
    #     aggregated_df = df.filter(
    #         (col("timestamp") >= start_time) &
    #         (col("timestamp") <= end_time)
    #     ) \
    #         .groupBy("symbol") \
    #         .agg(spark_sum("trade_volume").alias("total_volume")) \
    #         .orderBy(col("total_volume").desc()) \
    #         .limit(top_n)
    #
    #     pandas_df = aggregated_df.toPandas()
    #
    #     result = {}
    #     for index, row in pandas_df.iterrows():
    #         key = f"top {index + 1}"
    #         result[key] = {
    #             "symbol": row['symbol'],
    #             "volume for last hour": row['total_volume']
    #         }
    #     return result

    # result_json = json.dumps(result, indent=4)

    def get_cryptocurrency_current_price(self, cryptocurrency):
        print(cryptocurrency, flush=True)
        print(type(cryptocurrency), flush=True)
        query = """
            SELECT buy_price, sell_price
            FROM currency_price_data
            WHERE symbol = %s
            LIMIT 1;
        """

        rows = self.session.execute(query, (cryptocurrency,))
        if rows:
            for row in rows:
                result = {
                    "Cryptocurrency": cryptocurrency,
                    "Buy Price": row.buy_price,
                    "Sell Price": row.sell_price}
                print(result, flush=True)
                return result
        else:
            return {"Message": f"No information about {cryptocurrency} in the database..."}

    def get_agg_transactions(self):
        now = datetime.utcnow()
        end_time = now.replace(minute=0, second=0, microsecond=0)  # Previous hour
        start_time = end_time - timedelta(hours=6)

        # Execute the query
        rows = self.session.execute(
            "SELECT * FROM precomputed_data WHERE timestamp >= %s AND timestamp < %s ALLOW FILTERING",
            [start_time, end_time])

        # Process the data to aggregate over hourly intervals
        result = {}
        for row in rows:
            symbol = row.symbol
            timestamp = row.timestamp
            total_trades = row.total_trades

            # Use timestamp as-is for aggregation since it's already rounded to the hour
            if (symbol, timestamp) not in result:
                result[(symbol, timestamp)] = 0

            result[(symbol, timestamp)] += total_trades

        # Format the result for output
        total_trades_statistics = [
            {'symbol': symbol, 'hour_start': hour_start, 'total_trades': total_trades}
            for (symbol, hour_start), total_trades in result.items()
        ]
        return total_trades_statistics

    def get_agg_volume(self):
        # Calculate start and end times
        now = datetime.utcnow()
        end_time = now.replace(minute=0, second=0, microsecond=0)  # Previous hour
        start_time = end_time - timedelta(hours=6)

        # Execute the query
        rows = self.session.execute(
            "SELECT * FROM precomputed_data WHERE timestamp >= %s AND timestamp < %s ALLOW FILTERING",
            [start_time, end_time])

        # Process the data to aggregate over hourly intervals
        result = {}
        for row in rows:
            symbol = row.symbol
            timestamp = row.timestamp
            trades_volume = row.trades_volume

            # Use timestamp as-is for aggregation since it's already rounded to the hour
            if (symbol, timestamp) not in result:
                result[(symbol, timestamp)] = 0.0

            result[(symbol, timestamp)] += trades_volume

        # Format the result for output
        trades_volume_statistics = [
            {'symbol': symbol, 'hour_start': hour_start, 'trades_volume': trades_volume}
            for (symbol, hour_start), trades_volume in result.items()
        ]

        return trades_volume_statistics

    def total_agg_stats(self):
        now = datetime.utcnow()
        end_time = now.replace(minute=0, second=0, microsecond=0)  # Previous hour
        start_time = end_time - timedelta(hours=12)

        # Execute the query
        rows = self.session.execute(
            "SELECT * FROM precomputed_data WHERE timestamp >= %s AND timestamp < %s ALLOW FILTERING",
            [start_time, end_time])

        # Process the data to aggregate over hourly intervals
        result = {}
        for row in rows:
            timestamp = row.timestamp
            total_trades = row.total_trades
            trades_volume = row.trades_volume


            # Use timestamp as-is for aggregation since it's already rounded to the hour
            if timestamp not in result:
                result[timestamp] = {'total_trades': 0, 'trades_volume': 0.0}

            result[timestamp]['total_trades'] += total_trades
            result[timestamp]['trades_volume'] += trades_volume


        # Format the result for output
        aggregated_statistics = [
            {'hour_start': hour_start, 'total_trades': data['total_trades'], 'trades_volume': data['trades_volume']}
            for hour_start, data in result.items()
        ]


        return aggregated_statistics
