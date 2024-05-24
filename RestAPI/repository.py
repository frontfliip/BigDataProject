import logging
from datetime import datetime, timedelta

from cassandra.cluster import Cluster

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, sum as spark_sum

logging.basicConfig(level=logging.DEBUG)


class Repository:

    def __init__(self, host="cassandra", port="9042", keyspace="bitmex_stream_data"):
        self.host = host
        self.port = port
        self.keyspace = keyspace
        cluster = Cluster([self.host], port=self.port)
        self.session = cluster.connect(self.keyspace)
        self.pr()
        # self.spark = SparkSession.builder \
        #     .appName("TopCryptocurrencies") \
        #     .config("spark.cassandra.connection.host", self.host) \
        #     .config("spark.cassandra.connection.port", self.port) \
        #     .getOrCreate()

    def number_of_transaction_for_cryptocurrency_n_last_min(self, symbol, minutes):
        print("Start n min", flush=True)
        now = datetime.utcnow()
        end_time = now.replace(second=0, microsecond=0)
        print(end_time)
        start_time = end_time - timedelta(minutes=minutes)
        print(start_time)
        end_time -= timedelta(minutes=1)
        print(end_time)
        query = """
            SELECT symbol, SUM(trade_count) as total_trades, SUM(trade_volume) as total_volume
            FROM ad_hoc_data
            WHERE symbol = %s
            AND start_time >= %s
            AND start_time <= %s;
        """

        rows = self.session.execute(query, (symbol, start_time, end_time))
        print(rows, flush=True)
        logging.info(rows)

        for row in rows:
            return {"symbol": symbol, "total_trades": row.total_trades}

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
    #         (col("start_time") >= start_time) &
    #         (col("start_time") <= end_time)
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
        print(rows, flush=True)
        logging.info(rows)
        for row in rows:
            result = {"Buy Price": row.buy_price, "Sell Price": row.sell_price}
            print(result, flush=True)
            return result

    def get_check(self):
        print(self.host)
        print({"Response": "COOL"}, flush=True)
        logging.info({"Response": "COOL"})
        return {"Response": "COOL"}

    def pr(self):
        print("Repository is created", flush=True)
        logging.info("Repository is created")
        query = """
            SELECT *
            FROM currency_price_data;
        """
        rows = self.session.execute(query)
        for row in rows:
            print(row, flush=True)
