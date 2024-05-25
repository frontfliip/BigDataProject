import logging
from datetime import datetime, timedelta

import pandas as pd
from cassandra.cluster import Cluster
import pytz

logging.basicConfig(level=logging.DEBUG)


class Repository:

    def __init__(self, host="cassandra", port="9042", keyspace="bitmex_stream_data"):
        self.host = host
        self.port = port
        self.keyspace = keyspace
        cluster = Cluster([self.host], port=self.port)
        self.session = cluster.connect(self.keyspace)

    def number_of_transaction_for_cryptocurrency_n_last_min(self, symbol, minutes):

        print(f"Start transactions for {symbol} in last {minutes} min", flush=True)
        now = datetime.utcnow()
        end_time = now.replace(second=0, microsecond=0)
        print(f"End time {end_time}", flush=True)
        start_time = end_time - timedelta(minutes=minutes)
        print(f"Start time {start_time}", flush=True)
        end_time -= timedelta(minutes=1)
        print(f"End time excluding current minute {end_time}", flush=True)
        query = """
            SELECT symbol, SUM(total_trades) as total_trades, SUM(trades_volume) as total_volume
            FROM ad_hoc_data
            WHERE symbol = %s
            AND timestamp >= %s
            AND timestamp <= %s;
        """

        rows = self.session.execute(query, (symbol, start_time, end_time))
        if rows:
            for row in rows:
                return {"symbol": symbol, "total_trades": row.total_trades}
        else:
            return {"Message": f"No information about {symbol} in the database..."}

    def get_top_n_cryptocurrencies_per_hour(self, top_n=5):

        data = []
        top_n = min(top_n, 5)
        columns = ['cryptocurrency', 'total_volume']

        now = datetime.utcnow()
        now_time = now.replace(second=0, microsecond=0)
        print(f"End time {now_time}", flush=True)
        start_time = now_time - timedelta(hours=1)

        query = """
        SELECT symbol as cryptocurrency, SUM(trades_volume) as total_volume FROM ad_hoc_data 
        WHERE symbol = %s 
        AND timestamp > %s;
"""
        cryptocurrency_list = ["BTCUSD", "ETHUSD", "SOLUSD", "AEVOUSD", "DOGEUSD"]

        for cryptocurrency in cryptocurrency_list:
            result_proxy = self.session.execute(query, (cryptocurrency, start_time))
            row = result_proxy.one()
            if row is not None:
                row_dict = {'cryptocurrency': row.cryptocurrency, 'total_volume': row.total_volume}
                data.append(row_dict)
            else:
                data.append({'cryptocurrency': cryptocurrency, 'total_volume': 0})

        df = pd.DataFrame(data, columns=columns)
        df = df.sort_values(by='total_volume', ascending=False)
        top_n_df = df.head(top_n)

        top_n_dict = {
            f"top{i + 1}": {
                "cryptocurrency": row['cryptocurrency'],
                "total_volume": round(row['total_volume'], 2)
            }
            for i, (_, row) in enumerate(top_n_df.iterrows())
        }

        return top_n_dict

    def get_cryptocurrency_current_price(self, cryptocurrency):

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
