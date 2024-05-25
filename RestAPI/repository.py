import logging
from datetime import datetime, timedelta

import pandas as pd
from cassandra.cluster import Cluster

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

    def get_check(self):
        return {"Message": "API is working horey!"}
