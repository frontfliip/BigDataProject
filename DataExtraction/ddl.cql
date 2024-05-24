CREATE KEYSPACE IF NOT EXISTS bitmex_stream_data WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
USE bitmex_stream_data;

CREATE TABLE IF NOT EXISTS ad_hoc_data
(
    symbol text,
    total_trades int,
    trades_volume double,
    timestamp timestamp,
    PRIMARY KEY (symbol, timestamp)
);

CREATE TABLE IF NOT EXISTS precomputed_data
(
    symbol text,
    total_trades int,
    trades_volume double,
    timestamp timestamp,
    PRIMARY KEY (symbol, timestamp)
);
