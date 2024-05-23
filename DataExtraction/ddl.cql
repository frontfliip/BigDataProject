CREATE KEYSPACE IF NOT EXISTS bitmex_stream_data WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
USE bitmex_stream_data;

CREATE TABLE IF NOT EXISTS ad_hoc_data
(
    symbol       text,
    trade_count  int,
    trade_volume float,
    start_time   timestamp,
    end_time     timestamp,
    PRIMARY KEY (symbol, end_time)
);

DESCRIBE TABLES;
