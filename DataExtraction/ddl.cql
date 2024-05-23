CREATE KEYSPACE IF NOT EXISTS crypto WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };
USE crypto;

CREATE TABLE IF NOT EXISTS precomputed(
    coin text,
    hour_timestamp text,
    total_volume int,
    num_trades int,
    PRIMARY KEY ((coin), hour_timestamp)
);
