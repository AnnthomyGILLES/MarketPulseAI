-- Create keyspace if it doesn't exist
CREATE KEYSPACE IF NOT EXISTS market_data 
WITH REPLICATION = { 
    'class' : 'SimpleStrategy', 
    'replication_factor' : 1 
};

-- Use the keyspace
USE market_data;

-- Create table for stock features
CREATE TABLE IF NOT EXISTS stock_features (
    name TEXT,
    date TIMESTAMP,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume BIGINT,
    PRIMARY KEY ((name), date)
) WITH CLUSTERING ORDER BY (date DESC)
  AND compaction = {'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': 64}
  AND compression = {'chunk_length_in_kb': '64', 'class': 'LZ4Compressor'};

-- Add a secondary index for date-based queries
-- This enables efficient queries across stocks for a specific timeframe
CREATE INDEX IF NOT EXISTS idx_stock_features_date ON stock_features (date);

-- Create a materialized view for querying by date efficiently
CREATE MATERIALIZED VIEW IF NOT EXISTS stock_features_by_date AS
    SELECT name, date, open, high, low, close, volume
    FROM stock_features
    WHERE name IS NOT NULL AND date IS NOT NULL
    PRIMARY KEY ((date), name);

-- Create a table for aggregated daily statistics
CREATE TABLE IF NOT EXISTS daily_stock_stats (
    date DATE,
    name TEXT,
    avg_price DOUBLE,
    volume_sum BIGINT,
    price_change DOUBLE,
    percent_change DOUBLE,
    PRIMARY KEY ((date), name)
); 