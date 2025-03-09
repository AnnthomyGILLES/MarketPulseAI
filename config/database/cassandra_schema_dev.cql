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
    -- Adding a primary key with stock name and timestamp
    -- This allows efficient queries by stock and time range
    PRIMARY KEY ((name), date)
) WITH CLUSTERING ORDER BY (date DESC);

-- Create indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_stock_features_date ON stock_features (date);