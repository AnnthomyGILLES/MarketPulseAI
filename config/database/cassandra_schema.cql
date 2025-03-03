-- Create keyspace for market data with SimpleStrategy replication
-- For production, consider using NetworkTopologyStrategy instead
CREATE KEYSPACE IF NOT EXISTS market_data
WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 3
};

-- Use the market_data keyspace
USE market_data;

-- Create table for stock features
-- Partitioning by symbol, year, month for efficient querying
-- Clustering by date, hour, minute for time-series ordering
CREATE TABLE IF NOT EXISTS stock_features (
    symbol TEXT,
    date TIMESTAMP,
    year INT,
    month INT,
    day INT,
    hour INT,
    minute INT,
    open DECIMAL,
    high DECIMAL,
    low DECIMAL,
    close DECIMAL,
    volume BIGINT,
    sma_5 DECIMAL,
    sma_20 DECIMAL,
    upper_band DECIMAL,
    lower_band DECIMAL,
    volume_sma_5 DECIMAL,
    price_to_sma_ratio DECIMAL,
    high_low_diff DECIMAL,
    daily_range_pct DECIMAL,
    rsi_diff DECIMAL,
    rsi_down DECIMAL,
    volatility DECIMAL,
    returns DECIMAL,
    processing_time TIMESTAMP,
    PRIMARY KEY ((symbol, year, month), date, hour, minute)
) WITH CLUSTERING ORDER BY (date ASC, hour ASC, minute ASC);

-- Create index for efficient querying by date range
CREATE INDEX IF NOT EXISTS idx_stock_features_date ON stock_features (date);

-- Create index for querying by specific day
CREATE INDEX IF NOT EXISTS idx_stock_features_day ON stock_features (day);

-- Create materialized view for querying latest data per symbol
CREATE MATERIALIZED VIEW IF NOT EXISTS latest_stock_features AS
    SELECT * FROM stock_features
    WHERE symbol IS NOT NULL AND year IS NOT NULL AND month IS NOT NULL 
      AND date IS NOT NULL AND hour IS NOT NULL AND minute IS NOT NULL
    PRIMARY KEY ((symbol), date, year, month, hour, minute)
    WITH CLUSTERING ORDER BY (date DESC, hour DESC, minute DESC);
