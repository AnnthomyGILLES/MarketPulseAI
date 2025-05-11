-- Create keyspace
CREATE KEYSPACE IF NOT EXISTS market_data 
WITH REPLICATION = { 
    'class' : 'SimpleStrategy', 
    'replication_factor' : 1 
};

-- Use the keyspace
USE market_data;

-- Enable materialized views at the session level
CONSISTENCY LOCAL_QUORUM;

-- Stock features table with time-series data
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
  AND compaction = {'class': 'TimeWindowCompactionStrategy', 
                   'compaction_window_unit': 'DAYS',
                   'compaction_window_size': 7}
  AND gc_grace_seconds = 86400
  AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'};

-- Create materialized view for querying by date
CREATE MATERIALIZED VIEW IF NOT EXISTS stock_features_by_date AS
    SELECT name, date, open, high, low, close, volume
    FROM stock_features
    WHERE name IS NOT NULL AND date IS NOT NULL
    PRIMARY KEY ((date), name);

-- Create daily statistics table
CREATE TABLE IF NOT EXISTS daily_stock_stats (
    date DATE,
    name TEXT,
    avg_price DOUBLE,
    volume_sum BIGINT,
    price_change DOUBLE,
    percent_change DOUBLE,
    PRIMARY KEY (date, name)
) WITH compaction = {'class': 'SizeTieredCompactionStrategy'};

-- Create index for fast lookups on symbol
CREATE INDEX IF NOT EXISTS idx_daily_stats_name ON daily_stock_stats (name); 