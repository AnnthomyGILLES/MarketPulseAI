-- Cassandra schema definition for MarketPulseAI Reddit sentiment aggregates

-- Create Keyspace (Idempotent)
-- Using SimpleStrategy for development/single-node setups.
-- For production clusters, use NetworkTopologyStrategy with appropriate data center replication factors.
CREATE KEYSPACE IF NOT EXISTS marketpulse
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

-- Switch to the keyspace
USE marketpulse;

-- Create Table (Idempotent)
-- Stores aggregated sentiment data for stock symbols from Reddit.
-- Primary Key:
--   - Partition Key: (symbol, window_start) - Efficiently query data for a specific symbol within a specific time window start.
--   - Clustering Key: timestamp DESC - Orders data within a partition by the event time, newest first.
CREATE TABLE IF NOT EXISTS reddit_sentiment_aggregate (
    symbol TEXT,                    // Stock symbol (e.g., TSLA, AAPL)
    window_start TIMESTAMP,         // Start timestamp of the aggregation window
    timestamp TIMESTAMP,            // Event timestamp (e.g., created_utc from Reddit post/comment, used for ordering)

    -- Window Information
    window_end TIMESTAMP,           // End timestamp of the aggregation window
    window_size TEXT,               // Description of the window size (e.g., '5 minutes')

    -- Source Information
    subreddit TEXT,                 // Source subreddit (e.g., wallstreetbets)
    collection_method TEXT,         // How the original data was collected (e.g., hot, new)
    collection_timestamp TIMESTAMP, // When the raw data point was ingested by the collector
    post_created_datetime TIMESTAMP,// Original creation time of the post/comment on Reddit

    -- Sentiment Metrics
    sentiment_score FLOAT,          // Average sentiment score (-1 to 1) within the window
    sentiment_magnitude FLOAT,      // Average sentiment magnitude (proxy, e.g., absolute score)

    -- Count Metrics
    post_count BIGINT,              // Total number of posts/comments aggregated in this window
    unique_authors BIGINT,          // Approximate count of unique authors
    content_types MAP<TEXT, BIGINT>,// Count per content type ('post', 'comment')

    -- Reddit Score Metrics
    avg_score DOUBLE,               // Average Reddit score (upvotes - downvotes)
    min_score INT,                  // Minimum Reddit score in the window
    max_score INT,                  // Maximum Reddit score in the window
    avg_upvote_ratio DOUBLE,        // Average upvote ratio (0 to 1)

    -- Context
    permalink_sample LIST<TEXT>,    // Sample list of permalinks included in the aggregation

    -- Define Primary Key
    PRIMARY KEY ((symbol, window_start), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

-- Optional: Add comments to the table and columns for better understanding
-- COMMENT ON TABLE marketpulse.reddit_sentiment_aggregate IS 'Aggregated Reddit sentiment analysis results per stock symbol and time window.';
-- COMMENT ON COLUMN marketpulse.reddit_sentiment_aggregate.symbol IS 'Stock ticker symbol';
-- ... add comments for other columns as needed ... 