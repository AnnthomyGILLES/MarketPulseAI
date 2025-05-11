# Cassandra Setup for Market Data

This directory contains the necessary configuration for setting up Cassandra for market data storage.

## Schema Design

The Cassandra schema is designed for time-series market data with the following components:

1. **stock_features** - Main table storing raw stock market data
    - Partitioned by stock name for efficient per-stock queries
    - Clustered by date in descending order for latest data first
    - Optimized with LeveledCompactionStrategy for read-heavy workloads
    - Uses LZ4 compression for efficient storage

2. **daily_stock_stats** - Aggregated daily statistics
    - Partitioned by date for efficient time-based queries
    - Contains pre-calculated statistics to avoid expensive aggregations

3. **Materialized View (stock_features_by_date)** - View for date-based queries
    - Enables efficient queries across all stocks for a specific date

## Performance Optimizations

The Cassandra setup includes several performance optimizations:

- **Compaction Strategy**: LeveledCompactionStrategy for read-heavy workloads
- **Compression**: LZ4 compression for efficient storage with minimal CPU impact
- **Caching**: Configured key cache for frequently accessed data
- **Memory Settings**: Optimized heap size and GC settings
- **Queries**: Partition and clustering keys designed for efficient queries

## Connection

Connect to Cassandra using:

```bash
cqlsh cassandra 9042
```

## Schema Initialization

The schema is automatically initialized when the container starts via the `cassandra-init` service in
docker-compose.yml. 